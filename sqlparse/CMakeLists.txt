# Add sqlparse module by vinchen 2013/3/5

INCLUDE_DIRECTORIES(
${CMAKE_SOURCE_DIR}/include 
${CMAKE_SOURCE_DIR}/sql
${CMAKE_SOURCE_DIR}/sqlparse 
${CMAKE_SOURCE_DIR}/regex 
${CMAKE_SOURCE_DIR}/client 
${CMAKE_SOURCE_DIR}/libmysql 
${CMAKE_SOURCE_DIR}/vio
${CMAKE_SOURCE_DIR}/strings
${ZLIB_INCLUDE_DIR}
${SSL_INCLUDE_DIRS}
${CMAKE_BINARY_DIR}/sql
${CMAKE_BINARY_DIR}/sqlparse
)

SET(PARSE_API_FUNCTIONS
parse_global_init
parse_global_destroy
parse_result_init
parse_result_destroy
query_parse
CACHE INTERNAL "Functions exported by sqlparse API"
)

ADD_DEFINITIONS(-DMYSQL_SERVER -DEMBEDDED_LIBRARY -DHAVE_EVENT_SCHEDULER) 


IF(WIN32)
  SET(SQL_PARSE_SOURCE sqlparse.cc ../sql/nt_servc.cc)
ELSE()
  SET(SQL_PARSE_SOURCE sqlparse.cc ${DTRACE_PROBES_ALL})
ENDIF()




SET(SQL_PARSE_SOURCE sqlparse.cc)

ADD_CONVENIENCE_LIBRARY(sqlparse_base ${SQL_PARSE_SOURCE})
DTRACE_INSTRUMENT(sqlparse_base)
#ADD_DEPENDENCIES(sqlparse_base mysqlserver)
############


SET(PARSE_LIBS sqlparse_base mysqlserver)

IF(WIN32)
  ADD_DEFINITIONS(-DAUTHENTICATION_WIN)
  LIST(APPEND PARSE_LIBS auth_win_client)
ENDIF()

# Merge several convenience libraries into one big mysqlclient
# and link them together into shared library.
MERGE_LIBRARIES(sqlparselib STATIC ${PARSE_LIBS} COMPONENT Development)

# Visual Studio users need debug  static library for debug projects
IF(MSVC)
 INSTALL_DEBUG_TARGET(sqlparselib DESTINATION ${INSTALL_LIBDIR}/debug)
ENDIF()

# to do
IF(UNIX)
  INSTALL_SYMLINK(${CMAKE_STATIC_LIBRARY_PREFIX}tmysqlparse.a sqlparselib ${INSTALL_LIBDIR} Development)
ENDIF()


IF(NOT DISABLE_SHARED)
  MERGE_LIBRARIES(sqlparse SHARED ${PARSE_LIBS} EXPORTS ${PARSE_API_FUNCTIONS} COMPONENT SharedLibraries)
  IF(UNIX)
    # libtool compatability
    IF(CMAKE_SYSTEM_NAME MATCHES "FreeBSD" OR APPLE)
      SET(OS_SHARED_LIB_VERSION "${SHARED_LIB_MAJOR_VERSION}")
    ELSEIF(CMAKE_SYSTEM_NAME MATCHES "HP-UX")
      SET(OS_SHARED_LIB_VERSION "${SHARED_LIB_MAJOR_VERSION}.0")
    ELSE()
      SET(OS_SHARED_LIB_VERSION "${SHARED_LIB_MAJOR_VERSION}.0.0")
    ENDIF()
    # Name of shared library is sqlparse on Unix
    SET_TARGET_PROPERTIES(sqlparse PROPERTIES 
      OUTPUT_NAME tmysqlparse 
      VERSION "${OS_SHARED_LIB_VERSION}" 
      SOVERSION "${SHARED_LIB_MAJOR_VERSION}")
    IF(LINK_FLAG_NO_UNDEFINED)
      GET_TARGET_PROPERTY(sqlparse_link_flags sqlparse LINK_FLAGS)
      IF(NOT sqlparse_link_flag)
        SET(sqlparse_link_flags)
      ENDIF()
      SET_TARGET_PROPERTIES(sqlparse PROPERTIES LINK_FLAGS 
        "${sqlparse_link_flags} ${LINK_FLAG_NO_UNDEFINED}")
    ENDIF() 
    # clean direct output needs to be set several targets have the same name
    #(sqlparse in this case)
    SET_TARGET_PROPERTIES(sqlparselib PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    SET_TARGET_PROPERTIES(sqlparse PROPERTIES CLEAN_DIRECT_OUTPUT 1)
  ENDIF()
ENDIF()


##############################
MYSQL_ADD_EXECUTABLE(sqlparse_test test_ps.cc DESTINATION ${INSTALL_SBINDIR} COMPONENT Server)

IF(APPLE) 
  # Add CoreServices framework since some dloadable plugins may need it 
  FIND_LIBRARY(CORESERVICES NAMES CoreServices) 
  IF(CORESERVICES) 
    TARGET_LINK_LIBRARIES(sqlparse_test ${CORESERVICES}) 
  ENDIF() 
ENDIF() 

SET_TARGET_PROPERTIES(sqlparse_test PROPERTIES ENABLE_EXPORTS TRUE) 
TARGET_LINK_LIBRARIES(sqlparse_test sqlparselib)

SET(PARSE_INTERFACE_LIBS ${LIBRT})
IF(PARSE_INTERFACE_LIBS)
 SET_TARGET_PROPERTIES(sqlparse_test PROPERTIES LINK_INTERFACE_LIBRARIES 
  "${PARSE_INTERFACE_LIBS}")
ENDIF()

