# Copyright (c) 2006, 2011, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/include
  ${ZLIB_INCLUDE_DIR}
  ${SSL_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/libmysql
  ${CMAKE_SOURCE_DIR}/regex
  ${CMAKE_SOURCE_DIR}/sql
  ${CMAKE_SOURCE_DIR}/sqlparse
  ${CMAKE_SOURCE_DIR}/strings
  ${READLINE_INCLUDE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

ADD_DEFINITIONS(${READLINE_DEFINES})
ADD_DEFINITIONS(${SSL_DEFINES})
MYSQL_ADD_EXECUTABLE(mysql completion_hash.cc mysql.cc readline.cc sql_string.cc)
TARGET_LINK_LIBRARIES(mysql mysqlclient)
IF(UNIX)
  TARGET_LINK_LIBRARIES(mysql ${READLINE_LIBRARY})
ENDIF(UNIX)

MYSQL_ADD_EXECUTABLE(mysqltest mysqltest.cc COMPONENT Test)
SET_SOURCE_FILES_PROPERTIES(mysqltest.cc PROPERTIES COMPILE_FLAGS "-DTHREADS")
TARGET_LINK_LIBRARIES(mysqltest mysqlclient regex)


MYSQL_ADD_EXECUTABLE(mysqlcheck mysqlcheck.c)
TARGET_LINK_LIBRARIES(mysqlcheck mysqlclient)

MYSQL_ADD_EXECUTABLE(mysqldump mysqldump.c ../sql-common/my_user.c)
TARGET_LINK_LIBRARIES(mysqldump mysqlclient)

MYSQL_ADD_EXECUTABLE(tmysqldump tmysqldump.cc ../sql-common/my_user.c)
TARGET_LINK_LIBRARIES(tmysqldump mysqlclient)


MYSQL_ADD_EXECUTABLE(mysqlimport mysqlimport.c)
TARGET_LINK_LIBRARIES(mysqlimport mysqlclient)

MYSQL_ADD_EXECUTABLE(mysql_upgrade mysql_upgrade.c)
TARGET_LINK_LIBRARIES(mysql_upgrade mysqlclient)
ADD_DEPENDENCIES(mysql_upgrade GenFixPrivs)

MYSQL_ADD_EXECUTABLE(mysqlshow mysqlshow.c)
TARGET_LINK_LIBRARIES(mysqlshow mysqlclient)

MYSQL_ADD_EXECUTABLE(mysql_plugin mysql_plugin.c)
TARGET_LINK_LIBRARIES(mysql_plugin mysqlclient)

MYSQL_ADD_EXECUTABLE(mysqlbinlog mysqlbinlog.cc)
TARGET_LINK_LIBRARIES(mysqlbinlog mysqlclient)

IF(UNIX)
  MYSQL_ADD_EXECUTABLE(tmysqlbinlogex tmysqlbinlogex.cc ../sqlparse/message_queue.cc DESTINATION ${INSTALL_BINDIR}/tmysqlbinlogex)
ELSE()
  MYSQL_ADD_EXECUTABLE(tmysqlbinlogex tmysqlbinlogex.cc ../sqlparse/message_queue.cc)
ENDIF(UNIX)
TARGET_LINK_LIBRARIES(tmysqlbinlogex mysqlclient sqlparse)

IF(MSVC)
	 ADD_CUSTOM_COMMAND(TARGET tmysqlbinlogex
                     POST_BUILD 
                     COMMAND copy ..\\sqlparse\\Debug\\sqlparse.dll Debug\\sqlparse.dll)
ENDIF()

#add by willhan. 2013-06-07
IF(UNIX)
  MYSQL_ADD_EXECUTABLE(tmysqlparse completion_hash.cc tmysqlparse.cc readline.cc sql_string.cc DESTINATION ${INSTALL_BINDIR}/tmysqlparse)
ELSE()
  MYSQL_ADD_EXECUTABLE(tmysqlparse completion_hash.cc tmysqlparse.cc readline.cc sql_string.cc)
ENDIF(UNIX)

TARGET_LINK_LIBRARIES(tmysqlparse mysqlclient sqlparse)
IF(UNIX)
  TARGET_LINK_LIBRARIES(tmysqlparse ${READLINE_LIBRARY})
ENDIF(UNIX)

IF(MSVC)
	 ADD_CUSTOM_COMMAND(TARGET tmysqlparse
                     POST_BUILD 
                     COMMAND copy ..\\sqlparse\\Debug\\sqlparse.dll Debug\\sqlparse.dll)
ENDIF()


MYSQL_ADD_EXECUTABLE(mysqladmin mysqladmin.cc)
TARGET_LINK_LIBRARIES(mysqladmin mysqlclient)

MYSQL_ADD_EXECUTABLE(mysqlslap mysqlslap.c)
SET_SOURCE_FILES_PROPERTIES(mysqlslap.c PROPERTIES COMPILE_FLAGS "-DTHREADS")
TARGET_LINK_LIBRARIES(mysqlslap mysqlclient)

# "WIN32" also covers 64 bit. "echo" is used in some files below "mysql-test/".
IF(WIN32)
  MYSQL_ADD_EXECUTABLE(echo echo.c)
ENDIF(WIN32)

SET_TARGET_PROPERTIES (mysqlcheck mysqldump mysqlimport mysql_upgrade mysqlshow mysqlslap mysql_plugin 
PROPERTIES HAS_CXX TRUE)

ADD_DEFINITIONS(-DHAVE_DLOPEN)

