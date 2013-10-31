/*
    

*/

#ifndef SQLPARSE_H
#define SQLPARSE_H

#ifdef __cplusplus
extern "C" {
#endif

enum enum_stmt_command {
  STMT_SELECT, STMT_CREATE_TABLE, STMT_CREATE_INDEX, STMT_ALTER_TABLE,
  STMT_UPDATE, STMT_INSERT, STMT_INSERT_SELECT,
  STMT_DELETE, STMT_TRUNCATE, STMT_DROP_TABLE, STMT_DROP_INDEX,

  STMT_SHOW_DATABASES, STMT_SHOW_TABLES, STMT_SHOW_FIELDS,
  STMT_SHOW_KEYS, STMT_SHOW_VARIABLES, STMT_SHOW_STATUS,
  STMT_SHOW_ENGINE_LOGS, STMT_SHOW_ENGINE_STATUS, STMT_SHOW_ENGINE_MUTEX,
  STMT_SHOW_PROCESSLIST, STMT_SHOW_MASTER_STAT, STMT_SHOW_SLAVE_STAT,
  STMT_SHOW_GRANTS, STMT_SHOW_CREATE, STMT_SHOW_CHARSETS,
  STMT_SHOW_COLLATIONS, STMT_SHOW_CREATE_DB, STMT_SHOW_TABLE_STATUS,
  STMT_SHOW_TRIGGERS,

  STMT_LOAD,STMT_SET_OPTION,STMT_LOCK_TABLES,STMT_UNLOCK_TABLES,
  STMT_GRANT,
  STMT_CHANGE_DB, STMT_CREATE_DB, STMT_DROP_DB, STMT_ALTER_DB,
  STMT_REPAIR, STMT_REPLACE, STMT_REPLACE_SELECT,
  STMT_CREATE_FUNCTION, STMT_DROP_FUNCTION,
  STMT_REVOKE,STMT_OPTIMIZE, STMT_CHECK,
  STMT_ASSIGN_TO_KEYCACHE, STMT_PRELOAD_KEYS,
  STMT_FLUSH, STMT_KILL, STMT_ANALYZE,
  STMT_ROLLBACK, STMT_ROLLBACK_TO_SAVEPOINT,
  STMT_COMMIT, STMT_SAVEPOINT, STMT_RELEASE_SAVEPOINT,
  STMT_SLAVE_START, STMT_SLAVE_STOP,
  STMT_BEGIN, STMT_CHANGE_MASTER,
  STMT_RENAME_TABLE,
  STMT_RESET, STMT_PURGE, STMT_PURGE_BEFORE, STMT_SHOW_BINLOGS,
  STMT_SHOW_OPEN_TABLES,
  STMT_HA_OPEN, STMT_HA_CLOSE, STMT_HA_READ,
  STMT_SHOW_SLAVE_HOSTS, STMT_DELETE_MULTI, STMT_UPDATE_MULTI,
  STMT_SHOW_BINLOG_EVENTS, STMT_DO,
  STMT_SHOW_WARNS, STMT_EMPTY_QUERY, STMT_SHOW_ERRORS,
  STMT_SHOW_STORAGE_ENGINES, STMT_SHOW_PRIVILEGES,
  STMT_HELP, STMT_CREATE_USER, STMT_DROP_USER, STMT_RENAME_USER,
  STMT_REVOKE_ALL, STMT_CHECKSUM,
  STMT_CREATE_PROCEDURE, STMT_CREATE_SPFUNCTION, STMT_CALL,
  STMT_DROP_PROCEDURE, STMT_ALTER_PROCEDURE,STMT_ALTER_FUNCTION,
  STMT_SHOW_CREATE_PROC, STMT_SHOW_CREATE_FUNC,
  STMT_SHOW_STATUS_PROC, STMT_SHOW_STATUS_FUNC,
  STMT_PREPARE, STMT_EXECUTE, STMT_DEALLOCATE_PREPARE,
  STMT_CREATE_VIEW, STMT_DROP_VIEW,
  STMT_CREATE_TRIGGER, STMT_DROP_TRIGGER,
  STMT_XA_START, STMT_XA_END, STMT_XA_PREPARE,
  STMT_XA_COMMIT, STMT_XA_ROLLBACK, STMT_XA_RECOVER,
  STMT_SHOW_PROC_CODE, STMT_SHOW_FUNC_CODE,
  STMT_ALTER_TABLESPACE,
  STMT_INSTALL_PLUGIN, STMT_UNINSTALL_PLUGIN,
  STMT_SHOW_AUTHORS, STMT_BINLOG_BASE64_EVENT,
  STMT_SHOW_PLUGINS,
  STMT_SHOW_CONTRIBUTORS,
  STMT_CREATE_SERVER, STMT_DROP_SERVER, STMT_ALTER_SERVER,
  STMT_CREATE_EVENT, STMT_ALTER_EVENT, STMT_DROP_EVENT,
  STMT_SHOW_CREATE_EVENT, STMT_SHOW_EVENTS,
  STMT_SHOW_CREATE_TRIGGER,
  STMT_ALTER_DB_UPGRADE,
  STMT_SHOW_PROFILE, STMT_SHOW_PROFILES,
  STMT_SIGNAL, STMT_RESIGNAL,
  STMT_SHOW_RELAYLOG_EVENTS, 
  /*
    When a command is added here, be sure it's also added in mysqld.cc
    in "struct show_var_st status_vars[]= {" ...
  */
  /* This should be the last !!! */
  STMT_END
};

/* copy from mysql_com.h */
#define HOSTNAME_LENGTH 60
#define SYSTEM_CHARSET_MBMAXLEN 3
#define NAME_CHAR_LEN	64              /* Field/table name length */
#define USERNAME_CHAR_LENGTH 16
#define NAME_LEN                (NAME_CHAR_LEN*SYSTEM_CHARSET_MBMAXLEN)
#define USERNAME_LENGTH         (USERNAME_CHAR_LENGTH*SYSTEM_CHARSET_MBMAXLEN)

// 增加版本号的同时，要注意修改sqlparse.cc中的数组 version_pos
enum enum_versio {VERSION_5_0, VERSION_5_1, VERSION_5_5, VERSION_TMYSQL_1_0, VERSION_TMYSQL_1_1,
					VERSION_TMYSQL_1_2, VERSION_TMYSQL_1_3, VERSION_TMYSQL_1_4};

struct parse_table_struct {
    char dbname[NAME_LEN];
    char tablename[NAME_LEN];
};
typedef struct parse_table_struct parse_table_t;

#define ROUTINE_TYPE_FUNC 0
#define ROUTINE_TYPE_PROC 1

struct parse_routine_struct {
    char dbname[NAME_LEN];
    char routinename[NAME_LEN];
    int  routine_type;
};
typedef struct parse_routine_struct parse_routine_t;

#define QUERY_FLAGS_CREATE_TEMPORARY_TABLE (1)          /* for STMT_CREATE_TABLE */
#define QUERY_FLAGS_CREATE_FOREIGN_KEY (1 << 1)         /* for STMT_CREATE_TABLE STMT_ALTER_TABLE STMT_CREATE_INDEX */


#define PARSE_RESULT_MAX_STR_LEN 512
struct parse_result_struct {
    void* thd_org;

    int query_type;
    int query_flags;

    char dbname[NAME_LEN];
    char objname[NAME_LEN]; /* 记录DDL对象名(临时表，存储函数） */

    unsigned short n_tables_alloced;
    unsigned short n_tables;
    parse_table_t*  table_arr;

    unsigned short n_routines_alloced;
    unsigned short n_routines;
    parse_routine_t* routine_arr;

    char err_msg[PARSE_RESULT_MAX_STR_LEN + 1];
};
typedef struct parse_result_struct parse_result_t;


/************************************************************************/
/* add by willhan. 2013-06-13                                                                     */
/************************************************************************/
typedef struct info_audit
{
	int non_ascii; //1 means has not ascii, 0 means has only ascii
}info_audit;


enum enum_result_types {SQLPARSE_SUCESS, SQLPARSE_WARNING, SQLPARSE_FAIL, SQLPARSE_FAIL_OTHER};
enum enum_warning_types {WARNINGS_DEFAULT, DROP_DB, DROP_TABLE, DROP_VIEW, TRUNCETE, DELETE_WITHOUT_WHERE, UPDATE_WITHOUT_WHERE,
						CREATE_TABLE_WITH_MUCH_BLOB, ALTER_TABLE_ADD_MUCH_BLOB, CREATE_TABLE_NOT_INNODB,
						CREATE_TABLE_NO_INDEX, ALTER_TABLE_WITH_AFTER, ALTER_TABLE_DEFAULT_WITHOUT_NOT_NULL, CREATE_TABLE_WITH_OTHER_CHARACTER};
struct parse_result_audit {
	void* thd_org;
	int query_type;
	int result;
	int warning_type;
	int result_type;
	/**0 success; 1 risking warning; 2 parse fail; 3 other fail***/

	int tbdb;
	/*tbdb==0 rising warnings from table; tbdb==1 risking warnings from dababase */
	char dbname[NAME_LEN];
	/** database name **/
	int blob_text_count;
	/*records the number of blob/text fields when create/alter table*/

	int table_without_primarykey;  /* 置1表示建表没有主键 */
	
	unsigned short n_tables_alloced;
	unsigned short n_tables;
	parse_table_t*  table_arr;
	//char** table_arr;       /* table name array, each is <dbname>.<tablename> */
	enum_versio mysql_version;

	int errcode;
	char err_msg[PARSE_RESULT_MAX_STR_LEN + 1];

	unsigned long line_number;//the error or warnings sql line

	info_audit info; //other info for result of tmysqlparse
};

int
parse_result_init(parse_result_t* pr);

int
parse_result_destroy(parse_result_t* pr);

void
parse_result_init_db(
    parse_result_t* pr,
    char*           db
);

int
parse_result_add_table(
    parse_result_t* pr, 
    char*           db_name,
    char*           table_name
);

int
query_parse(char* query, parse_result_t* pr);

const char*
parse_result_get_stmt_type_str(
    parse_result_t*    pr
);

void
parse_global_init();

void
parse_global_destroy();




/************************************************************************/
/* add by willhan. 2013-06-13                                                                     */
/************************************************************************/
int parse_result_audit_init(parse_result_audit* pr, char *version);
int query_parse_audit(char *query, parse_result_audit* pra);
int parse_result_audit_destroy(parse_result_audit* pra);
int parse_result_add_table_audit(parse_result_audit* pra, char* db_name, char* table_name);
const char* get_stmt_type_str(int type);
const char* get_warnings_type_str(int type);
void parse_result_audit_init_db(parse_result_audit* pra, char* db);

#ifdef __cplusplus
}
#endif

#endif
