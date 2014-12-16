#define MYSQL_LEX 1
#include "my_global.h"
#include "unireg.h"                    // REQUIRED: for other includes
#include "sql_parse.h"        // sql_kill, *_precheck, *_prepare
#include "sql_db.h"           // mysql_change_db, mysql_create_db,
#include "sqlparse.h"
#include "sp_head.h"
#include "mysql_com.h"
#include "sql_show.h"
#include "sql_string.h"


extern int parse_export;
#define PARSE_RESULT_N_TABLE_ARR_INITED 5
#define PARSE_RESULT_N_ROUTINE_ARR_INITED PARSE_RESULT_N_TABLE_ARR_INITED
#define PARSE_RESULT_DEFAULT_DB_NAME "<unknow_db>"

static int ignore_error_code[] = {ER_UNKNOWN_SYSTEM_VARIABLE,0};//0 means the end


static int version_pos[] = {0, 6, 13, 13, 13, 13, 13, 13};
static  char word_segmentation[] = {' ' , '\t', '\n', '\r', ',', '.', '(', ')', '<', '>', '=', '!', '\0'}; 
static parse_option sqlparse_option ;



static const char *reserve_words[] =
{
	"ACCESSIBLE",
	"LINEAR",
	"MASTER_SSL_VERIFY_SERVER_CERT",
	"RANGE",
	"READ_ONLY",
	"READ_WRITE",  /* 5.1 reserved words */
	"GENERAL",
	"IGNORE_SERVER_IDS",
	"MASTER_HEARTBEAT_PERIOD",
	"MAXVALUE",
	"RESIGNAL",
	"SIGNAL",
	"SLOW"			/* 5.5 reserved words */
};

void my_init_for_sqlparse();
void my_end_for_sqlparse();
static my_pthread_once_t sqlparse_global_inited = MY_PTHREAD_ONCE_INIT;
static int process_msg_error(int err_code, char *err_msg);
static int set_current_version(char *version, enum_version *current_version);
static const char* has_reserve_in_errmsg(enum_version mysql_version, char *err_msg);
static int isprefix_word(const char *s, const char *t, int first_pos);
static int is_word_segmentation(char ch);
static char* process_sql_for_reserve(char *fromsql, char *tosql, size_t to_len,const char *reserve);
//static int find_reserve_pos(char *reserve);
static void cp_parse_option(parse_option *dest, parse_option *src);
static int parse_create_info(THD *thd, String *packet, bool show_database);
static void filed_add_zerofill_and_unsigned(String &res, bool unsigned_flag, bool zerofill);
static void gettype_create_filed(Create_field *cr_field, String &res);
static bool get_createfield_default_value(Create_field *cr_field, String *def_value);
static void parse_append_directory(THD *thd, String *packet, const char *dir_type, const char *filename);
static int parse_getkey_for_spider(THD *thd,  char *key, char *db_name, char *table_name,char *result, int result_len);
static void print_quoted_xml_for_parse(FILE *xml_file, const char *str, ulong len);



void cp_parse_option(parse_option *dest, parse_option *src)
{
	dest->is_split_sql = src->is_split_sql;
	strcpy(dest->file_path, src->file_path);

	dest->is_show_create = src->is_show_create;
	strcpy(dest->show_create_file, src->show_create_file);
}


HASH create_hash;
HASH alter_hash;
HASH other_hash;

uchar* get_table_key(const char *entry, size_t *length,
                     my_bool not_used __attribute__((unused)))
{
    *length= strlen(entry);
    return (uchar*) entry;
}

void parse_global_init()
{
    my_pthread_once(&sqlparse_global_inited, my_init_for_sqlparse);
}

void parse_global_destroy()
{
    my_end_for_sqlparse();
    sqlparse_global_inited = MY_PTHREAD_ONCE_INIT;

#if defined(__WIN__) && defined(_MSC_VER)
    _CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE );
    _CrtSetReportFile( _CRT_WARN, _CRTDBG_FILE_STDERR );
    _CrtSetReportMode( _CRT_ERROR, _CRTDBG_MODE_FILE );
    _CrtSetReportFile( _CRT_ERROR, _CRTDBG_FILE_STDERR );
    _CrtSetReportMode( _CRT_ASSERT, _CRTDBG_MODE_FILE );
    _CrtSetReportFile( _CRT_ASSERT, _CRTDBG_FILE_STDERR );
    _CrtCheckMemory();
    _CrtDumpMemoryLeaks();
#endif
}

int
parse_result_init(parse_result_t* pr)
{
    THD* thd;

    //_CrtSetBreakAlloc(135);
    //_CrtSetBreakAlloc(136);
    //_CrtSetBreakAlloc(110);

    //for safe 
    parse_global_init();

    thd= new THD;

    thd->thread_id = 0xFFFFFFFF;
    thd->prior_thr_create_utime= thd->start_utime= my_micro_time();

    /*
    The thread stack does not start from this function but we cannot
    guess the real value. So better some value that doesn't assert than
    no value.
    */
    thd->thread_stack= (char*) &thd;
    if (thd->store_globals()){
        delete thd;    
        return -1;
    }
    thd->security_ctx->skip_grants();
    thd->set_db(PARSE_RESULT_DEFAULT_DB_NAME, strlen(PARSE_RESULT_DEFAULT_DB_NAME));

    /* init parse_result */
    memset(pr, 0, sizeof(parse_result_t));
    pr->thd_org = thd;

    /* allocate the table name buffer */
    pr->n_tables_alloced = PARSE_RESULT_N_TABLE_ARR_INITED;
    pr->n_tables = 0;
    pr->table_arr = (parse_table_t*)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(parse_table_t));

    pr->n_routines_alloced = PARSE_RESULT_N_TABLE_ARR_INITED;
    pr->n_routines = 0;
    pr->routine_arr = (parse_routine_t*)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(parse_routine_t));

    return 0;
}

int
parse_result_destroy(parse_result_t* pr)
{
    THD* thd;

    /* free table name buffer array */
    if (pr->table_arr) {

        free(pr->table_arr);
        pr->table_arr = NULL;
        pr->n_tables_alloced = pr->n_tables = 0;
    }

    if (pr->routine_arr)
    {
        free(pr->routine_arr);
        pr->routine_arr = NULL;
        pr->n_routines_alloced = pr->n_routines = 0;
    }

    thd = (THD*)pr->thd_org;
    if (thd != NULL)
    {
        thd->clear_error();
        /*
        Some extra safety, which should not been needed (normally, event deletion
        should already have done these assignments (each event which sets these
        variables is supposed to set them to 0 before terminating)).
        */
        thd->catalog= 0;
        thd->reset_query();

        if (thd->db)
            my_free(thd->db);
        thd->reset_db(NULL, 0);

        //close_connection(thd);
        delete thd;

        pr->thd_org = NULL;
    }

    return 0;
}

int
parse_result_add_table(
    parse_result_t* pr, 
    char*           db_name,
    char*           table_name
)
{
    int i;
    DBUG_ASSERT(db_name && table_name);
    DBUG_ASSERT(pr->n_tables <= pr->n_tables_alloced);

    if (strlen(db_name) > NAME_LEN - 1 || strlen(table_name) > NAME_LEN - 1)
    {
        sprintf(pr->err_msg, "%s", "too long db_name or table_name");
        return -1;
    }
    for (i = 0; i < pr->n_tables; i++)
    {
        if (!strcmp(pr->table_arr[i].dbname, db_name) && !strcmp(pr->table_arr[i].tablename, table_name))
            return 0;
    }
    //buffer is not enough
    if (pr->n_tables >= pr->n_tables_alloced)
    {
        parse_table_t* new_table_arr;

        new_table_arr = (parse_table_t*)calloc(pr->n_tables_alloced * 2, sizeof(parse_table_t));

        memcpy(new_table_arr, pr->table_arr, sizeof(parse_table_t) * pr->n_tables_alloced);
        free(pr->table_arr);

        pr->table_arr = new_table_arr;
        pr->n_tables_alloced *= 2;
    }

    strcpy(pr->table_arr[pr->n_tables].dbname, db_name);
    strcpy(pr->table_arr[pr->n_tables++].tablename, table_name);

    return 0;
}

int
parse_result_add_routine(
    parse_result_t* pr, 
    char*           db_name,
    char*           routine_name,
    bool            is_proc
)
{
    int i;
    DBUG_ASSERT(db_name && routine_name);
    DBUG_ASSERT(pr->n_routines <= pr->n_routines_alloced);
    int routine_type;

    routine_type = is_proc ? ROUTINE_TYPE_PROC : ROUTINE_TYPE_FUNC;

    if (strlen(db_name) > NAME_LEN - 1 || strlen(routine_name) > NAME_LEN - 1)
    {
        sprintf(pr->err_msg, "%s", "too long db_name or routine_name");
        return -1;
    }
    for (i = 0; i < pr->n_routines; i++)
    {
        /* 已存在同名存储过程或函数 */
        if (!strcmp(pr->routine_arr[i].dbname, db_name) && !strcmp(pr->routine_arr[i].routinename, routine_name) &&
            pr->routine_arr[i].routine_type == routine_type)
            return 0;
    }
    //buffer is not enough
    if (pr->n_routines >= pr->n_routines_alloced)
    {
        parse_routine_t* new_routine_arr;

        new_routine_arr = (parse_routine_t*)calloc(pr->n_routines_alloced * 2, sizeof(parse_routine_t));

        memcpy(new_routine_arr, pr->routine_arr, sizeof(parse_routine_t) * pr->n_routines_alloced);
        free(pr->routine_arr);

        pr->routine_arr = new_routine_arr;
        pr->n_routines_alloced *= 2;
    }

    strcpy(pr->routine_arr[pr->n_routines].dbname, db_name);
    strcpy(pr->routine_arr[pr->n_routines].routinename, routine_name);
    pr->routine_arr[pr->n_routines++].routine_type = routine_type;

    return 0;
}

int
query_parse(char* query, parse_result_t* pr)
{
    THD* thd;
    LEX* lex;
    Parser_state parser_state;
    TABLE_LIST* all_tables;
    TABLE_LIST* table;
    ROUTINE_LIST* routine;
    SELECT_LEX *select_lex;
	int exit_code = 0;
	bool err;
	SELECT_LEX *sl;
	sp_head* sp;

    thd = (THD*)pr->thd_org;
    DBUG_ASSERT(pr->n_tables_alloced > 0 && thd);

    pr->n_tables = 0;
    pr->n_routines = 0;
    pr->err_msg[0] = 0;
    pr->query_flags = 0;
    pr->dbname[0] = 0;
    pr->objname[0] = 0;

    if (strlen(query) == 0)
    {
        sprintf(pr->err_msg, "%s", "empty string");
		exit_code = -1;
        return exit_code;
    }

    if (alloc_query(thd, query, strlen(query))) 
    {
        sprintf(pr->err_msg, "%s", "alloc_query error");
		exit_code = -1;
		return exit_code;
    }
    
    if (parser_state.init(thd, thd->query(), thd->query_length()))
    {
        sprintf(pr->err_msg, "%s", "parser_state.init error");
		exit_code = -1;
		return exit_code;
    }

    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);

    err= parse_sql(thd, &parser_state, NULL);
    if (err)
    {
        strmake(pr->err_msg, thd->get_error(), sizeof(pr->err_msg) - 1);
		exit_code = -1;
		goto exit_pos;
    }
    lex = thd->lex;
    /* first SELECT_LEX (have special meaning for many of non-SELECTcommands) */
    select_lex= &lex->select_lex;

    /*
    In many cases first table of main SELECT_LEX have special meaning =>
    check that it is first table in global list and relink it first in 
    queries_tables list if it is necessary (we need such relinking only
    for queries with subqueries in select list, in this case tables of
    subqueries will go to global list first)

    all_tables will differ from first_table only if most upper SELECT_LEX
    do not contain tables.

    Because of above in place where should be at least one table in most
    outer SELECT_LEX we have following check:
    DBUG_ASSERT(first_table == all_tables);
    DBUG_ASSERT(first_table == all_tables && first_table != 0);
    */
    lex->first_lists_tables_same();
    /* should be assigned after making first tables same */
    all_tables= lex->query_tables;
    /* set context for commands which do not use setup_tables */
    select_lex->context.resolve_in_table_list_only(select_lex->
                                       table_list.first);
    pr->query_type = lex->sql_command;
    switch (lex->sql_command)
    {
    case SQLCOM_CHANGE_DB:
        {
            LEX_STRING db_str = { (char *) select_lex->db, strlen(select_lex->db) };

            if (!mysql_change_db(thd, &db_str, FALSE))
                my_ok(thd);
            break;
        }
    case SQLCOM_CREATE_TABLE:
        {
            strncpy(pr->dbname, all_tables->db, sizeof(pr->dbname));
            strncpy(pr->objname, all_tables->table_name, sizeof(pr->objname));

            if (lex->create_info.options & HA_LEX_CREATE_TMP_TABLE)
                pr->query_flags |= QUERY_FLAGS_CREATE_TEMPORARY_TABLE;

            if (lex->alter_info.flags & ALTER_FOREIGN_KEY)
                pr->query_flags |= QUERY_FLAGS_CREATE_FOREIGN_KEY; 
        }
        break;
    case SQLCOM_CREATE_SPFUNCTION:
        {
            uint namelen;
            char* name;
            strncpy(pr->dbname, lex->sphead->m_db.str, min(lex->sphead->m_db.length, sizeof(pr->dbname)));
            name= lex->sphead->name(&namelen);
            strncpy(pr->objname, name, min(namelen, sizeof(pr->dbname)));
        }
        break;

    case SQLCOM_CREATE_INDEX:
    case SQLCOM_ALTER_TABLE:
        {
            strncpy(pr->dbname, all_tables->db, sizeof(pr->dbname));
            strncpy(pr->objname, all_tables->table_name, sizeof(pr->objname));

            if (lex->alter_info.flags | ALTER_FOREIGN_KEY)
                pr->query_flags |= QUERY_FLAGS_CREATE_FOREIGN_KEY; 
        }
        break;

    default:
        break;
    }

    /* add table */
    for (table= all_tables; table; table= table->next_global)
    {
        if (parse_result_add_table(pr, table->db, table->table_name))
        {
			exit_code = -1;
			goto exit_pos;
        }

    }
    sl= lex->all_selects_list;
    for (; sl; sl= sl->next_select_in_list())
    {
        for (table = sl->table_list.first; table; table= table->next_global)
        {
            if (parse_result_add_table(pr, table->db, table->table_name))
            {
				exit_code = -1;
                goto exit_pos;
            }
        }
    }
    sp = thd->lex->sphead;
    if (sp)
    {
        TABLE_LIST* sp_tl = NULL;
        TABLE_LIST** sp_tl_ptr = &sp_tl;
        sp->add_used_tables_to_table_list(thd, &sp_tl_ptr, NULL);

        for (table= sp_tl; table; table= table->next_global)
        {
            if (parse_result_add_table(pr, table->db, table->table_name))
            {
				exit_code = -1;
                goto exit_pos;
            }
        }
    }

    /* add procedure and function */
    for (routine= lex->select_lex.routine_list.first; routine; routine= routine->next_local)
    {
        if (parse_result_add_routine(pr, routine->dbname, routine->routine_name, routine->item == NULL))
        {
            exit_code = -1;
            goto exit_pos;
        }
    }       
    sl= lex->all_selects_list;
    for (; sl; sl= sl->next_select_in_list())
    {
        for (routine= sl->routine_list.first; routine; routine= routine->next_local)
        {
            if (parse_result_add_routine(pr, routine->dbname, routine->routine_name, routine->item == NULL))
            {
                exit_code = -1;
                goto exit_pos;
            }
        }
    }
    sp = thd->lex->sphead;
    if (sp)
    {
        for (routine= sp->m_routine_list.first; routine; routine= routine->next_local)
        {
            if (parse_result_add_routine(pr, routine->dbname, routine->routine_name, routine->item == NULL))
            {
                exit_code = -1;
                goto exit_pos;
            }
        }
    }

exit_pos:

    thd->end_statement();
    thd->cleanup_after_query();
	free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));
    return exit_code;
}

void parse_result_init_db(parse_result_t* pr, char* db)
{
    THD* thd = (THD*)pr->thd_org;
    thd->set_db(db, strlen(db));
}

const char* parse_result_get_stmt_type_str(parse_result_t* pr)
{  
	return get_stmt_type_str(pr->query_type);
}

const char* get_warnings_type_str(int type)
{//  返回告警类型对应的字符串，用于输出
	switch(type)
	{
	case WARNINGS_DEFAULT: return "WARNINGS_DEFAULT";
	case DROP_DB: return "DROP_DB";
	case DROP_TABLE: return "DROP_TABLE";
	case DROP_VIEW: return "DROP_VIEW";
	case DROP_COLUMN: return "DROP_COLUMN";
	case TRUNCATE_TABLE: return "TRUNCATE";
	case DELETE_WITHOUT_WHERE: return "DELETE_WITHOUT_WHERE";
	case UPDATE_WITHOUT_WHERE: return "UPDATE_WITHOUT_WHERE";
	case CREATE_TABLE_WITH_MUCH_BLOB: return "CREATE_TABLE_WITH_MUCH_BLOB";
	case ALTER_TABLE_ADD_MUCH_BLOB: return "ALTER_TABLE_ADD_MUCH_BLOB";
	case CREATE_TABLE_NOT_INNODB: return "CREATE_TABLE_NOT_INNODB";
	case CREATE_TABLE_NO_INDEX: return "CREATE_TABLE_NO_INDEX";
	case CREATE_TABLE_NO_PRIMARYKEY: return "CREATE_TABLE_NO_PRIMARYKEY";
	case ALTER_TABLE_WITH_AFTER: return "ALTER_TABLE_WITH_AFTER";
	case ALTER_TABLE_DEFAULT_WITHOUT_NOT_NULL: return "ALTER_TABLE_DEFAULT_WITHOUT_NOT_NULL";
	case CREATE_TABLE_WITH_OTHER_CHARACTER: return "CREATE_TABLE_WITH_OTHER_CHARACTER";
	case CREATE_PROCEDURE_WITH_DEFINER: return "CREATE_PROCEDURE_WITH_DEFINER";
	case USE_UNKNOWN_SYSTEM_VARIABLE: return "USE_UNKNOWN_SYSTEM_VARIABLE";
	case OTHER_WARNINGS: return "OTHER_WARNINGS";
	default:
		break;
	}
	return "WARNINGS_DEFAULT";

}

const char* get_stmt_type_str(int type)
{
	switch(type)
	{
	case SQLCOM_SELECT: return "STMT_SELECT";
	case SQLCOM_CREATE_TABLE: return "STMT_CREATE_TABLE";
	case SQLCOM_CREATE_INDEX: return "STMT_CREATE_INDEX";
	case SQLCOM_ALTER_TABLE: return "STMT_ALTER_TABLE";
	case SQLCOM_UPDATE: return "STMT_UPDATE";
	case SQLCOM_INSERT: return "STMT_INSERT";
	case SQLCOM_INSERT_SELECT: return "STMT_INSERT_SELECT";
	case SQLCOM_DELETE: return "STMT_DELETE";
	case SQLCOM_TRUNCATE: return "STMT_TRUNCATE";
	case SQLCOM_DROP_TABLE: return "STMT_DROP_TABLE";
	case SQLCOM_DROP_INDEX: return "STMT_DROP_INDEX";
	case SQLCOM_SHOW_DATABASES: return "STMT_SHOW_DATABASES";
	case SQLCOM_SHOW_TABLES: return "STMT_SHOW_TABLES";
	case SQLCOM_SHOW_FIELDS: return "STMT_SHOW_FIELDS";
	case SQLCOM_SHOW_KEYS: return "STMT_SHOW_KEYS";
	case SQLCOM_SHOW_VARIABLES: return "STMT_SHOW_VARIABLES";
	case SQLCOM_SHOW_STATUS: return "STMT_SHOW_STATUS";
	case SQLCOM_SHOW_ENGINE_LOGS: return "STMT_SHOW_ENGINE_LOGS";
	case SQLCOM_SHOW_ENGINE_STATUS: return "STMT_SHOW_ENGINE_STATUS";
	case SQLCOM_SHOW_ENGINE_MUTEX: return "STMT_SHOW_ENGINE_MUTEX";
	case SQLCOM_SHOW_PROCESSLIST: return "STMT_SHOW_PROCESSLIST";
	case SQLCOM_SHOW_MASTER_STAT: return "STMT_SHOW_MASTER_STAT";
	case SQLCOM_SHOW_SLAVE_STAT: return "STMT_SHOW_SLAVE_STAT";
	case SQLCOM_SHOW_GRANTS: return "STMT_SHOW_GRANTS";
	case SQLCOM_SHOW_CREATE: return "STMT_SHOW_CREATE";
	case SQLCOM_SHOW_CHARSETS: return "STMT_SHOW_CHARSETS";
	case SQLCOM_SHOW_COLLATIONS: return "STMT_SHOW_COLLATIONS";
	case SQLCOM_SHOW_CREATE_DB: return "STMT_SHOW_CREATE_DB";
	case SQLCOM_SHOW_TABLE_STATUS: return "STMT_SHOW_TABLE_STATUS";
	case SQLCOM_SHOW_TRIGGERS: return "STMT_SHOW_TRIGGERS";
	case SQLCOM_LOAD: return "STMT_LOAD";
	case SQLCOM_SET_OPTION: return "STMT_SET_OPTION";
	case SQLCOM_LOCK_TABLES: return "STMT_LOCK_TABLES";
	case SQLCOM_UNLOCK_TABLES: return "STMT_UNLOCK_TABLES";
	case SQLCOM_GRANT: return "STMT_GRANT";
	case SQLCOM_CHANGE_DB: return "STMT_CHANGE_DB";
	case SQLCOM_CREATE_DB: return "STMT_CREATE_DB";
	case SQLCOM_DROP_DB: return "STMT_DROP_DB";
	case SQLCOM_ALTER_DB: return "STMT_ALTER_DB";
	case SQLCOM_REPAIR: return "STMT_REPAIR";
	case SQLCOM_REPLACE: return "STMT_REPLACE";
	case SQLCOM_REPLACE_SELECT: return "STMT_REPLACE_SELECT";
	case SQLCOM_CREATE_FUNCTION: return "STMT_CREATE_FUNCTION";
	case SQLCOM_DROP_FUNCTION: return "STMT_DROP_FUNCTION";
	case SQLCOM_REVOKE: return "STMT_REVOKE";
	case SQLCOM_OPTIMIZE: return "STMT_OPTIMIZE";
	case SQLCOM_CHECK: return "STMT_CHECK";
	case SQLCOM_ASSIGN_TO_KEYCACHE: return "STMT_ASSIGN_TO_KEYCACHE";
	case SQLCOM_PRELOAD_KEYS: return "STMT_PRELOAD_KEYS";
	case SQLCOM_FLUSH: return "STMT_FLUSH";
	case SQLCOM_KILL: return "STMT_KILL";
	case SQLCOM_ANALYZE: return "STMT_ANALYZE";
	case SQLCOM_ROLLBACK: return "STMT_ROLLBACK";
	case SQLCOM_ROLLBACK_TO_SAVEPOINT: return "STMT_ROLLBACK_TO_SAVEPOINT";
	case SQLCOM_COMMIT: return "STMT_COMMIT";
	case SQLCOM_SAVEPOINT: return "STMT_SAVEPOINT";
	case SQLCOM_RELEASE_SAVEPOINT: return "STMT_RELEASE_SAVEPOINT";
	case SQLCOM_SLAVE_START: return "STMT_SLAVE_START";
	case SQLCOM_SLAVE_STOP: return "STMT_SLAVE_STOP";
	case SQLCOM_BEGIN: return "STMT_BEGIN";
	case SQLCOM_CHANGE_MASTER: return "STMT_CHANGE_MASTER";
	case SQLCOM_RENAME_TABLE: return "STMT_RENAME_TABLE";
	case SQLCOM_RESET: return "STMT_RESET";
	case SQLCOM_PURGE: return "STMT_PURGE";
	case SQLCOM_PURGE_BEFORE: return "STMT_PURGE_BEFORE";
	case SQLCOM_SHOW_BINLOGS: return "STMT_SHOW_BINLOGS";
	case SQLCOM_SHOW_OPEN_TABLES: return "STMT_SHOW_OPEN_TABLES";
	case SQLCOM_HA_OPEN: return "STMT_HA_OPEN";
	case SQLCOM_HA_CLOSE: return "STMT_HA_CLOSE";
	case SQLCOM_HA_READ: return "STMT_HA_READ";
	case SQLCOM_SHOW_SLAVE_HOSTS: return "STMT_SHOW_SLAVE_HOSTS";
	case SQLCOM_DELETE_MULTI: return "STMT_DELETE_MULTI";
	case SQLCOM_UPDATE_MULTI: return "STMT_UPDATE_MULTI";
	case SQLCOM_SHOW_BINLOG_EVENTS: return "STMT_SHOW_BINLOG_EVENTS";
	case SQLCOM_DO: return "STMT_DO";
	case SQLCOM_SHOW_WARNS: return "STMT_SHOW_WARNS";
	case SQLCOM_EMPTY_QUERY: return "STMT_EMPTY_QUERY";
	case SQLCOM_SHOW_ERRORS: return "STMT_SHOW_ERRORS";
	case SQLCOM_SHOW_STORAGE_ENGINES: return "STMT_SHOW_STORAGE_ENGINES";
	case SQLCOM_SHOW_PRIVILEGES: return "STMT_SHOW_PRIVILEGES";
	case SQLCOM_HELP: return "STMT_HELP";
	case SQLCOM_CREATE_USER: return "STMT_CREATE_USER";
	case SQLCOM_DROP_USER: return "STMT_DROP_USER";
	case SQLCOM_RENAME_USER: return "STMT_RENAME_USER";
	case SQLCOM_REVOKE_ALL: return "STMT_REVOKE_ALL";
	case SQLCOM_CHECKSUM: return "STMT_CHECKSUM";
	case SQLCOM_CREATE_PROCEDURE: return "STMT_CREATE_PROCEDURE";
	case SQLCOM_CREATE_SPFUNCTION: return "STMT_CREATE_SPFUNCTION";
	case SQLCOM_CALL: return "STMT_CALL";
	case SQLCOM_DROP_PROCEDURE: return "STMT_DROP_PROCEDURE";
	case SQLCOM_ALTER_PROCEDURE: return "STMT_ALTER_PROCEDURE";
	case SQLCOM_ALTER_FUNCTION: return "STMT_ALTER_FUNCTION";
	case SQLCOM_SHOW_CREATE_PROC: return "STMT_SHOW_CREATE_PROC";
	case SQLCOM_SHOW_CREATE_FUNC: return "STMT_SHOW_CREATE_FUNC";
	case SQLCOM_SHOW_STATUS_PROC: return "STMT_SHOW_STATUS_PROC";
	case SQLCOM_SHOW_STATUS_FUNC: return "STMT_SHOW_STATUS_FUNC";
	case SQLCOM_PREPARE: return "STMT_PREPARE";
	case SQLCOM_EXECUTE: return "STMT_EXECUTE";
	case SQLCOM_DEALLOCATE_PREPARE: return "STMT_DEALLOCATE_PREPARE";
	case SQLCOM_CREATE_VIEW: return "STMT_CREATE_VIEW";
	case SQLCOM_DROP_VIEW: return "STMT_DROP_VIEW";
	case SQLCOM_CREATE_TRIGGER: return "STMT_CREATE_TRIGGER";
	case SQLCOM_DROP_TRIGGER: return "STMT_DROP_TRIGGER";
	case SQLCOM_XA_START: return "STMT_XA_START";
	case SQLCOM_XA_END: return "STMT_XA_END";
	case SQLCOM_XA_PREPARE: return "STMT_XA_PREPARE";
	case SQLCOM_XA_COMMIT: return "STMT_XA_COMMIT";
	case SQLCOM_XA_ROLLBACK: return "STMT_XA_ROLLBACK";
	case SQLCOM_XA_RECOVER: return "STMT_XA_RECOVER";
	case SQLCOM_SHOW_PROC_CODE: return "STMT_SHOW_PROC_CODE";
	case SQLCOM_SHOW_FUNC_CODE: return "STMT_SHOW_FUNC_CODE";
	case SQLCOM_ALTER_TABLESPACE: return "STMT_ALTER_TABLESPACE";
	case SQLCOM_INSTALL_PLUGIN: return "STMT_INSTALL_PLUGIN";
	case SQLCOM_UNINSTALL_PLUGIN: return "STMT_UNINSTALL_PLUGIN";
	case SQLCOM_SHOW_AUTHORS: return "STMT_SHOW_AUTHORS";
	case SQLCOM_BINLOG_BASE64_EVENT: return "STMT_BINLOG_BASE64_EVENT";
	case SQLCOM_SHOW_PLUGINS: return "STMT_SHOW_PLUGINS";
	case SQLCOM_SHOW_CONTRIBUTORS: return "STMT_SHOW_CONTRIBUTORS";
	case SQLCOM_CREATE_SERVER: return "STMT_CREATE_SERVER";
	case SQLCOM_DROP_SERVER: return "STMT_DROP_SERVER";
	case SQLCOM_ALTER_SERVER: return "STMT_ALTER_SERVER";
	case SQLCOM_CREATE_EVENT: return "STMT_CREATE_EVENT";
	case SQLCOM_ALTER_EVENT: return "STMT_ALTER_EVENT";
	case SQLCOM_DROP_EVENT: return "STMT_DROP_EVENT";
	case SQLCOM_SHOW_CREATE_EVENT: return "STMT_SHOW_CREATE_EVENT";
	case SQLCOM_SHOW_EVENTS: return "STMT_SHOW_EVENTS";
	case SQLCOM_SHOW_CREATE_TRIGGER: return "STMT_SHOW_CREATE_TRIGGER";
	case SQLCOM_ALTER_DB_UPGRADE: return "STMT_ALTER_DB_UPGRADE";
	case SQLCOM_SHOW_PROFILE: return "STMT_SHOW_PROFILE";
	case SQLCOM_SHOW_PROFILES: return "STMT_SHOW_PROFILES";
	case SQLCOM_SIGNAL: return "STMT_SIGNAL";
	case SQLCOM_RESIGNAL: return "STMT_RESIGNAL";
	case SQLCOM_SHOW_RELAYLOG_EVENTS: return "STMT_SHOW_RELAYLOG_EVENTS";
	default:
		break;
	}
	return "";
}


/************************************************************************/
/* add by willhan. 2013-06-13                                                                     */
/************************************************************************/
int parse_result_audit_init(parse_result_audit* pra, char *version, char *charset, bool only_output_ntables, parse_option *option)
{
    THD* thd;
	enum_version current_version;
	if(-1 == set_current_version(version, &current_version))
	{
		fprintf(stderr, "the input version is wrong.\n");
		return -1;
	}
    //for safe 
    parse_global_init();
    thd= new THD;
    thd->thread_id = 0xFFFFFFFF;
    thd->prior_thr_create_utime= thd->start_utime= my_micro_time();
    /*
    The thread stack does not start from this function but we cannot
    guess the real value. So better some value that doesn't assert than
    no value.
    */
    thd->thread_stack= (char*) &thd;
    if (thd->store_globals())
	{
        delete thd;    
		fprintf(stderr, "the thread stack does not start\n");
        return -1;
    }
    thd->security_ctx->skip_grants();
    //thd->set_db(PARSE_RESULT_DEFAULT_DB_NAME, strlen(PARSE_RESULT_DEFAULT_DB_NAME));
    /* init parse_result */
    memset(pra, 0, sizeof(parse_result_audit));
    pra->thd_org = thd;
	/* allocate the table name buffer */
	pra->n_tables_alloced = PARSE_RESULT_N_TABLE_ARR_INITED;
	pra->n_tables = 0;
	pra->line_number = 0;
	pra->info.non_ascii = 0;
	pra->info.is_all_dml = TRUE;
	pra->table_arr = (parse_table_t*)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(parse_table_t));
	pra->mysql_version = current_version;
	pra->only_output_ntables = only_output_ntables;
	cp_parse_option(&sqlparse_option, option);

	if(charset)
	{
		int charset_len = strlen(charset);
		pra->db_charset = (char*)calloc(charset_len+1, sizeof(char));
		memcpy(pra->db_charset, charset, charset_len+1);
	}
	else
		pra->db_charset = charset;
    return 0;
}


int 
query_parse_audit_tsqlparse(
		char *query,
		Parser_state *parser_state,
		parse_result_audit* pra)
{
	THD *thd;
    LEX* lex;
    SELECT_LEX *select_lex;
	TABLE_LIST* all_tables;
	TABLE_LIST* table;
	int exit_code = 0;
	SELECT_LEX *sl;
	bool err;
	sp_head* sp;
	List<Create_field> list_field ;
	List_iterator<Create_field> it_field;


	List<Alter_drop> list_field_drop;
	List_iterator<Create_field> it_field_drop;

	Create_field *cur_field;
	uint blob_text_count;
	char buff[2048];
	String buffer(buff, sizeof(buff), system_charset_info);
	FILE *fp_show_create;




    thd = (THD*)pra->thd_org;
	DBUG_ASSERT(pra->n_tables_alloced > 0 && thd);

    if (strlen(query) == 0)
    {
        sprintf(pra->err_msg, "%s", "empty string");
		pra->result_type = 3;
        exit_code = -1;
		return exit_code;
    }
    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);
    err = parse_sql(thd, parser_state, NULL);
    if (err)
    {
        strmake(pra->err_msg, thd->get_error(), sizeof(pra->err_msg) - 1);
        pra->errcode = thd->get_errcode();
		pra->result_type = process_msg_error(pra->errcode, pra->err_msg);
		exit_code = -1;
		goto exit_pos;
    }
	
    lex = thd->lex;
    /* first SELECT_LEX (have special meaning for many of non-SELECTcommands) */
    select_lex= &lex->select_lex;
	lex->first_lists_tables_same();
	/* should be assigned after making first tables same */
	all_tables= lex->query_tables;
	/* set context for commands which do not use setup_tables */
	select_lex->context.resolve_in_table_list_only(select_lex->table_list.first);
    pra->query_type = lex->sql_command;
	list_field = lex->alter_info.create_list;
	it_field = lex->alter_info.create_list;

	list_field_drop = lex->alter_info.drop_list;


	
	switch(lex->sql_command)
	{// 用于判定pra->info.is_all_dml值，即是否所有语句都为dml语句。  有一条语句非下面几种增删改查，则表示非dml
	case SQLCOM_SELECT:
	case SQLCOM_UPDATE:
	case SQLCOM_INSERT:
	case SQLCOM_DELETE:
	case SQLCOM_INSERT_SELECT:
	case SQLCOM_REPLACE:
	case SQLCOM_REPLACE_SELECT:
		break;
	default:
		pra->info.is_all_dml = FALSE;
		break;
	}

	if(sqlparse_option.is_split_sql)
	{
		char file_create[300];
		char file_alter[300]; 
		char file_dml[300];
		char file_other[300];
		
		strcpy(file_create, sqlparse_option.file_path);
		strcpy(file_alter, sqlparse_option.file_path);
		strcpy(file_dml, sqlparse_option.file_path);
		strcpy(file_other, sqlparse_option.file_path);

		strcat(file_create,"create.sql");
		strcat(file_alter,"alter.sql");
		strcat(file_dml,"dml.sql");
		strcat(file_other,"other.sql");


		FILE *fp_create = fopen(file_create, "a+");
		FILE *fp_alter = fopen(file_alter, "a+");
		FILE *fp_dml = fopen(file_dml, "a+");
		FILE *fp_other = fopen(file_other, "a+");

		switch(lex->sql_command)
		{
		case SQLCOM_SET_OPTION:
		case SQLCOM_CHANGE_DB:
			fprintf(fp_create, "%s;\n",query);
			fprintf(fp_alter, "%s;\n",query);
			fprintf(fp_dml, "%s;\n",query);
		//	fprintf(fp_other, "%s;\n",query);
			break;
		case SQLCOM_DROP_DB:
		case SQLCOM_CREATE_TABLE:
		case SQLCOM_DROP_TABLE:
		case SQLCOM_CREATE_DB:
			fprintf(fp_create, "%s;\n",query);
			break;
		case SQLCOM_CREATE_INDEX:
		case SQLCOM_ALTER_TABLE:
		case SQLCOM_RENAME_TABLE:

		case SQLCOM_DROP_INDEX:
			fprintf(fp_alter, "%s;\n",query);
			break;
		case SQLCOM_CREATE_FUNCTION:
		case SQLCOM_DROP_FUNCTION:
		case SQLCOM_CREATE_PROCEDURE:
//		case SQLCOM_CREATE_SPFUNCTION:
		case SQLCOM_DROP_PROCEDURE:
		case SQLCOM_ALTER_PROCEDURE:
		case SQLCOM_ALTER_FUNCTION:
//		case SQLCOM_CREATE_SERVER:
//		case SQLCOM_DROP_SERVER:
//		case SQLCOM_ALTER_SERVER:
//		case SQLCOM_CREATE_EVENT:
//		case SQLCOM_ALTER_EVENT:
//		case SQLCOM_DROP_EVENT:
//		case SQLCOM_CREATE_VIEW:
//		case SQLCOM_DROP_VIEW:
//		case SQLCOM_CREATE_TRIGGER:
//		case SQLCOM_DROP_TRIGGER:
//		case SQLCOM_CREATE_USER:
//		case SQLCOM_DROP_USER:
//		case SQLCOM_RENAME_USER:
//		case SQLCOM_REVOKE:
//		case SQLCOM_GRANT:
//		case SQLCOM_REVOKE_ALL:

		case SQLCOM_SELECT:
		case SQLCOM_UPDATE:
		case SQLCOM_INSERT:
		case SQLCOM_INSERT_SELECT:
		case SQLCOM_DELETE:
		case SQLCOM_REPLACE:
		case SQLCOM_REPLACE_SELECT:
		case SQLCOM_LOAD:
		case SQLCOM_CALL:
        case SQLCOM_TRUNCATE:
			fprintf(fp_dml, "%s;\n",query);	
			break;
		default:
			fprintf(fp_other, "%s;\n",query);
		}
		fclose(fp_create);
		fclose(fp_alter);
		fclose(fp_dml);
		fclose(fp_other);
		goto exit_pos;
	}

	switch (lex->sql_command)
    {
    case SQLCOM_CHANGE_DB:
        {
            LEX_STRING db_str= { (char *) select_lex->db, strlen(select_lex->db) };
            if (!mysql_change_db(thd, &db_str, FALSE))
                my_ok(thd);
            break;
        }
	case SQLCOM_DROP_DB:
		{
			strcpy(pra->dbname,(lex->name).str);
			pra->tbdb = 1;
			pra->result_type = 1;
			pra->warning_type = DROP_DB;
			break;
		}
	case SQLCOM_DROP_TABLE:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			pra->warning_type = DROP_TABLE;
			break;
		}		
	case SQLCOM_DROP_VIEW:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			pra->warning_type = DROP_VIEW;
			break;
		}
	case SQLCOM_TRUNCATE:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			pra->warning_type = TRUNCATE_TABLE;
			break;
		}
	case SQLCOM_DELETE:
		{
			if(select_lex->where == NULL)
			{
				pra->tbdb = 0;
				pra->result_type = 1;
				pra->warning_type = DELETE_WITHOUT_WHERE;
			}
			break;
		}
	case SQLCOM_UPDATE:
		{
			if(select_lex->where == NULL)
			{
				pra->tbdb = 0;
				pra->result_type = 1;
				pra->warning_type = UPDATE_WITHOUT_WHERE;
			}
			break;
		}
	case SQLCOM_CREATE_TABLE:
		{
			List_iterator<Key> key_iterator(lex->alter_info.key_list);
			Key *key;

			/* 建表语句中，显示指定了非innodb的存储引擎，告警 */
			if(lex->create_info.db_type)
			{
				/* 指针不为空，表示有显示指定存储引擎*/
				if(lex->create_info.db_type->db_type != DB_TYPE_INNODB)
				{
					pra->result_type = 1;
					pra->warning_type = CREATE_TABLE_NOT_INNODB;
					break;
				}
			}

			/* 建表不带主键或者索引告警 */
			key = key_iterator++;
			if(key == NULL)
			{
				/* 没有索引，建表不带索引告警*/
				pra->result_type = 1;
				pra->warning_type = CREATE_TABLE_NO_INDEX;
			}
			while (key)
			{
				/* 建表不带主键 */
				pra->result_type = 1;
				pra->warning_type = CREATE_TABLE_NO_PRIMARYKEY;  // 如果没有主建，则告警为建表不带主键
				if(key->type == Key::PRIMARY)
				{
					pra->result_type = 0;
					pra->warning_type = WARNINGS_DEFAULT;
					break;
				}
				key = key_iterator++;
			}


			/* 建表中，blob字段过多的告警 */
			if(list_field.elements >= 10)
			{
				blob_text_count = 0;
				while(!!(cur_field = it_field++))
				{
					switch(cur_field->sql_type)
					{
					case MYSQL_TYPE_BLOB:
					case MYSQL_TYPE_TINY_BLOB:
					case MYSQL_TYPE_MEDIUM_BLOB:
					case MYSQL_TYPE_LONG_BLOB:
						blob_text_count++;
						break;
					default:
						break;
					}
				}
				if(blob_text_count >= 10)
				{//have more than 10 blob/text field
					pra->result_type = 1;
					pra->warning_type = CREATE_TABLE_WITH_MUCH_BLOB;
					pra->blob_text_count = blob_text_count;
					break;
				}
			}
			{ // 处理字段中的字符集问题，表及每个字段的字符集类型，须与库的字符集一致
			  // 由于库的字符集的通过参数pra->db_charset传进来， 
				if(lex->create_info.default_table_charset && lex->create_info.default_table_charset->csname && pra->db_charset)
				{
					if(strcmp(lex->create_info.default_table_charset->csname, pra->db_charset))
					{// 建表指定的字符集与DB字符集不同，告警
						pra->result_type = 1;
						pra->warning_type = CREATE_TABLE_WITH_OTHER_CHARACTER;
						break;
					}
				}
				
				it_field.rewind();
				while(!!(cur_field = it_field++))
				{// 看字段中有没有指定其它类型的字符集，有则告警
					switch(cur_field->sql_type)
					{
					case MYSQL_TYPE_BLOB:
					case MYSQL_TYPE_TINY_BLOB:
					case MYSQL_TYPE_MEDIUM_BLOB:
					case MYSQL_TYPE_LONG_BLOB:
					case MYSQL_TYPE_VARCHAR:
					case MYSQL_TYPE_VAR_STRING:
					case MYSQL_TYPE_STRING:
					case MYSQL_TYPE_ENUM:
					case MYSQL_TYPE_SET:

						if(cur_field->charset && cur_field->charset->csname && pra->db_charset)
						{
							if(strcmp(cur_field->charset->csname, pra->db_charset) && strcmp(cur_field->charset->csname, "binary"))
							{// 字段中指定了与db不同的字符集，告警
								pra->result_type = 1;
								pra->warning_type = CREATE_TABLE_WITH_OTHER_CHARACTER;
							}
						}
						break;
					default:
						break;
					}

				}
			}
		}
		break;
	case SQLCOM_ALTER_TABLE:
		{
			/* alter table时，add blob字段过多的告警 */
			if(list_field.elements >= 10)
			{
				blob_text_count = 0;
				while(!!(cur_field = it_field++))
				{
					switch(cur_field->sql_type)
					{
					case MYSQL_TYPE_BLOB:
					case MYSQL_TYPE_TINY_BLOB:
					case MYSQL_TYPE_MEDIUM_BLOB:
					case MYSQL_TYPE_LONG_BLOB:
						blob_text_count++;
						break;
					default:
						break;
					}
				}
				if(blob_text_count >= 10)
				{//have more than 10 blob/text field
					pra->result_type = 1;
					pra->warning_type = CREATE_TABLE_WITH_MUCH_BLOB;
					pra->blob_text_count = blob_text_count;
					break;
				}
			}
			if(pra->mysql_version > VERSION_5_5)
			{// tmysql 下，才考虑特定加字段告警

				if(lex->alter_info.flags & ALTER_COLUMN_ORDER)
				{// 使用after来增加字段，则不能使用快速加字段功能，告警
					pra->result_type = 1;
					pra->warning_type = ALTER_TABLE_WITH_AFTER;
					break;
				}
				// 使用alter来增加字段，在指定了default值而未指定NOT NULL选项告警
				it_field.rewind();
				while(!!(cur_field = it_field++))
				{
					if(cur_field->def && !(cur_field->flags & NOT_NULL_FLAG))
					{// 表示有默认值, flags用于标识该字段的一些属性，其中1对应NOT_NULL_FLAG，
						pra->result_type = 1;
						pra->warning_type = ALTER_TABLE_DEFAULT_WITHOUT_NOT_NULL;
					}
				}
			}

			{// alter table t1 drop column c1, 处理drop column导致的告警

				if(list_field_drop.elements > 0)
				{
					pra->result_type = 1;
					pra->warning_type = DROP_COLUMN ;
				}
			}
		}
		break;
	case SQLCOM_CREATE_PROCEDURE:
	case SQLCOM_CREATE_FUNCTION:
	case SQLCOM_ALTER_PROCEDURE:
	case SQLCOM_ALTER_FUNCTION:
	case SQLCOM_CREATE_SPFUNCTION:
		if(lex->definer && &(lex->definer->user) && &(lex->definer->host))
		{// 使用了definer
			if(strcmp(lex->definer->user.str, "ADMIN")==0 && strcmp(lex->definer->host.str, "localhost")==0)
			{
				pra->result_type = 1;
				pra->warning_type = CREATE_PROCEDURE_WITH_DEFINER;
			}
		}
		break;
	case SQLCOM_DROP_INDEX:
	case SQLCOM_DROP_USER:
	case SQLCOM_DROP_TRIGGER:
	case SQLCOM_DROP_SERVER:
	case SQLCOM_DELETE_MULTI:
	case SQLCOM_UPDATE_MULTI:
    default:
		break;
    }

	if (pra->result_type != 0 || 
        (sqlparse_option.is_show_create && sqlparse_option.show_create_file))
	{
		pra->n_tables = 0;
		for(table= all_tables; table; table= table->next_global)
		{
			if (parse_result_add_table_audit(pra, table->db, table->table_name))
			{
				exit_code = -1;
				goto exit_pos;
			}
		}
		sl = lex->all_selects_list;
		for(; sl; sl= sl->next_select_in_list())
		{
			for (table = sl->table_list.first; table; table= table->next_global)
			{
				if (parse_result_add_table_audit(pra, table->db, table->table_name))
				{
					exit_code = -1;
					goto exit_pos;
				}
			}
		}
		sp = thd->lex->sphead;
		if (sp)
		{
			TABLE_LIST* sp_tl = NULL;
			TABLE_LIST** sp_tl_ptr = &sp_tl;
			sp->add_used_tables_to_table_list(thd, &sp_tl_ptr, NULL);
			for (table= sp_tl; table; table= table->next_global)
			{
				if (parse_result_add_table_audit(pra, table->db, table->table_name))
				{
					exit_code = -1;
					goto exit_pos;
				}
			}
		}
	}

    if(sqlparse_option.is_show_create && sqlparse_option.show_create_file)
    {
        char key_name[256], db_name[256], table_name[256], result[256];
        char full_name[1024];
        
        strcpy(result, "SUCCESS");
        
        if(lex->sql_command == SQLCOM_CREATE_TABLE)
        {
            parse_getkey_for_spider(thd, key_name, db_name, table_name, result, sizeof(result));

            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);
			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);

			if(lex->create_info.options & HA_LEX_CREATE_TABLE_LIKE)
			{
				fprintf(fp_show_create,"\t\t<sql_type>STMT_CREATE_TABLE_LIKE</sql_type>\n");
			}
			else if(lex->current_select && lex->current_select->select_n_having_items > 0)
			{// TODO,  这个条件是臆断，待证明正确性
				fprintf(fp_show_create,"\t\t<sql_type>STMT_CREATE_TABLE_SELECT</sql_type>\n");
			}
            else
			{
				fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n", get_stmt_type_str(lex->sql_command));
			}

            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", db_name);
            fprintf(fp_show_create,"\t\t<table_name>%s</table_name>\n", table_name);
            fprintf(fp_show_create,"\t\t<shard_key>%s</shard_key>\n", key_name);
            fprintf(fp_show_create,"\t\t<parse_result>%s</parse_result>\n", result);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else if(lex->sql_command == SQLCOM_CREATE_DB)
        {
            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);
			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);
            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", lex->name);
            fputs("\t\t<table_name></table_name>\n",fp_show_create);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fputs("\t\t<parse_result>SUCCESS</parse_result>\n", fp_show_create);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else if(lex->sql_command == SQLCOM_CHANGE_DB)
        {
            LEX_STRING db_str= { (char *) select_lex->db, strlen(select_lex->db) };
            //if (!mysql_change_db(thd, &db_str, FALSE))
            //    my_ok(thd);

            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);

			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);
            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", db_str.str);
            fputs("\t\t<table_name></table_name>\n",fp_show_create);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fputs("\t\t<parse_result>SUCCESS</parse_result>\n", fp_show_create);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else if(lex->sql_command == SQLCOM_DROP_DB)
        {
            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);

			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);

            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", lex->name);
            fputs("\t\t<table_name></table_name>\n",fp_show_create);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fputs("\t\t<parse_result>SUCCESS</parse_result>\n", fp_show_create);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else if(lex->sql_command == SQLCOM_DROP_TABLE)
        {
            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);

			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);
            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", lex->query_tables->db);
            fprintf(fp_show_create,"\t\t<table_name>%s</table_name>\n", lex->query_tables->table_name);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fputs("\t\t<parse_result>SUCCESS</parse_result>\n", fp_show_create);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else if(lex->sql_command == SQLCOM_ALTER_TABLE)
        {
            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);

			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);

            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fprintf(fp_show_create,"\t\t<db_name>%s</db_name>\n", lex->query_tables->db);
            fprintf(fp_show_create,"\t\t<table_name>%s</table_name>\n", lex->query_tables->table_name);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fprintf(fp_show_create,"\t\t<parse_result>%s</parse_result>\n", result);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }
        else
        {
            fp_show_create = fopen(sqlparse_option.show_create_file, "a+");
            fputs("\t<sql>\n",fp_show_create);

			fputs("\t\t<convert_sql>", fp_show_create);
			print_quoted_xml_for_parse(fp_show_create, query, strlen(query));
			fputs("</convert_sql>\n", fp_show_create);

            fprintf(fp_show_create,"\t\t<sql_type>%s</sql_type>\n",get_stmt_type_str(lex->sql_command));
            fputs("\t\t<db_name></db_name>\n",fp_show_create);

            fputs("\t\t<table_name>",fp_show_create);
            int i = 0;
            for (i=0; i < pra->n_tables; i++)
            {
                snprintf(&full_name[0], sizeof(full_name), "%s.%s", pra->table_arr[i].dbname, pra->table_arr[i].tablename);
                if (i == pra->n_tables - 1)
                {
                    fprintf(fp_show_create, "%s", full_name);
                }
                else
                {
                    fprintf(fp_show_create, "%s,", full_name);
                }
            }

            fputs("</table_name>\n",fp_show_create);
            fputs("\t\t<shard_key></shard_key>\n",fp_show_create);
            fputs("\t\t<parse_result>SUCCESS</parse_result>\n", fp_show_create);

            fputs("\t</sql>\n",fp_show_create);
            fclose(fp_show_create);
        }

        // 还原result_type
        pra->result_type = 0;
    }


exit_pos:
    thd->end_statement();
    thd->cleanup_after_query();
    return exit_code;
}


int query_parse_audit_low(char* query, parse_result_audit* pra)
{
    THD* thd;
    Parser_state parser_state;
	int exit_code = 0;
	char *packet_end;
    thd = (THD*)pra->thd_org;
	DBUG_ASSERT(pra->n_tables_alloced > 0 && thd);
	pra->n_tables = 0;
	pra->errcode = 0;
	pra->result_type = 0;
	pra->err_msg[0] = 0;
    if (strlen(query) == 0)
    {
        sprintf(pra->err_msg, "%s", "empty string");
		pra->result_type = 3;

        exit_code = -1;
		return exit_code;
    }
    if (alloc_query(thd, query, strlen(query))) 
    {
        sprintf(pra->err_msg, "%s", "alloc_query error");
        pra->result_type = 3;

		exit_code = -1;
		return exit_code;
    }
    if (parser_state.init(thd, thd->query(), thd->query_length()))
    {
        sprintf(pra->err_msg, "%s", "parser_state.init error");
		pra->result_type = 3;
		exit_code = -1;
		return exit_code;
    }
	packet_end= thd->query() + thd->query_length();
	thd->client_capabilities|= CLIENT_MULTI_QUERIES;
	exit_code = query_parse_audit_tsqlparse(query, &parser_state, pra);
	while (parser_state.m_lip.found_semicolon != NULL)
	{
		/*
		Multiple queries exits, execute them individually
		*/
		char *beginning_of_next_stmt= (char*) parser_state.m_lip.found_semicolon;
		ulong length= (ulong)(packet_end - beginning_of_next_stmt);
		/* Remove garbage at start of query */
		while (length > 0 && my_isspace(thd->charset(), *beginning_of_next_stmt))
		{
			beginning_of_next_stmt++;
			length--;
		}
		parser_state.reset(beginning_of_next_stmt, length);
		exit_code = query_parse_audit_tsqlparse(beginning_of_next_stmt, &parser_state, pra);
	}
	free_root(thd->mem_root,MYF(MY_KEEP_PREALLOC));
    return exit_code;
}


int query_parse_audit(char* query, parse_result_audit* pra )
{

	/*
	tmysqlparse --version 
	if ()
	query_parse();
	out
	return 0;
	
	*/
	if(pra->only_output_ntables)
	{//  如果tmysqlparse选择只输出SQL中表的个数
		parse_result_t pr;

		/* 初始化parse_result结构 */
		parse_result_init(&pr);


		/* 语法分析 */
		if (query_parse(query, &pr))
		{
			printf("query_parse error at line %ld: %s\n",pra->line_number, pr.err_msg);
			printf("error_sql: %s\n", query);
		}
		else 
		{
			if(pr.n_tables > 0)
				printf("sql: %s; n_tables=%d\n", query, pr.n_tables);
		}
		parse_result_destroy(&pr);
		return 0;
	}

	int exit_code = 0;
	int len = sizeof(reserve_words) / sizeof(reserve_words[0]);
	const char *reserve;
	char tmp_err_msg[PARSE_RESULT_MAX_STR_LEN + 1] = {0};
	int tmp_err_code;
	exit_code = query_parse_audit_low(query, pra);
	//process the error message,
	if(pra->errcode == ER_PARSE_ERROR)
	{
		reserve = has_reserve_in_errmsg(pra->mysql_version, pra->err_msg);
		if(reserve)
		{//have reserve to process
			char *sql1, *sql2, *tmp;

			size_t alloc_size = strlen(query) * 2;
			sql1 = (char*)calloc(alloc_size, sizeof(char));
			sql2 = (char*)calloc(alloc_size, sizeof(char));

			strcpy(sql1,query);
			for(int i=version_pos[pra->mysql_version]; i<len; i++)
			{
				const char* ret;
				reserve = reserve_words[i];
				ret = process_sql_for_reserve(sql1, sql2, alloc_size, reserve);
				assert(ret);
				tmp = sql1;
				sql1 = sql2;
				sql2 = tmp;
			}
			//保留先前的值
			strcpy(tmp_err_msg, pra->err_msg);
			tmp_err_code = pra->errcode;
			if(!query_parse_audit_low(sql1, pra))
			//保留字加上``，重新分析，语法正确
			{
				pra->result_type = 0;
				exit_code = 0;
			}
			else
			{
				pra->result_type = 2;
				strcpy(pra->err_msg, tmp_err_msg);
				pra->errcode = tmp_err_code;
			}
			free(sql1);
			free(sql2);
		}
	}
	else if(pra->errcode == ER_UNKNOWN_SYSTEM_VARIABLE)
	{
		pra->result_type = 1;
		pra->warning_type = USE_UNKNOWN_SYSTEM_VARIABLE;
	}
	return exit_code;
}

int parse_result_add_table_audit(
	parse_result_audit* pra, 
	char*           db_name,
	char*           table_name
)
{
	int i;
	DBUG_ASSERT(db_name && table_name);
	DBUG_ASSERT(pra->n_tables <= pra->n_tables_alloced);
/*	if(0==strcmp(db_name, PARSE_RESULT_DEFAULT_DB_NAME))
	{//No database selected
		strmake(pra->err_msg, "No databases selected", sizeof(pra->err_msg) - 1);
		pra->errcode = 1046;
		pra->result_type = 2;
		return -1;
	}
*/	if (strlen(db_name) > NAME_LEN || strlen(table_name) > NAME_LEN)
	{
		sprintf(pra->err_msg, "%s", "too long db_name or table_name");
		pra->result_type = 3;
		return -1;
	}
	for (i = 0; i < pra->n_tables; i++)
	{
		if (!strcmp(pra->table_arr[i].dbname, db_name) && !strcmp(pra->table_arr[i].tablename, table_name))
			return 0;
	}
	//buffer is not enough
	if (pra->n_tables >= pra->n_tables_alloced)
	{
		parse_table_t* new_table_arr;
		new_table_arr = (parse_table_t*)calloc(pra->n_tables_alloced * 2, sizeof(parse_table_t));
		memcpy(new_table_arr, pra->table_arr, sizeof(parse_table_t) * pra->n_tables_alloced);
		free(pra->table_arr);
		pra->table_arr = new_table_arr;
		pra->n_tables_alloced *= 2;
	}
	strcpy(pra->table_arr[pra->n_tables].dbname, db_name);
	strcpy(pra->table_arr[pra->n_tables++].tablename, table_name);
	return 0;
}

int
parse_result_audit_destroy(parse_result_audit* pra)
{
    THD* thd;

	/* free table name buffer array */
	if (pra->table_arr) {

		free(pra->table_arr);
		pra->table_arr = NULL;
		pra->n_tables_alloced = pra->n_tables = 0;
	}
    thd = (THD*)pra->thd_org;
    if (thd != NULL)
    {
        thd->clear_error();
        /*
        Some extra safety, which should not been needed (normally, event deletion
        should already have done these assignments (each event which sets these
        variables is supposed to set them to 0 before terminating)).
        */
        thd->catalog= 0;
        thd->reset_query();
        if (thd->db)
            my_free(thd->db);
        thd->reset_db(NULL, 0);
        //close_connection(thd);
        delete thd;
        pra->thd_org = NULL;
    }
	free(pra->db_charset);
    return 0;
}


void parse_result_audit_init_db(parse_result_audit* pra, char* db)
{
	THD* thd = (THD*)pra->thd_org;
	thd->set_db(db, strlen(db));
}

static int process_msg_error(int err_code, char *err_msg)
{
	int result_type = 2;//means parse fail
	for(int i=0; ignore_error_code[i] != 0; ++i)
	{
		if(ignore_error_code[i] == err_code)
		{
			result_type = 1;//means ignore this parse error
			break;
		}
	}
	return result_type;
}

static int set_current_version(char *version, enum_version *current_version)
{
	*current_version = VERSION_5_5;
	if(version == NULL)
		return 0;
	else
	{
		if(0 == strcmp(version, "5.0"))
			*current_version = VERSION_5_0;
		else if(0 == strcmp(version, "5.1"))
			*current_version = VERSION_5_1;
		else if(0 == strcmp(version, "5.5"))
			*current_version = VERSION_5_5;
		else if(0 == strcmp(version, "tmysql-1.0"))
			*current_version = VERSION_TMYSQL_1_0;
		else if(0 == strcmp(version, "tmysql-1.1"))
			*current_version = VERSION_TMYSQL_1_1;
		else if(0 == strcmp(version, "tmysql-1.2"))
			*current_version = VERSION_TMYSQL_1_2;
		else if(0 == strcmp(version, "tmysql-1.3"))
			*current_version = VERSION_TMYSQL_1_3;
		else if(0 == strcmp(version, "tmysql-1.4"))
			*current_version = VERSION_TMYSQL_1_4;
		else
			return -1;
	}
	return 0;
}


static const char* has_reserve_in_errmsg(enum_version mysql_version, char *err_msg)
{//return the reserve word
	char *start;
	char tmp[100]={0};
	int len = sizeof(reserve_words) / sizeof(reserve_words[0]);
	int start_pos = version_pos[mysql_version]; 
	int i;
	char substr[] = "your MySQL server version for the right syntax to use near \'";
	if (!(start = strstr(err_msg, substr)))
		return NULL;
	start += strlen(substr);
	/* 拷贝+转化为大写 */
	for(i=0; *(start+i)!='\0' && i<99; ++i)
	{//get the content of '' after near
		if(*(start+i)>='a' && *(start+i)<='z')
			tmp[i] = *(start+i)+'A'-'a';
		else
			tmp[i] = *(start+i);
	}
	tmp[i] = '\0';
	for(i=start_pos; i<len ; ++i)
	{
		if(isprefix_word(tmp, reserve_words[i], 0))
			return reserve_words[i];
	}
	return NULL;//have not reserve
}


static int isprefix_word(const char *s, const char *t, int first_pos)
{//first_pos 表示开始匹配字符的位置, 该位置前一个位置必须满足条件。
	if(first_pos > 0)
	{//前面有字符
		if(!is_word_segmentation(*(s-1)))
			return 0;//not prefix
	}
	while (*t)
	{
		if (*s++ != *t++)
			return 0;//not prefix
	}
	if(*s == '\0')
		return 1;
	else if(is_word_segmentation(*s))
		return 1;
	return 0;
}

static int is_word_segmentation(char ch)
{//if ch is int the word_segmentation[]
	int is_seg = 0;
	int i;
	for(i=0; word_segmentation[i]!='\0'; i++)
	{
		if(ch == word_segmentation[i])
		{
			is_seg = 1;
			break;
		}
	}
	return is_seg;
}

static char* process_sql_for_reserve(char *fromsql, char *tosql, size_t to_len,const char *reserve)
{//process the query, if the word is reserve, add `reserve`
	size_t i,j;
	char in_string = 0;
	char inchar;
	size_t fromsql_len = strlen(fromsql);
	
	for(size_t k=0; k<fromsql_len; k++)
	{//convert fromsql to uplower
		if(fromsql[k]>='a' && fromsql[k]<='z')
			fromsql[k] = fromsql[k]-'a'+'A';
	}

	for(i=j=0; fromsql[i]!='\0' && j < to_len ; i++,j++)
	{
		inchar = fromsql[i];

		//TODO
		if (inchar == '\\' /*&& no_*** */)
		{
			tosql[j] = fromsql[i];
			tosql[++j] = fromsql[++i];

			continue;
		}

		if(!in_string)
		{
			if(inchar=='\'' || inchar=='\"' || inchar=='`')
			{
				in_string = inchar;
				tosql[j] = fromsql[i];
			}
			else
			{
				if(isprefix_word(&fromsql[i],reserve,i))
				{//is prefix
					size_t k;
					tosql[j++] = '`';
					for(k=0;k<strlen(reserve);i++,j++,k++)
						tosql[j] = fromsql[i];
					i--;
					tosql[j] = '`';
				}
				else
					tosql[j] = fromsql[i];
			}
		}
		else
		{
			if(inchar == in_string)
			{
				tosql[j] = fromsql[i];
				in_string = 0;
			}
			else
				tosql[j] = fromsql[i];
		}
	}

	if (j == to_len && fromsql[i]!='\0')
		return NULL;

	return tosql;
}
/***************************************
static int find_reserve_pos(char *reserve)
{//return the pos of reserve in reserve_words
	int pos = -1;
	int i, len = version_pos[VERSION_5_5];
	for(i=0; i<=len; i++)
	{
		if(0 == strcmp(reserve, reserve_words[i]))
			pos = i;
	}
	return pos;
}
*************************************/

int parse_create_info(THD *thd,  String *packet, bool show_database)
{
	List<Item> field_list;
	char tmp[MAX_FIELD_WIDTH],  buff[128], def_value_buf[MAX_FIELD_WIDTH];
	const char *alias;
	String type(tmp, sizeof(tmp), system_charset_info);
	String def_value(def_value_buf, sizeof(def_value_buf), system_charset_info);


	LEX* lex = thd->lex;
	TABLE_LIST* table_list = lex->query_tables;
	List<Create_field> list_field = lex->alter_info.create_list;;
	List_iterator<Create_field> it_field = lex->alter_info.create_list;;

	Create_field *field, *first_field;

	List_iterator<Key> key_iterator(lex->alter_info.key_list);
	Key *key;
	HA_CREATE_INFO create_info = lex->create_info;
	int num_timestap = 0;
	char	*key_name;
	Key_part_spec *column;
	

	packet->append(STRING_WITH_LEN("CREATE TABLE "));
	packet->append(STRING_WITH_LEN("IF NOT EXISTS "));

	alias= table_list->table_name;

	append_identifier(thd, packet, table_list->db, strlen(table_list->db));
	packet->append(STRING_WITH_LEN("."));
	
	append_identifier(thd, packet, alias, strlen(alias));
	packet->append(STRING_WITH_LEN(" (\n"));


	first_field = it_field++ ;
	it_field.rewind();
	while(!!(field = it_field++))
	{
		uint flags = field->flags;

		if(field->sql_type == MYSQL_TYPE_TIMESTAMP)
			num_timestap++;


		if(field != first_field)
			packet->append(STRING_WITH_LEN(",\n"));

		packet->append(STRING_WITH_LEN("  "));
		append_identifier(thd,packet,field->field_name, strlen(field->field_name));
		packet->append(' ');
		// check for surprises from the previous call to Field::sql_type()
		if (type.ptr() != tmp)
			type.set(tmp, sizeof(tmp), system_charset_info);
		else
			type.set_charset(system_charset_info);

		gettype_create_filed(field, type);
		packet->append(type.ptr(), type.length(), system_charset_info);

		if(field->charset && field->charset->csname)
		{
			if(strcmp(field->charset->csname, "binary"))
			{
				if(lex->create_info.default_table_charset)
				{
					if(strcmp(field->charset->csname, lex->create_info.default_table_charset->csname))
					{
						packet->append(STRING_WITH_LEN(" CHARACTER SET "));
						packet->append(field->charset->csname);
					}
				}
				else
				{
					packet->append(STRING_WITH_LEN(" CHARACTER SET "));
					packet->append(field->charset->csname);
				}
			}
		}
		if ((field->charset) && !(field->charset->state & MY_CS_PRIMARY))
		{
			packet->append(STRING_WITH_LEN(" COLLATE "));
			packet->append(field->charset->name);
		}
		if (flags & NOT_NULL_FLAG)
			packet->append(STRING_WITH_LEN(" NOT NULL"));

		if (get_createfield_default_value(field, &def_value))
		{
			packet->append(STRING_WITH_LEN(" DEFAULT "));
			packet->append(def_value.ptr(), def_value.length(), system_charset_info);
		}

		if (field->unireg_check == Field::NEXT_NUMBER && 
			!(thd->variables.sql_mode & MODE_NO_FIELD_OPTIONS))
			packet->append(STRING_WITH_LEN(" AUTO_INCREMENT"));

		if (field->is_compressed() )
		{//add by willhan. 2013-7-23.
			packet->append(STRING_WITH_LEN(" /*!99104 COMPRESSED */"));
		}

		if (field->comment.length)
		{
			packet->append(STRING_WITH_LEN(" COMMENT "));
			append_unescaped(packet, field->comment.str, field->comment.length);
		}
	}
	
		
	while(key = key_iterator++)
	{
		bool found_primary=0;
		List_iterator<Key_part_spec> cols(key->columns);
		packet->append(STRING_WITH_LEN(",\n  "));


		
		for (uint column_nr=0 ; (column=cols++) ; column_nr++)
		{
			/* Create the key name based on the first column (if not given) */
			if (column_nr == 0)
			{//  只考虑非主键，主键的时候key name 为primary key
				if (key->type != Key::PRIMARY && !(key_name= key->name.str))
				//	key_name=make_unique_key_name(sql_field->field_name, *key_info_buffer, key_info);
					key_name = column->field_name.str;
			}
		}


		if (key->type == Key::PRIMARY)
		{
			packet->append(STRING_WITH_LEN("PRIMARY KEY"));
			found_primary = true;
		}
		else if (key->type == Key::UNIQUE)
			packet->append(STRING_WITH_LEN("UNIQUE KEY "));
		else if (key->type == Key::FULLTEXT)
			packet->append(STRING_WITH_LEN("FULLTEXT KEY "));
		else if (key->type == Key::SPATIAL)
			packet->append(STRING_WITH_LEN("SPATIAL KEY "));
		else
			packet->append(STRING_WITH_LEN("KEY "));

		if (!found_primary)
			append_identifier(thd, packet, key_name, strlen(key_name));

		packet->append(STRING_WITH_LEN(" ("));

		cols.rewind();
		for (uint column_nr=0 ; (column=cols++) ; column_nr++)
		{
			if (column_nr)
				packet->append(',');

			if (column->field_name.str)
				append_identifier(thd, packet, column->field_name.str, strlen(column->field_name.str));
			if (column->field_name.str && column->length > 0 && 
				 !(key->type== Key::FULLTEXT || key->type== Key::SPATIAL ))
			{
				char *end;
				buff[0] = '(';
				end= int10_to_str((long) column->length, buff + 1,10);
				*end++ = ')';
				packet->append(buff,(uint) (end-buff));
			}
		}
		packet->append(')');
	}

	packet->append(STRING_WITH_LEN("\n)"));

	/* TABLESPACE and STORAGE */
	if (create_info.tablespace || create_info.storage_media != HA_SM_DEFAULT)
	{
		packet->append(STRING_WITH_LEN(" /*!50100"));
		if (create_info.tablespace)
		{
			packet->append(STRING_WITH_LEN(" TABLESPACE "));
			packet->append(create_info.tablespace, strlen(create_info.tablespace));
		}

		if (create_info.storage_media == HA_SM_DISK)
			packet->append(STRING_WITH_LEN(" STORAGE DISK"));
		if (create_info.storage_media == HA_SM_MEMORY)
			packet->append(STRING_WITH_LEN(" STORAGE MEMORY"));

		packet->append(STRING_WITH_LEN(" */"));
	}

	if (create_info.used_fields & HA_CREATE_USED_ENGINE)
	{
		packet->append(STRING_WITH_LEN(" ENGINE="));
		if(create_info.db_type)
		{// 只考虑指定了innodb与myisam
			switch(create_info.db_type->db_type)
			{
			case DB_TYPE_MISAM:
				packet->append(STRING_WITH_LEN("MyISAM"));
				break;
			case DB_TYPE_INNODB:
				packet->append(STRING_WITH_LEN("InnoDB"));
				break;
			default:
				packet->append(STRING_WITH_LEN("InnoDB"));
				break;
			}
		}
		else
		{
			packet->append(STRING_WITH_LEN("InnoDB"));
		}

	}
	else
	{// 必须指定存储引擎，如果没有create_info信息，则指定innodb
		packet->append(STRING_WITH_LEN(" ENGINE=InnoDB"));
	}

//  create table后面，auto_increment=n这一句当前被忽略掉。
	if (create_info.auto_increment_value > 1)
	{
		char *end;
		packet->append(STRING_WITH_LEN(" AUTO_INCREMENT="));
		end= longlong10_to_str(create_info.auto_increment_value, buff,10);
		packet->append(buff, (uint) (end - buff));
	}

	if (create_info.default_table_charset)
	{// 如果有指定字符集，则使用  否则不指定字符集
		if ((create_info.used_fields & HA_CREATE_USED_DEFAULT_CHARSET))
		{
			packet->append(STRING_WITH_LEN(" DEFAULT CHARSET="));
			packet->append(create_info.default_table_charset->csname);
			if (!(create_info.default_table_charset->state & MY_CS_PRIMARY))
			{
				packet->append(STRING_WITH_LEN(" COLLATE="));
				packet->append(create_info.default_table_charset->name);
			}
		}
	}

	if (create_info.min_rows)
	{
		char *end;
		packet->append(STRING_WITH_LEN(" MIN_ROWS="));
		end= longlong10_to_str(create_info.min_rows, buff, 10);
		packet->append(buff, (uint) (end- buff));
	}

	if (create_info.max_rows && !table_list->schema_table)
	{
		char *end;
		packet->append(STRING_WITH_LEN(" MAX_ROWS="));
		end= longlong10_to_str(create_info.max_rows, buff, 10);
		packet->append(buff, (uint) (end - buff));
	}

	if (create_info.avg_row_length)
	{
		char *end;
		packet->append(STRING_WITH_LEN(" AVG_ROW_LENGTH="));
		end= longlong10_to_str(create_info.avg_row_length, buff,10);
		packet->append(buff, (uint) (end - buff));
	}
/************** database相关的一些option忽略掉 ******************************
	if (share->db_create_options & HA_OPTION_PACK_KEYS)
		packet->append(STRING_WITH_LEN(" PACK_KEYS=1"));
	if (share->db_create_options & HA_OPTION_NO_PACK_KEYS)
		packet->append(STRING_WITH_LEN(" PACK_KEYS=0"));
	if (share->db_create_options & HA_OPTION_CHECKSUM)
		packet->append(STRING_WITH_LEN(" CHECKSUM=1"));
	if (share->db_create_options & HA_OPTION_DELAY_KEY_WRITE)
		packet->append(STRING_WITH_LEN(" DELAY_KEY_WRITE=1"));
*************************************************************************/
	if (create_info.row_type != ROW_TYPE_DEFAULT)
	{
		if(create_info.row_type == ROW_TYPE_GCS ){
			char tversion[12];        
			sprintf(tversion,"%u",TMYSQL_VERSION_START_ID);

			packet->append(STRING_WITH_LEN(" /*!"));
			packet->append(tversion ,5);
			packet->append(STRING_WITH_LEN(" ROW_FORMAT="));

			packet->append(ha_row_type[(uint) create_info.row_type]);
			
			packet->append(STRING_WITH_LEN(" */ "));
		}else{
			packet->append(STRING_WITH_LEN(" ROW_FORMAT="));
			packet->append(ha_row_type[(uint) create_info.row_type]);
		}
	}
	if (create_info.key_block_size)
	{
		char *end;
		packet->append(STRING_WITH_LEN(" KEY_BLOCK_SIZE="));
		end= longlong10_to_str(create_info.key_block_size, buff, 10);
		packet->append(buff, (uint) (end - buff));
	}
	if (create_info.comment.length)
	{
		packet->append(STRING_WITH_LEN(" COMMENT="));
		append_unescaped(packet, create_info.comment.str, create_info.comment.length);
	}
	if (create_info.connect_string.length)
	{
		packet->append(STRING_WITH_LEN(" CONNECTION="));
		append_unescaped(packet, create_info.connect_string.str, create_info.connect_string.length);
	}
	parse_append_directory(thd, packet, "DATA",  create_info.data_file_name);
	parse_append_directory(thd, packet, "INDEX", create_info.index_file_name);
	return 0;
}



void gettype_create_filed(Create_field *cr_field, String &res)
{
	CHARSET_INFO *cs=res.charset();
	int field_length = cr_field->length;
	ulong length;
	bool unsigned_flag = cr_field->flags & UNSIGNED_FLAG;
	bool zerofill_flag = cr_field->flags & ZEROFILL_FLAG;
	uint tmp=field_length;

	switch(cr_field->sql_type)
	{
	case MYSQL_TYPE_DECIMAL:
		tmp=cr_field->length;
		if (!unsigned_flag)
			tmp--;
		if (cr_field->decimals)
			tmp--;
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
			"decimal(%d,%d)", tmp, cr_field->decimals));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_TINY:
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
			"tinyint(%d)",(int) field_length));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_SHORT:
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
			"smallint(%d)",(int) field_length));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_INT24:
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(), 
			"mediumint(%d)",(int) field_length));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_LONG:
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
			"int(%d)", field_length));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_FLOAT:
		if (cr_field->decimals == NOT_FIXED_DEC)
		{
			res.set_ascii(STRING_WITH_LEN("float"));
		}
		else
		{
			CHARSET_INFO *cs= res.charset();
			res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
				"float(%ld,%d)", cr_field->length, cr_field->decimals));
		}
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_DOUBLE:
		if (cr_field->decimals == NOT_FIXED_DEC)
		{
			res.set_ascii(STRING_WITH_LEN("double"));
		}
		else
		{
			res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
				"double(%ld,%d)", cr_field->length, cr_field->decimals));
		}
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
	case MYSQL_TYPE_NULL:
		res.set_ascii(STRING_WITH_LEN("null"));
		break;
	case MYSQL_TYPE_TIMESTAMP:
		res.set_ascii(STRING_WITH_LEN("timestamp"));
		break;
	case MYSQL_TYPE_LONGLONG:
		res.length(cs->cset->snprintf(cs,(char*) res.ptr(),res.alloced_length(),
			"bigint(%d)",(int) field_length));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);

	case MYSQL_TYPE_DATE:
		res.set_ascii(STRING_WITH_LEN("date"));
		break;
	case MYSQL_TYPE_TIME:
		res.set_ascii(STRING_WITH_LEN("time"));
		break;
	case MYSQL_TYPE_DATETIME:
		res.set_ascii(STRING_WITH_LEN("datetime"));
		break;
	case MYSQL_TYPE_YEAR:
		res.length(cs->cset->snprintf(cs,(char*)res.ptr(),res.alloced_length(),
			"year(%d)",(int) field_length));
		break;
	case MYSQL_TYPE_NEWDATE:
		res.set_ascii(STRING_WITH_LEN("date"));
		break;
	case MYSQL_TYPE_BIT:
		length= cs->cset->snprintf(cs, (char*) res.ptr(), res.alloced_length(),
			"bit(%d)", (int) field_length);
		res.length((uint) length);
		break;
	case MYSQL_TYPE_NEWDECIMAL:
		res.length(cs->cset->snprintf(cs, (char*) res.ptr(), res.alloced_length(),
			"decimal(%ld,%d)", cr_field->length - (cr_field->decimals>0 ? 1:0) - (unsigned_flag || !cr_field->length ? 0:1), 
			cr_field->decimals));
		filed_add_zerofill_and_unsigned(res, unsigned_flag, zerofill_flag);
		break;
		/*
	case MYSQL_TYPE_ENUM:
		char buffer[255];
		String enum_item(buffer, sizeof(buffer), res.charset());

		res.length(0);
		res.append(STRING_WITH_LEN("enum("));

		bool flag=0;
		uint *len= typelib->type_lengths;
		for (const char **pos= typelib->type_names; *pos; pos++, len++)
		{
			uint dummy_errors;
			if (flag)
				res.append(',');
			enum_item.copy(*pos, *len, charset(), res.charset(), &dummy_errors);
			append_unescaped(&res, enum_item.ptr(), enum_item.length());
			flag= 1;
		}
		res.append(')');
		break;
	case MYSQL_TYPE_SET:
		char buffer_tmp[255];
		String set_item(buffer_tmp, sizeof(buffer_tmp), res.charset());

		res.length(0);
		res.append(STRING_WITH_LEN("set("));

		bool flag=0;
		uint *len= typelib->type_lengths;
		for (const char **pos= typelib->type_names; *pos; pos++, len++)
		{
			uint dummy_errors;
			if (flag)
				res.append(',');
			set_item.copy(*pos, *len, charset(), res.charset(), &dummy_errors);
			append_unescaped(&res, set_item.ptr(), set_item.length());
			flag= 1;
		}
		res.append(')');
		break;
	case MYSQL_TYPE_GEOMETRY:
		CHARSET_INFO *cs= &my_charset_latin1;
		switch (geom_type)
		{
		case GEOM_POINT:
			res.set(STRING_WITH_LEN("point"), cs);
			break;
		case GEOM_LINESTRING:
			res.set(STRING_WITH_LEN("linestring"), cs);
			break;
		case GEOM_POLYGON:
			res.set(STRING_WITH_LEN("polygon"), cs);
			break;
		case GEOM_MULTIPOINT:
			res.set(STRING_WITH_LEN("multipoint"), cs);
			break;
		case GEOM_MULTILINESTRING:
			res.set(STRING_WITH_LEN("multilinestring"), cs);
			break;
		case GEOM_MULTIPOLYGON:
			res.set(STRING_WITH_LEN("multipolygon"), cs);
			break;
		case GEOM_GEOMETRYCOLLECTION:
			res.set(STRING_WITH_LEN("geometrycollection"), cs);
			break;
		default:
			res.set(STRING_WITH_LEN("geometry"), cs);
		}
		break;
*/	
		
	case MYSQL_TYPE_TINY_BLOB:
	case MYSQL_TYPE_MEDIUM_BLOB:
	case MYSQL_TYPE_LONG_BLOB:
	case MYSQL_TYPE_BLOB:
		const char *str;
		uint length;
		switch (cr_field->sql_type) 
		{
		case MYSQL_TYPE_TINY_BLOB:
			str="tiny"; length=4; break;
		case MYSQL_TYPE_BLOB:
			str="";     length=0; break;
		case MYSQL_TYPE_MEDIUM_BLOB:
			str="medium"; length= 6; break;
		case MYSQL_TYPE_LONG_BLOB:
			str="long";  length=4; break;
		default:
			break;
		}
		res.set_ascii(str,length);
		if (cr_field->charset == &my_charset_bin)
			res.append(STRING_WITH_LEN("blob"));
		else
		{
			res.append(STRING_WITH_LEN("text"));
		}
		break;
	case MYSQL_TYPE_VARCHAR:
	case MYSQL_TYPE_VAR_STRING:
		
		if(cr_field->charset)
		{
/* TODO,   这处的mbmaxlen是什么含义 ？！ varchar(10)的时候，mbmaxlen为3，为什么 ？
			length= cs->cset->snprintf(cs,(char*) res.ptr(), res.alloced_length(), "%s(%d)",
				((cr_field->charset && cr_field->charset->csname) ? "varchar" : "varbinary"),
				(int) field_length / cr_field->charset->mbmaxlen);
*/
			length= cs->cset->snprintf(cs,(char*) res.ptr(), res.alloced_length(), "%s(%d)",
				(strcmp(cr_field->charset->csname, "binary")==0 ?  "varbinary":"varchar"),
				(int) field_length);
		}
		else
		{
			length= cs->cset->snprintf(cs,(char*) res.ptr(), res.alloced_length(), "%s(%d)",
				("varchar"), (int) field_length);

		}
		res.length(length);
		break;
	case MYSQL_TYPE_STRING:
		if(cr_field->charset)
		{
			length= cs->cset->snprintf(cs,(char*) res.ptr(),
				res.alloced_length(), "%s(%d)",
				(strcmp(cr_field->charset->csname, "binary")==0 ? "binary":"char"), (int) field_length);
		}
		else
		{
			length= cs->cset->snprintf(cs,(char*) res.ptr(),
				res.alloced_length(), "%s(%d)", "char", (int) field_length);
		}
		res.length(length);
		break;
	default:
		break;
	}
}


void filed_add_zerofill_and_unsigned(String &res, bool unsigned_flag, bool zerofill)
{
	if (unsigned_flag)
		res.append(STRING_WITH_LEN(" unsigned"));
	if (zerofill)
		res.append(STRING_WITH_LEN(" zerofill"));
}



bool get_createfield_default_value(Create_field *cr_field, String *def_value)
{
  bool has_default;
  def_value->length(0);

  if(cr_field->def)
  {// 指定了def值
	  def_value->append(STRING_WITH_LEN(cr_field->def->name)); 
	  has_default = true;
  }
  else
  {// 没有指定def值
	  has_default = false;
  }
  return has_default;
}


static void parse_append_directory(THD *thd, String *packet, const char *dir_type, const char *filename)
{
	if (filename && !(thd->variables.sql_mode & MODE_NO_DIR_IN_CREATE))
	{
		uint length= dirname_length(filename);
		packet->append(' ');
		packet->append(dir_type);
		packet->append(STRING_WITH_LEN(" DIRECTORY='"));
#ifdef __WIN__
		/* Convert \ to / to be able to create table on unix */
		char *winfilename= (char*) thd->memdup(filename, length);
		char *pos, *end;
		for (pos= winfilename, end= pos+length ; pos < end ; pos++)
		{
			if (*pos == '\\')
				*pos = '/';
		}
		filename= winfilename;
#endif
		packet->append(filename, length);
		packet->append('\'');
	}
}


int parse_getkey_for_spider(THD *thd,  char *key_name, char *db_name, char *table_name, char *result, int result_len)
{
	LEX* lex = thd->lex;
	TABLE_LIST* table_list = lex->query_tables;

	List_iterator<Key> key_iterator(lex->alter_info.key_list);
	Key *key;
	Key_part_spec *column;
	int level = 0;  // 普通key的第一字段，level为1   unique key的第一个字段，level为2   primary key的第一个字段，level=3


	strcpy(db_name, table_list->db);
	strcpy(table_name, table_list->table_name);
    strcpy(result, "SUCCESS");
	
	while(key = key_iterator++)
	{
		bool found_primary=0;
		List_iterator<Key_part_spec> cols(key->columns);
		column = cols++;
		
        switch (key->type) {
            case Key::PRIMARY:
            case Key::UNIQUE:
                {
                    if (level > 1)
                    {
                        strcpy(key_name, "");
                        snprintf(result, result_len, "%s", "ERROR: too more unique key");
                        return 1;
                    }

			        strcpy(key_name, column->field_name.str);
                    level = ((key->type == Key::PRIMARY) ? 3 : 2);
                    break;
                }

            case Key::MULTIPLE:
                {
                    if (level < 1)
                    { 
			            strcpy(key_name, column->field_name.str);
                        level = 1; 
                    }
                    break;
                }

            case Key::FOREIGN_KEY:
            case Key::FULLTEXT:
            case Key::SPATIAL:
            default:
                {
                    strcpy(key_name, "");
                    snprintf(result, result_len, "%s", "ERROR: no support key type");
                    return 1;
                }
        }
	}

    // 如果只有key，并且key个数大于1，报错
    if (level == 1 && lex->alter_info.key_list.elements > 1)
    {
        //strcpy(key_name, "");  // key_name为第一个key
        snprintf(result, result_len, "%s", "ERROR: too many key more than 1, but without unique key");
        return 1;
    }
    
    // 必须包含索引
	if(level > 0)
		return 0;
	
    strcpy(key_name, "");
    snprintf(result, result_len, "%s", "ERROR: no key");
	return 1;
}



static void print_quoted_xml_for_parse(FILE *xml_file, const char *str, ulong len)
{
	const char *end;
	for (end= str + len; str != end; str++)
	{
		switch (*str) 
		{
		case '<':
			fputs("&lt;", xml_file);
			break;
		case '>':
			fputs("&gt;", xml_file);
			break;
		case '&':
			fputs("&amp;", xml_file);
			break;
		case '\"':
			fputs("&quot;", xml_file);
			break;
		default:
			fputc(*str, xml_file);
			break;
		}
	}
}