#define MYSQL_LEX 1
#include "my_global.h"
#include "unireg.h"                    // REQUIRED: for other includes
#include "sql_parse.h"        // sql_kill, *_precheck, *_prepare
#include "sql_db.h"           // mysql_change_db, mysql_create_db,
#include "sqlparse.h"
#include "sp_head.h"

extern int parse_export;
#define PARSE_RESULT_N_TABLE_ARR_INITED 5
#define PARSE_RESULT_DEFAULT_DB_NAME "<unknow_db>"

static int ignore_error_code[] = {ER_UNKNOWN_SYSTEM_VARIABLE,0};//0 means the end


static int version_pos[] = {0, 6, 13};
static  char word_segmentation[] = {' ' , '\t', '\n', '\r', ',', '.', '(', ')', '<', '>', '=', '!', '\0'};


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
static int set_current_version(char *version, enum_versio *current_version);
static const char* has_reserve_in_errmsg(enum_versio mysql_version, char *err_msg);
static int isprefix_word(const char *s, const char *t, int first_pos);
static int is_word_segmentation(char ch);
static char* process_sql_for_reserve(char *fromsql, char *tosql, size_t to_len,const char *reserve);
//static int find_reserve_pos(char *reserve);


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

    if (strlen(db_name) > NAME_LEN || strlen(table_name) > NAME_LEN)
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
query_parse(char* query, parse_result_t* pr)
{
    THD* thd;
    LEX* lex;
    Parser_state parser_state;
    TABLE_LIST* all_tables;
    TABLE_LIST* table;
    SELECT_LEX *select_lex;
	int exit_code = 0;
	bool err;
	SELECT_LEX *sl;
	sp_head* sp;
	

    thd = (THD*)pr->thd_org;
    DBUG_ASSERT(pr->n_tables_alloced > 0 && thd);

    pr->n_tables = 0;
    pr->err_msg[0] = 0;

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
    default:
        break;
    }
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
int parse_result_audit_init(parse_result_audit* pra, char *version)
{
    THD* thd;
	enum_versio current_version;
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
	pra->info.no_ascii = 1;
	pra->table_arr = (parse_table_t*)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(parse_table_t));
	pra->mysql_version = current_version;
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
	Create_field *cur_field;
	uint blob_text_count;

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
			break;
		}
	case SQLCOM_DROP_TABLE:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			break;
		}		
	case SQLCOM_DROP_VIEW:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			break;
		}
	case SQLCOM_TRUNCATE:
		{
			pra->tbdb = 0;
			pra->result_type = 1;
			break;
		}
	case SQLCOM_DELETE:
		{
			if(select_lex->where == NULL)
			{
				pra->tbdb = 0;
				pra->result_type = 1;
			}
			break;
		}
	case SQLCOM_UPDATE:
		{
			if(select_lex->where == NULL)
			{
				pra->tbdb = 0;
				pra->result_type = 1;
			}
			break;
		}
	case SQLCOM_CREATE_TABLE:
	case SQLCOM_ALTER_TABLE:
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
					pra->blob_text_count = blob_text_count;
				}
			}
			break;
	case SQLCOM_DROP_INDEX:
	case SQLCOM_DROP_FUNCTION:
	case SQLCOM_DROP_USER:
	case SQLCOM_DROP_PROCEDURE:
	case SQLCOM_DROP_TRIGGER:
	case SQLCOM_DROP_SERVER:
	case SQLCOM_DROP_EVENT:
	case SQLCOM_DELETE_MULTI:
	case SQLCOM_UPDATE_MULTI:
    default:
			break;
    }

	if (pra->result_type != 0)
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
			result_type = 0;//means ignore this parse error
			break;
		}
	}
	return result_type;
}

static int set_current_version(char *version, enum_versio *current_version)
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
		else
			return -1;
	}
	return 0;
}


static const char* has_reserve_in_errmsg(enum_versio mysql_version, char *err_msg)
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