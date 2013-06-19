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



void my_init_for_sqlparse();
void my_end_for_sqlparse();

static my_pthread_once_t sqlparse_global_inited = MY_PTHREAD_ONCE_INIT;

void
parse_global_init()
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

    thd = (THD*)pr->thd_org;
    DBUG_ASSERT(pr->n_tables_alloced > 0 && thd);

    pr->n_tables = 0;

    if (strlen(query) == 0)
    {
        sprintf(pr->err_msg, "%s", "empty string");
        return -1;
    }

    if (alloc_query(thd, query, strlen(query))) 
    {
        sprintf(pr->err_msg, "%s", "alloc_query error");
        return -1;
    }
    
    if (parser_state.init(thd, thd->query(), thd->query_length()))
    {
        sprintf(pr->err_msg, "%s", "parser_state.init error");
        return -1;
    }

    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);

    bool err= parse_sql(thd, &parser_state, NULL);
    if (err)
    {
        strmake(pr->err_msg, thd->get_error(), sizeof(pr->err_msg) - 1);
        return -1;
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
            LEX_STRING db_str= { (char *) select_lex->db, strlen(select_lex->db) };

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
            return -1;
        }

    }

    SELECT_LEX *sl= lex->all_selects_list;
    for (; sl; sl= sl->next_select_in_list())
    {
        for (table = sl->table_list.first; table; table= table->next_global)
        {
            if (parse_result_add_table(pr, table->db, table->table_name))
            {
                return -1;
            }
        }
    }

    sp_head* sp = thd->lex->sphead;
    if (sp)
    {
        TABLE_LIST* sp_tl = NULL;
        TABLE_LIST** sp_tl_ptr = &sp_tl;
        sp->add_used_tables_to_table_list(thd, &sp_tl_ptr, NULL);

        for (table= sp_tl; table; table= table->next_global)
        {
            if (parse_result_add_table(pr, table->db, table->table_name))
            {
                return -1;
            }
        }
    }
    

    thd->end_statement();
    thd->cleanup_after_query();

    return 0;
}

void
parse_result_init_db(
    parse_result_t* pr,
    char*           db
)
{
    THD* thd = (THD*)pr->thd_org;

    thd->set_db(db, strlen(db));
}

const char*
parse_result_get_stmt_type_str(
    parse_result_t*    pr
)
{
    switch(pr->query_type)
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

int
parse_result_audit_init(parse_result_audit* pra)
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
    memset(pra, 0, sizeof(parse_result_audit));
    pra->thd_org = thd;

	/* allocate the table name buffer */
	pra->n_tables_alloced = PARSE_RESULT_N_TABLE_ARR_INITED;
	pra->n_tables = 0;
	pra->table_arr = (parse_table_t*)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(parse_table_t));

    return 0;
}

int
query_parse_audit(char* query, parse_result_audit* pra)
{
    THD* thd;
    LEX* lex;
    Parser_state parser_state;
    SELECT_LEX *select_lex;
	TABLE_LIST* all_tables;
	TABLE_LIST* table;
	int exit_code = 0;

    thd = (THD*)pra->thd_org;
	DBUG_ASSERT(pra->n_tables_alloced > 0 && thd);

	pra->n_tables = 0;


    if (strlen(query) == 0)
    {
        sprintf(pra->err_msg, "%s", "empty string");
		pra->result_type = 3;

        exit_code = -1;
		goto exit_pos;
    }

    if (alloc_query(thd, query, strlen(query))) 
    {
        sprintf(pra->err_msg, "%s", "alloc_query error");
        pra->result_type = 3;

		exit_code = -1;
		goto exit_pos;
    }
    
    if (parser_state.init(thd, thd->query(), thd->query_length()))
    {
        sprintf(pra->err_msg, "%s", "parser_state.init error");
		pra->result_type = 3;
        
		exit_code = -1;
		goto exit_pos;
    }

    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);

    bool err= parse_sql(thd, &parser_state, NULL);
    if (err)
    {
        strmake(pra->err_msg, thd->get_error(), sizeof(pra->err_msg) - 1);
        pra->errcode = thd->get_errcode();
		pra->result_type = 2;
		
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

    switch (lex->sql_command)
    {
    case SQLCOM_CHANGE_DB:
        {
            LEX_STRING db_str= { (char *) select_lex->db, strlen(select_lex->db) };
            if (!mysql_change_db(thd, &db_str, FALSE))
                my_ok(thd);
			pra->result_type = 0;
            break;
        }
	case SQLCOM_DROP_DB:
		{
			strcpy(pra->name,(lex->name).str);
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
			else
				pra->result_type = 0;
			break;
		}
	case SQLCOM_UPDATE:
		{
			if(select_lex->where == NULL)
			{
				pra->tbdb = 0;
				pra->result_type = 1;
			}
			else
				pra->result_type = 0;
			break;
		}
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
		{
			pra->result_type = 0;
			break;
		}
    }
   
	for (table= all_tables; table; table= table->next_global)
	{
		if (parse_result_add_table_audit(pra, table->db, table->table_name))
		{
			exit_code = -1;
			goto exit_pos;
		}

	}

	SELECT_LEX *sl= lex->all_selects_list;
	for (; sl; sl= sl->next_select_in_list())
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

	sp_head* sp = thd->lex->sphead;
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

exit_pos:

    thd->end_statement();
    thd->cleanup_after_query();
    return exit_code;
}


int
parse_result_add_table_audit(
					   parse_result_audit* pra, 
					   char*           db_name,
					   char*           table_name
					   )
{
	int i;
	DBUG_ASSERT(db_name && table_name);
	DBUG_ASSERT(pra->n_tables <= pra->n_tables_alloced);

	if (strlen(db_name) > NAME_LEN || strlen(table_name) > NAME_LEN)
	{
		sprintf(pra->err_msg, "%s", "too long db_name or table_name");
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