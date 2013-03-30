#define MYSQL_LEX 1
#include "my_global.h"
#include "unireg.h"                    // REQUIRED: for other includes
#include "sql_parse.h"        // sql_kill, *_precheck, *_prepare
#include "sql_db.h"           // mysql_change_db, mysql_create_db,

#include "sqlparse.h"

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

    //_CrtSetBreakAlloc(76);
    //_CrtSetBreakAlloc(84);
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
    DBUG_ASSERT(db_name && table_name);
    DBUG_ASSERT(pr->n_tables <= pr->n_tables_alloced);

    if (strlen(db_name) > NAME_LEN || strlen(table_name) > NAME_LEN)
    {
        sprintf(pr->err_msg, "%s", "too long db_name or table_name");
        return -1;
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

    thd->end_statement();
    thd->cleanup_after_query();

    return 0;
}

