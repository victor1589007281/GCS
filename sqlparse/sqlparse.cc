#define MYSQL_LEX 1
#include "my_global.h"
#include "unireg.h"                    // REQUIRED: for other includes
#include "sql_parse.h"        // sql_kill, *_precheck, *_prepare
#include "sql_db.h"           // mysql_change_db, mysql_create_db,

#include "sqlparse.h"

extern int parse_export;
#define PARSE_RESULT_N_TABLE_ARR_INITED 5

int init_thread_environment();
int mysql_init_variables();

extern mysql_mutex_t LOCK_plugin;
extern Time_zone * my_tz_SYSTEM;

int
parse_result_init(parse_result_t* pr)
{
    THD* thd;
    int i;

    memset(pr, sizeof(parse_result_t), 0);

    /* set global parse export */
    parse_export = 1;
    my_progname = "test";

    if (init_thread_environment() ||
        mysql_init_variables() )
        return -1;

    //mysql_mutex_init(key_LOCK_plugin, &LOCK_plugin, MY_MUTEX_INIT_FAST);
    //mysql_mutex_init(NULL, &LOCK_plugin, MY_MUTEX_INIT_FAST);
    /*if (plugin_init(&peuso_argc, NULL, 0))
    {
        sql_print_error("Failed to initialize plugins.");
        unireg_abort(1);
    }*/
    //plugins_are_initialized= TRUE;  /* Don't separate from init function */

    my_thread_global_init();
    my_thread_init();

    /* need in THD() */
    randominit(&sql_rand,(ulong) 1234,(ulong) 1234/2);

    get_charset_number("utf8", MY_CS_PRIMARY);

    mysql_mutex_init(-1, &LOCK_plugin, MY_MUTEX_INIT_FAST);

    global_system_variables.time_zone= my_tz_SYSTEM;
    global_system_variables.lc_messages= my_default_lc_messages;
    global_system_variables.collation_server=	 default_charset_info;
    global_system_variables.collation_database=	 default_charset_info;
    global_system_variables.collation_connection=  default_charset_info;
    global_system_variables.character_set_results= default_charset_info;
    global_system_variables.character_set_client=  default_charset_info;
    global_system_variables.character_set_filesystem= character_set_filesystem;
    global_system_variables.lc_time_names= my_default_lc_time_names;

    error_handler_hook= my_message_sql;
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
    pr->thd_org = thd;

    /* allocate the table name buffer */
    pr->n_tables_alloced = PARSE_RESULT_N_TABLE_ARR_INITED;
    pr->n_tables = 0;
    pr->table_arr = (char**)calloc(PARSE_RESULT_N_TABLE_ARR_INITED, sizeof(char*));
    for(i = 0; i < pr->n_tables_alloced; ++i)
    {
        pr->table_arr[i]=(char*)calloc(NAME_LEN*2 + 2, sizeof(char));
    }

    return 0;
}

int
parse_result_destroy(parse_result_t* pr)
{
    THD* thd;
    int i; 

    /* free table name buffer array */
    if (!pr->table_arr) {

        for(i = 0; i < pr->n_tables_alloced; ++i)
            free(pr->table_arr[i]);

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
        thd->reset_db(NULL, 0);

        //close_connection(thd);

        delete thd;

        pr->thd_org = NULL;
    }

    //clean_up_mutexes();
    
    my_thread_end();
    my_thread_global_end();

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
        return -1;

    //buffer is not enough
    if (pr->n_tables >= pr->n_tables_alloced)
    {
        char** new_table_arr;
        int i;

        new_table_arr = (char**)calloc(pr->n_tables_alloced * 2, sizeof(char*));

        for (i = 0; i < pr->n_tables_alloced; ++i)
            new_table_arr[i] = pr->table_arr[i]; 

        for (; i < pr->n_tables_alloced * 2; ++i)
            new_table_arr[i] = (char*)calloc(NAME_LEN*2 + 2, sizeof(char)) ; 

        free(pr->table_arr);
        pr->table_arr = new_table_arr;
        pr->n_tables_alloced *= 2;
    }

    sprintf(pr->table_arr[pr->n_tables++],"%s.%s", db_name, table_name);

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

    if (alloc_query(thd, query, strlen(query)))
        return -1;
    
    if (parser_state.init(thd, thd->query(), thd->query_length()))
        return -1;

    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);

    bool err= parse_sql(thd, &parser_state, NULL);
    if (err)
        return -1;

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
    pr->n_tables = 0;

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
            return -2;
        }

    }

    return 0;
}

int maini(int argc, char **argv)
{
    parse_result_t pr;
    char* query = "select * from t1";
    char* query1 = "use db";

    parse_result_init(&pr);

    query_parse(query1, &pr);
    query_parse(query, &pr);

    parse_result_destroy(&pr);

    return 0;
}
