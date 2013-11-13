#ifndef _TMYSQLBINLOGEX_H
#define _TMYSQLBINLOGEX_H

#include "mysqld_error.h"

Exit_status binlogex_process_event(Log_event *ev,
                                   my_off_t pos, const char *logname);

int
safe_execute_sql(MYSQL *mysql, const char *query, ulong length);

int
binlogex_init();

void
binlogex_destroy();

void
binlogex_create_worker_thread();

void
binlogex_wait_all_worker_thread_exit();

int
binlogex_split_full_table_name(
    const char*       full_tabname,
    char*             dbname_out,
    uint              dbname_len,
    char*             tabname_out,
    uint              tabname_len
);

void
binlogex_routine_entry_add_table(
    char*           routine_db,
    char*           routine_name,
    bool            is_proc,
    char*           table_db,
    char*           table_name   
);

struct table_entry_struct 
{
    char full_table_name[NAME_LEN * 2 + 3];      // �����ǵ�һ����Ա
    char db[NAME_LEN];
    char table[NAME_LEN];
    uint thread_id;
    ulonglong rate;                         //binlog query_event rate
    bool    is_temp;      /* ���ڱ�ʾ�ñ��Ƿ���ʱ�������̺߳Ų��ܸı䣬ֻ����TEMP_TABLE_THREAD_ID */
};

#define TEMP_TABLE_THREAD_ID 0

typedef struct table_entry_struct table_entry_t;

struct routine_entry_struct
{
    char full_routine_name[NAME_LEN*2 + 10];
    char db[NAME_LEN];
    char routine[NAME_LEN];
    int  routine_type;

    unsigned short n_table_arr_alloced;
    unsigned short n_tables;

    table_entry_t*  table_arr;
};

typedef struct routine_entry_struct routine_entry_t;

routine_entry_t*
binlogex_routine_entry_get_by_name(
    char*           routine_db,
    char*           routine_name,
    int             routine_type
);

class Worker_vm
{
public:
    uint                    thread_id;
    PRINT_EVENT_INFO        print_info;
    FILE*                   result_file;
    FILE*                   tmp_file;
    DYNAMIC_STRING          dnstr;
    Load_log_processor      load_processor;
    uint16                  binlog_version;
    uint                    delimiter_len;
    MYSQL                   mysql;

    ulong                   normal_entry_cnt;
    ulong                   complex_entry_cnt; /* ִ�п�������� */
    ulong                   sync_entry_cnt;
    ulong                   sleep_cnt;
    ulong                   sync_wait_time;
    ulong                   sync_signal_time;

    Worker_vm(uint tid) : thread_id(tid), result_file(0), tmp_file(0),
                          normal_entry_cnt(0), complex_entry_cnt(0), sync_entry_cnt(0), sleep_cnt(0), sync_wait_time(0), sync_signal_time(0) {}
    ~Worker_vm() {}

};

int
binlogex_execute_sql(
    Worker_vm*  vm,
    char*       sql,
    uint        len
);

int
binlogex_get_count_by_sql(
    MYSQL*          mysql,
    const char*     sql,
    uint*           count
);

#include "sqlparse.h"

extern parse_result_t  parse_result;
extern my_bool parse_result_inited;
extern uint global_tables_pairs_num;

void
binlogex_add_to_hash_tab(
    const char*       dbname,
    const char*       tblname,
    uint              id_merge    /* �������hash��ʹ�����id_merge; ���򣬽���hash���и�id������ֵת����id_merge */
);

void
binlogex_adjust_hash_table_thread_id();

void
binlogex_print_all_tables_in_hash();

void
binlogex_print_all_routines_in_hash();

extern ulong           mysql_version;

#define ERR_PARSE_ERROR -1
#define ERR_PF_NOT_EXIST -2

int
binglogex_query_parse(
    char*               db,    
    char*               query,
    parse_result_t*     pr,
    bool                check_query_type
);

#endif