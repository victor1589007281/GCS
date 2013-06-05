#ifndef _TMYSQLBINLOGEX_H
#define _TMYSQLBINLOGEX_H

Exit_status binlogex_process_event(Log_event *ev,
                                   my_off_t pos, const char *logname);

int
binlogex_init();

void
binlogex_destroy();

void
binlogex_wait_all_worker_thread_exit();

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
    ulong                   complex_entry_cnt; /* 执行跨表语句个数 */
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

#endif