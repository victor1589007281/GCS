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
    Load_log_processor      load_processor;
    uint16                  binlog_version;

    Worker_vm(uint tid) : thread_id(tid){}
    ~Worker_vm() {}

};

#endif