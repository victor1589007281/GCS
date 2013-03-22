/*
    

*/

#ifndef SQLPARSE_H
#define SQLPARSE_H

#ifdef __cplusplus
extern "C" {
#endif

/* copy from mysql_com.h */
#define HOSTNAME_LENGTH 60
#define SYSTEM_CHARSET_MBMAXLEN 3
#define NAME_CHAR_LEN	64              /* Field/table name length */
#define USERNAME_CHAR_LENGTH 16
#define NAME_LEN                (NAME_CHAR_LEN*SYSTEM_CHARSET_MBMAXLEN)
#define USERNAME_LENGTH         (USERNAME_CHAR_LENGTH*SYSTEM_CHARSET_MBMAXLEN)

struct parse_table_struct {
    char dbname[NAME_LEN];
    char tablename[NAME_LEN];
};
typedef struct parse_table_struct parse_table_t;

#define PARSE_RESULT_MAX_STR_LEN 512
struct parse_result_struct {
    void* thd_org;

    int query_type;

    short n_tables_alloced;
    short n_tables;
    parse_table_t*  table_arr;
    //char** table_arr;       /* table name array, each is <dbname>.<tablename> */

    char err_msg[PARSE_RESULT_MAX_STR_LEN + 1];
};

typedef struct parse_result_struct parse_result_t;

int
parse_result_init(parse_result_t* pr);

int
parse_result_destroy(parse_result_t* pr);

int
query_parse(char* query, parse_result_t* pr);

void
parse_global_init();

void
parse_global_destroy();

#ifdef __cplusplus
}
#endif

#endif
