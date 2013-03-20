/*
    

*/

#ifndef SQLPARSE_H
#define SQLPARSE_H

#ifdef __cplusplus
extern "C" {
#endif

struct parse_result_struct {
    void* thd_org;

    int query_type;

    short n_tables_alloced;
    short n_tables;
    char** table_arr;       /* table name array, each is <dbname>.<tablename> */
};

typedef struct parse_result_struct parse_result_t;

int
parse_result_init(parse_result_t* pr);

int
parse_result_destroy(parse_result_t* pr);

int
query_parse(char* query, parse_result_t* pr);

#ifdef __cplusplus
}
#endif

#endif
