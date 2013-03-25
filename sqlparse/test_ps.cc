#include "sqlparse.h"
#include "my_global.h"
#include "my_sys.h"
#include <stdio.h>

//#include "mysql.cc"

int main(int argc, char **argv)
{
    parse_result_t pr;
    char* query = "select * from t1, t2 where t1.c1 = t2.c1";
    char* query1 = "select * form t1, t2 where t1.c1 = t2.c1";
    char* query2 = "use db";
    int i;

    /* 初始化全局数据 */
    parse_global_init();

    /* 初始化parse_result结构 */
    parse_result_init(&pr);

    /* 语法分析 */
    if (query_parse(query1, &pr))
    {
        printf("query_parse error: %s\n", pr.err_msg);
    }
    for ( i = 0; i < pr.n_tables; ++i)
    {
        printf("dbname:%s, tablename:%s\n", pr.table_arr[i].dbname, pr.table_arr[i].tablename);
    }

    if (query_parse(query, &pr))
    {
        printf("query_parse error: %s", pr.err_msg);
    }
    for ( i = 0; i < pr.n_tables; ++i)
    {
        printf("dbname:%s, tablename:%s\n", pr.table_arr[i].dbname, pr.table_arr[i].tablename);
    }

    parse_result_destroy(&pr);

//////////////////////////
    parse_result_init(&pr);

    query_parse(query1, &pr);
    query_parse(query, &pr);

    parse_result_destroy(&pr);

    parse_global_destroy();

    return 0;
}




