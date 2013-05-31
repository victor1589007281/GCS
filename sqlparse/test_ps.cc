#include "sqlparse.h"
#include "my_global.h"
#include "my_sys.h"
#include <stdio.h>
#include <string.h>

//#include "mysql.cc"

#define MAX_BUF_SIZE (1024+1)

int main(int argc, char **argv)
{
    parse_result_t pr;
    char buf[1024];
    int i;

    /* 初始化全局数据 */
    parse_global_init();

    /* 初始化parse_result结构 */
    parse_result_init(&pr);

    while(1)
    {
        fprintf(stdout, "please input a query:\n");
        if (fgets(buf, MAX_BUF_SIZE, stdin) == NULL)
        {
            fprintf(stderr, "fgets error\n");
            break;
        }

        if (buf[strlen(buf) -1] == '\n')
            buf[strlen(buf) - 1] = '\0';

        if (strcmp(buf, "exit") == 0)
            break;

        /* 语法分析 */
        if (query_parse(buf, &pr))
        {
            printf("query_parse error: %s\n", pr.err_msg);
        }
        else 
        {
            printf("%s :\n", parse_result_get_stmt_type_str(&pr));
            for ( i = 0; i < pr.n_tables; ++i)
            {
                printf("dbname:%s, tablename:%s\n", pr.table_arr[i].dbname, pr.table_arr[i].tablename);
            }
            printf("\n");
        }
    }
    parse_result_destroy(&pr);

    parse_global_destroy();

    return 0;
}

