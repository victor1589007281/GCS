#include "sqlparse.h"

int main(int argc, char **argv)
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
