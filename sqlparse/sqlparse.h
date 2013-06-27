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

enum enum_versio {VERSION_5_0, VERSION_5_1, VERSION_5_5};

struct parse_table_struct {
    char dbname[NAME_LEN];
    char tablename[NAME_LEN];
};
typedef struct parse_table_struct parse_table_t;

#define PARSE_RESULT_MAX_STR_LEN 512
struct parse_result_struct {
    void* thd_org;

    int query_type;

    unsigned short n_tables_alloced;
    unsigned short n_tables;
    parse_table_t*  table_arr;
    //char** table_arr;       /* table name array, each is <dbname>.<tablename> */

    char err_msg[PARSE_RESULT_MAX_STR_LEN + 1];
};
typedef struct parse_result_struct parse_result_t;


/************************************************************************/
/* add by willhan. 2013-06-13                                                                     */
/************************************************************************/
enum enum_result_types {SQLPARSE_SUCESS, SQLPARSE_WARNING, SQLPARSE_FAIL, SQLPARSE_FAIL_OTHER};
struct parse_result_audit {
	void* thd_org;
	int query_type;
	int result_type;
	/**0 success; 1 risking warning; 2 parse fail; 3 other fail***/

	int tbdb;
	/*tbdb==0 rising warnings from table; tbdb==1 risking warnings from dababase */
	char dbname[NAME_LEN];
	/** database name **/
	int blob_text_count;
	/*records the number of blob/text fields when create/alter table*/
	
	unsigned short n_tables_alloced;
	unsigned short n_tables;
	parse_table_t*  table_arr;
	//char** table_arr;       /* table name array, each is <dbname>.<tablename> */
	enum_versio mysql_version;

	int errcode;
	char err_msg[PARSE_RESULT_MAX_STR_LEN + 1];
};

int
parse_result_init(parse_result_t* pr);

int
parse_result_destroy(parse_result_t* pr);

void
parse_result_init_db(
    parse_result_t* pr,
    char*           db
);

int
query_parse(char* query, parse_result_t* pr);

const char*
parse_result_get_stmt_type_str(
    parse_result_t*    pr
);

void
parse_global_init();

void
parse_global_destroy();




/************************************************************************/
/* add by willhan. 2013-06-13                                                                     */
/************************************************************************/
int parse_result_audit_init(parse_result_audit* pr, char *version);
int query_parse_audit(char *query, parse_result_audit* pra);
int parse_result_audit_destroy(parse_result_audit* pra);
int parse_result_add_table_audit(parse_result_audit* pra, char* db_name, char* table_name);
const char* get_stmt_type_str(int type);
void parse_result_audit_init_db(parse_result_audit* pra, char* db);

#ifdef __cplusplus
}
#endif

#endif
