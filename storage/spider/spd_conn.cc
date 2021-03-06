/* Copyright (C) 2008-2013 Kentoku Shiba

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#define MYSQL_SERVER 1
#include "mysql_version.h"
#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "sql_priv.h"
#include "probes_mysql.h"
#include "sql_class.h"
#include "sql_partition.h"
#include "tztime.h"
#endif
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_trx.h"
#include "spd_conn.h"
#include "spd_table.h"
#include "spd_direct_sql.h"
#include "spd_ping_table.h"
#include "spd_malloc.h"
#include "spd_err.h"

extern ulong *spd_db_att_thread_id;

extern handlerton *spider_hton_ptr;
extern SPIDER_DBTON spider_dbton[SPIDER_DBTON_SIZE];
pthread_mutex_t spider_conn_id_mutex;
pthread_mutex_t spider_ipport_count_mutex;
pthread_mutex_t spider_conn_i_mutexs[SPIDER_MAX_PARTITION_NUM];
pthread_cond_t  spider_conn_i_conds[SPIDER_MAX_PARTITION_NUM];
ulonglong spider_conn_id = 1;
long spider_conn_mutex_id = 0;

#ifndef WITHOUT_SPIDER_BG_SEARCH
extern pthread_attr_t spider_pt_attr;

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key spd_key_mutex_mta_conn;
#ifndef WITHOUT_SPIDER_BG_SEARCH
extern PSI_mutex_key spd_key_mutex_bg_conn_chain;
extern PSI_mutex_key spd_key_mutex_bg_conn_sync;
extern PSI_mutex_key spd_key_mutex_bg_conn;
extern PSI_mutex_key spd_key_mutex_bg_job_stack;
extern PSI_mutex_key spd_key_mutex_bg_mon;
extern PSI_cond_key spd_key_cond_bg_conn_sync;
extern PSI_cond_key spd_key_cond_bg_conn;
extern PSI_cond_key spd_key_cond_bg_sts;
extern PSI_cond_key spd_key_cond_bg_sts_sync;
extern PSI_cond_key spd_key_cond_bg_crd;
extern PSI_cond_key spd_key_cond_bg_crd_sync;
extern PSI_cond_key spd_key_cond_bg_mon;
extern PSI_thread_key spd_key_thd_bg;
extern PSI_thread_key spd_key_thd_bg_sts;
extern PSI_thread_key spd_key_thd_bg_crd;
extern PSI_thread_key spd_key_thd_bg_mon;
#endif
#endif

extern pthread_mutex_t spider_global_trx_mutex;
extern SPIDER_TRX *spider_global_trx;
#endif

HASH spider_open_connections;
HASH spider_ipport_conns;
uint spider_open_connections_id;
const char *spider_open_connections_func_name;
const char *spider_open_connections_file_name;
ulong spider_open_connections_line_no;
pthread_mutex_t spider_conn_mutex;
/* harryczhang: mutex for spider_conn_meta_hash */
pthread_mutex_t spider_conn_meta_mutex;
HASH spider_conn_meta_info;


#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
HASH spider_hs_r_conn_hash;
uint spider_hs_r_conn_hash_id;
const char *spider_hs_r_conn_hash_func_name;
const char *spider_hs_r_conn_hash_file_name;
ulong spider_hs_r_conn_hash_line_no;
pthread_mutex_t spider_hs_r_conn_mutex;
HASH spider_hs_w_conn_hash;
uint spider_hs_w_conn_hash_id;
const char *spider_hs_w_conn_hash_func_name;
const char *spider_hs_w_conn_hash_file_name;
ulong spider_hs_w_conn_hash_line_no;
pthread_mutex_t spider_hs_w_conn_mutex;
#endif

/* harryczhang: */
extern PSI_thread_key spd_key_thd_conn_rcyc;
volatile bool      conn_rcyc_init = FALSE;
pthread_t          conn_rcyc_thread;
typedef struct {
    HASH *hash_info;
    DYNAMIC_STRING_ARRAY *arr_info[2];
} delegate_param;

/* for spider_open_connections and trx_conn_hash */
uchar *spider_conn_get_key(
  SPIDER_CONN *conn,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_conn_get_key");
  *length = conn->conn_key_length;
#ifndef DBUG_OFF
  spider_print_keys(conn->conn_key, conn->conn_key_length);
#endif
  DBUG_RETURN((uchar*) conn->conn_key);
}

uchar *spider_conn_meta_get_key(
  SPIDER_CONN_META_INFO *meta,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
  ) {
  DBUG_ENTER("spider_conn_meta_get_key");
  *length = meta->key_len;
#ifndef DBUG_OFF
  spider_print_keys(meta->key, meta->key_len);
#endif
  DBUG_RETURN((uchar*) meta->key);
}

uchar *spider_ipport_conn_get_key(
								  SPIDER_IP_PORT_CONN *ip_port,
								  size_t *length,
								  my_bool not_used __attribute__ ((unused))
								  )
{
	DBUG_ENTER("spider_ipport_conn_get_key");
	*length = ip_port->key_len;
	DBUG_RETURN((uchar*) ip_port->key);
}

int spider_reset_conn_setted_parameter(
  SPIDER_CONN *conn,
  THD *thd
) {
  DBUG_ENTER("spider_reset_conn_setted_parameter");
  conn->autocommit = spider_param_remote_autocommit();
  conn->sql_log_off = spider_param_remote_sql_log_off();
  if (thd && spider_param_remote_time_zone())
  {
    int tz_length = strlen(spider_param_remote_time_zone());
    String tz_str(spider_param_remote_time_zone(), tz_length,
      &my_charset_latin1);
    conn->time_zone = my_tz_find(thd, &tz_str);
  } else
    conn->time_zone = NULL;
  conn->trx_isolation = spider_param_remote_trx_isolation();
  DBUG_PRINT("info",("spider conn->trx_isolation=%d", conn->trx_isolation));
  if (spider_param_remote_access_charset())
  {
    if (!(conn->access_charset =
      get_charset_by_csname(spider_param_remote_access_charset(),
        MY_CS_PRIMARY, MYF(MY_WME))))
      DBUG_RETURN(ER_UNKNOWN_CHARACTER_SET);
  } else
    conn->access_charset = NULL;
  char *default_database = spider_param_remote_default_database();
  if (default_database)
  {
    uint default_database_length = strlen(default_database);
    if (conn->default_database.reserve(default_database_length + 1))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    conn->default_database.q_append(default_database,
      default_database_length + 1);
    conn->default_database.length(default_database_length);
  } else
    conn->default_database.length(0);
  DBUG_RETURN(0);
}

int spider_free_conn_alloc(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn_alloc");
  spider_db_disconnect(conn);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  spider_free_conn_thread(conn);
#endif
  if (conn->db_conn)
  {
    delete conn->db_conn;
    conn->db_conn = NULL;
  }
  DBUG_ASSERT(!conn->mta_conn_mutex_file_pos.file_name);
  pthread_mutex_destroy(&conn->mta_conn_mutex);
  conn->default_database.free();
  DBUG_RETURN(0);
}

void spider_free_conn_from_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  bool another,
  bool trx_free,
  int *roop_count
) {
  ha_spider *spider;
	SPIDER_IP_PORT_CONN *ip_port_conn = NULL;
  DBUG_ENTER("spider_free_conn_from_trx");
  spider_conn_clear_queue(conn);
  conn->use_for_active_standby = FALSE;
  conn->error_mode = 1;
  conn->ignore_dup_key = FALSE;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (
      trx_free ||
      (
        (
          conn->server_lost ||
          spider_param_conn_recycle_mode(trx->thd) != 2         /* 从会话连接池中取出 */
        ) &&
        !conn->opened_handlers
      )
    ) {
      conn->thd = NULL;
      if (another)
      {
        ha_spider *next_spider;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        my_hash_delete_with_hash_value(&trx->trx_another_conn_hash,
          conn->conn_key_hash_value, (uchar*) conn);
#else
        my_hash_delete(&trx->trx_another_conn_hash, (uchar*) conn);
#endif
        spider = (ha_spider*) conn->another_ha_first;
        while (spider)
        {
          next_spider = spider->next;
          spider_free_tmp_dbton_handler(spider);
          spider_free_tmp_dbton_share(spider->share);
          spider_free_tmp_share_alloc(spider->share);
          spider_free(spider_current_trx, spider->share, MYF(0));
          delete spider;
          spider = next_spider;
        }
        conn->another_ha_first = NULL;
        conn->another_ha_last = NULL;
      } else {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        my_hash_delete_with_hash_value(&trx->trx_conn_hash,
          conn->conn_key_hash_value, (uchar*) conn);
#else
        my_hash_delete(&trx->trx_conn_hash, (uchar*) conn);
#endif
      }

      if (
        !trx_free &&
        !conn->server_lost &&
       /* !conn->queued_connect &&*/ /* 连接未成功建立，也不释放 */
        spider_param_conn_recycle_mode(trx->thd) == 1                   /* 加入全局连接对象 */
      ) {
        /* conn_recycle_mode == 1 */
        *conn->conn_key = '0';
        if (
          conn->quick_target &&
          spider_db_free_result((ha_spider *) conn->quick_target, FALSE)
        ) {
          spider_free_conn(conn);
        } else {
          pthread_mutex_lock(&spider_conn_mutex);
          uint old_elements = spider_open_connections.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
          if (my_hash_insert_with_hash_value(&spider_open_connections,
            conn->conn_key_hash_value, (uchar*) conn))
#else
          if (my_hash_insert(&spider_open_connections, (uchar*) conn))
#endif
          {
            pthread_mutex_unlock(&spider_conn_mutex);
            spider_free_conn(conn);
          } else {
						long mutex_num=0;
						if(ip_port_conn = (SPIDER_IP_PORT_CONN*) my_hash_search_using_hash_value(&spider_ipport_conns, conn->conn_key_hash_value, (uchar*) conn->conn_key, conn->conn_key_length))
						{/* 如果存在 */
							if(opt_spider_max_connections)
							{
								mutex_num = ip_port_conn->conn_mutex_num;
								pthread_mutex_lock(&spider_conn_i_mutexs[mutex_num]);
								pthread_cond_signal(&spider_conn_i_conds[mutex_num]);
								pthread_mutex_unlock(&spider_conn_i_mutexs[mutex_num]);
							}
						}

            if (spider_open_connections.array.max_element > old_elements)
            {
              spider_alloc_calc_mem(spider_current_trx,
                spider_open_connections,
                (spider_open_connections.array.max_element - old_elements) *
                spider_open_connections.array.size_of_element);
            }
			/************************************************************************
             Create conn_meta whose status is updated then when CONN object is pushed
             into spider_open_connections
            ************************************************************************/
						/*
								TODO,
								conn对象的获取非常频率，尤其在高并发下。 为此尽量减少锁中的行为
						*/
            if (!spider_add_conn_meta_info(conn)) {
                spider_my_err_logging("[ERROR] spider_add_conn_meta_info failed for conn within conn_id=[%ull]!\n", conn->conn_id);
            }
            pthread_mutex_unlock(&spider_conn_mutex);
          }
        }
      } else {                                                      /* 释放 */
        /* conn_recycle_mode == 0 */
        spider_free_conn(conn);
      }
    } else if (roop_count)
      (*roop_count)++;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else if (conn->conn_kind == SPIDER_CONN_KIND_HS_READ)
  {
    spider_db_hs_request_buf_reset(conn);
    if (
      trx_free ||
      (
        (
          conn->server_lost ||
          spider_param_hs_r_conn_recycle_mode(trx->thd) != 2
        ) &&
        !conn->opened_handlers
      )
    ) {
      conn->thd = NULL;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      my_hash_delete_with_hash_value(&trx->trx_hs_r_conn_hash,
        conn->conn_key_hash_value, (uchar*) conn);
#else
      my_hash_delete(&trx->trx_hs_r_conn_hash, (uchar*) conn);
#endif

      DBUG_ASSERT(conn->opened_handlers ==
        conn->db_conn->get_opened_handler_count());
      if (conn->db_conn->get_opened_handler_count())
      {
        conn->db_conn->reset_opened_handler();
      }

      if (
        !trx_free &&
        !conn->server_lost &&
        !conn->queued_connect &&
        spider_param_hs_r_conn_recycle_mode(trx->thd) == 1
      ) {
        /* conn_recycle_mode == 1 */
        *conn->conn_key = '0';
        pthread_mutex_lock(&spider_hs_r_conn_mutex);
        uint old_elements = spider_hs_r_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        if (my_hash_insert_with_hash_value(&spider_hs_r_conn_hash,
          conn->conn_key_hash_value, (uchar*) conn))
#else
        if (my_hash_insert(&spider_hs_r_conn_hash, (uchar*) conn))
#endif
        {
          pthread_mutex_unlock(&spider_hs_r_conn_mutex);
          spider_free_conn(conn);
        } else {
          if (spider_hs_r_conn_hash.array.max_element > old_elements)
          {
            spider_alloc_calc_mem(spider_current_trx,
              spider_hs_r_conn_hash,
              (spider_hs_r_conn_hash.array.max_element - old_elements) *
              spider_hs_r_conn_hash.array.size_of_element);
          }
          pthread_mutex_unlock(&spider_hs_r_conn_mutex);
        }
      } else {
        /* conn_recycle_mode == 0 */
        spider_free_conn(conn);
      }
    } else if (roop_count)
      (*roop_count)++;
  } else {
    spider_db_hs_request_buf_reset(conn);
    if (
      trx_free ||
      (
        (
          conn->server_lost ||
          spider_param_hs_w_conn_recycle_mode(trx->thd) != 2
        ) &&
        !conn->opened_handlers
      )
    ) {
      conn->thd = NULL;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      my_hash_delete_with_hash_value(&trx->trx_hs_w_conn_hash,
        conn->conn_key_hash_value, (uchar*) conn);
#else
      my_hash_delete(&trx->trx_hs_w_conn_hash, (uchar*) conn);
#endif

      DBUG_ASSERT(conn->opened_handlers ==
        conn->db_conn->get_opened_handler_count());
      if (conn->db_conn->get_opened_handler_count())
      {
        conn->db_conn->reset_opened_handler();
      }

      if (
        !trx_free &&
        !conn->server_lost &&
        !conn->queued_connect &&
        spider_param_hs_w_conn_recycle_mode(trx->thd) == 1
      ) {
        /* conn_recycle_mode == 1 */
        *conn->conn_key = '0';
        pthread_mutex_lock(&spider_hs_w_conn_mutex);
        uint old_elements = spider_hs_w_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        if (my_hash_insert_with_hash_value(&spider_hs_w_conn_hash,
          conn->conn_key_hash_value, (uchar*) conn))
#else
        if (my_hash_insert(&spider_hs_w_conn_hash, (uchar*) conn))
#endif
        {
          pthread_mutex_unlock(&spider_hs_w_conn_mutex);
          spider_free_conn(conn);
        } else {
          if (spider_hs_w_conn_hash.array.max_element > old_elements)
          {
            spider_alloc_calc_mem(spider_current_trx,
              spider_hs_w_conn_hash,
              (spider_hs_w_conn_hash.array.max_element - old_elements) *
              spider_hs_w_conn_hash.array.size_of_element);
          }
          pthread_mutex_unlock(&spider_hs_w_conn_mutex);
        }
      } else {
        /* conn_recycle_mode == 0 */
        spider_free_conn(conn);
      }
    } else if (roop_count)
      (*roop_count)++;
  }
#endif
  DBUG_VOID_RETURN;
}

SPIDER_CONN *spider_create_conn(
  SPIDER_SHARE *share,
  ha_spider *spider,
  int link_idx,
  int base_link_idx,
  uint conn_kind,
  int *error_num
) {
  int *need_mon;
  SPIDER_CONN *conn;
  SPIDER_IP_PORT_CONN *ip_port_conn;
  char *tmp_name, *tmp_host, *tmp_username, *tmp_password, *tmp_socket;
  char *tmp_wrapper, *tmp_ssl_ca, *tmp_ssl_capath, *tmp_ssl_cert;
  char *tmp_ssl_cipher, *tmp_ssl_key, *tmp_default_file, *tmp_default_group;
  DBUG_ENTER("spider_create_conn");

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (!(conn = (SPIDER_CONN *)
      spider_bulk_malloc(spider_current_trx, 18, MYF(MY_WME | MY_ZEROFILL),
        &conn, sizeof(*conn),
        &tmp_name, share->conn_keys_lengths[link_idx] + 1,
        &tmp_host, share->tgt_hosts_lengths[link_idx] + 1,
        &tmp_username,
          share->tgt_usernames_lengths[link_idx] + 1,
        &tmp_password,
          share->tgt_passwords_lengths[link_idx] + 1,
        &tmp_socket, share->tgt_sockets_lengths[link_idx] + 1,
        &tmp_wrapper,
          share->tgt_wrappers_lengths[link_idx] + 1,
        &tmp_ssl_ca, share->tgt_ssl_cas_lengths[link_idx] + 1,
        &tmp_ssl_capath,
          share->tgt_ssl_capaths_lengths[link_idx] + 1,
        &tmp_ssl_cert,
          share->tgt_ssl_certs_lengths[link_idx] + 1,
        &tmp_ssl_cipher,
          share->tgt_ssl_ciphers_lengths[link_idx] + 1,
        &tmp_ssl_key,
          share->tgt_ssl_keys_lengths[link_idx] + 1,
        &tmp_default_file,
          share->tgt_default_files_lengths[link_idx] + 1,
        &tmp_default_group,
          share->tgt_default_groups_lengths[link_idx] + 1,
        &need_mon, sizeof(int),
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_conn;
    }

		conn->retry_count_flag = 0; // 这个值默认为0 ，则在connect中retry的次数为默认值1000
    conn->default_database.init_calc_mem(75);
    conn->conn_key_length = share->conn_keys_lengths[link_idx];
    conn->conn_key = tmp_name;
    memcpy(conn->conn_key, share->conn_keys[link_idx],
      share->conn_keys_lengths[link_idx]);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    conn->conn_key_hash_value = share->conn_keys_hash_value[link_idx];
#endif
    conn->tgt_host_length = share->tgt_hosts_lengths[link_idx];
    conn->tgt_host = tmp_host;
    memcpy(conn->tgt_host, share->tgt_hosts[link_idx],
      share->tgt_hosts_lengths[link_idx]);
    conn->tgt_username_length = share->tgt_usernames_lengths[link_idx];
    conn->tgt_username = tmp_username;
    memcpy(conn->tgt_username, share->tgt_usernames[link_idx],
      share->tgt_usernames_lengths[link_idx]);
    conn->tgt_password_length = share->tgt_passwords_lengths[link_idx];
    conn->tgt_password = tmp_password;
    memcpy(conn->tgt_password, share->tgt_passwords[link_idx],
      share->tgt_passwords_lengths[link_idx]);
    conn->tgt_socket_length = share->tgt_sockets_lengths[link_idx];
    conn->tgt_socket = tmp_socket;
    memcpy(conn->tgt_socket, share->tgt_sockets[link_idx],
      share->tgt_sockets_lengths[link_idx]);
    conn->tgt_wrapper_length = share->tgt_wrappers_lengths[link_idx];
    conn->tgt_wrapper = tmp_wrapper;
    memcpy(conn->tgt_wrapper, share->tgt_wrappers[link_idx],
      share->tgt_wrappers_lengths[link_idx]);
    conn->tgt_ssl_ca_length = share->tgt_ssl_cas_lengths[link_idx];
    if (conn->tgt_ssl_ca_length)
    {
      conn->tgt_ssl_ca = tmp_ssl_ca;
      memcpy(conn->tgt_ssl_ca, share->tgt_ssl_cas[link_idx],
        share->tgt_ssl_cas_lengths[link_idx]);
    } else
      conn->tgt_ssl_ca = NULL;
    conn->tgt_ssl_capath_length = share->tgt_ssl_capaths_lengths[link_idx];
    if (conn->tgt_ssl_capath_length)
    {
      conn->tgt_ssl_capath = tmp_ssl_capath;
      memcpy(conn->tgt_ssl_capath, share->tgt_ssl_capaths[link_idx],
        share->tgt_ssl_capaths_lengths[link_idx]);
    } else
      conn->tgt_ssl_capath = NULL;
    conn->tgt_ssl_cert_length = share->tgt_ssl_certs_lengths[link_idx];
    if (conn->tgt_ssl_cert_length)
    {
      conn->tgt_ssl_cert = tmp_ssl_cert;
      memcpy(conn->tgt_ssl_cert, share->tgt_ssl_certs[link_idx],
        share->tgt_ssl_certs_lengths[link_idx]);
    } else
      conn->tgt_ssl_cert = NULL;
    conn->tgt_ssl_cipher_length = share->tgt_ssl_ciphers_lengths[link_idx];
    if (conn->tgt_ssl_cipher_length)
    {
      conn->tgt_ssl_cipher = tmp_ssl_cipher;
      memcpy(conn->tgt_ssl_cipher, share->tgt_ssl_ciphers[link_idx],
        share->tgt_ssl_ciphers_lengths[link_idx]);
    } else
      conn->tgt_ssl_cipher = NULL;
    conn->tgt_ssl_key_length = share->tgt_ssl_keys_lengths[link_idx];
    if (conn->tgt_ssl_key_length)
    {
      conn->tgt_ssl_key = tmp_ssl_key;
      memcpy(conn->tgt_ssl_key, share->tgt_ssl_keys[link_idx],
        share->tgt_ssl_keys_lengths[link_idx]);
    } else
      conn->tgt_ssl_key = NULL;
    conn->tgt_default_file_length = share->tgt_default_files_lengths[link_idx];
    if (conn->tgt_default_file_length)
    {
      conn->tgt_default_file = tmp_default_file;
      memcpy(conn->tgt_default_file, share->tgt_default_files[link_idx],
        share->tgt_default_files_lengths[link_idx]);
    } else
      conn->tgt_default_file = NULL;
    conn->tgt_default_group_length =
      share->tgt_default_groups_lengths[link_idx];
    if (conn->tgt_default_group_length)
    {
      conn->tgt_default_group = tmp_default_group;
      memcpy(conn->tgt_default_group, share->tgt_default_groups[link_idx],
        share->tgt_default_groups_lengths[link_idx]);
    } else
      conn->tgt_default_group = NULL;
    conn->tgt_port = share->tgt_ports[link_idx];
    conn->tgt_ssl_vsc = share->tgt_ssl_vscs[link_idx];
    conn->dbton_id = share->sql_dbton_ids[link_idx];
		conn->bg_conn_working = false;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else if (conn_kind == SPIDER_CONN_KIND_HS_READ) {
    if (!(conn = (SPIDER_CONN *)
      spider_bulk_malloc(spider_current_trx, 19, MYF(MY_WME | MY_ZEROFILL),
        &conn, sizeof(*conn),
        &tmp_name, share->hs_read_conn_keys_lengths[link_idx] + 1,
        &tmp_host, share->tgt_hosts_lengths[link_idx] + 1,
        &tmp_socket, share->hs_read_socks_lengths[link_idx] + 1,
        &tmp_wrapper,
          share->tgt_wrappers_lengths[link_idx] + 1,
        &need_mon, sizeof(int),
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_conn;
    }

    conn->default_database.init_calc_mem(76);
    conn->conn_key_length = share->hs_read_conn_keys_lengths[link_idx];
    conn->conn_key = tmp_name;
    memcpy(conn->conn_key, share->hs_read_conn_keys[link_idx],
      share->hs_read_conn_keys_lengths[link_idx]);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    conn->conn_key_hash_value = share->hs_read_conn_keys_hash_value[link_idx];
#endif
    conn->tgt_host_length = share->tgt_hosts_lengths[link_idx];
    conn->tgt_host = tmp_host;
    memcpy(conn->tgt_host, share->tgt_hosts[link_idx],
      share->tgt_hosts_lengths[link_idx]);
    conn->hs_sock_length = share->hs_read_socks_lengths[link_idx];
    if (conn->hs_sock_length)
    {
      conn->hs_sock = tmp_socket;
      memcpy(conn->hs_sock, share->hs_read_socks[link_idx],
        share->hs_read_socks_lengths[link_idx]);
    } else
      conn->hs_sock = NULL;
    conn->tgt_wrapper_length = share->tgt_wrappers_lengths[link_idx];
    conn->tgt_wrapper = tmp_wrapper;
    memcpy(conn->tgt_wrapper, share->tgt_wrappers[link_idx],
      share->tgt_wrappers_lengths[link_idx]);
    conn->hs_port = share->hs_read_ports[link_idx];
    conn->dbton_id = share->hs_dbton_ids[link_idx];
  } else {
    if (!(conn = (SPIDER_CONN *)
      spider_bulk_malloc(spider_current_trx, 20, MYF(MY_WME | MY_ZEROFILL),
        &conn, sizeof(*conn),
        &tmp_name, share->hs_write_conn_keys_lengths[link_idx] + 1,
        &tmp_host, share->tgt_hosts_lengths[link_idx] + 1,
        &tmp_socket, share->hs_write_socks_lengths[link_idx] + 1,
        &tmp_wrapper,
          share->tgt_wrappers_lengths[link_idx] + 1,
        &need_mon, sizeof(int),
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_conn;
    }

    conn->default_database.init_calc_mem(77);
    conn->conn_key_length = share->hs_write_conn_keys_lengths[link_idx];
    conn->conn_key = tmp_name;
    memcpy(conn->conn_key, share->hs_write_conn_keys[link_idx],
      share->hs_write_conn_keys_lengths[link_idx]);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    conn->conn_key_hash_value = share->hs_write_conn_keys_hash_value[link_idx];
#endif
    conn->tgt_host_length = share->tgt_hosts_lengths[link_idx];
    conn->tgt_host = tmp_host;
    memcpy(conn->tgt_host, share->tgt_hosts[link_idx],
      share->tgt_hosts_lengths[link_idx]);
    conn->hs_sock_length = share->hs_write_socks_lengths[link_idx];
    if (conn->hs_sock_length)
    {
      conn->hs_sock = tmp_socket;
      memcpy(conn->hs_sock, share->hs_write_socks[link_idx],
        share->hs_write_socks_lengths[link_idx]);
    } else
      conn->hs_sock = NULL;
    conn->tgt_wrapper_length = share->tgt_wrappers_lengths[link_idx];
    conn->tgt_wrapper = tmp_wrapper;
    memcpy(conn->tgt_wrapper, share->tgt_wrappers[link_idx],
      share->tgt_wrappers_lengths[link_idx]);
    conn->hs_port = share->hs_write_ports[link_idx];
    conn->dbton_id = share->hs_dbton_ids[link_idx];
  }
#endif
  if (!(conn->db_conn = spider_dbton[conn->dbton_id].create_db_conn(conn)))
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_db_conn_create;
  }
  if ((*error_num = conn->db_conn->init()))
  {
    goto error_db_conn_init;
  }
  conn->join_trx = 0;
  conn->thd = NULL;
  conn->table_lock = 0;
  conn->semi_trx_isolation = -2;
  conn->semi_trx_isolation_chk = FALSE;
  conn->semi_trx_chk = FALSE;
  conn->link_idx = base_link_idx;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  conn->conn_kind = conn_kind;
#endif
  conn->conn_need_mon = need_mon;
  if (spider)
    conn->need_mon = &spider->need_mons[base_link_idx];
  else
    conn->need_mon = need_mon;

#if MYSQL_VERSION_ID < 50500
  if (pthread_mutex_init(&conn->mta_conn_mutex, MY_MUTEX_INIT_FAST))
#else
  if (mysql_mutex_init(spd_key_mutex_mta_conn, &conn->mta_conn_mutex,
    MY_MUTEX_INIT_FAST))
#endif
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_mta_conn_mutex_init;
  }

  spider_conn_queue_connect(share, conn, link_idx);
  conn->ping_time = (time_t) time((time_t*) 0);
  pthread_mutex_lock(&spider_conn_id_mutex);
  conn->conn_id = spider_conn_id;  // 每个conn都有唯一的 conn_id
  ++spider_conn_id;
  pthread_mutex_unlock(&spider_conn_id_mutex);
  /* harryczhang: */
  conn->last_visited = time(NULL);

 /* willhan */
	pthread_mutex_lock(&spider_ipport_count_mutex);
	if (ip_port_conn = (SPIDER_IP_PORT_CONN*) my_hash_search_using_hash_value(&spider_ipport_conns, conn->conn_key_hash_value, (uchar*)conn->conn_key, conn->conn_key_length))
	{/* 存在计算+1 */
		if(opt_spider_max_connections)
		{/* 启用了连接池限制 */
			if((unsigned long)(ip_port_conn->ip_port_count) >= opt_spider_max_connections)
			{/* 如果创建的连接数达到上限，则释放conn， return NULL */
				pthread_mutex_unlock(&spider_ipport_count_mutex);
				goto error_too_many_ipport_count;
			}
		}
		ip_port_conn->ip_port_count++;
	}
	else
	{// 不存在则创建
		ip_port_conn = spider_create_ipport_conn(conn);
		if (!ip_port_conn) {
			/* 失败亦不影响正常的create conn行为 */
			pthread_mutex_unlock(&spider_ipport_count_mutex);
			DBUG_RETURN(conn);
		}
		if (my_hash_insert(&spider_ipport_conns, (uchar *)ip_port_conn)) {
			/* insert failed, 不影响正常的create conn行为 */
			pthread_mutex_unlock(&spider_ipport_count_mutex);
			DBUG_RETURN(conn);
		}
	}
	pthread_mutex_unlock(&spider_ipport_count_mutex);
  
 DBUG_RETURN(conn);
/*
error_init_lock_table_hash:
  DBUG_ASSERT(!conn->mta_conn_mutex_file_pos.file_name);
  pthread_mutex_destroy(&conn->mta_conn_mutex);
*/
error_too_many_ipport_count:
error_mta_conn_mutex_init:
error_db_conn_init:
  delete conn->db_conn;
error_db_conn_create:
  spider_free(spider_current_trx, conn, MYF(0));
error_alloc_conn:
  DBUG_RETURN(NULL);
}

SPIDER_CONN *spider_get_conn(
  SPIDER_SHARE *share,
  int link_idx,
  char *conn_key,
  SPIDER_TRX *trx,
  ha_spider *spider,                /* 若非NULL，主要用于active-standby */
  bool another,                     /* 标记操作trx的连接池对象 */
  bool thd_chg,                     /* unused */
  uint conn_kind,
  int *error_num
) {
  SPIDER_CONN *conn = NULL;
  int base_link_idx = link_idx;
  DBUG_ENTER("spider_get_conn");
  DBUG_PRINT("info",("spider conn_kind=%u", conn_kind));

  if (spider)
    link_idx = spider->conn_link_idx[base_link_idx];
  DBUG_PRINT("info",("spider link_idx=%u", link_idx));
  DBUG_PRINT("info",("spider base_link_idx=%u", base_link_idx));

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
#ifndef DBUG_OFF
    spider_print_keys(conn_key, share->conn_keys_lengths[link_idx]);
#endif
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else if (conn_kind == SPIDER_CONN_KIND_HS_READ)
  {
    conn_key = share->hs_read_conn_keys[link_idx];
#ifndef DBUG_OFF
    spider_print_keys(conn_key, share->hs_read_conn_keys_lengths[link_idx]);
#endif
  } else {
    conn_key = share->hs_write_conn_keys[link_idx];
#ifndef DBUG_OFF
    spider_print_keys(conn_key, share->hs_write_conn_keys_lengths[link_idx]);
#endif
  }
#endif
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
  if (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    (conn_kind == SPIDER_CONN_KIND_MYSQL &&
      (
#endif
        (another &&
          !(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
            &trx->trx_another_conn_hash,
            share->conn_keys_hash_value[link_idx],
            (uchar*) conn_key, share->conn_keys_lengths[link_idx]))) ||
        (!another &&
          !(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
            &trx->trx_conn_hash,
            share->conn_keys_hash_value[link_idx],
            (uchar*) conn_key, share->conn_keys_lengths[link_idx])))
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      )
    ) ||
    (conn_kind == SPIDER_CONN_KIND_HS_READ &&
      !(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
        &trx->trx_hs_r_conn_hash,
        share->hs_read_conn_keys_hash_value[link_idx],
        (uchar*) conn_key, share->hs_read_conn_keys_lengths[link_idx]))
    ) ||
    (conn_kind == SPIDER_CONN_KIND_HS_WRITE &&
      !(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
        &trx->trx_hs_w_conn_hash,
        share->hs_write_conn_keys_hash_value[link_idx],
        (uchar*) conn_key, share->hs_write_conn_keys_lengths[link_idx]))
    )
#endif
  )
#else
  if (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    (conn_kind == SPIDER_CONN_KIND_MYSQL &&
      (
#endif
        (another &&
          !(conn = (SPIDER_CONN*) my_hash_search(&trx->trx_another_conn_hash,
            (uchar*) conn_key, share->conn_keys_lengths[link_idx]))) ||
        (!another &&
          !(conn = (SPIDER_CONN*) my_hash_search(&trx->trx_conn_hash,
            (uchar*) conn_key, share->conn_keys_lengths[link_idx])))
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      )
    ) ||
    (conn_kind == SPIDER_CONN_KIND_HS_READ &&
      !(conn = (SPIDER_CONN*) my_hash_search(&trx->trx_hs_r_conn_hash,
        (uchar*) conn_key, share->hs_read_conn_keys_lengths[link_idx]))
    ) ||
    (conn_kind == SPIDER_CONN_KIND_HS_WRITE &&
      !(conn = (SPIDER_CONN*) my_hash_search(&trx->trx_hs_w_conn_hash,
        (uchar*) conn_key, share->hs_write_conn_keys_lengths[link_idx]))
    )
#endif
  )
#endif
  { /* 1.1 trx->trx_another_conn_hash或trx->trx_conn_hash（本会话）不包含相应的conn对象，关注another的值 */
    if (
      !trx->thd ||
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      (conn_kind == SPIDER_CONN_KIND_MYSQL &&
#endif
        (
          (spider_param_conn_recycle_mode(trx->thd) & 1) ||     /* 2.1 可重用其他会话的连接 */
          spider_param_conn_recycle_strict(trx->thd)
        )
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      ) ||
      (conn_kind == SPIDER_CONN_KIND_HS_READ &&
        (
          (spider_param_hs_r_conn_recycle_mode(trx->thd) & 1) ||
          spider_param_hs_r_conn_recycle_strict(trx->thd)
        )
      ) ||
      (conn_kind == SPIDER_CONN_KIND_HS_WRITE &&
        (
          (spider_param_hs_w_conn_recycle_mode(trx->thd) & 1) ||
          spider_param_hs_w_conn_recycle_strict(trx->thd)
        )
      )
#endif
    ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if (conn_kind == SPIDER_CONN_KIND_MYSQL)
      {
#endif
        pthread_mutex_lock(&spider_conn_mutex);

#ifdef SPIDER_HAS_HASH_VALUE_TYPE
        if (!(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
          &spider_open_connections, share->conn_keys_hash_value[link_idx],
          (uchar*) share->conn_keys[link_idx],
          share->conn_keys_lengths[link_idx])))
#else
        if (!(conn = (SPIDER_CONN*) my_hash_search(&spider_open_connections,
          (uchar*) share->conn_keys[link_idx],
          share->conn_keys_lengths[link_idx])))
#endif
		{
			pthread_mutex_unlock(&spider_conn_mutex);
			if(opt_spider_max_connections)
			{ /* opt_spider_max_connections为0表示不启用连接池， */
				/* 只有 opt_spider_max_connections 大于0，opt_spider_max_connections表示单ip#port的最大连接上限 */
				conn = spider_get_conn_from_idle_connection(share, link_idx, conn_key, spider, conn_kind, base_link_idx, error_num);
				/* 获取连接失败，则goto error */
				if(!conn)
					goto error;

			}
			else
			{	/* 未启用连接限制，则create_conn */
				DBUG_PRINT("info",("spider create new conn"));
				if (!(conn = spider_create_conn(share, spider, link_idx,
					base_link_idx, conn_kind, error_num)))                          /*  不存在可重用资源，则新建 */
					goto error;
				*conn->conn_key = *conn_key;
				if (spider)
				{
					spider->conns[base_link_idx] = conn;
					if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
						conn->use_for_active_standby = TRUE;
				}
			}
		} else {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
          my_hash_delete_with_hash_value(&spider_open_connections,
            conn->conn_key_hash_value, (uchar*) conn);
#else
          my_hash_delete(&spider_open_connections, (uchar*) conn);          /* 3.2 如存在，重用该资源 */
#endif
          pthread_mutex_unlock(&spider_conn_mutex);
          DBUG_PRINT("info",("spider get global conn"));
          if (spider)
          {
            spider->conns[base_link_idx] = conn;
            if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
              conn->use_for_active_standby = TRUE;  
          }
        }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      } else if (conn_kind == SPIDER_CONN_KIND_HS_READ)
      {
        pthread_mutex_lock(&spider_hs_r_conn_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
        if (!(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
          &spider_hs_r_conn_hash,
          share->hs_read_conn_keys_hash_value[link_idx],
          (uchar*) share->hs_read_conn_keys[link_idx],
          share->hs_read_conn_keys_lengths[link_idx])))
#else
        if (!(conn = (SPIDER_CONN*) my_hash_search(&spider_hs_r_conn_hash,
          (uchar*) share->hs_read_conn_keys[link_idx],
          share->hs_read_conn_keys_lengths[link_idx])))
#endif
        {
          pthread_mutex_unlock(&spider_hs_r_conn_mutex);
          DBUG_PRINT("info",("spider create new hs r conn"));
          if (!(conn = spider_create_conn(share, spider, link_idx,
            base_link_idx, conn_kind, error_num)))
            goto error;
          *conn->conn_key = *conn_key;
          if (spider)
          {
            spider->hs_r_conns[base_link_idx] = conn;
            if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
              conn->use_for_active_standby = TRUE;
          }
        } else {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
          my_hash_delete_with_hash_value(&spider_hs_r_conn_hash,
            conn->conn_key_hash_value, (uchar*) conn);
#else
          my_hash_delete(&spider_hs_r_conn_hash, (uchar*) conn);
#endif
          pthread_mutex_unlock(&spider_hs_r_conn_mutex);
          DBUG_PRINT("info",("spider get global hs r conn"));
          if (spider)
          {
            spider->hs_r_conns[base_link_idx] = conn;
            if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
              conn->use_for_active_standby = TRUE;
          }
        }
      } else {
        pthread_mutex_lock(&spider_hs_w_conn_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
        if (!(conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
          &spider_hs_w_conn_hash,
          share->hs_write_conn_keys_hash_value[link_idx],
          (uchar*) share->hs_write_conn_keys[link_idx],
          share->hs_write_conn_keys_lengths[link_idx])))
#else
        if (!(conn = (SPIDER_CONN*) my_hash_search(&spider_hs_w_conn_hash,
          (uchar*) share->hs_write_conn_keys[link_idx],
          share->hs_write_conn_keys_lengths[link_idx])))
#endif
        {
          pthread_mutex_unlock(&spider_hs_w_conn_mutex);
          DBUG_PRINT("info",("spider create new hs w conn"));
          if (!(conn = spider_create_conn(share, spider, link_idx,
            base_link_idx, conn_kind, error_num)))
            goto error;
          *conn->conn_key = *conn_key;
          if (spider)
          {
            spider->hs_w_conns[base_link_idx] = conn;
            if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
              conn->use_for_active_standby = TRUE;
          }
        } else {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
          my_hash_delete_with_hash_value(&spider_hs_w_conn_hash,
            conn->conn_key_hash_value, (uchar*) conn);
#else
          my_hash_delete(&spider_hs_w_conn_hash, (uchar*) conn);
#endif
          pthread_mutex_unlock(&spider_hs_w_conn_mutex);
          DBUG_PRINT("info",("spider get global hs w conn"));
          if (spider)
          {
            spider->hs_w_conns[base_link_idx] = conn;
            if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
              conn->use_for_active_standby = TRUE;
          }
        }
      }
#endif
    } else {                                                /* 2.2 不可重用其他连接资源，新建 */
      DBUG_PRINT("info",("spider create new conn"));
      /* conn_recycle_strict = 0 and conn_recycle_mode = 0 or 2 */
      if (!(conn = spider_create_conn(share, spider, link_idx, base_link_idx, conn_kind, error_num)))
        goto error;
      *conn->conn_key = *conn_key;
      if (spider)
      {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        if (conn_kind == SPIDER_CONN_KIND_MYSQL)
        {
#endif
          spider->conns[base_link_idx] = conn;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        } else if (conn_kind == SPIDER_CONN_KIND_HS_READ)
        {
          spider->hs_r_conns[base_link_idx] = conn;
        } else {
          spider->hs_w_conns[base_link_idx] = conn;
        }
#endif
        if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
          conn->use_for_active_standby = TRUE;
      }
    }
    conn->thd = trx->thd;
    conn->priority = share->priority;

    /* 2.3 连接对象插入hash表trx->trx_another_conn_hash或trx->trx_conn_hash，视乎another */
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (conn_kind == SPIDER_CONN_KIND_MYSQL)
    {
#endif
      if (another)
      {
        uint old_elements = trx->trx_another_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        if (my_hash_insert_with_hash_value(&trx->trx_another_conn_hash,
          share->conn_keys_hash_value[link_idx],
          (uchar*) conn))
#else
        if (my_hash_insert(&trx->trx_another_conn_hash, (uchar*) conn))
#endif
        {
          spider_free_conn(conn);
          *error_num = HA_ERR_OUT_OF_MEM;
          goto error;
        }
        if (trx->trx_another_conn_hash.array.max_element > old_elements)
        {
          spider_alloc_calc_mem(spider_current_trx,
            trx->trx_another_conn_hash,
            (trx->trx_another_conn_hash.array.max_element - old_elements) *
            trx->trx_another_conn_hash.array.size_of_element);
        }
      } else {
        uint old_elements = trx->trx_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
        if (my_hash_insert_with_hash_value(&trx->trx_conn_hash,
          share->conn_keys_hash_value[link_idx],
          (uchar*) conn))
#else
        if (my_hash_insert(&trx->trx_conn_hash, (uchar*) conn))
#endif
        {
          spider_free_conn(conn);
          *error_num = HA_ERR_OUT_OF_MEM;
          goto error;
        }
        if (trx->trx_conn_hash.array.max_element > old_elements)
        {
          spider_alloc_calc_mem(spider_current_trx,
            trx->trx_conn_hash,
            (trx->trx_conn_hash.array.max_element - old_elements) *
            trx->trx_conn_hash.array.size_of_element);
        }
      }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else if (conn_kind == SPIDER_CONN_KIND_HS_READ)
    {
      uint old_elements = trx->trx_hs_r_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      if (my_hash_insert_with_hash_value(&trx->trx_hs_r_conn_hash,
        share->hs_read_conn_keys_hash_value[link_idx],
        (uchar*) conn))
#else
      if (my_hash_insert(&trx->trx_hs_r_conn_hash, (uchar*) conn))
#endif
      {
        spider_free_conn(conn);
        *error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
      if (trx->trx_hs_r_conn_hash.array.max_element > old_elements)
      {
        spider_alloc_calc_mem(spider_current_trx,
          trx->trx_hs_r_conn_hash,
          (trx->trx_hs_r_conn_hash.array.max_element - old_elements) *
          trx->trx_hs_r_conn_hash.array.size_of_element);
      }
    } else {
      uint old_elements = trx->trx_hs_w_conn_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      if (my_hash_insert_with_hash_value(&trx->trx_hs_w_conn_hash,
        share->hs_write_conn_keys_hash_value[link_idx],
        (uchar*) conn))
#else
      if (my_hash_insert(&trx->trx_hs_w_conn_hash, (uchar*) conn))
#endif
      {
        spider_free_conn(conn);
        *error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
      if (trx->trx_hs_w_conn_hash.array.max_element > old_elements)
      {
        spider_alloc_calc_mem(spider_current_trx,
          trx->trx_hs_w_conn_hash,
          (trx->trx_hs_w_conn_hash.array.max_element - old_elements) *
          trx->trx_hs_w_conn_hash.array.size_of_element);
      }
    }
#endif
  } else if (spider)            /* 1.2 本会话包含可用连接对象 */
  {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (conn_kind == SPIDER_CONN_KIND_MYSQL)
    {
#endif
      spider->conns[base_link_idx] = conn;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else if (conn_kind == SPIDER_CONN_KIND_HS_READ)
    {
      spider->hs_r_conns[base_link_idx] = conn;
    } else {
      spider->hs_w_conns[base_link_idx] = conn;
    }
#endif
    if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
      conn->use_for_active_standby = TRUE;
  }
  conn->link_idx = base_link_idx;

// 更新conn中的share对象， conn中的share对象存在未更新也使用的现象，若share被alter，
//  则会读到旧的share， 发生crash
  spider_conn_queue_connect_rewrite(share, conn, link_idx); 

  DBUG_PRINT("info",("spider conn=%p", conn));
  DBUG_RETURN(conn);

error:
  DBUG_RETURN(NULL);
}

int spider_free_conn(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn");
  DBUG_PRINT("info", ("spider conn=%p", conn));

	SPIDER_IP_PORT_CONN* ip_port_conn;
	/* willhan */
	/* 在free conn时，会加conn_mutex； 需要free的也不会在open_connection_hash中  */
	pthread_mutex_lock(&spider_ipport_count_mutex);
	if (ip_port_conn = (SPIDER_IP_PORT_CONN*) my_hash_search_using_hash_value(&spider_ipport_conns, conn->conn_key_hash_value, (uchar*)conn->conn_key, conn->conn_key_length))
	{/* 释放conn时让计数 -1 */
    if (ip_port_conn->ip_port_count > 0)
		  ip_port_conn->ip_port_count--;
	}
	pthread_mutex_unlock(&spider_ipport_count_mutex);

	conn->bg_conn_working = false;

  spider_free_conn_alloc(conn);
  spider_free(spider_current_trx, conn, MYF(0));
  DBUG_RETURN(0);
}

void spider_conn_queue_connect(
  SPIDER_SHARE *share,
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_conn_queue_connect");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_connect = TRUE;
  conn->queued_connect_share = share;
  conn->queued_connect_link_idx = link_idx;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_connect_rewrite(
  SPIDER_SHARE *share,
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_conn_queue_connect_rewrite");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_connect_share = share;
  conn->queued_connect_link_idx = link_idx;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_ping(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_conn_queue_ping");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_ping = TRUE;
  conn->queued_ping_spider = spider;
  conn->queued_ping_link_idx = link_idx;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation
) {
  DBUG_ENTER("spider_conn_queue_trx_isolation");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_trx_isolation = TRUE;
  conn->queued_trx_isolation_val = trx_isolation;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_semi_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation
) {
  DBUG_ENTER("spider_conn_queue_semi_trx_isolation");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_semi_trx_isolation = TRUE;
  conn->queued_semi_trx_isolation_val = trx_isolation;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_autocommit(
  SPIDER_CONN *conn,
  bool autocommit
) {
  DBUG_ENTER("spider_conn_queue_autocommit");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_autocommit = TRUE;
  conn->queued_autocommit_val = autocommit;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_sql_log_off(
  SPIDER_CONN *conn,
  bool sql_log_off
) {
  DBUG_ENTER("spider_conn_queue_sql_log_off");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_sql_log_off = TRUE;
  conn->queued_sql_log_off_val = sql_log_off;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_time_zone(
  SPIDER_CONN *conn,
  Time_zone *time_zone
) {
  DBUG_ENTER("spider_conn_queue_time_zone");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_time_zone = TRUE;
  conn->queued_time_zone_val = time_zone;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_start_transaction(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_conn_queue_start_transaction");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_trx_start = TRUE;
  conn->trx_start = TRUE;
  DBUG_VOID_RETURN;
}

void spider_conn_queue_xa_start(
  SPIDER_CONN *conn,
  XID *xid
) {
  DBUG_ENTER("spider_conn_queue_xa_start");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_xa_start = TRUE;
  conn->queued_xa_start_xid = xid;
  DBUG_VOID_RETURN;
}

void spider_conn_clear_queue(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_conn_clear_queue");
  DBUG_PRINT("info", ("spider conn=%p", conn));
/*
  conn->queued_connect = FALSE;
  conn->queued_ping = FALSE;
*/
  conn->queued_trx_isolation = FALSE;
  conn->queued_semi_trx_isolation = FALSE;
  conn->queued_autocommit = FALSE;
  conn->queued_sql_log_off = FALSE;
  conn->queued_time_zone = FALSE;
  conn->queued_trx_start = FALSE;
  conn->queued_xa_start = FALSE;
  DBUG_VOID_RETURN;
}

void spider_conn_clear_queue_at_commit(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_conn_clear_queue_at_commit");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  conn->queued_trx_start = FALSE;
  conn->queued_xa_start = FALSE;
  DBUG_VOID_RETURN;
}

void spider_conn_set_timeout(
  SPIDER_CONN *conn,
  uint net_read_timeout,
  uint net_write_timeout
) {
  DBUG_ENTER("spider_conn_set_timeout");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  if (net_read_timeout != conn->net_read_timeout)
  {
    DBUG_PRINT("info",("spider net_read_timeout set from %u to %u",
      conn->net_read_timeout, net_read_timeout));
    conn->queued_net_timeout = TRUE;
    conn->net_read_timeout = net_read_timeout;
  }
  if (net_write_timeout != conn->net_write_timeout)
  {
    DBUG_PRINT("info",("spider net_write_timeout set from %u to %u",
      conn->net_write_timeout, net_write_timeout));
    conn->queued_net_timeout = TRUE;
    conn->net_write_timeout = net_write_timeout;
  }
  DBUG_VOID_RETURN;
}

void spider_conn_set_timeout_from_share(
  SPIDER_CONN *conn,
  int link_idx,
  THD *thd,
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_conn_set_timeout_from_share");
  spider_conn_set_timeout(
    conn,
    spider_param_net_read_timeout(thd, share->net_read_timeouts[link_idx]),
    spider_param_net_write_timeout(thd, share->net_write_timeouts[link_idx])
  );
  DBUG_VOID_RETURN;
}

void spider_conn_set_timeout_from_direct_sql(
  SPIDER_CONN *conn,
  THD *thd,
  SPIDER_DIRECT_SQL *direct_sql
) {
  DBUG_ENTER("spider_conn_set_timeout_from_direct_sql");
  spider_conn_set_timeout(
    conn,
    spider_param_net_read_timeout(thd, direct_sql->net_read_timeout),
    spider_param_net_write_timeout(thd, direct_sql->net_write_timeout)
  );
  DBUG_VOID_RETURN;
}

void spider_tree_insert(
  SPIDER_CONN *top,
  SPIDER_CONN *conn
) {
  SPIDER_CONN *current = top;
  longlong priority = conn->priority;
  DBUG_ENTER("spider_tree_insert");
  while (TRUE)
  {
    if (priority < current->priority)
    {
      if (current->c_small == NULL)
      {
        conn->p_small = NULL;
        conn->p_big = current;
        conn->c_small = NULL;
        conn->c_big = NULL;
        current->c_small = conn;
        break;
      } else
        current = current->c_small;
    } else {
      if (current->c_big == NULL)
      {
        conn->p_small = current;
        conn->p_big = NULL;
        conn->c_small = NULL;
        conn->c_big = NULL;
        current->c_big = conn;
        break;
      } else
        current = current->c_big;
    }
  }
  DBUG_VOID_RETURN;
}

SPIDER_CONN *spider_tree_first(
  SPIDER_CONN *top
) {
  SPIDER_CONN *current = top;
  DBUG_ENTER("spider_tree_first");
  while (current)
  {
    if (current->c_small == NULL)
      break;
    else
      current = current->c_small;
  }
  DBUG_RETURN(current);
}

SPIDER_CONN *spider_tree_last(
  SPIDER_CONN *top
) {
  SPIDER_CONN *current = top;
  DBUG_ENTER("spider_tree_last");
  while (TRUE)
  {
    if (current->c_big == NULL)
      break;
    else
      current = current->c_big;
  }
  DBUG_RETURN(current);
}

SPIDER_CONN *spider_tree_next(
  SPIDER_CONN *current
) {
  DBUG_ENTER("spider_tree_next");
  if (current->c_big)
    DBUG_RETURN(spider_tree_first(current->c_big));
  while (TRUE)
  {
    if (current->p_big)
      DBUG_RETURN(current->p_big);
    if (!current->p_small)
      DBUG_RETURN(NULL);
    current = current->p_small;
  }
}

SPIDER_CONN *spider_tree_delete(
  SPIDER_CONN *conn,
  SPIDER_CONN *top
) {
  DBUG_ENTER("spider_tree_delete");
  if (conn->p_small)
  {
    if (conn->c_small)
    {
      conn->c_small->p_big = NULL;
      conn->c_small->p_small = conn->p_small;
      conn->p_small->c_big = conn->c_small;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
    } else if (conn->c_big)
    {
      conn->c_big->p_small = conn->p_small;
      conn->p_small->c_big = conn->c_big;
    } else
      conn->p_small->c_big = NULL;
  } else if (conn->p_big)
  {
    if (conn->c_small)
    {
      conn->c_small->p_big = conn->p_big;
      conn->p_big->c_small = conn->c_small;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
    } else if (conn->c_big)
    {
      conn->c_big->p_big = conn->p_big;
      conn->c_big->p_small = NULL;
      conn->p_big->c_small = conn->c_big;
    } else
      conn->p_big->c_small = NULL;
  } else {
    if (conn->c_small)
    {
      conn->c_small->p_big = NULL;
      conn->c_small->p_small = NULL;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
      DBUG_RETURN(conn->c_small);
    } else if (conn->c_big)
    {
      conn->c_big->p_small = NULL;
      DBUG_RETURN(conn->c_big);
    }
    DBUG_RETURN(NULL);
  }
  DBUG_RETURN(top);
}

#ifndef WITHOUT_SPIDER_BG_SEARCH
int spider_set_conn_bg_param(
  ha_spider *spider
) {
  int error_num, roop_count, bgs_mode;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  THD *thd = spider->trx->thd;
  DBUG_ENTER("spider_set_conn_bg_param");
  bgs_mode =
    spider_param_bgs_mode(thd, share->bgs_mode);

	/* 如果不使用pre call, 则没必要让子线程来执行，让bgs_mode=0 */
	if(!spider->use_pre_call)
		bgs_mode = 0;

  if (bgs_mode == 0)
    result_list->bgs_phase = 0;
  else if (
    bgs_mode <= 2 &&
    (result_list->lock_type == F_WRLCK || spider->lock_mode == 2)
  )
    result_list->bgs_phase = 0;
  else if (bgs_mode <= 1 && spider->lock_mode == 1)
    result_list->bgs_phase = 0;
  else {
    result_list->bgs_phase = 1;

    result_list->bgs_split_read = spider_bg_split_read_param(spider);
    if (spider->use_pre_call)
    {
      DBUG_PRINT("info",("spider use_pre_call=TRUE"));
      result_list->bgs_first_read = result_list->bgs_split_read;
      result_list->bgs_second_read = result_list->bgs_split_read;
    } else {
      DBUG_PRINT("info",("spider use_pre_call=FALSE"));
      result_list->bgs_first_read =
        spider_param_bgs_first_read(thd, share->bgs_first_read);
      result_list->bgs_second_read =
        spider_param_bgs_second_read(thd, share->bgs_second_read);
    }
    DBUG_PRINT("info",("spider bgs_split_read=%lld",
      result_list->bgs_split_read));
    DBUG_PRINT("info",("spider bgs_first_read=%lld", share->bgs_first_read));
    DBUG_PRINT("info",("spider bgs_second_read=%lld", share->bgs_second_read));

    result_list->split_read =
      result_list->bgs_first_read > 0 ?
      result_list->bgs_first_read :
      result_list->bgs_split_read;
  }

  if (result_list->bgs_phase > 0)
  {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        spider->lock_mode ?
        SPIDER_LINK_STATUS_RECOVERY : SPIDER_LINK_STATUS_OK);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        spider->lock_mode ?
        SPIDER_LINK_STATUS_RECOVERY : SPIDER_LINK_STATUS_OK)
    ) {
      if ((error_num = spider_create_conn_thread(spider->spider_get_conn_by_idx(roop_count))))
        DBUG_RETURN(error_num);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if ((error_num = spider_create_conn_thread(
        spider->hs_r_conns[roop_count])))
        DBUG_RETURN(error_num);
      if ((error_num = spider_create_conn_thread(
        spider->hs_w_conns[roop_count])))
        DBUG_RETURN(error_num);
#endif
    }
  }
  DBUG_RETURN(0);
}

int spider_create_conn_thread(
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_create_conn_thread");
	if(!conn)
	{
		DBUG_RETURN(ER_SPIDER_CON_COUNT_ERROR);
	}
  if (conn && !conn->bg_init)
  {
#if MYSQL_VERSION_ID < 50500
    if (pthread_mutex_init(&conn->bg_conn_chain_mutex, MY_MUTEX_INIT_FAST))
#else
    if (mysql_mutex_init(spd_key_mutex_bg_conn_chain,
      &conn->bg_conn_chain_mutex, MY_MUTEX_INIT_FAST))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_chain_mutex_init;
    }
    conn->bg_conn_chain_mutex_ptr = NULL;
#if MYSQL_VERSION_ID < 50500
    if (pthread_mutex_init(&conn->bg_conn_sync_mutex, MY_MUTEX_INIT_FAST))
#else
    if (mysql_mutex_init(spd_key_mutex_bg_conn_sync,
      &conn->bg_conn_sync_mutex, MY_MUTEX_INIT_FAST))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_mutex_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_mutex_init(&conn->bg_conn_mutex, MY_MUTEX_INIT_FAST))
#else
    if (mysql_mutex_init(spd_key_mutex_bg_conn, &conn->bg_conn_mutex,
      MY_MUTEX_INIT_FAST))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_mutex_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_mutex_init(&conn->bg_job_stack_mutex, MY_MUTEX_INIT_FAST))
#else
    if (mysql_mutex_init(spd_key_mutex_bg_job_stack, &conn->bg_job_stack_mutex,
      MY_MUTEX_INIT_FAST))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_job_stack_mutex_init;
    }
    if (SPD_INIT_DYNAMIC_ARRAY2(&conn->bg_job_stack, sizeof(void *), NULL, 16,
      16, MYF(MY_WME)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_job_stack_init;
    }
    spider_alloc_calc_mem_init(conn->bg_job_stack, 163);
    spider_alloc_calc_mem(spider_current_trx,
      conn->bg_job_stack,
      conn->bg_job_stack.max_element *
      conn->bg_job_stack.size_of_element);
    conn->bg_job_stack_cur_pos = 0;
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&conn->bg_conn_sync_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_conn_sync,
      &conn->bg_conn_sync_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_cond_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&conn->bg_conn_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_conn,
      &conn->bg_conn_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
    pthread_mutex_lock(&conn->bg_conn_mutex); // 控制创建线程。 线程创建完（成功或者失败）后unlock
#if MYSQL_VERSION_ID < 50500
    if (pthread_create(&conn->bg_thread, &spider_pt_attr,
      spider_bg_conn_action, (void *) conn)
    )
#else
    if (mysql_thread_create(spd_key_thd_bg, &conn->bg_thread,
      &spider_pt_attr, spider_bg_conn_action, (void *) conn)
    )
#endif
    {
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    pthread_mutex_lock(&conn->bg_conn_sync_mutex); // 同步线程状态。
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex); // 等线程启动成功
    pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
    if (!conn->bg_init)
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
  }
  DBUG_RETURN(0);

error_thread_create:
  pthread_cond_destroy(&conn->bg_conn_cond);
error_cond_init:
  pthread_cond_destroy(&conn->bg_conn_sync_cond);
error_sync_cond_init:
  spider_free_mem_calc(spider_current_trx,
    conn->bg_job_stack_id,
    conn->bg_job_stack.max_element *
    conn->bg_job_stack.size_of_element);
  delete_dynamic(&conn->bg_job_stack);
error_job_stack_init:
  pthread_mutex_destroy(&conn->bg_job_stack_mutex);
error_job_stack_mutex_init:
  pthread_mutex_destroy(&conn->bg_conn_mutex);
error_mutex_init:
  pthread_mutex_destroy(&conn->bg_conn_sync_mutex);
error_sync_mutex_init:
  pthread_mutex_destroy(&conn->bg_conn_chain_mutex);
error_chain_mutex_init:
  DBUG_RETURN(error_num);
}

void spider_free_conn_thread(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn_thread");
  if (conn->bg_init)
  {
    spider_bg_conn_break(conn, NULL);
    pthread_mutex_lock(&conn->bg_conn_mutex);
    conn->bg_kill = TRUE;
    pthread_mutex_lock(&conn->bg_conn_sync_mutex);
    pthread_cond_signal(&conn->bg_conn_cond);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
    pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
    pthread_join(conn->bg_thread, NULL);
    pthread_cond_destroy(&conn->bg_conn_cond);
    pthread_cond_destroy(&conn->bg_conn_sync_cond);
    spider_free_mem_calc(spider_current_trx,
      conn->bg_job_stack_id,
      conn->bg_job_stack.max_element *
      conn->bg_job_stack.size_of_element);
    delete_dynamic(&conn->bg_job_stack);
    pthread_mutex_destroy(&conn->bg_job_stack_mutex);
    pthread_mutex_destroy(&conn->bg_conn_mutex);
    pthread_mutex_destroy(&conn->bg_conn_sync_mutex);
    pthread_mutex_destroy(&conn->bg_conn_chain_mutex);
    conn->bg_kill = FALSE;
    conn->bg_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void spider_bg_conn_wait(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_bg_conn_wait");
  if (conn->bg_init)
  {
    pthread_mutex_lock(&conn->bg_conn_mutex);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
  }
  DBUG_VOID_RETURN;
}

void spider_bg_all_conn_wait(
  ha_spider *spider
) {
  int roop_count;
  SPIDER_CONN *conn;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_bg_all_conn_wait");
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (conn && result_list->bgs_working)
      spider_bg_conn_wait(conn);
#endif
  }
  DBUG_VOID_RETURN;
}

int spider_bg_all_conn_pre_next(
  ha_spider *spider,
  int link_idx
) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int roop_start, roop_end, roop_count, lock_mode, link_ok, error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
#endif
  DBUG_ENTER("spider_bg_all_conn_pre_next");
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (result_list->bgs_phase > 0)
  {
    lock_mode = spider_conn_lock_mode(spider);
    if (lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_end = spider->share->link_count;
    } else {
      link_ok = link_idx;
      roop_start = link_idx;
      roop_end = link_idx + 1;
    }

    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      if ((error_num = spider_bg_conn_search(spider, roop_count, roop_start,
        TRUE, TRUE, (roop_count != link_ok))))
        DBUG_RETURN(error_num);
    }
  }
#endif
  DBUG_RETURN(0);
}

void spider_bg_conn_break(
  SPIDER_CONN *conn,
  ha_spider *spider
) {
  DBUG_ENTER("spider_bg_conn_break");
  if (
    conn->bg_init &&
    conn->bg_thd != current_thd &&
    (
      !spider ||
      (
        spider->result_list.bgs_working &&
        conn->bg_target == spider
      )
    )
  ) {
    conn->bg_break = TRUE;
    pthread_mutex_lock(&conn->bg_conn_mutex);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    conn->bg_break = FALSE;
  }
  DBUG_VOID_RETURN;
}

void spider_bg_all_conn_break(
  ha_spider *spider
) {
  int roop_count;
  SPIDER_CONN *conn;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_bg_all_conn_break");
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (conn && result_list->bgs_working)
      spider_bg_conn_break(conn, spider);
#endif
    if (spider->quick_targets[roop_count])
    {
      DBUG_ASSERT(spider->quick_targets[roop_count] == conn->quick_target);
      DBUG_PRINT("info", ("spider conn[%p]->quick_target=NULL", conn));
      conn->quick_target = NULL;
      spider->quick_targets[roop_count] = NULL;
    }
  }
  DBUG_VOID_RETURN;
}

bool spider_bg_conn_get_job(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_bg_conn_get_job");
  pthread_mutex_lock(&conn->bg_job_stack_mutex);
  if (conn->bg_job_stack_cur_pos >= conn->bg_job_stack.elements)
  {
    DBUG_PRINT("info",("spider bg all jobs are completed"));
    conn->bg_get_job_stack_off = FALSE;
    pthread_mutex_unlock(&conn->bg_job_stack_mutex);
    DBUG_RETURN(FALSE);
  }
  DBUG_PRINT("info",("spider bg get job %u",
    conn->bg_job_stack_cur_pos));
  conn->bg_target = ((void **) (conn->bg_job_stack.buffer +
    conn->bg_job_stack.size_of_element * conn->bg_job_stack_cur_pos))[0];
  conn->bg_job_stack_cur_pos++;
  if (conn->bg_job_stack_cur_pos == conn->bg_job_stack.elements)
  {
    DBUG_PRINT("info",("spider bg shift job stack"));
    conn->bg_job_stack_cur_pos = 0;
    conn->bg_job_stack.elements = 0;
  }
  pthread_mutex_unlock(&conn->bg_job_stack_mutex);
  DBUG_RETURN(TRUE);
}

int spider_bg_conn_search(
  ha_spider *spider,
  int link_idx,
  int first_link_idx,
  bool first,
  bool pre_next,
  bool discard_result
) {
  int error_num;
  SPIDER_CONN *conn, *first_conn = NULL;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
	bool with_lock = FALSE;
	THD *thd = current_thd;
  DBUG_ENTER("spider_bg_conn_search");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_MYSQL)
  {
#endif
		if(!(conn = spider->spider_get_conn_by_idx(link_idx)))
		{
			error_num = ER_SPIDER_CON_COUNT_ERROR;
			DBUG_RETURN(error_num);
		}
    with_lock = (spider_conn_lock_mode(spider) != SPIDER_LOCK_MODE_NO_LOCK);
		if(!(first_conn = spider->spider_get_conn_by_idx(first_link_idx)))
		{
			error_num = ER_SPIDER_CON_COUNT_ERROR;
			DBUG_RETURN(error_num);
		}
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_HS_READ)
    conn = spider->hs_r_conns[link_idx];
  else
    conn = spider->hs_w_conns[link_idx];
#endif
  if (first)
  {
		if (spider->use_pre_call)
		{
			DBUG_PRINT("info",("spider skip bg first search"));
		} else {
			DBUG_PRINT("info",("spider bg first search"));
			pthread_mutex_lock(&conn->bg_conn_mutex);
			result_list->bgs_working = TRUE;
			conn->bg_search = TRUE;
			conn->bg_caller_wait = TRUE;
			conn->bg_target = spider;
			conn->link_idx = link_idx;
			conn->bg_discard_result = discard_result;
			thd_proc_info(thd, "bg_search wait start 2");
			pthread_mutex_lock(&conn->bg_conn_sync_mutex); // 必须保证sinal前，后台线程是wait状态
			pthread_cond_signal(&conn->bg_conn_cond);
			pthread_mutex_unlock(&conn->bg_conn_mutex);
			pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex); // 如果不wait，也没影响 ？  相当于一次握手 ？
			pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
			thd_proc_info(thd, "bg_search wait end 2");
			conn->bg_caller_wait = FALSE;
			if (result_list->bgs_error)
			{
				if (result_list->bgs_error_with_message)
					my_message(result_list->bgs_error,
					result_list->bgs_error_msg, MYF(0));
				DBUG_RETURN(result_list->bgs_error);
			}
		}
    if (!result_list->finish_flg || conn->bg_conn_working)
    {
			thd_proc_info(thd, "bg_search wait start 4");
      pthread_mutex_lock(&conn->bg_conn_mutex); /* 只有spider_bg_action在pthread_cond_wait时才会加锁成功：1,等query;2，等再次处理结果 */
			assert(!conn->bg_conn_working);
			thd_proc_info(thd, "bg_search wait end 4");
      if (!result_list->finish_flg)
      {
        DBUG_PRINT("info",("spider bg second search"));
        if (!spider->use_pre_call || pre_next)
        {
          if (result_list->bgs_error)
          {
            pthread_mutex_unlock(&conn->bg_conn_mutex);
            DBUG_PRINT("info",("spider bg error"));
            if (result_list->bgs_error == HA_ERR_END_OF_FILE)
            {
              DBUG_PRINT("info",("spider bg current->finish_flg=%s",
                result_list->current ?
                (result_list->current->finish_flg ? "TRUE" : "FALSE") : "NULL"));
              DBUG_RETURN(0);
            }
            if (result_list->bgs_error_with_message)
              my_message(result_list->bgs_error,
                result_list->bgs_error_msg, MYF(0));
            DBUG_RETURN(result_list->bgs_error);
          }
          if (
            result_list->quick_mode == 0 ||
            !result_list->bgs_current->result
          ) {
            result_list->split_read =
              result_list->bgs_second_read > 0 ?
              result_list->bgs_second_read :
              result_list->bgs_split_read;
            result_list->limit_num =
              result_list->internal_limit - result_list->record_num >=
              result_list->split_read ?
              result_list->split_read :
              result_list->internal_limit - result_list->record_num;
            if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
            {
              if ((error_num = spider->reappend_limit_sql_part(
                result_list->internal_offset + result_list->record_num,
                result_list->limit_num,
                SPIDER_SQL_TYPE_SELECT_SQL)))
              {
                pthread_mutex_unlock(&conn->bg_conn_mutex);
                DBUG_RETURN(error_num);
              }
              if (
                !result_list->use_union &&
                (error_num = spider->append_select_lock_sql_part(
                  SPIDER_SQL_TYPE_SELECT_SQL))
              ) {
                pthread_mutex_unlock(&conn->bg_conn_mutex);
                DBUG_RETURN(error_num);
              }
            }
            if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
            {
              spider_db_append_handler_next(spider);
              if ((error_num = spider->reappend_limit_sql_part(
                0, result_list->limit_num,
                SPIDER_SQL_TYPE_HANDLER)))
              {
                pthread_mutex_unlock(&conn->bg_conn_mutex);
                DBUG_RETURN(error_num);
              }
            }
          }
          result_list->bgs_phase = 2;
        }
        result_list->bgs_working = TRUE;
        conn->bg_search = TRUE;
        if (with_lock) // 通常为0
          conn->bg_conn_chain_mutex_ptr = &first_conn->bg_conn_chain_mutex;
        conn->bg_caller_sync_wait = TRUE;
        conn->bg_target = spider;
        conn->link_idx = link_idx;
        conn->bg_discard_result = discard_result;
				thd_proc_info(thd, "bg_search wait start 5");
        pthread_mutex_lock(&conn->bg_conn_sync_mutex);
        pthread_cond_signal(&conn->bg_conn_cond); // 发信号，后台线程执行sql
        pthread_mutex_unlock(&conn->bg_conn_mutex);
        pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex); // 确定后台线程已执行sql，不是尚处于wait状态
        pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
				thd_proc_info(thd, "bg_search wait end 5");
        conn->bg_caller_sync_wait = FALSE;
      } else {
        pthread_mutex_unlock(&conn->bg_conn_mutex);
        DBUG_PRINT("info",("spider bg current->finish_flg=%s",
          result_list->current ? (result_list->current->finish_flg ? "TRUE" : "FALSE") : "NULL"));
      }
    } else {
      DBUG_PRINT("info",("spider bg current->finish_flg=%s",
        result_list->current ? (result_list->current->finish_flg ? "TRUE" : "FALSE") : "NULL"));
    }
  } else {
    DBUG_PRINT("info",("spider bg search"));
    if (result_list->current->finish_flg)
    {
      DBUG_PRINT("info",("spider bg end of file"));
      result_list->table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    if (result_list->bgs_working)
    {
      /* wait */
      DBUG_PRINT("info",("spider bg working wait"));
			thd_proc_info(thd, "bg_search wait start 7");
      pthread_mutex_lock(&conn->bg_conn_mutex);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
			thd_proc_info(thd, "bg_search wait end 7");
    }
    if (result_list->bgs_error)
    {
      DBUG_PRINT("info",("spider bg error"));
      if (result_list->bgs_error == HA_ERR_END_OF_FILE)
      {
        result_list->current = result_list->current->next;
        result_list->current_row_num = 0;
        result_list->table->status = STATUS_NOT_FOUND;
      }
      if (result_list->bgs_error_with_message)
        my_message(result_list->bgs_error,
          result_list->bgs_error_msg, MYF(0));
      DBUG_RETURN(result_list->bgs_error);
    }
    result_list->current = result_list->current->next;
    result_list->current_row_num = 0;
    if (result_list->current == result_list->bgs_current)
    {
      DBUG_PRINT("info",("spider bg next search"));
      if (!result_list->current->finish_flg)
      {
        pthread_mutex_lock(&conn->bg_conn_mutex);
        result_list->bgs_phase = 3;
        if (
          result_list->quick_mode == 0 ||
          !result_list->bgs_current->result
        ) {
          result_list->split_read = result_list->bgs_split_read;
          result_list->limit_num =
            result_list->internal_limit - result_list->record_num >=
            result_list->split_read ?
            result_list->split_read :
            result_list->internal_limit - result_list->record_num;
          if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
          {
            if ((error_num = spider->reappend_limit_sql_part(
              result_list->internal_offset + result_list->record_num,
              result_list->limit_num,
              SPIDER_SQL_TYPE_SELECT_SQL)))
            {
              pthread_mutex_unlock(&conn->bg_conn_mutex);
              DBUG_RETURN(error_num);
            }
            if (
              !result_list->use_union &&
              (error_num = spider->append_select_lock_sql_part(
                SPIDER_SQL_TYPE_SELECT_SQL))
            ) {
              pthread_mutex_unlock(&conn->bg_conn_mutex);
              DBUG_RETURN(error_num);
            }
          }
          if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
          {
            spider_db_append_handler_next(spider);
            if ((error_num = spider->reappend_limit_sql_part(
              0, result_list->limit_num,
              SPIDER_SQL_TYPE_HANDLER)))
            {
              pthread_mutex_unlock(&conn->bg_conn_mutex);
              DBUG_RETURN(error_num);
            }
          }
        }
        conn->bg_target = spider;
        conn->link_idx = link_idx;
        conn->bg_discard_result = discard_result;
        result_list->bgs_working = TRUE;
        conn->bg_search = TRUE;
        if (with_lock) // 通常为0
          conn->bg_conn_chain_mutex_ptr = &first_conn->bg_conn_chain_mutex;
        conn->bg_caller_sync_wait = TRUE;
				thd_proc_info(thd, "bg_search wait start 9");
        pthread_mutex_lock(&conn->bg_conn_sync_mutex);
        pthread_cond_signal(&conn->bg_conn_cond);
        pthread_mutex_unlock(&conn->bg_conn_mutex);
        pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
        pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
				thd_proc_info(thd, "bg_search wait end 9");
        conn->bg_caller_sync_wait = FALSE;
      }
    }
  }
  DBUG_RETURN(0);
}

void spider_bg_conn_simple_action(
  SPIDER_CONN *conn,
  uint simple_action
) { // oracle下走
  DBUG_ENTER("spider_bg_conn_simple_action");
  pthread_mutex_lock(&conn->bg_conn_mutex);
  conn->bg_caller_wait = TRUE;
  conn->bg_simple_action = simple_action;
  pthread_mutex_lock(&conn->bg_conn_sync_mutex);
  pthread_cond_signal(&conn->bg_conn_cond);
  pthread_mutex_unlock(&conn->bg_conn_mutex);
  pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
  pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
  conn->bg_caller_wait = FALSE;
  DBUG_VOID_RETURN;
}

void *spider_bg_conn_action(
  void *arg
) {
  int error_num;
  SPIDER_CONN *conn = (SPIDER_CONN*) arg;
  SPIDER_TRX *trx;
  ha_spider *spider;
  SPIDER_RESULT_LIST *result_list;
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_conn_action");
  /* init start */
  if (!(thd = new THD()))
  {
    pthread_mutex_lock(&conn->bg_conn_sync_mutex);
    pthread_cond_signal(&conn->bg_conn_sync_cond);
    pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id = (*spd_db_att_thread_id)++;
  pthread_mutex_unlock(&LOCK_thread_count);
#ifdef HAVE_PSI_INTERFACE
  mysql_thread_set_psi_id(thd->thread_id);
#endif
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  if (!(trx = spider_get_trx(thd, FALSE, &error_num)))
  {
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    pthread_mutex_lock(&conn->bg_conn_sync_mutex);
    pthread_cond_signal(&conn->bg_conn_sync_cond);
    pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  /* lex_start(thd); */
  conn->bg_thd = thd;
  pthread_mutex_lock(&conn->bg_conn_mutex);
  pthread_mutex_lock(&conn->bg_conn_sync_mutex);
  pthread_cond_signal(&conn->bg_conn_sync_cond); // 线程创建成功
  conn->bg_init = TRUE;
  pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
  /* init end */

  while (TRUE)
  {
    if (conn->bg_conn_chain_mutex_ptr)
    {
      pthread_mutex_unlock(conn->bg_conn_chain_mutex_ptr);
      conn->bg_conn_chain_mutex_ptr = NULL;
    }
    thd->clear_error();
		conn->bg_conn_working = false;
    pthread_cond_wait(&conn->bg_conn_cond, &conn->bg_conn_mutex); /* 等待主线程query语句。 或者query执行完了，等主线和处理结果 */
		conn->bg_conn_working = true;
    DBUG_PRINT("info",("spider bg roop start"));
    if (conn->bg_caller_sync_wait)
    { /* bg_serch发过signal信号后，告诉主线程子线程开始工作 */
      pthread_mutex_lock(&conn->bg_conn_sync_mutex);
      if (conn->bg_direct_sql) /* udf,不走 */
        conn->bg_get_job_stack_off = TRUE;
      pthread_cond_signal(&conn->bg_conn_sync_cond); /* 发出线程已响应query的信号 */
      pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
      if (conn->bg_conn_chain_mutex_ptr)
      { /* 通常为0. 另外，tspider中，同一分片只有一个conn */
        pthread_mutex_lock(conn->bg_conn_chain_mutex_ptr);
        if ((&conn->bg_conn_chain_mutex) != conn->bg_conn_chain_mutex_ptr)
        {
          pthread_mutex_unlock(conn->bg_conn_chain_mutex_ptr);
          conn->bg_conn_chain_mutex_ptr = NULL;
        }
      }
    }
    if (conn->bg_kill)
    { // 释放conn时条件为真
      DBUG_PRINT("info",("spider bg kill start"));
      if (conn->bg_conn_chain_mutex_ptr)
      {
        pthread_mutex_unlock(conn->bg_conn_chain_mutex_ptr);
        conn->bg_conn_chain_mutex_ptr = NULL;
      }
      spider_free_trx(trx, TRUE);
      /* lex_end(thd->lex); */
      delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
      set_current_thd(NULL);
#endif
      pthread_mutex_lock(&conn->bg_conn_sync_mutex);
      pthread_cond_signal(&conn->bg_conn_sync_cond);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
      my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (conn->bg_get_job_stack)
    {// udf才走的逻辑
      conn->bg_get_job_stack = FALSE;
      if (!spider_bg_conn_get_job(conn))
      {
        conn->bg_direct_sql = FALSE;
      }
    }
    if (conn->bg_search)
    {
      SPIDER_SHARE *share;
      spider_db_handler *dbton_handler;
      DBUG_PRINT("info",("spider bg search start"));
	//		thd_proc_info(0, "spider bg start");
      spider = (ha_spider*) conn->bg_target;
      share = spider->share;
      dbton_handler = spider->dbton_handler[conn->dbton_id];
      result_list = &spider->result_list;
      result_list->bgs_error = 0;
      result_list->bgs_error_with_message = FALSE;
      if (
        result_list->quick_mode == 0 ||
        result_list->bgs_phase == 1 ||
        !result_list->bgs_current->result
      ) {
        ulong sql_type;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
        {
#endif
          if (spider->sql_kind[conn->link_idx] == SPIDER_SQL_KIND_SQL)
          {
            sql_type = SPIDER_SQL_TYPE_SELECT_SQL | SPIDER_SQL_TYPE_TMP_SQL;
          } else {
            sql_type = SPIDER_SQL_TYPE_HANDLER;
          }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        } else {
          sql_type = SPIDER_SQL_TYPE_SELECT_HS;
        }
#endif
	//			thd_proc_info(0, "spider bg execute query");
        if (dbton_handler->need_lock_before_set_sql_for_exec(sql_type))
        {// false, 不走
          spider_mta_conn_mutex_lock(conn);
        }
        if ((error_num = dbton_handler->set_sql_for_exec(sql_type,
          conn->link_idx)))
        {
          result_list->bgs_error = error_num;
          if ((result_list->bgs_error_with_message = thd->is_error()))
            strmov(result_list->bgs_error_msg, spider_stmt_da_message(thd));
        }
        if (!dbton_handler->need_lock_before_set_sql_for_exec(sql_type))
        {
          spider_mta_conn_mutex_lock(conn);
        }
        sql_type &= ~SPIDER_SQL_TYPE_TMP_SQL;
        DBUG_PRINT("info",("spider sql_type=%lu", sql_type));
#ifdef HA_CAN_BULK_ACCESS
        if (spider->is_bulk_access_clone)
        {
          spider->connection_ids[conn->link_idx] = conn->connection_id;
          spider_trx_add_bulk_access_conn(spider->trx, conn);
        }
#endif
        if (!result_list->bgs_error)
        {
          conn->need_mon = &spider->need_mons[conn->link_idx];
          conn->mta_conn_mutex_lock_already = TRUE;
          conn->mta_conn_mutex_unlock_later = TRUE;
#ifdef HA_CAN_BULK_ACCESS
          if (!spider->is_bulk_access_clone)
          {
#endif
            if (!(result_list->bgs_error =
              spider_db_set_names(spider, conn, conn->link_idx)))
            {
              if (
                result_list->tmp_table_join && spider->bka_mode != 2 &&
                spider_bit_is_set(result_list->tmp_table_join_first,
                  conn->link_idx)
              ) {
                spider_clear_bit(result_list->tmp_table_join_first,
                  conn->link_idx);
                spider_set_bit(result_list->tmp_table_created,
                  conn->link_idx);
                result_list->tmp_tables_created = TRUE;
                spider_conn_set_timeout_from_share(conn, conn->link_idx,
                  spider->trx->thd, share);
                if (dbton_handler->execute_sql(
                  SPIDER_SQL_TYPE_TMP_SQL,
                  conn,
                  -1,
                  &spider->need_mons[conn->link_idx])
                ) {
                  result_list->bgs_error = spider_db_errorno(conn);
                  if ((result_list->bgs_error_with_message = thd->is_error()))
                    strmov(result_list->bgs_error_msg,
                      spider_stmt_da_message(thd));
                } else
                  spider_db_discard_multiple_result(spider, conn->link_idx, conn);
              }
              if (!result_list->bgs_error)
              {
                spider_conn_set_timeout_from_share(conn, conn->link_idx,
                  spider->trx->thd, share);
                if (dbton_handler->execute_sql(
                  sql_type,
                  conn,
                  result_list->quick_mode,
                  &spider->need_mons[conn->link_idx])
                ) {
                  result_list->bgs_error = spider_db_errorno(conn);
                  if ((result_list->bgs_error_with_message = thd->is_error()))
                    strmov(result_list->bgs_error_msg,
                      spider_stmt_da_message(thd));
                } else {
                  spider->connection_ids[conn->link_idx] = conn->connection_id;
                  if (!conn->bg_discard_result)
                  {
                    if (!(result_list->bgs_error =
                      spider_db_store_result(spider, conn->link_idx,
                        result_list->table)))
                      spider->result_link_idx = conn->link_idx;
                    else {
                      if ((result_list->bgs_error_with_message =
                        thd->is_error()))
                        strmov(result_list->bgs_error_msg,
                          spider_stmt_da_message(thd));
                    }
                  } else {
                    result_list->bgs_error = 0;
                    spider_db_discard_result(spider, conn->link_idx, conn);
                  }
                }
              }
            } else {
              if ((result_list->bgs_error_with_message = thd->is_error()))
                strmov(result_list->bgs_error_msg,
                  spider_stmt_da_message(thd));
            }
#ifdef HA_CAN_BULK_ACCESS
          }
#endif
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          spider_mta_conn_mutex_unlock(conn);
        } else {
          spider_mta_conn_mutex_unlock(conn);
        }
      } else {/* 一次没取完结果，继续fetch走此逻辑 */
        spider->connection_ids[conn->link_idx] = conn->connection_id;
        conn->mta_conn_mutex_unlock_later = TRUE;
        result_list->bgs_error =
          spider_db_store_result(spider, conn->link_idx, result_list->table);
        if ((result_list->bgs_error_with_message = thd->is_error()))
          strmov(result_list->bgs_error_msg, spider_stmt_da_message(thd));
        conn->mta_conn_mutex_unlock_later = FALSE;
      }
      conn->bg_search = FALSE;
      result_list->bgs_working = FALSE;
      if (conn->bg_caller_wait)
      {
        pthread_mutex_lock(&conn->bg_conn_sync_mutex);
        pthread_cond_signal(&conn->bg_conn_sync_cond);
        pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
      }
//			thd_proc_info(0, "spider bg end");
      continue; // 后面的逻辑都不会走呀
    }
    if (conn->bg_direct_sql)
    { // udf，不走
      bool is_error = FALSE;
      DBUG_PRINT("info",("spider bg direct sql start"));
      do {
        SPIDER_DIRECT_SQL *direct_sql = (SPIDER_DIRECT_SQL *) conn->bg_target;
        if (
          (error_num = spider_db_udf_direct_sql(direct_sql))
        ) {
          if (thd->is_error())
          {
            SPIDER_BG_DIRECT_SQL *bg_direct_sql =
              (SPIDER_BG_DIRECT_SQL *) direct_sql->parent;
            pthread_mutex_lock(direct_sql->bg_mutex);
            bg_direct_sql->bg_error = spider_stmt_da_sql_errno(thd);
            strmov((char *) bg_direct_sql->bg_error_msg,
              spider_stmt_da_message(thd));
            pthread_mutex_unlock(direct_sql->bg_mutex);
            is_error = TRUE;
          }
        }
        if (direct_sql->modified_non_trans_table)
        {
          SPIDER_BG_DIRECT_SQL *bg_direct_sql =
            (SPIDER_BG_DIRECT_SQL *) direct_sql->parent;
          pthread_mutex_lock(direct_sql->bg_mutex);
          bg_direct_sql->modified_non_trans_table = TRUE;
          pthread_mutex_unlock(direct_sql->bg_mutex);
        }
        spider_udf_free_direct_sql_alloc(direct_sql, TRUE);
      } while (!is_error && spider_bg_conn_get_job(conn));
      if (is_error)
      {
        while (spider_bg_conn_get_job(conn))
          spider_udf_free_direct_sql_alloc(
            (SPIDER_DIRECT_SQL *) conn->bg_target, TRUE);
      }
      conn->bg_direct_sql = FALSE;
      continue;
    }
    if (conn->bg_exec_sql)
    { // udf，不走
      DBUG_PRINT("info",("spider bg exec sql start"));
      spider = (ha_spider*) conn->bg_target;
      *conn->bg_error_num = spider_db_query_with_set_names(
        conn->bg_sql_type,
        spider,
        conn,
        conn->link_idx
      );
      conn->bg_exec_sql = FALSE;
      continue;
    }
    if (conn->bg_simple_action)
    {// oracle下走
      switch (conn->bg_simple_action)
      {
        case SPIDER_BG_SIMPLE_CONNECT:
          conn->db_conn->bg_connect();
          break;
        case SPIDER_BG_SIMPLE_DISCONNECT:
          conn->db_conn->bg_disconnect();
          break;
        default:
          break;
      }
      conn->bg_simple_action = SPIDER_BG_SIMPLE_NO_ACTION;
      if (conn->bg_caller_wait)
      {
        pthread_mutex_lock(&conn->bg_conn_sync_mutex);
        pthread_cond_signal(&conn->bg_conn_sync_cond);
        pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
      }
      continue;
    }
    if (conn->bg_break)
    {
      DBUG_PRINT("info",("spider bg break start"));
      spider = (ha_spider*) conn->bg_target;
      result_list = &spider->result_list;
      result_list->bgs_working = FALSE;
      continue;
    }
  }
}

int spider_create_sts_thread(
  SPIDER_SHARE *share
) {
  int error_num;
  DBUG_ENTER("spider_create_sts_thread");
  if (!share->bg_sts_init)
  {
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&share->bg_sts_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_sts,
      &share->bg_sts_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&share->bg_sts_sync_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_sts_sync,
      &share->bg_sts_sync_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_cond_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_create(&share->bg_sts_thread, &spider_pt_attr,
      spider_bg_sts_action, (void *) share)
    )
#else
    if (mysql_thread_create(spd_key_thd_bg_sts, &share->bg_sts_thread,
      &spider_pt_attr, spider_bg_sts_action, (void *) share)
    )
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    share->bg_sts_init = TRUE;
  }
  DBUG_RETURN(0);

error_thread_create:
  pthread_cond_destroy(&share->bg_sts_sync_cond);
error_sync_cond_init:
  pthread_cond_destroy(&share->bg_sts_cond);
error_cond_init:
  DBUG_RETURN(error_num);
}

void spider_free_sts_thread(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_sts_thread");
  if (share->bg_sts_init)
  {
    pthread_mutex_lock(&share->sts_mutex);
    share->bg_sts_kill = TRUE;
    pthread_cond_signal(&share->bg_sts_cond);
    pthread_cond_wait(&share->bg_sts_sync_cond, &share->sts_mutex);
    pthread_mutex_unlock(&share->sts_mutex);
    pthread_join(share->bg_sts_thread, NULL);
    pthread_cond_destroy(&share->bg_sts_sync_cond);
    pthread_cond_destroy(&share->bg_sts_cond);
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_sts_action(
  void *arg
) {
  SPIDER_SHARE *share = (SPIDER_SHARE*) arg;
  SPIDER_TRX *trx;
  int error_num = 0, roop_count;
  ha_spider spider;
#ifdef _MSC_VER
  int *need_mons;
  SPIDER_CONN **conns;
  uint *conn_link_idx;
  uchar *conn_can_fo;
  char **conn_keys;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  char **hs_r_conn_keys;
  char **hs_w_conn_keys;
#endif
  spider_db_handler **dbton_hdl;
#else
  int need_mons[share->link_count];
  SPIDER_CONN *conns[share->link_count];
  uint conn_link_idx[share->link_count];
  uchar conn_can_fo[share->link_bitmap_size];
  char *conn_keys[share->link_count];
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  char *hs_r_conn_keys[share->link_count];
  char *hs_w_conn_keys[share->link_count];
#endif
  spider_db_handler *dbton_hdl[SPIDER_DBTON_SIZE];
#endif
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_sts_action");
  /* init start */
#ifdef _MSC_VER
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (!(need_mons = (int *)
    spider_bulk_malloc(spider_current_trx, 21, MYF(MY_WME),
      &need_mons, sizeof(int) * share->link_count,
      &conns, sizeof(SPIDER_CONN *) * share->link_count,
      &conn_link_idx, sizeof(uint) * share->link_count,
      &conn_can_fo, sizeof(uchar) * share->link_bitmap_size,
      &conn_keys, sizeof(char *) * share->link_count,
      &hs_r_conn_keys, sizeof(char *) * share->link_count,
      &hs_w_conn_keys, sizeof(char *) * share->link_count,
      &dbton_hdl, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE,
      NullS))
  )
#else
  if (!(need_mons = (int *)
    spider_bulk_malloc(spider_current_trx, 21, MYF(MY_WME),
      &need_mons, sizeof(int) * share->link_count,
      &conns, sizeof(SPIDER_CONN *) * share->link_count,
      &conn_link_idx, sizeof(uint) * share->link_count,
      &conn_can_fo, sizeof(uchar) * share->link_bitmap_size,
      &conn_keys, sizeof(char *) * share->link_count,
      &dbton_hdl, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE,
      NullS))
  )
#endif
  {
    pthread_mutex_lock(&share->sts_mutex);
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
    pthread_mutex_unlock(&share->sts_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
#endif
  pthread_mutex_lock(&share->sts_mutex);
  if (!(thd = new THD()))
  {
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
    pthread_mutex_unlock(&share->sts_mutex);
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id = (*spd_db_att_thread_id)++;
  pthread_mutex_unlock(&LOCK_thread_count);
#ifdef HAVE_PSI_INTERFACE
  mysql_thread_set_psi_id(thd->thread_id);
#endif
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  if (!(trx = spider_get_trx(thd, FALSE, &error_num)))
  {
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
    pthread_mutex_unlock(&share->sts_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  share->bg_sts_thd = thd;
/*
  spider.trx = spider_global_trx;
*/
  spider.trx = trx;
  spider.share = share;
  spider.conns = conns;
  spider.conn_link_idx = conn_link_idx;
  spider.conn_can_fo = conn_can_fo;
  spider.need_mons = need_mons;
  spider.conn_keys_first_ptr = share->conn_keys[0];
  spider.conn_keys = conn_keys;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  spider.hs_r_conn_keys = hs_r_conn_keys;
  spider.hs_w_conn_keys = hs_w_conn_keys;
#endif
  spider.dbton_handler = dbton_hdl;
  memset(conns, 0, sizeof(SPIDER_CONN *) * share->link_count);
  memset(need_mons, 0, sizeof(int) * share->link_count);
  memset(dbton_hdl, 0, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE);
  spider_trx_set_link_idx_for_all(&spider);
  spider.search_link_idx = spider_conn_first_link_idx(thd,
    share->link_statuses, share->access_balances, spider.conn_link_idx,
    share->link_count, SPIDER_LINK_STATUS_OK);
  for (roop_count = 0; roop_count < SPIDER_DBTON_SIZE; roop_count++)
  {
    if (
      spider_bit_is_set(share->dbton_bitmap, roop_count) &&
      spider_dbton[roop_count].create_db_handler
    ) {
      if ((dbton_hdl[roop_count] = spider_dbton[roop_count].create_db_handler(
        &spider, share->dbton_share[roop_count])))
        break;
      if (dbton_hdl[roop_count]->init())
        break;
    }
  }
  if (roop_count == SPIDER_DBTON_SIZE)
  {
    DBUG_PRINT("info",("spider handler init error"));
    for (roop_count = SPIDER_DBTON_SIZE - 1; roop_count >= 0; --roop_count)
    {
      if (
        spider_bit_is_set(share->dbton_bitmap, roop_count) &&
        dbton_hdl[roop_count]
      ) {
        delete dbton_hdl[roop_count];
        dbton_hdl[roop_count] = NULL;
      }
    }
    spider_free_trx(trx, TRUE);
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
    pthread_mutex_unlock(&share->sts_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg sts roop start"));
    if (share->bg_sts_kill)
    {
      DBUG_PRINT("info",("spider bg sts kill start"));
      for (roop_count = SPIDER_DBTON_SIZE - 1; roop_count >= 0; --roop_count)
      {
        if (
          spider_bit_is_set(share->dbton_bitmap, roop_count) &&
          dbton_hdl[roop_count]
        ) {
          delete dbton_hdl[roop_count];
          dbton_hdl[roop_count] = NULL;
        }
      }
      spider_free_trx(trx, TRUE);
      delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
      set_current_thd(NULL);
#endif
      pthread_cond_signal(&share->bg_sts_sync_cond);
      pthread_mutex_unlock(&share->sts_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
      my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
      my_thread_end();
#ifdef _MSC_VER
      spider_free(NULL, need_mons, MYF(MY_WME));
#endif
      DBUG_RETURN(NULL);
    }
    if (spider.search_link_idx == -1)
    {
      spider_trx_set_link_idx_for_all(&spider);
      spider.search_link_idx = spider_conn_next_link_idx(
        thd, share->link_statuses, share->access_balances,
        spider.conn_link_idx, spider.search_link_idx, share->link_count,
        SPIDER_LINK_STATUS_OK);
    }
    if (spider.search_link_idx >= 0)
    {
      if (difftime(share->bg_sts_try_time, share->sts_get_time) >=
        share->bg_sts_interval)
      {
        if (!conns[spider.search_link_idx])
        {
          pthread_mutex_lock(&spider_global_trx_mutex);
          spider_get_conn(share, spider.search_link_idx,
            share->conn_keys[spider.search_link_idx],
            spider_global_trx, &spider, FALSE, FALSE, SPIDER_CONN_KIND_MYSQL,
            &error_num);
          conns[spider.search_link_idx]->error_mode = 0;
          pthread_mutex_unlock(&spider_global_trx_mutex);
          if (
            error_num &&
            share->monitoring_kind[spider.search_link_idx] &&
            need_mons[spider.search_link_idx]
          ) {
            lex_start(thd);
            error_num = spider_ping_table_mon_from_table(
                spider_global_trx,
                thd,
                share,
                (uint32) share->monitoring_sid[spider.search_link_idx],
                share->table_name,
                share->table_name_length,
                spider.conn_link_idx[spider.search_link_idx],
                NULL,
                0,
                share->monitoring_kind[spider.search_link_idx],
                share->monitoring_limit[spider.search_link_idx],
                TRUE
              );
            lex_end(thd->lex);
          }
        }
        if (conns[spider.search_link_idx])
        {
#ifdef WITH_PARTITION_STORAGE_ENGINE
          if (spider_get_sts(share, spider.search_link_idx,
            share->bg_sts_try_time, &spider,
            share->bg_sts_interval, share->bg_sts_mode,
            share->bg_sts_sync,
            2, HA_STATUS_CONST | HA_STATUS_VARIABLE))
#else
          if (spider_get_sts(share, spider.search_link_idx,
            share->bg_sts_try_time, &spider,
            share->bg_sts_interval, share->bg_sts_mode,
            2, HA_STATUS_CONST | HA_STATUS_VARIABLE))
#endif
          {
            if (
              share->monitoring_kind[spider.search_link_idx] &&
              need_mons[spider.search_link_idx]
            ) {
              lex_start(thd);
              error_num = spider_ping_table_mon_from_table(
                  spider_global_trx,
                  thd,
                  share,
                  (uint32) share->monitoring_sid[spider.search_link_idx],
                  share->table_name,
                  share->table_name_length,
                  spider.conn_link_idx[spider.search_link_idx],
                  NULL,
                  0,
                  share->monitoring_kind[spider.search_link_idx],
                  share->monitoring_limit[spider.search_link_idx],
                  TRUE
                );
              lex_end(thd->lex);
            }
            spider.search_link_idx = -1;
          }
        }
      }
    }
    memset(need_mons, 0, sizeof(int) * share->link_count);
    share->bg_sts_thd_wait = TRUE;
    pthread_cond_wait(&share->bg_sts_cond, &share->sts_mutex);
  }
}

int spider_create_crd_thread(
  SPIDER_SHARE *share
) {
  int error_num;
  DBUG_ENTER("spider_create_crd_thread");
  if (!share->bg_crd_init)
  {
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&share->bg_crd_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_crd,
      &share->bg_crd_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_cond_init(&share->bg_crd_sync_cond, NULL))
#else
    if (mysql_cond_init(spd_key_cond_bg_crd_sync,
      &share->bg_crd_sync_cond, NULL))
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_cond_init;
    }
#if MYSQL_VERSION_ID < 50500
    if (pthread_create(&share->bg_crd_thread, &spider_pt_attr,
      spider_bg_crd_action, (void *) share)
    )
#else
    if (mysql_thread_create(spd_key_thd_bg_crd, &share->bg_crd_thread,
      &spider_pt_attr, spider_bg_crd_action, (void *) share)
    )
#endif
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    share->bg_crd_init = TRUE;
  }
  DBUG_RETURN(0);

error_thread_create:
  pthread_cond_destroy(&share->bg_crd_sync_cond);
error_sync_cond_init:
  pthread_cond_destroy(&share->bg_crd_cond);
error_cond_init:
  DBUG_RETURN(error_num);
}

void spider_free_crd_thread(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_crd_thread");
  if (share->bg_crd_init)
  {
    pthread_mutex_lock(&share->crd_mutex);
    share->bg_crd_kill = TRUE;
    pthread_cond_signal(&share->bg_crd_cond);
    pthread_cond_wait(&share->bg_crd_sync_cond, &share->crd_mutex);
    pthread_mutex_unlock(&share->crd_mutex);
    pthread_join(share->bg_crd_thread, NULL);
    pthread_cond_destroy(&share->bg_crd_sync_cond);
    pthread_cond_destroy(&share->bg_crd_cond);
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_crd_action(
  void *arg
) {
  SPIDER_SHARE *share = (SPIDER_SHARE*) arg;
  SPIDER_TRX *trx;
  int error_num = 0, roop_count;
  ha_spider spider;
  TABLE table;
#ifdef _MSC_VER
  int *need_mons;
  SPIDER_CONN **conns;
  uint *conn_link_idx;
  uchar *conn_can_fo;
  char **conn_keys;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  char **hs_r_conn_keys;
  char **hs_w_conn_keys;
#endif
  spider_db_handler **dbton_hdl;
#else
  int need_mons[share->link_count];
  SPIDER_CONN *conns[share->link_count];
  uint conn_link_idx[share->link_count];
  uchar conn_can_fo[share->link_bitmap_size];
  char *conn_keys[share->link_count];
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  char *hs_r_conn_keys[share->link_count];
  char *hs_w_conn_keys[share->link_count];
#endif
  spider_db_handler *dbton_hdl[SPIDER_DBTON_SIZE];
#endif
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_crd_action");
  /* init start */
#ifdef _MSC_VER
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (!(need_mons = (int *)
    spider_bulk_malloc(spider_current_trx, 22, MYF(MY_WME),
      &need_mons, sizeof(int) * share->link_count,
      &conns, sizeof(SPIDER_CONN *) * share->link_count,
      &conn_link_idx, sizeof(uint) * share->link_count,
      &conn_can_fo, sizeof(uchar) * share->link_bitmap_size,
      &conn_keys, sizeof(char *) * share->link_count,
      &hs_r_conn_keys, sizeof(char *) * share->link_count,
      &hs_w_conn_keys, sizeof(char *) * share->link_count,
      &dbton_hdl, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE,
      NullS))
  )
#else
  if (!(need_mons = (int *)
    spider_bulk_malloc(spider_current_trx, 22, MYF(MY_WME),
      &need_mons, sizeof(int) * share->link_count,
      &conns, sizeof(SPIDER_CONN *) * share->link_count,
      &conn_link_idx, sizeof(uint) * share->link_count,
      &conn_can_fo, sizeof(uchar) * share->link_bitmap_size,
      &conn_keys, sizeof(char *) * share->link_count,
      &dbton_hdl, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE,
      NullS))
  )
#endif
  {
    pthread_mutex_lock(&share->crd_mutex);
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
    pthread_mutex_unlock(&share->crd_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
#endif
  pthread_mutex_lock(&share->crd_mutex);
  if (!(thd = new THD()))
  {
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
    pthread_mutex_unlock(&share->crd_mutex);
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id = (*spd_db_att_thread_id)++;
  pthread_mutex_unlock(&LOCK_thread_count);
#ifdef HAVE_PSI_INTERFACE
  mysql_thread_set_psi_id(thd->thread_id);
#endif
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  if (!(trx = spider_get_trx(thd, FALSE, &error_num)))
  {
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
    pthread_mutex_unlock(&share->crd_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  share->bg_crd_thd = thd;
  table.s = share->table_share;
  table.field = share->table_share->field;
  table.key_info = share->table_share->key_info;
/*
  spider.trx = spider_global_trx;
*/
  spider.trx = trx;
  spider.change_table_ptr(&table, share->table_share);
  spider.share = share;
  spider.conns = conns;
  spider.conn_link_idx = conn_link_idx;
  spider.conn_can_fo = conn_can_fo;
  spider.need_mons = need_mons;
  spider.conn_keys_first_ptr = share->conn_keys[0];
  spider.conn_keys = conn_keys;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  spider.hs_r_conn_keys = hs_r_conn_keys;
  spider.hs_w_conn_keys = hs_w_conn_keys;
#endif
  spider.dbton_handler = dbton_hdl;
  memset(conns, 0, sizeof(SPIDER_CONN *) * share->link_count);
  memset(need_mons, 0, sizeof(int) * share->link_count);
  memset(dbton_hdl, 0, sizeof(spider_db_handler *) * SPIDER_DBTON_SIZE);
  spider_trx_set_link_idx_for_all(&spider);
  spider.search_link_idx = spider_conn_first_link_idx(thd,
    share->link_statuses, share->access_balances, spider.conn_link_idx,
    share->link_count, SPIDER_LINK_STATUS_OK);
  for (roop_count = 0; roop_count < SPIDER_DBTON_SIZE; roop_count++)
  {
    if (
      spider_bit_is_set(share->dbton_bitmap, roop_count) &&
      spider_dbton[roop_count].create_db_handler
    ) {
      if ((dbton_hdl[roop_count] = spider_dbton[roop_count].create_db_handler(
        &spider, share->dbton_share[roop_count])))
        break;
      if (dbton_hdl[roop_count]->init())
        break;
    }
  }
  if (roop_count == SPIDER_DBTON_SIZE)
  {
    DBUG_PRINT("info",("spider handler init error"));
    for (roop_count = SPIDER_DBTON_SIZE - 1; roop_count >= 0; --roop_count)
    {
      if (
        spider_bit_is_set(share->dbton_bitmap, roop_count) &&
        dbton_hdl[roop_count]
      ) {
        delete dbton_hdl[roop_count];
        dbton_hdl[roop_count] = NULL;
      }
    }
    spider_free_trx(trx, TRUE);
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
    pthread_mutex_unlock(&share->crd_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
#ifdef _MSC_VER
    spider_free(NULL, need_mons, MYF(MY_WME));
#endif
    DBUG_RETURN(NULL);
  }
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg crd roop start"));
    if (share->bg_crd_kill)
    {
      DBUG_PRINT("info",("spider bg crd kill start"));
      for (roop_count = SPIDER_DBTON_SIZE - 1; roop_count >= 0; --roop_count)
      {
        if (
          spider_bit_is_set(share->dbton_bitmap, roop_count) &&
          dbton_hdl[roop_count]
        ) {
          delete dbton_hdl[roop_count];
          dbton_hdl[roop_count] = NULL;
        }
      }
      spider_free_trx(trx, TRUE);
      delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
      set_current_thd(NULL);
#endif
      pthread_cond_signal(&share->bg_crd_sync_cond);
      pthread_mutex_unlock(&share->crd_mutex);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
      my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
      my_thread_end();
#ifdef _MSC_VER
      spider_free(NULL, need_mons, MYF(MY_WME));
#endif
      DBUG_RETURN(NULL);
    }
    if (spider.search_link_idx == -1)
    {
      spider_trx_set_link_idx_for_all(&spider);
      spider.search_link_idx = spider_conn_next_link_idx(
        thd, share->link_statuses, share->access_balances,
        spider.conn_link_idx, spider.search_link_idx, share->link_count,
        SPIDER_LINK_STATUS_OK);
    }
    if (spider.search_link_idx >= 0)
    {
      if (difftime(share->bg_crd_try_time, share->crd_get_time) >=
        share->bg_crd_interval)
      {
        if (!conns[spider.search_link_idx])
        {
          pthread_mutex_lock(&spider_global_trx_mutex);
          spider_get_conn(share, spider.search_link_idx,
            share->conn_keys[spider.search_link_idx],
            spider_global_trx, &spider, FALSE, FALSE, SPIDER_CONN_KIND_MYSQL,
            &error_num);
          conns[spider.search_link_idx]->error_mode = 0;
          pthread_mutex_unlock(&spider_global_trx_mutex);
          if (
            error_num &&
            share->monitoring_kind[spider.search_link_idx] &&
            need_mons[spider.search_link_idx]
          ) {
            lex_start(thd);
            error_num = spider_ping_table_mon_from_table(
                spider_global_trx,
                thd,
                share,
                (uint32) share->monitoring_sid[spider.search_link_idx],
                share->table_name,
                share->table_name_length,
                spider.conn_link_idx[spider.search_link_idx],
                NULL,
                0,
                share->monitoring_kind[spider.search_link_idx],
                share->monitoring_limit[spider.search_link_idx],
                TRUE
              );
            lex_end(thd->lex);
          }
        }
        if (conns[spider.search_link_idx])
        {
#ifdef WITH_PARTITION_STORAGE_ENGINE
          if (spider_get_crd(share, spider.search_link_idx,
            share->bg_crd_try_time, &spider, &table,
            share->bg_crd_interval, share->bg_crd_mode,
            share->bg_crd_sync,
            2))
#else
          if (spider_get_crd(share, spider.search_link_idx,
            share->bg_crd_try_time, &spider, &table,
            share->bg_crd_interval, share->bg_crd_mode,
            2))
#endif
          {
            if (
              share->monitoring_kind[spider.search_link_idx] &&
              need_mons[spider.search_link_idx]
            ) {
              lex_start(thd);
              error_num = spider_ping_table_mon_from_table(
                  spider_global_trx,
                  thd,
                  share,
                  (uint32) share->monitoring_sid[spider.search_link_idx],
                  share->table_name,
                  share->table_name_length,
                  spider.conn_link_idx[spider.search_link_idx],
                  NULL,
                  0,
                  share->monitoring_kind[spider.search_link_idx],
                  share->monitoring_limit[spider.search_link_idx],
                  TRUE
                );
              lex_end(thd->lex);
            }
            spider.search_link_idx = -1;
          }
        }
      }
    }
    memset(need_mons, 0, sizeof(int) * share->link_count);
    share->bg_crd_thd_wait = TRUE;
    pthread_cond_wait(&share->bg_crd_cond, &share->crd_mutex);
  }
}

int spider_create_mon_threads(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share
) {
  bool create_bg_mons = FALSE;
  int error_num, roop_count, roop_count2;
  SPIDER_LINK_PACK link_pack;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  DBUG_ENTER("spider_create_mon_threads");
  if (!share->bg_mon_init)
  {
    for (roop_count = 0; roop_count < (int) share->all_link_count;
      roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
      {
        create_bg_mons = TRUE;
        break;
      }
    }
    if (create_bg_mons)
    {
      char link_idx_str[SPIDER_SQL_INT_LEN];
      int link_idx_str_length;
#ifdef _MSC_VER
      spider_string conv_name_str(share->table_name_length +
        SPIDER_SQL_INT_LEN + 1);
      conv_name_str.set_charset(system_charset_info);
#else
      char buf[share->table_name_length + SPIDER_SQL_INT_LEN + 1];
      spider_string conv_name_str(buf, share->table_name_length +
        SPIDER_SQL_INT_LEN + 1, system_charset_info);
#endif
      conv_name_str.init_calc_mem(105);
      conv_name_str.length(0);
      conv_name_str.q_append(share->table_name, share->table_name_length);
      for (roop_count = 0; roop_count < (int) share->all_link_count;
        roop_count++)
      {
        if (share->monitoring_bg_kind[roop_count])
        {
          conv_name_str.length(share->table_name_length);
          link_idx_str_length = my_sprintf(link_idx_str, (link_idx_str,
            "%010d", roop_count));
          conv_name_str.q_append(link_idx_str, link_idx_str_length + 1);
          conv_name_str.length(conv_name_str.length() - 1);
          if (!(table_mon_list = spider_get_ping_table_mon_list(trx, trx->thd,
            &conv_name_str, share->table_name_length, roop_count,
            (uint32) share->monitoring_sid[roop_count], FALSE, &error_num)))
            goto error_get_ping_table_mon_list;
          spider_free_ping_table_mon_list(table_mon_list);
        }
      }
      if (!(share->bg_mon_thds = (THD **)
        spider_bulk_malloc(spider_current_trx, 23, MYF(MY_WME | MY_ZEROFILL),
          &share->bg_mon_thds, sizeof(THD *) * share->all_link_count,
          &share->bg_mon_threads, sizeof(pthread_t) * share->all_link_count,
          &share->bg_mon_mutexes, sizeof(pthread_mutex_t) *
            share->all_link_count,
          &share->bg_mon_conds, sizeof(pthread_cond_t) * share->all_link_count,
          NullS))
      ) {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error_alloc_base;
      }
      for (roop_count = 0; roop_count < (int) share->all_link_count;
        roop_count++)
      {
        if (
          share->monitoring_bg_kind[roop_count] &&
#if MYSQL_VERSION_ID < 50500
          pthread_mutex_init(&share->bg_mon_mutexes[roop_count],
            MY_MUTEX_INIT_FAST)
#else
          mysql_mutex_init(spd_key_mutex_bg_mon,
            &share->bg_mon_mutexes[roop_count], MY_MUTEX_INIT_FAST)
#endif
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_mutex_init;
        }
      }
      for (roop_count = 0; roop_count < (int) share->all_link_count;
        roop_count++)
      {
        if (
          share->monitoring_bg_kind[roop_count] &&
#if MYSQL_VERSION_ID < 50500
          pthread_cond_init(&share->bg_mon_conds[roop_count], NULL)
#else
          mysql_cond_init(spd_key_cond_bg_mon,
            &share->bg_mon_conds[roop_count], NULL)
#endif
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_cond_init;
        }
      }
      link_pack.share = share;
      for (roop_count = 0; roop_count < (int) share->all_link_count;
        roop_count++)
      {
        if (share->monitoring_bg_kind[roop_count])
        {
          link_pack.link_idx = roop_count;
          pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
#if MYSQL_VERSION_ID < 50500
          if (pthread_create(&share->bg_mon_threads[roop_count],
            &spider_pt_attr, spider_bg_mon_action, (void *) &link_pack)
          )
#else
          if (mysql_thread_create(spd_key_thd_bg_mon,
            &share->bg_mon_threads[roop_count], &spider_pt_attr,
            spider_bg_mon_action, (void *) &link_pack)
          )
#endif
          {
            error_num = HA_ERR_OUT_OF_MEM;
            goto error_thread_create;
          }
          pthread_cond_wait(&share->bg_mon_conds[roop_count],
            &share->bg_mon_mutexes[roop_count]);
          pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
        }
      }
      share->bg_mon_init = TRUE;
    }
  }
  DBUG_RETURN(0);

error_thread_create:
  roop_count2 = roop_count;
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
  }
  share->bg_mon_kill = TRUE;
  for (roop_count = roop_count2 - 1; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
    {
      pthread_cond_wait(&share->bg_mon_conds[roop_count],
        &share->bg_mon_mutexes[roop_count]);
      pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
    }
  }
  share->bg_mon_kill = FALSE;
  roop_count = share->all_link_count;
error_cond_init:
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      pthread_cond_destroy(&share->bg_mon_conds[roop_count]);
  }
  roop_count = share->all_link_count;
error_mutex_init:
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      pthread_mutex_destroy(&share->bg_mon_mutexes[roop_count]);
  }
  spider_free(spider_current_trx, share->bg_mon_thds, MYF(0));
error_alloc_base:
error_get_ping_table_mon_list:
  DBUG_RETURN(error_num);
}

void spider_free_mon_threads(
  SPIDER_SHARE *share
) {
  int roop_count;
  DBUG_ENTER("spider_free_mon_threads");
  if (share->bg_mon_init)
  {
    for (roop_count = 0; roop_count < (int) share->all_link_count;
      roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
        pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
    }
    share->bg_mon_kill = TRUE;
    for (roop_count = 0; roop_count < (int) share->all_link_count;
      roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
      {
        pthread_cond_wait(&share->bg_mon_conds[roop_count],
          &share->bg_mon_mutexes[roop_count]);
        pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
        pthread_join(share->bg_mon_threads[roop_count], NULL);
        pthread_cond_destroy(&share->bg_mon_conds[roop_count]);
        pthread_mutex_destroy(&share->bg_mon_mutexes[roop_count]);
      }
    }
    spider_free(spider_current_trx, share->bg_mon_thds, MYF(0));
    share->bg_mon_kill = FALSE;
    share->bg_mon_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_mon_action(
  void *arg
) {
  SPIDER_LINK_PACK *link_pack = (SPIDER_LINK_PACK*) arg;
  SPIDER_SHARE *share = link_pack->share;
  SPIDER_TRX *trx;
  int error_num, link_idx = link_pack->link_idx;
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_mon_action");
  /* init start */
  pthread_mutex_lock(&share->bg_mon_mutexes[link_idx]);
  if (!(thd = new THD()))
  {
    share->bg_mon_kill = FALSE;
    share->bg_mon_init = FALSE;
    pthread_cond_signal(&share->bg_mon_conds[link_idx]);
    pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  pthread_mutex_lock(&LOCK_thread_count);
  thd->thread_id = (*spd_db_att_thread_id)++;
  pthread_mutex_unlock(&LOCK_thread_count);
#ifdef HAVE_PSI_INTERFACE
  mysql_thread_set_psi_id(thd->thread_id);
#endif
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  if (!(trx = spider_get_trx(thd, FALSE, &error_num)))
  {
    delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
    set_current_thd(NULL);
#endif
    share->bg_mon_kill = FALSE;
    share->bg_mon_init = FALSE;
    pthread_cond_signal(&share->bg_mon_conds[link_idx]);
    pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
    my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  share->bg_mon_thds[link_idx] = thd;
  pthread_cond_signal(&share->bg_mon_conds[link_idx]);
  pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg mon sleep %lld",
      share->monitoring_bg_interval[link_idx]));
    if (!share->bg_mon_kill)
      my_sleep((ulong) share->monitoring_bg_interval[link_idx]);
    DBUG_PRINT("info",("spider bg mon roop start"));
    if (share->bg_mon_kill)
    {
      DBUG_PRINT("info",("spider bg mon kill start"));
      pthread_mutex_lock(&share->bg_mon_mutexes[link_idx]);
      pthread_cond_signal(&share->bg_mon_conds[link_idx]);
      pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
      spider_free_trx(trx, TRUE);
      delete thd;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100000
      set_current_thd(NULL);
#endif
#if !defined(MYSQL_DYNAMIC_PLUGIN) || !defined(_WIN32) || defined(_MSC_VER)
      my_pthread_setspecific_ptr(THR_THD, NULL);
#endif
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (share->monitoring_bg_kind[link_idx])
    {
      lex_start(thd);
      error_num = spider_ping_table_mon_from_table(
        spider_global_trx,
        thd,
        share,
        (uint32) share->monitoring_sid[link_idx],
        share->table_name,
        share->table_name_length,
        link_idx,
        NULL,
        0,
        share->monitoring_bg_kind[link_idx],
        share->monitoring_limit[link_idx],
        TRUE
      );
      lex_end(thd->lex);
    }
  }
}
#endif

#ifndef SPIDER_DISABLE_LINK
int spider_conn_first_link_idx(
  THD *thd,
  long *link_statuses,
  long *access_balances,
  uint *conn_link_idx,
  int link_count,
  int link_status
) {
  int roop_count, active_links = 0;
  longlong balance_total = 0, balance_val;
  double rand_val;
#ifdef _MSC_VER
  int *link_idxs, link_idx;
  long *balances;
#else
  int link_idxs[link_count];
  long balances[link_count];
#endif
  DBUG_ENTER("spider_conn_first_link_idx");
#ifdef _MSC_VER
  if (!(link_idxs = (int *)
    spider_bulk_malloc(spider_current_trx, 24, MYF(MY_WME),
      &link_idxs, sizeof(int) * link_count,
      &balances, sizeof(long) * link_count,
      NullS))
  ) {
    DBUG_PRINT("info",("spider out of memory"));
    DBUG_RETURN(-1);
  }
#endif
  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    DBUG_ASSERT((conn_link_idx[roop_count] - roop_count) % link_count == 0);
    if (link_statuses[conn_link_idx[roop_count]] <= link_status)
    {
      link_idxs[active_links] = roop_count;
      balances[active_links] = access_balances[roop_count];
      balance_total += access_balances[roop_count];
      active_links++;
    }
  }

  if (active_links == 0)
  {
    DBUG_PRINT("info",("spider all links are failed"));
#ifdef _MSC_VER
    spider_free(spider_current_trx, link_idxs, MYF(MY_WME));
#endif
    DBUG_RETURN(-1);
  }
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100002
  DBUG_PRINT("info",("spider server_id=%lu", thd->variables.server_id));
#else
  DBUG_PRINT("info",("spider server_id=%u", thd->server_id));
#endif
  DBUG_PRINT("info",("spider thread_id=%lu", thd_get_thread_id(thd)));
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100002
  rand_val = spider_rand(thd->variables.server_id + thd_get_thread_id(thd));
#else
  rand_val = spider_rand(thd->server_id + thd_get_thread_id(thd));
#endif
  DBUG_PRINT("info",("spider rand_val=%f", rand_val));
  balance_val = (longlong) (rand_val * balance_total);
  DBUG_PRINT("info",("spider balance_val=%lld", balance_val));
  for (roop_count = 0; roop_count < active_links - 1; roop_count++)
  {
    DBUG_PRINT("info",("spider balances[%d]=%ld",
      roop_count, balances[roop_count]));
    if (balance_val < balances[roop_count])
      break;
    balance_val -= balances[roop_count];
  }

  DBUG_PRINT("info",("spider first link_idx=%d", link_idxs[roop_count]));
#ifdef _MSC_VER
  link_idx = link_idxs[roop_count];
  spider_free(spider_current_trx, link_idxs, MYF(MY_WME));
  DBUG_RETURN(link_idx);
#else
  DBUG_RETURN(link_idxs[roop_count]);
#endif
}

int spider_conn_next_link_idx(
  THD *thd,
  long *link_statuses,
  long *access_balances,
  uint *conn_link_idx,
  int link_idx,
  int link_count,
  int link_status
) {
  int tmp_link_idx;
  DBUG_ENTER("spider_conn_next_link_idx");
  DBUG_ASSERT((conn_link_idx[link_idx] - link_idx) % link_count == 0);
  tmp_link_idx = spider_conn_first_link_idx(thd, link_statuses,
    access_balances, conn_link_idx, link_count, link_status);
  if (
    tmp_link_idx >= 0 &&
    tmp_link_idx == link_idx
  ) {
    do {
      tmp_link_idx++;
      if (tmp_link_idx >= link_count)
        tmp_link_idx = 0;
      if (tmp_link_idx == link_idx)
        break;
    } while (link_statuses[conn_link_idx[tmp_link_idx]] > link_status);
    DBUG_PRINT("info",("spider next link_idx=%d", tmp_link_idx));
    DBUG_RETURN(tmp_link_idx);
  }
  DBUG_PRINT("info",("spider next link_idx=%d", tmp_link_idx));
  DBUG_RETURN(tmp_link_idx);
}

int spider_conn_link_idx_next(
  long *link_statuses,
  uint *conn_link_idx,
  int link_idx,
  int link_count,
  int link_status
) {
  DBUG_ENTER("spider_conn_link_idx_next");
  do {
    link_idx++;
    if (link_idx >= link_count)
      break;
    DBUG_ASSERT((conn_link_idx[link_idx] - link_idx) % link_count == 0);
  } while (link_statuses[conn_link_idx[link_idx]] > link_status);
  DBUG_PRINT("info",("spider link_idx=%d", link_idx));
  DBUG_RETURN(link_idx);
}
#endif

int spider_conn_lock_mode(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_conn_lock_mode");
  if (result_list->lock_type == F_WRLCK || spider->lock_mode == 2)
    DBUG_RETURN(SPIDER_LOCK_MODE_EXCLUSIVE); // lock_mode不会为2
  else if (spider->lock_mode == 1) // select * from t1 lock in share mode时 lock_mode才为1
    DBUG_RETURN(SPIDER_LOCK_MODE_SHARED);
  DBUG_RETURN(SPIDER_LOCK_MODE_NO_LOCK); // 应用场景下走此逻辑
}

bool spider_conn_check_recovery_link(
  SPIDER_SHARE *share
) {
  int roop_count;
  DBUG_ENTER("spider_check_recovery_link");
  for (roop_count = 0; roop_count < (int) share->link_count; roop_count++)
  {
    if (share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY)
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}

bool spider_conn_use_handler(
  ha_spider *spider,
  int lock_mode,
  int link_idx
) {
  THD *thd = spider->trx->thd;
  int use_handler = spider_param_use_handler(thd,
    spider->share->use_handlers[link_idx]);
  DBUG_ENTER("spider_conn_use_handler");
  DBUG_PRINT("info",("spider use_handler=%d", use_handler));
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->conn_kind[link_idx] != SPIDER_CONN_KIND_MYSQL)
  {
    DBUG_PRINT("info",("spider TRUE by HS"));
    spider->sql_kinds |= SPIDER_SQL_KIND_HS;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_HS;
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    if (
      spider->do_direct_update &&
      spider_bit_is_set(spider->do_hs_direct_update, link_idx)
    ) {
      DBUG_PRINT("info",("spider using HS direct_update"));
      spider->direct_update_kinds |= SPIDER_SQL_KIND_HS;
    }
#endif
    DBUG_RETURN(TRUE);
  }
#endif
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
  if (spider->do_direct_update)
  {
    spider->sql_kinds |= SPIDER_SQL_KIND_SQL;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_SQL;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (spider_bit_is_set(spider->do_hs_direct_update, link_idx))
    {
      spider->direct_update_kinds |= SPIDER_SQL_KIND_HS;
      DBUG_PRINT("info",("spider TRUE by using HS direct_update"));
      DBUG_RETURN(TRUE);
    } else
#endif
      spider->direct_update_kinds |= SPIDER_SQL_KIND_SQL;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_MYSQL)
    {
#endif
      DBUG_PRINT("info",("spider FALSE by using direct_update"));
      DBUG_RETURN(FALSE);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      DBUG_PRINT("info",("spider TRUE by using BOTH"));
      DBUG_RETURN(TRUE);
    }
#endif
  }
#endif
  if (spider->use_spatial_index)
  {
    DBUG_PRINT("info",("spider FALSE by use_spatial_index"));
    spider->sql_kinds |= SPIDER_SQL_KIND_SQL;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_SQL;
    DBUG_RETURN(FALSE);
  }
  uint dbton_id;
  spider_db_handler *dbton_hdl;
  dbton_id = spider->share->sql_dbton_ids[spider->conn_link_idx[link_idx]];
  dbton_hdl = spider->dbton_handler[dbton_id];
  if (!dbton_hdl->support_use_handler(use_handler))
  {
    DBUG_PRINT("info",("spider FALSE by dbton"));
    spider->sql_kinds |= SPIDER_SQL_KIND_SQL;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_SQL;
    DBUG_RETURN(FALSE);
  }
  if (
    spider->sql_command == SQLCOM_HA_READ &&
    (
      !(use_handler & 2) ||
      (
        spider_param_sync_trx_isolation(thd) &&
        thd_tx_isolation(thd) == ISO_SERIALIZABLE
      )
    )
  ) {
    DBUG_PRINT("info",("spider TRUE by HA"));
    spider->sql_kinds |= SPIDER_SQL_KIND_HANDLER;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_HANDLER;
    DBUG_RETURN(TRUE);
  }
  if (
    spider->sql_command != SQLCOM_HA_READ &&
    lock_mode == SPIDER_LOCK_MODE_NO_LOCK &&
    spider_param_sync_trx_isolation(thd) &&
    thd_tx_isolation(thd) != ISO_SERIALIZABLE &&
    (use_handler & 1)
  ) {
    DBUG_PRINT("info",("spider TRUE by PARAM"));
    spider->sql_kinds |= SPIDER_SQL_KIND_HANDLER;
    spider->sql_kind[link_idx] = SPIDER_SQL_KIND_HANDLER;
    DBUG_RETURN(TRUE);
  }
  spider->sql_kinds |= SPIDER_SQL_KIND_SQL;
  spider->sql_kind[link_idx] = SPIDER_SQL_KIND_SQL;
  DBUG_RETURN(FALSE);
}

bool spider_conn_need_open_handler(
  ha_spider *spider,
  uint idx,
  int link_idx
) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  SPIDER_CONN *conn;
#endif
  DBUG_ENTER("spider_conn_need_open_handler");
  DBUG_PRINT("info",("spider spider=%p", spider));
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->handler_opened(link_idx, spider->conn_kind[link_idx]))
#else
  if (spider->handler_opened(link_idx, SPIDER_CONN_KIND_MYSQL))
#endif
  {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    if (
      spider->do_direct_update &&
      spider_bit_is_set(spider->do_hs_direct_update, link_idx)
    ) {
      conn = spider->hs_w_conns[link_idx];
      if (
        !conn->server_lost &&
        conn->hs_pre_age == spider->hs_w_conn_ages[link_idx]
      ) {
        DBUG_PRINT("info",("spider hs_write is already opened"));
        DBUG_RETURN(FALSE);
      }
    } else
#endif
    if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_MYSQL)
    {
#endif
      DBUG_PRINT("info",("spider HA already opened"));
      DBUG_RETURN(FALSE);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_HS_READ)
    {
      DBUG_PRINT("info",("spider r_handler_index[%d]=%d",
        link_idx, spider->r_handler_index[link_idx]));
      DBUG_PRINT("info",("spider idx=%d", idx));
      DBUG_PRINT("info",("spider hs_pushed_ret_fields_num=%zu",
        spider->hs_pushed_ret_fields_num));
      DBUG_PRINT("info",("spider hs_r_ret_fields_num[%d]=%lu",
        link_idx, spider->hs_r_ret_fields_num[link_idx]));
      DBUG_PRINT("info",("spider hs_r_ret_fields[%d]=%p",
        link_idx, spider->hs_r_ret_fields[link_idx]));
#ifndef DBUG_OFF
      if (
        spider->hs_pushed_ret_fields_num < MAX_FIELDS &&
        spider->hs_r_ret_fields[link_idx] &&
        spider->hs_pushed_ret_fields_num ==
          spider->hs_r_ret_fields_num[link_idx]
      ) {
        int roop_count;
        for (roop_count = 0; roop_count < (int) spider->hs_pushed_ret_fields_num;
          ++roop_count)
        {
          DBUG_PRINT("info",("spider hs_pushed_ret_fields[%d]=%u",
            roop_count, spider->hs_pushed_ret_fields[roop_count]));
          DBUG_PRINT("info",("spider hs_r_ret_fields[%d][%d]=%u",
            link_idx, roop_count,
            spider->hs_r_ret_fields[link_idx][roop_count]));
        }
      }
#endif
      if (
        spider->r_handler_index[link_idx] == idx
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
        && (
          (
            spider->hs_pushed_ret_fields_num == MAX_FIELDS &&
            spider->hs_r_ret_fields_num[link_idx] == MAX_FIELDS
          ) ||
          (
            spider->hs_pushed_ret_fields_num < MAX_FIELDS &&
            spider->hs_r_ret_fields[link_idx] &&
            spider->hs_pushed_ret_fields_num ==
              spider->hs_r_ret_fields_num[link_idx] &&
            !memcmp(spider->hs_pushed_ret_fields,
              spider->hs_r_ret_fields[link_idx],
              sizeof(uint32) * spider->hs_pushed_ret_fields_num)
          )
        )
#endif
      ) {
        SPIDER_CONN *conn = spider->hs_r_conns[link_idx];
        DBUG_PRINT("info",("spider conn=%p", conn));
        DBUG_PRINT("info",("spider conn->conn_id=%llu", conn->conn_id));
        DBUG_PRINT("info",("spider conn->connection_id=%llu",
          conn->connection_id));
        DBUG_PRINT("info",("spider conn->server_lost=%s",
          conn->server_lost ? "TRUE" : "FALSE"));
        DBUG_PRINT("info",("spider conn->hs_pre_age=%llu", conn->hs_pre_age));
        DBUG_PRINT("info",("spider hs_w_conn_ages[%d]=%llu",
          link_idx, spider->hs_w_conn_ages[link_idx]));
        if (
          !conn->server_lost &&
          conn->hs_pre_age == spider->hs_r_conn_ages[link_idx]
        ) {
          DBUG_PRINT("info",("spider hs_r same idx"));
          DBUG_RETURN(FALSE);
        }
      }
    } else if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_HS_WRITE)
    {
      DBUG_PRINT("info",("spider w_handler_index[%d]=%d",
        link_idx, spider->w_handler_index[link_idx]));
      DBUG_PRINT("info",("spider idx=%d", idx));
      DBUG_PRINT("info",("spider hs_pushed_ret_fields_num=%zu",
        spider->hs_pushed_ret_fields_num));
      DBUG_PRINT("info",("spider hs_w_ret_fields_num[%d]=%lu",
        link_idx, spider->hs_w_ret_fields_num[link_idx]));
      DBUG_PRINT("info",("spider hs_w_ret_fields[%d]=%p",
        link_idx, spider->hs_w_ret_fields[link_idx]));
#ifndef DBUG_OFF
      if (
        spider->hs_pushed_ret_fields_num < MAX_FIELDS &&
        spider->hs_w_ret_fields[link_idx] &&
        spider->hs_pushed_ret_fields_num ==
          spider->hs_w_ret_fields_num[link_idx]
      ) {
        int roop_count;
        for (roop_count = 0; roop_count < (int) spider->hs_pushed_ret_fields_num;
          ++roop_count)
        {
          DBUG_PRINT("info",("spider hs_pushed_ret_fields[%d]=%u",
            roop_count, spider->hs_pushed_ret_fields[roop_count]));
          DBUG_PRINT("info",("spider hs_w_ret_fields[%d][%d]=%u",
            link_idx, roop_count,
            spider->hs_w_ret_fields[link_idx][roop_count]));
        }
      }
#endif
      if (
        spider->w_handler_index[link_idx] == idx
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
        && (
          (
            spider->hs_pushed_ret_fields_num == MAX_FIELDS &&
            spider->hs_w_ret_fields_num[link_idx] == MAX_FIELDS
          ) ||
          (
            spider->hs_pushed_ret_fields_num < MAX_FIELDS &&
            spider->hs_w_ret_fields[link_idx] &&
            spider->hs_pushed_ret_fields_num ==
              spider->hs_w_ret_fields_num[link_idx] &&
            !memcmp(spider->hs_pushed_ret_fields,
              spider->hs_w_ret_fields[link_idx],
              sizeof(uint32) * spider->hs_pushed_ret_fields_num)
          )
        )
#endif
      ) {
        SPIDER_CONN *conn = spider->hs_w_conns[link_idx];
        DBUG_PRINT("info",("spider conn=%p", conn));
        DBUG_PRINT("info",("spider conn->conn_id=%llu", conn->conn_id));
        DBUG_PRINT("info",("spider conn->connection_id=%llu",
          conn->connection_id));
        DBUG_PRINT("info",("spider conn->server_lost=%s",
          conn->server_lost ? "TRUE" : "FALSE"));
        DBUG_PRINT("info",("spider conn->hs_pre_age=%llu", conn->hs_pre_age));
        DBUG_PRINT("info",("spider hs_w_conn_ages[%d]=%llu",
          link_idx, spider->hs_w_conn_ages[link_idx]));
        if (
          !conn->server_lost &&
          conn->hs_pre_age == spider->hs_w_conn_ages[link_idx]
        ) {
          DBUG_PRINT("info",("spider hs_w same idx"));
          DBUG_RETURN(FALSE);
        }
      }
    }
#endif
  }
  DBUG_RETURN(TRUE);
}

static void my_polling_last_visited(uchar *entry, void *data) {
    DBUG_ENTER("my_polling_last_visited");
    /* harryczhang: This conn object must be in hash pool. So conn->last_visited can't be modified. */
    SPIDER_CONN *conn = (SPIDER_CONN *)entry;
    delegate_param *param = (delegate_param *) data;
    DYNAMIC_STRING_ARRAY **arr_info = param->arr_info;
    if (conn) {
        time_t time_now = time((time_t *) 0);
        if (time_now > 0 && time_now > conn->last_visited && time_now - conn->last_visited >= spider_param_idle_conn_recycle_interval()) {
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
            append_dynamic_string_array(arr_info[0], (char *) &conn->conn_key_hash_value, sizeof(conn->conn_key_hash_value));
#else
            HASH *spider_open_connections = param->hash_info;
            my_hash_value_type hash_value = my_calc_hash(spider_open_connections, conn->conn_key, conn->conn_key_length);
            append_dynamic_string_array(arr_info[0], (char *) &hash_value, sizeof(hash_value));
#endif
            append_dynamic_string_array(arr_info[1], (char *) conn->conn_key, conn->conn_key_length);
        } 

        DBUG_VOID_RETURN;
    }
}

static void *spider_conn_recycle_action(void *arg)
{
    DBUG_ENTER ("spider_conn_recycle_action");
    DYNAMIC_STRING_ARRAY idle_conn_key_hash_value_arr;
    DYNAMIC_STRING_ARRAY idle_conn_key_arr;

    if (init_dynamic_string_array(&idle_conn_key_hash_value_arr, 64, 64) ||
        init_dynamic_string_array(&idle_conn_key_arr, 64, 64)) {
        DBUG_RETURN(NULL);
    }

    delegate_param param;
    param.hash_info = &spider_open_connections;        
    param.arr_info[0] = &idle_conn_key_hash_value_arr;
    param.arr_info[1] = &idle_conn_key_arr;
    while (conn_rcyc_init) {
        clear_dynamic_string_array(&idle_conn_key_hash_value_arr);
        clear_dynamic_string_array(&idle_conn_key_arr);
        
        pthread_mutex_lock(&spider_conn_mutex);
        my_hash_delegate(&spider_open_connections, my_polling_last_visited, &param);
        pthread_mutex_unlock(&spider_conn_mutex);

        for (size_t i = 0; i < idle_conn_key_hash_value_arr.cur_idx; ++i) {
            my_hash_value_type *tmp_ptr = NULL;
            if (get_dynamic_string_array(&idle_conn_key_hash_value_arr, (char **) &tmp_ptr, NULL, i)) {
                spider_my_err_logging("[ERROR] fill tmp_ptr error by idle_conn_key_hash_value_arr!\n");
                break;
            }

            char *conn_key = NULL;
            size_t conn_key_len;
            my_hash_value_type conn_key_hash_value = *tmp_ptr;
            if (get_dynamic_string_array(&idle_conn_key_arr, &conn_key, &conn_key_len, i)) {
                spider_my_err_logging("[ERROR] fill conn_key error from idle_conn_key_arr!\n");
                break;
            }
            pthread_mutex_lock (&spider_conn_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE           
            SPIDER_CONN *conn = (SPIDER_CONN *) my_hash_search_using_hash_value (
                            &spider_open_connections,
                            conn_key_hash_value,
                            (uchar *) conn_key, 
                            conn_key_len);
            
#else
            SPIDER_CONN *conn = (SPIDER_CONN *) my_hash_search(&spider_open_connections, conn_key, conn_key_len);            
#endif
            if (conn) {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
                my_hash_delete_with_hash_value (&spider_open_connections,
                                                conn->conn_key_hash_value, (uchar *) conn);
#else
                my_hash_delete (&spider_open_connections, (uchar *) conn);
#endif
            }
            pthread_mutex_unlock(&spider_conn_mutex);
            if (conn) {
                spider_update_conn_meta_info(conn, SPIDER_CONN_INVALID_STATUS);
                spider_free_conn(conn);
            }
        }
        
        // NOTE: In worst case, idle connection would be freed in 1.25 * spider_param_idle_conn_recycle_interval
		//sleep(spider_param_idle_conn_recycle_interval() >> 2); 
		int sleep_sec = (spider_param_idle_conn_recycle_interval() >> 2);
		while (sleep_sec > 0) {
			sleep(8);
			sleep_sec -= 8;
			if (sleep_sec > (spider_param_idle_conn_recycle_interval() >> 2) ) {
				sleep_sec = (spider_param_idle_conn_recycle_interval() >> 2);
			}
		}
    }

    free_dynamic_string_array(&idle_conn_key_hash_value_arr);
    free_dynamic_string_array(&idle_conn_key_arr);
    DBUG_RETURN(NULL);
}

int spider_create_conn_recycle_thread()
{
    int error_num;
    DBUG_ENTER("spider_create_conn_recycle_thread");
    if (conn_rcyc_init) {
        DBUG_RETURN(0);
    }

#if MYSQL_VERSION_ID < 50500
    if (pthread_create (&conn_rcyc_thread, NULL, spider_conn_recycle_action, NULL))
#else
    if (mysql_thread_create (spd_key_thd_conn_rcyc, &conn_rcyc_thread, NULL, spider_conn_recycle_action, NULL))
#endif
    {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error_thread_create;
    }
    conn_rcyc_init = TRUE;
    DBUG_RETURN (0);

error_thread_create:
    DBUG_RETURN (error_num);
}

void spider_free_conn_recycle_thread()
{
    DBUG_ENTER ("spider_free_conn_recycle_thread");
    if (conn_rcyc_init) {
        pthread_cancel(conn_rcyc_thread);
        pthread_join(conn_rcyc_thread, NULL);
        conn_rcyc_init = FALSE; 
    }

    DBUG_VOID_RETURN;
}

void
spider_free_conn_meta(void *meta)
{
  DBUG_ENTER("free_spider_conn_meta");
  if (meta) {
    SPIDER_CONN_META_INFO *p = (SPIDER_CONN_META_INFO *)meta;
    my_free(p->key, MYF(0));
    my_free(p, MYF(0));
  }

  DBUG_VOID_RETURN;
}

void
spider_free_ipport_conn(void *info)
{
  DBUG_ENTER("spider_free_ipport_conn");
  if (info) {
    SPIDER_IP_PORT_CONN *p = (SPIDER_IP_PORT_CONN *)info;
    my_free(p->key, MYF(0));
    my_free(p, MYF(0));
  }
  DBUG_VOID_RETURN;
}

SPIDER_CONN_META_INFO *
spider_create_conn_meta(SPIDER_CONN *conn) 
{
    DBUG_ENTER("spider_create_conn_meta");
    if (conn) {
        SPIDER_CONN_META_INFO *ret = (SPIDER_CONN_META_INFO *) my_malloc(sizeof(*ret), MY_ZEROFILL | MY_WME);
        if (!ret) {
            goto err_return_direct;
        }

        ret->key_len = conn->conn_key_length;
        if (ret->key_len <= 0) {
            goto err_return_direct;
        }

        ret->key = (char *) my_malloc(ret->key_len, MY_ZEROFILL | MY_WME);
        if (!ret->key) {
            goto err_malloc_key;
        }

        memcpy(ret->key, conn->conn_key, ret->key_len);

        strncpy(ret->remote_user_str, conn->tgt_username, sizeof(ret->remote_user_str));
        strncpy(ret->remote_ip_str, conn->tgt_host, sizeof(ret->remote_ip_str));
        ret->remote_port = conn->tgt_port;

#ifdef SPIDER_HAS_HASH_VALUE_TYPE
        ret->key_hash_value = conn->conn_key_hash_value;
#endif

        ret->status = SPIDER_CONN_INIT_STATUS;
        ret->conn_id = conn->conn_id;
        ret->alloc_tm = time(NULL);
        ret->reusage_counter = 0;
        DBUG_RETURN(ret);    
err_malloc_key:
        my_free(ret, MYF(0));
err_return_direct:
        DBUG_RETURN(NULL);
    }

    DBUG_RETURN(NULL);
}


SPIDER_IP_PORT_CONN *
spider_create_ipport_conn(SPIDER_CONN *conn) 
{
  uint next_spider_conn_mutex_id;
  DBUG_ENTER("spider_create_ipport_conn");
  if (conn) {
    SPIDER_IP_PORT_CONN *ret = (SPIDER_IP_PORT_CONN *) my_malloc(sizeof(*ret), MY_ZEROFILL | MY_WME);
    if (!ret) {
      goto err_return_direct;
    }

    next_spider_conn_mutex_id = spider_conn_mutex_id;
    if (next_spider_conn_mutex_id >= SPIDER_MAX_PARTITION_NUM) {
      // RETURN ERROR and output error log;
      time_t cur_time = (time_t) time((time_t*) 0);
      struct tm lt;
      struct tm *l_time = localtime_r(&cur_time, &lt);
      fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN SPIDER RESULT] "
        "error to spider_create_ipport_conn, next_spider_conn_mutex_id is %d\n",
        l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
        l_time->tm_hour, l_time->tm_min, l_time->tm_sec, next_spider_conn_mutex_id);
      goto err_malloc_key;
    }

    if (next_spider_conn_mutex_id >= opt_spider_max_connections) 
    {
      //to do dynamic alloc
      if (pthread_mutex_init(&(spider_conn_i_mutexs[next_spider_conn_mutex_id].m_mutex), MY_MUTEX_INIT_FAST)) {
        //error
         goto err_malloc_key;
      }

      if (pthread_cond_init(&(spider_conn_i_conds[next_spider_conn_mutex_id].m_cond), NULL)) 
      {
        pthread_mutex_destroy(&(spider_conn_i_mutexs[next_spider_conn_mutex_id]));
        goto err_malloc_key;
        //error
      }
    }

    ret->conn_mutex_num = spider_conn_mutex_id++;

    ret->key_len = conn->conn_key_length;
    if (ret->key_len <= 0) {
      goto err_malloc_key;
    }

    ret->key = (char *) my_malloc(ret->key_len, MY_ZEROFILL | MY_WME);
    if (!ret->key) {
      goto err_malloc_key;
    }

    memcpy(ret->key, conn->conn_key, ret->key_len);

    strncpy(ret->remote_ip_str, conn->tgt_host, sizeof(ret->remote_ip_str));
    ret->remote_port = conn->tgt_port;
    ret->conn_id = conn->conn_id;
    ret->ip_port_count = 1; // 初始化为1

#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    ret->key_hash_value = conn->conn_key_hash_value;
#endif
    DBUG_RETURN(ret);    
err_malloc_key:
    my_free(ret, MYF(0));
err_return_direct:
    DBUG_RETURN(NULL);
  }

  DBUG_RETURN(NULL);
}

/*
static my_bool 
update_alloc_time(SPIDER_CONN_META_INFO *meta)
{
    DBUG_ENTER("update_alloc_time");
    if (meta && SPIDER_CONN_IS_INVALID(meta)) {
        meta->status_str = SPIDER_CONN_META_INIT2_STATUS;
        spider_gettime_str(meta->alloc_time_str, SPIDER_CONN_META_BUF_LEN); 
        DBUG_RETURN(TRUE);
    }

    DBUG_RETURN(FALSE);
}

my_bool
update_visit_time(SPIDER_CONN_META_INFO *meta)
{
    DBUG_ENTER("update_visit_time");
    if (meta && !SPIDER_CONN_IS_INVALID(meta)) {
        meta->status_str = SPIDER_CONN_META_ACTIVE_STATUS;
        spider_gettime_str(meta->last_visit_time_str, SPIDER_CONN_META_BUF_LEN);
        DBUG_RETURN(TRUE);
    }

    DBUG_RETURN(FALSE);
}
*/

my_bool
spider_add_conn_meta_info(SPIDER_CONN *conn)
{
    DBUG_ENTER("spider_add_conn_meta_info");
    SPIDER_CONN_META_INFO *meta_info;
    pthread_mutex_lock(&spider_conn_meta_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    if (!(meta_info = (SPIDER_CONN_META_INFO *) my_hash_search_using_hash_value(
        &spider_conn_meta_info, 
        conn->conn_key_hash_value,
        (uchar*) conn->conn_key,
        conn->conn_key_length)))
#else
    if (!(meta_info = (SPIDER_CONN_META_INFO *) my_hash_search(&spider_conn_meta_info,
        (uchar*) conn->conn_key,
        conn->conn_key_length)))
#endif
    {
        pthread_mutex_unlock(&spider_conn_meta_mutex);
        meta_info = spider_create_conn_meta(conn);
        if (!meta_info) {
            DBUG_RETURN(FALSE);    
        }
        pthread_mutex_lock(&spider_conn_meta_mutex);
        if (my_hash_insert(&spider_conn_meta_info, (uchar *)meta_info)) {
            /* insert failed */
            pthread_mutex_unlock(&spider_conn_meta_mutex);
            DBUG_RETURN(FALSE);
        } 
        pthread_mutex_unlock(&spider_conn_meta_mutex);
    } else {
        pthread_mutex_unlock(&spider_conn_meta_mutex);
        /* exist already */
        if (SPIDER_CONN_IS_INVALID(meta_info)) {
            meta_info->alloc_tm = time(NULL);
            meta_info->status = SPIDER_CONN_INIT2_STATUS;
            meta_info->reusage_counter = 0;
        } else {
            /* NOTE: deleted from spider_open_connections for in-place usage, not be freed. */
            ++meta_info->reusage_counter;
        }
    }

    DBUG_RETURN(TRUE);
}

void
spider_update_conn_meta_info(SPIDER_CONN *conn, uint new_status) 
{
    DBUG_ENTER("spider_update_conn_meta_info");
    SPIDER_CONN_META_INFO *meta_info;
    pthread_mutex_lock(&spider_conn_meta_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    if (!(meta_info = (SPIDER_CONN_META_INFO *) my_hash_search_using_hash_value(
        &spider_conn_meta_info, 
        conn->conn_key_hash_value,
        (uchar*) conn->conn_key,
        conn->conn_key_length)))
#else
    if (!(meta_info = (SPIDER_CONN_META_INFO *) my_hash_search(&spider_conn_meta_info,
        (uchar*) conn->conn_key,
        conn->conn_key_length)))
#endif
    {
        pthread_mutex_unlock(&spider_conn_meta_mutex);
        DBUG_VOID_RETURN;
    } else {
        pthread_mutex_unlock(&spider_conn_meta_mutex);
        /* exist already */
        if (!SPIDER_CONN_IS_INVALID(meta_info)) {
            if (new_status == SPIDER_CONN_ACTIVE_STATUS) {
                meta_info->last_visit_tm = time(NULL);
            } else if (new_status == SPIDER_CONN_INVALID_STATUS) {
                meta_info->free_tm = time(NULL);
            }
            meta_info->status = new_status;
        } 
    }

    DBUG_VOID_RETURN;
}

SPIDER_CONN* spider_get_conn_from_idle_connection(
	SPIDER_SHARE *share,
	int link_idx,
	char *conn_key,
	ha_spider *spider,
	uint conn_kind,
	int base_link_idx,
	int *error_num
)
{	
	DBUG_ENTER("spider_wait_idle_connection");
	SPIDER_IP_PORT_CONN *ip_port_conn;
	SPIDER_CONN *conn = NULL;
	uint spider_max_connections = 0;
	struct timespec abstime;
	ulonglong start, inter_val = 0;
	ulonglong wait_time = (ulonglong)opt_spider_conn_wait_timeout*1000*1000*1000; // default 10s

	unsigned long ip_port_count = 0; // 初始为0，表示不存在当前ip#port的连接 
	long mutex_num=0;
	spider_max_connections = opt_spider_max_connections;

	set_timespec(abstime, 10);
	
	pthread_mutex_lock(&spider_ipport_count_mutex);
	if(ip_port_conn = (SPIDER_IP_PORT_CONN*) my_hash_search_using_hash_value(&spider_ipport_conns, share->conn_keys_hash_value[link_idx], (uchar*) share->conn_keys[link_idx], share->conn_keys_lengths[link_idx]))
	{/* 如果存在 */
		ip_port_count = ip_port_conn->ip_port_count;
		mutex_num = ip_port_conn->conn_mutex_num;
	}

	if(ip_port_count >= spider_max_connections && spider_max_connections > 0)
	{ /* 当前 spider_open_connections 无空闲连接，且当前连接的使用个数大于 opt_spider_max_connections */
		pthread_mutex_unlock(&spider_ipport_count_mutex);
		start = my_getsystime();
		while(1)
		{
			int error;
			inter_val = my_getsystime() - start;
			if(wait_time - inter_val*100 <= 0)
			{/* 已等超时 */
				DBUG_RETURN(NULL);
			}
			set_timespec_nsec(abstime, wait_time - inter_val*100);
			pthread_mutex_lock(&spider_conn_i_mutexs[mutex_num]);
			error = pthread_cond_timedwait(&spider_conn_i_conds[mutex_num], &spider_conn_i_mutexs[mutex_num], &abstime);
			if (error == ETIMEDOUT || error == ETIME || error!=0 )
			{
				pthread_mutex_unlock(&spider_conn_i_mutexs[mutex_num]); /* 超时退出 */
				DBUG_RETURN(NULL);
			}
			pthread_mutex_unlock(&spider_conn_i_mutexs[mutex_num]);

			pthread_mutex_lock(&spider_conn_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
			if ((conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
				&spider_open_connections, share->conn_keys_hash_value[link_idx],
				(uchar*) share->conn_keys[link_idx],
				share->conn_keys_lengths[link_idx])))
#else
			if ((conn = (SPIDER_CONN*) my_hash_search(&spider_open_connections,
				(uchar*) share->conn_keys[link_idx],
				share->conn_keys_lengths[link_idx])))
#endif
			{
				/* 从空闲连接中找到可重用资源, 则将conn从空闲连接中删除掉 */
#ifdef HASH_UPDATE_WITH_HASH_VALUE
				my_hash_delete_with_hash_value(&spider_open_connections,
					conn->conn_key_hash_value, (uchar*) conn);
#else
				my_hash_delete(&spider_open_connections, (uchar*) conn);  
#endif
				/* 通过spider_wait_idle_connection 分配了conn， 对spider_open_connections上锁，delete后释放*/
				pthread_mutex_unlock(&spider_conn_mutex);
				DBUG_PRINT("info",("spider get global conn"));
				if (spider)
				{
					spider->conns[base_link_idx] = conn;
					if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
						conn->use_for_active_standby = TRUE;  
				}
				DBUG_RETURN(conn);
			}
			else
			{
				pthread_mutex_unlock(&spider_conn_mutex);
			}
		}
	}
	else
	{/* 需要创建连接 */
		pthread_mutex_unlock(&spider_ipport_count_mutex); 
		DBUG_PRINT("info",("spider create new conn"));
		if (!(conn = spider_create_conn(share, spider, link_idx, base_link_idx, conn_kind, error_num)))
			DBUG_RETURN(conn); // 函数外围 goto error;
		*conn->conn_key = *conn_key;
		if (spider)
		{
			spider->conns[base_link_idx] = conn;
			if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
				conn->use_for_active_standby = TRUE;
		}
	}

	DBUG_RETURN(conn);
}


SPIDER_CONN* spider_get_conn_from_idle_connection_bak(
	SPIDER_SHARE *share,
	int link_idx,
	char *conn_key,
	ha_spider *spider,
	uint conn_kind,
	int base_link_idx,
	int *error_num
	)
{
	DBUG_ENTER("spider_wait_idle_connection");
	uint retry_times = 0;
	SPIDER_IP_PORT_CONN *ip_port_conn;
	SPIDER_CONN *conn = NULL;
	// opt_spider_max_connections为0时，不启用连接池限制
	assert(opt_spider_max_connections > 0); // opt_spider_max_connections大于0才等空闲连接

	unsigned long ip_port_count = 0; // 初始为0，表示不存在当前ip#port的连接 
	pthread_mutex_lock(&spider_ipport_count_mutex);
	if(ip_port_conn = (SPIDER_IP_PORT_CONN*) my_hash_search_using_hash_value(&spider_ipport_conns, share->conn_keys_hash_value[link_idx], (uchar*) share->conn_keys[link_idx], share->conn_keys_lengths[link_idx]))
	{/* 如果存在 */
		ip_port_count = ip_port_conn->ip_port_count;
	}
	if(ip_port_count >= opt_spider_max_connections)
	{ /* 当前 spider_open_connections 无空闲连接，且当前连接的使用个数大于 opt_spider_max_connections */
		pthread_mutex_unlock(&spider_ipport_count_mutex);
		while(retry_times++ < opt_spider_conn_wait_timeout)
		{
			my_sleep(1*1000); // wait 1 ms
			pthread_mutex_lock(&spider_conn_mutex);
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
			if ((conn = (SPIDER_CONN*) my_hash_search_using_hash_value(
				&spider_open_connections, share->conn_keys_hash_value[link_idx],
				(uchar*) share->conn_keys[link_idx],
				share->conn_keys_lengths[link_idx])))
#else
			if ((conn = (SPIDER_CONN*) my_hash_search(&spider_open_connections,
				(uchar*) share->conn_keys[link_idx],
				share->conn_keys_lengths[link_idx])))
#endif
			{
				/* 从空闲连接中找到可重用资源, 则将conn从空闲连接中删除掉 */
#ifdef HASH_UPDATE_WITH_HASH_VALUE
				my_hash_delete_with_hash_value(&spider_open_connections,
					conn->conn_key_hash_value, (uchar*) conn);
#else
				my_hash_delete(&spider_open_connections, (uchar*) conn);  
#endif
				/* 通过spider_wait_idle_connection 分配了conn， 对spider_open_connections上锁，delete后释放*/
				pthread_mutex_unlock(&spider_conn_mutex);
				DBUG_PRINT("info",("spider get global conn"));
				if (spider)
				{
					spider->conns[base_link_idx] = conn;
					if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
						conn->use_for_active_standby = TRUE;  
				}
				DBUG_RETURN(conn);
			}
			else
			{/* 空闲连接中未找到，尝试等待10ms，再找 */
				pthread_mutex_unlock(&spider_conn_mutex);
			}
		}
	}
	else
	{/* 需要创建连接 */
		pthread_mutex_unlock(&spider_ipport_count_mutex); 
		DBUG_PRINT("info",("spider create new conn"));
		if (!(conn = spider_create_conn(share, spider, link_idx, base_link_idx, conn_kind, error_num)))
			DBUG_RETURN(conn); // 函数外围 goto error;
		*conn->conn_key = *conn_key;
		if (spider)
		{
			spider->conns[base_link_idx] = conn;
			if (spider_bit_is_set(spider->conn_can_fo, base_link_idx))
				conn->use_for_active_standby = TRUE;
		}
	}

	DBUG_RETURN(conn);
}

//void spider_mta_conn_mutex_lock(SPIDER_CONN *conn)
//{
//  //if(conn && !conn->mta_conn_mutex_lock_already)
//  pid_t pid = my_pthread_get_tid();
//  if(conn && conn->mta_conn_mutex_os_thread_id != pid)
//  {
//    pthread_mutex_lock(&conn->mta_conn_mutex);
//    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
//    conn->mta_conn_mutex_os_thread_id = pid;
//    //  conn->mta_conn_mutex_unlock_later = TRUE;
//    //conn->mta_conn_mutex_lock_already = TRUE;
//    return;
//  }
//  assert(0);
//}
//void spider_mta_conn_mutex_unlock(SPIDER_CONN *conn)
//{
//  pid_t pid = my_pthread_get_tid();
//  if(conn && conn->mta_conn_mutex_os_thread_id == pid)
//  {
//    //conn->mta_conn_mutex_lock_already = FALSE;
//    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
//    pthread_mutex_unlock(&conn->mta_conn_mutex);
//    conn->mta_conn_mutex_os_thread_id = 0;
//    //  conn->mta_conn_mutex_unlock_later = TRUE;
//    return;
//  }
//  assert(0);
//}


