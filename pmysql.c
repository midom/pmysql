/*
    Copyright 2010-2018 Domas Mituzas, Facebook

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <glib.h>
#include <mysql.h>
#include <openssl/crypto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <time.h>
#include <unistd.h>

#include "threadpool.h"

#ifndef GLIB_VERSION_2_32
#error "Need at glib 2.32 or later"
#endif

/* Allow providing special meta database to skip at --all */

#define MYSQLDBS "information_schema", "mysql", "performance_schema", "test", "sys"

#ifndef METADB
const char *mdbs[] = {MYSQLDBS, NULL};
#else
const char *mdbs[] = {MYSQLDBS, METADB, NULL};
#endif

char *username = NULL;
char *password = NULL;
int port = 0;
int port_incr = 0;

char *socket_path = NULL;
char *db = NULL;
char **databases = NULL;

gboolean all_databases = FALSE;

char *the_query = NULL;
char *queryfile = NULL;

char *serversfile = NULL;

int default_num_threads = 200;
int num_threads = 0;

#define CONNECT_TIMEOUT 2

#define MAXLINE 10240

#ifdef HAVE_MYSQL_NONBLOCKING_CLIENT
#define TIMEOUT(X) (X.value_ms_ / 1000)
#else
#define TIMEOUT(X) (X)
#endif

int connect_timeout = 0;

int read_timeout = 0;

gboolean should_escape = 1;
gboolean vertical = 0;
gboolean should_not_escape = 0;
gboolean prepend_keys = 0;

gboolean should_compress = 0;

gboolean tagged_input = 0;
gboolean timing = 0;

char *ssl_ca = NULL;
char *ssl_cert = NULL;
char *ssl_key = NULL;

gboolean ssl = 0;
gboolean async = 0;

SSL_CTX *ssl_ctx = NULL;

gint ssl_mode = 0;

static GOptionEntry entries[] = {
    {"query", 'Q', 0, G_OPTION_ARG_STRING, &the_query, "Queries to run", NULL},
    {"query-file", 'F', 0, G_OPTION_ARG_STRING, &queryfile,
     "File to read queries from", NULL},
    {"servers-file", 'X', 0, G_OPTION_ARG_STRING, &serversfile,
     "File to read servers from (stdin otherwise)", NULL},
    {"async", 'Z', 0, G_OPTION_ARG_NONE, &async, "Use asynchronuous I/O", NULL},
    {"user", 'u', 0, G_OPTION_ARG_STRING, &username,
     "Username with privileges to run the dump", NULL},
    {"password", 'p', 0, G_OPTION_ARG_STRING, &password, "User password", NULL},
    {"port", 'P', 0, G_OPTION_ARG_INT, &port,
     "Default TCP/IP port to connect to", NULL},
    {"port-increment", 'i', 0, G_OPTION_ARG_INT, &port_incr,
     "Increment TCP/IP port by this", NULL},
    {"socket", 'S', 0, G_OPTION_ARG_STRING, &socket_path,
     "UNIX domain socket file to use for connection", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
     "Databases (comma-separated) to run query against", NULL},
    {"all", 'A', 0, G_OPTION_ARG_NONE, &all_databases,
     "Run on all databases except i_s, mysql and test", NULL},
    {"threads", 't', 0, G_OPTION_ARG_INT, &num_threads,
     "Number of parallel threads", NULL},
    {"dont-escape", 'E', 0, G_OPTION_ARG_NONE, &should_not_escape,
     "Should tabs, newlines and zero bytes be not escaped", NULL},
    {"escape", 'e', 0, G_OPTION_ARG_NONE, &should_escape,
     "Should tabs, newlines and zero bytes be escaped (default)", NULL},
    {"vertical", 'G', 0, G_OPTION_ARG_NONE, &vertical, "Show line per field",
     NULL},
    {"dict", 'D', 0, G_OPTION_ARG_NONE, &prepend_keys, "Prepend field names",
     NULL},
    {"compress", 'c', 0, G_OPTION_ARG_NONE, &should_compress,
     "Compress server-client communication", NULL},
    {"connect-timeout", 'T', 0, G_OPTION_ARG_INT, &connect_timeout,
     "Connect timeout in seconds (default: 2)", NULL},
    {"read-timeout", 'R', 0, G_OPTION_ARG_INT, &read_timeout,
     "Read timeout in seconds", NULL},
    {"tagged", 'x', 0, G_OPTION_ARG_NONE, &tagged_input,
     "Expect tag input column", NULL},
    {"timing", 'z', 0, G_OPTION_ARG_NONE, &timing,
     "Report elapsed time for query", NULL},
    {"ssl", 's', 0, G_OPTION_ARG_NONE, &ssl, "Force real ssl", NULL},
    {"ssl-ca", 0, 0, G_OPTION_ARG_STRING, &ssl_ca, "File with SSL CA info",
     NULL},
    {"ssl-key", 0, 0, G_OPTION_ARG_STRING, &ssl_key, "Private key file", NULL},
    {"ssl-cert", 0, 0, G_OPTION_ARG_STRING, &ssl_cert, "Public key file", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

typedef enum {
  JOB_INIT,
  JOB_CONNECTING,
  JOB_CONNECTED,
  JOB_LIST_DATABASES,
  JOB_FETCH_DATABASES,
  JOB_SELECT_DATABASE,
  JOB_QUERY,
  JOB_FETCHING_RESULT,
  JOB_FETCHING_ROWS,
  JOB_FREE_RESULT,
  JOB_NEXT_RESULT,
} JOB_STATE;

struct job_entry {
  char *server;
  char *database;
  // For fine grained jobs
  char *query;
  char *tag;

  // For multi-db ones
  GQueue *dblist;
  char *in_db;

  // Derived from server and settings
  char *host;
  int port;

  // for async
  JOB_STATE state;
  GIOChannel *channel;

  MYSQL *mysql;
  MYSQL_RES *result;
  gint64 start_time;
};

static void job_mysql_init(struct job_entry *);

#ifdef PMYSQL_ASYNC
static gboolean park(struct job_entry *);
#endif

static pthread_mutex_t *ssl_lockarray;
void pmysql_lock_callback(int mode, int type, const char *file, int line) {
  int c = 0;

  if (file && line)
    c++;

  if (mode & CRYPTO_LOCK)
    pthread_mutex_lock(&(ssl_lockarray[type]));
  else
    pthread_mutex_unlock(&(ssl_lockarray[type]));
}

void init_openssl_locks(void) {
  ssl_lockarray =
      (pthread_mutex_t *)g_new0(pthread_mutex_t, CRYPTO_num_locks());
  for (int i = 0; i < CRYPTO_num_locks(); i++)
    pthread_mutex_init(&(ssl_lockarray[i]), NULL);

  CRYPTO_set_id_callback((unsigned long (*)())pthread_self);
  CRYPTO_set_locking_callback(pmysql_lock_callback);
}

struct job_entry *init_job(const char *server, const char *database,
                           const char *query, const char *tag) {
  struct job_entry *je = g_new0(struct job_entry, 1);
  je->server = strdup(server);

  /* Extract hostname or IP and port from passed server identifier */
  char *h = strdup(server);
  char *host = h;

  char *p;
  /* Handle [::1]:3306 style IPv6 addresses */
  if (h[0] == '[') {
    host++;
    if ((p = strchr(host, ']'))) {
      *p = 0;
      p = strchr(++p, ':');
    }
  } else {
    p = strchr(h, ':');
  }

  if (p) {
    *p++ = 0;
    je->port = atoi(p) + port_incr;
  } else {
    je->port = port;
  }

  if (h == host) {
    je->host = host;
  } else {
    je->host = strdup(host);
    free(h);
  }

  if (database)
    je->database = strdup(database);
  if (query)
    je->query = strdup(query);
  if (tag)
    je->tag = strdup(tag);

  return je;
}

void free_job(struct job_entry *je) {
  free(je->server);
  free(je->host);

  if (je->database)
    free(je->database);
  if (je->query)
    free(je->query);
  if (je->tag)
    free(je->tag);

  if (je->mysql)
    mysql_close(je->mysql);

  if (je->channel)
    g_io_channel_unref(je->channel);

  if (je->in_db)
    free(je->in_db);

  if (je->dblist) {
	  char *p;
    while ((p = g_queue_pop_head(je->dblist)))
      free(p);
    g_queue_free(je->dblist);
  }

  g_free(je);
}

void write_g_string(int fd, GString *data) {
  static GMutex write_mutex;
  ssize_t written = 0, r = 0;

  g_mutex_lock(&write_mutex);
  while (written < (ssize_t)data->len) {
    r = write(fd, data->str + written, data->len);
    if (r < 0) {
      g_critical("Couldn't write output data to a file: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }
    written += r;
  }
  g_mutex_unlock(&write_mutex);
}

/* Escape few whitespace chars,
   needs preallocated output buffer
   and source length provided */

gulong line_escape(char *from, gulong length, char *to) {

  char *p = from;
  char *t = to;

  while (length--) {
    switch (*p) {
    case '\n':
      *t++ = '\\';
      *t++ = 'n';
      break;
    case '\t':
      *t++ = '\\';
      *t++ = 't';
      break;
    case 0:
      *t++ = '\\';
      *t++ = '0';
      break;
    default:
      *t++ = *p;
    }
    p++;
  }
  *t++ = 0;
  return t - to;
}

static inline void g_private_string_destroy(void *p) { g_string_free(p, TRUE); }

static GPrivate rowtext_key = G_PRIVATE_INIT(g_private_string_destroy);
static GPrivate escaped_key = G_PRIVATE_INIT(g_private_string_destroy);

static inline GString *init_private_string(GPrivate *private) {
  GString *ret = g_private_get(private);
  if (!ret) {
    ret = g_string_new(NULL);
    g_private_set(private, ret);
  }
  return ret;
}

void print_row(struct job_entry *je, MYSQL_ROW row) {
  char *timing_value = timing ? g_newa(char, 30) : NULL;
  GString *rowtext = init_private_string(&rowtext_key);
  GString *escaped = init_private_string(&escaped_key);
  unsigned long *lengths = mysql_fetch_lengths(je->result);
  MYSQL_FIELD *fields = mysql_fetch_fields(je->result);

  int num_fields = mysql_num_fields(je->result);

  if (timing) {
    snprintf(timing_value, 30, "\t%.3f",
             (g_get_monotonic_time() - je->start_time) / 1000000.0);
  }
  char *tag = je->tag;
  char *db_name = je->in_db ? je->in_db : je->database;

  if (!vertical) {

    g_string_printf(rowtext, "%s%s%s%s%s%s\t", je->server, tag ? "\t" : "",
                    tag ? tag : "", db_name ? "\t" : "", db_name ? db_name : "",
                    timing ? timing_value : "");

    for (int i = 0; i < num_fields; i++) {
      if (!should_escape || !row[i]) {
        if (prepend_keys) {
          g_string_append(rowtext, fields[i].name);
          g_string_append(rowtext, ":");
        }
        g_string_append(rowtext, row[i] ? row[i] : "\\N");
        g_string_append(rowtext, (num_fields - i == 1) ? "\n" : "\t");
      } else {
        if (prepend_keys) {

          g_string_set_size(escaped, fields[i].name_length * 2 + 1);
          line_escape(fields[i].name, fields[i].name_length, escaped->str);

          g_string_append(rowtext, escaped->str);
          g_string_append(rowtext, ":");
        }

        g_string_set_size(escaped, lengths[i] * 2 + 1);
        line_escape(row[i], lengths[i], escaped->str);
        g_string_append(rowtext, escaped->str);
        g_string_append(rowtext, (num_fields - i == 1) ? "\n" : "\t");
      }
    }
    write_g_string(STDOUT_FILENO, rowtext);
  } else {
    for (int i = 0; i < num_fields; i++) {
      g_string_printf(rowtext, "%s%s%s%s%s\t%s\t", je->server, tag ? "\t" : "",
                      tag ? tag : "", db_name ? "\t" : "",
                      db_name ? db_name : "", fields[i].name);

      if (!row[i]) {
        g_string_append(rowtext, "\\N\n");
      } else if (!should_escape) {
        g_string_append(rowtext, row[i]);
        g_string_append(rowtext, "\n");
      } else {
        g_string_set_size(escaped, lengths[i] * 2 + 1);
        line_escape(row[i], lengths[i], escaped->str);
        g_string_append(rowtext, escaped->str);
        g_string_append(rowtext, "\n");
      }
      write_g_string(STDOUT_FILENO, rowtext);
    }
  }
}

#ifdef PMYSQL_ASYNC
gboolean run_query_async_cb(GIOChannel *source, GIOCondition cond,
                            struct job_entry *je) {
  int db_error;
  int ret;

  MYSQL_ROW row;

  (void)source;
  (void)cond;

  switch (je->state) {
  case JOB_INIT:
  case JOB_CONNECTING:
    je->state = JOB_CONNECTING;

    ret = mysql_real_connect_nonblocking_run(je->mysql, &db_error);
    if (ret != NET_ASYNC_COMPLETE)
      return park(je);

    if (db_error) {
      g_critical("%s", mysql_error(je->mysql));
      free_job(je);
      return FALSE;
    }

  case JOB_CONNECTED:
    je->state = JOB_CONNECTED;
    je->start_time = g_get_monotonic_time();

    /* First connected session will store its SSL context globally,
     * there will be other ones in flight that will have their own,
     * but no more than there're workers */

    static gsize ssl_ctx_init;
    if (g_once_init_enter(&ssl_ctx_init)) {
      ssl_ctx = mysql_take_ssl_context_ownership(je->mysql);
      g_once_init_leave(&ssl_ctx_init, 1);
    }
  case JOB_LIST_DATABASES:
    je->state = JOB_LIST_DATABASES;
    if (all_databases) {
      char *query = "SHOW DATABASES";
      ret = mysql_real_query_nonblocking(je->mysql, query, strlen(query),
                                         &db_error);
      if (ret != NET_ASYNC_COMPLETE)
        return park(je);

      if (db_error) {
        g_critical("%s", mysql_error(je->mysql));
        free_job(je);
        return FALSE;
      }
      je->result = mysql_use_result(je->mysql);
      if (!je->result) {
        g_critical("Could not retrieve database list: %s",
                   mysql_error(je->mysql));
        free_job(je);
        return FALSE;
      }
    }
  case JOB_FETCH_DATABASES:
    je->state = JOB_FETCH_DATABASES;
    if (databases) {
       if (!je->dblist)
         je->dblist = g_queue_new();
       for (int i = 0; databases[i]; i++)
         g_queue_push_tail(je->dblist, strdup(databases[i]));
    }


    if (all_databases) {
      if (!je->dblist)
        je->dblist = g_queue_new();

      while (je->result) {
        ret = mysql_fetch_row_nonblocking(je->result, &row);
        if (ret != NET_ASYNC_COMPLETE)
          return park(je);

        if (row) {
          g_queue_push_tail(je->dblist, strdup(row[0]));
        } else {
          mysql_free_result(je->result);
          je->result = NULL;
        }
      }
    }

  case JOB_SELECT_DATABASE:
  select_db:
    je->state = JOB_SELECT_DATABASE;
    if (je->dblist) {
      if (!je->in_db) {
        je->in_db = g_queue_pop_head(je->dblist);
      }

      if (!je->in_db) {
        free_job(je);
        return FALSE;
      }

      char *query = g_strdup_printf("USE `%s`", je->in_db);
      ret = mysql_real_query_nonblocking(je->mysql, query, strlen(query), &db_error);
      free(query);
      if (ret == NET_ASYNC_NOT_READY) {
        return park(je);
      }

      if (db_error && mysql_errno(je->mysql)) {
        g_critical("%s", mysql_error(je->mysql));
        free_job(je);
        return FALSE;
      }
    }

  case JOB_QUERY:
    je->state = JOB_QUERY;
    char *query = je->query ? je->query : the_query;
    ret = mysql_real_query_nonblocking(je->mysql, query, strlen(query),
                                       &db_error);
    if (ret != NET_ASYNC_COMPLETE)
      return park(je);

    if (db_error) {
      g_critical("%s", mysql_error(je->mysql));
      free_job(je);
      return FALSE;
    }

  case JOB_FETCHING_RESULT:

    je->result = mysql_use_result(je->mysql);
    if (!je->result) {
      if (mysql_field_count(je->mysql) != 0) {
        g_critical("Could not retrieve result set from %s: %s", je->server,
                   mysql_error(je->mysql));
        free_job(je);
        return FALSE;
      }
    }

  case JOB_FETCHING_ROWS:
    je->state = JOB_FETCHING_ROWS;
    while (je->result) {
      ret = mysql_fetch_row_nonblocking(je->result, &row);
      if (ret != NET_ASYNC_COMPLETE)
        return park(je);

      if (row)
        print_row(je, row);

      if (mysql_errno(je->mysql)) {
        g_critical("Could not fully retrieve results from %s: %s", je->server,
                   mysql_error(je->mysql));
        free_job(je);
        return FALSE;
      }

      if (!row)
        break;
    }
  case JOB_FREE_RESULT:
    je->state = JOB_FREE_RESULT;
    if (je->result) {
      if (mysql_free_result_nonblocking(je->result) != NET_ASYNC_COMPLETE)
        return park(je);
    }
    je->result = NULL;

  case JOB_NEXT_RESULT:
    je->state = JOB_NEXT_RESULT;
    ret = mysql_next_result_nonblocking(je->mysql, &db_error);
    if (ret != NET_ASYNC_COMPLETE)
      return park(je);
    if (db_error > 0) {
      g_critical("Could not execute statement on %s: %s", je->server,
                 mysql_error(je->mysql));
    } else if (db_error == 0) {
      je->state = JOB_FETCHING_RESULT;
      return park(je);
    }
  default:
    if (je->dblist) {
      free(je->in_db);
      je->in_db = NULL;
      goto select_db;
    }
    free_job(je);
    return FALSE;
  }
}

gboolean park(struct job_entry *je) {
  GIOCondition cond;
  switch (je->mysql->net.async_blocking_state) {
  case NET_NONBLOCKING_WRITE:
    cond = G_IO_OUT;
    break;
  case NET_NONBLOCKING_READ:
    cond = G_IO_IN;
    break;
  default:
    // Need to be sure connection is writable, if it is not waiting
    cond = G_IO_OUT;
    break;
  }

  g_event_pool_wait((GIOFunc)run_query_async_cb, je->channel, cond, je);
  return FALSE;
}

int run_query_async_init(GIOChannel *src, GIOCondition cond, gpointer data) {
  g_assert(!src);
  g_assert(!cond);

  struct job_entry *je = data;
  int db_error;

  je->state = JOB_INIT;
  job_mysql_init(je);

  int client_flags = CLIENT_MULTI_STATEMENTS; // XXX: flags
  mysql_real_connect_nonblocking_init(je->mysql, je->host, username, password,
                                      je->database ? je->database : db,
                                      je->port, socket_path, client_flags);

  je->state = JOB_CONNECTING;
  int ret = mysql_real_connect_nonblocking_run(je->mysql, &db_error);
  int fd = mysql_get_file_descriptor(je->mysql);
  if (fd == -1) {
    g_critical("%s", mysql_error(je->mysql));
    free_job(je);
    return FALSE;
  }
  je->channel = g_io_channel_unix_new(fd);

  if (ret == NET_ASYNC_COMPLETE) {
    if (db_error) {
      g_critical("%s", mysql_error(je->mysql));
      free_job(je);
      return FALSE;
    }
    je->state = JOB_CONNECTED;
  }
  return park(je);
}

#endif /* PMYSQL_ASYNC */

void run_query(struct job_entry *je) {
  int status;

  MYSQL *mysql = je->mysql;
  MYSQL_ROW row = NULL;
  if (je->database) {
    if (mysql_select_db(mysql, je->database)) {
      g_warning("Could not select db %s on %s: %s", je->database, je->server,
                mysql_error(mysql));
      return;
    }
  }

  if (timing)
    je->start_time = g_get_monotonic_time();

  char *query = je->query ? je->query : the_query;
  if ((status = mysql_query(mysql, query))) {
    g_warning("Could not execute query on %s: %s", je->server,
              mysql_error(mysql));
  }

  do {
    je->result = mysql_use_result(mysql);
    if (je->result) {
      while ((row = mysql_fetch_row(je->result))) {
        print_row(je, row);
      }

      if (mysql_errno(mysql))
        g_critical("Could not retrieve result set fully from %s: %s",
                   je->server, mysql_error(mysql));

      mysql_free_result(je->result);
      je->result = NULL;
    } else { /* no result set or error */
      if (mysql_field_count(mysql) != 0) {
        g_critical("Could not retrieve result set from %s: %s", je->server,
                   mysql_error(mysql));
        break;
      }
    }
    if ((status = mysql_next_result(mysql)) > 0)
      g_critical("Could not execute statement on %s: %s", je->server,
                 mysql_error(mysql));
  } while (status == 0);
}

void run_query_on_db(struct job_entry *je, char *db) {
  char *prev_db = je->database;
  je->database = db;
  run_query(je);
  je->database = prev_db;
}

/* Search for string in NULL-terminated array */
gboolean str_in_array(const char *string, const char **array) {
  for (const char **s = &array[0]; *s != NULL; s++) {
    if (!strcmp(*s, string))
      return (TRUE);
  }
  return (FALSE);
}

static void job_mysql_init(struct job_entry *je) {
  MYSQL *mysql = je->mysql = mysql_init(NULL);

  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "pmysql");
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT,
                (const char *)&connect_timeout);

  if (ssl_ctx)
    mysql_options(mysql, MYSQL_OPT_SSL_CONTEXT, ssl_ctx);

  if (read_timeout)
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (const char *)&read_timeout);

#ifdef MYSQL_OPT_SSL_MODE
  if (ssl)
    ssl_mode = SSL_MODE_VERIFY_IDENTITY;

  mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);
#endif
  mysql_ssl_set(mysql, ssl_key, ssl_cert, ssl_ca, NULL, NULL);
}

static void run_job(struct job_entry *je) {
  job_mysql_init(je);

  gulong client_flags = CLIENT_MULTI_STATEMENTS;
  if (should_compress)
    client_flags |= CLIENT_COMPRESS;

  if (!mysql_real_connect(je->mysql, je->host, username, password,
                          je->database ? je->database : db, je->port,
                          socket_path, client_flags)) {
    g_warning("Could not connect to %s: %s", je->server,
              mysql_error(je->mysql));
    goto cleanup;
  }

  static gsize ssl_ctx_init;
  if (g_once_init_enter(&ssl_ctx_init)) {
    ssl_ctx = mysql_take_ssl_context_ownership(je->mysql);
    g_once_init_leave(&ssl_ctx_init, 1);
  }

  /* We run query on all databases except
     mysql,test,information_schema,performance_schema if --all is specified

     If --database has been specified, it is merged with full db list without
     checking for dupes (so query may be executed twice)
  */
  if (all_databases) {
    MYSQL_RES *res = NULL;
    MYSQL_ROW row = NULL;
    int status;

    if ((status = mysql_query(je->mysql, "SHOW DATABASES"))) {
      g_warning("Could not get list of databases");
      goto cleanup;
    }

    if (!(res = mysql_store_result(je->mysql))) {
      g_warning("Could not get list of databases");
      goto cleanup;
    }

    while ((row = mysql_fetch_row(res))) {
      if (!str_in_array(row[0], mdbs))
        run_query_on_db(je, row[0]);
    }
    mysql_free_result(res);

    /* Merge --database if specified (both for multiple and single case) */
    if (databases) {
      for (int i = 0; databases[i]; i++) {
        run_query_on_db(je, databases[i]);
      }
    } else if (db) {
      run_query_on_db(je, db);
    }
  } else {
    /* Run on all specified databases */
    if (databases) {
      for (int i = 0; databases[i]; i++) {
        run_query_on_db(je, databases[i]);
      }
    } else {
      run_query(je);
    }
  }

cleanup:
  free_job(je);
}

static void worker_thread(gpointer data, gpointer user_data) {
  (void)user_data;
  struct job_entry *je = (struct job_entry *)data;
  run_job(je);
#ifdef DEBUG
  mysql_thread_end();
#endif
}

/* This autodetects input format, which is somewhat dynamic.
 * One column: server
 * Two columns: server, query
 * Three columns: server, db or tag, query
 * Four columns: server, tag, db, query
 */
struct job_entry *read_job(FILE *fd) {
  char line[MAXLINE];
  for (;;) {
    if (!fgets(line, MAXLINE - 1, fd))
      return NULL;

    if (line[0] == '#')
      continue;

    char *nl = strchr(line, '\n');
    if (nl)
      nl[0] = 0;

    char *last = line;
    char *server = strsep(&last, "\t");
    if (!server)
      continue;

    char *second = strsep(&last, "\t");
    if (!second)
      return init_job(server, NULL, NULL, NULL);

    /* Nothing remaining, second column was query */
    if (!last)
      return init_job(server, NULL, second, NULL);

    char *third = strsep(&last, "\t");
    if (!*second)
      second = NULL;
    /* Three columns, middle is either a tag or a db */
    if (!last) {
      if (tagged_input) {
        return init_job(server, NULL, third, second);
      } else {
        return init_job(server, second, third, NULL);
      }
    } else {
      if (!*third)
        third = NULL;

      return init_job(server, third, last, second);
    }
  }
}

int main(int argc, char **argv) {
  GError *error = NULL;

  FILE *serversfd;

  /* Command line option parsing */
  GOptionContext *context = g_option_context_new("[query]");
  g_option_context_set_summary(context,
                               "Parallel multiple-server MySQL querying tool");

  g_option_context_add_main_entries(context, entries, NULL);
  if (!g_option_context_parse(context, &argc, &argv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }
  g_option_context_free(context);

  /* Option postprocessing */
  if (queryfile) {
    if (the_query || argc > 1) {
      g_critical("Both query and query-file provided, "
                 "they are mutually exclusive");
      exit(EXIT_FAILURE);
    }

    if (!g_file_get_contents(queryfile, &the_query, NULL, &error)) {
      g_critical("Could not read query file (%s): %s", queryfile,
                 error->message);
      exit(EXIT_FAILURE);
    }
  }

  if ((the_query && argc > 1) || argc > 2) {
    g_critical("Multiple query arguments provided, "
               "use ; separated list for multiple queries");
    exit(EXIT_FAILURE);
  }

  if (!the_query && !queryfile) {
    if (argc < 2) {
      g_critical("Need query!");
      exit(EXIT_FAILURE);
    }
    the_query = argv[1];
  }

  if (should_not_escape)
    should_escape = FALSE;

  mysql_library_init(0, NULL, NULL);
  MYSQL *mysql = mysql_init(NULL);

  mysql_thread_init();
  init_openssl_locks();

  mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "pmysql");

  /* This is fake connect to remember various options from my.cnf
     for other threads
     Do note, this connect may succeed :-)
  */
  mysql_real_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL,
                     CLIENT_REMEMBER_OPTIONS);

  if (!username)
    username = mysql->options.user;
  if (!password)
    password = mysql->options.password;
  if (!port)
    port = mysql->options.port;

  /* Support for a list of databases */
  if (db && strchr(db, ',')) {
    databases = g_strsplit(db, ",", 0);
    *strchr(db, ',') = 0;
  }

  if (!socket_path)
    socket_path = mysql->options.unix_socket;

  if (!connect_timeout) {
    if (TIMEOUT(mysql->options.connect_timeout))
      connect_timeout = TIMEOUT(mysql->options.connect_timeout);
    else
      connect_timeout = CONNECT_TIMEOUT;
  }

  if (serversfile && strcmp(serversfile, "-")) {
    serversfd = fopen(serversfile, "r");
    if (!serversfd) {
      g_critical("Could not open servers list (%s): %s", serversfile,
                 strerror(errno));
      exit(EXIT_FAILURE);
    }
  } else {
    serversfd = stdin;
  }

  struct job_entry *je;

  if (!async) {
    GThreadPool *tp = g_thread_pool_new(
        worker_thread, NULL, num_threads ? num_threads : default_num_threads,
        TRUE, NULL);

    while ((je = read_job(serversfd))) {
      g_thread_pool_push(tp, je, NULL);
    }

    g_thread_pool_free(tp, FALSE, TRUE);
  } else {
#ifdef PMYSQL_ASYNC
    GEventPool *pool =
        g_event_pool_new(run_query_async_init, NULL,
                         num_threads ? num_threads : get_nprocs(), NULL);
    while ((je = read_job(serversfd))) {
      g_event_pool_push(pool, je);
    }
    g_event_pool_shutdown(pool, TRUE);
#else
    g_critical("Async I/O not supported in this build");
#endif
  }

#ifdef DEBUG
  mysql_close(mysql);
  mysql_thread_end();
  mysql_library_end();
  fclose(serversfd);
#endif

  exit(EXIT_SUCCESS);
}
