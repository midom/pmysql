#include <mysql.h>

enum net_async_block_state {
  NET_NONBLOCKING_CONNECT = 0,
  NET_NONBLOCKING_READ,
  NET_NONBLOCKING_WRITE
};

typedef struct NET_ASYNC {
  unsigned char* cur_pos;
  enum net_async_block_state async_blocking_state;
} NET_ASYNC;

typedef struct NET_EXTENSION {
  NET_ASYNC* net_async_context;
  mysql_compress_context compress_ctx;
} NET_EXTENSION;

#define NET_EXTENSION_PTR(N) \
  ((NET_EXTENSION*)((N)->extension ? (N)->extension : NULL))

#define NET_ASYNC_DATA(M) \
  ((NET_EXTENSION_PTR(M)) ? (NET_EXTENSION_PTR(M)->net_async_context) : NULL)
