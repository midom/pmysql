#include <glib.h>

typedef struct GEventPool GEventPool;
typedef struct GEventPoolRunner GEventPoolRunner;

struct GEventPoolRunner {
        GEventPool *pool;
        GMainContext *context;
	GThread *thread;
	guint tag;
};

struct GEventPool {
  GIOFunc func;
  gpointer user_data;
  // Would gladly use GWakeup, if public
  int eventfd;

  GAsyncQueue *queue;
  gboolean quit;
  GEventPoolRunner **threads;
  int num_threads;
  int count;
};

typedef struct GEventPool GEventPool;
gboolean g_event_pool_push(GEventPool *, void *);
GEventPool *g_event_pool_new(GIOFunc, gpointer,
                             gint, GError **);
void g_event_pool_wait(GIOFunc, GIOChannel *, GIOCondition , gpointer);
void g_event_pool_shutdown(GEventPool *, gboolean);
