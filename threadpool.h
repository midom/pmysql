#include <glib.h>

typedef struct GEventPool GEventPool;

/* GEventPool creates multiple threads (Runners), each of them running their own
 * asynchronuous event loop. Pool can be created with g_event_pool_new() and
 * destroyed with g_event_pool_shutdown()
 *
 * New job can be inserted with g_event_pool_push(), and each job can set up how
 * to resume with g_event_pool_wait() or g_event_pool_timed_wait()
 *
 * Clients have to pass GIOChannel-wrapped objects to set up event loop waits.
 */

struct GEventPoolRunner {
  GEventPool* pool;
  GMainContext* context;
  GThread* thread;
  guint tag;
};
typedef struct GEventPoolRunner GEventPoolRunner;

struct GEventPool {
  GIOFunc func;
  gpointer user_data;
  // Would gladly use GWakeup, if public
  int eventfd;

  GAsyncQueue* queue;
  _Atomic gboolean quit;
  GEventPoolRunner** threads;
  int num_threads;
};

typedef struct GEventPool GEventPool;
gboolean g_event_pool_push(GEventPool*, void*);
GEventPool* g_event_pool_new(GIOFunc, gpointer, gint, GError**);
void g_event_pool_wait(GIOFunc, GIOChannel*, GIOCondition, gpointer);
void g_event_pool_timed_wait(
    GIOFunc,
    GIOChannel*,
    GIOCondition,
    gpointer,
    guint);
void g_event_pool_shutdown(GEventPool*, gboolean);
