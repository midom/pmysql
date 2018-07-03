#define _GNU_SOURCE

#include "threadpool.h"
#include <glib.h>
#include <stdio.h>
#include <sys/eventfd.h>
#include <unistd.h>

int g_event_pool_queue_task(GSource *source, GIOCondition condition,
                            void *user_data) {
  (void)source;
  g_assert(condition == G_IO_IN);
  GEventPool *pool = user_data;
  void *job = g_async_queue_try_pop(pool->queue);
  if (!job)
    return !pool->quit;
  guint64 i;
  int r = read(pool->eventfd, &i, sizeof(i));
  if (r != 8)
    g_critical("Wrong read from eventfd: %d", r);


  pool->func(NULL, 0, job);
  return TRUE;
}

gboolean g_event_pool_push(GEventPool *pool, void *user_data) {
  g_async_queue_push(pool->queue, user_data);
  guint64 i = 1;
  int r = write(pool->eventfd, &i, sizeof(i));
  if (r != 8)
    return FALSE;
  return TRUE;
}

void g_event_pool_wait(GIOFunc func, GIOChannel *channel,
                       GIOCondition condition, gpointer data) {
  GSource *source = g_io_create_watch(channel, condition);
  g_source_set_callback(source, (GSourceFunc)func, data, NULL);
  g_source_attach(source, g_main_context_get_thread_default());
  g_source_unref(source);
}

gboolean g_event_pool_runner_cleanup(GEventPoolRunner *runner) {
  GMainContext *context = runner->context;

  gint timeout;

  GSource *source = g_main_context_find_source_by_id(context, runner->tag);
  if (source)
	  g_source_destroy(source);

  GPollFD fds[3];
  while (g_main_context_query(context, 0, &timeout, fds, 3)>2)
	  g_main_context_iteration(context, TRUE);

  return FALSE;
}

void g_event_pool_runner(struct GEventPoolRunner *runner) {
  GEventPool *pool = runner->pool;
  GMainContext *context = runner->context;
  // GMainContext *context = g_main_context_new();
  g_main_context_push_thread_default(context);
  // GMainLoop *loop = g_main_loop_new(context, FALSE);
  GIOChannel *channel = g_io_channel_unix_new(pool->eventfd);
  // g_io_channel_set_encoding(channel, NULL, &error);

  GSource *source = g_io_create_watch(channel, G_IO_IN);
  g_source_set_callback(source, (GSourceFunc)g_event_pool_queue_task, pool,
                        NULL);
  g_source_attach(source, context);
  g_source_unref(source);

  /* We loop when we're:
   * 1. not shut down
   * 2. still have pending stuff in queue
   * 3. iterations keep doing work
   * 4. have FDs to poll
   * */
  while (!pool->quit)
    g_main_context_iteration(context, TRUE);

  while (g_async_queue_length(pool->queue))
    g_main_context_iteration(context, TRUE);

  while (g_main_context_iteration(context, FALSE));


  /* There is no clener way to check if eventloop has
   * more events than itself, so we do it this way.
   *
   * Alternative would be tracking all the pending contexts
   * outside of the GMainContext
   * */
  g_source_destroy(source);
  GPollFD fds[2];
  int timeout;
  while (g_main_context_query(context, 0, &timeout, fds, 2) > 1)
	  g_main_context_iteration(context, TRUE);


  /* 

  GSource *task = g_idle_source_new();
  g_source_set_callback(task, (GSourceFunc)g_event_pool_runner_cleanup, runner, NULL);
  runner->tag = g_source_attach(task, g_main_context_get_thread_default());
  g_source_unref(task);

  g_main_context_iteration(context, TRUE);
  */
}

void g_event_pool_shutdown(GEventPool *pool, gboolean wait) {
  pool->quit = TRUE;
  for (int i = 0; i < pool->num_threads; i++) {
    GEventPoolRunner *runner = pool->threads[i];
    g_main_context_wakeup(runner->context);
    if (wait)
      g_thread_join(runner->thread);
  }
}

GEventPool *g_event_pool_new(GIOFunc func, gpointer user_data,
                             gint num_threads, GError **error) {

  (void)error;

  GEventPool *pool = g_new0(GEventPool, 1);
  pool->eventfd = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK);
  pool->queue = g_async_queue_new();
  pool->user_data = user_data;
  pool->func = func;
  pool->threads = g_new0(GEventPoolRunner *, num_threads);
  pool->num_threads = num_threads;

  for (int i = 0; i < num_threads; i++) {
    struct GEventPoolRunner *runner = g_new0(struct GEventPoolRunner, 1);
    runner->pool = pool;
    runner->context = g_main_context_new();
    runner->thread =
        g_thread_new(NULL, (GThreadFunc)g_event_pool_runner, runner);
    pool->threads[i] = runner;
  }
  return pool;
}
