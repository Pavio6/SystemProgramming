#include "thread_pool.h"

#include <cmath>
#include <ctime>
#include <deque>
#include <errno.h>
#include <pthread.h>
#include <vector>

enum task_state {
	TASK_STATE_NEW = 0,
	TASK_STATE_QUEUED,
	TASK_STATE_RUNNING,
	TASK_STATE_FINISHED,
};

struct thread_task {
	thread_task_f function;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	task_state state;
	bool was_pushed;
	bool is_joined;
	bool is_detached;
};

struct thread_pool {
	std::vector<pthread_t> threads;
	std::deque<thread_task *> queue;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int max_threads;
	int active_tasks;
	bool is_stopping;
};

static void
thread_task_destroy(struct thread_task *task)
{
	pthread_cond_destroy(&task->cond);
	pthread_mutex_destroy(&task->mutex);
	delete task;
}

static int
thread_task_join_impl(struct thread_task *task, bool has_timeout, double timeout)
{
	pthread_mutex_lock(&task->mutex);
	if (!task->was_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (!has_timeout) {
		while (task->state != TASK_STATE_FINISHED)
			pthread_cond_wait(&task->cond, &task->mutex);
		task->is_joined = true;
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}

	if (timeout <= 0) {
		if (task->state != TASK_STATE_FINISHED) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
		task->is_joined = true;
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}

	if (!std::isfinite(timeout)) {
		while (task->state != TASK_STATE_FINISHED)
			pthread_cond_wait(&task->cond, &task->mutex);
		task->is_joined = true;
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}

	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	double intpart = 0;
	double fracpart = modf(timeout, &intpart);
	ts.tv_sec += static_cast<time_t>(intpart);
	ts.tv_nsec += static_cast<long>(fracpart * 1000000000.0);
	if (ts.tv_nsec >= 1000000000L) {
		ts.tv_sec += 1;
		ts.tv_nsec -= 1000000000L;
	}

	while (task->state != TASK_STATE_FINISHED) {
		int rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
		if (rc == ETIMEDOUT && task->state != TASK_STATE_FINISHED) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
	}
	task->is_joined = true;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

static void *
thread_pool_worker(void *arg)
{
	thread_pool *pool = static_cast<thread_pool *>(arg);
	while (true) {
		pthread_mutex_lock(&pool->mutex);
		while (pool->queue.empty() && !pool->is_stopping)
			pthread_cond_wait(&pool->cond, &pool->mutex);
		if (pool->queue.empty() && pool->is_stopping) {
			pthread_mutex_unlock(&pool->mutex);
			break;
		}
		thread_task *task = pool->queue.front();
		pool->queue.pop_front();
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->state = TASK_STATE_RUNNING;
		pthread_mutex_unlock(&task->mutex);

		task->function();

		bool should_destroy = false;
		pthread_mutex_lock(&task->mutex);
		task->state = TASK_STATE_FINISHED;
		should_destroy = task->is_detached;
		if (!should_destroy)
			pthread_cond_broadcast(&task->cond);
		pthread_mutex_unlock(&task->mutex);

		pthread_mutex_lock(&pool->mutex);
		--pool->active_tasks;
		pthread_mutex_unlock(&pool->mutex);

		if (should_destroy)
			thread_task_destroy(task);
	}
	return NULL;
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;
	thread_pool *p = new thread_pool();
	pthread_mutex_init(&p->mutex, NULL);
	pthread_cond_init(&p->cond, NULL);
	p->max_threads = thread_count;
	p->active_tasks = 0;
	p->is_stopping = false;
	*pool = p;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->active_tasks != 0 || !pool->queue.empty()) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}
	pool->is_stopping = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);

	for (pthread_t tid : pool->threads)
		pthread_join(tid, NULL);
	pthread_cond_destroy(&pool->cond);
	pthread_mutex_destroy(&pool->mutex);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->active_tasks >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	task->state = TASK_STATE_QUEUED;
	task->was_pushed = true;
	task->is_joined = false;
	task->is_detached = false;
	pthread_mutex_unlock(&task->mutex);

	pool->queue.push_back(task);
	++pool->active_tasks;

	if (static_cast<int>(pool->threads.size()) < pool->max_threads &&
	    static_cast<int>(pool->threads.size()) < pool->active_tasks) {
		pthread_t tid;
		pthread_create(&tid, NULL, thread_pool_worker, pool);
		pool->threads.push_back(tid);
	}
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	thread_task *t = new thread_task();
	t->function = function;
	pthread_mutex_init(&t->mutex, NULL);
	pthread_cond_init(&t->cond, NULL);
	t->state = TASK_STATE_NEW;
	t->was_pushed = false;
	t->is_joined = false;
	t->is_detached = false;
	*task = t;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t *>(&task->mutex));
	bool is_finished = task->state == TASK_STATE_FINISHED && task->is_joined;
	pthread_mutex_unlock(const_cast<pthread_mutex_t *>(&task->mutex));
	return is_finished;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t *>(&task->mutex));
	bool is_running = task->state == TASK_STATE_RUNNING;
	pthread_mutex_unlock(const_cast<pthread_mutex_t *>(&task->mutex));
	return is_running;
}

int
thread_task_join(struct thread_task *task)
{
	return thread_task_join_impl(task, false, 0);
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	return thread_task_join_impl(task, true, timeout);
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	bool is_pushed_not_joined = task->was_pushed && !task->is_joined;
	bool is_running = task->state == TASK_STATE_QUEUED ||
			  task->state == TASK_STATE_RUNNING;
	if (is_running || is_pushed_not_joined) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	pthread_mutex_unlock(&task->mutex);
	thread_task_destroy(task);
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (!task->was_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (task->state == TASK_STATE_FINISHED) {
		pthread_mutex_unlock(&task->mutex);
		thread_task_destroy(task);
		return 0;
	}
	task->is_detached = true;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif
