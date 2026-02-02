#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
	struct coro_bus_channel *channel;
	bool in_queue;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_init(struct wakeup_queue *queue)
{
	rlist_create(&queue->coros);
}
struct coro_bus_channel {
	size_t size_limit;
	struct wakeup_queue send_queue;
	struct wakeup_queue recv_queue;
	bool is_closed;
	int waiters;
	struct rlist zombie_link;
	bool in_zombie;
	struct coro_bus *bus;
	int index;
	unsigned *data;
	size_t data_size;
	size_t data_head;
	size_t data_tail;
};
struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
	int channel_capacity;
	struct rlist zombies;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}
void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static void
channel_cleanup_if_possible(struct coro_bus_channel *ch)
{
	// Defer free until channel is closed and no coroutine waits on it.
	if (!ch->is_closed || ch->waiters != 0)
		return;
	if (ch->in_zombie) {
		rlist_del_entry(ch, zombie_link);
		ch->in_zombie = false;
	}
	delete[] ch->data;
	delete ch;
}
static void
wakeup_queue_wakeup_one(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	rlist_del_entry(entry, base);
	entry->in_queue = false;
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	// Wake every suspended coroutine in FIFO order.
	while (!rlist_empty(&queue->coros)) {
		struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
			struct wakeup_entry, base);
		rlist_del_entry(entry, base);
		entry->in_queue = false;
		coro_wakeup(entry->coro);
	}
}

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue,
	struct coro_bus_channel *channel)
{
	struct wakeup_entry *entry = new wakeup_entry;
	entry->coro = coro_this();
	entry->channel = channel;
	entry->in_queue = true;
	rlist_add_tail_entry(&queue->coros, entry, base);
	++channel->waiters;
	// Suspend until another coroutine wakes us up.
	coro_suspend();

	if (entry->in_queue) {
		rlist_del_entry(entry, base);
		entry->in_queue = false;
	}
	--channel->waiters;
	channel_cleanup_if_possible(channel);
	delete entry;
}

static struct coro_bus_channel *
channel_get(struct coro_bus *bus, int channel)
{
	// Validate bus/channel and return NULL with errno on failure.
	if (bus == NULL || channel < 0 || channel >= bus->channel_count ||
		bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return NULL;
	}
	return bus->channels[channel];
}

static bool
channel_has_space(struct coro_bus_channel *ch)
{
	return ch->data_size < ch->size_limit;
}

static bool
channel_has_data(struct coro_bus_channel *ch)
{
	return ch->data_size > 0;
}

static void
channel_push(struct coro_bus_channel *ch, unsigned data)
{
	// Ring-buffer push.
	ch->data[ch->data_tail] = data;
	ch->data_tail = (ch->data_tail + 1) % ch->size_limit;
	++ch->data_size;
}

static unsigned
channel_pop(struct coro_bus_channel *ch)
{
	// Ring-buffer pop.
	unsigned data = ch->data[ch->data_head];
	ch->data_head = (ch->data_head + 1) % ch->size_limit;
	--ch->data_size;
	return data;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = new coro_bus;
	bus->channels = NULL;
	bus->channel_count = 0;
	bus->channel_capacity = 0;
	rlist_create(&bus->zombies);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		delete[] ch->data;
		delete ch;
	}
	while (!rlist_empty(&bus->zombies)) {
		struct coro_bus_channel *ch = rlist_first_entry(&bus->zombies,
			struct coro_bus_channel, zombie_link);
		rlist_del_entry(ch, zombie_link);
		delete[] ch->data;
		delete ch;
	}
	delete[] bus->channels;
	delete bus;
}
int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	// Reuse a free slot if possible, otherwise extend the table.
	int index = -1;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			index = i;
			break;
		}
	}
	if (index < 0) {
		if (bus->channel_count == bus->channel_capacity) {
			int new_cap = bus->channel_capacity == 0 ? 4 :
				bus->channel_capacity * 2;
			struct coro_bus_channel **new_arr =
				new struct coro_bus_channel*[new_cap];
			for (int i = 0; i < new_cap; ++i)
				new_arr[i] = NULL;
			for (int i = 0; i < bus->channel_count; ++i)
				new_arr[i] = bus->channels[i];
			delete[] bus->channels;
			bus->channels = new_arr;
			bus->channel_capacity = new_cap;
		}
		index = bus->channel_count++;
	}
	struct coro_bus_channel *ch = new coro_bus_channel;
	ch->size_limit = size_limit;
	wakeup_queue_init(&ch->send_queue);
	wakeup_queue_init(&ch->recv_queue);
	ch->is_closed = false;
	ch->waiters = 0;
	ch->bus = bus;
	ch->index = index;
	rlist_create(&ch->zombie_link);
	ch->in_zombie = false;
	ch->data_size = 0;
	ch->data_head = 0;
	ch->data_tail = 0;
	if (size_limit > 0)
		ch->data = new unsigned[size_limit];
	else
		ch->data = NULL;
	bus->channels[index] = ch;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return index;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return;
	ch->is_closed = true;
	bus->channels[channel] = NULL;
	// Wake waiters so they can observe closure.
	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);
	if (ch->waiters == 0) {
		delete[] ch->data;
		delete ch;
		return;
	}
	// Defer freeing until all waiters leave their queues.
	if (!ch->in_zombie) {
		rlist_add_tail_entry(&bus->zombies, ch, zombie_link);
		ch->in_zombie = true;
	}
}
int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	
	while (true) {
		// Retry on WOULD_BLOCK by suspending the sender.
		int rc = coro_bus_try_send(bus, channel, data);
		if (rc == 0)
			return 0;
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL)
			return -1;
		wakeup_queue_suspend_this(&ch->send_queue, ch);
	}
}
int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{

	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	// Non-blocking: fail fast when buffer is full.
	if (!channel_has_space(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	channel_push(ch, data);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// A new item may unblock one receiver.
	wakeup_queue_wakeup_one(&ch->recv_queue);
	return 0;
}
int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		// Retry on WOULD_BLOCK by suspending the receiver.
		int rc = coro_bus_try_recv(bus, channel, data);
		if (rc == 0)
			return 0;
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL)
			return -1;

		wakeup_queue_suspend_this(&ch->recv_queue, ch);
	}
}
int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	// Non-blocking: fail fast when buffer is empty.
	if (!channel_has_data(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	*data = channel_pop(ch);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// A freed slot may unblock one sender.
	wakeup_queue_wakeup_one(&ch->send_queue);
	return 0;
}


#if NEED_BROADCAST
int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		// Retry on WOULD_BLOCK by waiting on a full channel.
		int rc = coro_bus_try_broadcast(bus, data);
		if (rc == 0)
			return 0;
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *wait_ch = NULL;
		if (bus != NULL) {
			// Find any full channel to wait on.
			for (int i = 0; i < bus->channel_count; ++i) {
				struct coro_bus_channel *ch = bus->channels[i];
				if (ch != NULL && !channel_has_space(ch)) {
					wait_ch = ch;
					break;
				}
			}
		}
		if (wait_ch == NULL) {
			continue;
		}
		wakeup_queue_suspend_this(&wait_ch->send_queue, wait_ch);
	}
}
int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	// Ensure all channels have space before broadcasting.
	int count = 0;
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		++count;
		if (!channel_has_space(ch)) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		// One message per channel.
		channel_push(ch, data);
		wakeup_queue_wakeup_one(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH
int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	while (true) {
		int rc = coro_bus_try_send_v(bus, channel, data, count);
		if (rc >= 0)
			return rc;
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL)
			return -1;
		wakeup_queue_suspend_this(&ch->send_queue, ch);
	}
}
int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	if (!channel_has_space(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	// Send as many as possible, return number sent.
	unsigned sent = 0;
	while (sent < count && channel_has_space(ch)) {
		channel_push(ch, data[sent]);
		++sent;
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// Multiple receivers may proceed after a batch send.
	wakeup_queue_wakeup_all(&ch->recv_queue);
	return (int)sent;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	while (true) {
		int rc = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (rc >= 0)
			return rc;
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL)
			return -1;
		wakeup_queue_suspend_this(&ch->recv_queue, ch);
	}
}
int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	if (!channel_has_data(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	// Receive as many as available up to capacity.
	unsigned recvd = 0;
	while (recvd < capacity && channel_has_data(ch)) {
		data[recvd] = channel_pop(ch);
		++recvd;
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// Freeing slots may unblock a sender.
	wakeup_queue_wakeup_one(&ch->send_queue);
	return (int)recvd;
}

#endif
