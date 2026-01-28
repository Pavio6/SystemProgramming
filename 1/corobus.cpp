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

static void
wakeup_queue_init(struct wakeup_queue *queue)
{
	rlist_create(&queue->coros);
}
// 通道(channel)数据结构
struct coro_bus_channel {
	// 通道容量上限
	size_t size_limit;
	// 发送者等待队列
	struct wakeup_queue send_queue;
	// 接收者等待队列
	struct wakeup_queue recv_queue;
	// 是否已关闭
	bool is_closed;
	// 当前通道挂起的协程数量 (发送+接收)
	int waiters;
	// 若通道关闭但仍有等待协程 需要挂到bus的zombie_link
	struct rlist zombie_link;
	// 标记当前channel是否已在zombie链表中
	bool in_zombie;
	// 指回所属的bus
	struct coro_bus *bus;
	int index;
	// 消息缓冲区
	unsigned *data;
	// 当前缓冲区已有的消息数量
	size_t data_size;
	// 循环队列头指针
	size_t data_head;
	// 循环队列尾指针
	size_t data_tail;
};
// 消息总线对象
struct coro_bus {
	struct coro_bus_channel **channels; // 通道指针数组
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
/**
 * 设置全局错误码
 * 记录最近一次corobus操作的错误原因
 */
void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static void
channel_cleanup_if_possible(struct coro_bus_channel *ch)
{
	if (!ch->is_closed || ch->waiters != 0)
		return;
	if (ch->in_zombie) {
		rlist_del_entry(ch, zombie_link);
		ch->in_zombie = false;
	}
	delete[] ch->data;
	delete ch;
}
/**
 * 唤醒队列头部的协程
 */
static void
wakeup_queue_wakeup_one(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	// 取出队列头部的等待节点
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	// 将该节点从等待队列移除
	rlist_del_entry(entry, base);
	entry->in_queue = false;
	// 唤醒对应协程
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	while (!rlist_empty(&queue->coros)) {
		struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
			struct wakeup_entry, base);
		rlist_del_entry(entry, base);
		entry->in_queue = false;
		coro_wakeup(entry->coro);
	}
}

/**
 * 将当前协程挂到等待队列中 
 */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue,
	struct coro_bus_channel *channel)
{
	// 分配一个等待节点 用来放到队列中
	struct wakeup_entry *entry = new wakeup_entry;
	entry->coro = coro_this();
	entry->channel = channel;
	entry->in_queue = true;
	// 等待节点插入到队列尾部
	rlist_add_tail_entry(&queue->coros, entry, base);
	++channel->waiters;
	// 挂起当前协程 之后不会继续执行 而是当前协程被挂起
	// 只有当该协程被唤醒(wakeup)后才会执行后面的代码
	coro_suspend();

	if (entry->in_queue) {
		rlist_del_entry(entry, base);
		entry->in_queue = false;
	}
	--channel->waiters;
	// 通道已关闭且没有等待者 释放通道资源
	channel_cleanup_if_possible(channel);
	delete entry;
}

static struct coro_bus_channel *
channel_get(struct coro_bus *bus, int channel)
{
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
	ch->data[ch->data_tail] = data;
	ch->data_tail = (ch->data_tail + 1) % ch->size_limit;
	++ch->data_size;
}

static unsigned
channel_pop(struct coro_bus_channel *ch)
{
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
		// 按约定此时不应有挂起的协程。
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
/**
 * 创建通道
 * 返回通道号
 */
int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	// 遍历已有通道槽位 找是否有空槽
	int index = -1;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			index = i;
			break;
		}
	}
	// 没有空槽 需扩容或新增新的通道号
	if (index < 0) {
		// 数组已满 需要扩容
		if (bus->channel_count == bus->channel_capacity) {
			// 第一次分配4 之后翻倍
			int new_cap = bus->channel_capacity == 0 ? 4 :
				bus->channel_capacity * 2;
			// 分配新的通道指针数组
			struct coro_bus_channel **new_arr =
				new struct coro_bus_channel*[new_cap];
			for (int i = 0; i < new_cap; ++i)
				new_arr[i] = NULL;
			// 把旧数组里的通道指针搬到新数组中
			for (int i = 0; i < bus->channel_count; ++i)
				new_arr[i] = bus->channels[i];
			// 释放旧数组
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
	/*
	 * One of the tests will force you to reuse the channel
	 * descriptors. It means, that if your maximal channel
	 * descriptor is N, and you have any free descriptor in
	 * the range 0-N, then you should open the new channel on
	 * that old descriptor.
	 *
	 * A more precise instruction - check if any of the
	 * bus->channels[i] with i = 0 -> bus->channel_count is
	 * free (== NULL). If yes - reuse the slot. Don't grow the
	 * bus->channels array, when have space in it.
	 */
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
	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);
	if (ch->waiters == 0) {
		delete[] ch->data;
		delete ch;
		return;
	}
	if (!ch->in_zombie) {
		rlist_add_tail_entry(&bus->zombies, ch, zombie_link);
		ch->in_zombie = true;
	}
	/*
	 * Be very attentive here. What happens, if the channel is
	 * closed while there are coroutines waiting on it? For
	 * example, the channel was empty, and some coros were
	 * waiting on its recv_queue.
	 *
	 * If you wakeup those coroutines and just delete the
	 * channel right away, then those waiting coroutines might
	 * on wakeup try to reference invalid memory.
	 *
	 * Can happen, for example, if you use an intrusive list
	 * (rlist), delete the list itself (by deleting the
	 * channel), and then the coroutines on wakeup would try
	 * to remove themselves from the already destroyed list.
	 *
	 * Think how you could address that. Remove all the
	 * waiters from the list before freeing it? Yield this
	 * coroutine after waking up the waiters but before
	 * freeing the channel, so the waiters could safely leave?
	 */
}
/**
 * 阻塞式发送
 * 思路: 先试一次发送,若通道满就挂起自己,等被唤醒后再重试
 */
int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	
	while (true) {
		// 尝试使用非阻塞发送
		int rc = coro_bus_try_send(bus, channel, data);
		// 发送成功 返回
		if (rc == 0)
			return 0;
		// 如果失败原因不是CORO_BUS_ERR_WOULD_BLOCK 直接返回错误
		if (coro_bus_errno() != CORO_BUS_ERR_WOULD_BLOCK)
			return -1;
		// 再取一次通道对象 (因为可能已经被关闭)
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL)
			return -1;
		// 通道满, 当前协程挂到发送等待队列
		wakeup_queue_suspend_this(&ch->send_queue, ch);
	}
}
/**
 * 非阻塞发送
 */
int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{

	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	// 判断通道是否还有空间
	if (!channel_has_space(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	// 将数据写入通道的循环队列
	channel_push(ch, data);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// 由于有了新消息 唤醒一个等待接收的协程
	wakeup_queue_wakeup_one(&ch->recv_queue);
	return 0;
}
/**
 * 阻塞接收消息
 */
int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
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
/**
 * 非阻塞接收
 */
int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return -1;
	if (!channel_has_data(ch)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	*data = channel_pop(ch);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// 由于取走了一条消息 唤醒一个等待发送的协程
	wakeup_queue_wakeup_one(&ch->send_queue);
	return 0;
}


#if NEED_BROADCAST
/**
 * 阻塞式广播
 * 向bus中所有通道广播一条消息
 */
int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int rc = coro_bus_try_broadcast(bus, data);
		if (rc == 0)
			return 0;
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		// 找一个满的通道作为等待目标
		struct coro_bus_channel *wait_ch = NULL;
		if (bus != NULL) {
			// 遍历所有通道 找到第一个已满的通道
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
		// 当前协程挂起到该通道的等待队列
		wakeup_queue_suspend_this(&wait_ch->send_queue, wait_ch);
	}
}
/**
 * 非阻塞广播
 */
int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	int count = 0;
	// 遍历所有通道槽位 只要有一个通道满了 广播就无法进行
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
		// 往该通道写入一条消息
		channel_push(ch, data);
		// 并唤醒当前通道等待接收的协程
		wakeup_queue_wakeup_one(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH
/**
 * 阻塞式批量发送
 */
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
/**
 * 非阻塞式批量发送
 */
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
	unsigned sent = 0;
	// 向通道发送消息 无论是否全部发送 返回发送消息的条数
	while (sent < count && channel_has_space(ch)) {
		channel_push(ch, data[sent]);
		++sent;
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	// 唤醒通道上所有等着等待接收的协程
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
/**
 * 非阻塞接收
 */
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
	unsigned recvd = 0;
	while (recvd < capacity && channel_has_data(ch)) {
		data[recvd] = channel_pop(ch);
		++recvd;
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_one(&ch->send_queue);
	return (int)recvd;
}

#endif
