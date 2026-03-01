#include "userfs.h"

#include "rlist.h"

#include <algorithm>
#include <stddef.h>
#include <string.h>
#include <string>
#include <vector>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	char memory[BLOCK_SIZE];
	rlist in_block_list = RLIST_LINK_INITIALIZER;
};

struct file {
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	int refs = 0;
	std::string name;
	rlist in_file_list = RLIST_LINK_INITIALIZER;
	size_t size = 0;
	size_t block_count = 0;
	/* True after ufs_delete(); data stays alive while refs > 0. */
	bool is_deleted = false;
};

static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile;
	size_t pos = 0;
	bool can_read = true;
	bool can_write = true;
};

static std::vector<filedesc*> file_descriptors;

static inline void
set_error(enum ufs_error_code code)
{
	ufs_error_code = code;
}

static file *
find_active_file(const char *filename)
{
	file *it = NULL;
	rlist_foreach_entry(it, &file_list, in_file_list) {
		if (!it->is_deleted && it->name == filename)
			return it;
	}
	return NULL;
}

static rlist *
get_block_node_by_index(file *f, size_t index)
{
	if (index >= f->block_count)
		return NULL;
	rlist *node = rlist_first(&f->blocks);
	for (size_t i = 0; i < index; ++i)
		node = rlist_next(node);
	return node;
}

static bool
ensure_block_count(file *f, size_t need_blocks)
{
	/* Grow storage lazily block-by-block and zero-initialize new blocks. */
	while (f->block_count < need_blocks) {
		block *b = new block();
		memset(b->memory, 0, sizeof(b->memory));
		rlist_add_tail_entry(&f->blocks, b, in_block_list);
		++f->block_count;
	}
	return true;
}

static void
free_file(file *f)
{
	/* Release all file blocks, then unlink and destroy metadata node. */
	block *b = NULL;
	block *tmp = NULL;
	rlist_foreach_entry_safe(b, &f->blocks, in_block_list, tmp) {
		rlist_del_entry(b, in_block_list);
		delete b;
	}
	rlist_del_entry(f, in_file_list);
	delete f;
}

static filedesc *
get_filedesc(int fd)
{
	if (fd < 0 || static_cast<size_t>(fd) >= file_descriptors.size())
		return NULL;
	return file_descriptors[fd];
}

static void
adjust_positions_after_shrink(file *f, size_t new_size)
{
	for (filedesc *d : file_descriptors) {
		if (d != NULL && d->atfile == f && d->pos > new_size)
			d->pos = new_size;
	}
}

static void
truncate_blocks(file *f, size_t new_size)
{
	size_t keep_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
	while (f->block_count > keep_blocks) {
		block *tail = rlist_last_entry(&f->blocks, block, in_block_list);
		rlist_del_entry(tail, in_block_list);
		delete tail;
		--f->block_count;
	}
	if (new_size > 0 && keep_blocks > 0) {
		size_t tail_off = new_size % BLOCK_SIZE;
		if (tail_off != 0) {
			block *tail = rlist_last_entry(&f->blocks, block, in_block_list);
			memset(tail->memory + tail_off, 0, BLOCK_SIZE - tail_off);
		}
	}
}

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

int
ufs_open(const char *filename, int flags)
{
	set_error(UFS_ERR_NO_ERR);
	file *f = find_active_file(filename);
	if (f == NULL) {
		if ((flags & UFS_CREATE) == 0) {
			set_error(UFS_ERR_NO_FILE);
			return -1;
		}
		f = new file();
		f->name = filename;
		rlist_add_tail_entry(&file_list, f, in_file_list);
	}

	filedesc *desc = new filedesc();
	desc->atfile = f;
#if NEED_OPEN_FLAGS
	desc->can_read = true;
	desc->can_write = true;
	const int mode_mask = UFS_READ_ONLY | UFS_WRITE_ONLY;
	const int mode = flags & mode_mask;
	if (mode == UFS_READ_ONLY) {
		desc->can_read = true;
		desc->can_write = false;
	} else if (mode == UFS_WRITE_ONLY) {
		desc->can_read = false;
		desc->can_write = true;
	} else if (mode == UFS_READ_WRITE) {
		desc->can_read = true;
		desc->can_write = true;
	}
#endif
	++f->refs;

	for (size_t i = 0; i < file_descriptors.size(); ++i) {
		if (file_descriptors[i] == NULL) {
			file_descriptors[i] = desc;
			return static_cast<int>(i);
		}
	}
	file_descriptors.push_back(desc);
	return static_cast<int>(file_descriptors.size() - 1);
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	set_error(UFS_ERR_NO_ERR);
	filedesc *desc = get_filedesc(fd);
	if (desc == NULL) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if (!desc->can_write) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	file *f = desc->atfile;
	if (size == 0)
		return 0;
	if (desc->pos >= MAX_FILE_SIZE) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	size_t writable = std::min(size, MAX_FILE_SIZE - desc->pos);
	if (writable == 0) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	size_t end_pos = desc->pos + writable;
	size_t need_blocks = (end_pos + BLOCK_SIZE - 1) / BLOCK_SIZE;
	if (!ensure_block_count(f, need_blocks)) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	/* Copy user buffer across consecutive blocks from current descriptor offset. */
	size_t remaining = writable;
	size_t src_off = 0;
	size_t in_block_off = desc->pos % BLOCK_SIZE;
	rlist *node = get_block_node_by_index(f, desc->pos / BLOCK_SIZE);
	while (remaining > 0) {
		block *cur = rlist_entry(node, block, in_block_list);
		size_t can_copy = BLOCK_SIZE - in_block_off;
		if (can_copy > remaining)
			can_copy = remaining;
		memcpy(cur->memory + in_block_off, buf + src_off, can_copy);
		remaining -= can_copy;
		src_off += can_copy;
		in_block_off = 0;
		if (remaining > 0)
			node = rlist_next(node);
	}

	desc->pos += writable;
	if (f->size < desc->pos)
		f->size = desc->pos;
	if (writable < size)
		set_error(UFS_ERR_NO_MEM);
	return static_cast<ssize_t>(writable);
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	set_error(UFS_ERR_NO_ERR);
	filedesc *desc = get_filedesc(fd);
	if (desc == NULL) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if (!desc->can_read) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	file *f = desc->atfile;
	if (size == 0)
		return 0;
	if (desc->pos >= f->size)
		return 0;

	size_t readable = std::min(size, f->size - desc->pos);
	/* Mirror ufs_write() traversal, but copy from blocks into caller buffer. */
	size_t remaining = readable;
	size_t dst_off = 0;
	size_t in_block_off = desc->pos % BLOCK_SIZE;
	rlist *node = get_block_node_by_index(f, desc->pos / BLOCK_SIZE);
	while (remaining > 0) {
		block *cur = rlist_entry(node, block, in_block_list);
		size_t can_copy = BLOCK_SIZE - in_block_off;
		if (can_copy > remaining)
			can_copy = remaining;
		memcpy(buf + dst_off, cur->memory + in_block_off, can_copy);
		remaining -= can_copy;
		dst_off += can_copy;
		in_block_off = 0;
		if (remaining > 0)
			node = rlist_next(node);
	}
	desc->pos += readable;
	return static_cast<ssize_t>(readable);
}

int
ufs_close(int fd)
{
	set_error(UFS_ERR_NO_ERR);
	filedesc *desc = get_filedesc(fd);
	if (desc == NULL) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
	file_descriptors[fd] = NULL;
	file *f = desc->atfile;
	delete desc;
	--f->refs;
	/* Deferred destruction: removed names are freed on last close. */
	if (f->refs == 0 && f->is_deleted)
		free_file(f);
	return 0;
}

int
ufs_delete(const char *filename)
{
	set_error(UFS_ERR_NO_ERR);
	file *f = find_active_file(filename);
	if (f == NULL) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
	/* Hide from namespace immediately, keep data for already-open fds. */
	f->is_deleted = true;
	if (f->refs == 0)
		free_file(f);
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	set_error(UFS_ERR_NO_ERR);
	filedesc *desc = get_filedesc(fd);
	if (desc == NULL) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if (!desc->can_write) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	if (new_size > MAX_FILE_SIZE) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}
	file *f = desc->atfile;
	if (new_size == f->size)
		return 0;

	if (new_size > f->size) {
		size_t need_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
		if (!ensure_block_count(f, need_blocks)) {
			set_error(UFS_ERR_NO_MEM);
			return -1;
		}
		/* Zero the tail of the previous last block to preserve sparse semantics. */
		if (f->size > 0 && f->size % BLOCK_SIZE != 0) {
			rlist *node = get_block_node_by_index(f, f->size / BLOCK_SIZE);
			block *b = rlist_entry(node, block, in_block_list);
			size_t off = f->size % BLOCK_SIZE;
			memset(b->memory + off, 0, BLOCK_SIZE - off);
		}
		f->size = new_size;
		return 0;
	}

	truncate_blocks(f, new_size);
	f->size = new_size;
	/* Clamp all open descriptor positions to the new end-of-file. */
	adjust_positions_after_shrink(f, new_size);
	return 0;
}

#endif

void
ufs_destroy(void)
{
	for (filedesc *d : file_descriptors)
		delete d;
	std::vector<filedesc*>().swap(file_descriptors);

	file *f = NULL;
	file *tmp = NULL;
	rlist_foreach_entry_safe(f, &file_list, in_file_list, tmp) {
		free_file(f);
	}
}
