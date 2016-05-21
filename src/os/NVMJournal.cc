#include <iostream>
#include <map>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#include "include/types.h"
#include "include/unordered_map.h"
#include "include/buffer.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "common/blkdev.h"
#include "ranktree.h"
#include "NVMJournal.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << __FILE__ << "(" << __LINE__ << ") "

#define PAGE_SIZE CEPH_PAGE_SIZE
#define PAGE_MASK (~((off64_t)PAGE_SIZE - 1))
#define PAGE_SHIFT (CEPH_PAGE_SHIFT - 1)

utime_t NVMJournal::heat_info_t::now;
utime_t NVMJournal::heat_info_t::cycle;

static uint32_t create_checksum32(uint32_t *data, int len)
{
    uint32_t checksum = 0;
    while(len--) 
	checksum ^= data[len];
    return checksum;
}

static bool valid_checksum32(uint32_t *data, int len)
{
    uint32_t res = 0;
    while(len--)
	res ^= data[len];
    return res == 0;
}

static ssize_t safe_pread64(int fd, void *buf, ssize_t len, uint64_t off)
{
    ssize_t cnt = 0;
    char *b = (char*)buf;
    while (cnt < len) {
        ssize_t r = pread64(fd, b+cnt, len-cnt, off+cnt);
        if (r <= 0) {
            if (r == 0) 
                return cnt;
            if (errno == EINTR)
                continue;
            return -errno;
        }
        cnt += r;
    }
    return cnt;
}

void NVMJournal::dump_journal_stat()
{
    Formatter *f = new_formatter("json-pretty");
    f->open_object_section("journal stat");
    f->dump_string("hot_write_pos", stringify(hot_write_pos));
    f->dump_string("data_sync_pos", stringify(data_sync_pos));
    f->dump_string("hot_start_pos", stringify(hot_start_pos));
    f->dump_string("cold_write_pos", stringify(cold_write_pos));
    f->dump_string("cold_sync_pos", stringify(cold_sync_pos));
    f->dump_string("cold_start_pos", stringify(cold_start_pos));
    f->dump_float("total_hot_wrap", total_hot_wrap.read());
    f->dump_float("total_cold_wrap", total_cold_wrap.read());
    f->dump_float("total_write", total_write.read());
    f->dump_float("total_flush", total_flush.read());
    f->dump_float("total_evict", total_evict.read());
    f->dump_float("total_cached", total_cached.read());
    f->dump_float("total_overlap", total_overlap.read());
    f->dump_int("heat_rank", heat_rank.size());
    f->dump_int("hot_object_set", hot_object_set.size());
    f->close_section();
    dout(0) << "Dump Journal stat:\n" ;
    f->flush(*_dout);
    *_dout << dendl;
    if (enable_cache) {
	stringstream ss;
	for (int i=0; i<cache_slots; i++) {
	    ss << "buffer_cache[" << i << "] :";
	    buffer_cache[i].dump(ss);
	}
	dout(5) << ss.str() << dendl;
    }
    if (0) {
	stringstream ss;
	dump_heat_rank(ss);
	dout(5) << ss.str() << dendl;
    }
}

int NVMJournal::_open(string &path, uint64_t &size, bool io_direct)
{
    int fd = -1;
    int flags = O_RDWR | O_DSYNC;
    int ret = 0;

    if (io_direct)
        flags |=  O_DIRECT;

    fd = TEMP_FAILURE_RETRY(::open(path.c_str(), flags, 0644));
    if (fd < 0) {
        derr << "NVMJournal::Open: unable to open journal " 
            << path << " : " << cpp_strerror(errno) << dendl;
        return -1;
    }

    do {
	struct stat st;
	ret = ::fstat(fd, &st);
	if (ret) {
	    derr << "NVMJournal::Open: unable to fstat journal:" << cpp_strerror(errno) << dendl;
	    break;
	}

	if (!S_ISBLK(st.st_mode)) {
	    derr << "NVMJournal::Open: journal not be a block device" << dendl;
	    break;
	}
	int64_t bdev_size;
	ret = get_block_device_size (fd, &bdev_size);
	if (ret) {
	    derr << "NVMJournal::Open: failed to read block device size." << dendl;
	    break;
	}

	if (bdev_size < (1024*1024*1024)) {
	    derr << "NVMJournal::Open: Your Journal device must be at least ONE GB: " << bdev_size << dendl;
	    break;
	}
	size = bdev_size - bdev_size % PAGE_SIZE;

	if (io_direct && !aio_ctx_ready) {
	    aio_ctx = 0;
	    ret = io_setup(128, &aio_ctx);
	    if (ret < 0) {
		derr << "NVMJournal::Open: unable to setup io_context " << cpp_strerror(errno) << dendl;
		break;
	    }
	    aio_ctx_ready = true;
	}
	return fd;
    }while(false);

    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return -1;
}

int NVMJournal::mkjournal()
{
    hot_fd = _open(hot_journal_path, hot_journal_size);
    cold_fd = _open(cold_journal_path, cold_journal_size);
    try {
        string fn = conf + "/current/journal.meta";
        bufferlist bl;
        header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        ::encode(hdr, bl);
        bl.write_file(fn.c_str());
    }
    catch (...) {

    }
    return 0;
}

NVMJournal::JournalReplayer::JournalReplayer(NVMJournal *j, deque<Op*> *q): 
    Journal(j), queue(q), stop(false),
    lock("NVMJournal::JournalReplayer::Lock", false, true, false, g_ceph_context)
{ }

void *NVMJournal::JournalReplayer::entry()
{
    while (!stop)
    {
	Op *op = NULL;
	{
	    Mutex::Locker l(lock);
	    if (queue->empty()) {
		if (stop)
		    return NULL;
		cond2.Signal();
		cond.Wait(lock);
		continue;
	    }
	    op = queue->front();
	    queue->pop_front();
	    cond.Signal();
	}
	Journal->do_op(op);
	delete op;
    }
    return NULL;
}

void NVMJournal::JournalReplayer::queue_op(Op *op)
{
    Mutex::Locker l(lock);
    while (queue->size() > 20)
	cond.Wait(lock);
    queue->push_back(op);
    cond.Signal();
}

void NVMJournal::JournalReplayer::stop_and_wait()
{
    while(true) {
	Mutex::Locker l(lock);
	if (queue->empty()) {
	    stop = true;
	    cond.Signal();
	    break;
	}
	cond2.Wait(lock);
    }
}

int NVMJournal::_journal_replay()
{
    replay = true;
    not_update_meta = true;
    uint64_t seq, hot_pos, cold_pos;
    seq = 0;
    hot_pos = data_sync_pos;
    cold_pos = cold_sync_pos;

    deque<Op*> op_queue;
    JournalReplayer Replayer(this, &op_queue);
    Replayer.create();

    int ret = 0;
    while (!ret) {
        Op *op = new Op();
        check_replay_point(hot_pos);
        ret = read_entry(hot_pos, cold_pos, seq, op);
        if (ret == 0  && hot_pos != 0) {
            Replayer.queue_op(op);
            cur_seq = seq;
            hot_write_pos = hot_pos;
	    cold_write_pos = cold_pos;
        }
    }
    Replayer.stop_and_wait();
    Replayer.join();

    meta_sync_pos = hot_write_pos;
    replay = false;
    not_update_meta = false;
    return 0;
}

int NVMJournal::replay_journal()
{
    int ret = 0;
    do {
        ret = store->list_checkpoints(checkpoints);
        if (ret < 0)
            break;
        if (!checkpoints.empty()) {
            stringstream ss;
            for (deque<uint64_t>::iterator it = checkpoints.begin();
                    it != checkpoints.end();
                    ++ it)
                ss << *it << " ";
            dout(0) << "dump checkpoints : " << ss.str() << dendl;
            next_checkpoint_seq = checkpoints.back() + 1;
        }

        bufferlist bl;
        string fn = conf + "/current/journal.meta";
        string err;

        ret = bl.read_file(fn.c_str(), &err);
        if (ret < 0 && errno!=ENOENT) 
            break;

        try {
            bufferlist::iterator p = bl.begin();
            ::decode(header, p);
        } 
        catch (buffer::error& e) {
            derr << "read journal.meta error!!" << dendl;
            memset(&header, 0, sizeof(header));
        }

        bool valid = valid_checksum32((uint32_t*)&header, sizeof(header)/sizeof(uint32_t));
        if (!valid) {
            derr << "invalid checksum of journal header!" << dendl;
	    ret = -1;
            break;
        }

        data_sync_pos = header.data_sync_pos;
        meta_sync_pos = header.meta_sync_pos;
        hot_write_pos = 0;
        hot_start_pos = data_sync_pos;
	cold_sync_pos = header.cold_sync_pos;
	cold_start_pos = cold_sync_pos;
	cold_write_pos = 0;

        cur_seq = 1;

        hot_fd =  _open(hot_journal_path, hot_journal_size, false);
	cold_fd = _open(cold_journal_path, cold_journal_size, false);
        if (hot_fd < 0 || cold_fd < 0) {
	    ret = -1;
	    break;
	}
	dout(0) << "journal replay begin.." << dendl;
	ret = _journal_replay();
	dout(0) << "journal replay end.." << dendl;
        if (ret < 0)
            break;
    }while(false);

    if (ret < 0) {
        dout(0) << "failed to replay journal!" << dendl;
	if (hot_fd)
	    ::close(hot_fd);
	if (cold_fd)
	    ::close(cold_fd);
	hot_fd = cold_fd = -1;
        return ret;
    }
    
    init_heat_map();

    dout(5) << "replay journal success: "
        << ", data_sync_pos = " << data_sync_pos \
        << ", meta_sync_pos = " << meta_sync_pos \
        << ", hot_write_pos = " << hot_write_pos \
	<< ", cold_sync_pos = " << cold_sync_pos \
	<< ", cold_write_pos = " << cold_write_pos \
	<< ", cur_seq = " << cur_seq << dendl;
    return ret;
}

int NVMJournal::start()
{
    if (hot_fd)
	::close(hot_fd);
    hot_fd = _open(hot_journal_path, hot_journal_size, true);
    if (hot_fd < 0)
	return -1;

    if (cold_fd)
	::close(cold_fd);
    cold_fd = _open(cold_journal_path, cold_journal_size, true);
    if (cold_fd < 0) {
	::close(hot_fd);
	hot_fd = -1;
	return -1;
    }

    init_heat_map();
    
    if (zero_buf)
        delete[] zero_buf;
    zero_buf = new char[PAGE_SIZE];
    memset(zero_buf, 0, PAGE_SIZE);

    if(!op_tp_started) {
        op_tp.start();
        op_tp_started = true;
    }
    if (!ev_tp_started) {
        ev_tp.start();
        ev_tp_started = true;
    }
    if (!writer.is_started()) {
        writer_stop = false;
        writer.create();
    }
    if (!reaper.is_started()) {
        reaper_stop = false;
        reaper.create();
    }
    if (!evictor.is_started()) {
        ev_stop = false;
        evictor.create();
    }
    dout(10) << "all working threads started..." << dendl;
    return 0;
}

void NVMJournal::stop()
{
    if (writer.is_started())
        stop_writer();
    if (evictor.is_started())
        stop_evictor();
    if (reaper.is_started())
        stop_reaper();
    if (op_tp_started) {
        op_tp.stop();
        op_tp_started = false;
    }
    if (ev_tp_started) {
        ev_tp.stop();
        ev_tp_started = false;
    }
    dout(10) << "ALL THREAD HAVE BEEN STOPPED !" << dendl;
}

void NVMJournal::ApplyManager::sync_start()
{
    Mutex::Locker locker(lock);
    blocked = true;
    while (open_ops > 0)
        cond.Wait(lock);
}
void NVMJournal::ApplyManager::sync_finish()
{
    Mutex::Locker locker(lock);
    blocked = false;
    cond.Signal();
}
void NVMJournal::ApplyManager::op_apply_start(uint32_t ops)
{
    Mutex::Locker locker(lock);
    while (blocked)
        cond.Wait(lock);
    open_ops += ops;
}
void NVMJournal::ApplyManager::op_apply_finish(uint32_t ops)
{
    Mutex::Locker locker(lock);
    open_ops -= ops;
    cond.Signal();
}
void NVMJournal::build_header(bufferlist& bl)
{
    header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.meta_sync_pos = meta_sync_pos;
    hdr.data_sync_pos = data_sync_pos;
    hdr.cold_sync_pos = cold_sync_pos;
    hdr.checksum = create_checksum32((uint32_t*)&hdr, sizeof(hdr)/sizeof(uint32_t));
    ::encode(hdr, bl);
}

void NVMJournal::_sync()
{
    assert(sync_lock.is_locked());
    apply_manager.sync_start();
    data_sync_pos_recorded = data_sync_pos;

    bufferlist hdr;
    build_header(hdr); 
    string fn = conf + "/current/journal.meta";
    int r = hdr.write_file(fn.c_str());
    if (r < 0) {
        assert(0 == "error update journal.meta");
    }
    
    uint64_t cp = next_checkpoint_seq;
    next_checkpoint_seq += 1;
    uint64_t transid;
    assert(0 == store->create_checkpoint(cp, &transid));
    apply_manager.sync_finish();
    assert(0 == store->flush_checkpoint(transid));
    checkpoints.push_back(cp);
 
    /*delete old checkpoint*/
    if (checkpoints.size()>=3) {
        uint64_t oldcp = checkpoints.front();
        checkpoints.pop_front();
        assert(0 == store->delete_checkpoint(oldcp));
    }
    cold_start_pos = cold_sync_pos;
}
NVMJournal::NVMJournal(string hot_journal, string cold_journal, string c, BackStore *s, Finisher *fin)
    :RWJournal (s, fin),
    cur_seq(1),
    hot_write_pos(0),
    meta_sync_pos(0),
    data_sync_pos(0),
    data_sync_pos_recorded(0),
    hot_start_pos(0),
    hot_journal_size(0),
    cold_write_pos(0),
    cold_sync_pos(0),
    cold_start_pos(0),
    cold_journal_size(0),
    next_checkpoint_seq(1),
    total_hot_wrap(0),
    total_cold_wrap(0),
    total_write(0),
    total_flush(0),
    total_evict(0),
    total_cached(0),
    total_overlap(0),
    enable_hot_journal(true),
    conf (c), 
    hot_journal_path(hot_journal),
    cold_journal_path(cold_journal),
    hot_fd(-1), cold_fd(-1),
    replay(false),
    not_update_meta(false),
    sync_lock ("NVMJournal::sync_lock", false, true, false, g_ceph_context),
    writeq_lock ("NVMJournal::writeq_lock", false, true, false, g_ceph_context),
    op_throttle_lock ("NVMJournal::op_throttle_lock", false, true, false, g_ceph_context),
    op_queue_len(0),
    aioq_lock ("NVMJournal::aioq_lock",false, true, false, g_ceph_context), 
    aio_num(0),
    aio_ctx(0), 
    aio_ctx_ready(false),
    zero_buf (NULL), 
    cold_journal_wrap (false),
    hot_journal_wrap (false),
    writer(this),
    writer_stop (false), 
    reaper_stop (false),
    op_queue_lock ("NVMJournal::op_queue_lock", false, true, false, g_ceph_context),
    reaper(this),
    latency_lock("NVMJournal::latency_lock", false, true, false, g_ceph_context),   
    op_tp (g_ceph_context, "NVMJournal::op_tp", g_conf->filestore_op_threads*2, "filestore_op_threads"),
    op_tp_started(false),
    op_wq (g_conf->filestore_op_thread_timeout, 
            g_conf->filestore_op_thread_suicide_timeout, &op_tp, this),
    heat_map_lock ("NVMJournal::heat_map_lock", false, true, false, g_ceph_context),
    hot_lock ("NVMJournal::hot_lock", false, true, false, g_ceph_context),
    Journal_queue_lock ("NVMJournal::Journal_queue_lock", false, true, false, g_ceph_context),
    ev_seq(0),
    ev_tp (g_ceph_context, "NVMJournal::ev_tp", g_conf->filestore_op_threads, "filestore_op_threads"),
    ev_tp_started(false),
    ev_wq (g_conf->filestore_op_thread_timeout,
            g_conf->filestore_op_thread_suicide_timeout, &ev_tp, this),
    evict_lock ("NVMJournal::evict_lock", false, true, false, g_ceph_context),
    waiter_lock ("NVMJournal::Waiter_lock", false, true, false, g_ceph_context),
    evictor(this),
    ev_stop(false),
    force_evict(false),
    ev_pause(0),
    ev_paused(false),
    reclaim_queue_lock ("NVMJournal::reclaim_queue_lock", false, true, false, g_ceph_context),
    omap_cache(this),
    enable_omap_cache(true),
    cthrottle(81960),
    mthrottle(1400*1024*1024),
    cache_slots(1),
    enable_cache(false)
{
    if (enable_cache) {
	ssize_t climit = cthrottle/cache_slots;
	ssize_t mlimit = mthrottle/cache_slots;
	buffer_cache = new LRUCache[cache_slots];
	for (int i=0; i<cache_slots; i++) {
	    buffer_cache[i].setthrottle(climit, mlimit);
	}
    }
}

NVMJournal::~NVMJournal() 
{
    stop();
    {
        Mutex::Locker l(latency_lock);
        dout(0) << " journal_aio_latency = " << journal_aio_latency
            << " osr_queue_latency = " << osr_queue_latency
            << " do_op_latency = " << do_op_latency
            << " ops = " << ops << dendl;
    }
    if (zero_buf) {
        delete[] zero_buf;
        zero_buf = 0;
    }
}

void NVMJournal::op_queue_reserve_throttle(ThreadPool::TPHandle *handle)
{
    Mutex::Locker l(op_throttle_lock);
    while (op_queue_len + 1 > op_queue_max) {
        if (handle)
            handle->suspend_tp_timeout();
        op_throttle_cond.Wait(op_throttle_lock);
        if (handle)
            handle->reset_tp_timeout();
    }
    ++ op_queue_len;

}
void NVMJournal::op_queue_release_throttle()
{
    Mutex::Locker l(op_throttle_lock);
    op_queue_len -= 1;
    op_throttle_cond.Signal();
}
void NVMJournal::submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle)
{
    dout(15) << " sequence = " << cur_seq << dendl;
    op_queue_reserve_throttle(handle);
    {
        Mutex::Locker locker(writeq_lock);
        OpSequencer *osr;
        if (!posr) {
            osr = &default_osr;
        } else if (posr->p) {
            osr = static_cast<OpSequencer *> (posr->p);
        } else {
            osr = new OpSequencer;
            posr->p = osr;
        }

        osr->register_op(cur_seq);
        writeq.push_back(write_item(cur_seq++, osr, tls));
        writeq_cond.Signal();
    }
}

uint64_t NVMJournal::prepare_single_write(bufferlist &meta, bufferlist &hot_data, bufferlist &cold_data, Op *op)
{
    write_item *pitem = NULL;       
    uint64_t seq = 0; 
    bufferlist tmeta;
    list< pair<ghobject_t, bufferlist> > tdata;

    {
	Mutex::Locker locker(writeq_lock); // check tailer and header of journal entry
	if (writeq.empty())
	    return seq;
	pitem = &writeq.front();
    }

    for (list<Transaction*>::iterator t = pitem->tls.begin();
	    t != pitem->tls.end();
	    t ++) {
	(*t)->_encode(tmeta, tdata);
    }

    uint32_t meta_len = tmeta.length();
    uint32_t data_len = 0;
    for (list< pair<ghobject_t, bufferlist> >::iterator it = tdata.begin();
	    it != tdata.end();
	    ++ it) {
	data_len += it->second.length();
    }
    meta_len += meta.length();
    data_len += hot_data.length() + cold_data.length();

    uint32_t size, pad = 0;
    if (data_len) {
	pad = (-(uint32_t)sizeof(entry_header_t)*2 - meta_len) & ~PAGE_MASK;
    }
    size = sizeof(entry_header_t)*2 + pad + meta_len + data_len + 16*PAGE_SIZE;
    size = ROUND_UP_TO(size, PAGE_SIZE);
    if (hot_write_pos + size > hot_journal_size) {
	hot_journal_wrap = true;
    }
    if (cold_write_pos + size > cold_journal_size) {
	cold_journal_wrap = true;
    }
    size += PAGE_SIZE;
    wait_for_more_space(size);
    if (cold_journal_wrap || hot_journal_wrap)
	return 0;
    
    op->seq = pitem->seq;
    op->entry_pos = 0;
    op->data_offset = hot_data.length();
    op->posr = pitem->posr;
    
    ::encode(tmeta, meta);
    set<ghobject_t> coldset;
    for (list< pair<ghobject_t, bufferlist> >::iterator it = tdata.begin();
	    it != tdata.end();
	    ++it) {
	if (check_is_hot(it->first)) {
	    hot_data.append(it->second);
	}
	else {
	    coldset.insert(it->first);
	    cold_data.append(it->second);
	}
    }
    
    op->tls.swap(pitem->tls);
    op->coldset.swap(coldset);

    {
	Mutex::Locker locker(writeq_lock);
	writeq.pop_front();
    }
    return op->seq;
}

int NVMJournal::prepare_multi_write(bufferlist &hot_bl, bufferlist &cold_bl, list<Op*> &ops)
{
    bufferlist meta, hot_data, cold_data;
    uint64_t first = 0;
    uint32_t count = 0;
    static const uint32_t max_transactions_per_entry = 64;
    
    int trans = max_transactions_per_entry - 1; 
    uint64_t seq;
    set<ghobject_t> coldset;
    do {
	Op *op = new Op();
	seq = prepare_single_write(meta, hot_data, cold_data, op);
	if(seq) {
	    ops.push_back(op);
	    coldset.insert(op->coldset.begin(), op->coldset.end());
	    count ++;
	} else {
	    trans = trans >> 1;
	    delete op;
	}
	if(!first) 
	    first = seq;
    } while(!(hot_journal_wrap || cold_journal_wrap) && trans--);

    if(!count && !(hot_journal_wrap||cold_journal_wrap)) 
	return -1;

    if(!count) // wrap the journal
	return 0;

    bufferlist coldset_bl;
    if (!coldset.empty()) {
	::encode(coldset, coldset_bl);
    }
    uint32_t coldset_len = coldset_bl.length();

    entry_header_t header;
    memset(&header, 0, sizeof(header));
    header.magic = magic;
    header.seq = first;
    header.ops = count;

    uint32_t pre_pad, post_pad;
    pre_pad = post_pad = 0;
    if(hot_data.length()) {
	pre_pad = (-(uint32_t)sizeof(header) - coldset_len - (uint32_t)meta.length()) & ~PAGE_MASK;
    }
    uint32_t size = sizeof(header)*2 + coldset_len + pre_pad + meta.length() + hot_data.length();
    post_pad = ROUND_UP_TO(size, PAGE_SIZE) - size;

    header.pre_pad = pre_pad;
    header.post_pad = post_pad;
    header.meta_len = meta.length();
    header.data_len = hot_data.length();
    header.coldset_len = coldset_len;
    header.checksum = create_checksum32((uint32_t*)&header, sizeof(header)/sizeof(uint32_t));

    hot_bl.append((const char*)&header, sizeof(header));
    if (coldset_len)
	hot_bl.append(coldset_bl);
    if (pre_pad) {
	bufferptr bp = buffer::create_static(pre_pad, zero_buf);
	hot_bl.push_back(bp);
    }
    hot_bl.append(meta);
    hot_bl.append(hot_data);
    if (post_pad) {
	bufferptr bp = buffer::create_static(post_pad, zero_buf);
	hot_bl.push_back(bp);
    }
    hot_bl.append((const char*)&header, sizeof(header));

    if (cold_data.length()) {
	entry_header_t header2;
	memset(&header2, 0, sizeof(header2));
	header2.magic = magic;
	header2.seq = seq;

	pre_pad = (-(uint32_t)sizeof(header2)) & ~PAGE_MASK;
	size = 2*sizeof(header2) + pre_pad + cold_data.length();
	post_pad = ROUND_UP_TO(size, PAGE_SIZE) - size;
	header2.pre_pad = pre_pad;
	header2.post_pad = post_pad;
	header2.data_len = cold_data.length();
	header2.checksum = create_checksum32((uint32_t*)&header2, sizeof(header2)/sizeof(uint32_t));

	cold_bl.append((const char*)&header2, sizeof(header2));
	if (pre_pad) {
	    bufferptr bp = buffer::create_static(pre_pad, zero_buf);
	    cold_bl.push_back(bp);
	}
	cold_bl.append(cold_data);
	if (post_pad) {
	    bufferptr bp = buffer::create_static(post_pad, zero_buf);
	    cold_bl.push_back(bp);
	}
	cold_bl.append((const char*)&header2, sizeof(header2));
    }

    // update
    utime_t now = ceph_clock_now(NULL);
    for (list<Op*>::iterator p = ops.begin();
	    p != ops.end();
	    p ++) {
	Op *op = *p;
	op->entry_pos = hot_write_pos;
	op->data_offset += sizeof(header) + coldset_len + header.pre_pad + meta.length();
	op->replay = false;
	op->journal_latency = now;
    }

    return 0;
}
/* read_entry
 * this function read the journal with fd 
 * which opened without flag DIRECT, SO there
 * is no need PAGE-ALIGNED buffer... 
 */
int NVMJournal::read_entry(uint64_t &hot_pos, uint64_t &cold_pos, uint64_t &next_seq, Op *op)
{
    entry_header_t h;
    bufferlist bl;
    off64_t entry_pos = hot_pos;

    ssize_t cnt = safe_pread64(hot_fd, &h, sizeof(h), hot_pos);
    if (cnt != sizeof(h) 
            || h.magic != magic
            || !valid_checksum32 ((uint32_t*)&h, sizeof(h)/sizeof(uint32_t)) 
            || (!h.wrap && h.seq < next_seq))
        return -1;

    if (h.wrap) {
        assert(h.seq == 0);
        entry_header_t h2;
	uint64_t off = hot_pos + sizeof(h) + h.pre_pad;
        cnt = safe_pread64(hot_fd, &h2, sizeof(h2), off);
        assert(cnt == sizeof(h2));
        if (memcmp(&h, &h2, sizeof(h)))
            return -1;
        hot_pos = 0;
        return 0;
    }

    hot_pos += sizeof(h);
    set<ghobject_t> coldset;
    bufferlist cold_data;
    do {
	if (!h.coldset_len)
	    break;
	bufferptr bp = buffer::create(h.coldset_len);
	ssize_t r = safe_pread64(hot_fd, bp.c_str(), h.coldset_len, hot_pos);
	assert(r == h.coldset_len);
	bufferlist bl;
	bl.push_back(bp);
	try {
	    ::decode(coldset, bl);
	}
	catch(buffer::error &e) {
	    return -1;
	}
	if (not_update_meta)
	    break;
	if (!coldset.empty()) {
	    entry_header_t cold_h;
	    do {
		cnt = safe_pread64(cold_fd, &cold_h, sizeof(cold_h), cold_pos);
		if (cnt != sizeof(cold_h)
			|| cold_h.magic != magic
			|| !valid_checksum32((uint32_t*)&cold_h, sizeof(cold_h)/sizeof(uint32_t))
			|| (!cold_h.wrap && cold_h.seq != h.seq))
		    return -1;
		cold_pos += cnt + cold_h.pre_pad;
		if (cold_h.wrap) {
		    if (cold_h.seq != 0)
			return -1;
		    entry_header_t h2;
		    cnt = safe_pread64(cold_fd, &h2, sizeof(h2), cold_pos);
		    if (cnt != sizeof(h2)
			    || memcmp(&cold_h, &h2, sizeof(h2)))
			return -1;
		    cold_pos = 0;
		    continue;
		}else {
		    entry_header_t h2;
		    uint64_t off = cold_pos + cold_h.data_len + cold_h.post_pad;
		    cnt = safe_pread64(cold_fd, &h2, sizeof(h2), off);
		    if (cnt != sizeof(h2)
			    || memcmp(&cold_h, &h2, sizeof(h2)))
			return -1;
		}
	    }while(false);
	    int len = cold_h.data_len;
	    bufferptr bp = buffer::create(len);
	    cnt = safe_pread64(cold_fd, bp.c_str(), len, cold_pos);
	    if (cnt != len)
		return -1;
	    cold_data.push_back(bp);
	}
    }while(false);
    hot_pos += h.coldset_len + h.pre_pad;

    ssize_t size = h.meta_len;
    if (!size)
        return -1;
    bufferptr bp = buffer::create(size);

    cnt = safe_pread64(hot_fd, bp.c_str(), size, hot_pos);
    assert (cnt == size);
    bl.push_back(bp);

    entry_header_t h2;
    hot_pos += h.meta_len + h.data_len + h.post_pad;
    cnt = safe_pread64(hot_fd, &h2, sizeof(h2), hot_pos);
    assert (cnt == sizeof(h2));
    if (memcmp(&h, &h2, sizeof(h)))
        return -1;
    hot_pos += sizeof(h);

    bufferlist::iterator p = bl.begin();

    op->seq = h.seq;
    op->entry_pos = entry_pos;
    op->data_offset = sizeof(h) + h.coldset_len + h.pre_pad + h.meta_len;
    op->coldset.swap(coldset);
    op->colddata.swap(cold_data);
    op->replay = true;
    op->tls.clear();

    // next_seq >= h.seq+1
    next_seq = h.seq + 1;

    for (uint32_t n = 0; n < h.ops; n++) {
        bufferlist ts;
        ::decode(ts, p);
        bufferlist::iterator t = ts.begin();
        Transaction *trans = NULL;
        bool have_more = true;
        do {
            trans = new ObjectStore::Transaction();
            try {
                trans->_decode (t);
            }
            catch (buffer::error& e) {
                delete trans;
                trans = NULL;
                have_more = false;
            }
            if (trans)
                op->tls.push_back(trans);
        }while(have_more);
    }
    return 0;
}

void NVMJournal::do_aio_write(bufferlist &hot_entry, bufferlist &cold_entry, uint64_t seq)
{
    if (hot_entry.length() == 0)
        return;

    Mutex::Locker locker(aioq_lock);
    off64_t pos = hot_write_pos;

    hot_entry.rebuild_page_aligned();

    while (hot_entry.length() > 0) {
        int max = MIN(hot_entry.buffers().size(), IOV_MAX - 1);
        iovec *iov = new iovec[max];
        int n = 0;
        unsigned len = 0;
        for (std::list<buffer::ptr>::const_iterator p = hot_entry.buffers().begin();
                n < max;
                ++p, ++n) {
            iov[n].iov_base = (void*)p->c_str();
            iov[n].iov_len = p->length();
            len += iov[n].iov_len;
        }

        bufferlist tbl;
        hot_entry.splice(0, len, &tbl);
	uint64_t mark_end = seq;
	if (hot_entry.length() || cold_entry.length())
	    mark_end = 0;
        aio_queue.push_back(aio_info(tbl, mark_end));
        aio_info& aio = aio_queue.back();
        aio.iov = iov;
        aio.next_hot_write_pos = pos + aio.len;
	aio.next_cold_write_pos = cold_write_pos;

        io_prep_pwritev(&aio.iocb, hot_fd, aio.iov, n, pos);

        aio_num ++;
        iocb *piocb = &aio.iocb;
        int attempts = 10;
        do {
            int r = io_submit(aio_ctx, 1, &piocb);
            if (r < 0) {
                if(r == -EAGAIN && attempts--) {
                    usleep(500);
                    continue;
                }
                assert( 0 == "io_submit got unexpected error");
            }
        }while(false);
        pos += aio.len;
    }
    hot_write_pos = pos;

    pos = cold_write_pos;
    cold_entry.rebuild_page_aligned();

    while (cold_entry.length() > 0) {
        int max = MIN(cold_entry.buffers().size(), IOV_MAX - 1);
        iovec *iov = new iovec[max];
        int n = 0;
        unsigned len = 0;
        for (std::list<buffer::ptr>::const_iterator p = cold_entry.buffers().begin();
                n < max;
                ++p, ++n) {
            iov[n].iov_base = (void*)p->c_str();
            iov[n].iov_len = p->length();
            len += iov[n].iov_len;
        }

        bufferlist tbl;
        cold_entry.splice(0, len, &tbl);
	uint64_t mark_end = seq;
	if (cold_entry.length())
	    mark_end = 0;
        aio_queue.push_back(aio_info(tbl, mark_end));
        aio_info& aio = aio_queue.back();
        aio.iov = iov;
        aio.next_hot_write_pos = hot_write_pos;
	aio.next_cold_write_pos = pos + aio.len;

        io_prep_pwritev(&aio.iocb, cold_fd, aio.iov, n, pos);

        aio_num ++;
        iocb *piocb = &aio.iocb;
        int attempts = 10;
        do {
            int r = io_submit(aio_ctx, 1, &piocb);
            if (r < 0) {
                if(r == -EAGAIN && attempts--) {
                    usleep(500);
                    continue;
                }
                assert( 0 == "io_submit got unexpected error");
            }
        }while(false);
        pos += aio.len;
    }
    cold_write_pos = pos;
   
    aioq_cond.Signal();
}

void NVMJournal::do_wrap()
{
    entry_header_t h;
    bufferlist bl;
    static bufferlist empty_bl;

    memset(&h, 0, sizeof(h));
    h.magic = magic;
    h.wrap = 1;
    uint64_t size = sizeof(h) * 2;
    h.pre_pad = ROUND_UP_TO(size, PAGE_SIZE) - size;
    h.checksum = create_checksum32((uint32_t*)&h, sizeof(h)/sizeof(uint32_t));
    
    bl.append((const char*)&h, sizeof(h));
    if (h.pre_pad) {
        bufferptr bp = buffer::create_static(h.pre_pad, zero_buf);
        bl.push_back(bp);
    }
    bl.append((const char*)&h, sizeof(h));
    if (hot_journal_wrap) {
	do_aio_write(bl, empty_bl, 0);
	hot_write_pos = 0;
	hot_journal_wrap = false;
	total_hot_wrap.inc();
    }
    if (cold_journal_wrap){
	do_aio_write(empty_bl, bl, 0);
	cold_write_pos = 0;
	cold_journal_wrap = false;
	total_cold_wrap.inc();
    }
}

void NVMJournal::writer_entry()
{
    while(true) 
    {
	adjust_heat_map();
	update_hot_set();
        {
            Mutex::Locker locker(writeq_lock);
            if (writer_stop)
                return;
            if (writeq.empty()) {
                writeq_cond.Wait(writeq_lock);
                continue;
            }
        }

        bufferlist hot_entry, cold_entry;
        list<Op*> ops;
        int r = prepare_multi_write(hot_entry, cold_entry, ops);
        if (r != 0)
            continue;

        uint64_t seq = 0;
        if (hot_entry.length()) {
            Mutex::Locker locker(op_queue_lock);
            seq = ops.back()->seq;
            op_queue.insert(op_queue.end(), ops.begin(), ops.end());
        }

        do_aio_write(hot_entry, cold_entry, seq);
        if (hot_journal_wrap || cold_journal_wrap) { 
            do_wrap();
        }
    }
}
void NVMJournal::stop_writer()
{
    {
        Mutex::Locker l(writeq_lock);
        writer_stop = true;
        writeq_cond.Signal();	
    }
    writer.join();	
}
/* reaper */
void NVMJournal::reaper_entry() 
{
    while (true) 
    {
        {
            Mutex::Locker locker(aioq_lock);
            if (aio_queue.empty()) {
                if (reaper_stop) 
                    break;
                aioq_cond.Wait(aioq_lock);
                continue;
            }
        }
        io_event event[32];
        int r = io_getevents(aio_ctx, 1, 16, event, NULL);
        if (r < 0) {
            if (r == -EINTR) 
                continue;
            assert(0 == "got unexpected error from io_getevents");
        }
        for (int i=0; i<r; i++) {
            aio_info *aio = (aio_info*)event[i].obj;
            if (event[i].res != aio->len) {
                assert(0 == "unexpected aio error");
            }
            aio->done = true;
        }

        check_aio_completion();
    }
}
void NVMJournal::stop_reaper()
{
    {
        Mutex::Locker locker(aioq_lock);
        reaper_stop = true;
        aioq_cond.Signal();
    }
    reaper.join();
}

/*NVMJournal::OpSequencer*/
void NVMJournal::OpSequencer::register_op(uint64_t seq)
{
    Mutex::Locker l(lock);
    op_q.push_back(seq);
}
void NVMJournal::OpSequencer::unregister_op()
{
    Mutex::Locker l(lock);
    op_q.pop_front();
}
void NVMJournal::OpSequencer::wakeup_flush_waiters(list<Context *> &to_queue)
{
    Mutex::Locker l(lock);
    uint64_t seq = 0;
    if (op_q.empty())
        seq = -1;
    else
        seq = op_q.front();

    while (!flush_waiters.empty()) {
        if (flush_waiters.front().first >= seq)
            break;
        to_queue.push_back(flush_waiters.front().second);
        flush_waiters.pop_front();
    }
    cond.Signal();
}
NVMJournal::Op* NVMJournal::OpSequencer::peek_queue()
{
    assert(apply_lock.is_locked());
    return tq.front();
}
void NVMJournal::OpSequencer::queue(Op *op)
{
    Mutex::Locker l(lock);  
    tq.push_back(op);
}
void NVMJournal::OpSequencer::dequeue()
{
    Mutex::Locker l(lock);
    tq.pop_front();
}
void NVMJournal::OpSequencer::flush()
{
    Mutex::Locker l(lock);
    if (op_q.empty())
        return;
    uint64_t seq = op_q.back();
    while (!op_q.empty() && op_q.front() <= seq)
        cond.Wait(lock);
}
bool NVMJournal::OpSequencer::flush_commit(Context *c)
{
    Mutex::Locker l(lock);
    uint64_t seq = 0;
    if (op_q.empty()) {
        delete c;
        return true;
    }
    seq = op_q.back();
    flush_waiters.push_back( make_pair(seq, c));
    return false;
}

/*NVMJournal::OpWQ::*/
bool NVMJournal::OpWQ::_enqueue(OpSequencer *posr)
{
    Journal->os_queue.push_back(posr);
    return true;
}
void NVMJournal::OpWQ::_dequeue(OpSequencer *posr)
{
    assert(0);
}
bool NVMJournal::OpWQ::_empty()
{
    return Journal->os_queue.empty();
}
NVMJournal::OpSequencer *NVMJournal::OpWQ::_dequeue()
{
    if (Journal->os_queue.empty())
        return NULL;
    OpSequencer *osr = Journal->os_queue.front();
    Journal->os_queue.pop_front();
    return osr;
}
void NVMJournal::OpWQ::_process(OpSequencer *osr, ThreadPool::TPHandle &handle)
{
    Journal->_do_op(osr, &handle);
}
void NVMJournal::OpWQ::_process_finish(OpSequencer *osr)
{
}
void NVMJournal::OpWQ::_clear()
{
    // assert (Journal->op_queue.empty());
}

void NVMJournal::queue_op(OpSequencer *osr, Op *op)
{
    osr->queue(op);
    op_wq.queue(osr);
}

void NVMJournal::check_aio_completion()
{
    uint64_t new_completed_seq = 0;
    uint64_t meta_not_sync_pos = 0;
    uint64_t cold_not_sync_pos = 0;
    {
        Mutex::Locker locker(aioq_lock);
        deque<aio_info>::iterator it = aio_queue.begin();
        while (it != aio_queue.end() && it->done) {
            if (it->seq) {
                new_completed_seq = it->seq;
                meta_not_sync_pos = it->next_hot_write_pos;
		cold_not_sync_pos = it->next_cold_write_pos;
            }
            aio_num --;
            ++it;
        }
        if (it != aio_queue.begin())
            aio_queue.erase(aio_queue.begin(), it);
    }

    if (new_completed_seq) {
        deque<Op*> completed;
        {
            Mutex::Locker locker(op_queue_lock);
            while(!op_queue.empty() 
                    && op_queue.front()->seq <= new_completed_seq) {
                completed.push_back(op_queue.front());
                op_queue.pop_front();
            }
        }
        dout(15) << " completed.size() = " << completed.size() << dendl;
        uint32_t ops = completed.size();
        // reserve ops
        apply_manager.op_apply_start(ops);
        meta_sync_pos = meta_not_sync_pos;
	cold_sync_pos = cold_not_sync_pos;

        utime_t now = ceph_clock_now(NULL);
        while(!completed.empty()) {
            Op *op = completed.front();
            op->journal_latency = now - op->journal_latency;
            op->osr_queue_latency = now;
            notify_on_committed(op);
            assert(op->posr);
            queue_op(op->posr, op);
            completed.pop_front();
        }
    }
}
void NVMJournal::notify_on_committed(Op *op) 
{
    assert(op);
    Context *ondisk = NULL;
    Transaction::collect_contexts(
            op->tls, NULL, &ondisk, NULL);
    if (ondisk)
        finisher->queue(ondisk);
}
void NVMJournal::notify_on_applied(Op *op)
{
    assert(op);
    Context *onreadable, *onreadable_sync;
    ObjectStore::Transaction::collect_contexts(
            op->tls, &onreadable, NULL, &onreadable_sync);
    if (onreadable_sync)
        onreadable_sync->complete(0);
    if (onreadable)
        finisher->queue(onreadable);
}
/* object */
NVMJournal::CollectionRef NVMJournal::get_collection(coll_t cid, bool create) 
{
    Mutex::Locker locker(coll_map.lock);
    ceph::unordered_map<coll_t, CollectionRef>::iterator itr = coll_map.collections.find(cid);
    if(itr == coll_map.collections.end()) {
        if (!create)
            return CollectionRef();
        if (!not_update_meta && !store->collection_exists(cid))
            return CollectionRef();
        coll_map.collections[cid].reset(new Collection(cid));
    }
    return coll_map.collections[cid];
}
NVMJournal::ObjectRef NVMJournal::get_object(CollectionRef coll, const ghobject_t &oid, bool create) 
{
    ObjectRef obj = NULL;
    if (!coll)
        return obj;
    Mutex::Locker l(coll->lock);
    ceph::unordered_map<ghobject_t, ObjectRef>::iterator p = coll->Object_hash.find(oid);
    if (p == coll->Object_hash.end()) {
        if (!create)
            return NULL;
        obj = new Object(coll->cid, oid);
        coll->Object_hash[oid] = obj;
        coll->Object_map[oid] = obj;
        if (!not_update_meta)
            store->_touch(coll->cid, oid);
    }

    obj = coll->Object_hash[oid];
    obj->get();
    return obj;
}

NVMJournal::ObjectRef NVMJournal::get_object(coll_t cid, const ghobject_t &oid, bool create) 
{
    CollectionRef coll;
    coll = get_collection(cid, create);
    if (!coll) {
        dout(15) << "failed to get collection of (" << cid << ") create = " << create << dendl;
        return NULL; 
    }
    return get_object(coll, oid, create);
}

void NVMJournal::erase_object_with_lock_hold (CollectionRef coll, const ghobject_t &oid)
{
    coll->Object_hash.erase(oid);
    coll->Object_map.erase(oid);
}

void NVMJournal::put_object(ObjectRef obj, bool locked) 
{
    if (0 == obj)
        return;

    if (obj->alias.empty()) {
        if ( 0 == obj->put())
            delete obj;
    }
    else {
        map< CollectionRef, shared_ptr<Mutex::Locker> > lockers;
        {
            RWLock::RLocker lock(obj->lock);
            set< pair<coll_t, ghobject_t> >::iterator p = obj->alias.begin();
            while (p != obj->alias.end()) {
                CollectionRef c = get_collection(p->first);
                if (lockers.count(c) == 0)
                    lockers[c] = shared_ptr<Mutex::Locker>(new Mutex::Locker(c->lock));
                ++ p;
            }
        }
        if (0 == obj->put()) {
            put_object(obj->parent);
            for (set< pair<coll_t, ghobject_t> >::iterator p = obj->alias.begin(); 
                    p != obj->alias.end(); 
                    ++p ) {
                CollectionRef coll = get_collection(p->first);
                erase_object_with_lock_hold (coll, p->second);
            }
            if (enable_cache) {
		int index = reinterpret_cast<int>(obj) % cache_slots;
                Mutex::Locker lock(buffer_cache[index].lock);
                map<uint32_t, BufferHead*>::iterator it = obj->data.begin();
                while (it != obj->data.end()) {
                    buffer_cache[index].purge(it->second);
                }
            }
            obj->alias.clear();
        }
    }
}

/* op_wq */
void NVMJournal::_do_op(OpSequencer *osr, ThreadPool::TPHandle *handle) 
{
    Mutex::Locker l(osr->apply_lock);
    Op *op = osr->peek_queue();
    utime_t now = ceph_clock_now(NULL);
    op->osr_queue_latency = now - op->osr_queue_latency;
    op->do_op_latency = now;
    do_op(op, handle);
    now = ceph_clock_now(NULL);
    op->do_op_latency = now - op->do_op_latency;
    update_latency(op->journal_latency, op->osr_queue_latency, op->do_op_latency);
    notify_on_applied(op);
    delete op;

    if(handle)
        handle->reset_tp_timeout();

    osr->unregister_op();
    list<Context *> to_queue;
    osr->wakeup_flush_waiters(to_queue);
    finisher->queue(to_queue);
    op_queue_release_throttle();

    osr->dequeue();
}

void NVMJournal::do_op(Op *op, ThreadPool::TPHandle *handle)
{
    assert(op);
    list<Transaction*>::iterator p = op->tls.begin();
    uint64_t seq = op->seq;
    uint64_t entry_pos = op->entry_pos;
    uint32_t offset = op->data_offset;    

    while (p != op->tls.end()) {
        Transaction *t = *p++;
        assert(t);
        do_transaction(t, op, seq, entry_pos, offset);
        if(handle)
            handle->reset_tp_timeout();
        if (op->replay)
            delete t;
    }
    if (!op->replay)
        apply_manager.op_apply_finish();
}

void NVMJournal::check_replay_point(uint64_t entry_pos)
{
    if (!not_update_meta)
        return;
    if (hot_start_pos == meta_sync_pos)
        not_update_meta = false;

    else if ((hot_start_pos <= entry_pos && entry_pos <= meta_sync_pos)
            || !(meta_sync_pos < entry_pos && entry_pos < hot_start_pos))
        return;
    not_update_meta = false;
}

void NVMJournal::do_transaction(Transaction* t, Op *op, uint64_t seq, uint64_t entry_pos, uint32_t &offset) 
{
    Transaction::iterator i = t->begin();
    while (i.have_op()) {
        int opcode = i.decode_op();
        int r = 0;
        /**
         * deal with opcode :
         * OP_WRITE, OP_ZERO, OP_TRUNCATE
         * OP_REMOVE, OP_CLONE, OP_CLONE_RANGE
         * DESTROY_COLLECTION, COLLECTION_ADD, SPLIT_COLLECTION
         * these operations will change the content of objects
         * or move objects between collections, and should be reflected
         * to MEMORY index ...
         */
        switch (opcode) {
            case Transaction::OP_NOP:
                break;
            case Transaction::OP_TOUCH:
                {
                    dout(10) << "opcode = OP_TOUCH" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    /* do nothing */
                    r = _touch(cid, oid);
                }
                break;
            case Transaction::OP_WRITE:
                {
                    dout(10) << "opcode = OP_WRITE" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    uint64_t off = i.decode_length();
                    uint64_t len = i.decode_length();
		    bool is_cold = op->coldset.count(oid) != 0;
                    bufferlist bl;
                    if (!replay)
                        i.decode_data(bl, len);
		    else if (!not_update_meta && is_cold && len)
			op->colddata.splice(0, len, &bl);
                    r = _write(cid, oid, off, len, bl, entry_pos, offset, is_cold);
		    if (!is_cold)
			offset += len;
                }
                break;
            case Transaction::OP_ZERO:
                {
                    dout(10) <<  "opcode = OP_ZERO" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    uint64_t off = i.decode_length();
                    uint64_t len = i.decode_length();
                    // we should create a special bufferhead which stand for zero, 
                    // and commit to backend storage later 
                    r = _zero (cid, oid, off, len);
                }
                break;
            case Transaction::OP_TRIMCACHE:
                {
                    dout(10) << "opcode = OP_TRIMCACHE" << dendl;
                    i.decode_cid();
                    i.decode_oid();
                    i.decode_length();
                    i.decode_length();
                    r = -EOPNOTSUPP;
                }
                break;
            case Transaction::OP_TRUNCATE:
                {
                    dout(10) << "opcode = OP_TRUNCATE" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    uint64_t off = i.decode_length();
                    // create a special bufferhead which stand for trun,   
                    // and commit to backend storage later
                    r = _truncate(cid, oid, off);
                }
                break;
            case Transaction::OP_REMOVE:
                {
                    dout(10) << "opcode = OP_REMOVE" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid(); 
                    r = _remove (cid, oid);
                }
                break;

                /* deal with clone operation */
            case Transaction::OP_CLONE:
                {   /*clone data, xattr and omap ...*/
                    dout(10) << "opcode = OP_CLONE" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t src = i.decode_oid();
                    ghobject_t dst = i.decode_oid();
                    r = _clone(cid, src, dst);
                }
                break;
            case Transaction::OP_CLONERANGE:
            case Transaction::OP_CLONERANGE2:
                {
                    /*clone data only ...*/
                    dout(10) << "opcode = OP_CLONERANGE*" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t src = i.decode_oid();
                    ghobject_t dst = i.decode_oid();
                    uint64_t off = i.decode_length();
                    uint64_t len = i.decode_length();
                    uint64_t dstoff = off;
                    if (opcode == Transaction::OP_CLONERANGE2)
                        dstoff = i.decode_length();
                    r = _clone_range(cid, src, dst, off, len, dstoff);
                }
                break;
            case Transaction::OP_MKCOLL:
                {
                    dout(10) << "opcode = OP_MKCOLL" << dendl;
                    coll_t cid = i.decode_cid();
                    r = _create_collection(cid);
                }
                break;
            case Transaction::OP_RMCOLL:
                {
                    dout(10) << "opcode = OP_RMCOLL" << dendl;
                    coll_t cid = i.decode_cid();
                    r = _destroy_collection(cid);
                }
                break;
            case Transaction::OP_COLL_ADD:
                {
                    dout(10) <<  "opcode = OP_COLL_ADD" << dendl;
                    coll_t ncid = i.decode_cid();
                    coll_t ocid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    r = _collection_add(ncid, ocid, oid);
                }
                break;
            case Transaction::OP_COLL_REMOVE:
                {
                    dout(10) << "opcode = OP_COLL_REMOVE" << dendl;
                    coll_t cid = i.decode_cid();
                    ghobject_t oid = i.decode_oid();
                    r = _remove(cid, oid);
                }
            case Transaction::OP_COLL_MOVE:
                {
                    assert(0 == "deprecated");
                    break;
                }
            case Transaction::OP_COLL_MOVE_RENAME:
                {
                    dout(10) << "opcode = OP_MOVE_RENAME" << dendl;
                    coll_t oldcid = i.decode_cid();
                    ghobject_t oldoid = i.decode_oid();
                    coll_t newcid = i.decode_cid();
                    ghobject_t newoid = i.decode_oid();
                    r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
                }
                break;
            case Transaction::OP_COLL_RENAME:
                {
                    dout(10) << "opcode = OP_COLL_RENAME" << dendl;
                    coll_t cid = i.decode_cid();
                    coll_t ncid = i.decode_cid();
                    r = _collection_rename(cid, ncid);
                }
                break;
            case Transaction::OP_SPLIT_COLLECTION:
                assert(0 == "deprecated");
                break;
            case Transaction::OP_SPLIT_COLLECTION2:
                {
                    dout(10) << "opcode = OP_SPLIT_COLLECTION2" << dendl;
                    coll_t cid = i.decode_cid();
                    uint32_t bits = i.decode_u32();
                    uint32_t rem = i.decode_u32();
                    coll_t dest = i.decode_cid();
                    r = _split_collection(cid, bits, rem, dest);
                }
                break;
            default:
                r = do_other_op(opcode, i, entry_pos);
        }
        if (r != 0)
            dout(10) << "result = " << r << dendl;
    }
}
int NVMJournal::do_other_op(int opcode, Transaction::iterator& i, uint64_t entry_pos)
{
    dout(10) <<  "opcode = " << opcode << dendl;
    int r = 0 ;
    uint32_t pos = entry_pos >> PAGE_SHIFT;
    switch(opcode)   /* deal with the attributes of objects */
    {
        case Transaction::OP_SETATTR: 
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                string name = i.decode_attrname();
                bufferlist bl;
                i.decode_bl(bl);
                map<string, bufferptr> to_set;
                to_set[name] = bufferptr(bl.c_str(), bl.length());
		if (enable_omap_cache)
		    r = omap_cache.setattrs(cid, oid, to_set, pos);
		else if (!not_update_meta)
                    r = store->_setattrs(cid, oid, to_set);
            }
            break;
        case Transaction::OP_SETATTRS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		map<string, bufferptr> aset;
		i.decode_attrset(aset);
		if (enable_omap_cache)
		    r = omap_cache.setattrs(cid, oid, aset, pos);
		else if (!not_update_meta)
		    r = store->_setattrs(cid, oid, aset);
	    }
            break;
        case Transaction::OP_RMATTR:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                string name = i.decode_attrname();
                if (enable_omap_cache)
		    r = omap_cache.rmattr(cid, oid, name.c_str(), pos);
		else if (!not_update_meta)
                    r = store->_rmattr(cid, oid, name.c_str());
            }
            break;
        case Transaction::OP_RMATTRS:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
		if (enable_omap_cache)
		    r = omap_cache.rmattrs(cid, oid, pos);
		else if (!not_update_meta)
                    r = store->_rmattrs(cid, oid);
            }
            break;
        case Transaction::OP_COLL_HINT:
            {
                coll_t cid = i.decode_cid();
                uint32_t type = i.decode_u32();
                bufferlist hint;
                i.decode_bl(hint);
                bufferlist::iterator pitr = hint.begin();
                if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
                    uint32_t pg_num;
                    uint64_t num_objs;
                    ::decode(pg_num, pitr);
                    ::decode(num_objs, pitr);
                    if (!not_update_meta)
                        r = store->_collection_hint_expected_num_objs(cid, pg_num, num_objs);
                } 
            }
            break;
        case Transaction::OP_COLL_SETATTR:
            {
                coll_t cid = i.decode_cid();
                string name = i.decode_attrname();
                bufferlist bl;
                i.decode_bl(bl);
                if (!not_update_meta)
                    r = store->_collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
            }
            break;
        case Transaction::OP_COLL_RMATTR:
            {
                coll_t cid = i.decode_cid();
                string name = i.decode_attrname();
                if (!not_update_meta)
                    r = store->_collection_rmattr(cid, name.c_str());
            }
            break;
        case Transaction::OP_OMAP_CLEAR:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
		if (enable_omap_cache)
		    r = omap_cache.omap_clear(cid, oid, pos);
		else if (!not_update_meta)
                    r = store->_omap_clear(cid, oid);
            }
            break;
        case Transaction::OP_OMAP_SETKEYS:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                map<string, bufferlist> aset;
                i.decode_attrset(aset);
		if (enable_omap_cache)
		    r = omap_cache.omap_setkeys(cid, oid, aset, pos);
		else if (!not_update_meta)
                    r = store->_omap_setkeys(cid, oid, aset);
            }
            break;
        case Transaction::OP_OMAP_RMKEYS:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                set<string> keys;
                i.decode_keyset(keys);
		if (enable_omap_cache)
		    r = omap_cache.omap_rmkeys(cid, oid, keys, pos);
		else if (!not_update_meta)
                    r = store->_omap_rmkeys(cid, oid, keys);
            }
            break;
        case Transaction::OP_OMAP_RMKEYRANGE:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                string first = i.decode_key();
                string last = i.decode_key();
		if (enable_omap_cache)
		    r = omap_cache.omap_rmkeyrange(cid, oid, first, last, pos);
		else if (!not_update_meta)
                    r = store->_omap_rmkeyrange(cid, oid, first, last);
            }
            break;
        case Transaction::OP_OMAP_SETHEADER:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                bufferlist bl;
                i.decode_bl(bl);
		if (enable_omap_cache)
		    r = omap_cache.omap_setheader(cid, oid, bl, pos);
		else if (!not_update_meta)
                    r = store->_omap_setheader(cid, oid, bl);
            }
            break;
        case Transaction::OP_SETALLOCHINT:
            {
                coll_t cid = i.decode_cid();
                ghobject_t oid = i.decode_oid();
                i.decode_length();
                i.decode_length();
            }
            break;
        default:
            derr << "bad op " << opcode << dendl;
            assert(0);
    }
    return r;
}

static uint64_t build_range_bit64(uint32_t start, uint32_t end)
{
    const uint32_t block_size = 64*1024;
    const uint64_t one = 1;
    start = min(64u, start/block_size);
    end = min(64u, (end+block_size-1)/block_size);
    return (one<<end) - (one<<start);
}
double NVMJournal::heat_info_t::touch(uint32_t s, uint32_t e)
{
    now = ceph_clock_now(g_ceph_context);
    uint64_t bits = build_range_bit64(s, e);
    if (!bitmap)
	heat = 0.6;
    if (bitmap & bits)
	heat = heat*pow(0.2, (now - last_update_heat)/(double)cycle) + 0.8;
    if (now - last_update_bitmap > cycle*0.4)
	bitmap = 0;
    bitmap |= bits;
    last_update_bitmap = now;
    last_update_heat = now;
    return heat;
}
double NVMJournal::heat_info_t::get_heat()
{
    return heat*pow(0.2, (now - last_update_heat)/(double)(cycle));
}
bool NVMJournal::heat_info_wrapper::operator==(const heat_info_wrapper &other)
{
    return point->obj == other.point->obj; 
}
bool NVMJournal::heat_info_wrapper::operator<(const heat_info_wrapper &other)
{
    return point->get_heat() > other.point->get_heat();
}
bool NVMJournal::heat_info_wrapper::operator>(const heat_info_wrapper &other)
{
    return point->get_heat() < other.point->get_heat();
}
void NVMJournal::dump_heat_rank(ostream &os)
{
    const int step = 10;
    int count = 0;
    if (1) {
	Mutex::Locker lock(heat_map_lock);
	RankTree<heat_info_wrapper>::node_type* node = heat_rank.next();
	while (node) {
	    if (!(count%step)) {
		heat_info_t *h = node->key.point;
		os << count << ": " << h->obj.hobj.hash << ":" << h->get_heat() << std::endl;
	    }
	    count ++;
	    node = heat_rank.next(node);
	}
    }
}
double NVMJournal::get_object_heat(const ghobject_t& oid)
{
    Mutex::Locker lock(heat_map_lock);
    heat_hash_t::iterator it = object_heat_map.find(oid);
    if (it == object_heat_map.end())
	return 0.0;
    heat_info_t *h = it->second;
    return h->get_heat();
}

void NVMJournal::init_heat_map()
{
    const int object_size = 4*1024*1024;
    const int rate = 50*1024*1024;
    heat_info_t::now = ceph_clock_now(g_ceph_context);
    heat_info_t::cycle.set_from_double(0.4*hot_journal_size/rate);
    hot_rank_throttle = 0.5 * evict_threshold * hot_journal_size / object_size;
    hot_heat_throttle = 4.0;
}
void NVMJournal::adjust_heat_map()
{
    const int object_size = 4*1024*1024;
    uint64_t jsize = 0, used = 0;
    uint64_t threshold = 1.8*(1-evict_threshold) * hot_journal_size;
    if (hot_write_pos >= hot_start_pos)
	jsize = hot_journal_size - (hot_write_pos - hot_start_pos);
    else
	jsize = hot_start_pos - hot_write_pos;

    if (jsize < threshold) {
	Mutex::Locker lock(heat_map_lock);
	if (hot_write_pos >= data_sync_pos)
	    used = hot_write_pos - data_sync_pos;
	else
	    used = hot_journal_size - (data_sync_pos - hot_write_pos);
	jsize += (used + total_overlap.read() + total_evict.read() - total_cached.read()) * 0.7;
	int new_rank_throttle = 0.5 * jsize / object_size;
	for (int rank = new_rank_throttle; rank < hot_rank_throttle; rank ++) {
	    heat_info_t *h = heat_rank.rank2key(rank).point;
	    if (h) new_cold_object.insert(h->obj);
	}
	for (int rank = hot_rank_throttle; rank < new_rank_throttle; rank ++) {
	    heat_info_t *h = heat_rank.rank2key(rank).point;
	    if (h) new_hot_object.insert(h->obj);
	}
	hot_rank_throttle = new_rank_throttle;
    }
}
int NVMJournal::update_heat_map(ghobject_t oid, uint32_t s, uint32_t e)
{
    double heat = 0.0;
    int oldrank = -1, rank = -1;
    Mutex::Locker lock(heat_map_lock);
    heat_hash_t::iterator it = object_heat_map.find(oid);
    if (it == object_heat_map.end()) {
	heat_info_t *h= new heat_info_t(oid);
	object_heat_map[oid] = h;
	heat = h->touch(s, e);
	rank = heat_rank.insert(h);
    }
    else {
	heat_info_t *h = it->second;
	oldrank = heat_rank.remove(h);
	heat = h->touch(s, e);
	rank = heat_rank.insert(h);
    }
    if (rank < hot_rank_throttle) {
	Mutex::Locker l(hot_lock);
	if (heat > hot_heat_throttle)
	    new_hot_object.insert(oid);
	if (oldrank >= hot_rank_throttle) {
	    heat_info_t *h = heat_rank.rank2key(hot_rank_throttle).point;
	    if (h) new_cold_object.insert(h->obj);
	}
    }
    return heat;
}
void NVMJournal::update_hot_set()
{
    set<ghobject_t> to_insert, to_remove;
    {
	Mutex::Locker l(hot_lock);
	to_insert.swap(new_hot_object);
	to_remove.swap(new_cold_object);
    }
    if (!to_insert.empty()) {
	hot_object_set.insert(to_insert.begin(), to_insert.end());
    }
    if (!to_remove.empty()) {
	for (set<ghobject_t>::iterator it = to_remove.begin();
		it != to_remove.end();
		++ it)
	    hot_object_set.erase(*it);
    }
}

bool NVMJournal::check_is_hot(ghobject_t oid)
{
    if (!enable_hot_journal)
	return false;
    return hot_object_set.find(oid) != hot_object_set.end();
}

int NVMJournal::_touch(coll_t cid, const ghobject_t &oid)
{
    return store->_touch(cid, oid);
}

int NVMJournal::_write(coll_t cid, const ghobject_t& oid, uint32_t off, uint32_t len, 
        bufferlist& bl, uint64_t entry_pos, uint32_t boff, bool is_cold)
{
    ObjectRef obj = get_object(cid, oid, true);
    assert(NULL != obj);

    BufferHead *bh = new BufferHead;
    bh->owner = obj;
    bh->ext.start = off;
    bh->ext.end = off+len;
    bh->bentry = entry_pos >> PAGE_SHIFT;
    bh->boff = boff;
    bh->heat = 0.0;
    bh->dirty = true;

    {
        RWLock::WLocker locker(obj->lock);
        uint32_t overlap = merge_new_bh(obj, bh);
	
	if (!is_cold)
	    total_cached.add(len);
	total_overlap.add(overlap);
        
	if (is_cold && !not_update_meta) {
            store->_write(cid, oid, off, len, bl);
	    bh->dirty = false;
            total_flush.add(len);
        }
	if (is_cold){
	    map<uint32_t, BufferHead*>::iterator it = obj->data.find(off);
	    assert(it != obj->data.end());
	    obj->data.erase(it);
	    delete bh;
	}
        if (enable_cache && !is_cold && !replay) {
	    int index = reinterpret_cast<int>(obj) % cache_slots;
            Mutex::Locker lock(buffer_cache[index].lock);
            buffer_cache[index].add(bh, bl);
        }
    }

    if (!is_cold) {
        Mutex::Locker locker(Journal_queue_lock);
        Journal_queue.push_back(bh);
    }
    else {
	put_object(obj);
    }

    update_heat_map(oid, off, off+len);
    total_write.add(len);
    return 0;
}
/* zero */
int NVMJournal::_zero(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len)
{
    ObjectRef obj = get_object(cid, oid, true);
    if (!obj)
        assert(0 == "get unexpected error from _zero"); 

    BufferHead *bh = new BufferHead;
    bh->owner = obj;
    bh->ext.start = off;
    bh->ext.end = off + len;
    bh->bentry = BufferHead::ZERO;
    bh->boff = 0;
    bh->dirty = true;

    {
        RWLock::WLocker l(obj->lock);
        uint32_t overlap = merge_new_bh (obj, bh);

	if (overlap)
	    total_overlap.add(overlap);
        if (!not_update_meta) {
            store->_zero(cid, oid, off, len);
            bh->dirty = false;
        }
    }

    {
        // deallocate the object space when evicted
        Mutex::Locker l(Journal_queue_lock);
        Journal_queue.push_back (bh);
    }
    return 0;
}
#define _ONE_GB	(1024*1024*1024)
/* do truncate */
int NVMJournal::_truncate(coll_t cid, const ghobject_t &oid, uint32_t off)
{
    ObjectRef obj = get_object(cid, oid, true);
    assert (obj);

    BufferHead *bh = new BufferHead;
    bh->owner = obj;
    bh->ext.start = off;
    bh->ext.end = _ONE_GB;
    bh->bentry = BufferHead::TRUNC;
    bh->boff = 0;
    bh->dirty = true;

    {
        RWLock::WLocker l(obj->lock);
        int overlap = merge_new_bh (obj, bh);
	if (overlap)
	    total_overlap.add(overlap);
    }

    if (!not_update_meta)
        store->_truncate(cid, oid, (uint64_t)off);

    {
        Mutex::Locker l(Journal_queue_lock);
        Journal_queue.push_back (bh);
    }
    return 0;
}
/* _remove */
int NVMJournal::_remove(coll_t cid, const ghobject_t& oid)
{
    CollectionRef coll = get_collection(cid);
    if (coll != NULL) {
        ObjectRef obj = get_object(coll, oid);
        if (obj != NULL) {
            RWLock::WLocker l(obj->lock);  
            obj->alias.erase(make_pair(cid, oid));
            {
                Mutex::Locker l(coll-> lock);
                erase_object_with_lock_hold(coll, oid);
            }
            put_object(obj);
        }
    }
    if (!not_update_meta)
        store->_remove(cid, oid);
    return 0;
}

int NVMJournal::_clone(coll_t cid, const ghobject_t& src, const ghobject_t &dst)
{
    dout(5) << "not supported!" << dendl;
    return -1;
}
int NVMJournal::_clone_range(coll_t cid, ghobject_t &src, ghobject_t &dst, 
        uint64_t off, uint64_t len, uint64_t dst_off)
{
    dout(5) << "not supported!" << dendl;
    return -1;
}
int NVMJournal::_create_collection(coll_t cid)
{
    int r = 0;
    if (!not_update_meta)
        r = store->_create_collection(cid);
    return r;
}
int NVMJournal::_destroy_collection(coll_t cid)
{
    int ret = 1;
    {
        Mutex::Locker locker(coll_map.lock);
        ceph::unordered_map<coll_t, CollectionRef>::iterator it = coll_map.collections.find(cid);
        if (it != coll_map.collections.end()) {
            CollectionRef c = it->second;
            Mutex::Locker l(c->lock); 
            if (c->Object_map.empty()) {
                coll_map.collections.erase(it);
                ret = 0;
            }
        }
        else
            ret = 0;
    }

    if (!ret && !not_update_meta)
        ret = store->_destroy_collection(cid);
    return ret;

}
int NVMJournal::_collection_add(coll_t dst, coll_t src, const ghobject_t &oid)
{
    int ret = 0;
    ObjectRef obj = get_object(src, oid);
    if (obj) {
        CollectionRef dstc = get_collection(dst, true);
        assert(dstc);
        RWLock::WLocker l1 (obj->lock);
        {
            Mutex::Locker l2 (dstc->lock);
            dstc->Object_hash[oid] = obj;
            dstc->Object_map[oid] = obj;
        }
        obj->alias.insert( make_pair(dst, oid));
    }
    if (!not_update_meta)
        ret = store->_collection_add(dst, src, oid);
    return ret;
}

int NVMJournal::_collection_move_rename(coll_t oldcid, const ghobject_t &oldoid,
        coll_t newcid, const ghobject_t &newoid)
{
    int ret = 0;
    CollectionRef srcc = get_collection(oldcid);
    if (srcc) {
        CollectionRef dstc = get_collection(newcid, true);
        assert(dstc);
        ObjectRef obj = get_object(srcc, oldoid);
        RWLock::WLocker locker (obj->lock);
        if (obj) {
            if (srcc != dstc) {
                Mutex::Locker l1 (MIN(&(*srcc), &(*dstc))->lock);
                Mutex::Locker l2 (MAX(&(*srcc), &(*dstc))->lock);
                erase_object_with_lock_hold(srcc, oldoid);
                dstc->Object_hash[newoid] = obj;
                dstc->Object_map[newoid] = obj;
            }
            else {
                Mutex::Locker l (srcc->lock);
                erase_object_with_lock_hold(srcc, oldoid);
                srcc->Object_hash[newoid] = obj;
                srcc->Object_map[newoid] = obj;
            }
            obj->alias.erase( make_pair(oldcid, oldoid));
            obj->alias.insert( make_pair(newcid, newoid));
        }
    }
    if (!not_update_meta)
        ret = store->_collection_move_rename(oldcid, oldoid, newcid, newoid);
    return ret;
}
int NVMJournal::_collection_rename(coll_t cid, coll_t ncid)
{
    int ret = 0;
    CollectionRef coll = get_collection(cid);
    pause_ev_work();
    if (coll) {
        {
            Mutex::Locker l(coll_map.lock);
            coll_map.collections[ncid] = coll;
            coll_map.collections.erase(cid);
        }
        for (map<ghobject_t, ObjectRef>::iterator itr = coll->Object_map.begin();
                itr != coll->Object_map.end();
                ++itr) {
            ObjectRef o = itr->second;
            o->alias.erase( make_pair(cid, itr->first));
            o->alias.insert( make_pair(ncid, itr->first));
        }
    }
    if (!not_update_meta)
        ret = store->_collection_rename(cid, ncid);
    unpause_ev_work();
    return ret;
}
int NVMJournal::_split_collection(coll_t src, uint32_t bits, uint32_t match, coll_t dst)
{
    int ret = 0;
    pause_ev_work();
    CollectionRef srcc = get_collection(src);
    if (srcc) {
        CollectionRef dstc = get_collection(dst, true);
        map<ghobject_t, ObjectRef>::iterator p = srcc->Object_map.begin();
        while (p != srcc->Object_map.end())
        {
            if (p->first.match(bits, match)) {
                ObjectRef obj = p->second;
                srcc->Object_hash.erase(p->first);
                dstc->Object_hash[p->first] = obj;
                dstc->Object_map[p->first] = obj;
                // update the alias of object
                obj->alias.insert( make_pair(dst, p->first));
                obj->alias.erase( make_pair(src, p->first));
                srcc->Object_map.erase(p++);
            }
            else 
                ++ p;
        }
    }
    if (!not_update_meta)
        ret = store->_split_collection(src, bits, match, dst);
    unpause_ev_work();
    return ret;
}
uint32_t NVMJournal::merge_new_bh(ObjectRef obj, BufferHead* new_bh)
{
    assert(obj->lock.is_wlocked());
    uint32_t new_start, new_end, overlap;
    new_start = new_bh->ext.start;
    new_end = new_bh->ext.end;
    overlap = 0;
    if (new_start == new_end)
        return 0;
    int index = reinterpret_cast<int>(obj) % cache_slots;
    
    map<uint32_t, BufferHead*>::iterator p = obj->data.lower_bound(new_bh->ext.start); 
    if (p != obj->data.begin())
        --p;
    while (p != obj->data.end()) {
        BufferHead *bh = p->second;
        uint32_t start, end;
        start = bh->ext.start;
        end = bh->ext.end;

        if (new_start <= start) {
            /* new_start, start, end, new_end */
            if (end <= new_end) {
                overlap += end - start;
		bh->ext.start = bh->ext.end; 
                obj->data.erase(p++);
                /* cache */
                if (enable_cache) {
                    Mutex::Locker lock(buffer_cache[index].lock);
                    buffer_cache[index].purge(bh);
                }
		if (!bh->owner) 
                    delete bh;
                continue;
            }
            /* new_start, start, new_end, end */
            else if (start < new_end) {
                overlap += new_end - start;
		if (enable_cache) { /* cache */
                    Mutex::Locker lock(buffer_cache[index].lock);
                    bufferlist bl;
                    bool found = buffer_cache[index].lookup(bh, bl);
                    if (found) {
                        bufferlist newbl;
                        newbl.substr_of(bl, new_end-start, end-new_end);
			buffer_cache[index].purge(bh);
                        buffer_cache[index].add(bh, newbl);
                    }
                }
                bh->boff += new_end - start;
                bh->ext.start = new_end;
                obj->data.erase(p);
                obj->data[bh->ext.start] = bh;
            }
            /* new_start, new_end, start, end */
            else { /* do nothing */}
            break;
        }
        else {
            /* start, end, new_start, new_end */
            if (end <= new_start) {
                p++; /* pass */
            }
            /* start, new_start, end, new_end */
            else if (end <= new_end) {
		overlap += end - new_start;
                if (enable_cache) {
                    Mutex::Locker lock(buffer_cache[index].lock);
                    bufferlist bl;
                    bool found = buffer_cache[index].lookup(bh, bl);
                    if (found) {
                        bufferlist newbl;
                        newbl.substr_of(bl, 0, new_start-start);
                        buffer_cache[index].purge(bh);
                        buffer_cache[index].add(bh, newbl);
                    }
                }
                bh->ext.end = new_start;
                p ++;
            }
            /*start, new_start, new_end, end */
            else {
		overlap += new_end - new_start;
                obj->data.erase(p);
                /* create two new BufferHead */
                BufferHead *left, *right;
                left = new BufferHead();
                left->owner = NULL; 
                left->ext.start = start;
                left->ext.end = new_start;
                left->bentry = bh->bentry;
                left->boff = bh->boff;
                left->dirty = bh->dirty;
                if (left->bentry == BufferHead::TRUNC)
                    left->bentry = BufferHead::ZERO;
                obj->data[start] = left;

                right = new BufferHead();
                right->owner = NULL;
                right->ext.start = new_end;
                right->ext.end = end;
                right->bentry = bh->bentry;
                right->boff = bh->boff + (new_end-start);
                right->dirty = bh->dirty;
                obj->data[new_end] = right;

                if (enable_cache) { /*cache*/
                    Mutex::Locker lock(buffer_cache[index].lock);
                    bufferlist bl;
                    bool found = buffer_cache[index].lookup(bh, bl);
                    if (found) {
                        bufferlist left_bl, right_bl;
                        left_bl.substr_of(bl, 0, new_start-start);
                        right_bl.substr_of(bl, new_end-start, end-new_end);
                        buffer_cache[index].purge(bh);
                        buffer_cache[index].add(right, right_bl);
                        buffer_cache[index].add(left, left_bl);
                    }
                }
                if (!bh->owner) 
                    delete bh;
                break;	
            }
        }
    }

    obj->data[new_bh->ext.start] = new_bh;
    if (new_bh->bentry == BufferHead::TRUNC) 
        obj->size = new_bh->ext.start;
    else if (new_bh->ext.end > obj->size)
        obj->size = new_bh->ext.end;

    return overlap;
}

void NVMJournal::delete_bh(ObjectRef obj, uint32_t off, uint32_t end, uint32_t bentry)
{
    assert(obj->lock.is_wlocked());
    map<uint32_t, BufferHead*>::iterator p = obj->data.lower_bound(off);
    if (p!=obj->data.begin())
        p--;
    int index = reinterpret_cast<int>(obj) % cache_slots;
    while (p != obj->data.end()) {
        BufferHead *bh = p->second;
        if (bh->ext.start >= end)
            break;
        if (bh->ext.end <= off) {
            p ++;
            continue;
        }
        if (bh->bentry == bentry) {
            if (enable_cache) {
                Mutex::Locker lock(buffer_cache[index].lock);
                buffer_cache[index].purge(p->second);
            }
            obj->data.erase(p++);
            if (!bh->owner)
                delete bh;
        }
        else 
            p ++;
    }
}

/* READ */
#define SSD_OFF(pbh) (((uint64_t)(pbh->bentry) << PAGE_SHIFT) + pbh->boff)

void NVMJournal::map_read(ObjectRef obj, uint32_t off, uint32_t end,
        map<uint32_t, uint32_t> &hits,
        map<uint32_t, bufferlist> &hitcache,
        map<uint32_t, uint64_t> &trans,
        map<uint32_t, uint32_t> &missing,
        bool trunc_as_zero)
{
    assert (obj && obj->lock.is_locked());
    map<uint32_t,BufferHead*>::iterator p = obj->data.lower_bound(off);
    if (p != obj->data.begin())
        -- p;
    int index = reinterpret_cast<int>(obj) % cache_slots;
    while (p != obj->data.end()) 
    {
        BufferHead *pbh = p->second;
        if (pbh->ext.start <= off) {
            // _bh_off_, _bh_end_, off, end
            if (pbh->ext.end <= off ) {
                p++;
                continue;
            }
            // _bh_off_, off, _bh_end_, end
            else if (pbh->ext.end < end) 
            {
                assert(pbh->bentry != BufferHead::TRUNC);
                if (pbh->bentry != BufferHead::ZERO) {
                    trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                    if (enable_cache) {
                        Mutex::Locker lock(buffer_cache[index].lock);
                        bufferlist bl;
                        bool found = buffer_cache[index].lookup(pbh, bl);
                        if (found) {
                            bufferlist hitbl;
                            hitbl.substr_of(bl, off-pbh->ext.start, pbh->ext.end-off);
                            hitcache[off] = hitbl;
                        }
                    }
                }
                else
                    trans[off] = ((uint64_t)(pbh->bentry)) << PAGE_SHIFT;

                hits[off] = pbh->ext.end - off;
                off = pbh->ext.end;
                p++;
                continue;
            }
            // _bh_off_, off, end, _bh_end_     
            else {
                if (pbh->bentry != BufferHead::ZERO && pbh->bentry != BufferHead::TRUNC) {
                    trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                    if (enable_cache) {
                        Mutex::Locker lock(buffer_cache[index].lock);
                        bufferlist bl;
                        bool found = buffer_cache[index].lookup(pbh, bl, true);
                        if (found) {
                            bufferlist hitbl;
                            hitbl.substr_of(bl, off-pbh->ext.start, end-off);
                            hitcache[off] = hitbl;
                        }
                    }
                }
                else if (trunc_as_zero)
                    trans[off] = ((uint64_t)BufferHead::ZERO) << PAGE_SHIFT;
                else
                    trans[off] = ((uint64_t)(pbh->bentry)) << PAGE_SHIFT;
                hits[off] = end - off;
                return;
            }
        }
        else {
            if (end <= pbh->ext.start) {
                missing[off] = end - off;
                return;
            }
            // off, _bh_off_, _bh_end_, end OR off, _bh_off_, end, _bh_end_
            missing[off] = pbh->ext.start - off;
            off = pbh->ext.start;
            continue;
        }
    }

    if (off < end)
        missing[off] = end-off;
}

void NVMJournal::build_read(coll_t &cid, const ghobject_t &oid, uint64_t off, size_t len, ReadOp &op) 
{
    uint32_t attempts = 4;
    ObjectRef obj = NULL;
    do {
        obj = get_object(cid, oid);
        if (!obj) 
            break;
        get_read_lock(obj);
        if (!obj->alias.empty())
            break;
        put_read_lock(obj);
        put_object(obj);
        obj = NULL;
    } while(attempts--);

    op.cid = cid;
    op.oid = oid;
    op.obj = obj;
    op.off = off;
    op.length = len;

    if (!op.obj 
            //|| !obj->cachable /*must read journal first, or will get wrong data after replay journal*/
            || obj->alias.empty()) {
        op.missing[off] = len;
        return;
    }

    map_read(obj, off, off+len, op.hits, op.hitcache, op.trans, op.missing);

    if (obj->parent && !op.missing.empty())
        build_read_from_parent(obj->parent, obj, op);
}
void NVMJournal::build_read_from_parent(ObjectRef parent, ObjectRef obj, ReadOp& op)
{
    assert (parent);
    if (parent->data.empty())
        return;

    get_read_lock(parent);
    op.parents.push_back(parent);

    map<uint32_t, uint32_t> missing;
    map<uint32_t, uint32_t>::iterator p = op.missing.begin();
    while (p != op.missing.end()) {
        map_read(parent, p->first, p->first+p->second, 
                op.hits, op.hitcache, op.trans, missing, true);
        p ++;
    }
    missing.swap(op.missing);
    if (parent->parent && !missing.empty())
        build_read_from_parent(parent->parent, parent, op);
}

int NVMJournal::do_read(ReadOp &op)
{
    if (!op.obj && !store->exists(op.cid, op.oid)){
        dout(10) << op.cid << "/" << op.oid << "not exist!!" << dendl;
        return -ENOENT;
    }

    map<uint32_t, bufferptr> data;
    for (map<uint32_t, uint32_t>::iterator p = op.hits.begin();
            p != op.hits.end(); 
            ++p) {
        /*hit cache*/
        if (enable_cache) {
            map<uint32_t, bufferlist>::iterator it = op.hitcache.find(p->first);
            if (it != op.hitcache.end()) {
                uint32_t off = p->first;
                bufferlist &bl = it->second;
                for (std::list<bufferptr>::const_iterator it2 = bl.buffers().begin();
                        it2 != bl.buffers().end();
                        ++ it2) {
                    data[off] = *it2;
                    off += it2->length();
                }
                continue;
            }
        }
        /*read from Journal*/
        uint64_t off = op.trans[p->first];
        uint32_t len = p->second;
        uint32_t bentry = off >> PAGE_SHIFT;

        if (bentry != BufferHead::ZERO && bentry != BufferHead::TRUNC) {
            uint64_t start = off & PAGE_MASK;
            uint64_t end = ROUND_UP_TO(off+len, PAGE_SIZE);

            bufferptr ptr(buffer::create_page_aligned(end - start));

            uint32_t r = safe_pread64(hot_fd, ptr.c_str(), ptr.length(), start);
            if(r != ptr.length()) {
                dout(5) << "safe_pread64: hot_fd = " << hot_fd
                    << ", length = " << ptr.length()
                    << ", start = " << start << dendl;
                assert(0);
            } 
            off -= start;
            data[p->first] = bufferptr(ptr, off, len);
        } 
        else if (bentry == BufferHead::ZERO) {
            bufferptr bp(len);
            bp.zero();
            data[p->first] = bp;
        }
        else {
            map<uint32_t, uint32_t>::iterator next = p;
            ++ next;
            assert(next == op.hits.end() && bentry == BufferHead::TRUNC);
        }
    }

    while(!op.parents.empty()) {
        put_read_lock(op.parents.front());
        op.parents.pop_front();
    }

    if (op.obj) {
        put_read_lock(op.obj);
        put_object(op.obj);
    }

    map<uint32_t, uint32_t>::iterator p = op.missing.begin();
    map<uint32_t, bufferlist> data_from_store;
    while(p != op.missing.end()) {
        bufferlist bl;
        size_t got = store->_read(op.cid, op.oid, p->first, p->second, bl);
        dout(10) << "got = " << got << dendl;
        if (got < (size_t)p->second) {
            if (op.obj && p->first + got < op.obj->size) {
                dout(10) << "off = " << p->first << ", got = " << got << ", size = " << op.obj->size << dendl;
                uint32_t len = op.obj->size - p->first;
                if (len > p->second)
                    len = p->second;
                len -= got;
                got += len;
                bufferptr bp(len);
                bl.append(bp);
            }
            if (got > 0)
                data_from_store[p->first].claim(bl);

            {
                ++ p;
                /*
                while (p != op.missing.end()) {
                    uint32_t len = op.obj->size - p->first;
                    if (len > p->second)
                        len = p->second;
                    if (!len)
                        break;
                    bufferptr bp(len);
                    bufferlist bl;
                    bl.append(bp);
                    data_from_store[p->first].claim(bl);
                    ++ p;
                } 
                */
                assert(p == op.missing.end());
            }
            break;
        }
        data_from_store[p->first].claim(bl);
        ++p;
    }

    map<uint32_t, bufferptr>::iterator it = data.begin();
    map<uint32_t, bufferlist>::iterator it2 = data_from_store.begin();
    while (it != data.end() && it2 != data_from_store.end()) {
        if (it->first < it2->first) {
            op.buf.append(it->second);
            ++ it;
        }
        else {
            op.buf.append(it2->second);
            ++ it2;
        }
    }
    while (it != data.end()) {
        op.buf.append(it->second);
        ++ it;
    }
    while (it2 != data_from_store.end()) {
        op.buf.append(it2->second);
        ++ it2;
    }
    return 0;
}

void NVMJournal::dump(const ReadOp &op)
{
    Formatter *f = new_formatter("json-pretty");
    f->open_object_section("store");
    f->dump_string("collection", stringify(op.cid));
    f->dump_string("object", stringify(op.oid));
    if (op.obj){
        f->open_array_section("content");
        for (map<uint32_t, BufferHead *>::const_iterator p = op.obj->data.begin();
                p != op.obj->data.end();
                ++p) {
            f->open_object_section("content");
            f->dump_int("start", (int)p->second->ext.start);
            f->dump_int("end", (int)p->second->ext.end);
            f->dump_int("pos", (int)p->second->bentry);
            f->close_section();
        }
        f->close_section();
    }
    {
        f->open_array_section("hits");
        for (map<uint32_t, uint32_t>::const_iterator p = op.hits.begin();
                p != op.hits.end();
                ++p ) {
            f->open_object_section("range");
            f->dump_int("start", (int)p->first);
            f->dump_int("length", (int)p->second);
            if (op.hitcache.count(p->first))
                f->dump_bool("hitcahce", true);
            f->close_section();
        }
        f->close_section();
    }
    {
        f->open_array_section("missing");
        for (map<uint32_t, uint32_t>::const_iterator p = op.missing.begin();
                p != op.missing.end();
                ++p ) {
            f->open_object_section("range");
            f->dump_int("start", (int)p->first);
            f->dump_int("length", (int)p->second);
            f->close_section();
        }
        f->close_section();
    }
    f->close_section();
    dout(0) << "DUMP READOP:\n";
    f->flush(*_dout);
    *_dout << dendl;
    delete f;
}
int NVMJournal::read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl)
{
    ReadOp op;
    build_read(cid, oid, off, len, op);
    int r = do_read(op);
    //dump(op);
    bl.swap(op.buf);
    return r;
}

uint64_t NVMJournal::get_object_size(coll_t cid, const ghobject_t &oid)
{
   uint64_t size = 0;
   ObjectRef obj = get_object(cid, oid);
   if (!obj)
       return size;
   {
       RWLock::RLocker lock(obj->lock);
       size = obj->size;
   }
   put_object(obj);
   return size;

}
/* backgroud evict thread */
void NVMJournal::evict_entry()
{
    static uint32_t synced = 0, cur = 0;
    uint64_t seq;
    utime_t interval;
    int max_evict_in_flight = 10;
    interval.set_from_double(1.0);
    
    assert(replay == false);
    utime_t dump_interval;
    dump_interval.set_from_double(60.0);
    utime_t latest = ceph_clock_now(g_ceph_context);
    while (true) 
    {
        utime_t now = ceph_clock_now(g_ceph_context);
        if (now > dump_interval + latest) {
	    dump_journal_stat();
            latest = now;
        }
        /* check the completion of evict work */
        check_ev_completion();
        {
            Mutex::Locker l(evict_lock);
            if (ev_stop) {
                return;
            }
            if (!(should_evict() || force_evict)
                    || evict_in_flight.size() > max_evict_in_flight
                    || ev_pause) {
                if (ev_pause && running_ev.empty() && !ev_paused) {
                    ev_paused = true;
                    evict_pause_cond.Signal();
                }
                evict_cond.WaitInterval(g_ceph_context, evict_lock, interval);
                continue; 
            }
        }
        ev_paused = false;
	
        deque<BufferHead*> to_evict, to_reclaim;
        to_evict.clear();
        to_reclaim.clear();
        {
            Mutex::Locker l(Journal_queue_lock);
            uint32_t limit = 128;
            while(limit && !Journal_queue.empty()) {
                to_evict.push_back(Journal_queue.front());
                Journal_queue.pop_front();
                limit --;
            }
        }

        deque<BufferHead*>::iterator p = to_evict.begin();
        map<ObjectRef, deque<BufferHead*> > obj2bh;

        while (p != to_evict.end())
        {
            BufferHead *pbh = *p++;
            ObjectRef obj = pbh->owner; 
            assert(obj);

            if (pbh->bentry != cur) {
                if(cur)
                    synced = cur; // the log entry which all the bh has been evicted
                cur = pbh->bentry;
            }

            if (pbh->ext.start == pbh->ext.end) {
                put_object(obj);
                delete pbh;
                continue;
            }

            to_reclaim.push_back(pbh);

            if (obj2bh.find(obj) == obj2bh.end())
                obj->get();
            obj2bh[obj].push_back(pbh);
        }

        seq = ev_seq ++;
        uint32_t ops = obj2bh.size();
        if (!ops) {
            BufferHead *bh = new BufferHead();
            bh->owner = NULL;
            bh->ext.start = bh->ext.end = 0;
            bh->bentry = synced;

            EvOp *ev = new EvOp(NULL, to_reclaim);
            ev->synced = synced;
            ev->seq = seq;
            ev->done = true;
            running_ev.push_back(ev);
            dout(5) << "bufferhead = " << bh << dendl;
            evict_in_flight[seq].push_back(bh);
            continue;
        }

        //apply_manager.op_apply_start(ops);
        map<ObjectRef, deque<BufferHead *> >::iterator it = obj2bh.begin();
        while (it != obj2bh.end()) 
        {
            ObjectRef obj = it->first;
            EvOp *ev = new EvOp(obj, it->second);
            assert(ev);
            if (ops == 1) {
                ev->synced = synced;
                ev->seq = seq;
            }
            queue_ev(ev);
            running_ev.push_back(ev);
            ++ it;
            -- ops;               
        }
        obj2bh.clear();

        dout(8) << "register evict_in_flight[" << seq << "]" << dendl;
        evict_in_flight[seq].swap(to_reclaim);
    }
}

void NVMJournal::evict_trigger()
{
    Mutex::Locker l(evict_lock);
    evict_cond.Signal();
}

void NVMJournal::stop_evictor()
{
    {
        Mutex::Locker l(evict_lock);
        ev_stop = true;
        evict_cond.Signal();
    }
    dout(10) << "stop evictor ..." << dendl;
    evictor.join();
}
void NVMJournal::pause_ev_work()
{
    Mutex::Locker l(evict_lock);
    ev_pause ++;
    while (!ev_paused)
        evict_pause_cond.Wait(evict_lock);
}
void NVMJournal::unpause_ev_work()
{
    Mutex::Locker l(evict_lock);
    ev_pause --;
    evict_cond.Signal();
}
void NVMJournal::check_ev_completion()
{
    uint64_t new_completed_ev = 0;
    uint32_t new_synced = 0;
    deque<EvOp*>::iterator it = running_ev.begin();
    while (it != running_ev.end() && (*it)->done) {
        EvOp *ev = *it;
        if (ev->seq) {
            new_completed_ev = ev->seq;
            new_synced = ev->synced;
        }
        ++it;
        delete ev;
    }
    if (it != running_ev.begin()) {
        running_ev.erase(running_ev.begin(), it);
    }

    if (new_completed_ev) {
	dout(8) << "new_completed_ev = " << new_completed_ev << dendl; 
	Mutex::Locker l (waiter_lock);
	map<uint64_t, deque<BufferHead*> >::iterator p = evict_in_flight.begin();
	while (p != evict_in_flight.end() &&
		p->first <= new_completed_ev) {
	    reclaim_queue.insert(reclaim_queue.end(), p->second.begin(), p->second.end());
	    evict_in_flight.erase(p++);
	}
	data_sync_pos = ((uint64_t)new_synced) << PAGE_SHIFT;
	if (enable_omap_cache)
	    omap_cache.evict_omap(new_synced);
	waiter_cond.Signal();  
	dout(8) << "data_sync_pos = " << data_sync_pos << dendl;
    }

    if (!ev_pause) {
        Mutex::Locker l(sync_lock);
        uint64_t sync_not_recorded = 0;
        if (data_sync_pos >= data_sync_pos_recorded)
            sync_not_recorded = data_sync_pos - data_sync_pos_recorded;
        else
            sync_not_recorded = (hot_journal_size - data_sync_pos_recorded) + data_sync_pos;

        if (sync_not_recorded >= sync_threshold)
            _sync();
    }
}

void NVMJournal::_flush_bh(ObjectRef obj, BufferHead *pbh)
{
    assert (obj->lock.is_locked());
    if (!pbh->dirty || obj->alias.empty())
        return;
    const coll_t& cid = obj->alias.begin()->first;
    const ghobject_t& oid = obj->alias.begin()->second;
    static const int flags = SPLICE_F_NONBLOCK;
    int *fds = (int *)tls_pipe.get_resource();
    assert(fds);

    uint64_t pos = pbh->bentry;
    loff_t off = (loff_t)(pbh->ext.start);
    uint32_t len = pbh->ext.end - off;
    if (!len)
        return;
    if (pos == BufferHead::TRUNC) {
        /* do nothing */
    }
    else if (pos == BufferHead::ZERO) {
        store->_zero(cid, oid, off, len);
    }
    else {
        bool flushed = false;
        if (enable_cache) {
	    int index = reinterpret_cast<int>(obj) % cache_slots;
            Mutex::Locker lock(buffer_cache[index].lock);
            bufferlist bl;
            bool found = buffer_cache[index].lookup(pbh, bl);
            if (found) {
                store->_write(cid, oid, off, len, bl, 0);
                flushed = true;
            }
        } 
        if (!flushed) {
            loff_t ssd_off = SSD_OFF(pbh);
            ssize_t r = safe_splice(hot_fd, &ssd_off, fds[1], NULL, len, flags);
            int ofd = store->_open(cid, oid);
            if (ofd < 0) {
                bufferptr bp(len);
                assert(safe_read(fds[0], bp.c_str(), len) == len);
                bufferlist bl;
                bl.push_back(bp);
                store->_write(cid, oid, off, len, bl, 0);
            }
            else {
                /* zero copy */
                assert(r == safe_splice(fds[0], NULL, ofd, &off, len, flags));
            }
        }
    }
    pbh->dirty = false;
}
void * NVMJournal::ThreadLocalPipe::init_resource()
{
    int *fds = new int[2];
    assert(fds);
    assert(::pipe(fds) == 0);
    ::fcntl(fds[1], F_SETPIPE_SZ, 4*1024*1024);
    ::fcntl(fds[0], F_SETFL, O_NONBLOCK);
    ::fcntl(fds[1], F_SETFL, O_NONBLOCK);
    return (void *)fds;
}
void NVMJournal::ThreadLocalPipe::release_resource(void *rc)
{
    if (rc) {
        int *fds = (int *)rc;
        close (fds[0]);
        close (fds[1]);
    }
}
void * NVMJournal::ThreadLocalPipe::get_resource()
{
    void *rc = pthread_getspecific(thread_pipe_key);
    if (!rc) {
        while (!rc)
            rc = init_resource();
        pthread_setspecific (thread_pipe_key, rc);
    }
    return rc;
}

void NVMJournal::do_ev(EvOp *ev, ThreadPool::TPHandle *handle) 
{
    ObjectRef obj = ev->obj;
    deque<BufferHead *>::iterator p = ev->queue.begin();

    RWLock::WLocker l(obj->lock);
    if (obj->alias.empty())
        goto done;
    
    while (p != ev->queue.end()) {
        BufferHead *bh = *p++;
        if (!bh->dirty) {
            continue;
        }
        // check the object 
        bool valid = false; 
        map<uint32_t, BufferHead *>::iterator t = obj->data.lower_bound(bh->ext.start);
        if (t != obj->data.begin())
            -- t;

        // nothing special to do with removed object,
        // because of the empty data map
        while (t != obj->data.end()) {
            uint32_t off = t->first;
            if (off > bh->ext.end) 
                break;

            BufferHead *bh2 = t->second;
            assert (off == bh2->ext.start);
            if (bh2->ext.start < bh->ext.end 
                    && bh2->ext.end > bh->ext.start) 
            {
                // if bh2 is the child of bh, or bh2 == bh,  then ...
                if (bh2->bentry == bh->bentry) {
                    _flush_bh (obj, bh2);
		    uint32_t len = bh2->ext.end - bh2->ext.start;
		    total_evict.add(len);
		    total_flush.add(len);
		    valid = true;
                }
            }// if 
            ++ t;
        }// while

        // mark the invalid bufferhead ...
        if (!valid)
            bh->ext.end = bh->ext.start;
        if (handle)
            handle->reset_tp_timeout();
    } // while

done:
    ev->done = true;
    {
        Mutex::Locker locker(evict_lock);
        evict_cond.Signal();
    }
    // apply_manager.op_apply_finish();
}

void NVMJournal::change_journal_mode()
{
    double cached_data = total_cached.read();
    const double high = 0.4, low = 0.1;
    uint64_t threshold = evict_threshold * hot_journal_size;
    uint64_t used;
    if (hot_write_pos >= data_sync_pos)
	used = hot_write_pos - data_sync_pos;
    else
	used = hot_journal_size - (data_sync_pos - hot_write_pos);

    cached_data =  cached_data / hot_journal_size;
    if (enable_hot_journal
	    && used > threshold
	    && cached_data > high)
    {
	enable_hot_journal = false;
    }
    else if (!enable_hot_journal && cached_data < low)
	enable_hot_journal = true;
}

bool NVMJournal::should_evict() 
{
    uint64_t used;
    uint64_t threshold = evict_threshold * hot_journal_size;
    static bool evicting = false;
    const uint64_t batch = 4 << 20; 

    if (hot_write_pos >= data_sync_pos)
        used = hot_write_pos - data_sync_pos;
    else
        used = hot_journal_size - (data_sync_pos - hot_write_pos);

    if (used > threshold) {
        evicting = true;
    }
    else if (evicting){
        if (threshold - used >= batch)
            evicting = false; 
    }

    return evicting ;
}

bool NVMJournal::should_reclaim_cold_journal()
{
    double threshold = 0.7, used = 0;
    if (cold_write_pos >= cold_start_pos)
	used = cold_write_pos - cold_start_pos;
    else
	used = cold_journal_size - (cold_start_pos - cold_write_pos);
    used = used / cold_journal_size;
    return used > threshold;
}

bool NVMJournal::should_reclaim_hot_journal()
{
    double threshold = 0.90, used = 0;
    if (hot_write_pos >= hot_start_pos)
        used = hot_write_pos - hot_start_pos;
    else
        used = hot_journal_size - (hot_start_pos - hot_write_pos);
    used = used / hot_journal_size;
    return used > threshold;
}

void NVMJournal::reclaim_cold_journal()
{
    Mutex::Locker l(sync_lock);
    _sync();
}
void NVMJournal::reclaim_hot_journal()
{
    deque<BufferHead *> to_reclaim;
    {
        Mutex::Locker locker(waiter_lock);
        while(reclaim_queue.empty()) {
            force_evict = true;
            evict_trigger();
            waiter_cond.Wait(waiter_lock);
        }
        force_evict = false;
        uint32_t batch = 128;
        if (reclaim_queue.size() < batch)
            batch = reclaim_queue.size();
        while (batch --) {
            to_reclaim.push_back(reclaim_queue.front());
            reclaim_queue.pop_front();
        }
    }
    static uint32_t pre = -1, cur = -1;

    while (!to_reclaim.empty()) 
    {
        BufferHead *bh = to_reclaim.front();
        ObjectRef obj = bh->owner;

        if (bh->ext.start != bh->ext.end) {
            RWLock::WLocker locker(obj->lock);
            delete_bh(obj, bh->ext.start, bh->ext.end, bh->bentry);
        }

        put_object(obj);	
        if (bh->bentry != BufferHead::ZERO
                && bh->bentry != BufferHead::TRUNC
                && bh->bentry != cur) {
            pre = cur;
            cur = bh->bentry;
        }
        to_reclaim.pop_front();
        delete bh; // free BufferHead 
    }

    if (pre != (uint32_t)-1) {
        uint64_t now = ((uint64_t)pre) << PAGE_SHIFT;
        if (now < hot_start_pos)
            hot_start_pos = 0;
        else if (now - hot_start_pos >= 64*1024*1024)
            hot_start_pos = now;
    }

    {
        bool bsync = false;
        Mutex::Locker l(sync_lock);
        if (hot_write_pos >= hot_start_pos) {
            if (hot_start_pos > data_sync_pos_recorded)
                bsync = true;
        }
        else if (hot_write_pos < hot_start_pos) {
            if (data_sync_pos_recorded > hot_write_pos 
                    && data_sync_pos_recorded < hot_start_pos)
                bsync = true;
        }
        if (bsync) 
            _sync();
    }
}

void NVMJournal::wait_for_more_space(uint64_t min)
{
    uint64_t free = 0;
    uint32_t attempts = 0;
    do {
        attempts ++;
        if (should_evict())
            evict_trigger();

	if (hot_write_pos >= hot_start_pos)
	    free = hot_journal_size - (hot_write_pos - hot_start_pos);
	else
	    free = hot_start_pos - hot_write_pos;

	if (free < min || should_reclaim_hot_journal()) {
	    reclaim_hot_journal();
	    continue;
	}

	if (cold_write_pos >= cold_start_pos)
	    free = cold_journal_size - (cold_write_pos - cold_start_pos);
	else
	    free = cold_start_pos - cold_journal_size;;
        if (free < min || should_reclaim_cold_journal()) {
            reclaim_cold_journal();
	    continue;
        }
    }while(false);
}

void NVMJournal::get_object_omap(coll_t cid, const ghobject_t &oid, bufferlist &head,
	map<string, bufferptr> &xattr, map<string, bufferlist> &omap)
{
    store->_getattrs(cid, oid, xattr);
    store->_omap_get(cid, oid, head, omap);
}

void NVMJournal::omap_cache_t::omap_init(object_omap_t *obj)
{
    map<string, bufferptr> xattr;
    map<string, bufferlist> omap;
    journal->get_object_omap(obj->cid, obj->oid, obj->head, xattr, omap);
    for (map<string, bufferptr>::iterator it = xattr.begin();
	    it != xattr.end();
	    ++ it) {
	omap_elem_t *e = new omap_elem_t();
	e->owner = obj;
	e->type = omap_elem_t::XATTR;
	e->status = omap_elem_t::STORE;
	e->key = it->first;
	e->value.push_back(it->second);
	e->lst = fifo_queue.end();
	obj->xattr[e->key] = e;
    }
    for (map<string, bufferlist>::iterator it = omap.begin();
	    it != omap.end();
	    ++ it) {
	omap_elem_t *e = new omap_elem_t();
	e->owner = obj;
	e->type = omap_elem_t::OMAP;
	e->status = omap_elem_t::STORE;
	e->key = it->first;
	e->value = it->second;
	e->lst = fifo_queue.end();
	obj->omap[e->key] = e;
    }
}
int NVMJournal::omap_cache_t::remove(coll_t c, const ghobject_t &oid)
{
    return 0;
}
int NVMJournal::omap_cache_t::clone(coll_t c, const ghobject_t &oo, const ghobject_t &no, uint32_t pos)
{
    assert(0 == "clone not supported");
}
int NVMJournal::omap_cache_t::setattrs(coll_t cid, const ghobject_t &oid, const map<string, bufferptr> &aset, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);
    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    for (map<string, bufferptr>::const_iterator it = aset.begin();
	    it != aset.end();
	    ++ it) {
	map<string, omap_elem_t*>::iterator t;
	t = object->xattr.find(it->first);
	uint8_t status = omap_elem_t::CACHE;
	if (t != object->xattr.end()) {
	    omap_elem_t *e = t->second;
	    status = e->status;
	    if (e->lst != fifo_queue.end())
		fifo_queue.erase(e->lst);
	    delete e;
	}

	const string &key = it->first;
	omap_elem_t *elem = new omap_elem_t();
	elem->owner = object;
	elem->type = omap_elem_t::XATTR;
	elem->status = status;
	elem->logpos = pos;
	elem->key = key;
	elem->value.push_back(it->second);
	fifo_queue.push_back(elem);
	list<omap_elem_t*>::iterator i = fifo_queue.end();
	-- i;
	elem->lst = i;
	object->xattr[key] = elem;
    }
    return 0;
}

int NVMJournal::omap_cache_t::rmattr(coll_t cid, const ghobject_t &oid, const char *name, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);
    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    map<string, omap_elem_t*>::iterator t;
    t = object->xattr.find(name);
    if (t != object->xattr.end())
    {
	omap_elem_t *e = t->second;
	if (e->lst != fifo_queue.end())
	    fifo_queue.erase(e->lst);
	if (e->status == omap_elem_t::STORE) {
	    e->value = bufferlist();
	    e->logpos = pos;
	    fifo_queue.push_back(e);
	    list<omap_elem_t*>::iterator i = fifo_queue.end();
	    -- i;
	    e->lst = i;
	}
	else {
	    object->xattr.erase(t);
	    delete e;
	}
    }
    return 0;
}

int NVMJournal::omap_cache_t::rmattrs(coll_t cid, const ghobject_t &oid, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);
    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    for (map<string, omap_elem_t*>::iterator it = object->xattr.begin();
	    it != object->xattr.end();
	    ++ it) {
	omap_elem_t *e = it->second;
	if (e->lst != fifo_queue.end())
	    fifo_queue.erase(e->lst);
	delete e;
    }
    omap_elem_t *e = new omap_elem_t();
    e->owner = object;
    e->type = omap_elem_t::RM_ALL_XATTR;
    e->logpos = pos;
    fifo_queue.push_back(e);

    object->xattr.clear();
    return 0;
}
int NVMJournal::omap_cache_t::getattr(coll_t cid, const ghobject_t &oid, const char *name, bufferptr &value)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    map<string, omap_elem_t*>::iterator it = object->xattr.find(name);
    if (it == object->xattr.end())
	return -ENODATA;
    const bufferlist &bl = it->second->value;
    if (!bl.length())
	return -ENODATA;
    value = bl.buffers().front();
    return 0;
}

int NVMJournal::omap_cache_t::getattrs(coll_t cid, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }
    for (map<string, omap_elem_t*>::iterator it = object->xattr.begin();
	    it != object->xattr.end();
	    ++ it) {
	const bufferlist &bl = it->second->value;
	const bufferptr &value = bl.buffers().front();
	if (value.length())
	    aset.insert(make_pair(it->first, value));
    }
    return 0;
}

int NVMJournal::omap_cache_t::omap_clear(coll_t cid, const ghobject_t &oid, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    for (map<string, omap_elem_t*>::iterator it = object->omap.begin();
	    it != object->xattr.end();
	    ++ it) {
	omap_elem_t *e = it->second;
	if (e->lst != fifo_queue.end())
	    fifo_queue.erase(e->lst);
	delete e;
    }
    omap_elem_t *e = new omap_elem_t();
    e->owner = object;
    e->type = omap_elem_t::RM_ALL_OMAP;
    e->logpos = pos;
    fifo_queue.push_back(e);

    object->omap.clear();
    return 0;
}
int NVMJournal::omap_cache_t::omap_setkeys(coll_t cid, const ghobject_t &oid, const map<string, bufferlist> &aset, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }
    
    for (map<string, bufferlist>::const_iterator it = aset.begin();
	    it != aset.end();
	    ++ it) {
	uint8_t status = omap_elem_t::CACHE;
	map<string, omap_elem_t*>::iterator t = object->omap.find(it->first);
	if (t != object->omap.end()) {
	    omap_elem_t *e = t->second;
	    status = e->status;
	    if (e->lst != fifo_queue.end())
		fifo_queue.erase(e->lst);
	    object->omap.erase(t);
	    delete e;
	}

	const string &key = it->first;
	const bufferlist &value = it->second;
	if (!value.length() && status==omap_elem_t::CACHE)
	    continue;
	omap_elem_t *elem = new omap_elem_t();
	elem->owner = object;
	elem->type = omap_elem_t::OMAP;
	elem->status = status;
	elem->logpos = pos;
	elem->key = key;
	elem->value = value;
	fifo_queue.push_back(elem);
	list<omap_elem_t*>::iterator i = fifo_queue.end();
	-- i;
	elem->lst = i;
	object->omap[key] = elem;
    }
    return 0;
}

int NVMJournal::omap_cache_t::omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys, uint32_t pos)
{
    map<string, bufferlist> aset;
    for (set<string>::iterator it = keys.begin();
	    it != keys.end();
	    ++ it) {
	aset.insert(make_pair(*it, bufferlist()));
    }
    return omap_setkeys(cid, oid, aset, pos);
}

int NVMJournal::omap_cache_t::omap_rmkeyrange(coll_t cid, const ghobject_t &oid, const string &first, const string &last, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    map<string, omap_elem_t*>::iterator it, end;
    it = object->omap.upper_bound(first);
    end = object->omap.lower_bound(last);
    while (it != end) {
	omap_elem_t *e = it->second;
	fifo_queue.erase(e->lst);
	if (e->status == omap_elem_t::CACHE) {
	    object->omap.erase(it++);
	    delete e;
	}
	else {
	    e->value = bufferlist();
	    e->logpos = pos;
	    fifo_queue.push_back(e);
	    list<omap_elem_t*>::iterator i = fifo_queue.end();
	    -- i;
	    e->lst = i;
	    ++ it;
	}
    }
    return 0;
}

int NVMJournal::omap_cache_t::omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl, uint32_t pos)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    object->head = bl;
    journal->store->_omap_setheader(cid, oid, bl);
    return 0;
}
int NVMJournal::omap_cache_t::omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    map<string, omap_elem_t*>::iterator it = object->omap.begin();
    while (it != object->omap.end()) {
	omap_elem_t *e = it->second;
	if (e->value.length())
	    out->insert(make_pair(e->key, e->value));
	++ it;
    }
    return 0;
}
int NVMJournal::omap_cache_t::omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }
    *header = object->head;
    return 0;
}
int NVMJournal::omap_cache_t::omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    map<string, omap_elem_t*>::iterator it = object->omap.begin();
    while (it != object->omap.end()) {
	omap_elem_t *e = it->second;
	if (e->value.length())
	    keys->insert(e->key);
    }
    return 0;
}
int NVMJournal::omap_cache_t::omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys, map<string, bufferlist> *out)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }

    for (set<string>::const_iterator it = keys.begin();
	    it != keys.end();
	    ++ it) {
	const string &key = *it;
	map<string, omap_elem_t*>::iterator t = object->omap.find(key);
	if (t != object->omap.end()) {
	    omap_elem_t *e = t->second;
	    if (e->value.length())
		out->insert(make_pair(key, e->value));
	}
    }
    return 0;
}
int NVMJournal::omap_cache_t::omap_check_keys(coll_t cid, const ghobject_t &oid, const set<string> &keys, set<string> *out)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }
    for (set<string>::const_iterator it = keys.begin();
	    it != keys.end();
	    ++ it) {
	const string &key = *it;
	map<string, omap_elem_t*>::iterator t = object->omap.find(key);
	if (t != object->omap.end()) {
	    omap_elem_t *e = t->second;
	    if (e->value.length())
		out->insert(key);
	}
    }
    return 0;
}

ObjectMap::ObjectMapIterator NVMJournal::omap_cache_t::get_omap_iterator(coll_t cid, const ghobject_t &oid)
{
    Mutex::Locker l(lock);
    collection_omap_t *coll = get_collection(cid);
    if (!coll)
	coll = touch_collection(cid);

    object_omap_t *object = coll->get_object(oid);
    if (!object) {
	object = coll->touch_object(oid);
	omap_init(object);
    }
    return ObjectMap::ObjectMapIterator(new omap_iterator_t(object));
}

void NVMJournal::omap_cache_t::evict_omap(uint32_t pos)
{
    map<object_omap_t*, map<string, bufferptr> > xattr_to_set;
    map<object_omap_t*, map<string, bufferlist> > omap_to_set;
    map<object_omap_t*, set<string> > xattr_to_remove, omap_to_remove;
    set<object_omap_t*> xattr_to_clear, omap_to_clear;
    int count = 0;
    {
	Mutex::Locker l(lock);
	if (fifo_queue.empty())
	    return;
	uint32_t end = fifo_queue.back()->logpos;
	while (!fifo_queue.empty()) {
	    omap_elem_t *e = fifo_queue.front();
	    if (!((e->logpos < pos && pos < end)
		    || !(end < pos && pos < e->logpos)))
		break;
	    fifo_queue.pop_front();
	    e->lst = fifo_queue.end();
	    count ++;
	    switch (e->type) {
		case omap_elem_t::XATTR:
		    {
			if (e->value.length()) {
			    e->status = omap_elem_t::STORE;
			    const bufferptr &value = e->value.buffers().front();
			    xattr_to_set[e->owner].insert(make_pair(e->key, value));
			}
			else {
			    xattr_to_remove[e->owner].insert(e->key);
			    e->owner->xattr.erase(e->key);
			    delete e;
			}
		    }
		    break;
		case omap_elem_t::OMAP:
		    {
			if (e->value.length()) {
			    e->status = omap_elem_t::STORE;
			    omap_to_set[e->owner].insert(make_pair(e->key, e->value));
			}
			else {
			    omap_to_remove[e->owner].insert(e->key);
			    e->owner->omap.erase(e->key);
			    delete e;
			}
		    }
		    break;
		case omap_elem_t::HEAD:
		    {
		    }
		    break;
		case omap_elem_t::RM_ALL_XATTR: 
		    {
			xattr_to_clear.insert(e->owner);
			delete e;
		    }
		    break;
		case omap_elem_t::RM_ALL_OMAP:
		    {
			omap_to_clear.insert(e->owner);
			delete e;
		    }
		    break;
		default:
		    dout(0) << "unexpected omap_elem_t::type = " << e->type << dendl;
	    }
	}
    }
    for (set<object_omap_t*>::iterator it = xattr_to_clear.begin();
	    it != xattr_to_clear.end();
	    ++ it) {
	object_omap_t *obj = *it;
	journal->store->_rmattrs(obj->cid, obj->oid);
    }
    for (set<object_omap_t*>::iterator it = omap_to_clear.end();
	    it != omap_to_clear.end();
	    ++ it) {
	object_omap_t *obj = *it;
	journal->store->_omap_clear(obj->cid, obj->oid);
    }

    for (map<object_omap_t*, map<string, bufferptr> >::iterator it = xattr_to_set.begin();
	    it != xattr_to_set.end();
	    ++ it) {
	object_omap_t *obj = it->first;
	journal->store->_setattrs(obj->cid, obj->oid, it->second);
    }
    for (map<object_omap_t*, map<string, bufferlist> >::iterator it = omap_to_set.begin();
	    it != omap_to_set.end();
	    ++ it) {
	object_omap_t *obj = it->first;
	journal->store->_omap_setkeys(obj->cid, obj->oid, it->second);
    }

    for (map<object_omap_t*, set<string> >::iterator it = xattr_to_remove.begin();
	    it != xattr_to_remove.end();
	    ++ it) {
	object_omap_t *obj = it->first;
	for (set<string>::iterator t = it->second.begin();
		t != it->second.end();
		++ t) {
	    const string &key = *t;
	    journal->store->_rmattr(obj->cid, obj->oid, key.c_str());
	}
    }
    for (map<object_omap_t*, set<string> >::iterator it = omap_to_remove.begin();
	    it != omap_to_remove.end();
	    ++ it) {
	object_omap_t *obj = it->first;
	journal->store->_omap_rmkeys(obj->cid, obj->oid, it->second);
    }
}
/*LRUCache*/
NVMJournal::LRUCache::LRUCache(int climit, int mlimit) :
    cthrottle(climit), count(0), mthrottle(mlimit), mem(0),
    lock("NVMJournal::cache_lock", false, true, false, g_ceph_context)
{ }
void NVMJournal::LRUCache::adjust()
{
    while ((count && count>cthrottle)
            || (mem && mem>mthrottle)) {
        BufferHead *bh = lru.back().first;
        bufferlist bl = lru.back().second;
        mem -= bl.length();
        count -= 1;
        hash.erase(bh);
        lru.pop_back();
    }
}
void NVMJournal::LRUCache::setthrottle(ssize_t climit, ssize_t mlimit)
{
    cthrottle = climit;
    mthrottle = mlimit;
    adjust();
}
bool NVMJournal::LRUCache::lookup(BufferHead* key, bufferlist &value, bool move_to_head)
{
    hash_t::iterator it = hash.find(key);
    if (it == hash.end())
        return false;
    if (move_to_head)
	lru.splice(lru.begin(), lru, it->second);
    value = it->second->second;
    return true;
}
void NVMJournal::LRUCache::add(BufferHead* key, bufferlist &value)
{
    hash_t::iterator it = hash.find(key);
    if (it != hash.end()) {
        lru.splice(lru.begin(), lru, it->second);
    }
    else {
        lru.push_front(make_pair(key, value));
        hash[key] = lru.begin();
        ++ count;
        mem += value.length();
        adjust();
    }
}
void NVMJournal::LRUCache::purge(BufferHead* key)
{
    hash_t::iterator it = hash.find(key);
    if (it != hash.end()) {
        bufferlist &bl = it->second->second;
        mem -= bl.length();
        count -= 1;
        lru.erase(it->second);
        hash.erase(it);
    }
}
void NVMJournal::LRUCache::dump(ostream &os)
{
    os << "cthrottle = " << cthrottle
	<< ", count = " << count
	<< ", mthrottle = " << mthrottle
	<< ", menory = " << mem << std::endl;
}

void NVMJournal::get_read_lock(ObjectRef obj)
{
    assert(obj);
    obj->lock.get_read();
}
void NVMJournal::put_read_lock(ObjectRef obj)
{
    assert(obj);
    obj->lock.put_read();
}
void NVMJournal::get_write_lock(ObjectRef obj)
{
    assert(obj);
    obj->lock.get_write();
}
void NVMJournal::put_write_lock(ObjectRef obj)
{
    assert(obj);
    obj->lock.put_write();
}
