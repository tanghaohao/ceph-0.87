#ifndef CEPH_NVMJOURNAL_H
#define CEPH_NVMJOURNAL_H

#include "include/assert.h"
#include "include/unordered_map.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/shared_cache.hpp"
#include "ranktree.h"
#include "RWJournal.h"

#include <deque>
#include <map>
#include <list>
#include <libaio.h>

using std::deque;
using std::map;
using std::list;

class NVMJournal : public RWJournal {
    private:
	struct header_t {
	    uint64_t meta_sync_pos;
	    uint64_t data_sync_pos;
	    uint64_t cold_sync_pos;
	    uint32_t checksum;
	};

	header_t header;
	friend void encode(const header_t&, bufferlist&, uint64_t);
	friend void decode(header_t&, bufferlist::iterator&);
	void build_header(bufferlist& bl);

	uint64_t cur_seq;

	/*hot journal points*/
	uint64_t hot_write_pos;
	uint64_t meta_sync_pos;
	uint64_t data_sync_pos;
	uint64_t data_sync_pos_recorded;
	uint64_t hot_start_pos;
	uint64_t hot_journal_size;

	/*cold journal points*/
	uint64_t cold_write_pos;
	uint64_t cold_sync_pos;
	uint64_t cold_start_pos;
	uint64_t cold_journal_size;

	/*checkpoint tracker*/
	static const uint64_t sync_threshold = 2 << 30; // 2GB
	deque<uint64_t> checkpoints;
	uint64_t next_checkpoint_seq;

	/*statistics*/
	atomic64_t total_hot_wrap;
	atomic64_t total_cold_wrap;
	atomic64_t total_write;
	atomic64_t total_flush;
	atomic64_t total_evict;
	atomic64_t total_cached;
	atomic64_t total_overlap;
	bool enable_hot_journal;
	void change_journal_mode();
	void dump_journal_stat();

	string conf;
	string hot_journal_path, cold_journal_path;
	int hot_fd, cold_fd;	
	int _open(string &path, uint64_t &size, bool io_direct = true);

	/**
	 * when replay journal after reboot, set replay = true
	 * when replay journal between [data_sync_pos, meta_sync_pos] set not_update_meta = true
	 */
	bool replay; 
	bool not_update_meta;
	class Op;
	class JournalReplayer: public Thread {
	    public:
		JournalReplayer(NVMJournal *j, deque<Op*> *q);
		void *entry();	
		void queue_op(Op *op);
		void stop_and_wait(); 
	    private:
		NVMJournal *Journal;
		deque<Op*> *queue;
		bool stop;
		Mutex lock;
		Cond cond, cond2;
	};
	void check_replay_point(uint64_t pos);
	int _journal_replay();

	class ApplyManager {
	    bool blocked;
	    uint32_t open_ops;
	    Mutex lock;
	    Cond cond;
	    public:
	    ApplyManager()
		: blocked(false),
		open_ops(0), 
		lock("NVMJournal::ApplyManager::lock", false, true, false, g_ceph_context)
	    { }
	    void sync_start();
	    void sync_finish();
	    void op_apply_start(uint32_t ops = 1);
	    void op_apply_finish(uint32_t ops = 1);
	};
	ApplyManager apply_manager;

	Mutex sync_lock;
	void _sync();

    public:
	int replay_journal();
	int start();
	void stop();
	int mkjournal();

    private:
	// Writer : Append entry to the tail of Journal
	struct OpSequencer ;
	struct write_item {
	    uint64_t seq;
	    OpSequencer *posr;
	    list<Transaction*> tls;
	    write_item(uint64_t s, OpSequencer *o, list<Transaction *> &l)
		: seq(s), posr(o), tls(l) { }
	};

	deque<write_item> writeq;
	Mutex writeq_lock;
	Cond writeq_cond;

	Mutex op_throttle_lock;
	Cond op_throttle_cond;
	uint64_t op_queue_len;
	static const uint64_t op_queue_max = 256; 
	void op_queue_reserve_throttle(ThreadPool::TPHandle *handle);
	void op_queue_release_throttle();
    public:
	void submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle);

    private:
	static const uint32_t magic = 0x12345678;
	struct entry_header_t {
	    uint32_t magic;
	    uint32_t wrap;
	    uint64_t seq;	// sequence of the first transaction
	    uint32_t ops;	// number of the transactions in the journal entry
	    uint32_t pre_pad;
	    uint32_t post_pad;
	    uint32_t coldset_len;
	    uint32_t meta_len;
	    uint32_t data_len;
	    uint32_t checksum;
	};

	struct aio_info {
	    struct iocb iocb;
	    uint64_t seq;
	    bufferlist bl; // KEEP REFERENCE TO THE IOV BUFFER
	    uint32_t len;
	    struct iovec *iov;
	    bool done;
	    uint64_t next_hot_write_pos;
	    uint64_t next_cold_write_pos;

	    aio_info(bufferlist &other, uint64_t s) :
		seq(s), len(other.length()), iov(NULL), done(false),
		next_hot_write_pos(0), next_cold_write_pos(0)
	    { 
		bl.claim(other);
		memset((void*)&iocb, 0, sizeof(iocb));
	    }

	    ~aio_info() {
		delete[] iov;
	    }
	};

	Mutex aioq_lock;
	Cond aioq_cond;	
	uint32_t aio_num;
	deque<aio_info> aio_queue; 

	io_context_t aio_ctx;
	bool aio_ctx_ready;
	void do_aio_write(bufferlist &hot_bl, bufferlist &cold_bl, uint64_t seq);

	struct Op {
	    utime_t journal_latency;
	    utime_t osr_queue_latency;
	    utime_t do_op_latency;

	    uint64_t seq;
	    uint64_t entry_pos;
	    uint32_t data_offset;
	    bool replay;
	    OpSequencer *posr;
	    list<Transaction*> tls;
	    set<ghobject_t> coldset;
	    bufferlist colddata;
	};

	char *zero_buf;
	bool cold_journal_wrap;
	bool hot_journal_wrap;

	uint64_t prepare_single_write(bufferlist &meta, bufferlist &hot_data, bufferlist &cold_data, Op *op);
	int prepare_multi_write(bufferlist &hot_bl, bufferlist &cold_bl, list<Op*> &ops);
	void do_wrap();
	int read_entry(uint64_t &hot_pos, uint64_t &cold_pos, uint64_t &next_seq, Op *op);

	/* Journal writer */
	void writer_entry();
	class Writer : public Thread {
	    NVMJournal *Journal;
	    public:
	    Writer(NVMJournal *j) : Journal(j) { }
	    void *entry() {
		Journal->writer_entry();
		return 0;
	    }
	} writer;

	bool writer_stop;
	void stop_writer();

	/* reaper */
	bool reaper_stop;
	deque<Op*> op_queue;
	Mutex op_queue_lock;

	void check_aio_completion();
	void notify_on_committed(Op *op);
	void notify_on_applied(Op *op);
	void reaper_entry();
	void stop_reaper();

	class Reaper : public Thread {
	    NVMJournal *Journal;
	    public:
	    Reaper(NVMJournal *J) : Journal(J) { }
	    void *entry() {
		Journal->reaper_entry();
		return 0;
	    }
	} reaper;

	/* latency statics */
	Mutex latency_lock;
	utime_t journal_aio_latency;
	utime_t osr_queue_latency;
	utime_t do_op_latency;
	int ops;
	void update_latency(utime_t l1, utime_t l2, utime_t l3) {
	    Mutex::Locker l(latency_lock);
	    journal_aio_latency += l1;
	    osr_queue_latency += l2;
	    do_op_latency += l3;
	    ++ops;
	}

	/* thread pool: do op */
	/* op sequencer */
	class OpSequencer : public Sequencer_impl {
	    Mutex lock;
	    Cond cond;
	    list<Op *> tq;
	    list<uint64_t> op_q;
	    list< pair<uint64_t, Context *> > flush_waiters;
	    public:
	    OpSequencer() : 
		Sequencer_impl(),
		lock("NVMJournal::OpSequencer::lock", false, true, false, g_ceph_context),
		apply_lock("NVMJournal::OpSequencer::apply_lock", false, true, false, g_ceph_context) {}
	    ~OpSequencer() {
		flush();
	    }
	    void register_op(uint64_t seq);
	    void unregister_op();
	    void wakeup_flush_waiters(list<Context *> &to_queue);
	    Op* peek_queue();
	    void queue(Op *op);
	    void dequeue();
	    void flush();
	    bool flush_commit(Context *c);
	    public:
	    Mutex apply_lock;
	};

	OpSequencer default_osr;
	deque<OpSequencer*> os_queue;

	ThreadPool op_tp;
	bool op_tp_started;
	struct OpWQ: public ThreadPool::WorkQueue<OpSequencer> {
	    NVMJournal *Journal;
	    public:
	    OpWQ(time_t timeout, time_t suicide_timeout, ThreadPool *tp, NVMJournal *J) 
		: ThreadPool::WorkQueue<OpSequencer>("NVMJournal::OpWQ", timeout, suicide_timeout, tp), Journal(J) { }
	    bool _enqueue(OpSequencer *posr);
	    void _dequeue(OpSequencer *posr);
	    bool _empty();
	    OpSequencer *_dequeue();
	    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle);
	    void _process_finish(OpSequencer *osr);
	    void _clear();
	}op_wq;

	void queue_op(OpSequencer *osr, Op *op);
	void _do_op(OpSequencer *posr, ThreadPool::TPHandle *handle);

	void do_op(Op *op, ThreadPool::TPHandle *handle = NULL);
	void do_transaction(Transaction *t, Op *op, uint64_t seq, uint64_t entry_pos, uint32_t &off);
	int _touch(coll_t cid, const ghobject_t &oid);
	int _write(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len, 
		bufferlist& bl, uint64_t entry_pos, uint32_t boff, bool is_cold);
	int _zero(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len);
	int _truncate(coll_t cid, const ghobject_t &oid, uint32_t off);
	int _remove(coll_t cid, const ghobject_t &oid);
	int _clone(coll_t cid, const ghobject_t &src, const ghobject_t &dst);
	int _clone_range(coll_t cid, ghobject_t &src, ghobject_t &dst,
		uint64_t off, uint64_t len, uint64_t dst_off);
	int _create_collection(coll_t cid);
	int _destroy_collection(coll_t cid);
	int _collection_add(coll_t dst, coll_t src, const ghobject_t &oid);
	int _collection_move_rename(coll_t oldcid, const ghobject_t &oldoid, coll_t newcid, const ghobject_t &newoid);
	int _collection_rename(coll_t cid, coll_t ncid);
	int _split_collection(coll_t src, uint32_t bits, uint32_t match, coll_t dst);
	int do_other_op(int op, Transaction::iterator& p, uint64_t logpos);

	/* memory data structure */
	
	class heat_info_t {
	    public:
		heat_info_t(ghobject_t o): obj(o), bitmap(0), heat(0.0) {};
		double touch(uint32_t s, uint32_t e);
		double get_heat();
	    public:
		ghobject_t obj;
		static utime_t now, cycle;
		utime_t last_update_bitmap;
		utime_t last_update_heat;
		uint64_t bitmap;
		double heat;
	};
	class heat_info_wrapper {
	    public:
		heat_info_wrapper(heat_info_t *p = NULL):point(p) { }
		bool operator==(const heat_info_wrapper &other);
		bool operator<(const heat_info_wrapper &other);
		bool operator>(const heat_info_wrapper &other);
		heat_info_t *point;
	};

	RankTree<heat_info_wrapper> heat_rank;
	void dump_heat_rank(ostream &os);

	typedef unordered_map<ghobject_t, heat_info_t*> heat_hash_t;
	heat_hash_t object_heat_map;
	Mutex heat_map_lock;

	double hot_heat_throttle;
	int hot_rank_throttle;
	unordered_set<ghobject_t> hot_object_set;
	set<ghobject_t> new_hot_object;
	set<ghobject_t> new_cold_object;
	Mutex hot_lock;

	void init_heat_map();
	void adjust_heat_map();
	int update_heat_map(ghobject_t oid, uint32_t off, uint32_t len);
	double get_object_heat(const ghobject_t& oid);
	void update_hot_set();
	bool check_is_hot(ghobject_t oid);

	/*memory indics*/
	struct Object;
	typedef Object* ObjectRef;

	struct BufferHead {
	    ObjectRef owner;
	    struct extent{
		uint32_t start;
		uint32_t end;
	    } ext;
	    enum {ZERO = ~(uint32_t)3, TRUNC};
	    uint32_t bentry;
	    uint32_t boff;
	    double heat;
	    bool dirty;
	};

	void _flush_bh(ObjectRef obj, BufferHead *pbh);
	class ThreadLocalPipe {
	    pthread_key_t thread_pipe_key;
	    void *init_resource(); 
	    static void release_resource(void *rc);
	    public:
	    ThreadLocalPipe() {
		pthread_key_create(&thread_pipe_key, release_resource);
	    };
	    void *get_resource();
	} tls_pipe;

	deque<BufferHead*> Journal_queue;
	Mutex Journal_queue_lock;
	Cond Journal_queue_cond;

	/* writer will wait on this lock if not enougth space remained in journal */	
	struct EvOp {
	    uint64_t seq;
	    uint32_t synced;
	    ObjectRef obj;
	    bool done;
	    deque<BufferHead *> queue;

	    EvOp(ObjectRef o, deque<BufferHead *> &q)
		: seq(0), synced(0), obj(0), done(false) {
		    obj = o;
		    queue.swap(q);
		}
	};

	uint64_t ev_seq;
	map< uint64_t, deque<BufferHead *> > evict_in_flight;
	deque<EvOp *> running_ev;
	deque<EvOp *> ev_queue;

	ThreadPool ev_tp;
	bool ev_tp_started;
	class EvWQ: public ThreadPool::WorkQueue<EvOp> {
	    NVMJournal *Journal;
	    public:
	    EvWQ(time_t timeout, time_t suicide_timeout, ThreadPool *tp, NVMJournal *J) 
		: ThreadPool::WorkQueue<EvOp>("NVMJournal::EvWQ", timeout, suicide_timeout, tp), Journal(J) { }
	    bool _enqueue(EvOp *op) {
		Journal->ev_queue.push_back(op);
		return true;
	    }
	    void _dequeue(EvOp *op) {
		assert(0);
	    }
	    bool _empty() {
		return Journal->ev_queue.empty();
	    }
	    EvOp *_dequeue() {
		if (Journal->ev_queue.empty())
		    return NULL;
		EvOp *op = Journal->ev_queue.front();
		Journal->ev_queue.pop_front();
		return op;
	    }
	    void _process(EvOp *op, ThreadPool::TPHandle &handle) {
		Journal->do_ev(op, &handle);
	    }
	    void _process_finish(EvOp *op) {
	    }
	    void _clear() {
		// assert (Journal->op_queue.empty());
	    }
	}ev_wq;
	void queue_ev(EvOp *op) {
	    ev_wq.queue(op);
	}
	void do_ev(EvOp *ev, ThreadPool::TPHandle *handle);

	Mutex evict_lock;
	Cond evict_cond;
	Mutex waiter_lock;
	Cond waiter_cond;

	bool should_evict();
	void wait_for_more_space(uint64_t min);
	void evict_entry();
	void check_ev_completion();

	class JournalEvictor : public Thread {
	    NVMJournal *Journal;
	    public:
	    JournalEvictor(NVMJournal *j) : Journal(j) {	}
	    void *entry() {
		Journal->evict_entry();
		return 0;
	    }
	} evictor;

	bool ev_stop;
	bool force_evict;
	uint32_t ev_pause;
	bool ev_paused;
	Cond evict_pause_cond;

	void evict_trigger();
	void stop_evictor();
	void pause_ev_work();
	void unpause_ev_work();


	deque<BufferHead*> reclaim_queue;
	Mutex reclaim_queue_lock;
	bool should_reclaim_cold_journal();
	bool should_reclaim_hot_journal();
	void reclaim_cold_journal();
	void reclaim_hot_journal();

	struct Object {
	    set< pair<coll_t, ghobject_t> > alias;
	    ObjectRef parent;
	    atomic_t ref;

	    uint32_t size;
	    map<uint32_t, BufferHead*> data;
	    RWLock lock;

	    Object(const coll_t &c, const ghobject_t &o) :
		parent(NULL),
		ref(0),
		size(0),
		lock("NVMJournal::object.lock") {
		    alias.insert( make_pair(c, o) );
		}

	    uint32_t get() { return ref.inc(); }
	    uint32_t put() { return ref.dec(); }
	};

	struct Collection {
	    coll_t cid;
	    ceph::unordered_map<ghobject_t, ObjectRef> Object_hash;
	    map<ghobject_t, ObjectRef> Object_map;
	    OpSequencer *osr;
	    Mutex lock;
	    
	    Collection(coll_t c) 
		: cid(c), lock("NVMJournal::Collection::lock",false, true, false, g_ceph_context) { }
	};

	typedef ceph::shared_ptr<Collection> CollectionRef;

	struct CollectionMap {
	    ceph::unordered_map<coll_t, CollectionRef> collections;
	    Mutex lock;
	    CollectionMap() :
		lock("NVMJournal::CacheShard::lock",false, true, false, g_ceph_context) {}
	} coll_map;

	CollectionRef get_collection(coll_t cid, bool create = false) ;

	void get_object_omap(coll_t cid, const ghobject_t &oid, bufferlist& bl, map<string, bufferptr> &xattr, map<string, bufferlist> &omap);
	class omap_cache_t
	{
	    struct omap_elem_t ;
	    struct object_omap_t {
		coll_t cid;
		ghobject_t oid;
		
		bufferlist head;
		map<string, omap_elem_t*> xattr;
		map<string, omap_elem_t*> omap;

		object_omap_t(coll_t c, ghobject_t o) :
		    cid(c), oid(o) { }
	    };
	    void omap_init(object_omap_t *);

	    struct collection_omap_t {
		coll_t cid;
		ceph::unordered_map<ghobject_t, object_omap_t*> omap_hash;

		collection_omap_t(coll_t c) : cid(c) { }
		object_omap_t* get_object(const ghobject_t &oid) {
		    ceph::unordered_map<ghobject_t, object_omap_t*>::iterator it = omap_hash.find(oid);
		    if (it == omap_hash.end())
			return NULL;
		    return it->second;
		}
		object_omap_t* touch_object(const ghobject_t &oid) {
		    object_omap_t* o = new object_omap_t(cid, oid);
		    omap_hash[oid] = o;
		    return o;
		}
	    };
	    ceph::unordered_map<coll_t, collection_omap_t*> coll_hash;
	    collection_omap_t* get_collection(coll_t cid) {
		ceph::unordered_map<coll_t, collection_omap_t*>::iterator it = coll_hash.find(cid);
		if (it == coll_hash.end())
		    return NULL;
		return it->second;
	    }
	    collection_omap_t* touch_collection(coll_t cid) {
		collection_omap_t* coll = new collection_omap_t(cid);
		coll_hash[cid] = coll;
		return coll;
	    }

	    struct omap_elem_t {
		object_omap_t *owner;
		enum { XATTR = 1 ,OMAP, HEAD, RM_ALL_XATTR, RM_ALL_OMAP };
		uint8_t type;
		enum { STORE = 1, CACHE};
		uint8_t status;
		uint32_t logpos;

		string key;
		bufferlist value;
		list<omap_elem_t*>::iterator lst;
	    };
	    list<omap_elem_t*> fifo_queue;

	    NVMJournal *journal;
	public:
	    Mutex lock;
	public:
	    omap_cache_t(NVMJournal *j) :
		journal(j), lock("NVMJournal::omap_cache_lock", false, true, false, g_ceph_context)
	    { }

	    class omap_iterator_t: public ObjectMap::ObjectMapIteratorImpl {
		object_omap_t *object;
		map<string, omap_elem_t*>::iterator it;
	    public:
		omap_iterator_t(object_omap_t *obj) :
		    object(obj), it(obj->omap.begin()) 
		{ }
		void adjust() {
		    while (it != object->omap.end()
			    && it->second->value.length() == 0)
			++ it;
		}
		int seek_to_first() {
		    it = object->omap.begin();
		    adjust();
		    return 0;
		}
		int upper_bound(const string &after) {
		    it = object->omap.upper_bound(after);
		    adjust();
		    return 0;
		}
		int lower_bound(const string &to) {
		    it = object->omap.lower_bound(to);
		    adjust();
		    return 0;
		}
		bool valid() {
		    return it != object->omap.end();
		}
		int next() {
		    ++ it;
		    adjust();
		    return 0;
		}
		string key() {
		    return it->first;
		}
		bufferlist value() {
		    return it->second->value;
		}
		int status() {
		    return 0;
		}
	    };
	    int remove(coll_t c, const ghobject_t &oid);
	    int clone(coll_t c, const ghobject_t &oo, const ghobject_t &no, uint32_t pos);

	    int setattrs(coll_t c, const ghobject_t &oid, const map<string, bufferptr> &aset, uint32_t pos);
	    int rmattr(coll_t c, const ghobject_t &oid, const char *name, uint32_t pos);
	    int rmattrs(coll_t c, const ghobject_t &oid, uint32_t pos);
	    int getattr(coll_t c, const ghobject_t &oid, const char *name, bufferptr &value);
	    int getattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset);

	    int omap_clear(coll_t c, const ghobject_t &o, uint32_t pos);
	    int omap_setkeys(coll_t c, const ghobject_t &o, const map<string, bufferlist> &aset, uint32_t pos);
	    int omap_rmkeys(coll_t c, const ghobject_t &o, const set<string> &keys, uint32_t pos);
	    int omap_rmkeyrange(coll_t c, const ghobject_t &o, const string &first, const string &last, uint32_t pos);
	    int omap_setheader(coll_t c, const ghobject_t &o, const bufferlist &bl, uint32_t pos);
	    int omap_get(coll_t c, const ghobject_t &o, bufferlist *header, map<string, bufferlist> *out);
	    int omap_get_header(coll_t c, const ghobject_t &o, bufferlist *header);
	    int omap_get_keys(coll_t c, const ghobject_t &o, set<string> *keys);
	    int omap_get_values(coll_t c, const ghobject_t &o, const set<string> &keys, map<string, bufferlist> *out);
	    int omap_check_keys(coll_t c, const ghobject_t &o, const set<string> &keys, set<string> *out);
	    ObjectMap::ObjectMapIterator get_omap_iterator(coll_t cid, const ghobject_t &c);
	    void evict_omap(uint32_t pos);
	}omap_cache;
	bool enable_omap_cache;
	bool support_omap() {
	    return enable_omap_cache;
	}
	int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value) {
	    return omap_cache.getattr(cid, oid, name, value);
	}
	int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset) {
	    return omap_cache.getattrs(cid, oid, aset);
	}
	int omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out) {
	    return omap_cache.omap_get(cid, oid, header, out);
	}
	int omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header, bool allow_eio = false) {
	    return omap_cache.omap_get_header(cid, oid, header);
	}
	int omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys) {
	    return omap_cache.omap_get_keys(cid, oid, keys);
	}
	int omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys,  map<string, bufferlist> *out) {
	    return omap_cache.omap_get_values(cid, oid, keys, out);
	}
	int omap_check_keys(coll_t c, const ghobject_t &oid, const set<string> &keys, set<string> *out) {
	    return omap_cache.omap_check_keys(c, oid, keys, out);
	}
	ObjectMap::ObjectMapIterator get_omap_iterator(coll_t cid, const ghobject_t &oid) {
	    return omap_cache.get_omap_iterator(cid, oid);
	}

	class LRUCache {
	    private:
		ssize_t cthrottle, count;
		ssize_t mthrottle, mem;
		list< pair<BufferHead*,bufferlist> > lru;
		typedef unordered_map<BufferHead*, 
			typename list< pair<BufferHead*,bufferlist> >::iterator> hash_t;
		hash_t hash;
		void adjust();
	    public:
		Mutex lock;
		LRUCache(int climit=1024, int mlimit=1024*1024*512);
		void setthrottle(ssize_t climit, ssize_t mlimit);
		bool lookup(BufferHead* key, bufferlist &value, bool move_to_head = false);
		void add(BufferHead* key, bufferlist &value);
		void purge(BufferHead* key);
		void dump(ostream &os);
	};
	ssize_t cthrottle;
	ssize_t mthrottle;
	int cache_slots;
	LRUCache *buffer_cache;
	bool enable_cache;

	ObjectRef get_object(coll_t cid, const ghobject_t &oid, bool create = false) ;
	ObjectRef get_object(CollectionRef coll, const ghobject_t &oid, bool create = false);
	inline void erase_object_with_lock_hold(CollectionRef coll, const ghobject_t &obj) ;
	void put_object(ObjectRef obj, bool locked = false) ;

	// we should never try to obtain a lock of an object
	// when we have got a lock of a collection
	void get_read_lock(ObjectRef obj);
	void put_read_lock(ObjectRef obj);
	void get_write_lock(ObjectRef obj);
	void put_write_lock(ObjectRef obj);

	atomic_t merge_ops;
	atomic_t merge_size;
	atomic_t merge_and_overlap_ops;
	atomic_t merge_and_overlap_size;
	uint32_t merge_new_bh(ObjectRef obj, BufferHead *bh);
	void delete_bh(ObjectRef obj, uint32_t off, uint32_t end, uint32_t bentry);

	/* Read operation */
	struct ReadOp 
	{
	    coll_t cid;
	    ghobject_t oid;
	    ObjectRef obj;
	    uint32_t off;
	    uint32_t length;
	    bufferlist buf; 
	    list<ObjectRef> parents;

	    map<uint32_t, uint32_t> hits; // off->len 
	    map<uint32_t, bufferlist> hitcache; // off->bufferlist
	    map<uint32_t, uint64_t> trans; // off->ssd_off
	    map<uint32_t, uint32_t> missing; // 
	};
	void dump(const ReadOp &op);
	void map_read(ObjectRef obj, uint32_t off, uint32_t end,
		map<uint32_t, uint32_t> &hits,
		map<uint32_t, bufferlist> &hitcache,
		map<uint32_t, uint64_t> &trans,
		map<uint32_t, uint32_t> &missing,
		bool trunc_as_zero = false);
	void build_read(coll_t &cid, const ghobject_t &oid, uint64_t off, size_t len, ReadOp &op);
	void build_read_from_parent(ObjectRef parent, ObjectRef obj, ReadOp& op);
	int do_read(ReadOp &op);

    public:
	int read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl);
	uint64_t get_object_size(coll_t cid, const ghobject_t &oid);

	NVMJournal(string hot_journal, string cold_journal, string conf, BackStore *s, Finisher *fin);

	virtual ~NVMJournal();

};
WRITE_RAW_ENCODER(NVMJournal::header_t);
#endif
