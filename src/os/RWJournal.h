#ifndef CEPH_RWJOURNAL_H
#define CEPH_RWJOURNAL_H

#include "include/unordered_map.h"
#include "common/Finisher.h"
#include "common/shared_cache.hpp"
#include "ObjectStore.h"

using std::deque;
using std::map;
using std::list;

class BackStore {
    public:
        BackStore() { };
        virtual ~BackStore() { };
    public:
        virtual int list_checkpoints(deque<uint64_t>& cps) = 0;
        virtual int rollback_to(uint64_t cp) = 0;
        virtual int create_checkpoint(uint64_t cp, uint64_t *id) = 0;
        virtual int flush_checkpoint(uint64_t id) = 0;
        virtual int delete_checkpoint(uint64_t cp) = 0;

        virtual int _setattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset) = 0; 
        virtual int _rmattr(coll_t c, const ghobject_t &o, const char *name)  = 0;
        virtual int _rmattrs(coll_t c, const ghobject_t &o) = 0;
	virtual int _getattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset) = 0;
	virtual int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num, uint64_t num_objs) const
        { return 0; }
        virtual int _collection_setattr(coll_t c, const char *name, const void *value, size_t size)  = 0;
        virtual int _collection_rmattr(coll_t c, const char *name) = 0;

        virtual int _omap_clear(coll_t c, const ghobject_t &o) = 0;
        virtual int _omap_setkeys(coll_t c, const ghobject_t& o, const map<string, bufferlist> &aset) = 0;
        virtual int _omap_rmkeys(coll_t c, const ghobject_t& o, const set<string> &keys) = 0;
        virtual int _omap_rmkeyrange(coll_t c, const ghobject_t& o, const string& first, const string& last) = 0;
        virtual int _omap_setheader(coll_t c, const ghobject_t& o, const bufferlist &bl) = 0;
	virtual int _omap_get(coll_t c, const ghobject_t &o, bufferlist &header, map<string, bufferlist> &out) = 0;

        virtual bool collection_exists(coll_t c) = 0;
        virtual bool exists(coll_t c, const ghobject_t& o) = 0;
        virtual int _open(coll_t c, const ghobject_t& o) = 0;
        virtual int _touch(coll_t c, const ghobject_t& o) = 0;
        virtual int _zero(coll_t c, const ghobject_t& o, uint64_t off, size_t len) = 0;
        virtual int _truncate(coll_t c, const ghobject_t& o, uint64_t size) = 0;
        virtual int _remove(coll_t c, const ghobject_t &o) = 0;
        virtual int _clone(coll_t c, const ghobject_t& oo, const ghobject_t& no) = 0;
        virtual int _clone_range(coll_t c, const ghobject_t& oo, const ghobject_t& no, 
                uint64_t srcoff, uint64_t len, uint64_t dstoff) = 0;

        virtual int _create_collection(coll_t c) = 0;
        virtual int _destroy_collection(coll_t c) = 0;
        virtual int _collection_add(coll_t dst, coll_t src, const ghobject_t &o) = 0;
        virtual int _collection_move_rename(coll_t oc, const ghobject_t& oo, coll_t nc, const ghobject_t& no) = 0;
        virtual int _collection_rename(const coll_t &c, const coll_t &nc) = 0;
        virtual int _split_collection(coll_t c, uint32_t bits, uint32_t rem, coll_t dest) = 0;

        virtual int _read(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, bufferlist& bl) = 0;
        virtual int _write(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, 
                const bufferlist& bl, bool replica = false) = 0;
} ;

class RWJournal {
    protected:
	BackStore *store;
	Finisher *finisher;
    public:
	RWJournal(BackStore *s, Finisher *fin) : 
	    store(s),
	    finisher(fin),
	    evict_threshold(0.85) {	}
	virtual ~RWJournal() { }

	virtual int replay_journal() = 0;
	virtual int start() = 0;
	virtual void stop() = 0;
	virtual int mkjournal() = 0;

	typedef ObjectStore::Transaction Transaction;
	typedef ObjectStore::Sequencer Sequencer;
	typedef ObjectStore::Sequencer_impl Sequencer_impl;

	virtual void submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle) = 0;

	virtual int read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl) = 0;
	virtual uint64_t get_object_size(coll_t cid, const ghobject_t &oid) = 0;

	virtual bool support_omap() { return false; }
	virtual int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value) { return -1; }
	virtual int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset) { return -1; }

	virtual int omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out) {return -1;}
	virtual int omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header, bool allow_eio = false) {return -1;}
	virtual int omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys) {return -1;}
	virtual int omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys,  map<string, bufferlist> *out) {return -1;}
	virtual int omap_check_keys(coll_t cid, const ghobject_t &oid, const set<string> &keys, set<string> *out) {return -1;};
	virtual ObjectMap::ObjectMapIterator get_omap_iterator(coll_t cid, const ghobject_t &oid) {
	    return ObjectMap::ObjectMapIterator();
	}

	void set_evict_threshold(double ev) {
	    evict_threshold = ev;
	}
	static RWJournal* create(string hot_journal, string cold_journal, string conf, BackStore* s, Finisher *fin, int type = 0);
    protected:
	double evict_threshold;
};
#endif
