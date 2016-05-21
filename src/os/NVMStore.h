#ifndef CEPH_NVMSTORE_H
#define CEPH_NVMSTORE_H

#include "common/Finisher.h"
#include "ObjectStore.h"
#include "RWJournal.h"
#include "ObjectDataStore.h"
#include "ObjectAttrStore.h"

class NVMStore: public ObjectStore {
    class BackStore_Imp: public BackStore {
        private:
            NVMStore *store;
        public:
            BackStore_Imp(NVMStore *s) :
                store(s) { }
        public:
            int list_checkpoints(deque<uint64_t>& ls);
            int create_checkpoint(uint64_t cp, uint64_t *id);
            int flush_checkpoint(uint64_t id);
            int rollback_to(uint64_t cp);
            int delete_checkpoint(uint64_t cp);

            void sync() { }
            int _setattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset);
            int _rmattr(coll_t c, const ghobject_t &o, const char *name);
            int _rmattrs(coll_t c, const ghobject_t &o);
	    int _getattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset);

	    int _collection_setattr(coll_t c, const char *name, const void *value, size_t size);
            int _collection_rmattr(coll_t c, const char *name);

            int _omap_clear(coll_t c, const ghobject_t &o);
            int _omap_setheader(coll_t c, const ghobject_t &o, const bufferlist &bl);
            int _omap_setkeys(coll_t c, const ghobject_t& o, const map<string, bufferlist> &aset);
            int _omap_rmkeys(coll_t c, const ghobject_t& o, const set<string> &keys);
            int _omap_rmkeyrange(coll_t c, const ghobject_t& o, const string& first, const string& last);
	    int _omap_get(coll_t c, const ghobject_t &o, bufferlist &header, map<string, bufferlist> &out);
            
	    bool collection_exists(coll_t c);
            bool exists(coll_t c, const ghobject_t& o);
            int _open(coll_t c, const ghobject_t& o);
            int _touch(coll_t c, const ghobject_t& o);
            int _zero(coll_t c, const ghobject_t& o, uint64_t off, size_t len);
            int _truncate(coll_t c, const ghobject_t& o, uint64_t size);
            int _remove(coll_t c, const ghobject_t &o);
            int _clone(coll_t c, const ghobject_t& oo, const ghobject_t& no);
            int _clone_range(coll_t c, const ghobject_t& oo, const ghobject_t& no, 
                    uint64_t srcoff, uint64_t len, uint64_t dstoff);

            int _create_collection(coll_t c);
            int _destroy_collection(coll_t c);
            int _collection_add(coll_t dst, coll_t src, const ghobject_t &o);
            int _collection_move_rename(coll_t oc, const ghobject_t& oo, coll_t nc, const ghobject_t& no);
            int _collection_rename(const coll_t &c, const coll_t &nc);
            int _split_collection(coll_t c, uint32_t bits, uint32_t rem, coll_t dest);

            int _read(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, bufferlist& bl);
            int _write(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, 
                    const bufferlist& bl, bool replica = false);

            virtual ~BackStore_Imp() { }
    };
    friend class BackStore_Imp;

    string base_path, current_path;
    int basefd, currentfd;
    
    Finisher finisher;
    ObjectDataStore data;
    ObjectAttrStore attr;

    BackStore_Imp bs;
    RWJournal *Journal;

    bool really_mountted;
    int init();
    int _mkfs();
    int list_checkpoints(deque<uint64_t>& cps);
    int create_checkpoint(uint64_t cp, uint64_t* transid);
    int flush_checkpoint(uint64_t transid);
    int rollback_to(uint64_t cp);
    int delete_checkpoint(uint64_t cp);
    int delete_subvolume(string volname);
public:
    NVMStore(CephContext* cct, const string &path, const string &hot_journal, const string &cold_journal) :
        ObjectStore(path),
        base_path(path),
        basefd(-1), currentfd(-1),
        finisher(cct),
        data(path),
        bs(this),
        really_mountted(false)
    {
        Journal = RWJournal::create(hot_journal, cold_journal, path, &bs, &finisher);
    }
    virtual ~NVMStore()
    {
        if (really_mountted) {
            data.umount();
            finisher.stop();
            close(basefd);
            close(currentfd);
        }
    }
    int update_version_stamp() {
        return 0;
    }
    uint32_t get_target_version() {
        return 1;
    }
    int peek_journal_fsid(uuid_d *fsid) {
        *fsid = uuid_d();
        return 0;
    }
    bool test_mount_in_use() {
        return false;
    }
    int mount();
    int umount();

    unsigned get_max_object_name_length(){
        return 1024;
    }
    unsigned get_max_attr_name_length(){
        return 1024;
    }

    int mkfs();
    int mkjournal();

    void set_allow_sharded_objects() {
    }
    bool get_allow_sharded_objects() {
        return true;
    }

    int statfs(struct statfs *buf);
    bool exists(coll_t cid, const ghobject_t& oid);
    int stat(
            coll_t cid,
            const ghobject_t& oid,
            struct stat *st,
            bool allow_eio = false);
    int read(
            coll_t cid,
            const ghobject_t& oid,
            uint64_t offset,
            size_t len,
            bufferlist& bl,
            bool allow_eio = false);
    int _read(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
    int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
    int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value);
    int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

    int list_collections(vector<coll_t>& ls);
    bool collection_exists(coll_t c);
    int collection_getattr(coll_t cid, const char *name,
            void *value, size_t size);
    int collection_getattr(coll_t cid, const char *name, bufferlist& bl);
    int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);
    bool collection_empty(coll_t c);
    int collection_list(coll_t cid, vector<ghobject_t>& o);
    int collection_list_partial(coll_t cid, ghobject_t start,
            int min, int max, snapid_t snap, 
            vector<ghobject_t> *ls, ghobject_t *next);
    int collection_list_range(coll_t cid, ghobject_t start, ghobject_t end,
            snapid_t seq, vector<ghobject_t> *ls);

    int omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out);
    int omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header, bool allow_eio = false);
    int omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys);
    int omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys,  map<string, bufferlist> *out);
    int omap_check_keys(coll_t cid, const ghobject_t &oid, const set<string> &keys, set<string> *out);

    ObjectMap::ObjectMapIterator get_omap_iterator(coll_t cid, const ghobject_t &oid);
    void set_fsid(uuid_d u);
    uuid_d get_fsid();

    objectstore_perf_stat_t get_cur_stats(){
        return objectstore_perf_stat_t();
    }
    int queue_transactions(
            Sequencer *osr, list<Transaction*>& tls,
            TrackedOpRef op = TrackedOpRef(),
            ThreadPool::TPHandle *handle = NULL);
};
#endif 
