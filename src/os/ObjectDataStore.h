/**
 * backend storage for object and collection   
 **/
#ifndef OBJECT_DATASTORE_H
#define OBJECT_DATASTORE_H
#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "include/types.h"

#include "FlatIndex.h"
#include "FDCache.h"
#include "WBThrottle.h"

class ObjectDataStore {
    string base_path;
    FDCache fdcache;
    WBThrottle wbthrottle;
    
    bool mountted;
    Mutex lock;
    map<coll_t, FlatIndex*> coll_indics;

public:
    ObjectDataStore(const string &path) :
	base_path(path),
	fdcache(g_ceph_context),
	wbthrottle(g_ceph_context),
	mountted(false),
	lock("ObjectDataStore::lock") { }
    
    ~ObjectDataStore();

private:
    typedef FlatIndex::IndexedPath IndexedPath;

    int get_cdir(const coll_t& cid, char *path, int len);
    int get_index(const coll_t& cid, FlatIndex **index);
    int init_index(const coll_t& cid);
    int lfn_find(const ghobject_t& oid, FlatIndex *index, IndexedPath *path);
    int lfn_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
    int lfn_stat(const coll_t& cid, const ghobject_t& oid, struct stat *s);
    int lfn_open(const coll_t& cid, const ghobject_t& oid, bool create, FDRef *fd);   
    int lfn_close(FDRef fd);
    int lfn_link(const coll_t& c, const coll_t& nc, const ghobject_t& o, const ghobject_t& no);
    int lfn_unlink(const coll_t& cid, const ghobject_t& oid);

public:
    /* object */
    int open(const coll_t& cid, const ghobject_t& oid);
    int touch(const coll_t& cid, const ghobject_t& oid);
    int write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist &bl);
    int zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len);
    int truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
    int remove(const coll_t& cid, const ghobject_t& oid);
    int clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid);
    int clone_range(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid,
	    uint64_t srcoff, size_t len, uint64_t dstoff);
    
    /* collection */
    int create_collection(const coll_t& cid);
    int destroy_collection(const coll_t& cid);
    int collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid);
    int collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid, const coll_t& cid, const ghobject_t& oid);
    int collection_rename(const coll_t& cid, const coll_t& ncid);
    int split_collection(const coll_t& cid, uint32_t bits, uint32_t match, const coll_t& dest);
    
    /* collection attribute */
    int collection_setattr(const coll_t& cid, const char* name, const void* value, size_t len);
    int collection_setattrs(const coll_t& cid, map<string, bufferptr>& aset);
    int collection_rmattr(const coll_t& cid, const char *name);

    bool exists(const coll_t& cid, const ghobject_t& oid);
    int stat(const coll_t& cid, const ghobject_t& oid, struct stat* st);
    int read(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist &bl);

    int list_collections(vector<coll_t>& ls);
    bool collection_exists(const coll_t& cid);
    int collection_getattr(const coll_t& cid, const char *name, void *val, size_t len);
    int collection_getattr(const coll_t& cid, const char *name, bufferlist& bl);
    int collection_getattrs(const coll_t& cid, map<string, bufferptr>& aset);
    bool collection_empty(const coll_t& c);
    int collection_list(const coll_t& cid, vector<ghobject_t>& os);
public:
    int mkfs();
    int mount();
    void umount();
    void sync();
    int statfs(struct statfs *stat);
};
#endif
