#ifndef CEPH_OBJECTATTRSTORE_H
#define CEPH_OBJECTATTRSTORE_H

#include <map>
#include <string>
#include <vector>
using std::map;
using std::string;
using std::vector;

#include "common/RWLock.h"
#include "include/buffer.h"
#include "include/types.h"
#include "include/unordered_map.h"
#include "common/errno.h"
#include "osd/osd_types.h"
#include "ObjectMap.h"
#include "LevelDBStore.h"
#include "DBObjectMap.h"
class ObjectAttrStore {
    /*memory*/
    struct Object {
	map<string, bufferptr> xattr;
	bufferlist omap_header;
	map<string, bufferlist> omap;

	void encode(bufferlist &bl) {
	    ENCODE_START(1,1,bl);
	    ::encode(xattr, bl);
	    ::encode(omap_header, bl);
	    ::encode(omap, bl);
	    ENCODE_FINISH(bl);
	}
	void decode(bufferlist::iterator it) {
	    DECODE_START(1, it);
	    ::decode(xattr, it);
	    ::decode(omap_header, it);
	    ::decode(omap, it);
	    DECODE_FINISH(it);
	}
    };
    typedef ceph::shared_ptr<Object> ObjectRef;
    
    struct Collection {
	ceph::unordered_map<ghobject_t, ObjectRef> object_hash;
	map<ghobject_t, ObjectRef> object_map;
	RWLock lock;

	Collection():
	    lock("ObjectAttrStore::Lock") { }

	ObjectRef get_object(const ghobject_t &oid) {
	    ceph::unordered_map<ghobject_t, ObjectRef>::iterator it
		= object_hash.find(oid);
	    if (it == object_hash.end())
		return ObjectRef();
	    return it->second;
	}
	void encode(bufferlist &bl) const {
	    ENCODE_START(1, 1, bl);
	    uint32_t size = object_map.size();
	    ::encode(size, bl);
	    for (map<ghobject_t, ObjectRef>::const_iterator it = object_map.begin();
		    it != object_map.end();
		    ++it) {
		::encode(it->first, bl);
		it->second->encode(bl);
	    }
	    ENCODE_FINISH(bl);
	}
	void decode(bufferlist::iterator &it) {
	    DECODE_START(1, it);
	    uint32_t size;
	    ::decode(size, it);
	    while(size--) {
		ghobject_t oid;
		::decode(oid, it);
		ObjectRef o(new Object);
		o->decode(it);
		object_hash[oid] = o;
		object_map[oid] = o;
	    }
	    DECODE_FINISH(it);
	}
    };
    typedef ceph::shared_ptr<Collection> CollectionRef;

    class OmapIteratorImpl: public ObjectMap::ObjectMapIteratorImpl {
	CollectionRef coll;
	ObjectRef obj;
	map<string, bufferlist>::iterator it;
    public:
	OmapIteratorImpl(CollectionRef c, ObjectRef o) :
	    coll(c), obj(o), it(o->omap.begin()) { }

	int seek_to_first(){
	    it = obj->omap.begin();
	    return 0;
	}
	int upper_bound(const string &after) {
	    it = obj->omap.upper_bound(after);
	    return 0;
	}
	int lower_bound(const string &to) {
	    it = obj->omap.lower_bound(to);
	    return 0;
	}
	bool valid() {
	    return it != obj->omap.end();
	}
	int next() {
	    ++ it;
	    return 0;
	}
	string key() {
	    return it->first;
	}
	bufferlist value() {
	    return it->second;
	}
	int status() {
	    return 0;
	}
    };
    ceph::unordered_map<coll_t, CollectionRef> coll_map;
    RWLock coll_lock;

    CollectionRef get_collection(const coll_t &cid) {
	RWLock::RLocker l(coll_lock);
	ceph::unordered_map<coll_t, CollectionRef>::iterator it = coll_map.find(cid);
	if (it == coll_map.end())
	    return CollectionRef();
	return it->second;
    }
    /*ObjectMap*/
    LevelDBStore *store;
    boost::scoped_ptr<ObjectMap> object_map;

    bool using_dbmemory;

private:
    void debug_dump();

public:
    ObjectAttrStore(bool all_in_memory=true) :
        coll_lock("ObjectAttrStore::coll_lock"),
        using_dbmemory(all_in_memory)
    { }
    int init(string path, ostream& err) {
        if (!using_dbmemory) {
            store = new LevelDBStore(g_ceph_context, path);
            int ret = store->create_and_open(err);
            if (ret < 0) {
                delete store;
                return ret;
            }
            ObjectMap *omap = new DBObjectMap(store);
            object_map.reset(omap);
        }
        return 0;
    }

    int create_collection(const coll_t &c);
    int destroy_collection(const coll_t &c);
    int collection_add(const coll_t &dst, const coll_t &src, const ghobject_t &oid);
    int collection_move_rename(const coll_t &oc, const ghobject_t &oo, const coll_t &nc, const ghobject_t &no);
    int collection_rename(const coll_t &c, const coll_t &nc);
    
    int touch(const coll_t &c, const ghobject_t &oid);
    int remove(const coll_t &c, const ghobject_t &oid);
    int clone(const coll_t &c, const ghobject_t &oo, const ghobject_t &no);

    int setattrs(const coll_t &c, const ghobject_t &oid, const map<string, bufferptr> &aset);
    int rmattr(const coll_t &c, const ghobject_t &oid, const char *name);
    int rmattrs(const coll_t &c, const ghobject_t &oid);
    int getattr(const coll_t &c, const ghobject_t &oid, const char *name, bufferptr &value);
    int getattrs(const coll_t &c, const ghobject_t &oid, map<string, bufferptr> &aset);

    int omap_clear(const coll_t &c, const ghobject_t &o);
    int omap_setkeys(const coll_t &c, const ghobject_t &o, const map<string, bufferlist> &aset);
    int omap_rmkeys(const coll_t &c, const ghobject_t &o, const set<string> &keys);
    int omap_rmkeyrange(const coll_t &c, const ghobject_t &o, const string &first, const string &last);
    int omap_setheader(const coll_t &c, const ghobject_t &o, const bufferlist &bl);
    int omap_get(const coll_t &c, const ghobject_t &o, bufferlist *header, map<string, bufferlist> *out);
    int omap_get_header(const coll_t &c, const ghobject_t &o, bufferlist *header);
    int omap_get_keys(const coll_t &c, const ghobject_t &o, set<string> *keys);
    int omap_get_values(const coll_t &c, const ghobject_t &o, const set<string> &keys, map<string, bufferlist> *out);
    int omap_check_keys(const coll_t &c, const ghobject_t &o, const set<string> &keys, set<string> *out);
    ObjectMap::ObjectMapIterator get_omap_iterator(const coll_t &cid, const ghobject_t &c);
};

#endif
