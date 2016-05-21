#include <sys/vfs.h>
#include "chain_xattr.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "ObjectDataStore.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << __FILE__ << " " << __LINE__ << " " << __func__ << " "

int ObjectDataStore::get_cdir(const coll_t& cid, char *path, int len)
{
    const string &cname(cid.to_str());
    return snprintf(path, len, "%s/current/%s", base_path.c_str(), cname.c_str());
}

int ObjectDataStore::get_index(const coll_t& cid, FlatIndex **index)
{
    Mutex::Locker l(lock);
    map<coll_t, FlatIndex*>::iterator it = coll_indics.find(cid);
    if (it == coll_indics.end()) {
	char path[PATH_MAX];
	get_cdir(cid, path, sizeof(path));
	*index = new FlatIndex(cid, path);
	coll_indics[cid] = *index;
    }
    else 
	*index = it->second;
    return 0;
}

int ObjectDataStore::init_index(const coll_t& cid)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    FlatIndex index(cid, path);
    return index.init();
}

int ObjectDataStore::lfn_find(const ghobject_t& oid, FlatIndex *index, IndexedPath *path)
{
    IndexedPath tmp;
    if (!path) 
	path = &tmp;
    assert(NULL != index);
    int exist;
    int r = index->lookup(oid, path, &exist);
    if (r < 0) {
	assert(r != -EIO);
	return r;
    }
    if (!exist)
	return -ENOENT;
    return 0;
}

int ObjectDataStore::lfn_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
    FDRef fd;
    int r = lfn_open(cid, oid, false, &fd);
    if (r < 0)
	return r;
    r = ::ftruncate(**fd, size);
    if (r < 0)
	return -errno;
    return r;
}

int ObjectDataStore::lfn_stat(const coll_t& cid, const ghobject_t& oid, struct stat *st)
{
    FlatIndex *index;
    int r = get_index(cid, &index);
    if (r < 0)
	return r;

    IndexedPath path;
    RWLock::RLocker l(index->access_lock);
    r = lfn_find(oid, index, &path);
    if (r < 0)
	return r;
    r = ::stat(path->path(),  st);
    if (r < 0)
	return -errno;
    return r;
}

int ObjectDataStore::lfn_open(const coll_t& cid, const ghobject_t& oid, bool create, FDRef *outfd)
{
    int flags = O_RDWR;
    if (create)
	flags |= O_CREAT;

    FlatIndex *index = NULL;
    int r = get_index(cid, &index);
    if (r < 0) {
	dout(0) << "failed to get index of " << cid << dendl;
	return r;
    }
    assert(NULL != index);
    RWLock::WLocker l(index->access_lock);
    *outfd = fdcache.lookup(oid);
    if (*outfd)
	return 0;

    IndexedPath path;
    int exist;
    r = index->lookup(oid, &path, &exist);
    if (r < 0) {
	dout(0) << "cannot find " << oid << " in index :" << cpp_strerror(-r) << dendl;
	return r;
    }
    
    int fd = ::open(path->path(), flags, 0777);
    if (fd < 0)
	return -1;

    if (create && !exist) {
	r = index->created(oid, path->path());
	if (r < 0) {
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	    return r;
	}
        bufferlist bl;
        oid.encode(bl);
        r = chain_setxattr(path->path(), "user.ceph.obj", bl.c_str(), bl.length());
        if (r < 0) {
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
            return r;
        }
    }

    *outfd = fdcache.add(oid, fd, NULL);
    return 0;
}

int ObjectDataStore::lfn_close(FDRef fd) {
    return 0;
}

int ObjectDataStore::lfn_link(const coll_t& c, const coll_t& nc, const ghobject_t& o, const ghobject_t& no)
{
    FlatIndex *new_index, *old_index;
    IndexedPath	new_path, old_path;
    bool same = false;
    int r, exist;
    if (c == nc) {
        r = get_index(nc, &new_index);
        if (r < 0)
            return r;
        old_index = new_index;
        same = true;
    }
    else {
        r = get_index(c, &old_index);
        if (r < 0)
            return r;
        r = get_index(nc, &new_index);
        if (r < 0)
            return r;
    }

    if (same) {
        RWLock::WLocker l(old_index->access_lock);
        r = old_index->lookup(o, &old_path, &exist);
        if (r < 0) {
            assert(r != -EIO);
            return r;
        }
        if (!exist)
            return -ENOENT;
        r = old_index->lookup(no, &new_path, &exist);
        if (r < 0) {
            assert(r != -EIO);
            return r;
        }
        if (exist)
            return -EEXIST;
        r = ::link(old_path->path(), new_path->path());
        if (r < 0)
            return r;
        r = old_index->created(no, new_path->path());
        if (r < 0)
            return r;
    }
    else {
        RWLock::RLocker l1(old_index->access_lock);
        r = old_index->lookup(o, &old_path, &exist);
        if (r < 0) {
            assert(r != -EIO);
            return r;
        }
        if (!exist)
            return -ENOENT;

        RWLock::WLocker l2(new_index->access_lock);
        r = new_index->lookup(no, &new_path, &exist);
        if (r < 0) {
            assert(r != -EIO);
            return r;
        }
        if (exist) 
            return -EEXIST;

        r = ::link(old_path->path(), new_path->path());
        if (r < 0)
            return r;
        r = new_index->created(no, new_path->path());
        if (r < 0)
            return r;
    }
    bufferlist bl;
    no.encode(bl);
    r = chain_setxattr(new_path->path(), "user.ceph.obj", bl.c_str(), bl.length());
    return r;
}

int ObjectDataStore::lfn_unlink(const coll_t& cid, const ghobject_t& oid)
{
    FlatIndex *index;
    int r = get_index(cid, &index);
    if (r < 0)
	return r;
    wbthrottle.clear_object(oid);
    fdcache.clear(oid);
    return index->unlink(oid);
}

/* object */
int ObjectDataStore::open(const coll_t& cid, const ghobject_t& oid)
{
    dout(15) << "open " << cid << "/" << oid << dendl;
    FDRef fd;
    int r = lfn_open(cid, oid, true, &fd);
    if (r < 0) {
	dout(10) << "failed to open the " << cid << "/" << oid << ":" 
	    << cpp_strerror(r) << dendl;
	return -1;
    }
    return **fd;
}
int ObjectDataStore::touch(const coll_t& cid, const ghobject_t& oid)
{
    dout(15) << "touch " << cid << "/" << oid << dendl;
    FDRef fd;
    int r = lfn_open(cid, oid, true, &fd);
    if (r < 0)
	return r;
    lfn_close(fd);
    return 0;
}

int ObjectDataStore::write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl)
{
    dout(15) << "write " << cid << "/" << oid << dendl;
    FDRef fd;
    int r = lfn_open(cid, oid, true, &fd);
    if (r < 0) {
	dout(0) << "could not open the " << cid << "/" << oid << ":" 
	    << cpp_strerror(r) << dendl;
	return r;
    }
    int64_t off = ::lseek64(**fd, offset, SEEK_SET);
    if (off < 0) {
	dout(0) << "failed to lseek64 " << offset << dendl;
	return -errno;
    }
    if (off != offset) {
	dout(0) << "lseek64 return bad offset:" << off << dendl;
	return -EIO;
    }
    r = bl.write_fd(**fd);
    if (r == 0) {
	r = bl.length();
	wbthrottle.queue_wb(fd, oid, offset, len, false);
    }

    lfn_close(fd);
    return r;
}

int ObjectDataStore::zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len)
{
    dout(15) << "zero" << cid << "/" << oid << dendl;
    bufferptr bp(len);
    bp.zero();
    bufferlist bl;
    bl.push_back(bp);
    return write(cid, oid, offset, len, bl);
}

int ObjectDataStore::truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
    dout(15) << "truncate " << cid << "/" << oid << dendl;
    return lfn_truncate(cid, oid, size);
}

int ObjectDataStore::remove(const coll_t& cid, const ghobject_t& oid)
{
    dout(15) << "remove " << cid << "/" << oid << dendl;
    return lfn_unlink(cid, oid);
}

int ObjectDataStore::clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid)
{
    dout(15) << "clone " << cid << ": " << oldoid << "->" << newoid << dendl;
    assert(0 == "clone not supportted yet");
    return 0;
}

int ObjectDataStore::clone_range(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid,
	uint64_t srcoff, size_t len, uint64_t dstoff)
{
    dout(15) << "clone range " << cid << ":" << oldoid << "->" << newoid << dendl;
    assert(0 == "clone range not supportted yet");
    return 0;
}

/* collection */
int ObjectDataStore::create_collection(const coll_t& cid)
{
    dout(15) << "create collection " << cid << dendl;
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int r = ::mkdir(path, 0777);
    if (r < 0)
	return -errno;
    return init_index(cid);
}

int ObjectDataStore::destroy_collection(const coll_t& cid)
{
    dout(15) << "destroy_collection " << cid << dendl;
    FlatIndex *index;
    int r = get_index(cid, &index);
    if (r < 0)
	return r;
    RWLock::WLocker l(index->access_lock);
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    r = ::rmdir(path);
    if (r < 0)
	return -errno;
    return r;
}

int ObjectDataStore::collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid)
{
    dout(15) << "collection add " << ocid << "->" << cid << ":" << oid << dendl;
    return lfn_link(ocid, cid, oid, oid);
}

int ObjectDataStore::collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid, const coll_t& cid, const ghobject_t& oid)
{
    dout(15) << "collection_move_rename " << oldcid << "/" << oldoid << "->" << cid << "/" << oid << dendl;
    int r = lfn_link(oldcid, cid, oldoid, oid);
    if (r < 0)
	return r;
    return lfn_unlink(oldcid, oldoid);
}

int ObjectDataStore::collection_rename(const coll_t& cid, const coll_t& ncid)
{
    char old_coll[PATH_MAX], new_coll[PATH_MAX];
    get_cdir(cid, old_coll, sizeof(old_coll));
    get_cdir(ncid, new_coll, sizeof(new_coll));
    return ::rename(old_coll, new_coll);
}

int ObjectDataStore::split_collection(const coll_t& cid, uint32_t bits, uint32_t match, const coll_t& dest) 
{
    assert(0 == "not supportted yet");
}

/* collection attribute */
static void get_attrname(const char* name, char* outname, int len)
{
    snprintf(outname, len, "user.ceph.%s", name);
}

static bool parse_attrname(char **name)
{
  if (strncmp(*name, "user.ceph.", 10) == 0) {
    *name += 10;
    return true;
  }
  return false;
}

int ObjectDataStore::collection_setattr(const coll_t& cid, const char* name, const void* value, size_t len)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0) 
	return -errno;
    char attrname[PATH_MAX];
    get_attrname(name, attrname, sizeof(attrname));
    int r = chain_fsetxattr(fd, attrname, value, len);
    close(fd);
    return r;
}

int ObjectDataStore::collection_setattrs(const coll_t& cid, map<string, bufferptr>& aset)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0)
	return -errno;
    int r = 0;
    for(map<string, bufferptr>::iterator it = aset.begin();
	    it != aset.end();
	    ++ it) {
	char attrname[PATH_MAX];
	get_attrname(it->first.c_str(), attrname, sizeof(attrname));
	r = chain_fsetxattr(fd, attrname, it->second.c_str(), it->second.length());
	if (r < 0) 
	    break;
    }
    close(fd);
    return r;
}

int ObjectDataStore::collection_rmattr(const coll_t& cid, const char *name)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0)
	return -errno;
    char attr[PATH_MAX];
    get_attrname(name, attr, sizeof(attr));
    int r = chain_fremovexattr(fd, attr);
    close(fd);
    return r;
}

/* read object */
bool ObjectDataStore::exists(const coll_t& cid, const ghobject_t& oid)
{
    struct stat st;
    return lfn_stat(cid, oid, &st) == 0;
}

int ObjectDataStore::stat(const coll_t& cid, const ghobject_t& oid, struct stat* st)
{
    return lfn_stat(cid, oid, st);
}

int ObjectDataStore::read(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist &bl)
{
    FDRef fd;
    int r = lfn_open(cid, oid, false, &fd);
    if (r < 0) {
	dout(10) << "READ(" << cid << "/" << oid << ") open error!" << dendl;
	return r;
    }
    if (len == 0) {
	struct stat st;
	memset(&st, 0, sizeof(st));
	r = ::fstat(**fd, &st);
	len = st.st_size;
    }

    bufferptr bp(len);
    int got = safe_pread(**fd, bp.c_str(), len, offset);
    if (got < 0) {
	dout(10) << "READ(" << cid << "/" << oid << ") pread error!" << dendl;
	return got;
    }
    bp.set_length(got);
    bl.push_back(bp);
    lfn_close(fd);
    return got;
}

/* read collection */
int ObjectDataStore::list_collections(vector<coll_t>& ls)
{
    char current[PATH_MAX];
    snprintf(current, sizeof(current), "%s/current", base_path.c_str());
    int r = 0;
    DIR *dir = ::opendir(current);
    if (!dir) {
	derr << "open directory " << current << ": " << cpp_strerror(errno) << dendl;
	return -errno;
    }

    char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
    struct dirent *ent; 
    while ((r = ::readdir_r(dir, (struct dirent*)buf, &ent))==0) {
	if (!ent)
	    break;
	if (ent->d_type == DT_UNKNOWN) {
	    struct stat st;
	    char filename[PATH_MAX];
	    snprintf(filename, sizeof(filename), "%s/%s", current, ent->d_name);
	    r = ::stat(filename, &st);
	    if (r < 0) {
		r = -errno;
		break;
	    }
	    if (!S_ISDIR(st.st_mode))
		continue;
	}
	else if (ent->d_type != DT_DIR) 
	    continue;
	
	if (strcmp(ent->d_name, "omap") == 0)
	    continue;
	if (ent->d_name[0] == '.' &&
		(ent->d_name[1] == '\0' ||
		  (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
	    continue;
	ls.push_back(coll_t(ent->d_name));
    }
    ::closedir(dir);
    return r;
}

bool ObjectDataStore::collection_exists(const coll_t& cid)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    struct stat st;
    return 0 == ::stat(path, &st);
}

int ObjectDataStore::collection_getattr(const coll_t& cid, const char *name, void *val, size_t len)
{
    char path[PATH_MAX];
    get_cdir(cid, path,sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0)
	return -errno;
    char attr[PATH_MAX];
    get_attrname(name, attr, sizeof(PATH_MAX));
    int r = chain_fgetxattr(fd, attr, val, len);
    ::close(fd);
    return r;
}

static int _fgetattr(int fd, const char *name, bufferptr& bp)
{
    char val[100];
    int l = chain_fgetxattr(fd, name, val, sizeof(val));
    if (l >= 0) {
	bp = buffer::create(l);
	memcpy(bp.c_str(), val, l);
    } else if (l == -ERANGE) {
	l = chain_fgetxattr(fd, name, 0, 0);
	if (l > 0) {
	    bp = buffer::create(l);
	    l = chain_fgetxattr(fd, name, bp.c_str(), l);
	}
    }
    return l;
}

static int _fgetattrs(int fd, map<string,bufferptr>& aset)
{
    // get attr list
    char names1[100];
    int len = chain_flistxattr(fd, names1, sizeof(names1)-1);
    char *names2 = 0;
    char *name = 0;
    if (len == -ERANGE) {
        len = chain_flistxattr(fd, 0, 0);
        if (len < 0) {
            return len;
        }
        names2 = new char[len+1];
        len = chain_flistxattr(fd, names2, len);
        if (len < 0) {
            return len;
        }
        name = names2;
    } else if (len < 0) {
        return len;
    } else {
        name = names1;
    }

    name[len] = 0;
    char *end = name + len;
    while (name < end) {
        char *attrname = name;
        if (parse_attrname(&name)) {
            if (*name) {
                int r = _fgetattr(fd, attrname, aset[name]);
                if (r < 0)
                    return r;
            }
        }
        name += strlen(name) + 1;
    }

    delete[] names2;
    return 0;
}

int ObjectDataStore::collection_getattr(const coll_t& cid, const char *name, bufferlist& bl)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0) 
        return -errno;
    bufferptr bp;
    char attrname[PATH_MAX];
    get_attrname(name, attrname, sizeof(attrname));
    int r = _fgetattr(fd, attrname, bp);
    if (r < 0)
        return r;
    bl.push_back(bp);
    return r;
}
int ObjectDataStore::collection_getattrs(const coll_t& cid, map<string, bufferptr>& aset)
{
    char path[PATH_MAX];
    get_cdir(cid, path, sizeof(path));
    int fd = ::open(path, O_RDONLY);
    if (fd < 0)
	return -errno;
    int r = _fgetattrs(fd, aset);
    ::close(fd);
    return r;
}

bool ObjectDataStore::collection_empty(const coll_t& cid)
{
    vector<ghobject_t> vec;
    collection_list(cid, vec);
    return vec.empty();
}

int ObjectDataStore::collection_list(const coll_t& cid, vector<ghobject_t>& ls)
{
    ls.clear();
    FlatIndex *index;
    int r = get_index(cid, &index);
    if (r < 0)
	return r;
    RWLock::RLocker l(index->access_lock);
    r = index->collection_list(&ls);
    if (r < 0)
        return r;
    for (vector<ghobject_t>::iterator it = ls.begin();
            it != ls.end();
            ++ it) {
        IndexedPath path;
        r = lfn_find(*it, index, &path);
        if (r < 0)
            return r;
        bufferptr bp(PATH_MAX);
        r = chain_getxattr(path->path(), "user.ceph.obj", bp.c_str(), bp.length());
        if (r < 0) {
            dout(5) << "oid = '" << *it << "', path = " << path->path() << ": no user.ceph.obj attribute" << dendl;
            continue;
        }
        bufferlist bl;
        bl.push_back(bp);
        bufferlist::iterator p = bl.begin();
        it->decode(p);
    }
    return 0;
}

int ObjectDataStore::mount()
{
    wbthrottle.start();
    mountted = true;
    return 0;
}

void ObjectDataStore::umount() 
{
    if (mountted) {
	wbthrottle.stop();
	mountted = false;
    }
}

ObjectDataStore::~ObjectDataStore()
{
    umount();
}

void ObjectDataStore::sync()
{
    wbthrottle.clear();
}

int ObjectDataStore::statfs(struct statfs *st)
{
    if (::statfs(base_path.c_str(), st ) < 0) {
	int r = -errno;
	assert(r != -EIO);
	return r;
    }
    return 0;
}

int ObjectDataStore::mkfs()
{
    return 0;
}
