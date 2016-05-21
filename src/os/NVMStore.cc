#include <string>
#include <sys/ioctl.h>
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "include/int_types.h"
#include "btrfs_ioctl.h"
#include "NVMStore.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << __FILE__ << ": " << __func__ << ": " << __LINE__ << ": "

static const int BTRFS_SUPER_MAGIC = 0x9123683E;

int NVMStore::BackStore_Imp::list_checkpoints(deque<uint64_t>& ls)
{
    return store->list_checkpoints(ls);
}
int NVMStore::BackStore_Imp::create_checkpoint(uint64_t cp, uint64_t *id)
{
    return store->create_checkpoint(cp, id);
}
int NVMStore::BackStore_Imp::flush_checkpoint(uint64_t id)
{
    return store->flush_checkpoint(id);
}
int NVMStore::BackStore_Imp::rollback_to(uint64_t cp)
{
    return store->rollback_to(cp);
}
int NVMStore::BackStore_Imp::delete_checkpoint(uint64_t cp)
{
    return store->delete_checkpoint(cp);
}

int NVMStore::BackStore_Imp::_setattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    return store->attr.setattrs(c, oid, aset);
}

int NVMStore::BackStore_Imp::_rmattr(coll_t c, const ghobject_t &oid, const char *name)
{
    return store->attr.rmattr(c, oid, name);
}

int NVMStore::BackStore_Imp::_rmattrs(coll_t c, const ghobject_t &oid)
{
    return store->attr.rmattrs(c, oid);
}

int NVMStore::BackStore_Imp::_getattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &out)
{
    return store->attr.getattrs(c, oid, out);
}

int NVMStore::BackStore_Imp::_collection_setattr(coll_t c, const char *name, const void *value, size_t size)
{
    return store->data.collection_setattr(c, name, value, size);
}

int NVMStore::BackStore_Imp::_collection_rmattr(coll_t c, const char *name)
{
    return store->data.collection_rmattr(c, name);
}

int NVMStore::BackStore_Imp::_omap_clear(coll_t c, const ghobject_t &oid)
{
    return store->attr.omap_clear(c, oid);
}

int NVMStore::BackStore_Imp::_omap_setheader(coll_t c, const ghobject_t &oid, const bufferlist &bl)
{
    return store->attr.omap_setheader(c, oid, bl);
}
int NVMStore::BackStore_Imp::_omap_setkeys(coll_t c, const ghobject_t &oid, const map<string, bufferlist> &aset)
{
    return store->attr.omap_setkeys(c, oid, aset);
}

int NVMStore::BackStore_Imp::_omap_rmkeys(coll_t c, const ghobject_t &oid, const set<string> &keys)
{
    return store->attr.omap_rmkeys(c, oid, keys);
}

int NVMStore::BackStore_Imp::_omap_rmkeyrange(coll_t c, const ghobject_t &oid, const string &first, const string &last)
{
    return store->attr.omap_rmkeyrange(c, oid, first, last);
}

int NVMStore::BackStore_Imp::_omap_get(coll_t c, const ghobject_t &oid, bufferlist &header, map<string, bufferlist> &out)
{
    return store->attr.omap_get(c, oid, &header, &out);
}
bool NVMStore::BackStore_Imp::collection_exists(coll_t c)
{
    return store->data.collection_exists(c);
}

bool NVMStore::BackStore_Imp::exists(coll_t c, const ghobject_t &oid)
{
    return store->data.exists(c, oid);
}

int NVMStore::BackStore_Imp::_open(coll_t c, const ghobject_t &oid)
{
    return store->data.open(c, oid);
}

int NVMStore::BackStore_Imp::_touch(coll_t c, const ghobject_t &oid)
{
    int r = store->attr.touch(c, oid);
    if (!r)
	r = store->data.touch(c, oid);
    return r;
}

int NVMStore::BackStore_Imp::_zero(coll_t c, const ghobject_t &oid, uint64_t off, size_t len)
{
    return store->data.zero(c, oid, off, len);
}

int NVMStore::BackStore_Imp::_truncate(coll_t c, const ghobject_t &oid, uint64_t size)
{
    return store->data.truncate(c, oid, size);
}

int NVMStore::BackStore_Imp::_remove(coll_t c, const ghobject_t &oid)
{
    store->attr.remove(c, oid);
    store->data.remove(c, oid);
    return 0;
}

int NVMStore::BackStore_Imp::_clone(coll_t c, const ghobject_t &oo, const ghobject_t &no)
{
    store->attr.clone(c, oo, no);
    store->data.clone(c, oo, no);
    return 0;
}

int NVMStore::BackStore_Imp::_clone_range(coll_t c, const ghobject_t &oo, const ghobject_t &no, 
	uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
    return store->data.clone_range(c, oo, no, srcoff, len, dstoff);
}

int NVMStore::BackStore_Imp::_create_collection(coll_t c)
{
    store->attr.create_collection(c);
    return store->data.create_collection(c);
}

int NVMStore::BackStore_Imp::_destroy_collection(coll_t c)
{
    int r = store->attr.destroy_collection(c);
    if (!r)
	r = store->data.destroy_collection(c);
    return r;
}

int NVMStore::BackStore_Imp::_collection_add(coll_t dst, coll_t src, const ghobject_t &oid)
{
    int r = store->attr.collection_add(dst, src, oid);
    if (!r)
	r = store->data.collection_add(dst, src, oid);
    return r;
}

int NVMStore::BackStore_Imp::_collection_move_rename(coll_t oc, const ghobject_t &oo, coll_t nc, const ghobject_t &no)
{
    int r = store->attr.collection_move_rename(oc, oo, nc, no);
    if (!r)
	r = store->data.collection_move_rename(oc, oo, nc, no);
    return r;
}

int NVMStore::BackStore_Imp::_collection_rename(const coll_t &c, const coll_t &nc)
{
    int r = store->attr.collection_rename(c, nc);
    if (!r)
	r = store->data.collection_rename(c, nc);
    return r;
}

int NVMStore::BackStore_Imp::_split_collection(coll_t c, uint32_t bits, uint32_t rem, coll_t dest)
{
    assert(0 == "no supported yet!");
    return 0;
}

int NVMStore::BackStore_Imp::_read(coll_t c, const ghobject_t &oid, uint64_t offset, size_t len, bufferlist &bl)
{
    return store->data.read(c, oid, offset, len, bl);
}

int NVMStore::BackStore_Imp::_write(coll_t c, const ghobject_t &oid, uint64_t offset, size_t len, 
	const bufferlist &bl, bool replica)
{
    store->attr.touch(c, oid);
    return store->data.write(c, oid, offset, len, bl);
}

int NVMStore::statfs(struct statfs *st)
{
    return data.statfs(st);
}

bool NVMStore::exists(coll_t cid, const ghobject_t &oid)
{
    return data.exists(cid, oid);
}

int NVMStore::stat(coll_t cid, const ghobject_t &oid, struct stat *st, bool allow_eio)
{
    int ret = data.stat(cid, oid, st);
    if (ret < 0)
        return ret;
    /*FIXME*/
    uint64_t size = Journal->get_object_size(cid, oid);
    if (size > st->st_size)
        st->st_size = size;
    return ret;
}

int NVMStore::read(coll_t cid, const ghobject_t &oid, uint64_t offset, size_t len, bufferlist &bl, bool allow_eio)
{
    size_t max = 16 << 20; // The size of object must be smaller the 1MB
    if (!len)
	len = max;
    int r = Journal->read_object(cid, oid, offset, len, bl);
    if (r < 0)
	return r;
    return bl.length();
}

int NVMStore::fiemap(coll_t cid, const ghobject_t &oid, 
	uint64_t offset, size_t len, bufferlist &bl) 
{
    assert(0 == "no supportted yet");
    return 0;
}

int NVMStore::getattr(coll_t cid, const ghobject_t &oid, const char *name, bufferptr &value)
{
    if (Journal->support_omap())
	return Journal->getattr(cid, oid, name, value);
    return attr.getattr(cid, oid, name, value);
}

int NVMStore::getattrs(coll_t cid, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    if (Journal->support_omap())
	return Journal->getattrs(cid, oid, aset);
    return attr.getattrs(cid, oid, aset);
}

int NVMStore::list_collections(vector<coll_t> &ls)
{
    return data.list_collections(ls);
}

bool NVMStore::collection_exists(coll_t c)
{
    return data.collection_exists(c);
}

int NVMStore::collection_getattr(coll_t cid, const char *name,
	void *value, size_t size)
{
    return data.collection_getattr(cid, name, value, size);
}

int NVMStore::collection_getattr(coll_t cid, const char *name, bufferlist &bl)
{
    return data.collection_getattr(cid, name, bl);
}

int NVMStore::collection_getattrs(coll_t cid, map<string, bufferptr> &aset)
{
    return data.collection_getattrs(cid, aset);
}

bool NVMStore::collection_empty(coll_t cid)
{
    return data.collection_empty(cid);
}

int NVMStore::collection_list(coll_t cid, vector<ghobject_t> &o)
{
    return data.collection_list(cid, o);
}

int NVMStore::collection_list_partial(coll_t cid, ghobject_t start, 
	int min, int max, snapid_t snap,
	vector<ghobject_t> *ls, ghobject_t *next)
{
    vector<ghobject_t> lst;
    data.collection_list(cid, lst);
    set<ghobject_t> oset(lst.begin(), lst.end());
    int count = 0;
    set<ghobject_t>::iterator it = oset.lower_bound(start);
    while (it != oset.end() && count < max) {
	ls->push_back(*it);
	++ count;
	++ it;
    }
    if (it == oset.end())
	*next = ghobject_t::get_max();
    else
	*next = *it;
    return 0;
}

int NVMStore::collection_list_range(coll_t cid, ghobject_t start, ghobject_t end, 
	snapid_t seq, vector<ghobject_t> *ls)
{
    vector<ghobject_t> lst;
    data.collection_list(cid, lst);
    set<ghobject_t> oset(lst.begin(), lst.end());
    set<ghobject_t>::iterator it = oset.lower_bound(start);
    while (it != oset.end() && *it < end) {
	ls->push_back(*it);
	++ it;
    }
    return 0;
}

int NVMStore::omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out)
{
    if (Journal->support_omap())
	return Journal->omap_get(cid, oid, header, out);
    return attr.omap_get(cid, oid, header, out);
}

int NVMStore::omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header, bool allow_eio)
{
    if (Journal->support_omap())
	return Journal->omap_get_header(cid, oid, header);
    return attr.omap_get_header(cid, oid, header);
}

int NVMStore::omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys)
{
    if (Journal->support_omap())
	return Journal->omap_get_keys(cid, oid, keys);
    return attr.omap_get_keys(cid, oid, keys);
}

int NVMStore::omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys, map<string, bufferlist> *out)
{
    if (Journal->support_omap())
	return Journal->omap_get_values(cid, oid, keys, out);
    return attr.omap_get_values(cid, oid, keys, out);
}

int NVMStore::omap_check_keys(coll_t cid, const ghobject_t &oid, const set<string> &keys, set<string> *out)
{
    if (Journal->support_omap())
	return Journal->omap_check_keys(cid, oid, keys, out);
    return attr.omap_check_keys(cid, oid, keys, out);
}

ObjectMap::ObjectMapIterator NVMStore::get_omap_iterator(coll_t cid, const ghobject_t &oid)
{
    if (Journal->support_omap())
	return Journal->get_omap_iterator(cid, oid);
    return attr.get_omap_iterator(cid, oid);
}

int NVMStore::init()
{
    int ret = 0;
    do {
        if (basefd > 0) {
            ::close(basefd);
            basefd = -1;
        }
        basefd = ::open(base_path.c_str(), O_RDONLY);
        if (basefd < 0) {
            ret = -errno;
            dout(0) << "open '" << base_path << "' failed with error : " << cpp_strerror(ret) << dendl;
            break;
        }

        struct statfs fs;
        current_path = base_path + "/current";
        if (currentfd > 0) {
            ::close(currentfd);
            currentfd = -1;
        }
        currentfd = ::open(current_path.c_str(), O_RDONLY);
        if (currentfd < 0) {
            ret = -errno;
            dout(0) << "open '" << current_path << "' failed with error : " << cpp_strerror(ret) << dendl;
            break;
        }
        ret = ::statfs(current_path.c_str(), &fs);
        if (ret < 0) {
            ret = -errno;
            dout(0) << "cannot statfs " << base_path << dendl;
            break;
        }
        if (fs.f_type != BTRFS_SUPER_MAGIC) {
            dout(0) << "filesystem should be btrfs" << dendl;
            ret = -EINVAL;
        }
    }while(false);

    if (ret == 0)
        return ret;

    if (basefd > 0) {
        close(basefd);
        basefd = -1;
    }
    if (currentfd > 0) {
        close(currentfd);
        currentfd = -1;
    }
    return ret;
}

int NVMStore::_mkfs()
{
    struct stat st;
    current_path = base_path + "/current";
    int ret = ::stat(current_path.c_str(), &st);
    if (ret == 0) {
        if (!S_ISDIR(st.st_mode)) {
            dout(0) << "current/ exists but is not directionary" << dendl;
            return -EINVAL;
        }
        struct statfs fs;
        ret = ::statfs(current_path.c_str(), &fs);
        if (ret < 0) {
            ret = -errno;
            dout(0) << "cannot statfs " << base_path << dendl;
            return ret;
        }
        if (fs.f_type != BTRFS_SUPER_MAGIC) {
            dout(0) << "filesystem should be btrfs" << dendl;
            return -EINVAL;
        }
        return 0;
    }

    if (basefd < 0) {
        basefd = ::open(base_path.c_str(), O_RDONLY);
        if (basefd < 0) {
            ret = -errno;
            dout(0) << "failed to open '" << base_path << "' with error : " << cpp_strerror(ret) << dendl;
            return ret;
        }
    }

    struct btrfs_ioctl_vol_args vol;
    memset(&vol, 0, sizeof(vol));
    vol.fd = 0;
    strcpy(vol.name, "current");

    if (::ioctl(basefd, BTRFS_IOC_SUBVOL_CREATE,(unsigned long int)&vol) < 0) {
        ret = -errno;
        dout(0) << "failed to create subvol with error: " << cpp_strerror(ret) << dendl;
        return ret;
    }

    if (::chmod(current_path.c_str(), 0755) < 0) {
        ret = -errno;
        dout(0) << "failed to chmod " <<  current_path << " with error: " << cpp_strerror(ret) << dendl;
        return ret;
    }
    return 0;
}

int NVMStore::list_checkpoints(deque<uint64_t>& cps)
{
    int ret = 0;
    DIR *dir = ::opendir(base_path.c_str());
    if (!dir) {
        ret = -errno;
        dout(0) << "opendir " << base_path << " failed with error: " << cpp_strerror(ret) << dendl;
        return ret;
    }
    list<uint64_t> snapshots;
    char path[PATH_MAX];
    char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
    struct dirent *ent;
    while (::readdir_r(dir, (struct dirent*)&buf, &ent) == 0) {
        if (!ent)
            break;
        snprintf(path, sizeof(path), "%s/%s", base_path.c_str(), ent->d_name);
        struct stat st;
        ret = ::stat(path, &st);
        if (ret < 0) {
            int err = -errno;
            dout(0) << "stat " << path << " failed with error : " << cpp_strerror(err) << dendl;
            break;
        }
        if (!S_ISDIR(st.st_mode))
            continue;
        struct statfs fs;
        ret = ::statfs(path, &fs);
        if (ret < 0) {
            int err = -errno;
            dout(0) << "statfs " << path << " failed with error : " << cpp_strerror(err) << dendl;
            break;
        }
        if (fs.f_type == BTRFS_SUPER_MAGIC) {
            stringstream ss;
            ss << ent->d_name;
            uint64_t seq = 0;
            ss >> seq;
            if (seq > 0)
                snapshots.push_back(seq);
        }
    }
    ::closedir(dir);
    snapshots.sort();
    for (list<uint64_t>::iterator it=snapshots.begin();
            it != snapshots.end();
            ++ it) {
        cps.push_back(*it);
    }
    return 0;
}
int NVMStore::create_checkpoint(uint64_t cp, uint64_t* transid)
{
    dout(8) << "create checkpoint:" << cp << dendl;
    struct btrfs_ioctl_vol_args_v2 vol;
    stringstream ss;
    ss << cp;
    string volname;
    ss >> volname;

    memset(&vol, 0, sizeof(vol));
    vol.fd = currentfd;
    vol.flags = BTRFS_SUBVOL_CREATE_ASYNC;
    strncpy(vol.name, volname.c_str(), sizeof(vol.name));
    int ret = ::ioctl(basefd, BTRFS_IOC_SNAP_CREATE_V2, &vol);
    if (ret < 0) {
        ret = -errno;
        dout(0) << "async snap create '" << volname << "' failed with error : " << cpp_strerror(ret) 
            << ", basefd = " << basefd
            << ", currentfd = " << currentfd << dendl;
        return ret;
    }
    *transid = vol.transid;
    return 0;
}
int NVMStore::flush_checkpoint(uint64_t transid)
{
    dout(8) << "waiting for completion of checkpoint, tranid =  " << transid << dendl;
    int ret = ::ioctl(basefd, BTRFS_IOC_WAIT_SYNC, &transid);
    if (ret < 0) {
        ret = -errno;
        dout(0) << "ioctl BTRFS_IOC_WAIT_SYNC failed with error : " << cpp_strerror(ret) << dendl;
        return ret;
    }
    dout(8) << "checkpoint success, transid = " << transid << dendl;
    return 0;
}
int NVMStore::rollback_to(uint64_t cp)
{
    dout(5) << " rollback to checkpoint '" << cp << "'" << dendl;
    if (currentfd > 0) {
        ::close(currentfd);
        currentfd = -1;
    }
    int ret = delete_subvolume("current"); 
    if (ret && errno!=ENOENT) {
        dout(0) << "failed to remove current subvol with error: " << cpp_strerror(ret) << dendl;
        stringstream ss;
        string name;
        ss << base_path << "/" << "current.old." << rand();
        ss >> name;
        ::rename(current_path.c_str(), name.c_str());
    }

    btrfs_ioctl_vol_args vol;
    memset(&vol, 0, sizeof(vol));
    string volpath;
    {
        stringstream ss;
        ss << base_path << "/" << cp;
        ss >> volpath;
    }
    vol.fd = ::open(volpath.c_str(), O_RDONLY);
    if (vol.fd < 0) {
        ret = -errno;
        dout(0) << "failed to open '" << volpath << "' with error : " << cpp_strerror(ret) << dendl;
        return ret;
    }
    strcpy(vol.name, "current");

    ret = ::ioctl(basefd, BTRFS_IOC_SNAP_CREATE, &vol);
    if (ret < 0) {
        ret = -errno;
        dout(0) << "BTRFS_IOC_SNAP_CREATE failed with error : " << cpp_strerror(ret) << dendl;
    }
    close(vol.fd);
   
    currentfd = ::open(current_path.c_str(), O_RDONLY);
    if (currentfd < 0) {
        ret = -errno;
        dout(0) << "failed to open '" << current_path << "' with error: " << cpp_strerror(ret) << dendl;
    }

    dout(5) << "rollback success!" << dendl;
    return ret;
}

int NVMStore::delete_checkpoint(uint64_t cp)
{
    string volname;
    {
        stringstream ss;
        ss << cp;
        ss >> volname;
    }
    return delete_subvolume(volname);
}

int NVMStore::delete_subvolume(string volname)
{
    dout(8) << "delete subvolume '" << volname << "'" << dendl;
    btrfs_ioctl_vol_args vol;
    memset(&vol, 0, sizeof(vol));
    vol.fd = 0;
    strncpy(vol.name, volname.c_str(), sizeof(vol.name));

    int ret = ::ioctl(basefd, BTRFS_IOC_SNAP_DESTROY, &vol);
    if (ret) {
        ret = -errno;
        dout(0) << "ioctl SNAP_DESTROY failed with error : " << cpp_strerror(ret) << dendl;
        return ret;
    }
    return 0;
}

int NVMStore::mount()
{
    if (0) {
        /*just debug*/
        stringstream ss;
        ceph::BackTrace bt(1);
        bt.print(ss);
        dout(0) << ss.str() << dendl;
    }
    int ret = 0;
    do {
        if (really_mountted)
            break;
        finisher.start();
        ret = init();
        if (ret < 0)
            break;

        deque<uint64_t> checkpoints;
        int ret = list_checkpoints(checkpoints);
        if (ret < 0)
            break;
        if (!checkpoints.empty()) {
            rollback_to(checkpoints.back());
        }
        else {
            /*clear current*/
        }

        ret = data.mount();
        if (ret < 0)
            break;

        stringstream ss;
        ret = attr.init(current_path+"/omap", ss);
        if (ret < 0) {
            dout(0) << "failed to initialize omap with error :" << ss.str() << dendl;
            break;
        }

        ret = Journal->replay_journal();
        if (ret < 0)
            break;
        ret = Journal->start();
        if (ret < 0)
            break;
        really_mountted = true;
    }while(false);

    if (ret < 0) {
        data.umount();
        finisher.stop();
        close(basefd);
        close(currentfd);
        basefd = -1;
        currentfd = -1;
    }
    return ret;
}

int NVMStore::umount()
{
    return 0;
}

int NVMStore::mkfs()
{
    string fsid;
    int r = read_meta("fs_fsid", &fsid);
    if (r == -ENOENT) {
	uuid_d id;
	id.generate_random();
	fsid = stringify(id);
	r = write_meta("fs_fsid", fsid);
	if (r < 0)
	    return r;
	dout(1) << "new fsid " << fsid << dendl;
    }
    else
	dout(1) << "had fsid" << fsid << dendl;

    r = _mkfs();
    if (r < 0)
        return r;
    
    stringstream ss;
    r = attr.init(current_path + "/omap", ss);
    if (r < 0) {
        dout(0) << ss.str() << dendl;
        return r;
    }

    r = Journal->mkjournal();
    if (r < 0) {
	dout(0) << "failed to mkjournal" << dendl;
	return r;
    }
    return r;
}

int NVMStore::mkjournal()
{
    return 0;
}

void NVMStore::set_fsid(uuid_d u)
{
    int r =  write_meta("fs_fsid", stringify(u));
    assert(r>=0);
}

uuid_d NVMStore::get_fsid()
{
    string fsid;
    int r = read_meta("fs_fsid", &fsid);
    assert(r >= 0);
    uuid_d uuid;
    bool b = uuid.parse(fsid.c_str());
    assert(b);
    return uuid;
}

int NVMStore::queue_transactions(Sequencer *osr, list<Transaction*>& tls,
	TrackedOpRef op, ThreadPool::TPHandle *handle)
{
    Journal->submit_entry(osr, tls, handle);
    return 0;
}
