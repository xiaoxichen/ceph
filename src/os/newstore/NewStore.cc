// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "NewStore.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "common/safe_io.h"

const string PREFIX_COLL = "C"; // collection name -> (nothing)
const string PREFIX_OBJ = "O";  // object name -> onode
const string PREFIX_WAL = "L";  // write ahead log


/*
 * key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '%' or >
 * 126 with % plus the 2 digit hex string.
 *
 * We use ! as a separator for strings; this works because it is < %
 * and will get escaped if it is present in the string.
 *
 * For the fixed length numeric fields, we just use hex and '.' as a
 * convenient visual separator.  Two oddities here:
 *
 *   1. for the -1 shard value we use --; it's the only negative value
 *      and it sorts < 0 that way.
 *
 *   2. for the pool value, we add 2^63 so that it sorts correctly
 *
 * We could do something much more compact here, but it would be less
 * readable by humans.  :/
 */

const string KEY_SEP_S = "!";

static void append_escaped(const string &in, string *out)
{
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '%' || *i > 126) {
      snprintf(hexbyte, sizeof(hexbyte), "%%%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
}

static void get_object_key(const ghobject_t& oid, string *key)
{
  char buf[PATH_MAX];
  char *t = buf;
  char *end = t + sizeof(buf);

  key->clear();

  // make field ordering match with ghobject_t compare operations
  if (oid.shard_id == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    *key = "--";
  } else {
    snprintf(buf, sizeof(buf), "%02x", (int)oid.shard_id);
    key->append(buf);
  }

  t += snprintf(t, end - t, ".%016llx.%.*X.",
		(long long)(oid.hobj.pool + 0x8000000000000000),
		(int)(sizeof(oid.hobj.get_hash())*2),
		(uint32_t)oid.get_filestore_key_u32());
  key->append(buf);

  append_escaped(oid.hobj.nspace, key);
  key->append(KEY_SEP_S);

  append_escaped(oid.hobj.get_key(), key);
  key->append(KEY_SEP_S);

  append_escaped(oid.hobj.oid.name, key);
  key->append(KEY_SEP_S);

  t = buf;
  t += snprintf(t, end - t, "%016llx.%016llx",
		(long long unsigned)oid.hobj.snap,
		(long long unsigned)oid.generation);
  key->append(buf);
}

// =======================================================

#define dout_subsys ceph_subsys_newstore
#undef dout_prefix
#define dout_prefix *_dout << "newstore(" << store->path << ").collection(" << cid << ") "

NewStore::Collection::Collection(NewStore *ns, coll_t c)
  : store(ns),
    cid(c),
    lock("NewStore::Collection::lock"),
    onode_map(store->cct, store->cct->_conf->newstore_onode_map_size)
{
}

NewStore::OnodeRef NewStore::Collection::get_onode(
  const ghobject_t& oid,
  bool create)
{
  assert(create ? lock.is_wlocked() : lock.is_locked());

  OnodeRef o = onode_map.lookup(oid);
  if (o)
    return o;

  string key;
  get_object_key(oid, &key);

  dout(20) << __func__ << " oid " << oid << " key '" << key << "'" << dendl;

  bufferlist v;
  int r = store->db->get(PREFIX_OBJ, key, &v);
  dout(20) << " r " << r << " v.len " << v.length() << dendl;
  Onode *on;
  assert(r >= 0);
  if (v.length() == 0) {
    if (!create)
      return OnodeRef();

    // new
    on = new Onode(oid, key);
    on->dirty = true;
  } else {
    // loaded
    on = new Onode(oid, key);
    bufferlist::iterator p = v.begin();
    ::decode(on->onode, p);
  }

  return onode_map.add(oid, on, NULL);
}



// =======================================================

#undef dout_prefix
#define dout_prefix *_dout << "newstore(" << path << ") "


NewStore::NewStore(CephContext *cct, const string& path)
  : ObjectStore(path),
    cct(cct),
    db(NULL),
    path_fd(-1),
    fsid_fd(-1),
    frag_fd(-1),
    fset_fd(-1),
    mounted(false),
    coll_lock("NewStore::coll_lock"),
    fid_lock("NewStore::fid_lock"),
    wal_lock("NewStore::wal_lock"),
    wal_seq(0),
    finisher(cct),
    logger(NULL),
    default_osr("default")
{
  _init_logger();
}

NewStore::~NewStore()
{
  _shutdown_logger();
  assert(!mounted);
  assert(db == NULL);
  assert(fsid_fd < 0);
  assert(frag_fd < 0);
}

void NewStore::_init_logger()
{
  // XXX
}

void NewStore::_shutdown_logger()
{
  // XXX
}

int NewStore::peek_journal_fsid(uuid_d *fsid)
{
  return 0;
}

int NewStore::_open_path()
{
  assert(path_fd < 0);
  path_fd = ::open(path.c_str(), O_DIRECTORY);
  if (path_fd < 0) {
    int r = -errno;
    derr << __func__ << " unable to open " << path << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  return 0;
}

void NewStore::_close_path()
{
  VOID_TEMP_FAILURE_RETRY(::close(path_fd));
  path_fd = -1;
}

int NewStore::_open_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int NewStore::_create_frag()
{
  assert(frag_fd < 0);
  frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  if (frag_fd < 0 && errno == ENOENT) {
    int r = ::mkdirat(path_fd, "fragments", 0755);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " cannot create " << path << "/fragments: "
	   << cpp_strerror(r) << dendl;
      return r;
    }
    frag_fd = ::openat(path_fd, "fragments", O_DIRECTORY);
  }
  if (frag_fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot open created " << path << "/fragments: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void NewStore::_close_frag()
{
  VOID_TEMP_FAILURE_RETRY(::close(frag_fd));
  frag_fd = -1;
}

int NewStore::_open_fsid(bool create)
{
  assert(fsid_fd < 0);
  int flags = O_RDWR;
  if (create)
    flags |= O_CREAT;
  fsid_fd = ::openat(path_fd, "fsid", flags, 0644);
  if (fsid_fd < 0) {
    int err = -errno;
    derr << __func__ << " " << cpp_strerror(err) << dendl;
    return err;
  }
  return 0;
}

int NewStore::_read_fsid(uuid_d *uuid)
{
  char fsid_str[40];
  int ret = safe_read(fsid_fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret > 36)
    fsid_str[36] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int NewStore::_write_fsid()
{
  int r = ::ftruncate(fsid_fd, 0);
  if (r < 0) {
    r = -errno;
    derr << __func__ << " fsid truncate failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  string str = stringify(fsid) + "\n";
  r = safe_write(fsid_fd, str.c_str(), str.length());
  if (r < 0) {
    derr << __func__ << " fsid write failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = ::fsync(fsid_fd);
  if (r < 0) {
    derr << __func__ << " fsid fsync failed: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void NewStore::_close_fsid()
{
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
}

int NewStore::_lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    derr << __func__ << " failed to lock " << path << "/fsid"
	 << " (is another ceph-osd still running?)"
	 << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool NewStore::test_mount_in_use()
{
  int r = _open_fsid(false);
  if (r < 0)
    return true;
  r = _lock_fsid();
  _close_fsid();
  return (r < 0);
}

int NewStore::_open_db()
{
  assert(!db);
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/db", path.c_str());
  db = KeyValueDB::create(g_ceph_context,
			  g_conf->newstore_backend /* fixme */,
			  fn);
  if (!db) {
    derr << __func__ << " error creating db" << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  db->init();
  stringstream err;
  if (db->create_and_open(err)) {
    derr << __func__ << " erroring opening db: " << err << dendl;
    delete db;
    db = NULL;
    return -EIO;
  }
  return 0;
}

void NewStore::_close_db()
{
  assert(db);
  delete db;
  db = NULL;
}

int NewStore::mkfs()
{
  dout(1) << __func__ << " path " << path << dendl;
  int r;
  uuid_d old_fsid;

  r = _open_path();
  if (r < 0)
    return r;

  r = _open_fsid(true);
  if (r < 0)
    goto out_path_fd;

  r = _lock_fsid();
  if (r < 0)
    goto out_close_fsid;

  r = _read_fsid(&old_fsid);
  if (r < 0 && old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << __func__ << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << __func__ << " using provided fsid " << fsid << dendl;
    }
    r = _write_fsid();
    if (r < 0)
      goto out_close_fsid;
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << __func__ << " on-disk fsid " << old_fsid
	   << " != provided " << fsid << dendl;
      r = -EINVAL;
      goto out_close_fsid;
    }
    fsid = old_fsid;
    dout(1) << __func__ << " fsid is already set to " << fsid << dendl;
  }

  r = _create_frag();
  if (r < 0)
    goto out_close_fsid;

  r = _open_db();
  if (r < 0)
    goto out_close_frag;

  // FIXME: superblock

  dout(10) << __func__ << " success" << dendl;
  r = 0;
  _close_db();

 out_close_frag:
  _close_frag();
 out_close_fsid:
  _close_fsid();
 out_path_fd:
  _close_path();
  return r;
}

int NewStore::mount()
{
  dout(1) << __func__ << " path " << path << dendl;

  int r = _open_path();
  if (r < 0)
    return r;
  r = _open_fsid(false);
  if (r < 0)
    goto out_path;

  r = _read_fsid(&fsid);
  if (r < 0)
    goto out_fsid;

  r = _lock_fsid();
  if (r < 0)
    goto out_fsid;

  r = _open_frag();
  if (r < 0)
    goto out_fsid;

  // FIXME: superblock, features

  r = _open_db();
  if (r < 0)
    goto out_frag;

  r = _recover_next_fid();
  if (r < 0)
    goto out_db;

  finisher.start();

  mounted = true;
  return 0;

 out_db:
  _close_db();
 out_frag:
  _close_frag();
 out_fsid:
  _close_fsid();
 out_path:
  _close_path();
  return r;
}

int NewStore::umount()
{
  assert(mounted);
  dout(1) << __func__ << dendl;
  finisher.wait_for_empty();
  mounted = false;
  if (fset_fd >= 0)
    VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
  finisher.stop();
  _close_db();
  _close_frag();
  _close_fsid();
  _close_path();
  return 0;
}

int NewStore::statfs(struct statfs *buf)
{
  if (::statfs(path.c_str(), buf) < 0) {
    int r = -errno;
    assert(!g_conf->newstore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

// ---------------
// cache

NewStore::CollectionRef NewStore::_get_collection(coll_t cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

NewStore::Onode::Onode(const ghobject_t& o, const string& k)
  : oid(o),
    key(k),
    dirty(false),
    exists(true),
    wal_lock("NewStore::Onode::wal_lock") {
}



// ---------------
// read operations

bool NewStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return false;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return false;
  return true;
}

int NewStore::stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists)
    return -ENOENT;
  st->st_size = o->onode.size;
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int NewStore::read(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t length,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  dout(15) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  bl.clear();
  CollectionRef c = _get_collection(cid);
  if (!c)
    return false;
  RWLock::RLocker l(c->lock);

  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  r = _do_read(o, offset, length, bl, op_flags);

 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_do_read(
    OnodeRef o,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint32_t op_flags)
{
  map<uint64_t,fragment_t>::iterator p;
  int r;
  int fd = -1;
  fid_t cur_fid;

  dout(20) << __func__ << " " << offset << "~" << length << " size "
	   << o->onode.size << dendl;

  if (offset > o->onode.size) {
    r = 0;
    goto out;
  }

  if (offset + length > o->onode.size) {
    length = o->onode.size - offset;
  }

  o->wait_wal();

  r = 0;
  for (p = o->onode.data_map.begin();   // fixme
       length > 0 && p != o->onode.data_map.end();
       ++p) {
    dout(30) << __func__ << " x " << p->first << "~" << p->second.length << dendl;
    if (p->first > offset && p != o->onode.data_map.begin()) {
      --p;
    }
    if (p->first + p->second.length <= offset) {
      dout(30) << __func__ << " skipping " << p->first << "~" << p->second.length
	       << dendl;
      continue;
    }
    if (p->first > offset) {
      unsigned l = p->first - offset;
      dout(30) << __func__ << " zero " << offset << "~" << l << dendl;
      bufferptr bp(l);
      bp.zero();
      bl.append(bp);
    }
    if (p->second.fid != cur_fid) {
      cur_fid = p->second.fid;
      if (fd >= 0) {
	VOID_TEMP_FAILURE_RETRY(::close(fd));
      }
      fd = _open_fid(cur_fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
    }
    unsigned x_off;
    if (p->first < offset) {
      x_off = offset - p->first;
    } else {
      x_off = 0;
    }
    unsigned x_len = MIN(length, p->second.length);
    dout(30) << __func__ << " data " << offset << "~" << x_len
	     << " fid " << cur_fid << " offset " << x_off + p->second.offset
	     << dendl;
    r = ::lseek64(fd, p->second.offset + x_off, SEEK_SET);
    if (r < 0) {
      r = -errno;
      goto out;
    }
    bufferlist t;
    r = t.read_fd(fd, x_len);
    if (r < 0) {
      goto out;
    }
    if ((unsigned)r < x_len) {
      derr << __func__ << "   short read " << r << " < " << x_len
	   << " from " << cur_fid << " offset " << p->second.offset + x_off
	   << dendl;
      r = -EIO;
      goto out;
    }
    bl.claim_append(t);
    offset += x_len;
    length -= x_len;
  }
  if (length > 0 && p == o->onode.data_map.end()) {
    dout(30) << __func__ << " trailing zero " << offset << "~" << length << dendl;
    bufferptr bp(length);
    bp.zero();
    bl.push_back(bp);
  }
  r = bl.length();
 out:
  return r;
}


int NewStore::fiemap(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl)
{
  assert(0);
}

int NewStore::getattr(
  coll_t cid,
  const ghobject_t& oid,
  const char *name,
  bufferptr& value)
{
  dout(15) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;
  string k(name);

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }

  if (!o->onode.attrs.count(k)) {
    r = -ENODATA;
    goto out;
  }
  value = o->onode.attrs[k];
  r = 0;
 out:
  dout(10) << __func__ << " " << cid << " " << oid << " " << name
	   << " = " << r << dendl;
  return r;
}

int NewStore::getattrs(
  coll_t cid,
  const ghobject_t& oid,
  map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r;

  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  aset = o->onode.attrs;
  r = 0;
 out:
  dout(10) << __func__ << " " << cid << " " << oid
	   << " = " << r << dendl;
  return r;
}

int NewStore::list_collections(vector<coll_t>& ls)
{
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p)
    ls.push_back(p->first);
  return 0;
}

bool NewStore::collection_exists(coll_t c)
{
  RWLock::RLocker l(coll_lock);
  return coll_map.count(c);
}

bool NewStore::collection_empty(coll_t cid)
{
  dout(15) << __func__ << " " << cid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  bool r = true;
  pair<ghobject_t,OnodeRef> next;
  while (c->onode_map.get_next(next.first, &next)) {
    if (next.second->exists) {
      r = false;
      break;
    }
  }

  dout(10) << __func__ << " " << cid << " = " << (int)r << dendl;
  return r;
}

int NewStore::collection_list(coll_t cid, vector<ghobject_t>& o)
{
  dout(15) << __func__ << " " << cid << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  pair<ghobject_t,OnodeRef> next;
  while (c->onode_map.get_next(next.first, &next)) {
    if (next.second->exists) {
      o.push_back(next.first);
    }
  }
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

int NewStore::collection_list_partial(
  coll_t cid, ghobject_t start,
  int min, int max, snapid_t snap,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " min/max " << min << "/" << max
	   << " snap " << snap << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  pair<ghobject_t,OnodeRef> next;
  next.first = start;
  while (true) {
    if (!c->onode_map.get_next(next.first, &next)) {
      *pnext = ghobject_t::get_max();
      break;
    }
    if (next.second->exists) {
      ls->push_back(next.first);
      if (ls->size() >= (unsigned)max) {
	*pnext = next.first;
	break;
      }
    }
  }
  dout(10) << __func__ << " " << cid
	   << " start " << start << " min/max " << min << "/" << max
	   << " snap " << snap << " = " << r << dendl;
  return r;
}

int NewStore::collection_list_range(
  coll_t cid, ghobject_t start, ghobject_t end,
  snapid_t seq, vector<ghobject_t> *ls)
{
  dout(15) << __func__ << " " << cid
	   << " start " << start << " end " << end
	   << " snap " << seq << dendl;
  CollectionRef c = _get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  int r = 0;
  pair<ghobject_t,OnodeRef> next;
  next.first = start;
  while (true) {
    if (!c->onode_map.get_next(next.first, &next)) {
      break;
    }
    if (next.second->exists) {
      if (next.first >= end)
	break;
      ls->push_back(next.first);
    }
  }
  dout(10) << __func__ << " " << cid
	   << " start " << start << " end " << end
	   << " snap " << seq << " = " << r << dendl;
  return r;
}

int NewStore::omap_get(
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  map<string, bufferlist> *out /// < [out] Key to value map
  )
{
  assert(0);
}

int NewStore::omap_get_header(
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  bufferlist *header,      ///< [out] omap header
  bool allow_eio ///< [in] don't assert on eio
  )
{
  assert(0);
}

int NewStore::omap_get_keys(
  coll_t cid,              ///< [in] Collection containing oid
  const ghobject_t &oid, ///< [in] Object containing omap
  set<string> *keys      ///< [out] Keys defined on oid
  )
{
  assert(0);
}

int NewStore::omap_get_values(
  coll_t cid,                    ///< [in] Collection containing oid
  const ghobject_t &oid,       ///< [in] Object containing omap
  const set<string> &keys,     ///< [in] Keys to get
  map<string, bufferlist> *out ///< [out] Returned keys and values
  )
{
  assert(0);
}

int NewStore::omap_check_keys(
  coll_t cid,                ///< [in] Collection containing oid
  const ghobject_t &oid,   ///< [in] Object containing omap
  const set<string> &keys, ///< [in] Keys to check
  set<string> *out         ///< [out] Subset of keys defined on oid
  )
{
  assert(0);
}

ObjectMap::ObjectMapIterator NewStore::get_omap_iterator(
  coll_t cid,              ///< [in] collection
  const ghobject_t &oid  ///< [in] object
  )
{
  assert(0);
}


// -----------------
// write helpers

int NewStore::_recover_next_fid()
{
  unsigned i=1;
  while (true) {
    char fn[32];
    snprintf(fn, sizeof(fn), "%u", i);
    struct stat st;
    derr << "frag_fd " << frag_fd << " fn " << fn << dendl;
    int r = ::fstatat(frag_fd, fn, &st, 0);
    if (r == 0) {
      ++i;
      continue;
    }
    r = -errno;
    if (r == -ENOENT)
      break;
    assert(r < 0);
    derr << __func__ << " failed to stat " << path << "/fragments/" << fn
	 << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  fid_cur.fset = i - 1;
  fid_cur.fno = 0;
  dout(10) << __func__ << " stopped somewhere in " << fid_cur << dendl;

  // FIXME: we should probably recover fno too so that we don't create
  // a new fset dir on every daemon restart.
}

int NewStore::_open_fid(fid_t fid)
{
  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int fd = ::openat(frag_fd, fn, O_RDWR);
  if (fd < 0)
    return -errno;
  dout(30) << __func__ << " " << fid << " = " << fd << dendl;
  return fd;
}

int NewStore::_create_fid(fid_t *fid)
{
  {
    Mutex::Locker l(fid_lock);
    if (fid_cur.fset > 0 &&
	fid_cur.fno > 0 &&
	fid_cur.fno < g_conf->newstore_max_dir_size) {
      ++fid_cur.fno;
    } else {
      ++fid_cur.fset;
      fid_cur.fno = 1;
      dout(10) << __func__ << " creating " << fid_cur.fset << dendl;
      char s[32];
      snprintf(s, sizeof(s), "%u", fid_cur.fset);
      int r = ::mkdirat(frag_fd, s, 0755);
      if (r < 0) {
	r = -errno;
	derr << __func__ << " cannot create " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
	return r;
      }
      if (fset_fd >= 0)
	VOID_TEMP_FAILURE_RETRY(::close(fset_fd));
      fset_fd = ::openat(frag_fd, s, O_DIRECTORY, 0644);
      if (fset_fd < 0) {
	r = -errno;
	derr << __func__ << " cannot open created " << path << "/fragments/"
	     << s << ": " << cpp_strerror(r) << dendl;
      }
    }
    *fid = fid_cur;
  }

  dout(10) << __func__ << " " << fid_cur << dendl;
  char s[32];
  snprintf(s, sizeof(s), "%u", fid->fno);
  int fd = ::openat(fset_fd, s, O_RDWR|O_CREAT, 0644);
  if (fd < 0) {
    int r = -errno;
    derr << __func__ << " cannot create " << path << "/fragments/"
	 << *fid << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  // stash ino too for later
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    r = -errno;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return r;
  }
  fid->ino = st.st_ino;
  dout(30) << __func__ << " " << *fid << " = " << fd
	   << " ino " << fid->ino << dendl;
  return fd;
}

int NewStore::_remove_fid(fid_t fid)
{
  char fn[32];
  snprintf(fn, sizeof(fn), "%u/%u", fid.fset, fid.fno);
  int r = ::unlinkat(frag_fd, fn, 0);
  if (r < 0)
    return -errno;
  return 0;
}

NewStore::TransContext *NewStore::_create_txc(OpSequencer *osr)
{
  TransContext *txc = new TransContext;
  txc->t = db->get_transaction();
  dout(20) << __func__ << " osr " << osr << " = " << txc << dendl;
  return txc;
}

int NewStore::_finalize_txc(OpSequencer *osr, TransContextRef& txc)
{
  dout(20) << __func__ << " osr " << osr << " txc " << txc << dendl;

  // finalize onodes
  for (list<OnodeRef>::iterator p = txc->onodes.begin();
       p != txc->onodes.end();
       ++p) {
    bufferlist bl;
    ::encode((*p)->onode, bl);
    txc->t->set(PREFIX_OBJ, (*p)->key, bl);
  }

  // journal wal items
  if (txc->wal_txn) {
    txc->wal_txn->seq = wal_seq.inc();
    bufferlist bl;
    ::encode(*txc->wal_txn, bl);
    txc->t->set(PREFIX_WAL, stringify(txc->wal_txn->seq), bl);
  }

  return 0;
}

wal_op_t *NewStore::_get_wal_op(TransContextRef& txc)
{
  if (!txc->wal_txn) {
    txc->wal_txn = new wal_transaction_t;
  }
  txc->wal_txn->ops.push_back(wal_op_t());
  return &txc->wal_txn->ops.back();
}

int NewStore::_apply_wal_transaction(TransContextRef& txc)
{
  wal_transaction_t& wt = *txc->wal_txn;
  dout(20) << __func__ << " txc " << txc << " seq " << wt.seq << dendl;

  vector<int> sync_fds;
  sync_fds.reserve(wt.ops.size());
  for (list<wal_op_t>::iterator p = wt.ops.begin(); p != wt.ops.end(); ++p) {
    switch (p->op) {
    case wal_op_t::OP_WRITE:
      {
	dout(20) << __func__ << " write " << p->fid << " "
		 << p->offset << "~" << p->length << dendl;
	int fd = _open_fid(p->fid);
	if (fd < 0)
	  return fd;
	int r = ::lseek64(fd, p->offset, SEEK_SET);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " lseek64 on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	p->data.write_fd(fd);
	sync_fds.push_back(fd);
      }
      break;
    case wal_op_t::OP_TRUNCATE:
      {
	dout(20) << __func__ << " truncate " << p->fid << " "
		 << p->offset << dendl;
	int fd = _open_fid(p->fid);
	if (fd < 0)
	  return fd;
	int r = ::ftruncate(fd, p->offset);
	if (r < 0) {
	  r = -errno;
	  derr << __func__ << " truncate on " << fd << " got: "
	       << cpp_strerror(r) << dendl;
	  return r;
	}
	//sync_fds.push_back(fd);  // do we care?
      }
      break;

    case wal_op_t::OP_REMOVE:
      dout(20) << __func__ << " remove " << p->fid << dendl;
      _remove_fid(p->fid);
      break;

    default:
      assert(0 == "unrecognized wal op");
    }
  }

  for (vector<int>::iterator p = sync_fds.begin();
       p != sync_fds.end();
       ++p) {
    int r = ::fsync(*p);
    assert(r == 0);
    VOID_TEMP_FAILURE_RETRY(::close(*p));
  }

  txc->finish_wal_apply();
  return 0;
}

struct C_ApplyWAL : public Context {
  NewStore *store;
  NewStore::TransContextRef txc;
  C_ApplyWAL(NewStore *s, NewStore::TransContextRef& t) : store(s), txc(t) {}
  void finish(int r) {
    store->_apply_wal_transaction(txc);
  }
};

// ---------------------------
// transactions

struct C_FinishRemoveCollections : public Context {
  NewStore *store;
  NewStore::TransContextRef txc;
  C_FinishRemoveCollections(NewStore *s, NewStore::TransContextRef t)
    : store(s), txc(t) {}
  void finish(int r) {
    store->_finish_remove_collections(txc);
  }
};

int NewStore::queue_transactions(
    Sequencer *posr,
    list<Transaction*>& tls,
    TrackedOpRef op,
    ThreadPool::TPHandle *handle)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);
  int r;

  // set up the sequencer
  OpSequencer *osr;
  if (!posr)
    posr = &default_osr;
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p);
    dout(5) << __func__ << " existing " << *osr << "/" << osr->parent << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << __func__ << " new " << *osr << "/" << osr->parent << dendl;
  }

  TransContextRef txc(_create_txc(osr));

  // XXX do it sync for now; this is not crash safe
  for (list<Transaction*>::iterator p = tls.begin(); p != tls.end(); ++p) {
    (*p)->set_osr(osr);
    _do_transaction(*p, txc, handle);
  }

  if (onreadable_sync)
    onreadable_sync->complete(0);
  if (onreadable)
    finisher.queue(onreadable);

  // XXX fixme
  r = _finalize_txc(osr, txc);
  assert(r == 0);

  /// XXX fixme: use aio fsync
  for (list<int>::iterator p = txc->fds.begin(); p != txc->fds.end(); ++p) {
    dout(30) << __func__ << " fsync " << *p << dendl;
    int r = ::fsync(*p);
    if (r < 0) {
      r = -errno;
      derr << __func__ << " fsync: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  // XXX
  db->submit_transaction_sync(txc->t);

  if (ondisk)
    finisher.queue(ondisk);

  if (txc->wal_txn) {
    dout(20) << __func__ << " starting wal apply" << dendl;
    txc->start_wal_apply();
    finisher.queue(new C_ApplyWAL(this, txc));
  } else {
    dout(20) << __func__ << " no wal" << dendl;
  }

  if (!txc->removed_collections.empty()) {
    finisher.queue(new C_FinishRemoveCollections(this, txc));
  }

  return 0;
}

int NewStore::_do_transaction(Transaction *t,
			      TransContextRef& txc,
			      ThreadPool::TPHandle *handle)
{
  Transaction::iterator i = t->begin();
  int pos = 0;

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = _get_collection(*p);
  }

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;
    CollectionRef &c = cvec[op->cid];

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _touch(txc, c, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, oid, off, len, bl, fadvise_flags);
      }
      break;

    case Transaction::OP_ZERO:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        uint64_t off = op->off;
	r = _truncate(txc, c, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
	r = _remove(txc, c, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(txc, c, oid, to_set);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	string name = i.decode_string();
	r = _rmattr(txc, c, oid, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
	r = _rmattrs(txc, c, oid);
      }
      break;

    case Transaction::OP_CLONE:
      {
        const ghobject_t& oid = i.get_oid(op->oid);
        const ghobject_t& noid = i.get_oid(op->dest_oid);
	r = _clone(txc, c, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
        const ghobject_t &oid = i.get_oid(op->oid);
        const ghobject_t &noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	assert(!c);
        coll_t cid = i.get_cid(op->cid);
	r = _create_collection(txc, cid, &c);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          //r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
#warning write me
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _remove_collection(txc, cid, &c);
      }
      break;

    case Transaction::OP_COLL_ADD:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_REMOVE:
      assert(0 == "not implmeented");
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	//r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
#warning writ eme
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      assert(0 == "not implmeneted");
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	//r = _omap_clear(cid, oid);
#warning write me
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferlist> aset;
        i.decode_attrset(aset);
	//r = _omap_setkeys(cid, oid, aset);
#warning write me
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        set<string> keys;
        i.decode_keyset(keys);
	//r = _omap_rmkeys(cid, oid, keys);
#warning write me
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	//r = _omap_rmkeyrange(cid, oid, first, last);
#warning write me
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
	//r = _omap_setheader(cid, oid, bl);
#warning write me
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
	//r = _split_collection(cid, bits, rem, dest);
#warning write me

      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
#warning write me
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }

  return 0;
}



// -----------------
// write operations

int NewStore::_touch(TransContextRef& txc,
		     CollectionRef& c,
		     const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  assert(o);
  o->exists = true;
  txc->write_onode(o);
 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_do_write(TransContextRef txc,
			OnodeRef o,
			uint64_t offset, uint64_t length,
			const bufferlist& bl,
			uint32_t fadvise_flags)
{
  int fd = -1;
  int r = 0;

  dout(20) << __func__ << " have " << o->onode.size
	   << " bytes in " << o->onode.data_map.size()
	   << " fragments" << dendl;

  o->exists = true;

  if (o->onode.size == offset ||
      o->onode.size == 0) {
    if (o->onode.data_map.empty()) {
      // create
      fragment_t &f = o->onode.data_map[0];
      f.offset = 0;
      f.length = offset + length;
      fd = _create_fid(&f.fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      ::lseek64(fd, offset, SEEK_SET);
      dout(20) << __func__ << " create " << f.fid << " writing "
	       << offset << "~" << length << dendl;
    } else {
      // append (possibly with gap)
      assert(o->onode.data_map.size() == 1);
      fragment_t &f = o->onode.data_map.rbegin()->second;
      f.length = (offset + length) - f.offset;
      fd = _open_fid(f.fid);
      if (fd < 0) {
	r = fd;
	goto out;
      }
      ::lseek64(fd, offset - f.offset, SEEK_SET);
      dout(20) << __func__ << " append " << f.fid << " writing "
	       << (offset - f.offset) << "~" << length << dendl;
    }
    o->onode.size = offset + length;
    r = bl.write_fd(fd);
    if (r < 0)
      goto out;
    txc->sync_fd(fd);
  } else {
    // WAL
    assert(o->onode.data_map.size() == 1);
    const fragment_t& f = o->onode.data_map.begin()->second;
    assert(f.offset == 0);
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_WRITE;
    op->offset = offset - f.offset;
    op->length = length;
    op->fid = f.fid;
    op->data = bl;
    if (offset + length > o->onode.size)
      o->onode.size = offset + length;
    dout(20) << __func__ << " wal " << f.fid << " write "
	     << (offset - f.offset) << "~" << length << dendl;
  }
  r = 0;

 out:
  return r;
}


int NewStore::_write(TransContextRef& txc,
		     CollectionRef& c,
		     const ghobject_t& oid,
		     uint64_t offset, size_t length,
		     const bufferlist& bl,
		     uint32_t fadvise_flags)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);

  r = _do_write(txc, o, offset, length, bl, fadvise_flags);
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_zero(TransContextRef& txc,
		    CollectionRef& c,
		    const ghobject_t& oid,
		    uint64_t offset, size_t length)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << dendl;
  int r = 0;

  assert(0 == "write me");

  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset << "~" << length
	   << " = " << r << dendl;
  return r;
}

int NewStore::_truncate(TransContextRef& txc,
			CollectionRef& c,
			const ghobject_t& oid,
			uint64_t offset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, true);
  if (!o->exists) {
    r = -ENOENT;
    goto out;
  }
  if (offset == 0) {
    while (!o->onode.data_map.empty()) {
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = o->onode.data_map.rbegin()->second.fid;
      o->onode.data_map.erase(o->onode.data_map.rbegin()->first);
    }
  } else if (offset < o->onode.size) {
    assert(o->onode.data_map.size() == 1);
    fragment_t& f = o->onode.data_map.begin()->second;
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_TRUNCATE;
    op->offset = offset;
    op->fid = f.fid;
    assert(f.offset == 0);
    f.length = offset;
  } else if (offset > o->onode.size) {
    // resize file up.  make sure we don't have trailing
    // garbage!
    // FIXME !!
  }
  o->onode.size = offset;
  txc->write_onode(o);

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << offset
	   << " = " << r << dendl;
  return r;
}

int NewStore::_remove(TransContextRef& txc,
		      CollectionRef& c,
		      const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r;
  RWLock::WLocker l(c->lock);
  string key;
  OnodeRef o = c->get_onode(oid, true);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->exists = false;
  if (!o->onode.data_map.empty()) {
    for (map<uint64_t,fragment_t>::iterator p = o->onode.data_map.begin();
	 p != o->onode.data_map.end();
	 ++p) {
      wal_op_t *op = _get_wal_op(txc);
      op->op = wal_op_t::OP_REMOVE;
      op->fid = p->second.fid;
    }
  }
  o->onode.data_map.clear();
  o->onode.size = 0;

  get_object_key(oid, &key);
  txc->t->rmkey(PREFIX_OBJ, key);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_setattr(TransContextRef& txc,
		       CollectionRef& c,
		       const ghobject_t& oid,
		       const string& name,
		       bufferptr& val)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs[name] = val;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " (" << val.length() << " bytes)"
	   << " = " << r << dendl;
  return r;
}

int NewStore::_setattrs(TransContextRef& txc,
			CollectionRef& c,
			const ghobject_t& oid,
			const map<string,bufferptr>& aset)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  for (map<string,bufferptr>::const_iterator p = aset.begin();
       p != aset.end(); ++p)
    o->onode.attrs[p->first] = p->second;
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << aset.size() << " keys"
	   << " = " << r << dendl;
  return r;
}


int NewStore::_rmattr(TransContextRef& txc,
		      CollectionRef& c,
		      const ghobject_t& oid,
		      const string& name)
{
  dout(15) << __func__ << " " << c->cid << " " << oid
	   << " " << name << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.erase(name);
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid
	   << " " << name << " = " << r << dendl;
  return r;
}

int NewStore::_rmattrs(TransContextRef& txc,
		       CollectionRef& c,
		       const ghobject_t& oid)
{
  dout(15) << __func__ << " " << c->cid << " " << oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  OnodeRef o = c->get_onode(oid, false);
  if (!o || !o->exists) {
    r = -ENOENT;
    goto out;
  }
  o->onode.attrs.clear();
  txc->write_onode(o);
  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << oid << " = " << r << dendl;
  return r;
}

int NewStore::_clone(TransContextRef& txc,
		     CollectionRef& c,
		     const ghobject_t& old_oid,
		     const ghobject_t& new_oid)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;

  r = _do_read(oldo, 0, oldo->onode.size, bl, 0);
  if (r < 0)
    goto out;

  // truncate any old data
  while (!newo->onode.data_map.empty()) {
    wal_op_t *op = _get_wal_op(txc);
    op->op = wal_op_t::OP_REMOVE;
    op->fid = newo->onode.data_map.rbegin()->second.fid;
    newo->onode.data_map.erase(newo->onode.data_map.rbegin()->first);
  }

  r = _do_write(txc, newo, 0, oldo->onode.size, bl, 0);

  newo->onode.attrs = oldo->onode.attrs;
  // fixme: omap

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " = " << r << dendl;
  return r;
}

int NewStore::_clone_range(TransContextRef& txc,
			   CollectionRef& c,
			   const ghobject_t& old_oid,
			   const ghobject_t& new_oid,
			   uint64_t srcoff, uint64_t length, uint64_t dstoff)
{
  dout(15) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff << dendl;
  int r = 0;

  RWLock::WLocker l(c->lock);
  bufferlist bl;
  OnodeRef newo;
  OnodeRef oldo = c->get_onode(old_oid, false);
  if (!oldo || !oldo->exists) {
    r = -ENOENT;
    goto out;
  }
  newo = c->get_onode(new_oid, true);
  assert(newo);
  newo->exists = true;

  r = _do_read(oldo, srcoff, length, bl, 0);
  if (r < 0)
    goto out;

  r = _do_write(txc, newo, dstoff, bl.length(), bl, 0);

  txc->write_onode(newo);

  r = 0;

 out:
  dout(10) << __func__ << " " << c->cid << " " << old_oid << " -> "
	   << new_oid << " from " << srcoff << "~" << length
	   << " to offset " << dstoff
	   << " = " << r << dendl;
  return r;
}



// collections

int NewStore::_create_collection(
  TransContextRef& txc,
  coll_t cid,
  CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;
  bufferlist empty;

  {
    RWLock::WLocker l(coll_lock);
    if (*c) {
      r = -EEXIST;
      goto out;
    }
    c->reset(new Collection(this, cid));
    coll_map[cid] = *c;
  }
  txc->t->set(PREFIX_COLL, stringify(cid), empty);
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

int NewStore::_remove_collection(TransContextRef& txc, coll_t cid,
				 CollectionRef *c)
{
  dout(15) << __func__ << " " << cid << dendl;
  int r;
  bufferlist empty;

  {
    RWLock::WLocker l(coll_lock);
    if (!*c) {
      r = -ENOENT;
      goto out;
    }
    pair<ghobject_t,OnodeRef> next;
    while ((*c)->onode_map.get_next(next.first, &next)) {
      if (next.second->exists) {
	r = -ENOTEMPTY;
	goto out;
      }
    }
    coll_map.erase(cid);
    txc->removed_collections.push_back(*c);
    c->reset();
  }
  txc->t->rmkey(PREFIX_COLL, stringify(cid));
  r = 0;

 out:
  dout(10) << __func__ << " " << cid << " = " << r << dendl;
  return r;
}

void NewStore::_finish_remove_collections(TransContextRef& txc)
{
  // clear out *my* lingering Onode refs
  txc->onodes.clear();

  for (list<CollectionRef>::iterator p = txc->removed_collections.begin();
       p != txc->removed_collections.end();
       ++p) {
    CollectionRef c = *p;
    dout(10) << __func__ << " " << c->cid << dendl;
    pair<ghobject_t,OnodeRef> next;
    while (c->onode_map.get_next(next.first, &next)) {
      assert(!next.second->exists);
      assert(next.second->unapplied_txns.empty());
    }
    c->onode_map.clear();
    c->onode_map.dump_weak_refs();
  }
}




// ===========================================
