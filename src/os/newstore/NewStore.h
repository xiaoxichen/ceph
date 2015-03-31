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

#ifndef CEPH_OSD_NEWSTORE_H
#define CEPH_OSD_NEWSTORE_H

#include <unistd.h>

#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "os/ObjectStore.h"
#include "os/KeyValueDB.h"

#include "newstore_types.h"

#include "boost/intrusive/list.hpp"

class NewStore : public ObjectStore {
  // -----------------------------------------------------
  // types
public:

  struct FragmentHandle {
    int fd;
    FragmentHandle() : fd(-1) {}
    FragmentHandle(int f) : fd(f) {}
    ~FragmentHandle() {
      if (fd >= 0)
	::close(fd);
    }
    int fsync() {
      return ::fsync(fd);
    }
    int fdatasync() {
      return ::fdatasync(fd);
    }
  };
  typedef ceph::shared_ptr<FragmentHandle> FragmentHandleRef;

  class TransContext;

  /// an in-memory object
  struct Onode {
    ghobject_t oid;
    string key;     ///< key under PREFIX_OBJ where we are stored
    boost::intrusive::list_member_hook<> lru_item;

    onode_t onode;  ///< metadata stored as value in kv store
    bool dirty;     // ???
    bool exists;

    Mutex flush_lock;  ///< protect unappliex_txns, num_fsyncs
    Cond flush_cond;   ///< wait here for unapplied txns, fsyncs
    set<TransContext*> flush_txns;   ///< fsyncing or committing or wal txns

    Onode(const ghobject_t& o, const string& k);

    void flush() {
      Mutex::Locker l(flush_lock);
      while (!flush_txns.empty())
	flush_cond.Wait(flush_lock);
    }
  };
  typedef ceph::shared_ptr<Onode> OnodeRef;

  struct OnodeHashLRU {
    typedef boost::intrusive::list<
      Onode,
      boost::intrusive::member_hook<
        Onode,
	boost::intrusive::list_member_hook<>,
	&Onode::lru_item> > lru_list_t;

    Mutex lock;
    ceph::unordered_map<ghobject_t,OnodeRef> onode_map;  ///< forward lookups
    lru_list_t lru;                                      ///< lru

    OnodeHashLRU() : lock("NewStore::OnodeHashLRU::lock") {}

    void add(const ghobject_t& oid, OnodeRef o);
    void _touch(OnodeRef o);
    OnodeRef lookup(const ghobject_t& o);
    void remove(const ghobject_t& o);
    void rename(const ghobject_t& old_oid, const ghobject_t& new_oid);
    void clear();
    bool get_next(const ghobject_t& after, pair<ghobject_t,OnodeRef> *next);
  };

  struct Collection {
    NewStore *store;
    coll_t cid;
    cnode_t cnode;
    RWLock lock;

    // cache onodes on a per-collection basis to avoid lock
    // contention.
    OnodeHashLRU onode_map;

    OnodeRef get_onode(const ghobject_t& oid, bool create);

    Collection(NewStore *ns, coll_t c);
  };
  typedef ceph::shared_ptr<Collection> CollectionRef;

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    OnodeRef o;
    //map<string,bufferlist>::iterator it;
  public:
    OmapIteratorImpl(CollectionRef c, OnodeRef o)
      : c(c), o(o) /*, it(o->omap.begin())*/ {}

    int seek_to_first() {
      RWLock::RLocker l(c->lock);
      //it = o->omap.begin();
      return 0;
    }
    int upper_bound(const string &after) {
      RWLock::RLocker l(c->lock);
      //it = o->omap.upper_bound(after);
      return 0;
    }
    int lower_bound(const string &to) {
      RWLock::RLocker l(c->lock);
      //it = o->omap.lower_bound(to);
      return 0;
    }
    bool valid() {
      RWLock::RLocker l(c->lock);
      return false; //return it != o->omap.end();
    }
    int next() {
      RWLock::RLocker l(c->lock);
      //++it;
      return 0;
    }
    string key() {
      RWLock::RLocker l(c->lock);
      return string(); //return it->first;
    }
    bufferlist value() {
      RWLock::RLocker l(c->lock);
      return bufferlist(); //return it->second;
    }
    int status() {
      return 0;
    }
  };

  class OpSequencer;

  struct TransContext {
    typedef enum {
      STATE_PREPARE,
      STATE_FSYNC_QUEUED,
      STATE_FSYNC_FSYNCING,
      STATE_FSYNC_DONE,
      STATE_KV_QUEUED,
      STATE_KV_COMMITTING,
      STATE_KV_DONE,
      STATE_WAL_QUEUED,
      STATE_WAL_APPLYING,
      STATE_WAL_DONE,
      STATE_FINISHING,
      STATE_DONE,
    } state_t;

    state_t state;

    const char *get_state_name() {
      switch (state) {
      case STATE_PREPARE: return "prepare";
      case STATE_FSYNC_QUEUED: return "fsync_queued";
      case STATE_FSYNC_FSYNCING: return "fsync_fsyncing";
      case STATE_FSYNC_DONE: return "fsync_done";
      case STATE_KV_QUEUED: return "kv_queued";
      case STATE_KV_COMMITTING: return "kv_committing";
      case STATE_KV_DONE: return "kv_done";
      case STATE_WAL_QUEUED: return "wal_queued";
      case STATE_WAL_APPLYING: return "wal_applying";
      case STATE_WAL_DONE: return "wal_done";
      case STATE_FINISHING: return "finishing";
      case STATE_DONE: return "done";
      }
      return "???";
    }

    OpSequencer *osr;
    boost::intrusive::list_member_hook<> sequencer_item;

    list<int> fds;             ///< these fds need to be synced
    set<OnodeRef> onodes;     ///< these onodes need to be updated/written
    KeyValueDB::Transaction t; ///< then we will commit this
    Context *oncommit;         ///< signal on commit
    list<Context*> oncommits;  ///< more commit completions
    list<CollectionRef> removed_collections; ///< colls we removed

    wal_transaction_t *wal_txn; ///< wal transaction (if any)
    unsigned num_fsyncs_completed;

    Mutex lock;
    Cond cond;

    TransContext(OpSequencer *o)
      : state(STATE_PREPARE),
	osr(o),
	wal_txn(NULL),
	num_fsyncs_completed(0),
	lock("NewStore::TransContext::lock") {
      //cout << "txc new " << this << std::endl;
    }
    ~TransContext() {
      delete wal_txn;
      //cout << "txc del " << this << std::endl;
    }

    void sync_fd(int f) {
      fds.push_back(f);
    }
    void write_onode(OnodeRef &o) {
      onodes.insert(o);
    }

    bool finish_fsync() {
      Mutex::Locker l(lock);
      ++num_fsyncs_completed;
      if (num_fsyncs_completed == fds.size()) {
	cond.Signal();
	return true;
      }
      return false;
    }
    void wait_fsync() {
      Mutex::Locker l(lock);
      while (num_fsyncs_completed < fds.size())
	cond.Wait(lock);
    }
  };


  class OpSequencer : public Sequencer_impl {
  public:
    Mutex qlock;
    Cond qcond;
    typedef boost::intrusive::list<
      TransContext,
      boost::intrusive::member_hook<
        TransContext,
	boost::intrusive::list_member_hook<>,
	&TransContext::sequencer_item> > q_list_t;
    q_list_t q;  ///< transactions

    Sequencer *parent;

    OpSequencer()
      : qlock("NewStore::OpSequencer::qlock", false, false),
	parent(NULL) {
    }
    ~OpSequencer() {
      assert(q.empty());
    }

    void queue_new(TransContext *txc) {
      Mutex::Locker l(qlock);
      q.push_back(*txc);
    }

    void flush() {
      Mutex::Locker l(qlock);
      while (!q.empty())
	qcond.Wait(qlock);
    }

    bool flush_commit(Context *c) {
      Mutex::Locker l(qlock);
      if (q.empty()) {
	delete c;
	return true;
      }
      TransContext *txc = &q.back();
      if (txc->state > TransContext::STATE_KV_DONE) {
	delete c;
	return true;
      }
      assert(txc->state <= TransContext::STATE_KV_DONE);
      txc->oncommits.push_back(c);
      return false;
    }
  };

  struct fsync_item {
    int fd;
    TransContext *txc;
    fsync_item(int f, TransContext *t) : fd(f), txc(t) {}
  };
  class FsyncWQ : public ThreadPool::WorkQueue<fsync_item> {
    NewStore *store;
    deque<fsync_item*> fd_queue;

  public:
    FsyncWQ(NewStore *s, time_t ti, time_t sti, ThreadPool *tp)
      : ThreadPool::WorkQueue<fsync_item>("NewStore::FsyncWQ", ti, sti, tp),
	store(s) {
    }
    bool _empty() {
      return fd_queue.empty();
    }
    bool _enqueue(fsync_item *i) {
      fd_queue.push_back(i);
      return true;
    }
    void _dequeue(fsync_item *p) {
      assert(0 == "not needed, not implemented");
    }
    fsync_item *_dequeue() {
      if (fd_queue.empty())
	return NULL;
      fsync_item *i = fd_queue.front();
      fd_queue.pop_front();
      return i;
    }
    void _process(fsync_item *i, ThreadPool::TPHandle &handle) {
      store->_txc_process_fsync(i);
    }
    void _clear() {
      while (!fd_queue.empty()) {
	delete fd_queue.front();
	fd_queue.pop_front();
      }
    }

    void flush() {
      lock();
      while (!fd_queue.empty())
	_wait();
      unlock();
      drain();
    }
  };

  struct KVSyncThread : public Thread {
    NewStore *store;
    KVSyncThread(NewStore *s) : store(s) {}
    void *entry() {
      store->_kv_sync_thread();
      return NULL;
    }
  };

  // --------------------------------------------------------
  // members
private:
  CephContext *cct;
  KeyValueDB *db;
  uuid_d fsid;
  int path_fd;  ///< open handle to $path
  int fsid_fd;  ///< open handle (locked) to $path/fsid
  int frag_fd;  ///< open handle to $path/fragments
  int fset_fd;  ///< open handle to $path/fragments/$cur_fid.fset
  bool mounted;

  RWLock coll_lock;    ///< rwlock to protect coll_map
  ceph::unordered_map<coll_t, CollectionRef> coll_map;

  Mutex fid_lock;
  fid_t fid_cur;

  atomic64_t omap_id;

  Mutex wal_lock;
  atomic64_t wal_seq;

  Finisher finisher;
  ThreadPool fsync_tp;
  FsyncWQ fsync_wq;

  KVSyncThread kv_sync_thread;
  Mutex kv_lock;
  Cond kv_cond, kv_sync_cond;
  bool kv_stop;
  deque<TransContext*> kv_queue, kv_committing;

  Logger *logger;

  Sequencer default_osr;

  Mutex reap_lock;
  Cond reap_cond;
  list<CollectionRef> removed_collections;


  // --------------------------------------------------------
  // private methods

  void _init_logger();
  void _shutdown_logger();

  int _open_path();
  void _close_path();
  int _open_fsid(bool create);
  int _lock_fsid();
  int _read_fsid(uuid_d *f);
  int _write_fsid();
  void _close_fsid();
  int _open_frag();
  int _create_frag();
  void _close_frag();
  int _open_db();
  void _close_db();
  int _open_collections();
  void _close_collections();

  CollectionRef _get_collection(coll_t cid);
  void _queue_reap_collection(CollectionRef& c);
  void _reap_collections();

  int _recover_next_fid();
  int _create_fid(fid_t *fid);
  int _open_fid(fid_t fid);
  int _remove_fid(fid_t fid);

  int _recover_next_omap_id();
  void _get_omap_id(TransContext *txc, OnodeRef o);

  int _clean_fid_tail(TransContext *txc, const fragment_t& f);

  TransContext *_txc_create(OpSequencer *osr);
  int _txc_finalize(OpSequencer *osr, TransContext *txc);
  void _txc_queue_fsync(TransContext *txc);
  void _txc_process_fsync(fsync_item *i);
  void _txc_finish_fsync(TransContext *txc);
  void _txc_submit_kv(TransContext *txc);
  void _txc_finish_kv(TransContext *txc);
  void _txc_finish_apply(TransContext *txc);

  void _osr_reap_done(OpSequencer *osr);

  void _kv_sync_thread();
  void _kv_stop() {
    {
      Mutex::Locker l(kv_lock);
      kv_stop = true;
      kv_cond.Signal();
    }
    kv_sync_thread.join();
  }

  wal_op_t *_get_wal_op(TransContext *txc);
  int _apply_wal_transaction(TransContext *txc);
  void _wait_object_wal(OnodeRef onode);
  friend class C_ApplyWAL;

public:
  NewStore(CephContext *cct, const string& path);
  ~NewStore();

  bool need_journal() { return true; };
  int peek_journal_fsid(uuid_d *fsid);

  bool test_mount_in_use();

  int mount();
  int umount();

  void sync(Context *onsync);
  void sync();
  void flush();
  void sync_and_flush();

  unsigned get_max_object_name_length() {
    return 4096;
  }
  unsigned get_max_attr_name_length() {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs();
  int mkjournal() {
    return 0;
  }

private:
  bool sharded;
public:
  void set_allow_sharded_objects() {
    sharded = true;
  }
  bool get_allow_sharded_objects() {
    return sharded;
  }

  int statfs(struct statfs *buf);

  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false);
  int _do_read(
    OnodeRef o,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0);

  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t cid, vector<ghobject_t>& o);
  int collection_list_partial(coll_t cid, ghobject_t start,
			      int min, int max, snapid_t snap,
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t cid, ghobject_t start, ghobject_t end,
			    snapid_t seq, vector<ghobject_t> *ls);

  int omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    coll_t cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u) {
    fsid = u;
  }
  uuid_d get_fsid() {
    return fsid;
  }

  objectstore_perf_stat_t get_cur_stats() {
    return objectstore_perf_stat_t();
  }

  int queue_transactions(
    Sequencer *osr,
    list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);

private:
  // --------------------------------------------------------
  // write ops

  int _do_transaction(Transaction *t,
		      TransContext *txc,
		      ThreadPool::TPHandle *handle);

  int _write(TransContext *txc,
	     CollectionRef& c,
	     const ghobject_t& oid,
	     uint64_t offset, size_t len,
	     const bufferlist& bl,
	     uint32_t fadvise_flags);
  int _do_write(TransContext *txc,
		OnodeRef o,
		uint64_t offset, uint64_t length,
		const bufferlist& bl,
		uint32_t fadvise_flags);
  int _touch(TransContext *txc,
	     CollectionRef& c,
	     const ghobject_t& oid);
  int _zero(TransContext *txc,
	    CollectionRef& c,
	    const ghobject_t& oid,
	    uint64_t offset, size_t len);
  int _truncate(TransContext *txc,
		CollectionRef& c,
		const ghobject_t& oid,
		uint64_t offset);
  int _remove(TransContext *txc,
	      CollectionRef& c,
	      const ghobject_t& oid);
  int _do_remove(TransContext *txc,
		 OnodeRef o);
  int _setattr(TransContext *txc,
	       CollectionRef& c,
	       const ghobject_t& oid,
	       const string& name,
	       bufferptr& val);
  int _setattrs(TransContext *txc,
		CollectionRef& c,
		const ghobject_t& oid,
		const map<string,bufferptr>& aset);
  int _rmattr(TransContext *txc,
	      CollectionRef& c,
	      const ghobject_t& oid,
	      const string& name);
  int _rmattrs(TransContext *txc,
	       CollectionRef& c,
	       const ghobject_t& oid);
  void _do_omap_clear(TransContext *txc, uint64_t id, bool remove_tail);
  int _omap_clear(TransContext *txc,
		  CollectionRef& c,
		  const ghobject_t& oid);
  int _omap_setkeys(TransContext *txc,
		    CollectionRef& c,
		    const ghobject_t& oid,
		    const map<string,bufferlist>& m);
  int _omap_setheader(TransContext *txc,
		      CollectionRef& c,
		      const ghobject_t& oid,
		      bufferlist& header);
  int _omap_rmkeys(TransContext *txc,
		   CollectionRef& c,
		   const ghobject_t& oid,
		   const set<string>& m);
  int _omap_rmkey_range(TransContext *txc,
			CollectionRef& c,
			const ghobject_t& oid,
			const string& first, const string& last);
  int _clone(TransContext *txc,
	     CollectionRef& c,
	     const ghobject_t& old_oid,
	     const ghobject_t& new_oid);
  int _clone_range(TransContext *txc,
		   CollectionRef& c,
		   const ghobject_t& old_oid,
		   const ghobject_t& new_oid,
		   uint64_t srcoff, uint64_t length, uint64_t dstoff);
  int _rename(TransContext *txc,
	      CollectionRef& c,
	      const ghobject_t& old_oid,
	      const ghobject_t& new_oid);
  int _create_collection(TransContext *txc, coll_t cid, unsigned bits,
			 CollectionRef *c);
  int _remove_collection(TransContext *txc, coll_t cid, CollectionRef *c);
  int _split_collection(TransContext *txc,
			CollectionRef& c,
			CollectionRef& d,
			unsigned bits, int rem);

};

inline ostream& operator<<(ostream& out, const NewStore::OpSequencer& s) {
  return out << *s.parent;
}

#endif
