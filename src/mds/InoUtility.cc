// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Mutex.h"
#include "common/errno.h"

#include "mds/inode_backtrace.h"
#include "include/encoding.h"
#include "mds/CInode.h"
#include "mds/MDS.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/filesystem/fstream.hpp>

#include "mds/InoUtility.h"

#include "osdc/Journaler.h"

#include <fstream>

#define dout_subsys ceph_subsys_mds

// FIXME: refactor main declaration of this so we don't have
// to duplicate it here.
__u32 hash_dentry_name(inode_t const & inode, const string &dn)
{
  int which = inode.dir_layout.dl_dir_hash;
  if (!which)
    which = CEPH_STR_HASH_LINUX;
  return ceph_str_hash(which, dn.data(), dn.length());
}


int InoUtility::init()
{
  int rc = MDSUtility::init();

  dout(1) << "InodeUtility::init: creating RADOS context" << dendl;
  rados.init("admin");
  // FIXME global conf init handles this for objecter, where shall I get it from?
  rados.conf_read_file("./ceph.conf");
  rados.connect();

  std::string metadata_name;
  std::string data_name;
  rc = rados.pool_reverse_lookup(mdsmap->get_metadata_pool(), &metadata_name);
  assert(rc == 0);
  // FIXME: consider multiple data pools
  rc = rados.pool_reverse_lookup(mdsmap->get_first_data_pool(), &data_name);
  assert(rc == 0);
  rados.ioctx_create(metadata_name.c_str(), md_ioctx);
  rados.ioctx_create(data_name.c_str(), data_ioctx);

  // Populate root_ino and root_fragtree
  dout(1) << "InodeUtility::init: fetching root inode" << dendl;
  object_t root_oid = CInode::get_object_name(MDS_INO_ROOT, frag_t(), ".inode");
  bufferlist root_inode_bl;
  int whole_file = 1 << 22;  // FIXME: magic number, this is what rados.cc uses for default op_size
  md_ioctx.read(root_oid.name, root_inode_bl, whole_file, 0);
  string magic;
  string symlink;
  bufferlist::iterator root_inode_bl_iter = root_inode_bl.begin();
  ::decode(magic, root_inode_bl_iter);
  std::cout << "Magic: " << magic << std::endl;
  if (magic != std::string(CEPH_FS_ONDISK_MAGIC)) {
    dout(0) << "Bad magic '" << magic << "' in root inode" << dendl;
    assert(0);
  }
  // This is the first part of CInode::decode_store, which we can't access
  // without constructing the mdcache that a cinode instance would want.
  DECODE_START_LEGACY_COMPAT_LEN(4, 4, 4, root_inode_bl_iter);
  ::decode(root_ino, root_inode_bl_iter);
  if (root_ino.is_symlink()) {
    ::decode(symlink, root_inode_bl_iter);
  }
  ::decode(root_fragtree, root_inode_bl_iter);
  DECODE_FINISH(root_inode_bl_iter);

  dout(1) << "InodeUtility::init: complete (" << rc << ")" << dendl;
  return rc;
}

void InoUtility::shutdown()
{
  MDSUtility::shutdown();

  rados.shutdown();
}


void InoUtility::by_path(std::string const &path)
{
  std::vector<std::string> dnames;
  boost::split(dnames, path, boost::is_any_of("/"));

  // Inode properties read through the path
  fragtree_t parent_fragtree = root_fragtree;
  inode_t parent_ino = root_ino;

  for (std::vector<std::string>::iterator dn = dnames.begin(); dn != dnames.end(); ++dn) {
    if (dn->empty()) {
      // FIXME get a better split function
      continue;
    }
    // Identify the fragment in the parent which will hold the next dentry+inode
    // Hash a la CInode::hash_dentry_name
    frag_t frag = parent_fragtree[hash_dentry_name(parent_ino, *dn)];
    object_t frag_object_id = CInode::get_object_name(parent_ino.ino, frag, "");
    dout(4) << "frag_object_id: " << frag_object_id << dendl;

    // Retrieve the next dentry from the parent directory fragment
    bufferlist inode_object_data;
    std::map<std::string, bufferlist> omap_out;
    // FIXME: support snaps other than head
    std::string dentry_name = *dn + std::string("_head");
    // FIXME: relying on prefix of dentry name to be unique, can this be
    // broken by snaps with underscores in name perhaps like 123_head vs 123_headusersnap?  or
    // is the snap postfix an ID?
    md_ioctx.omap_get_vals(
      frag_object_id.name,
      "", dentry_name, 11, &omap_out);
    if (omap_out.size() != 1) {
      // TODO: report nicely when there is an inconsistency like this
      dout(0) << "Missing dentry '" << dentry_name << "' in object '" << frag_object_id.name << "'" << dendl;
      assert(0);
    }
    if (omap_out.find(dentry_name) == omap_out.end()) {
      // TODO: report nicely when there is an inconsistency like this
      dout(0) << "Missing dentry '" << dentry_name << "' in object '" << frag_object_id.name << "'" << dendl;
      assert(0);
    } else {
      dout(4) << "Read dentry " << omap_out[dentry_name].length() << " bytes" << dendl;
    }
    std::cout << "Read dentry '" << dentry_name << "' from fragment '" << frag_object_id << "' of parent '" << parent_ino.ino << "'" << std::endl;

    // Decode the dentry, as encoded by CDir::_encode_dentry
    bufferlist &next_dentry_bl = omap_out[dentry_name];
    char type;
    bufferlist::iterator next_dentry_bl_iter = next_dentry_bl.begin();
    snapid_t dentry_snapid;
    ::decode(dentry_snapid, next_dentry_bl_iter);
    ::decode(type, next_dentry_bl_iter);
    if (type != 'I') {
      dout(0) << "Unknown type '" << type << "'" << dendl;
      assert(type == 'I'); // TODO: handle is_remote branch
    }
    ::decode(parent_ino, next_dentry_bl_iter);
    dout(10) << "next_ino.ino: " << parent_ino.ino << dendl;
    if (parent_ino.is_symlink()) {
      string symlink;
      ::decode(symlink, next_dentry_bl_iter);
    }
    ::decode(parent_fragtree, next_dentry_bl_iter);

    // Print the inode
    JSONFormatter jf(true);
    jf.open_object_section("inode");
    parent_ino.dump(&jf);
    jf.close_section();
    jf.flush(std::cout);
    std::cout << std::endl;
  }
  std::cout << "Path '" << path << "' is inode " << std::hex << parent_ino.ino << std::dec << std::endl;
}


std::string InoUtility::backtrace_to_path(inode_backtrace_t const &backtrace)
{
  std::string path;
  std::vector<inode_backpointer_t>::const_reverse_iterator i;
  for (i = backtrace.ancestors.rbegin(); i != backtrace.ancestors.rend(); ++i) {
      path += "/";
      path += i->dname;
  }
  return path;
}


/**
 * Use backtrace data to locate an inode by ID
 *
 * You'd think you could just read the inode_backpointer_t and resolve
 * the inode, but you'd be wrong: to load the inode we need to know
 * the fragtree of the parent, and for which we have to load the parent's
 * inode, and by induction all the ancestor inodes up to the root.  So
 * this method does a full traversal and validates that the dentries
 * match up with the backtrace in the process.
 */
void InoUtility::by_id(inodeno_t const id)
{
  dout(4) << __func__ << dendl;

  // Read backtrace object.  Because we don't know ahead of time
  // whether this is a directory, we must look in both the default
  // data pool (non-directories) and the metadata pool (directories)
  bufferlist parent_bl;
  object_t oid = CInode::get_object_name(id, frag_t(), "");
  int rc = md_ioctx.getxattr(oid.name, "parent", parent_bl);
  if (rc < 0) {
    dout(4) << "Backtrace for '" << id << "' not found in metadata pool" << dendl;
    int rc = data_ioctx.getxattr(oid.name, "parent", parent_bl);
    if (rc < 0) {
      dout(0) << "No backtrace found for inode '" << id << "'" << dendl;
      return;
    }
  }

  // We got the backtrace data, decode it.
  inode_backtrace_t backtrace;
  ::decode(backtrace, parent_bl);
  JSONFormatter jf(true);
  jf.open_object_section("backtrace");
  backtrace.dump(&jf);
  jf.close_section();
  jf.flush(std::cout);
  std::cout << std::endl;

  // Print the path we're going to traverse
  std::cout << "Backtrace path for inode '" << id << "' : '" << backtrace_to_path(backtrace) << "'" << std::endl;

  // Inode properties read through the backtrace
  fragtree_t parent_fragtree = root_fragtree;
  inode_t parent_ino = root_ino;

  std::vector<inode_backpointer_t>::reverse_iterator i;
  for (i = backtrace.ancestors.rbegin(); i != backtrace.ancestors.rend(); ++i) {
    // TODO: validate that the backtrace's dirino value matches up
    // with the ino that we are finding by resolving dentrys by name

    // Identify the fragment in the parent which will hold the next dentry+inode
    // Hash a la CInode::hash_dentry_name
    frag_t frag = parent_fragtree[hash_dentry_name(parent_ino, i->dname)];
    object_t frag_object_id = CInode::get_object_name(parent_ino.ino, frag, "");
    dout(4) << "frag_object_id: " << frag_object_id << dendl;

    // Retrieve the next dentry from the parent directory fragment
    bufferlist inode_object_data;
    std::map<std::string, bufferlist> omap_out;
    // FIXME: support snaps other than head
    std::string dentry_name = i->dname + std::string("_head");
    // FIXME: relying on prefix of dentry name to be unique, can this be
    // broken by snaps with underscores in name perhaps like 123_head vs 123_headusersnap?  or
    // is the snap postfix an ID?
    md_ioctx.omap_get_vals(
	frag_object_id.name,
	"", dentry_name, 11, &omap_out);
    if (omap_out.size() != 1) {
      // TODO: report nicely when there is an inconsistency like this
      assert(0);
    }
    if (omap_out.find(dentry_name) == omap_out.end()) {
      // TODO: report nicely when there is an inconsistency like this
      dout(0) << "Missing dentry '" << dentry_name << "' in object '" << frag_object_id.name << "'" << dendl;
      assert(0);
    } else {
      dout(4) << "Read dentry " << omap_out[dentry_name].length() << " bytes" << dendl;
    }
    std::cout << "Read dentry '" << dentry_name << "' from fragment '" << frag_object_id << "' of parent '" << parent_ino.ino << "'" << std::endl;

    // Decode the dentry, as encoded by CDir::_encode_dentry
    bufferlist &next_dentry_bl = omap_out[dentry_name];
    char type;
    bufferlist::iterator next_dentry_bl_iter = next_dentry_bl.begin();
    snapid_t dentry_snapid;
    ::decode(dentry_snapid, next_dentry_bl_iter);
    ::decode(type, next_dentry_bl_iter);
    if (type != 'I') {
      dout(0) << "Unknown type '" << type << "'" << dendl;
      assert(type == 'I'); // TODO: handle is_remote branch
    }
    ::decode(parent_ino, next_dentry_bl_iter);
    dout(10) << "next_ino.ino: " << parent_ino.ino << dendl;
    if (parent_ino.is_symlink()) {
      string symlink;
      ::decode(symlink, next_dentry_bl_iter);
    }
    ::decode(parent_fragtree, next_dentry_bl_iter);

    // Print the inode
    JSONFormatter jf(true);
    jf.open_object_section("inode");
    parent_ino.dump(&jf);
    jf.close_section();
    jf.flush(std::cout);
    std::cout << std::endl;
  }
}


bool InoUtility::path_match(std::string const &prefix, std::string const &path)
{
    if (path.length() < prefix.length()) {
        return false;
    }

    return prefix.compare(0, prefix.length(), path, 0, prefix.length()) == 0;
}


/**
 * Using only the data pool, output any files
 * whose path matches this
 */
void InoUtility::dump_path(std::string const &search_path)
{
    dout(4) << "InoUtility::dump_path: " << search_path << dendl;

#if 0
    int const rank = 0
    inodeno_t ino = MDS_INO_LOG_OFFSET + rank;
    Journaler *journaler = new Journaler(ino,
            mdsmap->get_metadata_pool(),
            CEPH_FS_ONDISK_MAGIC,
            objecter, 0, 0, &timer);

    Mutex mylock("InoUtility::trim_flush");
    Cond cond;
    bool done;
    int r;

    lock.Lock();
    journaler->recover(new C_SafeCond(&mylock, &cond, &done, &r));
    lock.Unlock();

    mylock.Lock();
    while (!done)
        cond.Wait(mylock);
    mylock.Unlock();

    if (r != 0) {
        derr << "Missing journal header for rank " << rank << dendl;
    }

    MDS *mds = new MDS(g_conf->name.get_id().c_str(), messenger, monc);
    bool got_data = true;
    lock.Lock();
    // Until the journal is empty, pop an event or wait for one to
    // be available.
    while (journaler->get_read_pos() != journaler->get_write_pos()) {
        bufferlist entry_bl;
        got_data = journaler->try_read_entry(entry_bl);
        dout(10) << "try_read_entry: " << got_data << dendl;
        if (got_data) {
            LogEvent *le = LogEvent::decode(entry_bl);
            if (!le) {
                dout(0) << "Error decoding LogEvent" << dendl;
                break;
            } else {
                le->
                delete le;
            }
        } else {
            bool done = false;
            Cond cond;

            journaler->wait_for_readable(new C_SafeCond(&localLock, &cond, &done));
            lock.Unlock();
            localLock.Lock();
            while (!done)
                cond.Wait(localLock);
            localLock.Unlock();
            lock.Lock();
        }
    }
    lock.Unlock();
#endif


    librados::ObjectIterator i = data_ioctx.objects_begin();
    for (; i != data_ioctx.objects_end(); ++i) {
        std::string const &oid = i->first;
        std::string const &oloc = i->second;
        bufferlist parent_bl;
        data_ioctx.locator_set_key(oloc);
        int rc = data_ioctx.getxattr(oid, "parent", parent_bl);
        if (rc < 0) {
            dout(4) << "No backtrace on '" << oid << "' (" << cpp_strerror(rc) << ")" << dendl;
            continue;
        }

        // Parse object ID into ino and offset
        std::string ino_str = oid.substr(0, oid.find("."));
        inodeno_t oid_ino = strtoll(ino_str.c_str(), NULL, 16);
        std::string offset_str = oid.substr(oid.find(".") + 1);
        uint64_t offset = strtoll(offset_str.c_str(), NULL, 16);
        if (offset != 0) {
            dout(10) << "Ignoring non-zero object " << oid << dendl;
        }

#if 0
        char data_oid[60];
        snprintf(
                data_oid,
                sizeof(data_oid),
                "%llx.%08llx",
                (unsigned long long)oid_ino, (unsigned long long)offset);
#endif

        // We got the backtrace data, decode it.
        inode_backtrace_t backtrace;
        ::decode(backtrace, parent_bl);
        std::string path = backtrace_to_path(backtrace);

        if (path_match(search_path, path)) {
            dout(4) << oid << ": " << path << dendl;
        }



        inodeno_t const ino = backtrace.ino;
        if (ino != oid_ino) {
            derr << "Bogus backtrace " << ino << " vs. " << oid_ino << " on oid " << oid << dendl;
            continue;
        }

        // Read out the file
        // In the absence of metadata, we do not know the file layout, so read the objects
        // one by one and see what we find.
        // Assume that individual objects have sane size and comfortably fit in RAM.
        //

        // Chop off the leading slash (let's not write out to our own root!)
        std::string dump_path = std::string("dump/") + path.substr(1);
        dout(10) << "dumping to " << dump_path << dendl;
        dout(10) << "creating dir " << boost::filesystem::path(dump_path).parent_path() << dendl;
        boost::filesystem::create_directories(boost::filesystem::path(dump_path).parent_path());

        boost::filesystem::ofstream dump_file(dump_path, std::ofstream::out | std::ofstream::binary);

        // Assume contiguous objects
        bool found = true;
        assert(offset == 0);
        while(found) {
            char data_oid[60];
            snprintf(
                    data_oid,
                    sizeof(data_oid),
                    "%llx.%08llx",
                    (unsigned long long)ino, (unsigned long long)offset);

            bufferlist bl;
            dout(10) << "Reading OID " << data_oid << dendl;
            int r = data_ioctx.read(data_oid, bl, INT_MAX, 0);
            if (r == -ENOENT) {
                found = false;
                continue;
            } else if (r < 0) {
                derr << "Failed to read offset 0x" << std::hex << offset << std::dec << dendl;
                found = false;
                continue;
            }
            offset += bl.length();
            if (bl.length() == 0) {
                found = false;
                continue;
            } else {
                bl.write_stream(dump_file);
            }
        }
        dump_file.close();
        dout(4) << "Read " << offset << " bytes for " << path << dendl;
    }
}


/**
 * Given the ID of a directory fragment, iterate
 * over the dentrys in the fragment and call traverse_dentry
 * for each one.
 */
void InoUtility::traverse_fragment(object_t const &fragment)
{
  std::string start_after;

  std::map<std::string, bufferlist> dname_to_dentry;
  do {
    dname_to_dentry.clear();
    int rc = md_ioctx.omap_get_vals(fragment.name, start_after, "", 1024, &dname_to_dentry);
    if (rc != 0) {
      std::cout << "Error reading fragment object '" << fragment.name << "'" << std::endl;
      return;
    }

    for(std::map<std::string, bufferlist>::iterator i = dname_to_dentry.begin();
	i != dname_to_dentry.end(); ++i) {
      std::string const &dname = i->first;
      bufferlist &dentry_data = i->second;

      inode_t inode;
      fragtree_t fragtree;

      bufferlist::iterator dentry_iter = dentry_data.begin();
      snapid_t dentry_snapid;
      ::decode(dentry_snapid, dentry_iter);
      char type;
      ::decode(type, dentry_iter);
      if (type != 'I') {
        dout(0) << "Unknown type '" << type << "'" << dendl;
        assert(type == 'I'); // TODO: handle is_remote branch
      }
      ::decode(inode, dentry_iter);
      if (inode.is_symlink()) {
        string symlink;
        ::decode(symlink, dentry_iter);
      }
      ::decode(fragtree, dentry_iter);

      if (inode.is_dir()) {
          std::cout << "Directory '" << dname << "' " << std::hex << inode.ino << std::dec << std::endl;
          traverse_dir(fragtree, inode);
      } else {
          std::cout << "File '" << dname << "' " << std::hex << inode.ino << std::dec << std::endl;
      }

      start_after = dname;
    }
  } while (dname_to_dentry.size() != 0);
}


/**
 * Given a directory inode and its fragment tree, iterate over
 * the fragments and call traverse_fragment for each one.
 */
void InoUtility::traverse_dir(fragtree_t const &frags, inode_t const &ino)
{
  std::list<frag_t> frag_leaves;
  frags.get_leaves(frag_leaves);

  for(std::list<frag_t>::iterator i = frag_leaves.begin(); i != frag_leaves.end(); ++i) {
    object_t frag_obj = CInode::get_object_name(ino.ino, *i, "");
    traverse_fragment(frag_obj);
  }
}


void InoUtility::forward_scan()
{
  traverse_dir(root_fragtree, root_ino);
}

