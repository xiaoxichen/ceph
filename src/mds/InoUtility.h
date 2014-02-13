
#ifndef INO_UTILITY_H_
#define INO_UTILITY_H_

#include "mds/MDSUtility.h"

#include "include/rados/librados.hpp"
#include "include/frag.h"
#include "mds/mdstypes.h"


class inode_backtrace_t;

/**
 * Utility for inspecting inode/directory fragment
 * objects directly from RADOS (requires the objects
 * of interest are flushed to first class objects and
 * are not still waiting in the journal: flush your
 * journal before running this).
 */
class InoUtility : public MDSUtility {
private:
  librados::Rados rados;
  librados::IoCtx md_ioctx;
  librados::IoCtx data_ioctx;

  inode_t root_ino;
  fragtree_t root_fragtree;

  void traverse_fragment(object_t const &fragment);
  void traverse_dir(fragtree_t const &frags, inode_t const &ino);

  std::string backtrace_to_path(inode_backtrace_t const &);
  bool path_match(std::string const &prefix, std::string const &path);

public:
  void by_path(std::string const &path);
  void by_id(inodeno_t const id);
  void dump_path(std::string const &path);
  void forward_scan();

  virtual int init();
  virtual void shutdown();
};

#endif // INO_UTILITY_H_
