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

#include "newstore_types.h"
#include "common/Formatter.h"

// cnode_t

void cnode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(bits, bl);
  ENCODE_FINISH(bl);
}

void cnode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(bits, p);
  DECODE_FINISH(p);
}

void cnode_t::dump(Formatter *f) const
{
  f->dump_unsigned("bits", bits);
}

void cnode_t::generate_test_instances(list<cnode_t*>& o)
{
  o.push_back(new cnode_t());
  o.push_back(new cnode_t(0));
  o.push_back(new cnode_t(123));
}

// fit_t

void fid_t::dump(Formatter *f) const
{
  f->dump_unsigned("fset", fset);
  f->dump_unsigned("fno", fno);
}

void fid_t::generate_test_instances(list<fid_t*>& o)
{
  o.push_back(new fid_t());
  o.push_back(new fid_t(0, 1));
  o.push_back(new fid_t(123, 3278));
}

// fragment_t

void fragment_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(offset, bl);
  ::encode(length, bl);
  ::encode(fid, bl);
  ENCODE_FINISH(bl);
}

void fragment_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(offset, p);
  ::decode(length, p);
  ::decode(fid, p);
  DECODE_FINISH(p);
}

void fragment_t::dump(Formatter *f) const
{
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
  f->dump_object("fid", fid);
}

void fragment_t::generate_test_instances(list<fragment_t*>& o)
{
  o.push_back(new fragment_t());
  o.push_back(new fragment_t(123, 456));
  o.push_back(new fragment_t(789, 1024, fid_t(3, 400)));
}

// onode_t

void onode_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(size, bl);
  ::encode(attrs, bl);
  ::encode(data_map, bl);
  ::encode(omap_head, bl);
  ::encode(expected_object_size, bl);
  ::encode(expected_write_size, bl);
  ENCODE_FINISH(bl);
}

void onode_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(size, p);
  ::decode(attrs, p);
  ::decode(data_map, p);
  ::decode(omap_head, p);
  ::decode(expected_object_size, p);
  ::decode(expected_write_size, p);
  DECODE_FINISH(p);
}

void onode_t::dump(Formatter *f) const
{
  f->dump_unsigned("size", size);
  f->open_object_section("attrs");
  for (map<string,bufferptr>::const_iterator p = attrs.begin();
       p != attrs.end(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_unsigned("len", p->second.length());
    f->close_section();
  }
  f->open_object_section("data_map");
  for (map<uint64_t, fragment_t>::const_iterator p = data_map.begin();
       p != data_map.end(); ++p) {
    f->open_object_section("fragment");
    f->dump_unsigned("fragment_offset", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("omap_head", omap_head);
  f->dump_unsigned("expected_object_size", expected_object_size);
  f->dump_unsigned("expected_write_size", expected_write_size);
}

void onode_t::generate_test_instances(list<onode_t*>& o)
{
  o.push_back(new onode_t());
  // FIXME
}

// wal_op_t

void wal_op_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(op, bl);
  ::encode(fid, bl);
  ::encode(offset, bl);
  ::encode(length, bl);
  ::encode(data, bl);
  ENCODE_FINISH(bl);
}

void wal_op_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(op, p);
  ::decode(fid, p);
  ::decode(offset, p);
  ::decode(length, p);
  ::decode(data, p);
  DECODE_FINISH(p);
}

void wal_op_t::dump(Formatter *f) const
{
  f->dump_unsigned("op", (int)op);
  f->dump_object("fid", fid);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void wal_transaction_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(seq, bl);
  ::encode(ops, bl);
  ENCODE_FINISH(bl);
}

void wal_transaction_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(seq, p);
  ::decode(ops, p);
  DECODE_FINISH(p);
}

void wal_transaction_t::dump(Formatter *f) const
{
  f->dump_unsigned("seq", seq);
  f->open_array_section("ops");
  for (list<wal_op_t>::const_iterator p = ops.begin(); p != ops.end(); ++p) {
    f->dump_object("op", *p);
  }
  f->close_section();
}
