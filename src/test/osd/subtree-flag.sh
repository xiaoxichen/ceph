#!/bin/bash
#
# Copyright (C) 2015 Intel <contact@intel.com.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Xiaoxi Chen <xiaoxi.chen@intel.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source ../qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7109" # git grep '\<7108\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function markdown_N_impl() {
  markdown_times=$1
  total_time=$2
  interval=$(($total_time / markdown_times))
  sleep 10
  for i in `seq 1 $markdown_times`
  do
    # check the OSD is UP
    ./ceph osd tree
    ./ceph osd tree | grep osd.0 |grep up || return 1
    # mark the OSD down.
    ./ceph osd down 0
    sleep $interval
  done
}


function TEST_markdown_exceed_maxdown_count() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ./ceph osd tree
    sleep 3
    ./ceph osd set-subtree localhost nodown
    echo "hello"
    # down N+1 times ,the osd.0 shoud die
#    ./ceph osd tree | grep down | grep osd.0 || return 1
}

main subtree-flag "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/osd/osd-bench.sh"
# End:

