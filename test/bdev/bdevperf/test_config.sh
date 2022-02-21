#!/usr/bin/env bash

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../../..)
source $rootdir/test/common/autotest_common.sh
source $testdir/common.sh

jsonconf=$testdir/conf.json
testconf=$testdir/test.conf

#dev1=Malloc0
#dev2=Malloc1
dev1="scinia"
dev2="scinib"
tm=2

trap 'cleanup; exit 1' SIGINT SIGTERM EXIT
#Test inheriting filename and rw_mode parameters from global section.
create_job "global" "read" $dev1
create_job "job0"
create_job "job1"
create_job "job2"
create_job "job3"
bdevperf_output=$($bdevperf -t $tm --json $jsonconf -j $testconf 2>&1)
[[ $(get_num_jobs "$bdevperf_output") == "4" ]]

bdevperf_output=$($bdevperf -C -t $tm --json $jsonconf -j $testconf)

cleanup
#Test missing global section.
create_job "job0" "write" $dev1
create_job "job1" "write" $dev1
create_job "job2" "write" $dev1
bdevperf_output=$($bdevperf -t $tm --json $jsonconf -j $testconf 2>&1)
[[ $(get_num_jobs "$bdevperf_output") == "3" ]]

cleanup
#Test inheriting multiple filenames and rw_mode parameters from global section.
create_job "global" "read" $dev1:$dev2
create_job "job0"
create_job "job1"
create_job "job2"
create_job "job3"
bdevperf_output=$($bdevperf -t $tm --json $jsonconf -j $testconf 2>&1)
[[ $(get_num_jobs "$bdevperf_output") == "4" ]]
cleanup
trap - SIGINT SIGTERM EXIT
