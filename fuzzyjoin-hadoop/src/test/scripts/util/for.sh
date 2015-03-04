#!/bin/bash
#
# Copyright 2010-2011 The Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS"; BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
# 
# Author: Rares Vernica <rares (at) ics.uci.edu>

# out=avg2_job1_part.txt
# touch $out
for f in 1 2 5 10
do
    fn=`printf %03d $f`

#     echo $f `head -15 ${fn}.txt | tail -3 | average.py` >> $out

#     cat ${fn}_6_job1.txt | job_history_summary.py t > ${f/job/timeline}
#     python ~/benchmarks/exps/timeline.py $f 150 40

#     echo $f `cat ${fn}_15_job1.log | job_history_summary.py s max` >> $out
#     cat ${fn}_15_job1.log | job_history_summary.py s w

#     cat ${fn}_6_job1.log | python /benchmarks/util/job_history_summary.py c

#     svn mv ${fn}.txt ../../80
#     svn mv ${fn}_log.txt ../../80
#     svn mv ${fn}_job1.txt ../../80
#     svn mv ${fn}_job2.txt ../../80
#     svn mv ${fn}_6_job.log ../../80/${fn}_job.log
#     svn mv ${fn}_15_job2.log ../../80/${fn}_job2.log
#     svn revert ${fn}_6_job.log
done

for f in *
do
#     svn mv $f ../../80/${f/avg2/avg}

    echo -n .
done

for f in 0??.txt 0??_log.txt 0??_job?.txt
do
    tail -3 $f > t
    mv --force t $f

    echo -n .
done