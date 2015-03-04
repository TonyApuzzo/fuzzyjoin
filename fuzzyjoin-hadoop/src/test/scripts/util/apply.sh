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

job=$1
f=avg_job${job}_part.txt

# echo "|| Nodes || Tot || Min || Max || Avg || Std Dev || Std Dev Rel"

for dir in 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384
do
for path in 10
do
#     echo
#     echo " * $path Nodes"
#     echo
    path_zeroed=`printf %03d $path`
    
#     echo $path `cat ${path_zeroed}_job$job.log | job.py s avg` >> $f

#     f=${path_zeroed}_job${job}_timeline.txt
#     cat ${path_zeroed}_job${job}.log | job.py t > $f
#     timeline.py $f 2400 80
#     lpr ${f/txt/eps}

#     cat ${path_zeroed}_job$job.log | job.py s w

    k="Map"
    c=`grep -ro "$k output records)([1-9][0-9]*" $dir/${path_zeroed}_job$job.log | tail -1 | cut --delimiter="(" --fields=2`
    echo $dir $c

#     grep -ro "Reduce input records)([1-9][0-9]*" ${path_zeroed}_job${job}.log | cut --delimiter="(" --fields=2 | uniq | sort --numeric-sort > t
#     l=`wc -l t | cut --delimiter=" " --field=1`
#     l=`expr $l - 1`
#     a=`head -$l t | average.py`
#     d=`head -$l t | stddev.py`
#     r=`head -$l t | stddevrel.py`
#     printf "|| %3d   || %d || %d || %d || %.2f || %.2f || %.2f\n" $path `tail -1 t` `head -1 t` `tail -2 t | head -1` $a $d $r
#     rm t

done
done

# speedup.py . /$f 1 "%d %f %f %f %f %f" "Start,Map,Shuffle,Merge,Reduce" 0
# speeduprel.py . /$f 1 "%d %f %f %f %f %f" "Start,Map,Shuffle,Merge,Reduce" 01111

# find . -not -path "*.svn*" -type f | xargs sudo chown rares:grad
# find . -not -path "*.svn*" -type f | xargs chmod a-x
# find . -not -path "*.svn*" -type f | xargs chmod g+w
