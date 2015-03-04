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

### Nodes : x
### ------:----------
### 10    : 5, 10, 25
###  4    : 10
###  2    : 5, 10

h=`hostname`

h=${h:8:3}
h=${h##0}
h=${h##0}
st=`expr \( $h - 1 \) \* 4`     # 4,2
en=`expr $st + 3`               # 3,1

for c in 10
do

#     for s in dblp. csx.
#     do

        w=dblp
        d=records

        p=/home/ubuntu/rares/data/hdfs/$w/010/$d.$s$c
        rm -rf $p
        
        mkdir -p $p
        for f in `seq --format=part-%05.0f $st $en`
        do
            echo "/data/$d.$s$c/$f ->"
            echo "-> $p/"
            /hadoop/bin/hadoop fs -copyToLocal /data/$d.$s$c/$f $p/
        done

#     done

done
