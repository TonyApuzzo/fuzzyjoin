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

MS="2 4 10 20 50 100 200 400"
RS="1 2 5 10 20 50 100 200 400"

### remove logs from failed jobs
for F in `grep --files-with-matches "Streaming Job Failed!" ../log/*`
do
    rm $F
done

# || Map\Reduce || 1 || 2 || 5 || 10 || 20 || 50 || 100 || 200 || 400
# ||  2         || 6 || 7 || 6 ||  5 ||  6 ||  5 ||   4 ||   5 ||   5
# ||  4         || 5 || 5 || 5 ||  4 ||  4 ||  3 ||   3 ||   3 ||   4
# || 10         || 5 || 6 || 3 ||  2 ||  2 ||  2 ||   2 ||   2 ||   2
# || 20         || 5 || 4 || 3 ||  2 ||  2 ||  1 ||   1 ||   1 ||   1
# || 50         || 4 || 4 || 3 ||  2 ||  2 ||  1 ||   1 ||   1 ||   1
# || 100        || 5 || 5 || 2 ||  2 ||  1 ||  1 ||   1 ||   1 ||   1
# || 200        || 5 || 4 || 3 ||  2 ||  2 ||  1 ||   1 ||   1 ||   1
# || 400        || 5 || 4 || 3 ||  2 ||  2 ||  2 ||   2 ||   3 ||   1

echo -n "|| Map\Reduce"
for R in $RS
do
    echo -n " || $R"
done
echo

for M in $MS
do
    echo -n "|| $M"
    for R in $RS
    do
        MIN_T=999
        MIN_F=
        for F in ../log/*-$M-$R-1-stream.txt
        do
            # echo -n "crt $F"

            T=`cat $F | python parselog.py`
            

            # echo " $T"
            if [ "$T" -le "$MIN_T" ]
            then
                # echo "rm  $MIN_F"
                if [ "$MIN_F" ]
                then
                    rm $MIN_F
                fi

                MIN_T=$T
                MIN_F=$F
            else
                # echo "rm  $F"
                rm $F
            fi
            # echo "min $MIN_F $MIN_T"
        done
        echo -n " || $MIN_T"
        # echo
    done
    echo
done