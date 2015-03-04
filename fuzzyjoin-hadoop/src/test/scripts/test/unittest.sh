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

H_BIN=/hadoop/bin
H=$H_BIN/hadoop

SLAVES=1
DBLP_PATH=/data/raw/dblp/dblp.raw.txt
CITESEERX_PATH=/data/raw/citeseerx/csx.raw.txt

echo "---------------"
echo "-- - SETUP - --"
echo "---------------"

$H_BIN/stop-all.sh
sleep 5

rm -rf /tmp/hadoop-*
echo "Y" | $H namenode -format

$H_BIN/start-all.sh
is_not_started=1
while [ "$is_not_started" -eq 1 ]
do
    sleep 5
    $H dfsadmin -report | grep "Datanodes available: $SLAVES"
    is_not_started=$?
done

# $H fs -rmr /data
$H fs -mkdir /data/raw-000

echo "--------------"
echo "-- - TEST - --"
echo "--------------"

IFS="
"
for d in dblp pub
do
    if [ "$d" = "dblp" ]
    then
        ts="\
recordbuild
tokensbasic
ridpairsimproved
recordpairsbasic
tokensimproved
ridpairsppjoin
recordpairsimproved"
        $H fs -copyFromLocal $DBLP_PATH /data/raw-000/
    else
        ts="\
recordbuild
ridpairsimproved
recordpairsbasic
ridpairsppjoin
recordpairsimproved"
        $H fs -copyFromLocal $CITESEERX_PATH /data/raw-000/
    fi
      
    for t in $ts
    do
        echo -n "$d-$t..."

        s=""
        if [ "$t" = "recordbuild" ]
        then
            if [ "$d" = "dblp" ]
            then
                $H fs -rmr /data/records-000 >& /dev/null
            else
                $H fs -rmr /data/records.csx-000 >& /dev/null
                s="-Dfuzzyjoin.data.suffix.input=csx"
            fi
        fi

        $H jar ../target/fuzzyjoin-hadoop-0.0.1.jar $t -conf $d/conf.xml $s >& $d/$t.out.txt

        if [ "$t" = "recordbuild" ]
        then
            if [ "$d" = "dblp" ]
            then
                $H fs -mv /data/recordsbulk-000 /data/records-000
            else
                $H fs -mv /data/records-000 /data/records.dblp-000
                $H fs -mv /data/recordsbulk.csx-000 /data/records.csx-000
            fi
        fi

        e=""
        for l in `cat $d/$t.ok.txt`
        do

            grep "$l" $d/$t.out.txt > /dev/null
            code=$?

            if [ "$code" -eq "1" ]
            then
                if [ "$e" = "" ]
                then
                    echo
                fi
                e="ERROR"
                echo "ERROR: $d-$t $l"
            fi
            
        done

        if [ "$e" = "" ]
        then
            echo "(PASS)"
            rm $d/$t.out.txt
        else
            echo "$d-$t...(FAIL)"
        fi

    done
done

