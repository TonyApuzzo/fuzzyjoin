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

source ./conf.sh

### Command line arguments
ARGS=4                   # Script requires 4 arguments.
E_BADARGS=85             # Wrong number of arguments passed to script.
if [ $# -ne "$ARGS" ]
then
  echo "Usage:   `basename $0` experiment dataset size slaves"
  echo "Example: `basename $0` perf dblp 10 10"
  exit $E_BADARGS
fi
EXPERIMENT=$1                   # perf, speedup, scaleup
DATASET=$2                      # dblp, pub
SIZE=$3                         # 5, 10, 25
SLAVES=$4                       # 2, 4, 8, 10

### Init
SLAVES_0=`printf %03d $SLAVES`
JB="-conf ../conf/fuzzyjoin/$DATASET.batch.xml"

source ./batch-utils.sh

######
###### Prepare
######

init

upload

prepare

######
###### Run
######

run
