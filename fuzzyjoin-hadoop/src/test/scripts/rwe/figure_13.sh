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

# Figure 13: Running time for
# joining the DBLP x10 and the
# CITESEERX x10 data sets on
# different cluster sizes.

EXPERIMENT=speedup
DATASET=pub
SIZE=10

pushd . > /dev/null
cd ../util

echo "===( 2 )==="
./batch.sh $EXPERIMENT $DATASET $SIZE  2
if [ "$?" != 0 ]; then exit; fi
echo "===( 4 )==="
./batch.sh $EXPERIMENT $DATASET $SIZE  4
if [ "$?" != 0 ]; then exit; fi
echo "===( 8 )==="
./batch.sh $EXPERIMENT $DATASET $SIZE  8
if [ "$?" != 0 ]; then exit; fi
echo "===( 10 )==="
./batch.sh $EXPERIMENT $DATASET $SIZE 10
if [ "$?" != 0 ]; then exit; fi

source ./postprocess.sh

avg $EXPERIMENT $DATASET
combine $EXPERIMENT $DATASET
plot_speedup $DATASET

popd > /dev/null
