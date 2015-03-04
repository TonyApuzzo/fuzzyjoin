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

# Figure 9: Running time for self-
# joining the DBLP x10 data set
# on different cluster sizes.

# Figure 10: Relative running time
# for self-joining the DBLP x10
# data set on different cluster sizes.

# Table 1: Running time (seconds) of each stage for
# self-joining the DBLP x10 data set on different clus-
# ter sizes

EXPERIMENT=speedup
DATASET=dblp
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

echo "Table data available in `cd $EXP_DIR; pwd`/$DATASET/$EXPERIMENT/[1-3]/*/avg.txt."

popd > /dev/null
