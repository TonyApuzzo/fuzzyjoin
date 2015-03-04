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

# Figure 14: Running time for
# joining the DBLP xn and the
# CITESEERX xn data sets
# (where n  [5, 25]) increased
# proportionally with cluster size.

EXPERIMENT=scaleup
DATASET=pub

pushd . > /dev/null
cd ../util

# echo "===( 2 )==="
# ./batch.sh $EXPERIMENT $DATASET  5  2
# if [ "$?" != 0 ]; then exit; fi
# echo "===( 4 )==="
# ./batch.sh $EXPERIMENT $DATASET 10  4
# if [ "$?" != 0 ]; then exit; fi
echo "===( 8 )==="
./batch.sh $EXPERIMENT $DATASET 20  8
if [ "$?" != 0 ]; then exit; fi
echo "===( 10 )==="
./batch.sh $EXPERIMENT $DATASET 25 10
if [ "$?" != 0 ]; then exit; fi

source ./postprocess.sh

avg $EXPERIMENT $DATASET
combine $EXPERIMENT $DATASET
plot_scaleup $DATASET

popd > /dev/null
