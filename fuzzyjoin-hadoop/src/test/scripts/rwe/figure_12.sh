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

# Figure 12: Running time for
# joining the DBLP xn and the
# CITESEERX xn data sets
# (where n  [5, 25]) on a 10-node
# cluster.

EXPERIMENT=perf
DATASET=pub
SLAVES=10

pushd . > /dev/null
cd ../util

echo "===( x5 )==="
./batch.sh $EXPERIMENT $DATASET  5 $SLAVES
if [ "$?" != 0 ]; then exit; fi
echo "===( x10 )==="
./batch.sh $EXPERIMENT $DATASET 10 $SLAVES
if [ "$?" != 0 ]; then exit; fi
echo "===( x25 )==="
./batch.sh $EXPERIMENT $DATASET 25 $SLAVES
if [ "$?" != 0 ]; then exit; fi

source ./postprocess.sh

avg $EXPERIMENT $DATASET
combine $EXPERIMENT $DATASET
plot_perf $DATASET

popd > /dev/null
