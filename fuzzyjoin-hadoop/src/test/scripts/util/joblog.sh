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

if [ -z $1 ]
then
    echo "Usage: $0 001_log.txt 1 [1]"
    exit 255
fi

log_index=$1
no=$2
no2=$3

log_path=~/logs
hadoop_path=/hadoop/logs/history

log=`head -$no $log_index | tail -1`

cp $hadoop_path/*`grep "Job complete:" $log_path/$log | cut --delimiter=" " --fields=7 | head -$no2 | tail -1`*[^xml] ./${log_index:0:3}_job$no2.log
