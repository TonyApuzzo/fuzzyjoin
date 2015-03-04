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
    echo "Usage: cat 001_log.txt | $0 1"
    exit 255
fi

pref=/home/ubuntu/logs
num=$1

while read f
do
#     echo $f
    grep "The job took" $pref/$f | head -$num | tail -1 | cut --delimiter=" " --fields=4
done