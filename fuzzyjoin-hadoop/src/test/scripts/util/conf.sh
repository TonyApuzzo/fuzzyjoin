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

### Hadoop
H_HOME=/hadoop
H_BIN=${H_HOME}/bin
H_CONF=${H_HOME}/conf
H=$H_BIN/hadoop
HDFS_DATA_DIR=/data             # reflected in conf/$DATESET.batch.xml
HDFS_FORMAT="rm -rf /mnt/c/hdfs/* /mnt/d/hdfs/* /mnt/e/hdfs/* /mnt/f/hdfs/*"

### Cluster
MEM_MAX=10240
CORE_MAX=4
PSSH="/usr/bin/parallel-ssh --print --hosts=$H_CONF/slaves"
# PSSH="../util/pusher --show-host --hosts=$H_CONF/slaves"
PSCP="/usr/bin/parallel-scp --hosts $H_CONF/slaves"

### Experiments
LOCAL_DATA_RAW_DBLP=/data/raw/dblp/dblp.raw.txt
LOCAL_DATA_RAW_CITESEERX=/data/raw/citeseerx/csx.raw.txt
LOG_DIR=../../logs
EXP_DIR=../../exps/asterix
DATE_FORMAT="+%F-%H-%M-%S"
LOG=$PWD/batch.log
JAR=../../target/fuzzyjoin-hadoop-0.0.1.jar
