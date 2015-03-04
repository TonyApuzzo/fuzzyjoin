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

DIR=`dirname $0`; if [ "${DIR:0:1}" == "." ]; then DIR=`pwd`"${DIR:1}"; fi
source $DIR/conf.sh

ARGS=1                   # Required number of arguments
E_BADARGS=85             # Wrong number of arguments passed to script.
if [ $# -lt "$ARGS" ]
then
  echo "Usage:   `basename $0` dataset"
  echo "Example: `basename $0` dblp-small"
  exit $E_BADARGS
fi

THR="0.80"
if [ "$1" == "dblp-small" ]; then
    THR="0.50"
fi


mkdir $DATA/$1.expected/$OUT
$SSJOIN/ppjoinplus j $THR $DATA/$1/$IN/part-00000 | \
    sed 's/0\.812/0\.813/' | \
    sort > $DATA/$1.expected/$OUT/expected.txt

mkdir $DATA/$1/$OUT
java \
    -Xmx2g \
    -jar $DIR/../../../target/fuzzyjoin-core-0.0.1.jar \
    $THR $DATA/$1/$IN/part-00000 | \
    sort > $DATA/$1/$OUT/part-00000

diff $DATA/$1.expected/$OUT/expected.txt $DATA/$1/$OUT/part-00000
