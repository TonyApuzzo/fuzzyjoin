#!/usr/bin/env python
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

import sys
import datetime

sz = 17
format = '%y/%m/%d %H:%M:%S'

for line in sys.stdin.readlines():
    if line.find('map 0%  reduce 0%') > 0:
        # print line[:sz]
        st = datetime.datetime.strptime(line[:sz], format)
        # print st
    elif line.find('map 100%  reduce 100%') > 0:
        # print line[:sz]
        en = datetime.datetime.strptime(line[:sz], format)
        # print en

t = en - st
# print t
print (t.seconds + 30) / 60
