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

noargs = len(sys.argv)

filein = sys.stdin
pattern = ''
field = 0

if noargs > 1:
    filein = open(sys.argv[1])
    if noargs > 2:
        pattern = sys.argv[2]
        if noargs > 3:
            field = int(sys.argv[3])

total = 0
for line in filein.readlines():
    line = line.strip()
    if not line:
        continue
    if line.find(pattern) >= 0:
        total += float(line.split()[field])

print total
