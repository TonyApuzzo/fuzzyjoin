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

import numpy
import sys

filein = sys.stdin

m = {}

for line in filein.readlines():
    s = line.split()
    for i in xrange(2):
        r = int(s[i])
        try:
            m[r] += 1
        except:
            m[r] = 1

v = m.values()
a = numpy.array(v)

print min(v), max(v), a.mean(), a.std()
