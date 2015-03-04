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
import re
import htmlentitydefs

def convertentity(m):
    if m.group(1)=='#':
        try:
            return chr(int(m.group(2)))
        except ValueError:
            return '&#%s;' % m.group(2)
    try:
        return htmlentitydefs.entitydefs[m.group(2)]
    except KeyError:
        return '&%s;' % m.group(2)

def converthtml(s):
    return re.sub(r'&(#?)(.+?);',convertentity,s)

if __name__ == '__main__':
    # converthtml('Some &lt;html&gt; string.')  # --> 'Some <html> string.'

    filein = sys.stdin
    if len(sys.argv) > 1:
        filein = open(sys.argv[1])

    for line in filein:
        print converthtml(line.strip())
