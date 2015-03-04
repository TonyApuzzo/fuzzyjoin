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

from pychart import *

class zap_y_coord(linear_coord.T):
    def __init__(self, beg, end, height, off):
        self.beg = beg
        self.end = end
        self.height = height
        self.off = off

    # Method get_data_range is inherited from linear_coord.T.
    def get_canvas_pos(self, size, val, min, max):
        # Zap Y values between 'beg' and 'end'. Thus, Y axis will
        # display 0..'beg' and 'end'.., 'hight' points total.
        if val <= self.beg:
            return linear_coord.T.get_canvas_pos(
                self, size, val, 0, self.height)
        elif val <= self.end:
            return linear_coord.T.get_canvas_pos(
                self, size, self.beg, 0, self.height)
        else:
            return linear_coord.T.get_canvas_pos(
                self, size, val - (self.end - self.beg), 0, self.height)

    def get_tics(self, min, max, interval):
        tics = linear_coord.T.get_tics(self, min, max, interval)
        return [x for x in tics if x < (self.beg - self.off) or 
                x > (self.end - self.off)]

