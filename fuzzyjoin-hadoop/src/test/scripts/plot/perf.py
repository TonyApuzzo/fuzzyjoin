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

import os
import sys
from pychart import *
from plotutils import *

(argv, p) = parse_arg(sys.argv)

###
### Data
###
data = []
for d in p['dirs']:
    dat = chart_data.read_csv(d + p['path'], p['format'])
    dat = dat[p['start']:]
    data.append(dat)

###
### Plot
###
p['loc_area'] = (0, 0) # (-50, 0)
p['loc_legend'] = (10, 70)

plot_init(theme, p)

theme.reinitialize()
can = canvas.init(p['fname_out'])
xaxis = axis.X(
    label = 'Dataset Size (times original)\n' + p['note'], format = '%d')
yaxis = axis.Y(label = 'Time (seconds)', format = '%d')
max = data[0][-1][0]
ar = area.T(
    loc = p['loc_area'],
    x_axis = xaxis,
    x_range = (5, 25), 
    x_grid_interval = 5, 
    y_axis = yaxis,
    y_range = (0, None), 
    legend = p['legend_instance'])

for i in xrange(len(data)):
    for j in xrange(len(p['labels'])):
        k = j
        ycol = j + 1
        if len(p['dirs']) > 1:
            k = i
            ycol = 1
        ar.add_plot(
            line_plot.T(
                label = p['labels'][k],
                data = data[i], 
                ycol = ycol, 
                tick_mark=tick_marks[k]))
        if len(p['dirs']) > 1:
            break

ar.draw()
can.close()
print_filename(p['fname_out'])
