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
p['loc_area'] = (0, 0)
p['loc_legend'] = (10, 10)

plot_init(theme, p)

theme.reinitialize()
can = canvas.init(p['fname_out'])
xaxis = axis.X(label = p['note'], format = '/a60{}%d')
yaxis = axis.Y(
    label = 'Data-nomalized communication', 
    format = '%.2f')
max = data[0][-1][0]
ar = area.T(
    loc = p['loc_area'],
    x_axis = xaxis,
    x_coord = category_coord.T(data[0], 0), 
#     x_coord = log_coord.T(), 
    y_axis = yaxis,
    y_range = (0, p['y_max']), 
    y_grid_interval = p['y_grid_interval'], 
    legend = p['legend_instance']
    )

tick_marks = [tick_mark.star, tick_mark.blacksquare, tick_mark.tri, tick_mark.circle3, tick_mark.gray70dtri, ]

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
