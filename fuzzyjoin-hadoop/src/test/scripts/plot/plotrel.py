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
import pprint
import sys
from pychart import *
from plotutils import parse_arg, plot_init

def rel(x):
    global dat
    y = []
    y = [x[0]]
    for i in xrange(1, len(x)):
        y.append(x[i] / dat[0][i])
    return y

(argv, p) = parse_arg(sys.argv)

###
### Data
###
pp = pprint.PrettyPrinter()
data = []
for d in p['dirs']:
    dat = chart_data.read_csv(d + p['path'], p['format'])
    dat = dat[p['start']:]
    dat = chart_data.transform(rel, dat)
    pp.pprint(dat)
    data.append(dat)

###
### Plot
###
p['loc_area'] = (0, 0)
p['loc_legend'] = (10, 70)

plot_init(theme, p)

theme.reinitialize()
can = canvas.init("rel_" + p['fname_out'])
xaxis = axis.X(label = p['note'], format = '/a60{}%d')
yaxis = axis.Y(label = 'Relative', format = '%.1f')
max = data[0][-1][0]
ar = area.T(
    loc = p['loc_area'],
    x_coord = category_coord.T(data[0], 0), 
#     x_coord = log_coord.T(), 
    x_axis = xaxis,
    y_axis = yaxis,
    y_range = (0, p['y_max']), 
    y_grid_interval = p['y_grid_interval'], 
    legend = None #p['legend_instance']
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
print p['fname_out'], 'wrote'
