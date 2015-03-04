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
from pychart import *
from plotutils import * 

def rel(x):
    global dat
    y = []
    y.append(x[0])
    for i in xrange(1, len(x)):
        y.append(dat[0][i] / x[i] * dat[0][0])
    return y

(argv, p) = parse_arg(sys.argv)

###
### Data
###
data = []
data_ideal = []
for d in p['dirs']:
    dat = chart_data.read_csv(d + p['path'], p['format'])
    dat = dat[p['start']:]
    data.append(dat)
    if p['ideal']:
        dat_ideal = [[dat[0][0], dat[0][1]]]
        for d in dat[1:]:
            dat_ideal.append([d[0], dat[0][1] / d[0] * dat[0][0]])
        data_ideal.append(dat_ideal)

###
### Plot
###
p['loc_area'] = (0, 0)
if p['loc_legend'] is None:
    p['loc_legend'] = (75, 75)

plot_init(theme, p)

theme.reinitialize()
can = canvas.init(p['fname_out'])
xaxis = axis.X(label = '# Nodes\n' + p['note'], format = '%d')
yaxis = axis.Y(label = 'Time (seconds)', format = '%d')
ar = area.T(
    loc = p['loc_area'],
    x_axis = xaxis,
    x_range = (2, 10), 
    y_axis = yaxis,
    y_range = (0, p['y_max']), 
    y_grid_interval = p['y_grid_interval'], 
    legend = p['legend_instance']
)

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

if p['ideal']:
    for i in xrange(len(data_ideal)):
        if i == 0:
            l = 'Ideal'
        else:
            l = None
        ar.add_plot(
            line_plot.T(
                label = l, 
                data = data_ideal[i], 
                tick_mark=None, 
                line_style = line_style.T(width = .2)))

ar.draw()
can.close()
print_filename(p['fname_out'])
