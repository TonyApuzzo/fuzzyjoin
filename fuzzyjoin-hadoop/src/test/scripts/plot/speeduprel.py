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
import pprint
from pychart import *
from plotutils import *

def rel(x):
    global dat
    y = []
    y.append(x[0])
    for i in xrange(1, len(x)):
        y.append(dat[0][i] / x[i])
    return y

(argv, p) = parse_arg(sys.argv)

if p['y_min'] is None:
    p['y_min'] = 1
if p['y_max'] is None:
    p['y_max'] = 5

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
p['loc_legend'] = (10, 80)

plot_init(theme, p)

theme.reinitialize()
p['fname_out'] = 'rel-' + p['fname_out']
can = canvas.init(p['fname_out'])
xaxis = axis.X(label='# Nodes\n' + p['note'], format = '%d')
yaxis = axis.Y(
    label='Speedup = Old Time // New Time', 
    format = '%d')
max = data[0][-1][0]
ar = area.T(
    size = (120, (p['y_max'] - p['y_min']) / 4 * 120),
    loc = p['loc_area'],
    x_axis = xaxis,
    y_axis = yaxis,
    x_range = (2, max), 
    y_range = (p['y_min'], p['y_max']), 
    y_grid_interval = 1, 
    legend = p['legend_instance'])

tick_marks = [tick_mark.star, tick_mark.blacksquare, tick_mark.tri, tick_mark.circle3, tick_mark.gray70dtri, ]

for i in xrange(len(data)):
    for j in xrange(len(p['labels'])):
        if p['mask'] and p['mask'][j] == '0':
            continue
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

data_ideal = [[x[0], 1. * x[0] / data[0][0][0]] for x in data[0]]
ar.add_plot(
    line_plot.T(
        label = 'Ideal',
        data = data_ideal, 
        tick_mark=None, 
        line_style = line_style.T(width = .2)))

ar.draw()
can.close()
print_filename(p['fname_out'])
