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
import os
import sys

fname_in = sys.argv[1]
fname_out = fname_in[:fname_in.rindex('.')] + '.eps'

x_max = y_max = x_grid_interval = y_grid_interval = None

if len(sys.argv) > 2:
    x_max = int(sys.argv[2])
    x_grid_interval = x_max / 3

if len(sys.argv) > 3:
    y_max = int(sys.argv[3])
    y_grid_interval = y_max / 10

###
### Data
###
### time maps shuffle merge reduce waste
data = chart_data.read_csv(fname_in, '%f %d %d %d %d %d')
data = chart_data.transform(
    lambda x: [
        x[0], 0, x[1], sum(x[1:3]), sum(x[1:4]), sum(x[1:5]), sum(x[1:6])], 
    data)
max = 50
if len(data) > max * 2:
    m = len(data) / max
    data = chart_data.filter(lambda x: x[0] % m == 0, data)

###
### Plot
###
theme.get_options()
theme.scale_factor = 3
loc = (0, 0)
loc = (-60, 0)
loc_legend = (-40, -80)
theme.reinitialize()
can = canvas.init(fname_out)

d = '_'.join(os.getcwd().split('/')[-6:] + [fname_out[:fname_out.rfind('.')]])
ar = area.T(
    x_axis = axis.X(label = 'Time (seconds)\n' + d, format = '/a-30{}%d'),
    x_range = (0, x_max), 
    x_grid_interval = x_grid_interval, 
    y_axis = axis.Y(label = '# Tasks', format = '%d'),
    y_range = (0, y_max),
    y_grid_interval = y_grid_interval, 
    loc = loc, 
    legend = legend.T(loc = loc_legend))

colors = [ fill_style.white, fill_style.gray90, fill_style.diag, fill_style.black, fill_style.rdiag3 ]

ar.add_plot(range_plot.T(
        label = 'maps',
        data = data,
        fill_style = colors[0]))
ar.add_plot(range_plot.T(
        label = 'shuffle',
        data = data,
        min_col = 2, 
        max_col = 3,
        fill_style = colors[1]))
ar.add_plot(range_plot.T(
        label = 'merge',
        data = data,
        min_col = 3, 
        max_col = 4,
        fill_style = colors[2]))
ar.add_plot(range_plot.T(
        label = 'reduce',
        data = data,
        min_col = 4, 
        max_col = 5,
        fill_style = colors[3]))
# ar.add_plot(range_plot.T(
#         label = 'waste',
#         data = data,
#         min_col = 5, 
#         max_col = 6,
#         fill_style = colors[4]))
ar.draw()
can.close()
print fname_out, 'wrote'

