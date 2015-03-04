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
p['loc_legend'] = (10, 60)

plot_init(theme, p)

theme.reinitialize()
can = canvas.init(p['fname_out'])
xaxis = axis.X(
    label = 'Dataset Size (times the original)\n' + p['note'], format = '%d')
yaxis = axis.Y(label = 'Time (seconds)', format = '%d')

theme.reinitialize()
can = canvas.init(p['fname_out'])

chart_object.set_defaults(bar_plot.T, cluster_sep = 4, width = 7)

ar = area.T(
    x_coord = category_coord.T(data[0], 0), 
    loc = p['loc_area'],
    x_axis = xaxis,
    y_axis = yaxis,
    y_range = (0, p['y_max']), 
    y_grid_interval = p['y_grid_interval'], 
    legend = p['legend_instance'])

labels_style = [
    [("1-BTO", fill_style.black), 
     ("2-BK", fill_style.gray50), 
     ("3-BRJ", fill_style.diag)], 
    [(None, fill_style.black),  # BTO
     ("2-PK", fill_style.white), 
     (None, fill_style.diag)],  # BDJ
    [(None, fill_style.black),  # BTO
     (None, fill_style.white),  # PK
     ("3-OPRJ", fill_style.rdiag2)]]

# labels_style = [
#     [("1-BTO", fill_style.green), 
#      ("2-BK", fill_style.red), 
#      ("3-BRJ", fill_style.yellow)], 
#     [(None, fill_style.green),  # BTO
#      ("2-PK", fill_style.blue), 
#      (None, fill_style.yellow)],  # BDJ
#     [(None, fill_style.green),  # BTO
#      (None, fill_style.blue),  # PK
#      ("3-OPRJ", fill_style.white)]]

l = len(data)
for i in xrange(l):
    p1 = bar_plot.T(data = data[i], hcol = 2, cluster=(i, l), 
                    label = labels_style[i][0][0], 
                    fill_style = labels_style[i][0][1])
    p2 = bar_plot.T(data = data[i], hcol = 3, cluster=(i, l), stack_on = p1, 
                    label = labels_style[i][1][0], 
                    fill_style = labels_style[i][1][1])
    p3 = bar_plot.T(data = data[i], hcol = 4, cluster=(i, l), stack_on = p2, 
                    label = labels_style[i][2][0], 
                    fill_style = labels_style[i][2][1])
    ar.add_plot(p1, p2, p3)

ar.draw()
can.close()
print_filename(p['fname_out'])
