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

tick_marks = [tick_mark.star, tick_mark.blacksquare, tick_mark.tri, tick_mark.circle3, tick_mark.gray70dtri, tick_mark.dia]

def plot_init(theme, p):
    # ext = 'eps'
    # ext = 'png'
    ext = 'pdf'

    theme.scale_factor = 1      # 3, 1.5, 2.4
    if ext != 'eps':
        theme.output_format = ext

    d = '_'.join(p['dirs']).replace('/', '_')
    path = p['path'][1:].replace('/', '_').split('.')[0]
    if d == '.':
        fname_out = '%s.%s' % (path, ext)
    elif path == '.txt':
        fname_out = '%s.%s' % (d, ext)
    else:
        if path[0] != '-':
            path = '-' + path
        fname_out = '%s%s.%s' % (d, path, ext)

    s = 50
    loc_area = (p['loc_area'][0] - s, p['loc_area'][1])
    loc_legend = (p['loc_legend'][0] - s, p['loc_legend'][1])

    legend_instance = None
    legend_instance = legend.T(
        loc = loc_legend, 
        shadow = (1, 1, fill_style.gray70))

#     note = '_'.join(
#         os.getcwd().split('/')[-4:] + [fname_out[:fname_out.rfind('.')]])
    note = ''

    p['fname_out']=fname_out
    p['loc_area']=loc_area
    p['loc_legend']=loc_legend
    p['legend_instance']=legend_instance
    p['note']=note

def parse_arg(argv):
    if len(argv) < 3:
        print 'Usage: %s [--start=2] [--format="%%d %%f %%f"] [--lables=Map,Reduce] [--ideal=1] [--exp="# Nodes"] [--ymin=0] [--ymax=100] [--ygrid=20] [--mask=0110] "bib,ipi" x10/80/avg.txt' % argv[0]
        sys.exit(-1)

    ### Defaults
    start = 0
    format = '%d %f'
    labels = None
    ideal = 1
    exp = None
    y_min = None
    y_max = None
    y_grid_interval = None
    loc_legend = (10, 80)
    mask = None

    l = len(argv)
    i = 1
    while i < l:
        arg = argv[i]
        if not arg.startswith('--'):
            i -= 1
            break
        split = arg.split('=')
        arg = split[0][2:]
        val = split[1]
        if arg == 'start':
            start = int(val)
        elif arg == 'format':
            format = val
        elif arg == 'labels':
            labels = val.split(',')
        elif arg == 'ideal':
            ideal = int(val)
        elif arg == 'exp':
            exp = val
        elif arg == 'ymin':
            y_min = float(val)
        elif arg == 'ymax':
            y_max = float(val)
        elif arg == 'ygrid':
            y_grid_interval = float(val)
        elif arg == 'legend':
            s = val.split(',')
            loc_legend = (int(s[0]), int(s[1]))
        elif arg == 'mask':
            mask = val
        i += 1

    argv = argv[i:]
    dirs = argv[1].split(',')
    if labels is None:
        labels = dirs
    path = argv[2]

    p = {}
    p['dirs']=dirs
    p['path']=path
    p['start']=start
    p['format']=format
    p['labels']=labels
    p['ideal']=ideal
    p['exp']=exp
    p['y_min']=y_min
    p['y_max']=y_max
    p['y_grid_interval']=y_grid_interval
    p['loc_legend']=loc_legend
    p['mask']=mask

    return (argv, p)

def print_filename(fname):
    sys.stderr.write('Figure available in ' + os.getcwd() + '/' + fname + '\n')
