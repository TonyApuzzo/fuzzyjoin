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
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy
import pprint
import re
import sys

pat = re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
groupPat = re.compile(r'{\((?P<key>[^)]+)\)\((?P<name>[^)]+)\)(?P<counters>[^}]+)}')
counterPat = re.compile(r'\[\((?P<key>[^)]+)\)\((?P<name>[^)]+)\)\((?P<value>[^)]+)\)\]')

def parseCounters(str):
  result = {}
  for k,n,c in re.findall(groupPat, str):
    group = {}
    result[n] = group
    for sk, sn, sv in re.findall(counterPat, c):
      group[sn] = int(sv)
  return result

def parse(tail):
  result = {}
  for n,v in re.findall(pat, tail):
    result[n] = v
  return result

mapStartTime = {}
mapEndTime = {}
setupStartTime = {}
setupEndTime = {}
cleanupStartTime = {}
cleanupEndTime = {}
mapTracker = {}
mapTaskCounters = {}
reduceStartTime = {}
reduceShuffleTime = {}
reduceSortTime = {}
reduceEndTime = {}
reduceBytes = {}
reduceTracker = {}
finalAttempt = {}
wastedAttempts = []
submitTime = None
finishTime = None
scale = 1000.
jobCounters = None

def parse_all():
  global submitTime, finishTime, jobCounters
  remainder = ""
  for line in sys.stdin:
    if len(line) < 3 or line[-3:] != " .\n":
      remainder += line
      continue
    line = remainder + line
    remainder = ""
    words = line.split(" ",1)
    event = words[0]
    attrs = parse(words[1])
    if event == 'Job':
      if attrs.has_key('SUBMIT_TIME'):
        submitTime = int(attrs['SUBMIT_TIME']) / scale
      elif attrs.has_key("FINISH_TIME"):
        finishTime = int(attrs["FINISH_TIME"]) / scale
        if attrs.has_key("COUNTERS"):
          jobCounters = parseCounters(attrs["COUNTERS"])
    elif event == 'MapAttempt' and attrs['TASK_TYPE'] == 'MAP':
      if attrs.has_key("START_TIME"): # and attrs['TASK_TYPE'] == 'MAP':
        time = int(attrs["START_TIME"]) / scale
        if time != 0:
          mapStartTime[attrs["TASK_ATTEMPT_ID"]] = time
      elif attrs.has_key("FINISH_TIME"):
        mapEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"])/scale
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          task = attrs["TASKID"]
          if finalAttempt.has_key(task):
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
        else:
          wastedAttempts.append(attrs["TASK_ATTEMPT_ID"])
      if attrs.has_key('TRACKER_NAME'):
        mapTracker[attrs['TASK_ATTEMPT_ID']] = attrs['TRACKER_NAME']
    elif event == 'MapAttempt' and attrs['TASK_TYPE'] == 'SETUP':
      if attrs.has_key("START_TIME"):
        time = int(attrs["START_TIME"]) / scale
        if time != 0:
          setupStartTime[attrs["TASK_ATTEMPT_ID"]] = time
      elif attrs.has_key("FINISH_TIME"):
        setupEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"])/scale
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          task = attrs["TASKID"]
          if finalAttempt.has_key(task):
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
    elif event == 'MapAttempt' and attrs['TASK_TYPE'] == 'CLEANUP':
      if attrs.has_key("START_TIME"):
        time = int(attrs["START_TIME"]) / scale
        if time != 0:
          cleanupStartTime[attrs["TASK_ATTEMPT_ID"]] = time
      elif attrs.has_key("FINISH_TIME"):
        cleanupEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"])/scale
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          task = attrs["TASKID"]
          if finalAttempt.has_key(task):
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
    elif event == 'ReduceAttempt':
      if attrs.has_key("START_TIME"):
        time = int(attrs["START_TIME"]) / scale
        if time != 0:
          reduceStartTime[attrs["TASK_ATTEMPT_ID"]] = time
      elif attrs.has_key("FINISH_TIME"):
        task = attrs["TASKID"]
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          if finalAttempt.has_key(task):
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
        else:
          wastedAttempts.append(attrs["TASK_ATTEMPT_ID"])
        reduceEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"]) / scale
        if attrs.has_key("SHUFFLE_FINISHED"):
          reduceShuffleTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["SHUFFLE_FINISHED"]) / scale
        if attrs.has_key("SORT_FINISHED"):
          reduceSortTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["SORT_FINISHED"]) / scale
      if attrs.has_key('TRACKER_NAME'):
        reduceTracker[attrs['TASK_ATTEMPT_ID']] = attrs['TRACKER_NAME']
    elif event == 'Task':
      if attrs["TASK_TYPE"] == "REDUCE" and attrs.has_key("COUNTERS"):
        counters = parseCounters(attrs["COUNTERS"])
        reduceBytes[attrs["TASKID"]] = int(counters.get('FileSystemCounters',{}).get('HDFS_BYTES_WRITTEN',0))
      elif attrs["TASK_TYPE"] == "MAP" and attrs.has_key("COUNTERS"):
        mapTaskCounters[attrs["TASKID"]] = parseCounters(attrs["COUNTERS"])

def timeline():
  reduces = reduceBytes.keys()
  reduces.sort()

  runningMaps = []
  shufflingReduces = []
  sortingReduces = []
  runningReduces = []

  waste = []
  final = {}
  for t in finalAttempt.values():
    final[t] = None

  for t in range(submitTime, finishTime):
    runningMaps.append(0)
    shufflingReduces.append(0)
    sortingReduces.append(0)
    runningReduces.append(0)
    waste.append(0)

  for map in mapEndTime.keys():
    isFinal = final.has_key(map)
    if mapStartTime.has_key(map):
      for t in range(mapStartTime[map]-submitTime, mapEndTime[map]-submitTime):
        if final:
          runningMaps[t] += 1
        else:
          waste[t] += 1

  for reduce in reduceEndTime.keys():
    if reduceStartTime.has_key(reduce):
      if final.has_key(reduce):
        for t in range(reduceStartTime[reduce]-submitTime, reduceShuffleTime[reduce]-submitTime):
          shufflingReduces[t] += 1
        for t in range(reduceShuffleTime[reduce]-submitTime, reduceSortTime[reduce]-submitTime):
          sortingReduces[t] += 1
        for t in range(reduceSortTime[reduce]-submitTime, reduceEndTime[reduce]-submitTime):
          runningReduces[t] += 1
      else:
        for t in range(reduceStartTime[reduce]-submitTime, reduceEndTime[reduce]-submitTime):
          waste[t] += 1

  off = 0
  if len(sys.argv) > 2:
    off = int(sys.argv[2])

  # print "time maps shuffle merge reduce waste"
  for t in range(len(runningMaps)):
    print off + t / float(f), runningMaps[t], shufflingReduces[t], sortingReduces[t], runningReduces[t], waste[t]

def stats():
  start_times_map = {}
  map_times_map = {}

  final = {}
  for t in finalAttempt.values():
    final[t] = None

  start_times = []
  finish_times = []
  setup_times = []
  cleanup_times = []
  map_times = []

  m_f = 0                       # maps finished
  for t in mapEndTime.keys():
    if not final.has_key(t):
      continue
    m_f += 1
  oneMapPerCore = False
  if m_f == len(set(mapTracker.values())) * 4: # one map per core
    oneMapPerCore = True

  if oneMapPerCore:
    for t in mapEndTime.keys():
      if not final.has_key(t):
        continue
      start_times.append(mapStartTime[t] - submitTime)
#       map_times.append(mapEndTime[t] - mapStartTime[t])
  else:                         # more/less than one map per core
    for t in mapEndTime.keys():
      if not final.has_key(t):
        continue
      k = mapTracker[t]
      a = mapStartTime[t] - submitTime
      try:
        start_times_map[k].append(a)
      except:
        start_times_map[k] = [a]
#       a = mapEndTime[t] - mapStartTime[t]
#       try:
#         map_times_map[k].append(a)
#       except:
#         map_times_map[k] = [a]
    for k in set(mapTracker.values()):
      l = start_times_map[k]
      l.sort()
      start_times += l[:4]
#       map_times.append(sum(map_times_map[k]) / 4)

  for t in reduceEndTime.keys():
      if not final.has_key(t):
        continue
      finish_times.append(finishTime - reduceEndTime[t])

  for t in setupEndTime.keys():
    if not final.has_key(t):
      continue
    setup_times.append(setupEndTime[t] - setupStartTime[t])

  for t in cleanupEndTime.keys():
    if not final.has_key(t):
      continue
    cleanup_times.append(cleanupEndTime[t] - cleanupStartTime[t])

  map_times_end = []
  shuffle_times_start = []

  for t in mapEndTime.keys():
    if not final.has_key(t):
      continue
    map_times.append(mapEndTime[t] - mapStartTime[t])
    map_times_end.append(mapEndTime[t])

  shuffle_times = []
  merge_times = []
  reduce_times = []

#   oneMapPerCore = False
  for t in reduceEndTime.keys():
    if not final.has_key(t):
      continue
    if oneMapPerCore:
      shuffle_times.append(reduceShuffleTime[t])
    else:
      shuffle_times.append(reduceShuffleTime[t] - reduceStartTime[t])
    merge_times.append(reduceSortTime[t] - reduceShuffleTime[t])
    reduce_times.append(reduceEndTime[t] - reduceSortTime[t])

  if oneMapPerCore and \
        len(map_times_end) > len(shuffle_times) and \
        len(shuffle_times) == 1:
    map_times_end = [max(map_times_end)]
    
  if oneMapPerCore:
    map_times_end.sort()
    shuffle_times.sort()
    for i in xrange(len(map_times_end)):
      shuffle_times[i] = shuffle_times[i] - map_times_end[i]

  map_times_a = numpy.array(map_times)
  map_times_s = (min(map_times), max(map_times), map_times_a.mean(), map_times_a.std(), map_times_a.std() * 100 / map_times_a.mean()) 
  start_times_a = numpy.array(start_times)
  start_times_s = (min(start_times), max(start_times), start_times_a.mean(), start_times_a.std(), start_times_a.std() * 100 / start_times_a.mean()) 
  finish_times_a = numpy.array(finish_times)
  finish_times_s = (min(finish_times), max(finish_times), finish_times_a.mean(), finish_times_a.std(), finish_times_a.std() * 100 / finish_times_a.mean()) 
  shuffle_times_a = numpy.array(shuffle_times)
  shuffle_times_s = (min(shuffle_times), max(shuffle_times), shuffle_times_a.mean(), shuffle_times_a.std(), shuffle_times_a.std() * 100 / shuffle_times_a.mean())
  merge_times_a = numpy.array(merge_times)
  merge_times_s = (min(merge_times), max(merge_times), merge_times_a.mean(), merge_times_a.std(), merge_times_a.std() * 100 / merge_times_a.mean()) 
  reduce_times_a = numpy.array(reduce_times)
  reduce_times_s = (min(reduce_times), max(reduce_times), reduce_times_a.mean(), reduce_times_a.std(), reduce_times_a.std() * 100 / reduce_times_a.mean()) 

  f = '%s%d %d %f %f'
  s = ('', '', '', '', '')

  i = -1
  if len(sys.argv) > 2:
    if sys.argv[2] == 'w':
      print '|| Task\Time ||   Min   ||   Max   ||   Avg   || Std Dev || Rel Std Dev'
      f = '|| %-9s || %7.2f || %7.2f || %7.2f || %7.2f || %7.2f%%'
      s = ('Start', 'Map', 'Shuffle', 'Merge', 'Reduce', 'Finish')
    elif sys.argv[2] == 'min':
      i = 0
    elif sys.argv[2] == 'max':
      i = 1
    elif sys.argv[2] == 'avg':
      i = 2

  if i != -1:
    print start_times_s[i], map_times_s[i], shuffle_times_s[i], merge_times_s[i], reduce_times_s[i], finish_times_s[i]
    sys.exit(0)

  print f % ((s[0], ) + start_times_s)
  print f % ((s[1], ) + map_times_s)
  print f % ((s[2], ) + shuffle_times_s)
  print f % ((s[3], ) + merge_times_s)
  print f % ((s[4], ) + reduce_times_s)
  print f % ((s[5], ) + finish_times_s)

def tracker():
  nodes = {}
  tk = []
  for k,v in reduceTracker.items():
    # tracker_asterix-001:localhost/127\\.0\\.0\\.1:58189
    l = len('tracker_asterix-')
    n = v[l:l + 3]
    p = (reduceStartTime[k], reduceShuffleTime[k])
    tk.append(p)
    try:
      nodes[n].append(p)
    except:
      nodes[n] = [p]
  tk.sort(lambda x, y: cmp(x[0], y[0]))
  tk = [(int(t[0] - submitTime), int(t[1] - submitTime)) for t in tk]
  keys = nodes.keys()
  keys.sort()
  for k in keys:
    print k
    tasks = nodes[k]
    tasks.sort(lambda x, y: cmp(x[0], y[0]))
    tot = 0
    for t in tasks:
      a = t[1] - t[0]
      tf = (int(t[0] - submitTime), int(t[1] - submitTime))
      tot += a
#       print '%d' % a, 
      print '\t%2d' % int(a), tf 
    print '\t%d' % tot

def bytes():
  b = mapBytesReadLocal.values()
  b_a = numpy.array(b)
  print min(b), max(b), b_a.mean(), b_a.std(), sum(b)
  b = mapBytesReadHDFS.values()
  b_a = numpy.array(b)
  print min(b), max(b), b_a.mean(), b_a.std(), sum(b)

def count():
#   pp.pprint(jobCounters['Map-Reduce Framework'])
#   print jobCounters['Map-Reduce Framework']['Reduce input records']
#   print jobCounters['Map-Reduce Framework']['Map input records']
  print jobCounters['File Systems']['HDFS bytes read'], '\t', 
  print jobCounters['File Systems']['Local bytes read']

  v = []
  v2 = []
  for map in mapTaskCounters.values():
#     pp.pprint(map)
#     v.append(int(map['Map-Reduce Framework']['Map input records']))
    b = int(map['File Systems']['HDFS bytes read'])
    v.append(b)
    print b, '\t', 
    b = int(map['File Systems'].get('Local bytes read', 0))
    v2.append(b)
    print b
  print sum(v), '\t', sum(v2)
  print len(v), '\t', len(v2)

if len(sys.argv) < 2:
  print 'Usage: %s t|s|k' % sys.argv[0]
  print '\tt : timeline'
  print '\ts : stats'
  print '\tk : tracker'
  sys.exit(255)

pp = pprint.PrettyPrinter()
if sys.argv[1] == 't':
  f = 1
  scale = int(scale) / f
  parse_all()
  timeline()
elif sys.argv[1] == 's':
  parse_all()
  stats()
elif sys.argv[1] == 'k':
  scale = int(scale)
  parse_all()
  tracker()
elif sys.argv[1] == 'b':
  parse_all()
  bytes()
elif sys.argv[1] == 'c':
  parse_all()
  count()

