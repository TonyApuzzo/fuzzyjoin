/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.fuzzyjoin.hadoop.datagen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;

public class MapRIDRecord extends MapReduceBase implements
        Mapper<Object, Text, IntWritable, Text> {

    private IntWritable outputKey = new IntWritable();
    private int noRecords;

    @Override
    public void configure(JobConf job) {
        noRecords = job.getInt(FuzzyJoinDriver.DATA_NORECORDS_PROPERTY, -1);
    }

    public void map(Object unused, Text inputValue,
            OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
        String splits[] = inputValue.toString().split(
                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
        int rid = Integer.valueOf(splits[FuzzyJoinConfig.RECORD_KEY]);
        if (noRecords == -1 || rid <= noRecords) {
            outputKey.set(rid);
            output.collect(outputKey, inputValue);
        }
    }

}