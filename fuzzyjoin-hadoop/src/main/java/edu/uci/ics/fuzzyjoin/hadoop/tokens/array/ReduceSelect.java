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

package edu.uci.ics.fuzzyjoin.hadoop.tokens.array;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntArrayWritable;

public class ReduceSelect extends MapReduceBase implements
        Reducer<IntArrayWritable, Text, Text, NullWritable> {

    private JobConf conf;
    private final HashMap<Integer, Integer> lengthFreq = new HashMap<Integer, Integer>();
    private int max = Integer.MIN_VALUE;
    private int min = Integer.MAX_VALUE;
    private final NullWritable nullWritable = NullWritable.get();
    private boolean lengthStats;

    @Override
    public void close() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String path = FileOutputFormat.getWorkOutputPath(conf).toString();

        FSDataOutputStream statsWriter = fs.create(new Path(path.toString()
                + "/" + FuzzyJoinDriver.DATA_LENGTH_STATS_FILE));
        statsWriter.writeInt(min);
        statsWriter.writeInt(max);
        for (Integer length : lengthFreq.keySet()) {
            statsWriter.writeInt(length);
            statsWriter.writeInt(lengthFreq.get(length));
        }
        statsWriter.close();

        // System.out.println(min);
        // System.out.println(max);
        // System.out.println(lengthFreq);
        // int sum = 0;
        // for (Integer freq : lengthFreq.values()) {
        // sum += freq;
        // }
        // System.out.println(sum);
    }

    @Override
    public void configure(JobConf job) {
        conf = job;
        lengthStats = job.getBoolean(FuzzyJoinDriver.TOKENS_LENGTHSTATS_PROPERTY,
                FuzzyJoinDriver.TOKENS_LENGTHSTATS_VALUE);
    }

    public void reduce(IntArrayWritable key, Iterator<Text> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        IntWritable[] stats = (IntWritable[]) key.toArray();

        if (stats.length == 2) {
            int length = stats[1].get();
            min = Math.min(min, length);
            max = Math.max(max, length);
        } else if (stats.length > 2) {
            min = Math.min(min, stats[1].get());
            max = Math.max(max, stats[2].get());
        }

        while (values.hasNext()) {
            output.collect(values.next(), nullWritable);
            // output.collect(new Text(key.get()[0] + " " + values.next()),
            // nullWritable);
            if (stats.length == 2 && lengthStats) {
                ReduceSort.addMap(lengthFreq, stats[1].get()); // for min
                // and max
                // only
            } else if (stats.length > 2) {
                for (int i = 3; i < stats.length; i += 2) {
                    ReduceSort.addMap(lengthFreq, stats[i].get(), stats[i + 1]
                            .get());
                }
            }
        }
    }

}
