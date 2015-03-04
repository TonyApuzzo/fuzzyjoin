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
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntArrayWritable;

/**
 * @author rares
 * 
 *         KEY: token
 * 
 *         VALUE: count
 * 
 *         OR
 * 
 *         VALUE: count, length
 * 
 *         OR
 * 
 *         VALUE: count, min, max(, length freqencies)
 * 
 */
public class ReduceAggregate extends MapReduceBase implements
        Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

    private boolean lengthStats;
    private final IntArrayWritable statsArray = new IntArrayWritable();

    @Override
    public void configure(JobConf job) {
        lengthStats = job.getBoolean(FuzzyJoinDriver.TOKENS_LENGTHSTATS_PROPERTY,
                FuzzyJoinDriver.TOKENS_LENGTHSTATS_VALUE);
    }

    public void reduce(Text key, Iterator<IntArrayWritable> values,
            OutputCollector<Text, IntArrayWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        HashMap<Integer, Integer> lengthFreq = new HashMap<Integer, Integer>();

        IntWritable[] stats = null;

        // System.out.println(key);
        // System.out.println("---");
        while (values.hasNext()) {
            IntArrayWritable value = values.next();
            // System.out.println(value);
            stats = (IntWritable[]) value.toArray();
            count += stats[0].get();
            if (stats.length == 2) { // count, length
                int length = stats[1].get();
                min = Math.min(min, length);
                max = Math.max(max, length);
                if (lengthStats) {
                    ReduceSort.addMap(lengthFreq, length);
                }
            } else if (stats.length > 2) { // count, min, max(, length
                // freqencies)
                min = Math.min(min, stats[1].get());
                max = Math.max(max, stats[2].get());
                for (int i = 3; i < stats.length; i += 2) {
                    ReduceSort.addMap(lengthFreq, stats[i].get(), stats[i + 1]
                            .get());
                }
            }
        }
        // System.out.println("---");

        if (count > 1) {
            stats[0].set(count);
            if (min == Integer.MAX_VALUE) { // none with length
                // nothing
            } else if (min == max) { // one with length
                IntWritable[] statsUpdated = new IntWritable[2]; // count and
                // length only
                statsUpdated[0] = stats[0];
                statsUpdated[1] = new IntWritable(min);
                stats = statsUpdated;
            } else { // more with length
                IntWritable[] statsUpdated;
                if (!lengthFreq.isEmpty()) {
                    statsUpdated = new IntWritable[3 + (lengthFreq.size() << 1)];
                    int i = 3;
                    for (Map.Entry<Integer, Integer> entry : lengthFreq
                            .entrySet()) {
                        statsUpdated[i++] = new IntWritable(entry.getKey());
                        statsUpdated[i++] = new IntWritable(entry.getValue());
                    }
                } else {
                    statsUpdated = new IntWritable[3]; // for min and max only
                }
                statsUpdated[0] = stats[0];
                statsUpdated[1] = new IntWritable(min);
                statsUpdated[2] = new IntWritable(max);
                stats = statsUpdated;
            }
        }

        statsArray.set(stats);
        output.collect(key, statsArray);
        // System.out.println(statsArray);
        // System.out.println("===");
    }
}
