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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceSort.TokenCount;

/**
 * @author rares
 * 
 *         KEY1: token
 * 
 *         VALUE1 (output): count, min length, max lenth, length freqency OR
 *         count, length freqency OR count only
 * 
 *         KEY2: token
 * 
 *         VALUE2: null
 * 
 * 
 */
public class ReduceSort extends MapReduceBase implements
        Reducer<Text, IntArrayWritable, Text, NullWritable> {

    public static void addMap(HashMap<Integer, Integer> mapResult, Integer key) {
        Integer freq = mapResult.get(key);
        if (freq == null) {
            freq = new Integer(1);
        } else {
            freq += 1;
        }
        mapResult.put(key, freq);
    }

    public static void addMap(HashMap<Integer, Integer> mapResult, Integer key,
            Integer value) {
        Integer freq = mapResult.get(key);
        if (freq == null) {
            freq = value;
        } else {
            freq += value;
        }
        mapResult.put(key, freq);
    }

    public static void addMaps(HashMap<Integer, Integer> mapAResult,
            HashMap<Integer, Integer> mapB) {
        for (Map.Entry<Integer, Integer> entry : mapB.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();
            if (mapAResult.containsKey(key)) {
                mapAResult.put(key, mapAResult.get(key) + value);
            } else {
                mapAResult.put(key, value);
            }
        }
    }

    private JobConf conf;
    private final HashMap<Integer, Integer> lengthFreq = new HashMap<Integer, Integer>();
    private boolean lengthStats;
    private int max = Integer.MIN_VALUE;
    private int min = Integer.MAX_VALUE;
    private final NullWritable nullWritable = NullWritable.get();
    private OutputCollector<Text, NullWritable> output;
    private final Text token = new Text();
    private ArrayList<TokenCount> tokenCounts = new ArrayList<TokenCount>();

    @Override
    public void close() throws IOException {
        Collections.sort(tokenCounts, new Comparator<TokenCount>() {
            public int compare(TokenCount tc1, TokenCount tc2) {
                return tc1.count - tc2.count;
            }
        });

        for (TokenCount tokenCount : tokenCounts) {
            token.set(tokenCount.token);
            output.collect(token, nullWritable);
        }

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

    public void reduce(Text key, Iterator<IntArrayWritable> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;
        while (values.hasNext()) {
            IntWritable[] stats = (IntWritable[]) values.next().toArray();

            int countCrt, minCrt, maxCrt;
            countCrt = stats[0].get();
            if (stats.length > 1) {
                if (stats.length == 2) {
                    int length = stats[1].get();
                    minCrt = maxCrt = length;
                    if (lengthStats) {
                        addMap(lengthFreq, length); // for min and max only
                    }
                } else {
                    minCrt = stats[1].get();
                    maxCrt = stats[2].get();

                    for (int i = 3; i < stats.length; i += 2) {
                        addMap(lengthFreq, stats[i].get(), stats[i + 1].get());
                    }
                }
                min = Math.min(min, minCrt);
                max = Math.max(max, maxCrt);
            }
            count += countCrt;
        }

        this.output = output;
        tokenCounts.add(new TokenCount(key.toString(), count));
    }
}
