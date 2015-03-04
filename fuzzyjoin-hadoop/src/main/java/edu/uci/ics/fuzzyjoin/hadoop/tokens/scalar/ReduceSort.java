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

package edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author rares
 * 
 *         KEY1: token
 * 
 *         VALUE1: count
 * 
 *         KEY1: token
 * 
 *         VALUE2: null
 * 
 * 
 */
public class ReduceSort extends MapReduceBase implements
        Reducer<Text, IntWritable, Text, NullWritable> {

    public static class TokenCount implements Comparable<TokenCount> {
        public int count;
        public String token;

        public TokenCount(String token, int count) {
            this.token = token;
            this.count = count;
        }

        @Override
        public int compareTo(TokenCount o) {
            if (count == o.count) {
                return token.compareTo(o.token);
            }
            return count - o.count;
        }
    }

    private final NullWritable nullWritable = NullWritable.get();
    private OutputCollector<Text, NullWritable> output;
    private final Text token = new Text();
    private ArrayList<TokenCount> tokenCounts = new ArrayList<TokenCount>();

    @Override
    public void close() throws IOException {
        Collections.sort(tokenCounts);

        for (TokenCount tokenCount : tokenCounts) {
            token.set(tokenCount.token);
            output.collect(token, nullWritable);
        }
    }

    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }

        this.output = output;
        tokenCounts.add(new TokenCount(key.toString(), count));
    }
}
