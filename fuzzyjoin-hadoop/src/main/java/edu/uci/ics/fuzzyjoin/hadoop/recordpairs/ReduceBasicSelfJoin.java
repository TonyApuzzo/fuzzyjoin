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

package edu.uci.ics.fuzzyjoin.hadoop.recordpairs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;

/**
 * @author rares
 * 
 *         KEY1: RID, 0/1 (0: Record, 1: RID Similarity)
 * 
 *         VALUE1: Record/"RID Similarity"
 * 
 *         KEY2: RID1, RID2
 * 
 *         VALUE2: "Similarity;Record"
 */
public class ReduceBasicSelfJoin extends MapReduceBase implements
        Reducer<IntPairWritable, Text, IntPairWritable, Text> {

    private final IntPairWritable outputKey = new IntPairWritable();
    private final Text outputValue = new Text();

    public void reduce(IntPairWritable inputKey, Iterator<Text> inputValue,
            OutputCollector<IntPairWritable, Text> output, Reporter reporter)
            throws IOException {
        String record = inputValue.next().toString();
        if (!inputValue.hasNext()) {
            return;
        }

        int rid1 = inputKey.getFirst();
        HashSet<Integer> rids = new HashSet<Integer>();
        while (inputValue.hasNext()) {
            String value = inputValue.next().toString();
            String valueSplit[] = value
                    .split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);
            Integer rid2 = Integer.parseInt(valueSplit[0]);
            if (!rids.contains(rid2)) {
                rids.add(rid2);

                if (rid1 < rid2) {
                    outputKey.set(rid1, rid2);
                } else {
                    outputKey.set(rid2, rid1);
                }
                outputValue.set(valueSplit[1]
                        + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + record);
                output.collect(outputKey, outputValue);
            }
        }
        rids.clear();
    }
}
