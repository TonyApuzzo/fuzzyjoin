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

package edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ppjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ValueSelfJoin;

/**
 * @author rares
 * 
 *         KEY1: token group, lenght
 * 
 *         VALUE1: record info: RID, [tokens]
 * 
 *         KEY2: RID pair and similarity
 * 
 *         VALUE2: unused
 */
public class ReduceSelfJoin extends MapReduceBase implements
        Reducer<Object, ValueSelfJoin, Text, NullWritable> {

    private float similarityThreshold;
    private final Text outputKey = new Text();
    private final NullWritable outputValue = NullWritable.get();

    @Override
    public void configure(JobConf job) {
        similarityThreshold = job.getFloat(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
    }

    public void reduce(Object unused, Iterator<ValueSelfJoin> inputValues,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        HashMap<Integer, Integer> rids = new HashMap<Integer, Integer>();
        FuzzyJoinMemory fuzzyJoinMemory = new FuzzyJoinMemory(
                similarityThreshold);
        int crtRecordId = 0;

        while (inputValues.hasNext()) {
            ValueSelfJoin recordInfo = inputValues.next();
            int[] record = recordInfo.getTokens();

            rids.put(crtRecordId, recordInfo.getRID());
            crtRecordId++;

            ArrayList<ResultSelfJoin> results = fuzzyJoinMemory
                    .selfJoinAndAddRecord(record);
            for (ResultSelfJoin result : results) {
                int rid1 = rids.get(result.indexX);
                int rid2 = rids.get(result.indexY);
                if (rid1 < rid2) {
                    int rid = rid1;
                    rid1 = rid2;
                    rid2 = rid;
                }

                outputKey.set("" + rid1 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                        + rid2 + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                        + result.similarity);
                output.collect(outputKey, outputValue);
            }
        }
    }
}
