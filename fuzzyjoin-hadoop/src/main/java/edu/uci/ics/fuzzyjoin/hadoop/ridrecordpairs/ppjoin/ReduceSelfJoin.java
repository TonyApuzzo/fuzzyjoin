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

package edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.ppjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.ValueSelfJoin;

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
        Reducer<Object, ValueSelfJoin, IntPairWritable, Text> {

    private final IntPairWritable outputKey = new IntPairWritable();
    private final Text outputValue = new Text();
    private float similarityThreshold;

    @Override
    public void configure(JobConf job) {
        similarityThreshold = job.getFloat(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
    }

    public void reduce(Object unused, Iterator<ValueSelfJoin> inputValues,
            OutputCollector<IntPairWritable, Text> output, Reporter reporter)
            throws IOException {
        HashMap<Integer, RIDRecord> indexRIDRecord = new HashMap<Integer, RIDRecord>();
        FuzzyJoinMemory fuzzyJoinMemory = new FuzzyJoinMemory(
                similarityThreshold);
        int crtRecordId = 0;

        while (inputValues.hasNext()) {
            ValueSelfJoin recordInfo = inputValues.next();
            int[] record = recordInfo.getTokens();

            indexRIDRecord.put(crtRecordId, new RIDRecord(recordInfo.getRID(),
                    recordInfo.getRecord()));
            crtRecordId++;

            ArrayList<ResultSelfJoin> results = fuzzyJoinMemory
                    .selfJoinAndAddRecord(record);
            for (ResultSelfJoin result : results) {
                RIDRecord ridRecord1 = indexRIDRecord.get(result.indexX);
                RIDRecord ridRecord2 = indexRIDRecord.get(result.indexY);
                if (ridRecord1.getRid() < ridRecord2.getRid()) {
                    RIDRecord ridRecord = ridRecord1;
                    ridRecord1 = ridRecord2;
                    ridRecord2 = ridRecord;
                }

                outputKey.setFirst(ridRecord1.getRid());
                outputKey.setSecond(ridRecord2.getRid());
                outputValue.set("" + ridRecord1.getRecord()
                        + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + result.similarity
                        + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR
                        + ridRecord2.getRecord());
                output.collect(outputKey, outputValue);
            }
        }
    }
}
