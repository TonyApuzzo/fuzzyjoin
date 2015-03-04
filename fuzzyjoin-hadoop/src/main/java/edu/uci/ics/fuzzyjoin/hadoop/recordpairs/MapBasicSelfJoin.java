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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;

/**
 * @author rares
 * 
 *         KEY1: unused
 * 
 *         VALUE1: "RID:Record"/"RID1 RID2 Similarity"
 * 
 *         KEY2: RID, 0/1 (0: Record, 1: "RID Similarity")
 * 
 *         VALUE2: Record/"RID Similarity"
 */
public class MapBasicSelfJoin extends MapReduceBase implements
        Mapper<Object, Text, IntPairWritable, Text> {

    private final IntPairWritable outputKey = new IntPairWritable();
    private final Text outputValue = new Text();

    public void map(Object unused, Text inputValue,
            OutputCollector<IntPairWritable, Text> output, Reporter reporter)
            throws IOException {
        String recordString = inputValue.toString();
        if (inputValue.find("" + FuzzyJoinConfig.RECORD_SEPARATOR) >= 0) {
            /*
             * VALUE1: RID:Record
             * 
             * KEY2: RID, 0
             * 
             * VALUE2: Record
             */
            String valueSplit[] = recordString
                    .split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
            outputKey.set(Integer
                    .valueOf(valueSplit[FuzzyJoinConfig.RECORD_KEY]), 0);
            outputValue.set(inputValue);
            output.collect(outputKey, outputValue);
        } else {
            /*
             * VALUE1: "RID1 RID2 Similarity"
             * 
             * KEY2: RID1, 1 and RID2, 1
             * 
             * VALUE2: "RID2 Similarity" and "RID1 Similarity"
             */
            String valueSplit[] = recordString
                    .split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);

            outputKey.set(Integer.parseInt(valueSplit[0]), 1);
            outputValue.set(valueSplit[1] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + valueSplit[2]);
            output.collect(outputKey, outputValue);

            outputKey.set(Integer.parseInt(valueSplit[1]), 1);
            outputValue.set(valueSplit[0] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + valueSplit[2]);
            output.collect(outputKey, outputValue);
        }
    }
}
