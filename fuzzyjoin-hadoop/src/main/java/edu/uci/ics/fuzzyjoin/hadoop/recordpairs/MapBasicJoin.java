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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleWritable;

/**
 * @author rares
 * 
 *         KEY1: unused
 * 
 *         VALUE1: "RID:Record"/"RID-R RID-S Similarity"
 * 
 *         KEY2: 0/1 (0: Relation R, 1: Relation S), RID, 0/1 (0: Record, 1:
 *         "RID Similarity")
 * 
 *         VALUE2: Record/"RID Similarity"
 */
public class MapBasicJoin extends MapReduceBase implements
        Mapper<Object, Text, IntTripleWritable, Text> {

    private final IntTripleWritable outputKey = new IntTripleWritable();
    private final Text outputValue = new Text();
    private String suffixSecond;

    @Override
    public void configure(JobConf job) {
        //
        // get suffix for second relation
        //
        suffixSecond = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "")
                .split(FuzzyJoinDriver.SEPSARATOR_REGEX)[1];
    }

    public void map(Object unused, Text record,
            OutputCollector<IntTripleWritable, Text> output, Reporter reporter)
            throws IOException {
        String recordString = record.toString();
        if (record.find("" + FuzzyJoinConfig.RECORD_SEPARATOR) >= 0) {
            /*
             * VALUE1: RID:Record
             * 
             * KEY2: 0/1 (0: Relation R, 1: Relation S), RID, 0
             * 
             * VALUE2: Record
             */
            String valueSplit[] = recordString
                    .split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);

            int relation = 0;
            if (reporter.getInputSplit().toString().contains(suffixSecond)) {
                relation = 1;
            }

            outputKey.set(relation, Integer
                    .valueOf(valueSplit[FuzzyJoinConfig.RECORD_KEY]), 0);
            outputValue.set(record);
            output.collect(outputKey, outputValue);
        } else {
            /*
             * VALUE1: "RID-R RID-S Similarity"
             * 
             * KEY2: 0/1 (0: Relation R, 1: Relation S), RID, 1
             * 
             * VALUE2: "RIDOther Similarity"
             */
            String valueSplit[] = recordString
                    .split(FuzzyJoinConfig.RIDPAIRS_SEPARATOR_REGEX);

            outputKey.set(0, Integer.parseInt(valueSplit[0]), 1);
            outputValue.set(valueSplit[1] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + valueSplit[2]);
            output.collect(outputKey, outputValue);

            outputKey.set(1, Integer.parseInt(valueSplit[1]), 1);
            outputValue.set(valueSplit[0] + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                    + valueSplit[2]);
            output.collect(outputKey, outputValue);
        }
    }
}
