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
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;

/**
 * @author rares
 * 
 *         KEY1: unused
 * 
 *         VALUE1: "Similarity;Record"
 * 
 *         KEY2: "Record1;Similarity;Record2"
 * 
 *         VALUE2: null
 */
public class Reduce extends MapReduceBase implements
        Reducer<Object, Text, Text, NullWritable> {

    private boolean isSelfJoin;

    private final Text outputKey = new Text();

    private final NullWritable outputValue = NullWritable.get();

    @Override
    public void configure(JobConf job) {
        isSelfJoin = "".equals(job.get(
                FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, ""));
    }

    public void reduce(Object unused, Iterator<Text> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        String records[] = new String[2];
        String splits[] = values.next().toString()
                .split(FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR_REGEX);
        String sim = splits[0];
        records[0] = splits[1];

        int rid0, rid1;
        rid0 = Integer
                .valueOf(records[0]
                        .split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX)[FuzzyJoinConfig.RECORD_KEY]);

        // need to loop and check RIDs because MapBroadcast might send
        // duplicates
        do {
            records[1] = values.next().toString()
                    .split(FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR_REGEX)[1];
            rid1 = Integer
                    .valueOf(records[1]
                            .split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX)[FuzzyJoinConfig.RECORD_KEY]);

            if ((isSelfJoin && rid0 != rid1)
                    || (!isSelfJoin && !records[0].equals(records[1]))) {
                /*
                 * if you want to have correct job counters (i.e. map output
                 * records = reduce input records)
                 */
                // while (values.hasNext()) {
                // values.next();
                // }
                break;
            }
        } while (values.hasNext());

        int i0 = 0, i1 = 1;
        if (isSelfJoin && rid0 > rid1) {
            i0 = 1;
            i1 = 0;
        }
        outputKey.set(records[i0] + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR
                + sim + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + records[i1]);
        output.collect(outputKey, outputValue);
    }
}
