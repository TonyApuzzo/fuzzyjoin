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

package edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.ValueSelfJoin;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetric;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetricFactory;

/**
 * @author rares
 * 
 *         KEY1: Token
 * 
 *         VALUE1: RID, Tokens
 * 
 *         KEY2: RID1, RID2
 * 
 *         VALUE2: Record1, Similarity, Record2
 * 
 */
public class ReduceVerifyListSelfJoin extends MapReduceBase implements
        Reducer<IntWritable, ValueSelfJoin, IntPairWritable, Text> {

    // private static enum Counters {
    // PAIRS_BUILD, PAIRS_FILTERED, PAIRS_VERIFIED
    // }

    private final IntPairWritable outputKey = new IntPairWritable();
    private final Text outputValue = new Text();
    private final ArrayList<ValueSelfJoin> records = new ArrayList<ValueSelfJoin>();
    private SimilarityFilters similarityFilters;
    private SimilarityMetric similarityMetric;
    private float similarityThreshold;

    @Override
    public void configure(JobConf job) {
        //
        // set SimilarityFilters
        //
        String similarityName = job.get(FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_NAME_VALUE);
        similarityThreshold = job.getFloat(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
        similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
                similarityName, similarityThreshold);
        similarityMetric = SimilarityMetricFactory
                .getSimilarityMetric(similarityName);
    }

    public void reduce(IntWritable key, Iterator<ValueSelfJoin> values,
            OutputCollector<IntPairWritable, Text> output, Reporter reporter)
            throws IOException {
        while (values.hasNext()) {
            ValueSelfJoin rec = values.next();
            ValueSelfJoin recCopy = new ValueSelfJoin(rec);
            records.add(recCopy);
        }

        for (ValueSelfJoin rec1 : records) {
            for (ValueSelfJoin rec2 : records) {
                if (rec1 == rec2) {
                    continue;
                }
                int rid1 = rec1.getRID();
                int rid2 = rec2.getRID();
                if (rid1 < rid2) {
                    continue;
                }
                // reporter.incrCounter(Counters.PAIRS_BUILD, 1);

                int[] tokens1 = rec1.getTokens();
                int[] tokens2 = rec2.getTokens();
                if (!similarityFilters.passLengthFilter(tokens1.length,
                        tokens2.length)) {
                    // reporter.incrCounter(Counters.PAIRS_FILTERED, 1);
                    continue;
                }
                float similarity = similarityMetric.getSimilarity(tokens1,
                        tokens2);
                // reporter.incrCounter(Counters.PAIRS_VERIFIED, 1);
                if (similarity >= similarityThreshold) {
                    outputKey.setFirst(rid1);
                    outputKey.setSecond(rid2);
                    outputValue.set("" + rec1.getRecord()
                            + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + similarity
                            + FuzzyJoinConfig.RECORD_EXTRA_SEPARATOR + rec2.getRecord());
                    output.collect(outputKey, outputValue);
                }
            }
        }
        records.clear();
    }
}
