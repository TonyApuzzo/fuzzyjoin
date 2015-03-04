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

package edu.uci.ics.fuzzyjoin.hadoop.ridpairs.token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ValueSelfJoin;
import edu.uci.ics.fuzzyjoin.similarity.PartialIntersect;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetric;

/**
 * @author rares
 * 
 *         KEY1: Token
 * 
 *         VALUE1: RID, Tokens
 * 
 *         KEY2: "RID1 RID2 similarity"
 * 
 *         VALUE2: Unused
 * 
 */
public class ReduceVerifyListSelfJoin extends MapReduceBase implements
        Reducer<IntWritable, ValueSelfJoin, Text, NullWritable> {

    // private static enum Counters {
    // PAIRS_BUILD, PAIRS_FILTERED, PAIRS_VERIFIED
    // }

    private float similarityThreshold;
    private SimilarityFilters similarityFilters;
    // private SimilarityMetric similarityMetric;
    private final ArrayList<ValueSelfJoin> records = new ArrayList<ValueSelfJoin>();
    private final Text text = new Text();
    private final NullWritable nullWritable = NullWritable.get();

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
        // similarityMetric = SimilarityMetricFactory
        // .getSimilarityMetric(similarityName);
    }

    public void reduce(IntWritable key, Iterator<ValueSelfJoin> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        while (values.hasNext()) {
            ValueSelfJoin rec = values.next();
            ValueSelfJoin recCopy = new ValueSelfJoin(rec);
            records.add(recCopy);
        }

        for (int i = 0; i < records.size(); i++) {
            ValueSelfJoin rec1 = records.get(i);
            for (int j = i + 1; j < records.size(); j++) {
                ValueSelfJoin rec2 = records.get(j);
                // reporter.incrCounter(Counters.PAIRS_BUILD, 1);

                int[] tokens1 = rec1.getTokens();
                int[] tokens2 = rec2.getTokens();
                if (!similarityFilters.passLengthFilter(tokens1.length,
                        tokens2.length)) {
                    // reporter.incrCounter(Counters.PAIRS_FILTERED, 1);
                    continue;
                }

                PartialIntersect p = SimilarityMetric.getPartialIntersectSize(
                        tokens1, tokens2, key.get());
                if (!similarityFilters.passPositionFilter(p.intersectSize,
                        p.posXStop, tokens1.length, p.posYStop, tokens2.length)) {
                    continue;
                }

                if (!similarityFilters.passSuffixFilter(tokens1, p.posXStart,
                        tokens2, p.posYStart)) {
                    continue;
                }

                float similarity = similarityFilters.passSimilarityFilter(
                        tokens1, p.posXStop + 1, tokens2, p.posYStop + 1,
                        p.intersectSize);
                // similarityMetric.getSimilarity(tokens1,
                // tokens2);
                // reporter.incrCounter(Counters.PAIRS_VERIFIED, 1);

                if (similarity > 0 // = similarityThreshold
                ) {
                    int ridA = rec1.getRID();
                    int ridB = rec2.getRID();
                    if (ridA < ridB) {
                        int rid = ridA;
                        ridA = ridB;
                        ridB = rid;
                    }
                    text.set("" + ridA + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                            + ridB + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                            + similarity);
                    output.collect(text, nullWritable);
                }
            }
        }
        records.clear();
    }
}
