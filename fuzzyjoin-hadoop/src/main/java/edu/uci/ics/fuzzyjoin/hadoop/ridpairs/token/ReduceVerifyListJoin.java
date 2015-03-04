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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ValueJoin;
import edu.uci.ics.fuzzyjoin.similarity.PartialIntersect;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetric;

/**
 * @author rares
 * 
 *         KEY1: Token, Relation
 * 
 *         VALUE1: Relation, RID, Tokens, Length
 * 
 *         KEY2: "RID1 RID2 similarity"
 * 
 *         VALUE2: Unused
 * 
 */
public class ReduceVerifyListJoin extends MapReduceBase implements
        Reducer<IntPairWritable, ValueJoin, Text, NullWritable> {

    // private static enum Counters {
    // PAIRS_BUILD, PAIRS_FILTERED, PAIRS_VERIFIED
    // }

    private float similarityThreshold;
    private SimilarityFilters similarityFilters;
    // private SimilarityMetric similarityMetric;
    private final ArrayList<ValueJoin> records = new ArrayList<ValueJoin>();
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

    public void reduce(IntPairWritable key, Iterator<ValueJoin> values,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        ValueJoin rec = null;
        boolean firstSRecordSet = false;

        while (values.hasNext()) {
            rec = values.next();
            if (rec.getRelation() != 0) {
                firstSRecordSet = true;
                break;
            }

            ValueJoin recCopy = new ValueJoin(rec);
            records.add(recCopy);
        }

        if (!records.isEmpty()) {
            while (firstSRecordSet || values.hasNext()) {
                if (firstSRecordSet) {
                    firstSRecordSet = false;
                } else {
                    rec = values.next();
                }

                int[] tokens2Trimmed = rec.getTokens();
                int[] tokens2;
                int length2 = rec.getLength();
                if (length2 == tokens2Trimmed.length) {
                    tokens2 = tokens2Trimmed;
                } else {
                    tokens2 = new int[length2];
                    int i = 0;
                    for (; i < tokens2Trimmed.length; i++) {
                        tokens2[i] = tokens2Trimmed[i];
                    }
                    // pad with non-existing tokens (for suffix filter)
                    for (; i < tokens2.length; i++) {
                        tokens2[i] = Integer.MAX_VALUE;
                    }
                }

                for (ValueJoin rec1 : records) {
                    // reporter.incrCounter(Counters.PAIRS_BUILD, 1);

                    int[] tokens1 = rec1.getTokens();
                    int length1 = tokens1.length;

                    if (!similarityFilters.passLengthFilter(length1, length2)) {
                        // reporter.incrCounter(Counters.PAIRS_FILTERED, 1);
                        continue;
                    }

                    PartialIntersect p = SimilarityMetric
                            .getPartialIntersectSize(tokens1, tokens2, key
                                    .getFirst());
                    if (!similarityFilters.passPositionFilter(p.intersectSize,
                            p.posXStop, length1, p.posYStop, length2)) {
                        continue;
                    }

                    if (!similarityFilters.passSuffixFilter(tokens1,
                            p.posXStart, tokens2, p.posYStart)) {
                        continue;
                    }

                    float similarity = similarityFilters.passSimilarityFilter(
                            tokens1, p.posXStop + 1, tokens2, p.posYStop + 1,
                            p.intersectSize);// similarityMetric.getSimilarity(tokens1,
                    // length1, tokens2, length2);
                    // reporter.incrCounter(Counters.PAIRS_VERIFIED, 1);
                    if (similarity > 0 // = similarityThreshold
                    ) {
                        text.set("" + rec1.getRID()
                                + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                                + rec.getRID()
                                + FuzzyJoinConfig.RIDPAIRS_SEPARATOR
                                + similarity);
                        output.collect(text, nullWritable);
                    }
                }
            }
            records.clear();
        }
    }
}
