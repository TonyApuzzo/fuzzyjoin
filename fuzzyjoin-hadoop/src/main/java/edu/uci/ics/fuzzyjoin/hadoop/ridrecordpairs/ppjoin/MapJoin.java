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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.ValueJoin;
import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroup;
import edu.uci.ics.fuzzyjoin.recordgroup.RecordGroupFactory;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenLoad;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenRank;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenRankFrequency;

/**
 * @author rares
 * 
 *         KEY1: not used
 * 
 *         VALUE1: record (e.g.,
 *         "unused_attribute:RID:unused_attribute:join_attribute:unused_attribute"
 *         )
 * 
 *         KEY2: group, length, relation
 * 
 *         VALUE2: record: relation, RID, length, tokens
 */
public class MapJoin extends MapReduceBase implements
        Mapper<Object, Text, IntTripleWritable, ValueJoin> {

    private int[][] dataColumns = new int[2][];
    private final IntTripleWritable outputKey = new IntTripleWritable();
    private final ValueJoin outputValue = new ValueJoin();
    private RecordGroup recordGroup;
    private SimilarityFilters similarityFilters;
    private String suffixSecond;
    private Tokenizer tokenizer;
    private final TokenRank tokenRank = new TokenRankFrequency();

    @Override
    public void configure(JobConf job) {
        //
        // set Tokenizer and SimilarityFilters
        //
        tokenizer = TokenizerFactory.getTokenizer(job.get(
                FuzzyJoinConfig.TOKENIZER_PROPERTY, FuzzyJoinConfig.TOKENIZER_VALUE),
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX, FuzzyJoinConfig.TOKEN_SEPARATOR);
        String similarityName = job.get(FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_NAME_VALUE);
        float similarityThreshold = job.getFloat(
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE);
        similarityFilters = SimilarityFiltersFactory.getSimilarityFilters(
                similarityName, similarityThreshold);
        //
        // set TokenRank and TokenGroup
        //
        Path tokensPath;
        Path lengthstatsPath = null;
        try {
            Path[] cache = DistributedCache.getLocalCacheFiles(job);
            if (cache == null) {
                tokensPath = new Path(job.get(FuzzyJoinConfig.DATA_TOKENS_PROPERTY));
                try {
                    lengthstatsPath = new Path(job
                            .get(FuzzyJoinDriver.DATA_LENGTHSTATS_PROPERTY));
                } catch (IllegalArgumentException e) {
                }
            } else {
                tokensPath = cache[0];
                if (job.getBoolean(FuzzyJoinDriver.TOKENS_LENGTHSTATS_PROPERTY,
                        FuzzyJoinDriver.TOKENS_LENGTHSTATS_VALUE)) {
                    lengthstatsPath = cache[1];
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
        String recordGroupClass = job.get(
                FuzzyJoinDriver.RIDPAIRS_GROUP_CLASS_PROPERTY,
                FuzzyJoinDriver.RIDPAIRS_GROUP_CLASS_VALUE);
        int recordGroupFactor = job.getInt(
                FuzzyJoinDriver.RIDPAIRS_GROUP_FACTOR_PROPERTY,
                FuzzyJoinDriver.RIDPAIRS_GROUP_FACTOR_VALUE);
        recordGroup = RecordGroupFactory.getRecordGroup(recordGroupClass, Math
                .max(1, job.getNumReduceTasks() * recordGroupFactor),
                similarityFilters, "" + lengthstatsPath);
        TokenLoad tokenLoad = new TokenLoad(tokensPath.toString(), tokenRank);
        tokenLoad.loadTokenRank();
        //
        // set dataColumn
        //
        dataColumns[0] = FuzzyJoinUtil.getDataColumns(job.get(
                FuzzyJoinConfig.RECORD_DATA_PROPERTY, FuzzyJoinConfig.RECORD_DATA_VALUE));
        dataColumns[1] = FuzzyJoinUtil.getDataColumns(job.get(
                FuzzyJoinConfig.RECORD_DATA1_PROPERTY, FuzzyJoinConfig.RECORD_DATA1_VALUE));
        //
        // get suffix for second relation
        //
        suffixSecond = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "")
                .split(FuzzyJoinDriver.SEPSARATOR_REGEX)[1];
    }

    public void map(Object unused, Text inputValue,
            OutputCollector<IntTripleWritable, ValueJoin> output,
            Reporter reporter) throws IOException {
        //
        // get RID and Tokens
        //
        String splits[] = inputValue.toString().split(
                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);

        int relation = 0;
        if (reporter.getInputSplit().toString().contains(suffixSecond)) {
            relation = 1;
        }
        int rid = Integer.valueOf(splits[FuzzyJoinConfig.RECORD_KEY]);
        List<String> tokensUnranked = tokenizer.tokenize(FuzzyJoinUtil.getData(
                inputValue.toString().split(FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                dataColumns[relation], FuzzyJoinConfig.TOKEN_SEPARATOR));
        Collection<Integer> tokensRanked = tokenRank
                .getTokenRanks(tokensUnranked);
        //
        // get Tokens as a DataBag and token groups
        //
        int length = tokensRanked.size();
        HashSet<Integer> tokenGroups = new HashSet<Integer>();
        int prefixLength = similarityFilters.getPrefixLength(length);
        int position = 0;
        if (recordGroup.isLengthOnly()) {
            for (Integer group : recordGroup.getGroups(0, length)) {
                tokenGroups.add(group);
            }
        }
        for (Integer token : tokensRanked) {
            if (!recordGroup.isLengthOnly()) {
                if (position < prefixLength) {
                    for (Integer group : recordGroup.getGroups(token, length)) {
                        tokenGroups.add(group);
                    }
                }
                position++;
            }
        }
        //
        // set key length
        //
        int lengthKey = similarityFilters.getLengthLowerBound(length);
        if (relation == 1) {
            length = lengthKey = tokensUnranked.size();
        }

        //
        // Key
        //
        outputKey.setSecond(lengthKey);
        outputKey.setThird(relation);
        //
        // Value
        //
        outputValue.setRelation(relation);
        outputValue.setRID(rid);
        outputValue.setLength(length);
        outputValue.setTokens(tokensRanked);
        outputValue.setRecord(inputValue);
        //
        // output one pair per group
        //
        for (Integer group : tokenGroups) {
            //
            // Key
            //
            outputKey.setFirst(group);
            //
            // Collect
            //
            output.collect(outputKey, outputValue);
        }
    }
}
