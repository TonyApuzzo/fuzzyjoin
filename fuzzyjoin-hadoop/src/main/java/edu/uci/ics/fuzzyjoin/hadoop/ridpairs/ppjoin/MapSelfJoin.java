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
import java.util.Collection;
import java.util.HashSet;

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
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ValueSelfJoin;
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
 *         KEY1: Unused
 * 
 *         VALUE1: Record
 * 
 *         KEY2: Token Group, Length
 * 
 *         VALUE2: RID, Tokens
 */
public class MapSelfJoin extends MapReduceBase implements
        Mapper<Object, Text, IntPairWritable, ValueSelfJoin> {

    private int[] dataColumns;
    private final IntPairWritable outputKey = new IntPairWritable();
    private final ValueSelfJoin outputValue = new ValueSelfJoin();
    private RecordGroup recordGroup;
    private SimilarityFilters similarityFilters;
    private Tokenizer tokenizer;
    private final TokenRank tokenRank = new TokenRankFrequency();

    @Override
    public void configure(JobConf job) {
        //
        // set Tokenizer and SimilarityFilters
        //
        tokenizer = TokenizerFactory.getTokenizer(job.get(
                FuzzyJoinConfig.TOKENIZER_PROPERTY,
                FuzzyJoinConfig.TOKENIZER_VALUE),
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);
        String similarityName = job.get(
                FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
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
                tokensPath = new Path(
                        job.get(FuzzyJoinConfig.DATA_TOKENS_PROPERTY));
                try {
                    lengthstatsPath = new Path(
                            job.get(FuzzyJoinDriver.DATA_LENGTHSTATS_PROPERTY));
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
        recordGroup = RecordGroupFactory.getRecordGroup(recordGroupClass,
                Math.max(1, job.getNumReduceTasks() * recordGroupFactor),
                similarityFilters, "" + lengthstatsPath);
        TokenLoad tokenLoad = new TokenLoad(tokensPath.toString(), tokenRank);
        tokenLoad.loadTokenRank();
        //
        // set dataColumn
        //
        dataColumns = FuzzyJoinUtil.getDataColumns(job.get(
                FuzzyJoinConfig.RECORD_DATA_PROPERTY,
                FuzzyJoinConfig.RECORD_DATA_VALUE));
    }

    public void map(Object unused, Text inputValue,
            OutputCollector<IntPairWritable, ValueSelfJoin> output,
            Reporter reporter) throws IOException {
        //
        // get RID and Tokens
        //
        String splits[] = inputValue.toString().split(
                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);
        int rid = Integer.valueOf(splits[FuzzyJoinConfig.RECORD_KEY]);
        Collection<Integer> tokensRanked = tokenRank.getTokenRanks(tokenizer
                .tokenize(FuzzyJoinUtil.getData(splits, dataColumns,
                        FuzzyJoinConfig.TOKEN_SEPARATOR)));
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
        // Key
        //
        outputKey.setSecond(length);
        //
        // Value
        //
        outputValue.setRID(rid);
        outputValue.setTokens(tokensRanked);
        //
        // output one pair per group
        //
        for (Integer group : tokenGroups) {
            //
            // Key
            //
            outputKey.setFirst(group);
            //
            // collect
            //
            output.collect(outputKey, outputValue);
        }
    }
}
