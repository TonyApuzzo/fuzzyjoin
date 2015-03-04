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

package edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;

/**
 * @author rares
 * 
 *         KEY1: not used
 * 
 *         VALUE1: record (e.g.,
 *         "unused_attribute:RID:unused_attribute:join_attribute:unused_attribute"
 *         )
 * 
 *         KEY2: token
 * 
 *         VALUE2: count
 * 
 */
public class Map extends MapReduceBase implements
        Mapper<Object, Text, Text, IntWritable> {

    private int[] dataColumns;
    private final IntWritable one = new IntWritable(1);
    protected final Text token = new Text();
    private Tokenizer tokenizer;

    @Override
    public void configure(JobConf job) {
        tokenizer = TokenizerFactory.getTokenizer(job.get(
                FuzzyJoinConfig.TOKENIZER_PROPERTY,
                FuzzyJoinConfig.TOKENIZER_VALUE),
                FuzzyJoinConfig.WORD_SEPARATOR_REGEX,
                FuzzyJoinConfig.TOKEN_SEPARATOR);
        //
        // set dataColumn
        //
        dataColumns = FuzzyJoinUtil.getDataColumns(job.get(
                FuzzyJoinConfig.RECORD_DATA_PROPERTY,
                FuzzyJoinConfig.RECORD_DATA_VALUE));
    }

    protected Collection<String> getTokens(Text record) {
        return tokenizer.tokenize(FuzzyJoinUtil
                .getData(
                        record.toString().split(
                                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX),
                        dataColumns, FuzzyJoinConfig.TOKEN_SEPARATOR));
    }

    public void map(Object unused, Text record,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        Collection<String> tokens = getTokens(record);

        for (String tokenString : tokens) {
            token.set(tokenString);
            output.collect(token, one);
        }
    }
}
