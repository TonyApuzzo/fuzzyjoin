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

package edu.uci.ics.fuzzyjoin.hadoop.datagen;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.FuzzyJoinUtil;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.tokenizer.Tokenizer;
import edu.uci.ics.fuzzyjoin.tokenizer.TokenizerFactory;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenLoad;

public class MapNewRecord extends MapReduceBase implements
        Mapper<Object, Text, Text, NullWritable> {

    private int[] dataColumns;
    private final NullWritable nullWritable = NullWritable.get();
    private int offsetRID;
    private RecordGenerator recordGenerator;
    private final Text recordNew = new Text();
    private Tokenizer tokenizer;

    @Override
    public void configure(JobConf job) {
        //
        // create RecordGenerator
        //
        int offset = job.getInt(FuzzyJoinDriver.DATA_CRTCOPY_PROPERTY, -1);
        if (offset == -1) {
            System.err.println("ERROR: fuzzyjoin.data.crtcopy not set.");
            System.exit(-1);
        }
        recordGenerator = new RecordGenerator(offset);
        int noRecords = job.getInt(FuzzyJoinDriver.DATA_NORECORDS_PROPERTY, -1);
        if (noRecords == -1) {
            System.err.println("ERROR: fuzzyjoin.data.norecords not set.");
            System.exit(-1);
        }
        offsetRID = offset * noRecords;
        int dictionaryFactor = job.getInt(
                FuzzyJoinDriver.DATA_DICTIONARY_FACTOR_PROPERTY, 1);
        //
        // set RecordGenerator
        //
        Path tokenRankFile;
        try {
            Path[] cache = DistributedCache.getLocalCacheFiles(job);
            if (cache == null) {
                tokenRankFile = new Path(
                        job.get(FuzzyJoinConfig.DATA_TOKENS_PROPERTY));
            } else {
                tokenRankFile = cache[0];
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TokenLoad tokenLoad = new TokenLoad(tokenRankFile.toString(),
                recordGenerator);
        tokenLoad.loadTokenRank(dictionaryFactor);
        //
        // set Tokenizer
        //
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
        // Arrays.sort(dataColumns);
    }

    public void map(Object unused, Text record,
            OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
        String splits[] = record.toString().split(
                FuzzyJoinConfig.RECORD_SEPARATOR_REGEX);

        String data = FuzzyJoinUtil.getData(splits, dataColumns,
                FuzzyJoinConfig.TOKEN_SEPARATOR);
        String newData = recordGenerator.generate(tokenizer.tokenize(data));

        // List<String> newTokens = tokenizer.tokenize(newData);
        // int[] tokensCount = new int[dataColumns.length];
        // for (int i = 0; i < dataColumns.length; ++i) {
        // String s = splits[dataColumns[i]];
        // int c = 0;
        // if (s.length() > 0) {
        // c = s.split(FuzzyJoinDriver.WORD_SEPARATOR_REGEX).length;
        // }
        // tokensCount[i] = c;
        // }

        // String[] newSplits = new String[dataColumns.length];
        // int pos = 0;
        // for (int i = 0; i < dataColumns.length; ++i) {
        // newSplits[i] = "";
        // for (int j = pos; j < pos + tokensCount[i]; ++j) {
        // if (j > pos) {
        // newSplits[i] += FuzzyJoinDriver.WORD_SEPARATOR;
        // }
        // newSplits[i] += newTokens.get(j).split(
        // FuzzyJoinDriver.WORD_SEPARATOR_REGEX)[0];
        // }
        // pos += tokensCount[i];
        // }

        String rec = "";
        boolean isFirst = true;
        for (int i = 0; i < splits.length; ++i) {
            if (i > 0) {
                rec += FuzzyJoinConfig.RECORD_SEPARATOR;
            }
            if (i == FuzzyJoinConfig.RECORD_KEY) {
                rec += Integer.parseInt(splits[i]) + offsetRID;
            } else {
                boolean isFound = false;
                for (int j = 0; j < dataColumns.length; ++j) {
                    if (i == dataColumns[j]) {
                        isFound = true;
                        if (isFirst) {
                            rec += newData;
                            isFirst = false;
                        }
                    }
                }
                if (!isFound) {
                    rec += splits[i];
                }
            }
        }
        recordNew.set(rec);
        output.collect(recordNew, nullWritable);
    }
}
