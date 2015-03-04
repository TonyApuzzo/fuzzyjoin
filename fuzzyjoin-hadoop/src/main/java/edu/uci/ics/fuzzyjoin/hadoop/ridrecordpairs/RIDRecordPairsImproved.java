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

package edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairComparatorFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairPartitionerFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.token.MapJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.token.MapSelfJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.token.ReduceVerifyListJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.token.ReduceVerifyListSelfJoin;

public class RIDRecordPairsImproved {
    public static void main(String[] args) throws IOException {
        //
        // ****************************** Phase 1 ******************************
        //
        // setup job
        //
        JobConf jobPhase1 = new JobConf();
        new GenericOptionsParser(jobPhase1, args);
        jobPhase1.setJobName(RIDRecordPairsImproved.class.getSimpleName()
                + ".phase1");

        String suffix = jobPhase1.get(
                FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (suffix.isEmpty()) {
            //
            // self-jon
            //
            jobPhase1.setMapperClass(MapSelfJoin.class);
            jobPhase1.setReducerClass(ReduceVerifyListSelfJoin.class);
            jobPhase1.setMapOutputKeyClass(IntWritable.class);
            jobPhase1.setMapOutputValueClass(ValueSelfJoin.class);
        } else {
            //
            // R-S join
            //
            jobPhase1.setMapperClass(MapJoin.class);
            jobPhase1.setReducerClass(ReduceVerifyListJoin.class);
            jobPhase1.setPartitionerClass(IntPairPartitionerFirst.class);
            jobPhase1
                    .setOutputValueGroupingComparator(IntPairComparatorFirst.class);
            jobPhase1.setMapOutputKeyClass(IntPairWritable.class);
            jobPhase1.setMapOutputValueClass(ValueJoin.class);
        }
        jobPhase1.setOutputKeyClass(IntPairWritable.class);
        jobPhase1.setOutputFormat(SequenceFileOutputFormat.class);

        //
        // set input & output
        //
        String dataDir = jobPhase1.get(FuzzyJoinDriver.DATA_DIR_PROPERTY);
        if (dataDir == null) {
            throw new UnsupportedOperationException(
                    "ERROR: fuzzyjoin.data.dir not set");
        }
        int dataCopy = jobPhase1.getInt(FuzzyJoinDriver.DATA_COPY_PROPERTY, 1);
        String dataCopyFormatted = String.format("-%03d", dataCopy - 1);
        if (suffix.isEmpty()) {
            FileInputFormat.addInputPath(jobPhase1, new Path(dataDir
                    + "/records" + dataCopyFormatted));
        } else {
            for (String s : suffix.split(FuzzyJoinDriver.SEPSARATOR_REGEX)) {
                FileInputFormat.addInputPath(jobPhase1, new Path(dataDir
                        + "/records." + s + dataCopyFormatted));
            }
        }
        String outputPathDir = dataDir + "/ridrecordpairs.phase1"
                + dataCopyFormatted;
        Path outputPath = new Path(outputPathDir);
        FileOutputFormat.setOutputPath(jobPhase1, outputPath);
        FileSystem.get(jobPhase1).delete(outputPath, true);

        //
        // set distribution cache
        //
        String tokensOutput = dataDir + "/tokens" + dataCopyFormatted;
        Path tokensPath = new Path(tokensOutput + "/part-00000");
        DistributedCache.addCacheFile(tokensPath.toUri(), jobPhase1);
        jobPhase1.set(FuzzyJoinConfig.DATA_TOKENS_PROPERTY, tokensPath.toString());

        //
        // ****************************** Phase 2 ******************************
        //
        // setup job
        //
        JobConf jobPhase2 = new JobConf();
        new GenericOptionsParser(jobPhase2, args);
        jobPhase2.setJobName(RIDRecordPairsImproved.class.getSimpleName()
                + ".phase2");

        jobPhase2.setInputFormat(SequenceFileInputFormat.class);
        jobPhase2.setMapperClass(IdentityMapper.class);
        jobPhase2.setReducerClass(ReduceOne.class);
        jobPhase2.setMapOutputKeyClass(IntPairWritable.class);

        //
        // set input & output
        //
        FileInputFormat.addInputPath(jobPhase2, outputPath);
        outputPath = new Path(dataDir + "/ridrecordpairs" + dataCopyFormatted);
        FileOutputFormat.setOutputPath(jobPhase2, outputPath);
        FileSystem.get(jobPhase2).delete(outputPath, true);

        //
        // run both
        //
        Date startTime = new Date();
        System.out.println("Multi-Job started: " + startTime);
        FuzzyJoinDriver.run(jobPhase1);
        FuzzyJoinDriver.run(jobPhase2);
        Date end_time = new Date();
        System.out.println("Multi-Job ended: " + end_time);
        System.out.println("The multi-job took "
                + (end_time.getTime() - startTime.getTime()) / (float) 1000.0
                + " seconds.");
    }
}
