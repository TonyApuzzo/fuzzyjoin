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

package edu.uci.ics.fuzzyjoin.hadoop.tokens;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntArrayWritable;

public class TokensBasic {
    public static void main(String[] args) throws IOException {
        //
        // ****************************** Phase 1 ******************************
        //
        // setup job
        //
        JobConf jobPhase1 = new JobConf();
        new GenericOptionsParser(jobPhase1, args);
        jobPhase1.setJobName(TokensBasic.class.getSimpleName() + ".phase1");

        jobPhase1.setMapOutputKeyClass(Text.class);
        jobPhase1.setOutputKeyClass(Text.class);
        jobPhase1.setOutputFormat(SequenceFileOutputFormat.class);

        String pkg = jobPhase1.get(FuzzyJoinDriver.TOKENS_PACKAGE_PROPERTY,
                FuzzyJoinDriver.TOKENS_PACKAGE_VALUE);
        if (pkg.equals("Array")) {
            jobPhase1
                    .setMapperClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.Map.class);
            jobPhase1
                    .setCombinerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.ReduceAggregate.class);
            jobPhase1
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.ReduceAggregate.class);
            jobPhase1.setMapOutputValueClass(IntArrayWritable.class);
            jobPhase1.setOutputValueClass(IntArrayWritable.class);
        } else if (pkg.equals("Scalar")) {
            jobPhase1
                    .setMapperClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.Map.class);
            jobPhase1
                    .setCombinerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceAggregate.class);
            jobPhase1
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceAggregate.class);
            jobPhase1.setMapOutputValueClass(IntWritable.class);
            jobPhase1.setOutputValueClass(IntWritable.class);
        } else {
            throw new UnsupportedOperationException("ERROR: Unknown "
                    + FuzzyJoinDriver.TOKENS_PACKAGE_PROPERTY + " " + pkg);
        }

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
        String suffix = jobPhase1.get(
                FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (suffix.isEmpty()) {
            FileInputFormat.addInputPath(jobPhase1, new Path(dataDir
                    + "/records" + dataCopyFormatted));
        } else {
            FileInputFormat.addInputPath(
                    jobPhase1,
                    new Path(dataDir + "/records."
                            + suffix.split(FuzzyJoinDriver.SEPSARATOR_REGEX)[0]
                            + dataCopyFormatted));
        }
        Path outputPath = new Path(dataDir + "/tokens.phase1"
                + dataCopyFormatted);
        FileOutputFormat.setOutputPath(jobPhase1, outputPath);
        FileSystem.get(jobPhase1).delete(outputPath, true);

        //
        // ****************************** Phase 2 ******************************
        //
        // setup job
        //
        JobConf jobPhase2 = new JobConf();
        new GenericOptionsParser(jobPhase2, args);
        jobPhase2.setJobName(TokensBasic.class.getSimpleName() + ".phase2");

        jobPhase2.setNumReduceTasks(1);
        jobPhase2.setInputFormat(SequenceFileInputFormat.class);
        jobPhase2.setMapperClass(InverseMapper.class);

        if (pkg.equals("Array")) {
            jobPhase2
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.ReduceSelect.class);
            jobPhase2.setMapOutputKeyClass(IntArrayWritable.class);
        } else if (pkg.equals("Scalar")) {
            jobPhase2
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceSelect.class);
            jobPhase2.setMapOutputKeyClass(IntWritable.class);
        } else {
            throw new UnsupportedOperationException("ERROR: Unknown "
                    + FuzzyJoinDriver.TOKENS_PACKAGE_PROPERTY + " " + pkg);
        }

        //
        // set input & output
        //
        FileInputFormat.addInputPath(jobPhase2, outputPath);
        outputPath = new Path(dataDir + "/tokens" + dataCopyFormatted);
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
