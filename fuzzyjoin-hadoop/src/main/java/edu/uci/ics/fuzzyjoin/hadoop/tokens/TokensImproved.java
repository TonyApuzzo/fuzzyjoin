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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntArrayWritable;

public class TokensImproved {
    public static void main(String[] args) throws IOException {
        //
        // setup job
        //
        JobConf job = new JobConf();
        new GenericOptionsParser(job, args);
        job.setJobName(TokensImproved.class.getSimpleName());

        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);

        String pkg = job.get(FuzzyJoinDriver.TOKENS_PACKAGE_PROPERTY,
                FuzzyJoinDriver.TOKENS_PACKAGE_VALUE);
        if (pkg.equals("Array")) {
            job
                    .setMapperClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.Map.class);
            job
                    .setCombinerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.ReduceAggregate.class);
            job
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.array.ReduceSort.class);
            job.setMapOutputValueClass(IntArrayWritable.class);
        } else if (pkg.equals("Scalar")) {
            job
                    .setMapperClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.Map.class);
            job
                    .setCombinerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceAggregate.class);
            job
                    .setReducerClass(edu.uci.ics.fuzzyjoin.hadoop.tokens.scalar.ReduceSort.class);
            job.setMapOutputValueClass(IntWritable.class);
        } else {
            throw new UnsupportedOperationException("ERROR: Unknown "
                    + FuzzyJoinDriver.TOKENS_PACKAGE_PROPERTY + " " + pkg);
        }

        //
        // set input & output
        //
        String dataDir = job.get(FuzzyJoinDriver.DATA_DIR_PROPERTY);
        if (dataDir == null) {
            throw new UnsupportedOperationException(
                    "ERROR: fuzzyjoin.data.dir not set");
        }
        int dataCopy = job.getInt(FuzzyJoinDriver.DATA_COPY_PROPERTY, 1);
        String dataCopyFormatted = String.format("-%03d", dataCopy - 1);
        String suffix = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (suffix.isEmpty()) {
            FileInputFormat.addInputPath(job, new Path(dataDir + "/records"
                    + dataCopyFormatted));
        } else {
            FileInputFormat.addInputPath(job, new Path(dataDir + "/records."
                    + suffix.split(FuzzyJoinDriver.SEPSARATOR_REGEX)[0]
                    + dataCopyFormatted));
        }
        Path outputPath = new Path(dataDir + "/tokens" + dataCopyFormatted);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem.get(job).delete(outputPath, true);

        //
        // run
        //
        FuzzyJoinDriver.run(job);
    }
}
