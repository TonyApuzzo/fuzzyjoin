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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;

public class RecordGenerate {
    public static void main(String[] args) throws IOException,
            InterruptedException {
        JobConf jobMaster = new JobConf();
        new GenericOptionsParser(jobMaster, args);
        jobMaster.setJarByClass(RecordGenerate.class);

        String dataDir = jobMaster.get(FuzzyJoinDriver.DATA_DIR_PROPERTY);
        if (dataDir == null) {
            throw new UnsupportedOperationException(
                    "ERROR: fuzzyjoin.data.dir not set");
        }

        int dataCopy = jobMaster.getInt(FuzzyJoinDriver.DATA_COPY_PROPERTY, 1);
        int dataCopyStart = jobMaster.getInt(
                FuzzyJoinDriver.DATA_COPY_START_PROPERTY, 1);

        JobClient jobClient = new JobClient(jobMaster);
        RunningJob[] runningJobs = new RunningJob[dataCopy];

        if (dataCopyStart >= dataCopy) {
            throw new UnsupportedOperationException("ERROR: "
                    + FuzzyJoinDriver.DATA_COPY_START_PROPERTY + " "
                    + dataCopyStart + " is greater or equal than "
                    + FuzzyJoinDriver.DATA_COPY_PROPERTY + " " + dataCopy);
        }

        String suffix = jobMaster.get(
                FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (!suffix.isEmpty()) {
            suffix = "." + suffix;
        }
        String recordsDir = dataDir + "/recordsbulk" + suffix;

        for (int i = dataCopyStart; i < dataCopy; i++) {
            //
            // setup job
            //
            JobConf job = new JobConf(jobMaster);
            new GenericOptionsParser(job, args);
            job.setJobName(RecordGenerate.class.getSimpleName());

            job.setMapperClass(MapNewRecord.class);
            job.setLong("mapred.min.split.size", Long.MAX_VALUE);
            job.setNumReduceTasks(0);

            //
            // set input & output
            //
            FileInputFormat.setInputPaths(job, new Path(recordsDir + "-000"));
            Path tokensPath = new Path(dataDir + "/tokens-000/part-00000");

            //
            // set distribution cache
            //
            DistributedCache.addCacheFile(tokensPath.toUri(), job);
            job.set(FuzzyJoinConfig.DATA_TOKENS_PROPERTY, tokensPath.toString());

            //
            // run
            //
            job.setInt(FuzzyJoinDriver.DATA_CRTCOPY_PROPERTY, i);

            Path outputPath = new Path(recordsDir + String.format("-%03d", i));
            FileOutputFormat.setOutputPath(job, outputPath);
            FileSystem.get(job).delete(outputPath, true);
            runningJobs[i] = jobClient.submitJob(job);
            // FuzzyJoinDriver.run(job);
        }

        boolean running = true;
        while (running) {
            running = false;
            for (int i = dataCopyStart; i < dataCopy; i++) {
                if (!runningJobs[i].isComplete()) {
                    running = true;
                    Thread.sleep(1000);
                    break;
                }
            }
        }
    }
}
