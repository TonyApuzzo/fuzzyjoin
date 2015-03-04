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

package edu.uci.ics.fuzzyjoin.hadoop.recordpairs;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleComparatorFirstSecond;
import edu.uci.ics.fuzzyjoin.hadoop.IntTriplePartitionerFirstSecond;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleWritable;

public class RecordPairsImproved {
    public static void main(String[] args) throws IOException {
        //
        // setup job
        //
        JobConf job = new JobConf();
        new GenericOptionsParser(job, args);
        job.setJobName(RecordPairsImproved.class.getSimpleName());

        String suffix = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (suffix.isEmpty()) {
            //
            // self-jon
            //
            job.setMapperClass(MapBroadcastSelfJoin.class);

            job.setMapOutputKeyClass(IntPairWritable.class);
        } else {
            //
            // R-S join
            //
            job.setMapperClass(MapBroadcastJoin.class);

            job.setPartitionerClass(IntTriplePartitionerFirstSecond.class);
            job.setOutputValueGroupingComparator(IntTripleComparatorFirstSecond.class);
            job.setMapOutputKeyClass(IntTripleWritable.class);
        }
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(Text.class);

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
        if (suffix.isEmpty()) {
            FileInputFormat.addInputPath(job, new Path(dataDir + "/records"
                    + dataCopyFormatted));
        } else {
            for (String s : suffix.split(FuzzyJoinDriver.SEPSARATOR_REGEX)) {
                FileInputFormat.addInputPath(job, new Path(dataDir
                        + "/records." + s + dataCopyFormatted));
            }
        }
        Path outputPath = new Path(dataDir + "/recordpairs" + dataCopyFormatted);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem.get(job).delete(outputPath, true);

        //
        // set distribution cache
        //
        Path ridpairsPath = new Path(dataDir + "/ridpairs" + dataCopyFormatted);
        FileSystem fs = FileSystem.get(job);
        for (Path ridpairdFile : FileUtil.stat2Paths(fs.listStatus(
                ridpairsPath, new OutputLogFilter()))) {
            DistributedCache.addCacheFile(ridpairdFile.toUri(), job);
        }
        job.set(FuzzyJoinDriver.DATA_JOININDEX_PROPERTY,
                ridpairsPath.toString() + "/part-00000");

        //
        // run
        //
        FuzzyJoinDriver.run(job);
    }
}
