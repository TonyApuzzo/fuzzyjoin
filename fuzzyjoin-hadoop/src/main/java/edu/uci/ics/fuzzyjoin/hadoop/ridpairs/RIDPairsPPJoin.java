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

package edu.uci.ics.fuzzyjoin.hadoop.ridpairs;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairComparatorFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairPartitionerFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntPairWritable;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleComparatorFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntTriplePartitionerFirst;
import edu.uci.ics.fuzzyjoin.hadoop.IntTripleWritable;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ppjoin.MapJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ppjoin.MapSelfJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ppjoin.ReduceJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.ppjoin.ReduceSelfJoin;

public class RIDPairsPPJoin {
    public static void main(String[] args) throws IOException {
        //
        // setup job
        //
        JobConf job = new JobConf();
        new GenericOptionsParser(job, args);
        String recordGroup = job.get(
                FuzzyJoinDriver.RIDPAIRS_GROUP_CLASS_PROPERTY,
                FuzzyJoinDriver.RIDPAIRS_GROUP_CLASS_VALUE.toString());
        job
                .setJobName(RIDPairsPPJoin.class.getSimpleName() + "."
                        + recordGroup);

        String suffix = job.get(FuzzyJoinDriver.DATA_SUFFIX_INPUT_PROPERTY, "");
        if (suffix.isEmpty()) {
            //
            // self-jon
            //
            job.setMapperClass(MapSelfJoin.class);
            job.setReducerClass(ReduceSelfJoin.class);
            job.setPartitionerClass(IntPairPartitionerFirst.class);
            job.setOutputValueGroupingComparator(IntPairComparatorFirst.class);
            job.setMapOutputKeyClass(IntPairWritable.class);
            job.setMapOutputValueClass(ValueSelfJoin.class);
        } else {
            //
            // R-S join
            //
            job.setMapperClass(MapJoin.class);
            job.setReducerClass(ReduceJoin.class);
            job.setPartitionerClass(IntTriplePartitionerFirst.class);
            job
                    .setOutputValueGroupingComparator(IntTripleComparatorFirst.class);
            job.setMapOutputKeyClass(IntTripleWritable.class);
            job.setMapOutputValueClass(ValueJoin.class);
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
        if (suffix.isEmpty()) {
            FileInputFormat.addInputPath(job, new Path(dataDir + "/records"
                    + dataCopyFormatted));
        } else {
            for (String s : suffix.split(FuzzyJoinDriver.SEPSARATOR_REGEX)) {
                FileInputFormat.addInputPath(job, new Path(dataDir
                        + "/records." + s + dataCopyFormatted));
            }
        }
        Path outputPath = new Path(dataDir + "/ridpairs" + dataCopyFormatted);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem.get(job).delete(outputPath, true);

        //
        // set distribution cache
        //
        String tokensOutput = dataDir + "/tokens" + dataCopyFormatted;
        Path tokensPath = new Path(tokensOutput + "/part-00000");
        DistributedCache.addCacheFile(tokensPath.toUri(), job);
        job.set(FuzzyJoinConfig.DATA_TOKENS_PROPERTY, tokensPath.toString());
        if (job.getBoolean(FuzzyJoinDriver.TOKENS_LENGTHSTATS_PROPERTY,
                FuzzyJoinDriver.TOKENS_LENGTHSTATS_VALUE)) {
            Path lengthstatsPath = new Path(tokensOutput + "/lengthstats");
            DistributedCache.addCacheFile(lengthstatsPath.toUri(), job);
            job.set(FuzzyJoinDriver.DATA_LENGTHSTATS_PROPERTY, lengthstatsPath
                    .toString());
        }

        //
        // run
        //
        FuzzyJoinDriver.run(job);
    }
}
