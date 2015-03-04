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

package edu.uci.ics.fuzzyjoin.hadoop;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsBasic;
import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.hadoop.tokens.TokensBasic;

public class FuzzyJoin {
    public static void bib(String[] args) throws IOException {
        TokensBasic.main(args);
        RIDPairsImproved.main(args);
        RecordPairsBasic.main(args);
    }

    public static void bpb(String[] args) throws IOException {
        TokensBasic.main(args);
        RIDPairsPPJoin.main(args);
        RecordPairsBasic.main(args);
    }

    public static void bpi(String[] args) throws IOException {
        TokensBasic.main(args);
        RIDPairsPPJoin.main(args);
        RecordPairsImproved.main(args);
    }

    public static void main(String[] args) throws IOException {
        // RecordBuild.main(args);

        JobConf job = new JobConf();
        new GenericOptionsParser(job, args);
        String dataDir = job.get(FuzzyJoinDriver.DATA_DIR_PROPERTY);
        if (dataDir == null) {
            throw new UnsupportedOperationException(
                    "ERROR: fuzzyjoin.data.dir not set");
        }
        int dataCopy = job.getInt(FuzzyJoinDriver.DATA_COPY_PROPERTY, 1);
        String dataCopyFormatted = String.format("-%03d", dataCopy - 1);
        FileSystem.get(job).delete(
                new Path(dataDir + "/tokens" + dataCopyFormatted), true);
        FileSystem.get(job).delete(
                new Path(dataDir + "/tokens.phase1" + dataCopyFormatted), true);
        FileSystem.get(job).delete(
                new Path(dataDir + "/ridpairs" + dataCopyFormatted), true);
        FileSystem.get(job).delete(
                new Path(dataDir + "/recordpairs" + dataCopyFormatted), true);
        FileSystem.get(job).delete(
                new Path(dataDir + "/recordpairs.phase1" + dataCopyFormatted),
                true);

        Date startTime = new Date();
        System.out.println("Complete-Job started: " + startTime);
        if ("bpb".equals(job.get(FuzzyJoinDriver.VERSION_PROPERTY))) {
            bpb(args);
        } else if ("bpi".equals(job.get(FuzzyJoinDriver.VERSION_PROPERTY))) {
            bpi(args);
        } else {
            bib(args);
        }
        Date end_time = new Date();
        System.out.println("Complete-Job ended: " + end_time);
        System.out.println("The complete-job took "
                + (end_time.getTime() - startTime.getTime()) / (float) 1000.0
                + " seconds.");
    }
}
