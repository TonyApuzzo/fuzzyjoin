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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ProgramDriver;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.DummyJob;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.RecordBalance;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.RecordBuild;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.RecordGenerate;
import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsBasic;
import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.RIDRecordPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs.RIDRecordPairsPPJoin;
import edu.uci.ics.fuzzyjoin.hadoop.tokens.TokensBasic;
import edu.uci.ics.fuzzyjoin.hadoop.tokens.TokensImproved;

public class FuzzyJoinDriver {
    public static final String NAMESPACE = "fuzzyjoin";
    public static final String VERSION_PROPERTY = NAMESPACE + ".version";
    //
    // tokens package
    //
    public static final String TOKENS_PACKAGE_PROPERTY = NAMESPACE
            + ".tokens.package";
    public static final String TOKENS_PACKAGE_VALUE = "Scalar";
    //
    // tokens length stats
    //
    public static final String TOKENS_LENGTHSTATS_PROPERTY = NAMESPACE
            + ".tokens.lengthstats";
    public static final boolean TOKENS_LENGTHSTATS_VALUE = false;
    //
    // record group
    //
    public static final String RIDPAIRS_GROUP_CLASS_PROPERTY = NAMESPACE
            + ".ridpairs.group.class";
    public static final String RIDPAIRS_GROUP_CLASS_VALUE = "TokenIdentity";
    public static final String RIDPAIRS_GROUP_FACTOR_PROPERTY = NAMESPACE
            + ".ridpairs.group.factor";
    public static final int RIDPAIRS_GROUP_FACTOR_VALUE = 1;
    //
    // data properties
    //
    public static final String DATA_DIR_PROPERTY = NAMESPACE + ".data.dir";
    public static final String DATA_RAW_PROPERTY = NAMESPACE + ".data.raw";
    public static final String DATA_RAW_VALUE = "*";
    public static final String DATA_LENGTHSTATS_PROPERTY = NAMESPACE
            + ".data.lengthstats";
    public static final String DATA_JOININDEX_PROPERTY = NAMESPACE
            + ".data.joinindex";
    public static final String DATA_CRTCOPY_PROPERTY = NAMESPACE
            + ".data.crtcopy";
    public static final String DATA_COPY_PROPERTY = NAMESPACE + ".data.copy";
    public static final String DATA_COPY_START_PROPERTY = NAMESPACE
            + ".data.copystart";
    public static final String DATA_SUFFIX_INPUT_PROPERTY = NAMESPACE
            + ".data.suffix.input";
    public static final String DATA_NORECORDS_PROPERTY = NAMESPACE
            + ".data.norecords";
    public static final String DATA_DICTIONARY_FACTOR_PROPERTY = NAMESPACE
            + ".data.dictionary.factor";
    //
    // other constants
    //
    public static final String DATA_LENGTH_STATS_FILE = "lengthstats";
    public static final char SEPSARATOR = ',';
    public static final String SEPSARATOR_REGEX = ",";

    //
    // data
    //

    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("dummyjob", DummyJob.class, "");
            pgd.addClass("recordbuild", RecordBuild.class, "");
            pgd.addClass("fuzzyjoin", FuzzyJoin.class, "");
            pgd.addClass("tokensbasic", TokensBasic.class, "");
            pgd.addClass("tokensimproved", TokensImproved.class, "");
            // pgd.addClass("ridpairsbasic", RIDPairsBasic.class, "");
            pgd.addClass("ridpairsimproved", RIDPairsImproved.class, "");
            pgd.addClass("ridpairsppjoin", RIDPairsPPJoin.class, "");
            pgd.addClass("recordpairsbasic", RecordPairsBasic.class, "");
            pgd.addClass("recordpairsimproved", RecordPairsImproved.class, "");
            pgd.addClass("ridrecordpairsimproved",
                    RIDRecordPairsImproved.class, "");
            pgd.addClass("ridrecordpairsppjoin", RIDRecordPairsPPJoin.class, "");
            pgd.addClass("recordgenerate", RecordGenerate.class, "");
            pgd.addClass("recordbalance", RecordBalance.class, "");
            pgd.driver(argv);
            // Success
            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    public static void run(JobConf job) throws IOException {
        job.setJarByClass(FuzzyJoinDriver.class);
        //
        // print info
        //
        String ret = "FuzzyJoinDriver(" + job.getJobName() + ")\n"
                + "  Input Path:  {";
        Path inputs[] = FileInputFormat.getInputPaths(job);
        for (int ctr = 0; ctr < inputs.length; ctr++) {
            if (ctr > 0) {
                ret += "\n                ";
            }
            ret += inputs[ctr].toString();
        }
        ret += "}\n";
        ret += "  Output Path: " + FileOutputFormat.getOutputPath(job) + "\n"
                + "  Map Jobs:    " + job.getNumMapTasks() + "\n"
                + "  Reduce Jobs: " + job.getNumReduceTasks() + "\n"
                + "  Properties:  {";
        String[][] properties = new String[][] {
                new String[] { FuzzyJoinConfig.SIMILARITY_NAME_PROPERTY,
                        FuzzyJoinConfig.SIMILARITY_NAME_VALUE },
                new String[] { FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY,
                        "" + FuzzyJoinConfig.SIMILARITY_THRESHOLD_VALUE },
                new String[] { FuzzyJoinConfig.TOKENIZER_PROPERTY,
                        FuzzyJoinConfig.TOKENIZER_VALUE },
                new String[] { TOKENS_PACKAGE_PROPERTY, TOKENS_PACKAGE_VALUE },
                new String[] { TOKENS_LENGTHSTATS_PROPERTY,
                        "" + TOKENS_LENGTHSTATS_VALUE },
                new String[] { RIDPAIRS_GROUP_CLASS_PROPERTY,
                        RIDPAIRS_GROUP_CLASS_VALUE },
                new String[] { RIDPAIRS_GROUP_FACTOR_PROPERTY,
                        "" + RIDPAIRS_GROUP_FACTOR_VALUE },
                new String[] { FuzzyJoinConfig.DATA_TOKENS_PROPERTY, "" },
                new String[] { DATA_JOININDEX_PROPERTY, "" }, };
        for (int crt = 0; crt < properties.length; crt++) {
            if (crt > 0) {
                ret += "\n                ";
            }
            ret += properties[crt][0] + "="
                    + job.get(properties[crt][0], properties[crt][1]);
        }
        ret += "}";
        System.out.println(ret);
        //
        // run job
        //
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        JobClient.runJob(job);
        Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took "
                + (end_time.getTime() - startTime.getTime()) / (float) 1000.0
                + " seconds.");
    }
}
