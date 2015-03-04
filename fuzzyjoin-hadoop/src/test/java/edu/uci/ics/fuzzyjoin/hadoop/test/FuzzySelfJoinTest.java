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

package edu.uci.ics.fuzzyjoin.hadoop.test;

import org.junit.Test;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.hadoop.FuzzyJoinDriver;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.RecordBalance;
import edu.uci.ics.fuzzyjoin.hadoop.datagen.RecordBuild;
import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsBasic;
import edu.uci.ics.fuzzyjoin.hadoop.recordpairs.RecordPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsImproved;
import edu.uci.ics.fuzzyjoin.hadoop.ridpairs.RIDPairsPPJoin;
import edu.uci.ics.fuzzyjoin.hadoop.tokens.TokensBasic;
import edu.uci.ics.fuzzyjoin.hadoop.tokens.TokensImproved;
import edu.uci.ics.fuzzyjoin.tests.FuzzyJoinTestUtil;
import edu.uci.ics.fuzzyjoin.tests.dataset.AbstractDataset;
import edu.uci.ics.fuzzyjoin.tests.dataset.AbstractDataset.Directory;
import edu.uci.ics.fuzzyjoin.tests.dataset.DBLPSmallDataset;

public class FuzzySelfJoinTest {
    private static final AbstractDataset dataset = new DBLPSmallDataset(
            "../data");

    private final String[] args = new String[] {
            "-D" + FuzzyJoinDriver.DATA_DIR_PROPERTY + "=" + dataset.getPath(),
            "-D" + FuzzyJoinConfig.RECORD_DATA_PROPERTY + "=2,3",
            "-D" + FuzzyJoinConfig.SIMILARITY_THRESHOLD_PROPERTY + "="
                    + dataset.getThreshold() };

    @Test
    public void test010() throws Exception {
        RecordBuild.main(args);
    }

    @Test
    public void test020() throws Exception {
        RecordBalance.main(args);
    }

    @Test
    public void test100() throws Exception {
        TokensBasic.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.TOKENS),
                dataset.getPathExpected(Directory.TOKENS), true);
    }

    @Test
    public void test110() throws Exception {
        TokensImproved.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.TOKENS),
                dataset.getPathExpected(Directory.TOKENS), true);
    }

    @Test
    public void test200() throws Exception {
        RIDPairsImproved.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.RIDPAIRS),
                dataset.getPathExpected(Directory.RIDPAIRS));
    }

    @Test
    public void test210() throws Exception {
        RIDPairsPPJoin.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.RIDPAIRS),
                dataset.getPathExpected(Directory.RIDPAIRS));
    }

    @Test
    public void test300() throws Exception {
        RecordPairsBasic.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.RECORDPAIRS),
                dataset.getPathExpected(Directory.RECORDPAIRS));
    }

    @Test
    public void test310() throws Exception {
        RecordPairsImproved.main(args);
        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.RECORDPAIRS),
                dataset.getPathExpected(Directory.RECORDPAIRS));
    }
}
