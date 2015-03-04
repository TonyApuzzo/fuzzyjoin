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

package edu.uci.ics.fuzzyjoin.tests;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;

import org.junit.Test;

import edu.uci.ics.fuzzyjoin.FuzzyJoinMemory;
import edu.uci.ics.fuzzyjoin.ResultSelfJoin;
import edu.uci.ics.fuzzyjoin.tests.dataset.AbstractDataset;
import edu.uci.ics.fuzzyjoin.tests.dataset.AbstractDataset.Directory;
import edu.uci.ics.fuzzyjoin.tests.dataset.DBLPSmallDataset;

public class FuzzyJoinTest {

    private static final AbstractDataset dataset = new DBLPSmallDataset(
            "../data");

    @Test
    public void test() throws Exception {

        ArrayList<int[]> records = new ArrayList<int[]>();
        ArrayList<Integer> rids = new ArrayList<Integer>();
        ArrayList<ResultSelfJoin> results = new ArrayList<ResultSelfJoin>();

        dataset.createDirecotries();

        FuzzyJoinMemory fj = new FuzzyJoinMemory(dataset.getThreshold());

        FuzzyJoinMemory.readRecords(dataset.getPathPart0(Directory.SSJOININ),
                records, rids);

        for (int[] record : records) {
            results.addAll(fj.selfJoinAndAddRecord(record));
        }

        BufferedWriter out = new BufferedWriter(new FileWriter(
                dataset.getPathPart0(Directory.SSJOINOUT)));
        for (ResultSelfJoin result : results) {
            out.write(String.format("%d %d %.3f\n", rids.get(result.indexX),
                    rids.get(result.indexY), result.similarity));
        }
        out.close();

        FuzzyJoinTestUtil.verifyDirectory(
                dataset.getPathPart0(Directory.SSJOINOUT),
                dataset.getPathExpected(Directory.SSJOINOUT));
    }
}
