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

package edu.uci.ics.fuzzyjoin.tests.dataset;


public class DBLPSmallDataset extends AbstractDataset {
    private final int NO_RECORDS = 100;
    private final String NAME = "dblp-small";
    private final String PATH = NAME;
    private final float THRESHOLD = .5f;

    public DBLPSmallDataset() {
    }

    public DBLPSmallDataset(String base) {
        setBase(base);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNoRecords() {
        return NO_RECORDS;
    }

    @Override
    public String getPath() {
        return base + '/' + PATH;
    }

    @Override
    public String getSuffix(Relation relation) {
        return "";
    }

    @Override
    public float getThreshold() {
        return THRESHOLD;
    }

}
