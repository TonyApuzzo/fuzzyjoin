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

import java.util.NoSuchElementException;

public class PUBDataset extends AbstractDataset {
    private static final int NO_RECORDS = 1385532;
    private static final String NAME = "pub";
    private static final String DBLP_SUFFIX = ".dblp";
    private static final String CSX_SUFFIX = ".csx";
    private static final String PATH = NAME;
    private static final float THRESHOLD = .8f;

    public PUBDataset() {
    }

    public PUBDataset(String base) {
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
        switch (relation) {
        case R:
            return DBLP_SUFFIX;
        case S:
            return CSX_SUFFIX;
        default:
            throw new NoSuchElementException();
        }
    }

    @Override
    public float getThreshold() {
        return THRESHOLD;
    }

}
