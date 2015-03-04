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

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.fuzzyjoin.FuzzyJoinConfig;
import edu.uci.ics.fuzzyjoin.tokenorder.TokenRankFrequency;

public class RecordGenerator extends TokenRankFrequency {
    private final Map<Integer, String> tokensMap = new HashMap<Integer, String>();
    private int maxRank = 0;
    private final int offset;

    public RecordGenerator(int offset) {
        this.offset = offset;
    }

    @Override
    public int add(String token) {
        Integer rank = super.add(token);
        tokensMap.put(rank, token);
        maxRank = Math.max(maxRank, rank);
        return rank;
    }

    public String generate(Iterable<String> tokens) {
        String record = "";
        for (String token : tokens) {
            if (record.length() > 0) {
                record += FuzzyJoinConfig.WORD_SEPARATOR;
            }
            Integer rank = getRank(token);
            String newToken;
            if (rank != null) {
                newToken = tokensMap.get((rank + offset) % (maxRank + 1));
            } else {
                newToken = token;
            }
            // remove _COUNT at the end of the token
            record += newToken.split(FuzzyJoinConfig.TOKEN_SEPARATOR_REGEX)[0];
        }
        return record;
    }
}
