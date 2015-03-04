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

package edu.uci.ics.fuzzyjoin.hadoop.ridrecordpairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ValueSelfJoin implements Writable {
    private int rid;

    private int[] tokens;

    private Text record;

    public ValueSelfJoin() {
        record = new Text();
    }

    public ValueSelfJoin(int rid, Collection<Integer> tokens, Text record) {
        this.rid = rid;
        setTokens(tokens);
        this.record = record;
    }

    public ValueSelfJoin(ValueSelfJoin v) {
        rid = v.rid;
        tokens = v.tokens;
        record = new Text(v.record);
    }

    public Text getRecord() {
        return record;
    }

    public int getRID() {
        return rid;
    }

    public int[] getTokens() {
        return tokens;
    }

    public void readFields(DataInput in) throws IOException {
        rid = in.readInt();
        tokens = new int[in.readInt()];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = in.readInt();
        }
        record.readFields(in);
    }

    public void setRecord(Text record) {
        this.record = record;
    }

    public void setRID(int rid) {
        this.rid = rid;
    }

    public void setTokens(Collection<Integer> tokens) {
        this.tokens = new int[tokens.size()];
        int i = 0;
        for (Integer token : tokens) {
            this.tokens[i++] = token;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(rid);
        out.writeInt(tokens.length);
        for (int token : tokens) {
            out.writeInt(token);
        }
        record.write(out);
    }
}
