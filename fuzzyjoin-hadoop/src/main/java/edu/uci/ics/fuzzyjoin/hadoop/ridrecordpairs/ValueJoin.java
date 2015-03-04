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

public class ValueJoin extends ValueSelfJoin {

    private int relation;
    private int length;

    public ValueJoin() {
    }

    public ValueJoin(int rid, Collection<Integer> tokens, Text record,
            int relation, int length) {
        super(rid, tokens, record);
        this.relation = relation;
        this.length = length;
    }

    public ValueJoin(ValueJoin v) {
        super(v);
        relation = v.relation;
        length = v.length;
    }

    public int getLength() {
        return length;
    }

    public int getRelation() {
        return relation;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        relation = in.readInt();
        length = in.readInt();
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setRelation(int relation) {
        this.relation = relation;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(relation);
        out.writeInt(length);
    }

}
