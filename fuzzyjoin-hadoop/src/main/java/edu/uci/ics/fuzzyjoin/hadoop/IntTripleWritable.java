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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IntTripleWritable implements WritableComparable {
    /** A Comparator optimized for IntTripleWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IntTripleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int c = WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
            if (c != 0) {
                return c;
            }
            c = WritableComparator.compareBytes(b1, s1 + 4, 4, b2, s2 + 4, 4);
            if (c != 0) {
                return c;
            }
            return WritableComparator
                    .compareBytes(b1, s1 + 8, 4, b2, s2 + 8, 4);
        }
    }

    static { // register this comparator
        WritableComparator.define(IntTripleWritable.class, new Comparator());
    }

    private int first;

    private int second;

    private int third;

    public int compareTo(Object o) {
        if (this == o) {
            return 0;
        }
        IntTripleWritable p = (IntTripleWritable) o;
        if (first != p.first) {
            return first < p.first ? -1 : 1;
        }
        if (second != p.second) {
            return second < p.second ? -1 : 1;
        }
        return third < p.third ? -1 : third > p.third ? 1 : 0;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public int getThird() {
        return third;
    }

    @Override
    public int hashCode() {
        return first * IntPairWritable.PRIME[0] + second
                * IntPairWritable.PRIME[1] + third;
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        third = in.readInt();
    }

    public void set(int first, int second, int third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public void setThird(int third) {
        this.third = third;
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + "," + third + ")";
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
        out.writeInt(third);
    }
}
