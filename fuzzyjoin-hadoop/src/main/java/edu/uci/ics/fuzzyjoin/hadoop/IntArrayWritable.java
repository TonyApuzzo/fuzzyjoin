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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntArrayWritable extends ArrayWritable implements
        WritableComparable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public int compareTo(Object o) {
        Writable[] w = get();
        Writable[] w_o = ((ArrayWritable) o).get();

        int i = 0;
        int i_o = 0;

        while (i < w.length && i_o < w_o.length) {

            int v = ((IntWritable) w[i]).get();
            int v_o = ((IntWritable) w_o[i_o]).get();

            if (v != v_o) {
                return v - v_o;
            }

            i++;
            i_o++;
        }

        if (w.length != w_o.length) {
            return w.length - w_o.length;
        }

        return 0;
    }

    @Override
    public String toString() {
        String out = "(";
        for (Writable w : get()) {
            out += w + ", ";
        }
        return out.substring(0, out.length() - 2) + ")";
    }
}
