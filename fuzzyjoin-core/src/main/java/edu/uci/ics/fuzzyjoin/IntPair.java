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

package edu.uci.ics.fuzzyjoin;

public class IntPair {

    public static int[] PRIME = new int[] { 17, 31 }; // used for hashCode
    // computations

    protected int first;

    protected int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int compareTo(Object o) {
        if (this == o) {
            return 0;
        }
        IntPair p = (IntPair) o;
        if (first != p.first) {
            return first < p.first ? -1 : 1;
        }
        return second < p.second ? -1 : second > p.second ? 1 : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (!(o instanceof IntPair)) {
            return false;
        }
        IntPair p = (IntPair) o;
        return first == p.first && second == p.second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        return first * PRIME[0] + second;
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "(" + first + "," + second + ")";
    }

}
