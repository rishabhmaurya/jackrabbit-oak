/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import java.util.Comparator;

/**
 * Represents a string term with a frequency.
 *
 * <p>
 * Sorted alphabetically by default.
 */
public class IndexTerm implements Comparable<IndexTerm> {

    private String term;
    private int freq;

    public IndexTerm(String term, int freq) {
        this.term = term;
        this.freq = freq;
    }

    public String term() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int freq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    public int compareTo(IndexTerm o) {
        return term().compareTo(o.term());
    }

    public static Comparator<IndexTerm> sortByFreq() {
        return new Comparator<IndexTerm>() {

            public int compare(IndexTerm a, IndexTerm b) {
                return b.freq() - a.freq();
            }

        };
    }

    public static Comparator<IndexTerm> sortAlphabetically() {
        return new Comparator<IndexTerm>() {

            public int compare(IndexTerm a, IndexTerm b) {
                return a.term().compareTo(b.term());
            }

        };
    }
}
