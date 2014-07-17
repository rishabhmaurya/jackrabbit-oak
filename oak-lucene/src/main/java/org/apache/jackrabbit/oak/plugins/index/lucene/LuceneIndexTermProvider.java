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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.felix.scr.annotations.Component;
import org.apache.jackrabbit.oak.plugins.index.IndexTerm;
import org.apache.jackrabbit.oak.plugins.index.IndexTermProvider;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


@Component(immediate = true)
public class LuceneIndexTermProvider implements IndexTermProvider {

    private final Logger log = LoggerFactory.getLogger(LuceneIndexTermProvider.class);

    private final IndexTracker indexTracker;

    public LuceneIndexTermProvider() {
        indexTracker = null;
    }

    public LuceneIndexTermProvider(IndexTracker indexTracker) {
        this.indexTracker = indexTracker;
    }

    public List<IndexTerm> retrieveTerms(String indexPath, Set<String> allowedFields, int minFreq, int minTermLength)
            throws IOException {

        IndexNode indexNode = indexTracker.acquireIndexNode("/");
        IndexReader reader = indexNode.getSearcher().getIndexReader();

        Map<String, Integer> termMap = new HashMap<String, Integer>();

        List<AtomicReaderContext> leaves = reader.leaves();
        log.info("Retrieving lucene index terms ...");

        for (AtomicReaderContext context : leaves) {
            AtomicReader atomicReader = context.reader();
            for (String fieldName : atomicReader.fields()) {
                if (allowedFields == null || allowedFields.isEmpty() || allowedFields.contains(fieldName)) {
                    Terms terms = atomicReader.terms(fieldName);
                    TermsEnum te = terms.iterator(null);
                    BytesRef term;
                    while ((term = te.next()) != null) {
                        int docFreq = atomicReader.docFreq(new Term(fieldName, term));
                        String termString = term.utf8ToString();
                        if (termString.length() >= minTermLength) {
                            termMap.put(termString,
                                    (termMap.containsValue(termString) ? termMap.get(termString) : 0) + docFreq);
                        }
                    }
                }
            }
        }

        List<IndexTerm> suggestionTerms = new ArrayList<IndexTerm>();

        for (Map.Entry<String, Integer> entry : termMap.entrySet()) {
            if (entry.getValue() >= minFreq ) {
                suggestionTerms.add(new IndexTerm(entry.getKey(), entry.getValue()));
            }
        }

        log.info("Finished retrieving lucene index terms.");
        indexNode.release();
        return suggestionTerms;
    }
}
