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

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface IndexTermProvider {
    /**
     * Retrieves terms with frequency from index at given path restricted to given set of fields
     * @param indexPath the index to be used for retrieving terms
     * @param fields the set of allowed fields to be used for retrieving index terms
     * @param minFreq the minimum term frequency across all allowed fields
     * @param minTermLength the minimum length of term string
     * @return  a list of {@link IndexTerm}s
     * @throws IOException
     */
    List<IndexTerm> retrieveTerms(String indexPath,  Set<String> fields, int minFreq, int minTermLength) throws IOException;
}
