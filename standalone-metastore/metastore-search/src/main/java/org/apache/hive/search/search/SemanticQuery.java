/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.search;

import java.util.Map;
import java.util.Objects;

import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.IndexMapping;

/** Semantic (vector) table search over configured segment fields. */
public record SemanticQuery(String queryText) implements SearchQuery.QueryBody {
  public SemanticQuery {
    Objects.requireNonNull(queryText, "queryText");
  }

  public static SemanticQuery fromBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new SemanticQuery(queryText);
    }
    if (body instanceof Map<?, ?> semanticMap && semanticMap.containsKey("query")) {
      return new SemanticQuery(semanticMap.get("query").toString());
    }
    throw new SearchException("semantic query must be a string or {query: text}");
  }

  public Map<String, String> toBody() {
    return Map.of("query", queryText());
  }

  public SemanticQuery resolve(IndexMapping mapping) throws SearchException {
    mapping.resolveSemanticSearchFields(null);
    return this;
  }
}
