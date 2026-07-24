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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.IndexMapping;

/** Lexical (keyword) table search. */
public record MatchQuery(String queryText, String field) implements SearchQuery.QueryBody {
  public MatchQuery(String queryText) {
    this(queryText, null);
  }

  public MatchQuery {
    Objects.requireNonNull(queryText, "queryText");
  }

  public static MatchQuery fromBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new MatchQuery(queryText);
    }
    if (body instanceof Map<?, ?> matchMap && matchMap.containsKey("query")) {
      String field = matchMap.containsKey("field") ?
          StringUtils.trimToNull(matchMap.get("field").toString()) : null;
      return new MatchQuery(matchMap.get("query").toString(), field);
    }
    throw new SearchException("match query must be a string or {query: text, field: name}");
  }

  public Map<String, String> toBody() {
    if (StringUtils.isEmpty(field())) {
      return Map.of("query", queryText());
    }
    Map<String, String> body = new LinkedHashMap<>();
    body.put("query", queryText());
    body.put("field", field());
    return body;
  }

  public MatchQuery resolve(IndexMapping mapping) throws SearchException {
    String resolvedField = StringUtils.trimToNull(field);
    if (resolvedField != null) {
      mapping.validateLexicalSearchField(resolvedField);
    }
    return new MatchQuery(queryText, resolvedField);
  }
}
