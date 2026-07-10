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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.exception.SearchException;

public record SearchQuery(
    SearchArgs args,
    String catalogName,
    String databaseName,
    int limit,
    List<String> returnFields) {

  /** Public search mode exposed on the Metastore Thrift API. */
  public enum Mode {
    MATCH,
    SEMANTIC,
    HYBRID
  }

  public SearchQuery {
    returnFields = returnFields == null ? List.of() : List.copyOf(returnFields);
  }

  public Mode mode() {
    return switch (args) {
      case SearchArgs.Match m -> Mode.MATCH;
      case SearchArgs.Semantic s -> Mode.SEMANTIC;
      case SearchArgs.Hybrid h -> Mode.HYBRID;
    };
  }

  private static void validate(SearchArgs args, int limit) throws SearchException {
    String queryText = switch (args) {
      case SearchArgs.Match m -> m.queryText();
      case SearchArgs.Semantic s -> s.queryText();
      case SearchArgs.Hybrid h -> h.queryText();
    };
    validate(queryText, limit);
  }

  private static void validate(String queryText, int limit) throws SearchException {
    if (StringUtils.isEmpty(queryText)) {
      throw new SearchException("queryText is required");
    }
    if (limit < 0) {
      throw new SearchException("limit must be non-negative");
    }
  }

  public static SearchQuery fromQueryBody(
      Map<String, String> queryBody,
      Mode mode,
      String catalogName,
      String databaseName,
      int limit,
      List<String> returnFields)
      throws SearchException {
    if (queryBody == null || queryBody.isEmpty()) {
      throw new SearchException("missing query body for mode " + mode);
    }
    SearchArgs args = SearchArgs.fromBody(queryBody, mode);
    validate(args, limit);
    return new SearchQuery(args, catalogName, databaseName, limit, returnFields);
  }

  public static SearchQuery of(String queryText) throws SearchException {
    validate(queryText, 0);
    return new SearchQuery(new SearchArgs.Hybrid(queryText, null, null, null), null, null, 0, List.of());
  }

  public static SearchQuery of(String queryText, Mode mode, int limit) throws SearchException {
    validate(queryText, limit);
    SearchArgs args = switch (mode) {
      case MATCH -> new SearchArgs.Match(queryText);
      case SEMANTIC -> new SearchArgs.Semantic(queryText, null);
      case HYBRID -> new SearchArgs.Hybrid(queryText, null, null, null);
    };
    return new SearchQuery(args, null, null, limit, List.of());
  }

  public static SearchQuery of(String keyWord, String catalogName, String databaseName)
      throws SearchException {
    validate(keyWord, 0);
    return new SearchQuery(
        new SearchArgs.Match(keyWord), catalogName, databaseName, 0, List.of());
  }

  public static SearchQuery of(SearchQuery query, List<String> returnFields, int limit) {
    return new SearchQuery(
        query.args(), query.catalogName(), query.databaseName(), limit, returnFields);
  }

  /** Serializes this query to a flat Thrift-ready query body map. */
  public Map<String, String> toQueryBody() {
    return SearchArgs.toQueryBody(args);
  }
}
