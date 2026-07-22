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
    QueryBody body,
    String catalogName,
    String databaseName,
    int limit,
    List<String> returnFields) {

  /** Union of mode-specific query payloads. */
  public sealed interface QueryBody permits MatchQuery, SemanticQuery, HybridQuery {

    String queryText();
  }

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
    return switch (body) {
      case MatchQuery m -> Mode.MATCH;
      case SemanticQuery s -> Mode.SEMANTIC;
      case HybridQuery h -> Mode.HYBRID;
    };
  }

  private static void validate(QueryBody body, int limit) throws SearchException {
    validate(body.queryText(), limit);
  }

  private static void validate(String queryText, int limit) throws SearchException {
    if (StringUtils.isBlank(queryText)) {
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
    QueryBody body = parseBody(queryBody, mode);
    validate(body, limit);
    return new SearchQuery(body, catalogName, databaseName, limit, returnFields);
  }

  private static QueryBody parseBody(Object queryBody, Mode mode) throws SearchException {
    return switch (mode) {
      case MATCH -> MatchQuery.fromBody(queryBody);
      case SEMANTIC -> SemanticQuery.fromBody(queryBody);
      case HYBRID -> HybridQuery.fromBody(queryBody);
    };
  }

  public static SearchQuery of(String queryText) throws SearchException {
    validate(queryText, 0);
    return new SearchQuery(new HybridQuery(queryText, null, null), null, null, 0, List.of());
  }

  public static SearchQuery of(String queryText, Mode mode, int limit) throws SearchException {
    validate(queryText, limit);
    QueryBody body = switch (mode) {
      case MATCH -> new MatchQuery(queryText);
      case SEMANTIC -> new SemanticQuery(queryText);
      case HYBRID -> new HybridQuery(queryText, null, null);
    };
    return new SearchQuery(body, null, null, limit, List.of());
  }

  public static SearchQuery of(String keyWord, String catalogName, String databaseName)
      throws SearchException {
    validate(keyWord, 0);
    return new SearchQuery(new MatchQuery(keyWord), catalogName, databaseName, 0, List.of());
  }

  public static SearchQuery of(SearchQuery query, List<String> returnFields, int limit) {
    return new SearchQuery(
        query.body(), query.catalogName(), query.databaseName(), limit, returnFields);
  }

  /** Serializes this query to a flat Thrift-ready query body map. */
  public Map<String, String> toQueryBody() {
    return switch (body) {
      case MatchQuery match -> match.toBody();
      case SemanticQuery semantic -> semantic.toBody();
      case HybridQuery hybrid -> hybrid.toBody();
    };
  }
}
