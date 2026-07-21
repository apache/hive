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

/** Typed search arguments. Map/JSON parsing belongs in {@link #fromBody(Object, SearchQuery.Mode)}. */
public sealed interface SearchMethod permits SearchMethod.Match, SearchMethod.Semantic, SearchMethod.Hybrid {

  record Match(String queryText, String field) implements SearchMethod {
    public Match(String queryText) {
      this(queryText, null);
    }

    public Match {
      Objects.requireNonNull(queryText, "queryText");
    }
  }

  record Semantic(String queryText) implements SearchMethod {
    public Semantic {
      Objects.requireNonNull(queryText, "queryText");
    }
  }

  record Hybrid(String queryText, Float matchWeight, Float semanticWeight) implements SearchMethod {
    public Hybrid {
      Objects.requireNonNull(queryText, "queryText");
    }
  }

  /** Deserializes an API/Thrift request body into typed search args. */
  static SearchMethod fromBody(Object body, SearchQuery.Mode mode) throws SearchException {
    return switch (mode) {
      case MATCH -> parseMatchBody(body);
      case SEMANTIC -> parseSemanticBody(body);
      case HYBRID -> parseHybridBody(body);
    };
  }

  /** Serializes typed search args to a flat string map for Thrift. */
  static Map<String, String> toBody(SearchMethod args) {
    return switch (args) {
      case Match match -> toMatchBody(match);
      case Semantic semantic -> Map.of("query", semantic.queryText());
      case Hybrid hybrid -> toHybridBody(hybrid);
    };
  }

  /** Serializes typed search args to a Thrift-ready query body map. */
  static Map<String, String> toQueryBody(SearchMethod args) {
    return toBody(args);
  }

  private static Map<String, String> toHybridBody(Hybrid hybrid) {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("query", hybrid.queryText());
    if (hybrid.matchWeight() != null) {
      body.put("match_weight", hybrid.matchWeight().toString());
    }
    if (hybrid.semanticWeight() != null) {
      body.put("semantic_weight", hybrid.semanticWeight().toString());
    }
    return body;
  }

  private static Map<String, String> toMatchBody(Match match) {
    if (StringUtils.isEmpty(match.field())) {
      return Map.of("query", match.queryText());
    }
    Map<String, String> body = new LinkedHashMap<>();
    body.put("query", match.queryText());
    body.put("field", match.field());
    return body;
  }

  private static Match parseMatchBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new Match(queryText);
    }
    if (body instanceof Map<?, ?> matchMap && matchMap.containsKey("query")) {
      String field = matchMap.containsKey("field") ?
          StringUtils.trimToNull(matchMap.get("field").toString()) : null;
      return new Match(matchMap.get("query").toString(), field);
    }
    throw new SearchException("match query must be a string or {query: text, field: name}");
  }

  private static Semantic parseSemanticBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new Semantic(queryText);
    }
    if (body instanceof Map<?, ?> semanticMap && semanticMap.containsKey("query")) {
      return new Semantic(semanticMap.get("query").toString());
    }
    throw new SearchException("semantic query must be a string or {query: text}");
  }

  private static Hybrid parseHybridBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new Hybrid(queryText, null, null);
    }
    if (!(body instanceof Map<?, ?> hybridMap)) {
      throw new SearchException("hybrid query must be a string or object");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> hybridBody = (Map<String, Object>) hybridMap;
    Float matchWeight = parseHybridWeight(hybridBody, "match_weight", "match");
    Float semanticWeight = parseHybridWeight(hybridBody, "semantic_weight", "semantic");
    validateHybridWeights(matchWeight, semanticWeight);

    if (hybridBody.containsKey("query")) {
      return new Hybrid(hybridBody.get("query").toString(), matchWeight, semanticWeight);
    }
    throw new SearchException(
        "hybrid query must be a string or {query: text, match_weight:, semantic_weight:}");
  }

  private static Float parseHybridWeight(
      Map<String, Object> hybridBody, String flatKey, String nestedKey)
      throws SearchException {
    Object flatValue = hybridBody.get(flatKey);
    if (flatValue != null) {
      return parseWeightValue(flatValue, flatKey);
    }
    if (hybridBody.get("weights") instanceof Map<?, ?> weights) {
      Object nestedValue = weights.get(nestedKey);
      if (nestedValue != null) {
        return parseWeightValue(nestedValue, nestedKey);
      }
    }
    return null;
  }

  private static Float parseWeightValue(Object value, String key) throws SearchException {
    if (value instanceof Number number) {
      return number.floatValue();
    }
    if (value instanceof String text) {
      try {
        return Float.parseFloat(text);
      } catch (NumberFormatException e) {
        throw new SearchException("Invalid hybrid weight for '" + key + "': " + text);
      }
    }
    throw new SearchException("Invalid hybrid weight for '" + key + "': " + value);
  }

  private static void validateHybridWeights(Float matchWeight, Float semanticWeight)
      throws SearchException {
    if (!isWeightValid(matchWeight)) {
      throw new SearchException(
          "Invalid hybrid lexical weight, it must be in (0, 1), but got " + matchWeight);
    }
    if (!isWeightValid(semanticWeight)) {
      throw new SearchException(
          "Invalid hybrid semantic weight, it must be in (0, 1), but got " + semanticWeight);
    }
    if (matchWeight != null && semanticWeight != null && (matchWeight + semanticWeight) > 1.0f) {
      throw new SearchException("Invalid hybrid weights, sum of weights exceeds 1.0f");
    }
  }

  private static boolean isWeightValid(Float weight) {
    return weight == null || (weight > 0.0f && weight < 1.0f);
  }
}
