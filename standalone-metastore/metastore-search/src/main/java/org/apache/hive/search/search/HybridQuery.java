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

import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.IndexMapping;

/** Hybrid lexical + semantic search with optional fusion weights. */
public record HybridQuery(String queryText, Float matchWeight, Float semanticWeight)
    implements SearchQuery.QueryBody {
  public HybridQuery {
    Objects.requireNonNull(queryText, "queryText");
  }

  public static HybridQuery fromBody(Object body) throws SearchException {
    if (body instanceof String queryText) {
      return new HybridQuery(queryText, null, null);
    }
    if (!(body instanceof Map<?, ?> hybridMap)) {
      throw new SearchException("hybrid query must be a string or object");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> hybridBody = (Map<String, Object>) hybridMap;
    Float matchWeight = parseWeight(hybridBody, "match_weight", "match");
    Float semanticWeight = parseWeight(hybridBody, "semantic_weight", "semantic");
    validateWeights(matchWeight, semanticWeight);

    if (hybridBody.containsKey("query")) {
      return new HybridQuery(hybridBody.get("query").toString(), matchWeight, semanticWeight);
    }
    throw new SearchException(
        "hybrid query must be a string or {query: text, match_weight:, semantic_weight:}");
  }

  public Map<String, String> toBody() {
    Map<String, String> body = new LinkedHashMap<>();
    body.put("query", queryText());
    if (matchWeight() != null) {
      body.put("match_weight", matchWeight().toString());
    }
    if (semanticWeight() != null) {
      body.put("semantic_weight", semanticWeight().toString());
    }
    return body;
  }

  /** Validates index semantic fields and normalizes fusion weights ({@code matchWeight} cleared). */
  public HybridQuery resolve(IndexMapping mapping) throws SearchException {
    mapping.resolveSemanticSearchFields(null);
    Float resolvedSemantic = resolveSemanticWeight(matchWeight, semanticWeight);
    return new HybridQuery(queryText, null, resolvedSemantic);
  }

  public MatchQuery toMatchQuery() {
    return new MatchQuery(queryText);
  }

  public SemanticQuery toSemanticQuery() {
    return new SemanticQuery(queryText);
  }

  /** Semantic fraction for fusion; uses config when the request omitted weights. */
  public float semanticWeight(SearchConfig searchConfig) throws SearchException {
    return semanticWeight != null ? semanticWeight : searchConfig.getHybridSemanticWeight();
  }

  private static Float resolveSemanticWeight(Float matchWeight, Float semanticWeight) {
    if (semanticWeight != null) {
      return semanticWeight;
    }
    if (matchWeight != null) {
      return 1.0f - matchWeight;
    }
    return null;
  }

  private static Float parseWeight(
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

  private static void validateWeights(Float matchWeight, Float semanticWeight)
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
