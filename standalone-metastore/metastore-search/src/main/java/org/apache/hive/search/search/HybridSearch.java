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
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.metastore.MetastoreTableMapper;

/** Builds RRF queries that fuse lexical and semantic retrieval on hybrid fields. */
public final class HybridSearch {
  private HybridSearch() {}

  public record ParsedHybridQuery(
      String field,
      String queryText,
      Float semanticWeight) {}

  public static ParsedHybridQuery parse(Object hybridBody, IndexMapping mapping)
      throws SearchException {
    if (hybridBody instanceof String queryText) {
      String field = requireHybridField(mapping, null);
      return new ParsedHybridQuery(field, queryText, null);
    }
    if (!(hybridBody instanceof Map<?, ?> body)) {
      throw new SearchException("hybrid query must be a string or object");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> hybridMap = (Map<String, Object>) body;

    HybridWeights weights = parseHybridWeights(hybridMap);

    if (hybridMap.containsKey("field") && hybridMap.containsKey("query")) {
      String field = hybridMap.get("field").toString();
      validateHybridField(mapping, field);
      return new ParsedHybridQuery(
          field, hybridMap.get("query").toString(), weights.getSemanticWeight());
    }

    if (hybridMap.containsKey("query")) {
      String field = requireHybridField(mapping, null);
      return new ParsedHybridQuery(
          field, hybridMap.get("query").toString(), weights.getSemanticWeight());
    }

    List<Map.Entry<String, Object>> fieldEntries =
        hybridMap.entrySet().stream()
            .filter(entry -> !isHybridOption(entry.getKey()))
            .filter(entry -> entry.getValue() != null)
            .toList();
    if (fieldEntries.size() == 1) {
      Map.Entry<String, Object> entry = fieldEntries.getFirst();
      validateHybridField(mapping, entry.getKey());
      return new ParsedHybridQuery(
          entry.getKey(), entry.getValue().toString(), weights.getSemanticWeight());
    }

    throw new SearchException(
        "hybrid query must be a string, {field:, query:}, {query:}, or {<hybrid_field>: text}");
  }

  public static SearchInternal.FusionRequest toFusionRequest(
      ParsedHybridQuery hybrid, int defaultSize, SearchConfig searchConfig) {
    float semanticWeight = hybrid.semanticWeight() != null ?
        hybrid.semanticWeight() : searchConfig.getHybridSemanticWeight();
    float matchWeight = 1.0f - semanticWeight;
    Map<String, Object> lexicalQuery = lexicalQueryForHybrid(hybrid);
    List<SearchInternal.RetrieverSpec> retrievers =
        List.of(
            new SearchInternal.RetrieverSpec(lexicalQuery, matchWeight, "match"),
            new SearchInternal.RetrieverSpec(
                Map.of("semantic", Map.of(hybrid.field(), hybrid.queryText())), semanticWeight, "semantic"));
    return new SearchInternal.FusionRequest(retrievers, defaultSize);
  }

  private static Map<String, Object> lexicalQueryForHybrid(ParsedHybridQuery hybrid) {
    if (MetastoreTableMapper.FIELD_SEARCH_TEXT.equals(hybrid.field())) {
      return Map.of("table_keyword", hybrid.queryText());
    }
    return Map.of("match", Map.of(hybrid.field(), hybrid.queryText()));
  }

  private record HybridWeights(Float match, Float semantic) {
    HybridWeights validate() {
      if (!validate(match)) {
        throw new IllegalArgumentException();
      }
      if (!validate(semantic)) {
        throw new IllegalArgumentException();
      }
      if (match != null && semantic != null && (match + semantic) > 1.0f) {
        throw new IllegalArgumentException();
      }
      return this;
    }

    boolean validate(Float weight) {
      if (weight == null) {
        return true;
      }
      if (weight >= 1f || weight <= 0.0f) {
        return false;
      }
      return true;
    }

    float getSemanticWeight() {
      if (semantic != null) {
        return semantic;
      } else if (match != null) {
        return 1.0f - match;
      }
      return 0.4f;
    }
  }

  private static HybridWeights parseHybridWeights(Map<String, Object> hybridMap) {
    Float match = null;
    Float semantic = null;
    if (hybridMap.get("match_weight") instanceof Number matchWeight) {
      match = matchWeight.floatValue();
    }
    if (hybridMap.get("semantic_weight") instanceof Number semanticWeight) {
      semantic = semanticWeight.floatValue();
    }
    if (hybridMap.get("weights") instanceof Map<?, ?> weights) {
      if (weights.get("match") instanceof Number matchValue) {
        match = matchValue.floatValue();
      }
      if (weights.get("semantic") instanceof Number semanticValue) {
        semantic = semanticValue.floatValue();
      }
    }
    return new HybridWeights(match, semantic).validate();
  }

  private static boolean isHybridOption(String key) {
    return "match_weight".equals(key)
        || "semantic_weight".equals(key)
        || "weights".equals(key);
  }

  private static String requireHybridField(IndexMapping mapping, String field)
      throws SearchException {
    if (StringUtils.isNotEmpty(field)) {
      validateHybridField(mapping, field);
      return field;
    }
    return mapping
        .soleHybridField()
        .orElseThrow(
            () ->
                new SearchException(
                    "hybrid query requires field when index has "
                        + mapping.hybridFields().size()
                        + " hybrid field(s): "
                        + mapping.hybridFields()));
  }

  private static void validateHybridField(IndexMapping mapping, String field)
      throws SearchException {
    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().hybrid()) {
      throw new SearchException("field '" + field + "' is not configured for hybrid search");
    }
  }
}
