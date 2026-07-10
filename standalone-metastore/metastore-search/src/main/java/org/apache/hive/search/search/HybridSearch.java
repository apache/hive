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

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;

public final class HybridSearch {
  private HybridSearch() {}

  public record ResolvedHybridQuery(String field, String queryText, Float semanticWeight) {}

  public static ResolvedHybridQuery resolve(SearchArgs.Hybrid args, IndexMapping mapping)
      throws SearchException {
    String field = requireHybridField(mapping, args.field());
    Float semanticWeight = resolveSemanticWeight(args.matchWeight(), args.semanticWeight());
    return new ResolvedHybridQuery(field, args.queryText(), semanticWeight);
  }

  public static SearchInternal.FusionRequest toFusionRequest(
      ResolvedHybridQuery hybrid, int defaultSize, SearchConfig searchConfig)
      throws SearchException {
    float semanticWeight = hybrid.semanticWeight() != null ?
        hybrid.semanticWeight() : searchConfig.getHybridSemanticWeight();
    float matchWeight = 1.0f - semanticWeight;
    List<SearchInternal.RetrieverSpec> retrievers =
        List.of(
            new SearchInternal.RetrieverSpec(
                new SearchArgs.Match(hybrid.queryText()), matchWeight, SearchQuery.Mode.MATCH.name()),
            new SearchInternal.RetrieverSpec(
                new SearchArgs.Semantic(hybrid.queryText(), hybrid.field()),
                semanticWeight,
                SearchQuery.Mode.SEMANTIC.name())
        );
    return new SearchInternal.FusionRequest(retrievers, defaultSize);
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
