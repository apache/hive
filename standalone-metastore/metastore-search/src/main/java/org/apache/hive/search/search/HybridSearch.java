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

import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.IndexMapping;

public final class HybridSearch {
  private HybridSearch() {}

  public record ResolvedHybridQuery(String queryText, Float semanticWeight) {
    public SemanticSearch.ResolvedSemanticQuery toSemanticQuery() {
      return new SemanticSearch.ResolvedSemanticQuery(queryText);
    }

    public LexicalSearch.ResolvedMatchQuery toMatchQuery() {
      return new LexicalSearch.ResolvedMatchQuery(queryText, null);
    }
  }

  public static ResolvedHybridQuery resolve(SearchMethod.Hybrid args, IndexMapping mapping)
      throws SearchException {
    mapping.resolveSemanticSearchFields(null);
    Float semanticWeight = resolveSemanticWeight(args.matchWeight(), args.semanticWeight());
    return new ResolvedHybridQuery(args.queryText(), semanticWeight);
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
}
