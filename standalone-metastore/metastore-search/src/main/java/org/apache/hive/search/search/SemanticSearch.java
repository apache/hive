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
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;

public final class SemanticSearch {
  private SemanticSearch() {}

  public record ResolvedSemanticQuery(String field, String queryText) {}

  public static ResolvedSemanticQuery resolve(SearchArgs.Semantic args, IndexMapping mapping)
      throws SearchException {
    String field = requireSemanticField(mapping, args.field());
    return new ResolvedSemanticQuery(field, args.queryText());
  }

  private static String requireSemanticField(IndexMapping mapping, String field)
      throws SearchException {
    if (StringUtils.isNotEmpty(field)) {
      validateSemanticField(mapping, field);
      return field;
    }
    List<String> semanticFields = mapping.fields().entrySet().stream()
        .filter(entry -> entry.getValue() instanceof FieldSchema.TextFieldSchema text
            && text.search().semantic())
        .map(java.util.Map.Entry::getKey)
        .toList();
    if (semanticFields.size() == 1) {
      return semanticFields.getFirst();
    }
    throw new SearchException(
        "semantic query requires field when index has "
            + semanticFields.size()
            + " semantic field(s): "
            + semanticFields);
  }

  private static void validateSemanticField(IndexMapping mapping, String field)
      throws SearchException {
    FieldSchema schema = mapping.fieldSchema(field);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("field '" + field + "' is not semantically searchable");
    }
  }
}
