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

package org.apache.hive.search.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.config.IndexStoreOptions;
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.metastore.SearchTextSegment;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public record IndexMapping(
    String indexName, Configuration configuration, Map<String, FieldSchema> fields) {

  public IndexOptions config() {
    return new IndexOptions(configuration);
  }

  public IndexStoreOptions store() {
    return new IndexStoreOptions(configuration, indexName);
  }

  public InferenceOptions inference() {
    return new InferenceOptions(configuration);
  }

  public SearchOptions search() {
    return new SearchOptions(configuration);
  }

  public FieldSchema fieldSchema(String fieldName) {
    return fields.get(fieldName);
  }

  public List<String> hybridFields() {
    List<String> result = new ArrayList<>();
    for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
      if (entry.getValue() instanceof FieldSchema.TextFieldSchema text && text.search().hybrid()) {
        result.add(entry.getKey());
      }
    }
    return result;
  }

  public Optional<String> soleHybridField() {
    List<String> hybridFields = hybridFields();
    return hybridFields.size() == 1 ? Optional.of(hybridFields.getFirst()) : Optional.empty();
  }

  public List<String> searchTextSegmentFields() {
    return fields.keySet().stream()
        .filter(SearchTextSegment::isSegmentField)
        .sorted(java.util.Comparator.comparingInt(SearchTextSegment::segmentIndex))
        .toList();
  }

  public boolean isSearchTextLogicalField(String fieldName) {
    return MetastoreTableMapper.FIELD_SEARCH_TEXT.equals(fieldName);
  }

  /**
   * Expands logical {@code search_text} to segment fields; passes through a single segment or
   * other semantic field name.
   */
  public List<String> resolveSemanticSearchFields(String fieldName) throws SearchException {
    if (StringUtils.isEmpty(fieldName) || isSearchTextLogicalField(fieldName)) {
      List<String> segments = searchTextSegmentFields();
      if (!segments.isEmpty()) {
        return segments;
      }
      if (isSearchTextLogicalField(fieldName)) {
        throw new SearchException(
            "field '" + MetastoreTableMapper.FIELD_SEARCH_TEXT + "' is not configured in this index");
      }
      List<String> semanticFields = semanticFieldNames();
      if (semanticFields.isEmpty()) {
        throw new SearchException("no semantic search fields are configured in this index");
      }
      return semanticFields;
    }
    if (SearchTextSegment.isSegmentField(fieldName)) {
      validateSemanticField(fieldName);
      return List.of(fieldName);
    }
    validateSemanticField(fieldName);
    return List.of(fieldName);
  }

  private List<String> semanticFieldNames() {
    return fields.entrySet().stream()
        .filter(entry -> entry.getValue() instanceof FieldSchema.TextFieldSchema text
            && text.search().semantic())
        .map(Map.Entry::getKey)
        .toList();
  }

  private void validateSemanticField(String fieldName) throws SearchException {
    FieldSchema schema = fieldSchema(fieldName);
    if (!(schema instanceof FieldSchema.TextFieldSchema text) || !text.search().semantic()) {
      throw new SearchException("field '" + fieldName + "' is not semantically searchable");
    }
  }

  public void validateLexicalSearchField(String fieldName) throws SearchException {
    if (isSearchTextLogicalField(fieldName)) {
      throw new SearchException(
          "field '" + MetastoreTableMapper.FIELD_SEARCH_TEXT + "' is not lexically searchable");
    }
    FieldSchema schema = fieldSchema(fieldName);
    if (!(schema instanceof FieldSchema.TextFieldSchema text)) {
      throw new SearchException("field '" + fieldName + "' is not lexically searchable");
    }
    if (!text.search().lexical() && !text.filter()) {
      throw new SearchException("field '" + fieldName + "' is not lexically searchable");
    }
  }

  public Analyzer analyzer() {
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
      if (entry.getValue() instanceof FieldSchema.TextFieldSchema text && text.search().lexical()) {
        fieldAnalyzers.put(entry.getKey(), new StandardAnalyzer());
      }
    }
    return new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), fieldAnalyzers);
  }
}
