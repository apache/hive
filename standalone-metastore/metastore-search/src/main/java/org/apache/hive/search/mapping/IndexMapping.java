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

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.IndexStateConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.config.SearchConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public record IndexMapping(
    String indexName, Configuration configuration, Map<String, FieldSchema> fields) {

  public IndexConfig config() {
    return new IndexConfig(configuration);
  }

  public IndexStateConfig store() {
    return new IndexStateConfig(configuration, indexName);
  }

  public InferenceConfig inference() {
    return new InferenceConfig(configuration);
  }

  public SearchConfig search() {
    return new SearchConfig(configuration);
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
