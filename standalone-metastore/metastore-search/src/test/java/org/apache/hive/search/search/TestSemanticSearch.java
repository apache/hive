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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.SearchParams;
import org.apache.hive.search.metastore.MetastoreIndexSchema;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestSemanticSearch {

  private static IndexMapping defaultMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreIndexSchema.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void resolveUsesDefaultSemanticFields() throws Exception {
    SemanticQuery resolved =
        new SemanticQuery("sales revenue").resolve(defaultMapping());
    assertEquals("sales revenue", resolved.queryText());
    assertEquals(4, defaultMapping().resolveSemanticSearchFields(null).size());
  }

  @Test
  public void resolveDefaultsToAllSemanticFieldsWhenNoSegments() throws Exception {
    Configuration conf = new Configuration(false);
    Map<String, FieldSchema> fields = new LinkedHashMap<>();
    fields.put(
        "field_a",
        new FieldSchema.TextFieldSchema(
            "field_a", new SearchParams(true, "model-a", SearchParams.VectorDistance.COSINE)));
    fields.put(
        "field_b",
        new FieldSchema.TextFieldSchema(
            "field_b", new SearchParams(true, "model-b", SearchParams.VectorDistance.COSINE)));
    IndexMapping mapping = new IndexMapping("idx", conf, fields);

    SemanticQuery resolved = new SemanticQuery("query").resolve(mapping);
    assertEquals("query", resolved.queryText());
    assertEquals(List.of("field_a", "field_b"), mapping.resolveSemanticSearchFields(null));
  }
}
