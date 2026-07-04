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
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.mapping.FieldSchema;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.SearchParams;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestHybridSearch {

  private static IndexMapping hybridMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void parseStringUsesSoleHybridField() throws Exception {
    HybridSearch.ParsedHybridQuery parsed =
        HybridSearch.parse("sales revenue", hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, parsed.field());
    assertEquals("sales revenue", parsed.queryText());
  }

  @Test
  public void parseFieldAndQueryObject() throws Exception {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_SEARCH_TEXT,
        "query", "orders");
    HybridSearch.ParsedHybridQuery parsed = HybridSearch.parse(body, hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, parsed.field());
    assertEquals("orders", parsed.queryText());
  }

  @Test
  public void parseSingleFieldMap() throws Exception {
    Map<String, Object> body = Map.of(MetastoreTableMapper.FIELD_SEARCH_TEXT, "inventory");
    HybridSearch.ParsedHybridQuery parsed = HybridSearch.parse(body, hybridMapping());
    assertEquals("inventory", parsed.queryText());
  }

  @Test
  public void parseCustomWeights() throws Exception {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_SEARCH_TEXT,
        "query", "metrics",
        "match_weight", 0.7,
        "semantic_weight", 0.3);
    HybridSearch.ParsedHybridQuery parsed = HybridSearch.parse(body, hybridMapping());
    assertEquals(0.7f, parsed.matchWeight(), 0.001f);
    assertEquals(0.3f, parsed.semanticWeight(), 0.001f);
  }

  @Test
  public void parseRejectsNonHybridField() {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_TABLE,
        "query", "t1");
    assertThrows(SearchException.class, () -> HybridSearch.parse(body, hybridMapping()));
  }

  @Test
  public void parseRequiresFieldWhenMultipleHybridFieldsExist() {
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

    assertThrows(SearchException.class, () -> HybridSearch.parse("query", mapping));
  }

  @Test
  public void toFusionRequestUsesDefaultsFromSearchConfig() {
    HybridSearch.ParsedHybridQuery parsed =
        new HybridSearch.ParsedHybridQuery(MetastoreTableMapper.FIELD_SEARCH_TEXT, "sales", null, null);
    SearchConfig searchConfig = new SearchConfig(new Configuration(false));
    SearchInternal.FusionRequest request =
        HybridSearch.toFusionRequest(parsed, 20, searchConfig);

    assertEquals(20, request.size());
    assertEquals(2, request.retrievers().size());
    assertEquals("match", request.retrievers().get(0).name());
    assertEquals(
        "sales",
        request.retrievers().get(0).query().get("table_keyword"));
    assertEquals(searchConfig.getHybridMatchWeight(), request.retrievers().get(0).weight(), 0.001f);
    assertEquals(searchConfig.getHybridSemanticWeight(), request.retrievers().get(1).weight(), 0.001f);
  }
}
