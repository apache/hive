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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestSearchArgs {

  private static IndexMapping hybridMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void parseHybridString() throws Exception {
    SearchArgs.Hybrid hybrid =
        (SearchArgs.Hybrid) SearchArgs.fromBody("sales revenue", SearchQuery.Mode.HYBRID);
    assertEquals("sales revenue", hybrid.queryText());
    assertNull(hybrid.field());
  }

  @Test
  public void parseHybridFieldAndQueryObject() throws Exception {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_SEARCH_TEXT,
        "query", "orders");
    SearchArgs.Hybrid hybrid =
        (SearchArgs.Hybrid) SearchArgs.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, hybrid.field());
    assertEquals("orders", hybrid.queryText());
  }

  @Test
  public void parseHybridSingleFieldMap() throws Exception {
    Map<String, Object> body = Map.of(MetastoreTableMapper.FIELD_SEARCH_TEXT, "inventory");
    SearchArgs.Hybrid hybrid =
        (SearchArgs.Hybrid) SearchArgs.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals("inventory", hybrid.queryText());
  }

  @Test
  public void parseHybridCustomWeights() throws Exception {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_SEARCH_TEXT,
        "query", "metrics",
        "match_weight", 0.7,
        "semantic_weight", 0.3);
    SearchArgs.Hybrid hybrid =
        (SearchArgs.Hybrid) SearchArgs.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals(0.3f, hybrid.semanticWeight(), 0.001f);
  }

  @Test
  public void parseHybridRejectsInvalidWeights() {
    Map<String, Object> body = Map.of(
        "query", "metrics",
        "match_weight", 0.9,
        "semantic_weight", 0.9);
    assertThrows(SearchException.class,
        () -> SearchArgs.fromBody(body, SearchQuery.Mode.HYBRID));
  }

  @Test
  public void parseSemanticString() throws Exception {
    SearchArgs.Semantic semantic =
        (SearchArgs.Semantic) SearchArgs.fromBody("sales revenue", SearchQuery.Mode.SEMANTIC);
    assertEquals("sales revenue", semantic.queryText());
    assertNull(semantic.field());
  }

  @Test
  public void parseSemanticFieldAndQueryObject() throws Exception {
    Map<String, Object> body = Map.of(
        "field", MetastoreTableMapper.FIELD_SEARCH_TEXT,
        "query", "orders");
    SearchArgs.Semantic semantic =
        (SearchArgs.Semantic) SearchArgs.fromBody(body, SearchQuery.Mode.SEMANTIC);
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, semantic.field());
    assertEquals("orders", semantic.queryText());
  }

  @Test
  public void parseSemanticSingleFieldMap() throws Exception {
    Map<String, Object> body = Map.of(MetastoreTableMapper.FIELD_SEARCH_TEXT, "inventory");
    SearchArgs.Semantic semantic =
        (SearchArgs.Semantic) SearchArgs.fromBody(body, SearchQuery.Mode.SEMANTIC);
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, semantic.field());
    assertEquals("inventory", semantic.queryText());
  }

  @Test
  public void resolveHybridUsesSoleHybridField() throws Exception {
    SearchArgs.Hybrid args =
        (SearchArgs.Hybrid) SearchArgs.fromBody("sales revenue", SearchQuery.Mode.HYBRID);
    HybridSearch.ResolvedHybridQuery resolved = HybridSearch.resolve(args, hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, resolved.field());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void resolveHybridRejectsNonHybridField() {
    SearchArgs.Hybrid args = new SearchArgs.Hybrid(
        "t1", MetastoreTableMapper.FIELD_TABLE, null, null);
    assertThrows(SearchException.class, () -> HybridSearch.resolve(args, hybridMapping()));
  }

  @Test
  public void resolveHybridRequiresFieldWhenMultipleHybridFieldsExist() throws Exception {
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
    SearchArgs.Hybrid args =
        (SearchArgs.Hybrid) SearchArgs.fromBody("query", SearchQuery.Mode.HYBRID);

    assertThrows(SearchException.class, () -> HybridSearch.resolve(args, mapping));
  }

  @Test
  public void resolveSemanticUsesSoleSemanticField() throws Exception {
    SearchArgs.Semantic args =
        (SearchArgs.Semantic) SearchArgs.fromBody("sales revenue", SearchQuery.Mode.SEMANTIC);
    SemanticSearch.ResolvedSemanticQuery resolved =
        SemanticSearch.resolve(args, hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_SEARCH_TEXT, resolved.field());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void resolveSemanticRejectsNonSemanticField() {
    SearchArgs.Semantic args = new SearchArgs.Semantic("t1", MetastoreTableMapper.FIELD_TABLE);
    assertThrows(SearchException.class, () -> SemanticSearch.resolve(args, hybridMapping()));
  }

  @Test
  public void serializeMatchRoundTrip() throws Exception {
    assertRoundTrip(new SearchArgs.Match("orders"), SearchQuery.Mode.MATCH);
    assertEquals(Map.of("query", "orders"), SearchArgs.toBody(new SearchArgs.Match("orders")));
  }

  @Test
  public void serializeSemanticRoundTrip() throws Exception {
    assertRoundTrip(new SearchArgs.Semantic("sales", null), SearchQuery.Mode.SEMANTIC);
    assertRoundTrip(
        new SearchArgs.Semantic("orders", MetastoreTableMapper.FIELD_SEARCH_TEXT),
        SearchQuery.Mode.SEMANTIC);
    assertEquals(
        Map.of(
            "query", "orders",
            "field", MetastoreTableMapper.FIELD_SEARCH_TEXT),
        SearchArgs.toBody(
            new SearchArgs.Semantic("orders", MetastoreTableMapper.FIELD_SEARCH_TEXT)));
  }

  @Test
  public void serializeHybridRoundTrip() throws Exception {
    assertRoundTrip(new SearchArgs.Hybrid("sales", null, null, null), SearchQuery.Mode.HYBRID);
    assertRoundTrip(
        new SearchArgs.Hybrid("metrics", MetastoreTableMapper.FIELD_SEARCH_TEXT, null, null),
        SearchQuery.Mode.HYBRID);
    assertRoundTrip(
        new SearchArgs.Hybrid("metrics", null, 0.7f, 0.3f),
        SearchQuery.Mode.HYBRID);
    assertRoundTrip(
        new SearchArgs.Hybrid("metrics", MetastoreTableMapper.FIELD_SEARCH_TEXT, 0.7f, 0.3f),
        SearchQuery.Mode.HYBRID);
  }

  @Test
  public void serializeToQueryBodyRoundTrip() throws Exception {
    SearchQuery query = SearchQuery.of("sales", SearchQuery.Mode.HYBRID, 10);
    SearchQuery roundTripped = SearchQuery.fromQueryBody(
        query.toQueryBody(),
        query.mode(),
        query.catalogName(),
        query.databaseName(),
        query.limit(),
        query.returnFields());
    assertEquals(query.args(), roundTripped.args());
    assertEquals(query.mode(), roundTripped.mode());
  }

  private static void assertRoundTrip(SearchArgs args, SearchQuery.Mode mode) throws SearchException {
    SearchArgs roundTripped = SearchArgs.fromBody(SearchArgs.toBody(args), mode);
    assertEquals(args, roundTripped);
  }
}
