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
import org.apache.hive.search.metastore.MetastoreIndexSchema;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestSearchArgs {

  private static IndexMapping hybridMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreIndexSchema.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void parseHybridString() throws Exception {
    SearchMethod.Hybrid hybrid =
        (SearchMethod.Hybrid) SearchMethod.fromBody("sales revenue", SearchQuery.Mode.HYBRID);
    assertEquals("sales revenue", hybrid.queryText());
  }

  @Test
  public void parseHybridQueryObject() throws Exception {
    Map<String, Object> body = Map.of("query", "orders");
    SearchMethod.Hybrid hybrid =
        (SearchMethod.Hybrid) SearchMethod.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals("orders", hybrid.queryText());
  }

  @Test
  public void parseHybridQueryOnlyObject() throws Exception {
    Map<String, Object> body = Map.of("query", "inventory");
    SearchMethod.Hybrid hybrid =
        (SearchMethod.Hybrid) SearchMethod.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals("inventory", hybrid.queryText());
  }

  @Test
  public void parseHybridCustomWeights() throws Exception {
    Map<String, Object> body = Map.of(
        "query", "metrics",
        "match_weight", 0.7,
        "semantic_weight", 0.3);
    SearchMethod.Hybrid hybrid =
        (SearchMethod.Hybrid) SearchMethod.fromBody(body, SearchQuery.Mode.HYBRID);
    assertEquals(0.3f, hybrid.semanticWeight(), 0.001f);
  }

  @Test
  public void parseHybridRejectsInvalidWeights() {
    Map<String, Object> body = Map.of(
        "query", "metrics",
        "match_weight", 0.9,
        "semantic_weight", 0.9);
    assertThrows(SearchException.class,
        () -> SearchMethod.fromBody(body, SearchQuery.Mode.HYBRID));
  }

  @Test
  public void parseSemanticString() throws Exception {
    SearchMethod.Semantic semantic =
        (SearchMethod.Semantic) SearchMethod.fromBody("sales revenue", SearchQuery.Mode.SEMANTIC);
    assertEquals("sales revenue", semantic.queryText());
  }

  @Test
  public void parseSemanticQueryObject() throws Exception {
    Map<String, Object> body = Map.of("query", "orders");
    SearchMethod.Semantic semantic =
        (SearchMethod.Semantic) SearchMethod.fromBody(body, SearchQuery.Mode.SEMANTIC);
    assertEquals("orders", semantic.queryText());
  }

  @Test
  public void parseSemanticQueryOnlyObject() throws Exception {
    Map<String, Object> body = Map.of("query", "inventory");
    SearchMethod.Semantic semantic =
        (SearchMethod.Semantic) SearchMethod.fromBody(body, SearchQuery.Mode.SEMANTIC);
    assertEquals("inventory", semantic.queryText());
  }

  @Test
  public void parseMatchWithField() throws Exception {
    Map<String, Object> body = Map.of(
        "query", "orders",
        "field", MetastoreTableMapper.FIELD_COMMENT);
    SearchMethod.Match match =
        (SearchMethod.Match) SearchMethod.fromBody(body, SearchQuery.Mode.MATCH);
    assertEquals("orders", match.queryText());
    assertEquals(MetastoreTableMapper.FIELD_COMMENT, match.field());
  }

  @Test
  public void resolveMatchValidatesLexicalField() throws Exception {
    SearchMethod.Match args = new SearchMethod.Match("x", MetastoreTableMapper.FIELD_COMMENT);
    LexicalSearch.ResolvedMatchQuery resolved = LexicalSearch.resolve(args, hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_COMMENT, resolved.field());
  }

  @Test
  public void resolveMatchRejectsSemanticOnlyField() {
    SearchMethod.Match args = new SearchMethod.Match("x", MetastoreTableMapper.FIELD_SEARCH_TEXT);
    assertThrows(SearchException.class, () -> LexicalSearch.resolve(args, hybridMapping()));
  }

  @Test
  public void resolveHybridUsesDefaultSemanticFields() throws Exception {
    SearchMethod.Hybrid args =
        (SearchMethod.Hybrid) SearchMethod.fromBody("sales revenue", SearchQuery.Mode.HYBRID);
    HybridSearch.ResolvedHybridQuery resolved = HybridSearch.resolve(args, hybridMapping());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void resolveHybridDefaultsWhenMultipleSemanticFieldsExist() throws Exception {
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
    SearchMethod.Hybrid args =
        (SearchMethod.Hybrid) SearchMethod.fromBody("query", SearchQuery.Mode.HYBRID);

    HybridSearch.ResolvedHybridQuery resolved = HybridSearch.resolve(args, mapping);
    assertEquals("query", resolved.queryText());
  }

  @Test
  public void resolveSemanticUsesDefaultSemanticFields() throws Exception {
    SearchMethod.Semantic args =
        (SearchMethod.Semantic) SearchMethod.fromBody("sales revenue", SearchQuery.Mode.SEMANTIC);
    SemanticSearch.ResolvedSemanticQuery resolved =
        SemanticSearch.resolve(args, hybridMapping());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void serializeMatchRoundTrip() throws Exception {
    assertRoundTrip(new SearchMethod.Match("orders"), SearchQuery.Mode.MATCH);
    assertEquals(Map.of("query", "orders"), SearchMethod.toBody(new SearchMethod.Match("orders")));
  }

  @Test
  public void serializeMatchWithFieldRoundTrip() throws Exception {
    SearchMethod.Match match =
        new SearchMethod.Match("orders", MetastoreTableMapper.FIELD_TABLE);
    assertRoundTrip(match, SearchQuery.Mode.MATCH);
    Map<String, String> body = SearchMethod.toBody(match);
    assertEquals("orders", body.get("query"));
    assertEquals(MetastoreTableMapper.FIELD_TABLE, body.get("field"));
  }

  @Test
  public void serializeSemanticRoundTrip() throws Exception {
    assertRoundTrip(new SearchMethod.Semantic("sales"), SearchQuery.Mode.SEMANTIC);
    assertEquals(Map.of("query", "orders"), SearchMethod.toBody(new SearchMethod.Semantic("orders")));
  }

  @Test
  public void serializeHybridRoundTrip() throws Exception {
    assertRoundTrip(new SearchMethod.Hybrid("sales", null, null), SearchQuery.Mode.HYBRID);
    assertRoundTrip(new SearchMethod.Hybrid("metrics", 0.7f, 0.3f), SearchQuery.Mode.HYBRID);
    Map<String, String> body = SearchMethod.toBody(new SearchMethod.Hybrid("metrics", 0.7f, 0.3f));
    assertEquals("metrics", body.get("query"));
    assertEquals("0.7", body.get("match_weight"));
    assertEquals("0.3", body.get("semantic_weight"));
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

  private static void assertRoundTrip(SearchMethod args, SearchQuery.Mode mode) throws SearchException {
    SearchMethod roundTripped = SearchMethod.fromBody(SearchMethod.toBody(args), mode);
    assertEquals(args, roundTripped);
  }
}
