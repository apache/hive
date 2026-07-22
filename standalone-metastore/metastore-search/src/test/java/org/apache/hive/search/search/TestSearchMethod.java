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
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.metastore.MetastoreIndexSchema;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestSearchMethod {

  private static IndexMapping hybridMapping() {
    Configuration conf = new Configuration(false);
    return MetastoreIndexSchema.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
  }

  @Test
  public void parseHybridString() throws Exception {
    HybridQuery hybrid = HybridQuery.fromBody("sales revenue");
    assertEquals("sales revenue", hybrid.queryText());
  }

  @Test
  public void parseHybridQueryObject() throws Exception {
    Map<String, Object> body = Map.of("query", "orders");
    assertEquals("orders", HybridQuery.fromBody(body).queryText());
  }

  @Test
  public void parseHybridCustomWeights() throws Exception {
    Map<String, Object> body = Map.of(
        "query", "metrics",
        "match_weight", 0.7,
        "semantic_weight", 0.3);
    HybridQuery hybrid = HybridQuery.fromBody(body);
    assertEquals(0.3f, hybrid.semanticWeight(), 0.001f);
  }

  @Test
  public void parseHybridRejectsInvalidWeights() {
    Map<String, Object> body = Map.of(
        "query", "metrics",
        "match_weight", 0.9,
        "semantic_weight", 0.9);
    assertThrows(SearchException.class, () -> HybridQuery.fromBody(body));
  }

  @Test
  public void parseSemanticString() throws Exception {
    assertEquals("sales revenue", SemanticQuery.fromBody("sales revenue").queryText());
  }

  @Test
  public void parseSemanticQueryObject() throws Exception {
    Map<String, Object> body = Map.of("query", "orders");
    assertEquals("orders", SemanticQuery.fromBody(body).queryText());
  }

  @Test
  public void parseMatchWithField() throws Exception {
    Map<String, Object> body = Map.of(
        "query", "orders",
        "field", MetastoreTableMapper.FIELD_COMMENT);
    MatchQuery match = MatchQuery.fromBody(body);
    assertEquals("orders", match.queryText());
    assertEquals(MetastoreTableMapper.FIELD_COMMENT, match.field());
  }

  @Test
  public void resolveMatchValidatesLexicalField() throws Exception {
    MatchQuery args = new MatchQuery("x", MetastoreTableMapper.FIELD_COMMENT);
    MatchQuery resolved = args.resolve(hybridMapping());
    assertEquals(MetastoreTableMapper.FIELD_COMMENT, resolved.field());
  }

  @Test
  public void resolveMatchRejectsSemanticOnlyField() {
    MatchQuery args = new MatchQuery("x", MetastoreTableMapper.FIELD_SEARCH_TEXT);
    assertThrows(SearchException.class, () -> args.resolve(hybridMapping()));
  }

  @Test
  public void resolveHybridUsesDefaultSemanticFields() throws Exception {
    HybridQuery args = HybridQuery.fromBody("sales revenue");
    HybridQuery resolved = args.resolve(hybridMapping());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void resolveSemanticUsesDefaultSemanticFields() throws Exception {
    SemanticQuery args = SemanticQuery.fromBody("sales revenue");
    SemanticQuery resolved = args.resolve(hybridMapping());
    assertEquals("sales revenue", resolved.queryText());
  }

  @Test
  public void serializeMatchRoundTrip() throws Exception {
    assertMatchRoundTrip(new MatchQuery("orders"));
    assertEquals(Map.of("query", "orders"), new MatchQuery("orders").toBody());
  }

  @Test
  public void serializeMatchWithFieldRoundTrip() throws Exception {
    MatchQuery match = new MatchQuery("orders", MetastoreTableMapper.FIELD_TABLE);
    assertMatchRoundTrip(match);
    Map<String, String> body = match.toBody();
    assertEquals("orders", body.get("query"));
    assertEquals(MetastoreTableMapper.FIELD_TABLE, body.get("field"));
  }

  @Test
  public void serializeSemanticRoundTrip() throws Exception {
    assertSemanticRoundTrip(new SemanticQuery("sales"));
    assertEquals(Map.of("query", "orders"), new SemanticQuery("orders").toBody());
  }

  @Test
  public void serializeHybridRoundTrip() throws Exception {
    assertHybridRoundTrip(new HybridQuery("sales", null, null));
    assertHybridRoundTrip(new HybridQuery("metrics", 0.7f, 0.3f));
    Map<String, String> body = new HybridQuery("metrics", 0.7f, 0.3f).toBody();
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
    assertEquals(query.body(), roundTripped.body());
    assertEquals(query.mode(), roundTripped.mode());
  }

  private static void assertMatchRoundTrip(MatchQuery query) throws SearchException {
    assertEquals(query, MatchQuery.fromBody(query.toBody()));
  }

  private static void assertSemanticRoundTrip(SemanticQuery query) throws SearchException {
    assertEquals(query, SemanticQuery.fromBody(query.toBody()));
  }

  private static void assertHybridRoundTrip(HybridQuery query) throws SearchException {
    assertEquals(query, HybridQuery.fromBody(query.toBody()));
  }
}
