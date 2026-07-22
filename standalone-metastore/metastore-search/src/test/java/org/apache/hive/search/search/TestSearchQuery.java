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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.exception.SearchException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestSearchQuery {

  @Test
  public void ofTextDefaultsToHybridMode() throws Exception {
    SearchQuery query = SearchQuery.of("sales");
    assertTrue(query.body() instanceof HybridQuery);
    assertEquals("sales", query.body().queryText());
    assertEquals(SearchQuery.Mode.HYBRID, query.mode());
    assertEquals(0, query.limit());
  }

  @Test
  public void ofTextWithModeAndLimit() throws Exception {
    SearchQuery query = SearchQuery.of("sales", SearchQuery.Mode.SEMANTIC, 25);
    assertTrue(query.body() instanceof SemanticQuery);
    assertEquals(SearchQuery.Mode.SEMANTIC, query.mode());
    assertEquals(25, query.limit());
  }

  @Test
  public void ofTableNameUsesKeywordMode() throws Exception {
    SearchQuery query = SearchQuery.of("orders", "hive", "default");
    assertTrue(query.body() instanceof MatchQuery);
    assertEquals("orders", query.body().queryText());
    assertEquals(SearchQuery.Mode.MATCH, query.mode());
    assertEquals("hive", query.catalogName());
    assertEquals("default", query.databaseName());
  }

  @Test
  public void rejectsEmptyQueryText() {
    org.junit.Assert.assertThrows(SearchException.class, () -> SearchQuery.of(""));
  }

  @Test
  public void rejectsNegativeLimit() {
    org.junit.Assert.assertThrows(SearchException.class,
        () -> SearchQuery.of("sales", SearchQuery.Mode.HYBRID, -1));
  }
}
