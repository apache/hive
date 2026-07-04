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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.exception.SearchException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(MetastoreUnitTest.class)
public class TestSearchQuery {

  @Test
  public void ofTextDefaultsToHybridMode() throws Exception {
    SearchQuery query = SearchQuery.of("sales");
    assertEquals("sales", query.queryText());
    assertEquals(SearchReqResp.Mode.HYBRID, query.mode());
    assertEquals(0, query.limit());
  }

  @Test
  public void ofTextWithModeAndLimit() throws Exception {
    SearchQuery query = SearchQuery.of("sales", SearchReqResp.Mode.SEMANTIC, 25);
    assertEquals(SearchReqResp.Mode.SEMANTIC, query.mode());
    assertEquals(25, query.limit());
  }

  @Test
  public void ofTableNameUsesKeywordMode() throws Exception {
    TableName tableName = new TableName("hive", "default", "orders");
    SearchQuery query = SearchQuery.of(tableName);
    assertEquals("orders", query.queryText());
    assertEquals(SearchReqResp.Mode.KEYWORD, query.mode());
    assertEquals("hive", query.catalogName());
    assertEquals("default", query.databaseName());
  }

  @Test
  public void rejectsEmptyQueryText() {
    assertThrows(SearchException.class, () -> SearchQuery.of(""));
  }

  @Test
  public void rejectsNegativeLimit() {
    assertThrows(SearchException.class,
        () -> SearchQuery.of("sales", SearchReqResp.Mode.HYBRID, -1));
  }
}
