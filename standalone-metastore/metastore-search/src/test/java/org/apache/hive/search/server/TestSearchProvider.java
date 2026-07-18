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

package org.apache.hive.search.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.search.SearchBackend;
import org.apache.hive.search.search.SearchQuery;
import org.apache.hive.search.search.TableSearchHit;
import org.apache.hive.search.search.TableSearchResult;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestSearchProvider {

  @After
  public void tearDown() throws Exception {
    SearchProvider.reset();
  }

  @Test
  public void createInitializesBackend() throws Exception {
    StubSearchBackend backend = new StubSearchBackend();
    SearchProvider provider =
        SearchProvider.install(new Configuration(false), backend);

    assertTrue(backend.initialized);
    assertTrue(provider.isReady());
    assertSame(backend, provider.backend());
    provider.close();
  }

  @Test
  public void installExposesSingleton() throws Exception {
    Configuration conf = new Configuration(false);
    SearchProvider installed =
        SearchProvider.install(conf, new StubSearchBackend());
    assertSame(installed, SearchProvider.get());
  }

  @Test
  public void installReplacesPreviousProvider() throws Exception {
    Configuration conf = new Configuration(false);
    StubSearchBackend firstBackend = new StubSearchBackend();
    StubSearchBackend secondBackend = new StubSearchBackend();
    SearchProvider first = SearchProvider.install(conf, firstBackend);
    SearchProvider second = SearchProvider.install(conf, secondBackend);

    assertSame(second, SearchProvider.get());
    assertTrue(firstBackend.closed);
    assertSame(secondBackend, second.backend());
  }

  @Test
  public void getFailsWhenNotInstalled() {
    assertThrows(IllegalStateException.class, SearchProvider::get);
  }

  @Test
  public void searchDelegatesToBackend() throws Exception {
    StubSearchBackend backend = new StubSearchBackend();
    SearchProvider provider =
        SearchProvider.install(new Configuration(false), backend);
    SearchQuery query = SearchQuery.of("sales", SearchQuery.Mode.MATCH, 5);

    TableSearchResult result = provider.search(query);

    assertEquals(1, result.total());
    assertEquals("t", result.hits().getFirst().table().getTable());
    provider.close();
  }

  private static final class StubSearchBackend implements SearchBackend {
    private boolean initialized;
    private boolean closed;

    @Override
    public void initialize(Configuration configuration) {
      initialized = true;
    }

    @Override
    public boolean isReady() {
      return initialized;
    }

    @Override
    public TableSearchResult search(SearchQuery query) {
      return new TableSearchResult(
          List.of(new TableSearchHit(
              TableName.fromString("default.t", "hive", "default"),
              1.0f,
              Map.of("table_name", "t"))),
          1);
    }

    @Override
    public void close() {
      closed = true;
      initialized = false;
    }
  }
}
