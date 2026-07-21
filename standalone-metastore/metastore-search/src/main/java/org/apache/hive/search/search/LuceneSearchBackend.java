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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.exception.IndexNotReadyException;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.exception.SearchException;
import org.apache.hive.search.index.IndexSession;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Phase 1 {@link SearchBackend} backed by an in-process Lucene index. */
public final class LuceneSearchBackend implements SearchBackend {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneSearchBackend.class);
  private IndexSession session;
  private Future<Void> future;
  private SearchConfig searchConfig;

  @Override
  public void initialize(Configuration configuration)
      throws InitializeException, IOException {
    searchConfig = new SearchConfig(configuration);
    session = new IndexSession(configuration);
    ExecutorService initThread = Executors.newFixedThreadPool(1);
    future = initThread.submit(() -> {
      long start = System.currentTimeMillis();
      session.initialize();
      LOG.info("The in-process Lucene search backend is ready for request now, time taken: {}ms",
          System.currentTimeMillis() - start);
      return null;
    });
    initThread.shutdown();
  }

  @Override
  public boolean isReady() throws IndexNotReadyException {
    if (future == null) {
      throw new IndexNotReadyException("The in-process Lucene search backend hasn't been initialized yet");
    }
    try {
      future.get(300, TimeUnit.MILLISECONDS);
      return true;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IndexNotReadyException("Search backend initialization failed", cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IndexNotReadyException("Search backend initialization interrupted", e);
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public TableSearchResult search(SearchQuery query)
      throws SearchException, IOException {
    if (!isReady()) {
      throw new IndexNotReadyException("Search index is not ready");
    }
    List<String> fields = query.returnFields();
    if (query.returnFields().isEmpty()) {
      fields = List.of(
          MetastoreTableMapper.FIELD_OWNER,
          MetastoreTableMapper.FIELD_COMMENT);
    }
    int limit = query.limit() > 0 ? query.limit() : searchConfig.getDefaultLimit();
    query = SearchQuery.of(query, fields, limit);
    try (Searcher searcher = session.getSearcher()) {
      return searcher.search(query);
    }
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
  }
}
