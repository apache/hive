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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
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
      future.get(searchConfig.getInitReadyTimeoutMs(), TimeUnit.MILLISECONDS);
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
    List<String> fields = buildReturnFields(query);
    int limit = query.limit() > 0 ? query.limit() : searchConfig.getDefaultLimit();
    try (SearchInternal searcher = session.getSearcher()) {
      SearchReqResp.Response response = searcher.search(SearchReqResp.Request.validated(
          toInternalQuery(query),
          fields,
          limit,
          query.catalogName(),
          query.databaseName()));
      List<TableSearchHit> hits = new ArrayList<>();
      for (Map<String, Object> raw : response.hits()) {
        hits.add(toHit(raw, query.catalogName()));
      }
      return new TableSearchResult(hits, response.total());
    }
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  private static List<String> buildReturnFields(SearchQuery query) {
    if (!query.returnFields().isEmpty()) {
      return query.returnFields();
    }
    return List.of(
        MetastoreTableMapper.FIELD_DB,
        MetastoreTableMapper.FIELD_TABLE,
        MetastoreTableMapper.FIELD_OWNER,
        MetastoreTableMapper.FIELD_COMMENT);
  }

  private static Map<String, Object> toInternalQuery(SearchQuery query) {
    String text = query.queryText();
    return switch (query.mode()) {
      case KEYWORD -> Map.of("table_keyword", text);
      case SEMANTIC -> Map.of("semantic",
          Map.of(MetastoreTableMapper.FIELD_SEARCH_TEXT, text));
      case HYBRID -> Map.of("hybrid", text);
    };
  }

  private static TableSearchHit toHit(Map<String, Object> raw, String defaultCatalog) {
    TableName tableName = TableName.fromString(raw.get("_id").toString(), defaultCatalog, "default");
    float score =
        raw.get("_score") instanceof Number number ? number.floatValue() : 0f;
    Map<String, String> fields = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : raw.entrySet()) {
      if ("_score".equals(entry.getKey()) || "_id".equals(entry.getKey())) {
        continue;
      }
      if (entry.getValue() != null) {
        fields.put(entry.getKey(), entry.getValue().toString());
      }
    }
    return new TableSearchHit(tableName, score, fields);
  }
}
