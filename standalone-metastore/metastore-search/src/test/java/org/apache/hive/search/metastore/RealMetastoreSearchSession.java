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

package org.apache.hive.search.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.config.IndexStoreOptions;
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.index.store.LocalStateClient;
import org.apache.hive.search.inference.EmbedderRegistry;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.search.MatchQuery;
import org.apache.hive.search.search.Searcher;
import org.apache.hive.search.search.SearchQuery;
import org.apache.hive.search.search.TableSearchResult;
import org.apache.hive.search.testutil.InMemoryIndexStateClient;
import org.apache.hive.search.testutil.RealMetastoreServer;
import org.apache.hive.search.testutil.StubEmbedder;
import org.apache.hive.search.testutil.TestLeaderElection;
import org.apache.lucene.search.BayesianScoreEstimator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.ByteBuffersDirectory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** End-to-end search session wired to a real HMS and in-memory Lucene index. */
public final class RealMetastoreSearchSession implements AutoCloseable {
  public static final String MODEL_NAME = "stub-model";
  private static final String ELECTION = "real-metastore-search-test";

  private final IndexManager indexManager;
  private final Indexer indexer;
  private final EmbedderRegistry modelRegistry;
  private final MetastoreIndexer metastoreIndexer;
  private final SearchOptions searchConfig;

  private SearcherManager searcherManager;
  private BayesianScoreEstimator.Parameters bayesianParameters;

  private RealMetastoreSearchSession(
      IndexManager indexManager,
      Indexer indexer,
      EmbedderRegistry modelRegistry,
      MetastoreIndexer metastoreIndexer,
      SearchOptions searchConfig) {
    this.indexManager = indexManager;
    this.indexer = indexer;
    this.modelRegistry = modelRegistry;
    this.metastoreIndexer = metastoreIndexer;
    this.searchConfig = searchConfig;
  }

  public static RealMetastoreSearchSession open(RealMetastoreServer server) throws Exception {
    return open(server, null);
  }

  public static RealMetastoreSearchSession open(
      RealMetastoreServer server, InMemoryIndexStateClient sharedRemote) throws Exception {
    Configuration conf = searchConfiguration(server.conf());
    IndexMapping mapping = MetastoreIndexSchema.defaultHiveTablesMapping(
        "test_index", MODEL_NAME, conf);

    ByteBuffersDirectory directory = new ByteBuffersDirectory();
    LocalStateClient local = new LocalStateClient(directory, "test_index");
    IndexManager indexManager = sharedRemote == null
        ? new IndexManager(mapping, directory, local, null)
        : new IndexManager(mapping, directory, local, sharedRemote);

    EmbedderRegistry modelRegistry = new EmbedderRegistry(
        Map.of(MODEL_NAME, new StubEmbedder(MODEL_NAME)));
    Indexer indexer = new Indexer(indexManager, modelRegistry);
    HiveMetaStoreClient sessionClient = new HiveMetaStoreClient(server.conf());
    MetastoreIndexer metastoreIndexer =
        new MetastoreIndexer(conf, indexManager, indexer, sessionClient, false);
    return new RealMetastoreSearchSession(
        indexManager, indexer, modelRegistry, metastoreIndexer, new SearchOptions(conf));
  }

  public static InMemoryIndexStateClient newSharedRemoteBackup() {
    return new InMemoryIndexStateClient();
  }

  public static String uniqueDbName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace('-', '_').substring(0, 8);
  }

  private static Configuration searchConfiguration(Configuration hmsConf) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(IndexStoreOptions.MEMORY, true);
    conf.set(IndexOptions.INDEX_NAME, "test_index");
    conf.set(InferenceOptions.EMBEDDER_NAME, MODEL_NAME);
    conf.setInt(IndexOptions.BOOTSTRAP_FETCH_THREADS, 2);
    conf.setInt(IndexOptions.BOOTSTRAP_QUEUE_DEPTH, 8);
    conf.setLong(IndexOptions.BOOTSTRAP_PROGRESS_INTERVAL_MS, Long.MAX_VALUE);
    conf.setLong(IndexOptions.EVENT_FAILURE_BACKOFF_MS, 0L);
    conf.setLong(IndexOptions.FLUSH_INTERVAL_MS, 60_000L);
    conf.setLong(IndexOptions.INDEX_SYNC_INTERVAL, 60_000L);
    conf.setInt(SearchOptions.BAYESIAN_SAMPLES, 8);
    conf.setInt(SearchOptions.BAYESIAN_TOKENS_PER_QUERY, 3);
    conf.setLong(SearchOptions.BAYESIAN_SEED, 1L);
    MetastoreConf.setVar(conf, ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
    MetastoreConf.setBoolVar(conf, ConfVars.CAPABILITY_CHECK, false);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, MetastoreConf.getVar(hmsConf, ConfVars.THRIFT_URIS));
    MetastoreConf.setVar(conf, ConfVars.THRIFT_BIND_HOST, MetastoreConf.getVar(hmsConf, ConfVars.THRIFT_BIND_HOST));
    TestLeaderElection election = new TestLeaderElection(true);
    MetastoreCluster.injectElection(conf, ELECTION, election, "mutex");
    return conf;
  }

  public void drainNotifications() throws Exception {
    for (int round = 0; round < 20; round++) {
      if (metastoreIndexer.pollEvents(100) == 0) {
        break;
      }
      Thread.sleep(50);
    }
    metastoreIndexer.flushCheckpoint();
    refreshSearcher();
  }

  public void waitUntilSearchable(String text, int limit) throws Exception {
    MetaStoreTestUtils.waitForAssertion(
        "table indexed for query '" + text + "'",
        () -> {
          try {
            drainNotifications();
            if (searchMatch(text, limit).hits().isEmpty()) {
              throw new AssertionError("not searchable yet");
            }
          } catch (AssertionError e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        30_000,
        300);
  }

  public void waitUntilNotSearchable(String text, int limit) throws Exception {
    MetaStoreTestUtils.waitForAssertion(
        "table removed for query '" + text + "'",
        () -> {
          try {
            drainNotifications();
            if (!searchMatch(text, limit).hits().isEmpty()) {
              throw new AssertionError("still searchable for '" + text + "'");
            }
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        30_000,
        300);
  }

  public void syncBackup() throws IOException {
    metastoreIndexer.syncBackup();
  }

  public void refreshSearcher() throws IOException {
    if (searcherManager == null) {
      searcherManager = new SearcherManager(indexer.writer(), null);
    } else {
      searcherManager.maybeRefresh();
    }
    if (bayesianParameters == null) {
      IndexSearcher searcher = searcherManager.acquire();
      try {
        bayesianParameters = BayesianScoreEstimator.estimate(
            searcher,
            MetastoreTableMapper.FIELD_SEARCH_TEXT,
            searchConfig.getBayesianSamples(),
            searchConfig.getBayesianTokensPerQuery(),
            searchConfig.getBayesianSeed());
      } finally {
        searcherManager.release(searcher);
      }
    }
  }

  public TableSearchResult searchMatch(String text, int limit) throws Exception {
    refreshSearcher();
    try (Searcher searchIO = new Searcher(
        searcherManager, indexManager, modelRegistry, searchConfig, bayesianParameters)) {
      return searchIO.search(new SearchQuery(
          new MatchQuery(text),
          null, null, limit,
          List.of(MetastoreTableMapper.FIELD_TABLE, MetastoreTableMapper.FIELD_COMMENT)));
    }
  }

  public TableSearchResult searchHybrid(String queryText, int limit) throws Exception {
    refreshSearcher();
    try (Searcher searchIO = new Searcher(
        searcherManager, indexManager, modelRegistry, searchConfig, bayesianParameters)) {
      return searchIO.search(SearchQuery.fromQueryBody(
          Map.of("query", queryText),
          SearchQuery.Mode.HYBRID,
          null, null, limit,
          List.of(MetastoreTableMapper.FIELD_TABLE, MetastoreTableMapper.FIELD_COMMENT)));
    }
  }

  public IndexManager indexManager() {
    return indexManager;
  }

  @Override
  public void close() throws Exception {
    if (searcherManager != null) {
      searcherManager.close();
    }
    metastoreIndexer.close();
    indexer.close();
    indexManager.close();
    modelRegistry.close();
  }
}
