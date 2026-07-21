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
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.index.store.LocalStateClient;
import org.apache.hive.search.inference.EmbedderRegistry;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.apache.hive.search.testutil.InMemoryIndexStateClient;
import org.apache.hive.search.testutil.MetastoreBootstrapMocks;
import org.apache.hive.search.testutil.StubEmbedder;
import org.apache.hive.search.testutil.TestLeaderElection;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Category(MetastoreUnitTest.class)
public class TestMetastoreIndexer {

  private static final String ELECTION = "metastore-indexer-test";

  @Test
  public void leaderBootstrapsEmptyIndex() throws Exception {
    Configuration conf = indexerConf(true);
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    MetastoreBootstrapMocks.stubCurrentNotificationId(client, 500L);
    MetastoreBootstrapMocks.stubCatchUp(client);
    MetastoreBootstrapMocks.stubBootstrapCatalog(client, orders, customers);

    try (IndexerFixture fixture = IndexerFixture.create(conf, null)) {
      try (MetastoreIndexer metastoreIndexer =
          new MetastoreIndexer(conf, fixture.indexManager, fixture.indexer, client)) {
        assertEquals(500L, fixture.indexManager.readLocalManifest().orElseThrow().lastEventId());
        assertEquals(2, fixture.indexer.writer().getDocStats().numDocs);
      }
    }
  }

  @Test
  public void reusesValidLocalIndexWithoutRebuild() throws Exception {
    Configuration conf = indexerConf(true);
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    MetastoreBootstrapMocks.stubCatchUp(client);

    try (IndexerFixture fixture = IndexerFixture.create(conf, null)) {
      fixture.indexer.initialize();
      List<TableDocument> docs = List.of(
          MetastoreTableMapper.fromTable(orders, fixture.indexManager.mapping()));
      fixture.indexer.addDocuments(docs);
      fixture.indexer.flush(42L, true);
      fixture.indexer.close();

      fixture.indexer = new Indexer(fixture.indexManager, fixture.modelRegistry);
      try (MetastoreIndexer metastoreIndexer =
          new MetastoreIndexer(conf, fixture.indexManager, fixture.indexer, client)) {
        verify(client, never()).getAllDatabases();
        assertEquals(42L, fixture.indexManager.readLocalManifest().orElseThrow().lastEventId());
        assertEquals(1, fixture.indexer.writer().getDocStats().numDocs);
      }
    }
  }

  @Test
  public void followerRestoresFromRemoteBackup() throws Exception {
    Configuration conf = indexerConf(false);
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");
    InMemoryIndexStateClient remote = new InMemoryIndexStateClient();

    IndexMapping mapping = MetastoreIndexSchema.defaultHiveTablesMapping(
        "test_index", InMemorySearchFixture.MODEL_NAME, conf);
    EmbedderRegistry registry = new EmbedderRegistry(
        Map.of(InMemorySearchFixture.MODEL_NAME, new StubEmbedder(InMemorySearchFixture.MODEL_NAME)));

    ByteBuffersDirectory leaderDir = new ByteBuffersDirectory();
    IndexManager leaderManager = new IndexManager(
        mapping, leaderDir, new LocalStateClient(leaderDir, "test_index"), remote);
    Indexer leaderIndexer = new Indexer(leaderManager, registry);
    leaderIndexer.initialize();
    leaderIndexer.addDocuments(List.of(
        MetastoreTableMapper.fromTable(orders, mapping),
        MetastoreTableMapper.fromTable(customers, mapping)));
    leaderIndexer.flush(200L, true);
    leaderIndexer.syncBackup();
    leaderIndexer.close();
    leaderManager.close();

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    MetastoreBootstrapMocks.stubCatchUp(client);

    try (IndexerFixture fixture = IndexerFixture.create(conf, remote)) {
      try (MetastoreIndexer metastoreIndexer =
          new MetastoreIndexer(conf, fixture.indexManager, fixture.indexer, client)) {
        verify(client, never()).getAllDatabases();
        assertEquals(200L, fixture.indexManager.readLocalManifest().orElseThrow().lastEventId());
        assertEquals(2, fixture.indexer.writer().getDocStats().numDocs);
      }
    }
  }

  private static Configuration indexerConf(boolean leader) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(IndexStoreConfig.MEMORY, true);
    conf.set(IndexConfig.INDEX_NAME, "test_index");
    conf.set(InferenceConfig.EMBEDDER_NAME, InMemorySearchFixture.MODEL_NAME);
    conf.setInt(IndexConfig.BOOTSTRAP_FETCH_THREADS, 1);
    conf.setInt(IndexConfig.BOOTSTRAP_QUEUE_DEPTH, 4);
    conf.setLong(IndexConfig.BOOTSTRAP_PROGRESS_INTERVAL_MS, Long.MAX_VALUE);
    conf.setLong(IndexConfig.EVENT_FAILURE_BACKOFF_MS, 0L);
    conf.setLong(IndexConfig.FLUSH_INTERVAL_MS, 60_000L);
    conf.setLong(IndexConfig.INDEX_SYNC_INTERVAL, 60_000L);
    conf.setInt(SearchConfig.BAYESIAN_SAMPLES, 5);
    conf.setInt(SearchConfig.BAYESIAN_TOKENS_PER_QUERY, 2);
    conf.setLong(SearchConfig.BAYESIAN_SEED, 1L);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY,
        JSONMessageEncoder.class.getName());
    TestLeaderElection election = new TestLeaderElection(leader);
    MetastoreCluster.injectElection(conf, ELECTION, election, "mutex");
    return conf;
  }

  private static final class IndexerFixture implements AutoCloseable {
    IndexManager indexManager;
    Indexer indexer;
    final EmbedderRegistry modelRegistry;

    private IndexerFixture(
        IndexManager indexManager, Indexer indexer, EmbedderRegistry modelRegistry) {
      this.indexManager = indexManager;
      this.indexer = indexer;
      this.modelRegistry = modelRegistry;
    }

    static IndexerFixture create(Configuration conf, InMemoryIndexStateClient remote)
        throws Exception {
      IndexMapping mapping = MetastoreIndexSchema.defaultHiveTablesMapping(
          "test_index", InMemorySearchFixture.MODEL_NAME, conf);
      EmbedderRegistry registry = new EmbedderRegistry(
          Map.of(InMemorySearchFixture.MODEL_NAME, new StubEmbedder(InMemorySearchFixture.MODEL_NAME)));
      ByteBuffersDirectory directory = new ByteBuffersDirectory();
      LocalStateClient local = new LocalStateClient(directory, "test_index");
      IndexManager indexManager = remote == null
          ? new IndexManager(mapping, directory, local, null)
          : new IndexManager(mapping, directory, local, remote);
      Indexer indexer = new Indexer(indexManager, registry);
      return new IndexerFixture(indexManager, indexer, registry);
    }

    @Override
    public void close() throws Exception {
      indexer.close();
      indexManager.close();
      modelRegistry.close();
    }
  }
}
