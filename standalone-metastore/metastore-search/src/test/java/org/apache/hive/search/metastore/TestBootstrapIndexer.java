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
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.config.IndexStoreOptions;
import org.apache.hive.search.config.InferenceOptions;
import org.apache.hive.search.exception.IndexIOException;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.inference.EmbedderRegistry;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.apache.hive.search.testutil.MetastoreBootstrapMocks;
import org.apache.hive.search.testutil.StubEmbedder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestBootstrapIndexer {

  @Test
  public void estimateRemainingMsUsesWriterThroughput() {
    assertEquals(-1, BootstrapIndexer.estimateRemainingMs(0, 1000, 5000));
    assertEquals(0, BootstrapIndexer.estimateRemainingMs(1000, 1000, 5000));
    assertEquals(10_000, BootstrapIndexer.estimateRemainingMs(500, 1000, 10_000));
  }

  @Test
  public void formatRemainingRendersHumanReadableDurations() {
    assertEquals("unknown", BootstrapIndexer.formatRemaining(-1));
    assertEquals("45s", BootstrapIndexer.formatRemaining(45_000));
    assertEquals("2m 30s", BootstrapIndexer.formatRemaining(150_000));
    assertEquals("1h 5m", BootstrapIndexer.formatRemaining(3_900_000));
  }

  @Test
  public void bootstrapsAllTablesAcrossDatabases() throws Exception {
    Configuration conf = bootstrapConf();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");
    Table parts = InMemorySearchFixture.table("hive", "inventory", "parts", "spare parts");

    try (BootstrapFixture fixture = BootstrapFixture.create(conf)) {
      MetastoreBootstrapMocks.stubBootstrapCatalog(fixture.client(), orders, customers, parts);

      new BootstrapIndexer(conf, fixture.mapping(), fixture.indexer(), fixture.client(), true)
          .run(500L);

      assertEquals(500L, fixture.indexManager().readLocalManifest().orElseThrow().lastEventId());
      assertEquals(3, fixture.indexer().writer().getDocStats().numDocs);
    }
  }

  @Test
  public void bootstrapHonorsBatchSize() throws Exception {
    Configuration conf = bootstrapConf();
    conf.setInt(IndexOptions.BOOTSTRAP_BATCH_SIZE, 1);
    conf.setInt(IndexOptions.BOOTSTRAP_FETCH_THREADS, 1);
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");

    try (BootstrapFixture fixture = BootstrapFixture.create(conf)) {
      MetastoreBootstrapMocks.stubBootstrapCatalog(fixture.client(), orders, customers);

      new BootstrapIndexer(conf, fixture.mapping(), fixture.indexer(), fixture.client(), true)
          .run(10L);

      assertEquals(2, fixture.indexer().writer().getDocStats().numDocs);
    }
  }

  @Test
  public void bootstrapFailsWhenMetastoreFetchFails() throws Exception {
    Configuration conf = bootstrapConf();
    try (BootstrapFixture fixture = BootstrapFixture.create(conf)) {
      IMetaStoreClient client = fixture.client();
      when(client.getAllDatabases()).thenReturn(List.of("sales"));
      when(client.getAllTables("sales")).thenReturn(List.of("orders"));
      when(client.getTableObjectsByName(eq("sales"), anyList()))
          .thenThrow(new RuntimeException("metastore unavailable"));

      IndexIOException error = assertThrows(IndexIOException.class,
          () -> new BootstrapIndexer(conf, fixture.mapping(), fixture.indexer(), client, true)
              .run(1L));
      assertTrue(error.getMessage().contains("Bootstrap indexing failed")
          || error.getMessage().contains("metastore unavailable"));
    }
  }

  private static Configuration bootstrapConf() {
    Configuration conf = new Configuration(false);
    conf.setInt(IndexOptions.BOOTSTRAP_FETCH_THREADS, 1);
    conf.setInt(IndexOptions.BOOTSTRAP_QUEUE_DEPTH, 4);
    conf.setInt(IndexOptions.COMMIT_FLUSHES, 1);
    conf.setLong(IndexOptions.BOOTSTRAP_PROGRESS_INTERVAL_MS, Long.MAX_VALUE);
    return conf;
  }

  private static final class BootstrapFixture implements AutoCloseable {
    private final IndexManager indexManager;
    private final Indexer indexer;
    private final EmbedderRegistry modelRegistry;
    private final IMetaStoreClient client;

    private BootstrapFixture(
        IndexManager indexManager,
        Indexer indexer,
        EmbedderRegistry modelRegistry,
        IMetaStoreClient client) {
      this.indexManager = indexManager;
      this.indexer = indexer;
      this.modelRegistry = modelRegistry;
      this.client = client;
    }

    static BootstrapFixture create(Configuration conf) throws Exception {
      conf = new Configuration(conf);
      conf.setBoolean(IndexStoreOptions.MEMORY, true);
      conf.set(IndexOptions.INDEX_NAME, "test_index");
      conf.set(InferenceOptions.EMBEDDER_NAME,
          InMemorySearchFixture.MODEL_NAME);
      org.apache.hive.search.mapping.IndexMapping mapping =
          MetastoreIndexSchema.defaultHiveTablesMapping(
              "test_index", InMemorySearchFixture.MODEL_NAME, conf);
      IndexManager indexManager = IndexManager.open(mapping, conf);
      EmbedderRegistry registry = new EmbedderRegistry(
          Map.of(InMemorySearchFixture.MODEL_NAME,
              new StubEmbedder(InMemorySearchFixture.MODEL_NAME)));
      Indexer indexer = new Indexer(indexManager, registry);
      indexer.initialize();
      return new BootstrapFixture(indexManager, indexer, registry, mock(IMetaStoreClient.class));
    }

    IndexManager indexManager() {
      return indexManager;
    }

    Indexer indexer() {
      return indexer;
    }

    IndexMapping mapping() {
      return indexManager.mapping();
    }

    IMetaStoreClient client() {
      return client;
    }

    @Override
    public void close() throws Exception {
      indexer.close();
      indexManager.close();
      modelRegistry.close();
    }
  }
}
