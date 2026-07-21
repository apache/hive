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

package org.apache.hive.search.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.inference.EmbedderRegistry;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.metastore.MetastoreIndexSchema;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.apache.hive.search.testutil.StubEmbedder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexerFlushCommit {

  @Test
  public void commitsAfterConfiguredFlushCount() throws Exception {
    Configuration conf = baseConf();
    conf.setInt(IndexConfig.INDEX_RAM_SIZE, 1024);
    conf.setInt(IndexConfig.COMMIT_FLUSHES, 2);

    try (IndexerFixture fixture = IndexerFixture.create(conf)) {
      fixture.indexer.addDocuments(List.of(tableDoc(fixture.mapping(), "hive.sales.orders", "orders")));
      assertFalse(fixture.indexer.flush(1L, false));
      assertEquals(1, fixture.indexer.flushesSinceCommit());

      fixture.indexer.addDocuments(List.of(tableDoc(fixture.mapping(), "hive.sales.customers", "customers")));
      assertTrue(fixture.indexer.flush(2L, false));
      assertEquals(0, fixture.indexer.flushesSinceCommit());
      assertEquals(2L, fixture.indexManager.readLocalManifest().orElseThrow().lastEventId());
    }
  }

  @Test
  public void forceCommitIgnoresFlushThreshold() throws Exception {
    Configuration conf = baseConf();
    conf.setInt(IndexConfig.COMMIT_FLUSHES, 5);

    try (IndexerFixture fixture = IndexerFixture.create(conf)) {
      fixture.indexer.addDocuments(List.of(tableDoc(fixture.mapping(), "hive.sales.orders", "orders")));
      assertTrue(fixture.indexer.flush(9L, true));
      assertEquals(0, fixture.indexer.flushesSinceCommit());
      assertEquals(9L, fixture.indexManager.readLocalManifest().orElseThrow().lastEventId());
    }
  }

  private static Configuration baseConf() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(IndexStoreConfig.MEMORY, true);
    conf.set(IndexConfig.INDEX_NAME, "test_index");
    conf.set(InferenceConfig.EMBEDDER_NAME, InMemorySearchFixture.MODEL_NAME);
    return conf;
  }

  private static TableDocument tableDoc(IndexMapping mapping, String id, String comment) {
    String[] parts = id.split("\\.");
    return MetastoreTableMapper.fromTable(
        InMemorySearchFixture.table(parts[0], parts[1], parts[2], comment), mapping);
  }

  private static final class IndexerFixture implements AutoCloseable {
    private final IndexManager indexManager;
    private final Indexer indexer;
    private final EmbedderRegistry modelRegistry;

    private IndexerFixture(IndexManager indexManager, Indexer indexer, EmbedderRegistry modelRegistry) {
      this.indexManager = indexManager;
      this.indexer = indexer;
      this.modelRegistry = modelRegistry;
    }

    static IndexerFixture create(Configuration conf) throws IOException {
      IndexMapping mapping = MetastoreIndexSchema.defaultHiveTablesMapping(
          "test_index", InMemorySearchFixture.MODEL_NAME, conf);
      IndexManager indexManager = IndexManager.open(mapping, conf);
      EmbedderRegistry registry = new EmbedderRegistry(
          Map.of(InMemorySearchFixture.MODEL_NAME,
              new StubEmbedder(InMemorySearchFixture.MODEL_NAME)));
      Indexer indexer = new Indexer(indexManager, registry);
      indexer.initialize();
      return new IndexerFixture(indexManager, indexer, registry);
    }

    IndexMapping mapping() {
      return indexManager.mapping();
    }

    @Override
    public void close() throws Exception {
      indexer.close();
      indexManager.close();
      modelRegistry.close();
    }
  }
}
