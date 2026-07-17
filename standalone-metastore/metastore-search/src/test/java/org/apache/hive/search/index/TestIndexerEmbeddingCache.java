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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.hive.search.inference.EmbeddingCache;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.apache.hive.search.testutil.StubEmbedModel;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexerEmbeddingCache {

  @Test
  public void dedupesIdenticalSearchTextAcrossTables() throws Exception {
    try (IndexerFixture fixture = IndexerFixture.create()) {
      TableDocument salesOrders = document(fixture.mapping(),
          InMemorySearchFixture.table("hive", "sales", "orders", "shared sales text"));
      TableDocument inventoryOrders = document(fixture.mapping(),
          InMemorySearchFixture.table("hive", "inventory", "orders", "shared sales text"));

      fixture.indexer().embedDocuments(List.of(salesOrders));
      int callsAfterFirst = fixture.model().encodeBatchCalls();
      assertTrue(callsAfterFirst >= 1);

      fixture.indexer().embedDocuments(List.of(inventoryOrders));
      assertEquals(callsAfterFirst, fixture.model().encodeBatchCalls());
      assertEquals(1, fixture.indexer().embeddingCache().hits());
    }
  }

  @Test
  public void skipsInferenceWhenSearchTextUnchangedOnUpdate() throws Exception {
    try (IndexerFixture fixture = IndexerFixture.create()) {
      Table table = InMemorySearchFixture.table("hive", "sales", "orders", "daily sales orders");
      TableDocument original = document(fixture.mapping(), table);
      fixture.indexer().embedDocuments(List.of(original));
      int callsAfterInitial = fixture.model().encodeBatchCalls();

      TableDocument updated = document(fixture.mapping(), table);
      fixture.indexer().embedDocuments(List.of(updated));
      assertEquals(callsAfterInitial, fixture.model().encodeBatchCalls());
      assertEquals(1, fixture.indexer().embeddingCache().hits());
    }
  }

  private static TableDocument document(IndexMapping mapping, Table table) {
    return MetastoreTableMapper.fromTable(table, mapping);
  }

  private static final class IndexerFixture implements AutoCloseable {
    private final IndexManager indexManager;
    private final Indexer indexer;
    private final IndexMapping mapping;
    private final StubEmbedModel model;

    private IndexerFixture(
        IndexManager indexManager, Indexer indexer, IndexMapping mapping, StubEmbedModel model) {
      this.indexManager = indexManager;
      this.indexer = indexer;
      this.mapping = mapping;
      this.model = model;
    }

    static IndexerFixture create() throws Exception {
      Configuration conf = new Configuration(false);
      conf.setBoolean(IndexStoreConfig.MEMORY, true);
      conf.set(IndexConfig.INDEX_NAME, "test_index");
      conf.set(InferenceConfig.MODEL_NAME, InMemorySearchFixture.MODEL_NAME);
      conf.setBoolean(InferenceConfig.EMBEDDING_CACHE_ENABLED, true);
      conf.setInt(InferenceConfig.EMBEDDING_CACHE_MAX_ENTRIES, 1000);

      IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping(
          "test_index", InMemorySearchFixture.MODEL_NAME, conf);
      StubEmbedModel model = new StubEmbedModel(InMemorySearchFixture.MODEL_NAME);
      EmbedModelRegistry registry =
          new EmbedModelRegistry(Map.of(model.name(), model), EmbeddingCache.create(conf));
      IndexManager indexManager = IndexManager.open(mapping, conf);
      Indexer indexer = new Indexer(indexManager, registry);
      indexer.initialize();
      return new IndexerFixture(indexManager, indexer, mapping, model);
    }

    IndexMapping mapping() {
      return mapping;
    }

    Indexer indexer() {
      return indexer;
    }

    StubEmbedModel model() {
      return model;
    }

    @Override
    public void close() throws Exception {
      indexer.close();
      indexManager.close();
    }
  }
}
