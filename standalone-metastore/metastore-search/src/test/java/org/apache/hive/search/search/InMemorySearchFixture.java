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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.IndexStoreConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.testutil.IndexMutationApplier;
import org.apache.hive.search.testutil.StubEmbedModel;
import org.apache.lucene.search.BayesianScoreEstimator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** In-memory Lucene index wired with a stub embedding model. */
public final class InMemorySearchFixture implements AutoCloseable {
  public static final String MODEL_NAME = "stub-model";

  private final IndexManager indexManager;
  private final Indexer indexer;
  private final EmbedModelRegistry modelRegistry;
  private final SearchConfig searchConfig;
  private final IndexMutationApplier mutations;
  private SearcherManager searcherManager;
  private BayesianScoreEstimator.Parameters bayesianParameters;

  private InMemorySearchFixture(
      IndexManager indexManager,
      Indexer indexer,
      EmbedModelRegistry modelRegistry,
      SearchConfig searchConfig) {
    this.indexManager = indexManager;
    this.indexer = indexer;
    this.modelRegistry = modelRegistry;
    this.searchConfig = searchConfig;
    this.mutations = new IndexMutationApplier(indexManager, indexer);
  }

  public static InMemorySearchFixture create() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(IndexStoreConfig.MEMORY, true);
    conf.set(IndexConfig.INDEX_NAME, "test_index");
    conf.set(InferenceConfig.MODEL_NAME, MODEL_NAME);
    conf.setInt(SearchConfig.BAYESIAN_SAMPLES, 5);
    conf.setInt(SearchConfig.BAYESIAN_TOKENS_PER_QUERY, 2);
    conf.setLong(SearchConfig.BAYESIAN_SEED, 1L);

    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping(
        "test_index", MODEL_NAME, conf);
    IndexManager indexManager = IndexManager.open(mapping, conf);
    EmbedModelRegistry registry =
        new EmbedModelRegistry(Map.of(MODEL_NAME, new StubEmbedModel(MODEL_NAME)));
    Indexer indexer = new Indexer(indexManager, registry);
    indexer.initialize();
    return new InMemorySearchFixture(indexManager, indexer, registry, new SearchConfig(conf));
  }

  public IndexMutationApplier mutations() {
    return mutations;
  }

  public void commit(long eventId) throws IOException {
    indexer.flush(eventId, true);
    refreshSearcher();
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

  public List<TableSearchHit> searchMatch(String text, int limit) throws Exception {
    return search(new SearchArgs.Match(text), limit);
  }

  public List<TableSearchHit> search(SearchArgs args, int limit) throws Exception {
    if (searcherManager == null || bayesianParameters == null) {
      throw new IllegalStateException("call commit() before searching");
    }
    try (SearchInternal searchIO = new SearchInternal(
        searcherManager, indexManager, modelRegistry, searchConfig, bayesianParameters)) {
      TableSearchResult result = searchIO.search(new SearchQuery(
          args,
          null, null, limit,
          List.of(MetastoreTableMapper.FIELD_TABLE, MetastoreTableMapper.FIELD_COMMENT)));
      return result.hits();
    }
  }

  public static Table table(String catalog, String db, String name, String comment) {
    Table table = new Table();
    table.setCatName(catalog);
    table.setDbName(db);
    table.setTableName(name);
    table.setOwner("owner");
    table.setTableType("MANAGED_TABLE");
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("hdfs://warehouse/" + db + "/" + name);
    Map<String, String> params = new HashMap<>();
    params.put("comment", comment);
    table.setParameters(params);
    return table;
  }

  public IndexManager indexManager() {
    return indexManager;
  }

  public Indexer indexer() {
    return indexer;
  }

  public SearcherManager searcherManager() {
    return searcherManager;
  }

  @Override
  public void close() throws Exception {
    if (searcherManager != null) {
      searcherManager.close();
    }
    indexer.close();
    indexManager.close();
    modelRegistry.close();
  }
}
