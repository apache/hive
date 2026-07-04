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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.search.exception.IndexNotReadyException;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.config.InferenceConfig;
import org.apache.hive.search.config.SearchConfig;
import org.apache.hive.search.inference.EmbedModelRegistry;
import org.apache.hive.search.metastore.MetastoreIndexer;
import org.apache.hive.search.metastore.MetastoreSchemas;
import org.apache.hive.search.metastore.MetastoreTableMapper;
import org.apache.hive.search.search.SearchInternal;
import org.apache.lucene.search.BayesianScoreEstimator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Colocated indexer and searcher */
public final class IndexSession implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(IndexSession.class);
  private final Configuration configuration;
  private final IndexManager indexManager;
  private final EmbedModelRegistry modelRegistry;
  private final SearchConfig searchConfig;

  private BayesianScoreEstimator.Parameters parameters;
  private MetastoreIndexer metastoreIndexer;
  private SearcherManager searcherManager;
  private Indexer indexer;

  private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

  public IndexSession(Configuration configuration)
      throws InitializeException, IOException {
    this.configuration = configuration;
    IndexConfig indexConfig = new IndexConfig(configuration);
    this.searchConfig = new SearchConfig(configuration);
    InferenceConfig inferenceConfig = new InferenceConfig(configuration);
    this.indexManager = IndexManager.open(
        MetastoreSchemas.defaultHiveTablesMapping(indexConfig.indexName(),
            inferenceConfig.modelName(), configuration), configuration);
    this.modelRegistry = EmbedModelRegistry.create(configuration);
  }

  public void maybeRefreshIndex() {
    try {
      searcherManager.maybeRefresh();
    } catch (IOException e) {
      LOG.info("Error while refreshing the index", e);
    }
  }

  public void initialize() throws Exception {
    indexer = new Indexer(indexManager, modelRegistry);
    metastoreIndexer = new MetastoreIndexer(configuration, indexManager, indexer);
    metastoreIndexer.start();
    searcherManager = new SearcherManager(indexer.writer(), null);
    service.scheduleAtFixedRate(this::maybeRefreshIndex, 0,
        searchConfig.getRefreshInterval().toSeconds(), TimeUnit.SECONDS);
    IndexSearcher searcher = searcherManager.acquire();
    try {
      parameters = BayesianScoreEstimator.estimate(searcher,
          MetastoreTableMapper.FIELD_SEARCH_TEXT,
          searchConfig.getBayesianSamples(),
          searchConfig.getBayesianTokensPerQuery(),
          searchConfig.getBayesianSeed());
      LOG.info("BayesianScore alpha={} beta={} baseRate={}",
          parameters.alpha(), parameters.beta(), parameters.baseRate());
    } finally {
      searcherManager.release(searcher);
    }
  }

  public SearchInternal getSearcher() throws IOException {
    if (parameters == null) {
      throw new IndexNotReadyException("Index session is not ready for search requests");
    }
    indexManager.checkIndexState();
    return new SearchInternal(searcherManager, indexManager,
        modelRegistry, searchConfig, parameters);
  }

  @Override
  public void close() throws Exception {
    service.shutdown();
    if (searcherManager != null) {
      searcherManager.close();
    }
    if (metastoreIndexer != null) {
      metastoreIndexer.close();
    }
    if (indexer != null) {
      indexer.close();
    }
    indexManager.close();
    modelRegistry.close();
  }
}
