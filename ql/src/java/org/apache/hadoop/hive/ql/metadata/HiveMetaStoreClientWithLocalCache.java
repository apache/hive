/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.HiveMetaStoreClientUtils;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClientProxy;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheKey;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.KeyType;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionNamesWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionSpecsWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionsWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.Supplier;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.TableWatermark;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.thrift.TException;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;

/**
 * This class introduces a caching layer in HS2 for metadata for some selected query APIs. It extends
 * HiveMetaStoreClient, and overrides some of its methods to add this feature.
 * Its design is simple, relying on snapshot information being queried to cache and invalidate the metadata.
 * It helps to reduce the time spent in compilation by using HS2 memory more effectively, and it allows to
 * improve HMS throughput for multi-tenant workloads by reducing the number of calls it needs to serve.
 */
public class HiveMetaStoreClientWithLocalCache extends BaseMetaStoreClientProxy {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientWithLocalCache.class);

  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private static Cache<CacheKey, Object> mscLocalCache = null;
  private static long maxSize;
  private static boolean recordStats;
  private static HashMap<Class<?>, ObjectEstimator> sizeEstimator = null;
  private static String cacheObjName = null;

  public static HiveMetaStoreClientWithLocalCache newClient(Configuration conf, IMetaStoreClient delegate) {
    return new HiveMetaStoreClientWithLocalCache(conf, delegate);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, IMetaStoreClient delegate) {
    super(delegate, conf);
  }

  public static synchronized void init(Configuration conf) {
    // init cache only once
    if (!INITIALIZED.get()) {
      LOG.info("Initializing local cache in HiveMetaStoreClient...");
      maxSize = HiveConf.getSizeVar(conf, HiveConf.ConfVars.MSC_CACHE_MAX_SIZE);
      recordStats = HiveConf.getBoolVar(conf, HiveConf.ConfVars.MSC_CACHE_RECORD_STATS);
      initSizeEstimator();
      initCache();
      LOG.info("Local cache initialized in HiveMetaStoreClient: {}", mscLocalCache);
      INITIALIZED.set(true);
    }
  }

  private static void initSizeEstimator() {
    sizeEstimator = new HashMap<>();
    IncrementalObjectSizeEstimator.createEstimators(CacheKey.class, sizeEstimator);
    for (KeyType e : KeyType.values()) {
      for (Class<?> c : e.keyClasses) {
        IncrementalObjectSizeEstimator.createEstimators(c, sizeEstimator);
      }
      IncrementalObjectSizeEstimator.createEstimators(e.valueClass, sizeEstimator);
    }
  }

  /**
   * Initializes the cache
   */
  private static void initCache() {
    int initSize = 100;
    Weigher<CacheKey, Object> weigher = new Weigher<CacheKey, Object>() {
      @Override
      public @NonNegative int weigh(@NonNull CacheKey key, @NonNull Object val) {
        return MetaStoreClientCacheUtils.getWeight(key, val, sizeEstimator);
      }
    };
    Caffeine<CacheKey, Object> cacheBuilder = Caffeine.newBuilder()
        .initialCapacity(initSize)
        .maximumWeight(maxSize)
        .weigher(weigher)
        .removalListener((key, val, cause) -> {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Caffeine - ({}, {}) was removed ({})", key, val, cause);
          }});
    if (recordStats) {
      cacheBuilder.recordStats();
    }
    mscLocalCache = cacheBuilder.build();
    cacheObjName = cacheBuilder.toString();
  }

  /**
   * Checks if cache is enabled and initialized
   *
   * @return boolean
   */
  private boolean isCacheEnabledAndInitialized() {
    // Do not use the cache if session level query cache is also disabled
    // Both caches can be used only at compilation time because execution may change
    // DB objects (Tables, Partition metadata objects) and cache entries may already invalid
    SessionState sessionState = SessionState.get();
    if (sessionState == null || sessionState.getQueryCache(MetaStoreClientCacheUtils.getQueryId()) == null) {
      return false;
    }

    return INITIALIZED.get();
  }

  @Override
  public Table getTable(GetTableRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // table should be transactional to get responses from the cache
      TableWatermark watermark = new TableWatermark(req.getValidWriteIdList(), req.getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.TABLE, req);
        Table r = (Table) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = delegate.getTable(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getTableInternal, dbName={}, tblName={}, columnStats={}",
              req.getDbName(), req.getTblName(), req.isGetColumnStats());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return delegate.getTable(req);
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) throws TException {
    try {
      GetTableRequest req = new GetTableRequest(dbName, tableName);
      req.setCatName(catName);
      return getTable(req) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public boolean listPartitionsByExpr(PartitionsByExprRequest req, List<Partition> result)
      throws TException {
    Supplier<PartitionsWrapper, TException> supplier = new Supplier<PartitionsWrapper, TException>() {
      @Override
      public PartitionsWrapper get() throws TException {
        List<Partition> parts = new ArrayList<>();
        boolean hasUnknownPart = delegate.listPartitionsByExpr(req, parts);
        return new PartitionsWrapper(parts, hasUnknownPart);
      }
    };
    PartitionsWrapper r = getPartitionsByExprInternal(req, supplier);
    result.addAll(r.partitions);
    return r.hasUnknownPartition;
  }

  private PartitionsWrapper getPartitionsByExprInternal(
      PartitionsByExprRequest req, Supplier<PartitionsWrapper, TException> supplier) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // table should be transactional to get responses from the cache
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), getTable(req.getCatName(), req.getDbName(), req.getTblName()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR, watermark, req);
        PartitionsWrapper r = (PartitionsWrapper) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = supplier.get();
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getPartitionsByExprInternal, dbName={}, tblName={}",
              req.getDbName(), req.getTblName());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }

    return supplier.get();
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(dbName, tableName),
          getTable(catName, dbName, tableName).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_ALL, watermark,
            catName, dbName, tableName, maxParts);
        PartitionNamesWrapper r = (PartitionNamesWrapper) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = new PartitionNamesWrapper(delegate.listPartitionNames(catName, dbName, tableName, maxParts));
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=listPartitionNamesInternal, dbName={}, tblName={}",
              dbName, tableName);
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r.partitionNames;
      }
    }
    return delegate.listPartitionNames(catName, dbName, tableName, maxParts);
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    Supplier<PartitionSpecsWrapper, TException> supplier = new Supplier<PartitionSpecsWrapper, TException>() {
      @Override
      public PartitionSpecsWrapper get() throws TException {
        List<PartitionSpec> parts = new ArrayList<>();
        boolean hasUnknownPart = delegate.listPartitionsSpecByExpr(req, parts);
        return new PartitionSpecsWrapper(parts, hasUnknownPart);
      }
    };
    PartitionSpecsWrapper r = getPartitionsSpecByExprInternal(req, supplier);
    result.addAll(r.partitionSpecs);
    return r.hasUnknownPartition;
  }

  private PartitionSpecsWrapper getPartitionsSpecByExprInternal(
      PartitionsByExprRequest req, Supplier<PartitionSpecsWrapper, TException> supplier) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // table should be transactional to get responses from the cache
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), getTable(req.getCatName(), req.getDbName(), req.getTblName()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_SPEC_BY_EXPR, watermark, req);
        PartitionSpecsWrapper r = (PartitionSpecsWrapper) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = supplier.get();
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getPartitionsSpecByExprInternal, dbName={}, tblName={}",
              req.getDbName(), req.getTblName());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }

    return supplier.get();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine, String validWriteIdList) throws TException {
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(dbName, tableName),
          getTable(catName, dbName, tableName).getId());
      if (watermark.isValid()) {
        CacheWrapper cache = new CacheWrapper(mscLocalCache);
        // 1) Retrieve from the cache those ids present, gather the rest
        Pair<List<ColumnStatisticsObj>, List<String>> p =
            MetaStoreClientCacheUtils.getTableColumnStatisticsCache(cache, catName, dbName, tableName,
                colNames, engine, validWriteIdList, watermark);
        List<String> colStatsMissing = p.getRight();
        List<ColumnStatisticsObj> colStats = p.getLeft();
        // 2) If they were all present in the cache, return
        if (colStatsMissing.isEmpty()) {
          return colStats;
        }
        // 3) If they were not, gather the remaining
        List<ColumnStatisticsObj> newColStats = delegate.getTableColumnStatistics(catName, dbName, tableName,
            colStatsMissing, engine, validWriteIdList);
        // 4) Populate the cache
        MetaStoreClientCacheUtils.loadTableColumnStatisticsCache(cache, newColStats, catName, dbName,
            tableName, engine, validWriteIdList, watermark);
        // 5) Sort result (in case there is any assumption) and return
        List<ColumnStatisticsObj> result =
            MetaStoreClientCacheUtils.computeTableColumnStatisticsFinal(colNames, colStats, newColStats);

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return result;
      }
    }

    return delegate.getTableColumnStatistics(catName, dbName, tableName, colNames, engine, validWriteIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine) throws TException {
    return getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine,
        getValidWriteIdList(dbName, tblName));
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList) throws TException {
    if (isCacheEnabledAndInitialized()) {
      PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
      req.setEngine(engine);
      req.setCatName(catName);
      req.setValidWriteIdList(writeIdList);
      TableWatermark watermark = new TableWatermark(writeIdList, getTable(catName, dbName, tblName).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.AGGR_COL_STATS, watermark, req);
        AggrStats r = (AggrStats) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getAggrStatsForInternal, dbName={}, tblName={}, partNames={}",
              dbName, tblName, partNames);
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }

    return delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req)
      throws TException {
    if (isCacheEnabledAndInitialized()) {
      String dbName = parseDbName(req.getDb_name(), conf)[1];
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), getTable(dbName, req.getTbl_name()).getId());
      if (watermark.isValid()) {
        CacheWrapper cache = new CacheWrapper(mscLocalCache);
        // 1) Retrieve from the cache those ids present, gather the rest
        Pair<List<Partition>, List<String>> p =
            MetaStoreClientCacheUtils.getPartitionsByNamesCache(cache, req, watermark);
        List<String> partitionsMissing = p.getRight();
        List<Partition> partitions = p.getLeft();
        // 2) If they were all present in the cache, return
        if (partitionsMissing.isEmpty()) {
          return new GetPartitionsByNamesResult(partitions);
        }
        // 3) If they were not, gather the remaining
        GetPartitionsByNamesRequest newRqst = new GetPartitionsByNamesRequest(req);
        newRqst.setNames(partitionsMissing);
        GetPartitionsByNamesResult r = delegate.getPartitionsByNames(newRqst);
        // 4) Populate the cache
        List<Partition> newPartitions =
            MetaStoreClientCacheUtils.loadPartitionsByNamesCache(cache, r, req, watermark);
        // 5) Sort result (in case there is any assumption) and return
        GetPartitionsByNamesResult result =
            MetaStoreClientCacheUtils.computePartitionsByNamesFinal(req, partitions, newPartitions);

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return result;
      }
    }

    return delegate.getPartitionsByNames(req);
  }

  // cf. SessionHiveMetaStoreClient.getValidWriteIdList
  private String getValidWriteIdList(String dbName, String tblName) {
    try {
      final String validTxnsList = Hive.get().getConf().get(ValidTxnList.VALID_TXNS_KEY);
      if (validTxnsList == null) {
        return HiveMetaStoreClientUtils.getValidWriteIdList(dbName, tblName, conf);
      }
      if (!AcidUtils.isTransactionalTable(getTable(dbName, tblName))) {
        return null;
      }
      final String validWriteIds = Hive.get().getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
      final String fullTableName = TableName.getDbTable(dbName, tblName);

      ValidTxnWriteIdList validTxnWriteIdList = (validWriteIds != null) ?
          ValidTxnWriteIdList.fromValue(validWriteIds) :
          SessionState.get().getTxnMgr().getValidWriteIds(ImmutableList.of(fullTableName), validTxnsList);

      ValidWriteIdList writeIdList = validTxnWriteIdList.getTableValidWriteIdList(fullTableName);
      return (writeIdList != null) ? writeIdList.toString() : null;
    } catch (Exception e) {
      throw new RuntimeException("Exception getting valid write id list", e);
    }
  }
}
