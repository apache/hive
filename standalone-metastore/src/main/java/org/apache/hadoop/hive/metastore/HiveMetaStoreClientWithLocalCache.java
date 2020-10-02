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
package org.apache.hadoop.hive.metastore;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ObjectDictionary;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;

/**
 * This class introduces a caching layer in HS2 for metadata for some selected query APIs. It extends
 * HiveMetaStoreClient, and overrides some of its methods to add this feature.
 * Its design is simple, relying on snapshot information being queried to cache and invalidate the metadata.
 * It helps to reduce the time spent in compilation by using HS2 memory more effectively, and it allows to
 * improve HMS throughput for multi-tenant workloads by reducing the number of calls it needs to serve.
 */
public class HiveMetaStoreClientWithLocalCache extends HiveMetaStoreClient implements IMetaStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientWithLocalCache.class);

  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private static Cache<CacheKey, Object> mscLocalCache = null;
  private static long maxSize;
  private static boolean recordStats;
  private static HashMap<Class<?>, ObjectEstimator> sizeEstimator = null;
  private static String cacheObjName = null;

  public static synchronized void init(Configuration conf) {
    // init cache only once
    if (!INITIALIZED.get()) {
      LOG.info("Initializing local cache in HiveMetaStoreClient...");
      maxSize = MetastoreConf.getSizeVar(conf, MetastoreConf.ConfVars.MSC_CACHE_MAX_SIZE);
      recordStats = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.MSC_CACHE_RECORD_STATS);
      initSizeEstimator();
      initCache();
      LOG.info("Local cache initialized in HiveMetaStoreClient: {}", mscLocalCache);
      INITIALIZED.set(true);
    }
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf) throws MetaException {
    this(conf, null, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
    this(conf, hookLoader, true);
  }

  public HiveMetaStoreClientWithLocalCache(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
    super(conf, hookLoader, allowEmbedded);
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
   * KeyType is used to differentiate the request types. More types can be added in future.
   * We added the unique classes that are part of the key for each request as well as the
   * class of the value stored in the cache: At initialization time, they will be registered
   * within the size estimator, which will be used to estimate the size of the objects
   * within the cache.
   */
  public enum KeyType {
    // String <-- getConfigValueInternal(String name, String defaultValue)
    CONFIG_VALUE(String.class),
    // Database <-- getDatabaseInternal(GetDatabaseRequest request)
    DATABASE(Database.class, GetDatabaseRequest.class),
    // GetTableResult <-- getTableInternal(GetTableRequest req)
    TABLE(GetTableResult.class, GetTableRequest.class),
    // PrimaryKeysResponse <-- getPrimaryKeysInternal(PrimaryKeysRequest req)
    PRIMARY_KEYS(PrimaryKeysResponse.class, PrimaryKeysRequest.class),
    // ForeignKeysResponse <-- getForeignKeysInternal(ForeignKeysRequest req)
    FOREIGN_KEYS(ForeignKeysResponse.class, ForeignKeysRequest.class),
    // UniqueConstraintsResponse <-- getUniqueConstraintsInternal(UniqueConstraintsRequest req)
    UNIQUE_CONSTRAINTS(UniqueConstraintsResponse.class, UniqueConstraintsRequest.class),
    // NotNullConstraintsResponse <-- getNotNullConstraintsInternal(NotNullConstraintsRequest req)
    NOT_NULL_CONSTRAINTS(NotNullConstraintsResponse.class, NotNullConstraintsRequest.class),
    // TableStatsResult <-- getTableColumnStatisticsInternal(TableStatsRequest rqst)
    // Stored individually as:
    // ColumnStatisticsObj <-- String dbName, String tblName, List<string> colNames,
    //      String catName, String validWriteIdList, String engine, long id, (TableWatermark tw ?)
    TABLE_COLUMN_STATS(ColumnStatisticsObj.class, String.class, long.class, TableWatermark.class),
    // AggrStats <-- getAggrStatsForInternal(PartitionsStatsRequest req), (TableWatermark tw ?)
    AGGR_COL_STATS(AggrStats.class, PartitionsStatsRequest.class, TableWatermark.class),
    // PartitionsByExprResult <-- getPartitionsByExprInternal(PartitionsByExprRequest req), (TableWatermark tw ?)
    PARTITIONS_BY_EXPR(PartitionsByExprResult.class, PartitionsByExprRequest.class, TableWatermark.class),
    // PartitionNamesWrapper <-- listPartitionNamesInternal(String catName, String dbName, String tableName,
    //       int maxParts), (TableWatermark tw ?)
    LIST_PARTITIONS_ALL(PartitionNamesWrapper.class, String.class, int.class, TableWatermark.class),
    // List<String> <-- listPartitionNamesInternal(String catName, String dbName, String tableName,
    //       List<String> partVals, int maxParts)
    LIST_PARTITIONS(String.class, int.class),
    // GetPartitionNamesPsResponse <-- listPartitionNamesRequestInternal(GetPartitionNamesPsRequest req)
    LIST_PARTITIONS_REQ(GetPartitionNamesPsResponse.class, GetPartitionNamesPsRequest.class),
    // List<Partition> <- listPartitionsWithAuthInfoInternal(String catName, String dbName, String tableName,
    //      int maxParts, String userName, List<String> groupNames)
    LIST_PARTITIONS_AUTH_INFO_ALL(Partition.class, String.class, int.class),
    // List<Partition> <- listPartitionsWithAuthInfoInternal(String catName, String dbName, String tableName,
    //      List<String> partialPvals, int maxParts, String userName, List<String> groupNames)
    LIST_PARTITIONS_AUTH_INFO(Partition.class, String.class, int.class),
    // GetPartitionsPsWithAuthResponse <- listPartitionsWithAuthInfoRequestInternal(GetPartitionsPsWithAuthRequest req)
    LIST_PARTITIONS_AUTH_INFO_REQ(GetPartitionsPsWithAuthResponse.class, GetPartitionsPsWithAuthRequest.class),
    // GetPartitionsByNamesResult <-- getPartitionsByNamesInternal(GetPartitionsByNamesRequest gpbnr)
    // Stored individually as:
    // Pair<Partition, ObjectDictionary> <-- String db_name, String tbl_name, List<String> partValues, boolean get_col_stats,
    //      List<String> processorCapabilities, String processorIdentifier, String engine,
    //      String validWriteIdList, (TableWatermark tw ?)
    PARTITIONS_BY_NAMES(Pair.class, Partition.class, ObjectDictionary.class, String.class, boolean.class, TableWatermark.class),
    // GetValidWriteIdsResponse <-- getValidWriteIdsInternal(GetValidWriteIdsRequest rqst)
    // Stored individually as:
    // TableValidWriteIds <-- String fullTableName, String validTxnList, long writeId
    VALID_WRITE_IDS(TableValidWriteIds.class, String.class, long.class);

    private final List<Class<?>> keyClasses;
    private final Class<?> valueClass;

    KeyType(Class<?> valueClass, Class<?>... keyClasses) {
      this.keyClasses = Collections.unmodifiableList(Arrays.asList(keyClasses));
      this.valueClass = valueClass;
    }
  }

  /**
   * CacheKey objects are used as key for the cache.
   */
  public static class CacheKey{
    KeyType IDENTIFIER;
    List<Object> obj;

    public CacheKey(KeyType IDENTIFIER, Object... objs) {
      this.IDENTIFIER = IDENTIFIER;
      this.obj = Collections.unmodifiableList(Arrays.asList(objs));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return IDENTIFIER == cacheKey.IDENTIFIER &&
          Objects.equals(obj, cacheKey.obj);
    }

    @Override
    public int hashCode() {
      return Objects.hash(IDENTIFIER, obj);
    }

    @Override
    public String toString() {
      return "CacheKey {" + IDENTIFIER.name() + " @@ " + obj.toString() + "}";
    }
  }

  private static int getWeight(CacheKey key, Object val) {
    ObjectEstimator keySizeEstimator = sizeEstimator.get(key.getClass());
    ObjectEstimator valSizeEstimator = sizeEstimator.get(key.IDENTIFIER.valueClass);
    int keySize = keySizeEstimator.estimate(key, sizeEstimator);
    int valSize = valSizeEstimator.estimate(val, sizeEstimator);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache entry weight - key: {}, value: {}, total: {}", keySize, valSize, keySize + valSize);
    }
    return keySize + valSize;
  }

  /**
   * Initializes the cache
   */
  private static void initCache() {
    int initSize = 100;
    Caffeine<CacheKey, Object> cacheBuilder = Caffeine.newBuilder()
        .initialCapacity(initSize)
        .maximumWeight(maxSize)
        .weigher(HiveMetaStoreClientWithLocalCache::getWeight)
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

  @Override
  protected GetTableResult getTableInternal(GetTableRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // table should be transactional to get responses from the cache
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), req.getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.TABLE, req);
        GetTableResult r = (GetTableResult) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getTableInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getTableInternal, dbName={}, tblName={}, columnStats={}, fileMetadata={}",
              req.getDbName(), req.getTblName(), req.isGetColumnStats(), req.isGetFileMetadata());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return super.getTableInternal(req);
  }

  @Override
  protected PartitionsByExprResult getPartitionsByExprInternal(PartitionsByExprRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // table should be transactional to get responses from the cache
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), getTable(req.getDbName(), req.getTblName()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR, watermark, req);
        PartitionsByExprResult r = (PartitionsByExprResult) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getPartitionsByExprInternal(req);
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

    return super.getPartitionsByExprInternal(req);
  }

  @Override
  protected List<String> listPartitionNamesInternal(String catName, String dbName, String tableName,
      int maxParts) throws TException {
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(dbName, tableName),
          getTable(dbName, tableName).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_ALL, watermark,
            catName, dbName, tableName, maxParts);
        PartitionNamesWrapper r = (PartitionNamesWrapper) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = new PartitionNamesWrapper(
              super.listPartitionNamesInternal(catName, dbName, tableName, maxParts));
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
    return super.listPartitionNamesInternal(catName, dbName, tableName, maxParts);
  }

  /* This class is needed so the size estimator can work properly. */
  private static class PartitionNamesWrapper {
    private final List<String> partitionNames;

    private PartitionNamesWrapper(List<String> partitionNames) {
      this.partitionNames = partitionNames;
    }
  }

  @Override
  protected PrimaryKeysResponse getPrimaryKeysInternal(PrimaryKeysRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // TODO: There is no consistency guarantees around constraints right now since
      // changing constraints does not change the snapshot nor the table id (CDPD-17940).
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(req.getDb_name(), req.getTbl_name()),
          getTable(req.getDb_name(), req.getTbl_name()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.PRIMARY_KEYS, watermark, req);
        PrimaryKeysResponse r = (PrimaryKeysResponse) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getPrimaryKeysInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getPrimaryKeysInternal, dbName={}, tblName={}",
              req.getDb_name(), req.getTbl_name());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return super.getPrimaryKeysInternal(req);
  }

  @Override
  protected ForeignKeysResponse getForeignKeysInternal(ForeignKeysRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // TODO: There is no consistency guarantees around constraints right now since
      // changing constraints does not change the snapshot nor the table id (CDPD-17940).
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(req.getForeign_db_name(), req.getForeign_tbl_name()),
          getTable(req.getForeign_db_name(), req.getForeign_tbl_name()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.FOREIGN_KEYS, watermark, req);
        ForeignKeysResponse r = (ForeignKeysResponse) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getForeignKeysInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getForeignKeysInternal, dbName={}, tblName={}",
              req.getForeign_db_name(), req.getForeign_tbl_name());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return super.getForeignKeysInternal(req);
  }

  @Override
  protected UniqueConstraintsResponse getUniqueConstraintsInternal(UniqueConstraintsRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // TODO: There is no consistency guarantees around constraints right now since
      // changing constraints does not change the snapshot nor the table id (CDPD-17940).
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(req.getDb_name(), req.getTbl_name()),
          getTable(req.getDb_name(), req.getTbl_name()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.UNIQUE_CONSTRAINTS, watermark, req);
        UniqueConstraintsResponse r = (UniqueConstraintsResponse) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getUniqueConstraintsInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getUniqueConstraintsInternal, dbName={}, tblName={}",
              req.getDb_name(), req.getTbl_name());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return super.getUniqueConstraintsInternal(req);
  }

  @Override
  protected NotNullConstraintsResponse getNotNullConstraintsInternal(NotNullConstraintsRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      // TODO: There is no consistency guarantees around constraints right now since
      // changing constraints does not change the snapshot nor the table id (CDPD-17940).
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(req.getDb_name(), req.getTbl_name()),
          getTable(req.getDb_name(), req.getTbl_name()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.NOT_NULL_CONSTRAINTS, watermark, req);
        NotNullConstraintsResponse r = (NotNullConstraintsResponse) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getNotNullConstraintsInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getNotNullConstraintsInternal, dbName={}, tblName={}",
              req.getDb_name(), req.getTbl_name());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return r;
      }
    }
    return super.getNotNullConstraintsInternal(req);
  }

  @Override
  protected TableStatsResult getTableColumnStatisticsInternal(TableStatsRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          getValidWriteIdList(req.getDbName(), req.getTblName()),
          getTable(req.getDbName(), req.getTblName()).getId());
      if (watermark.isValid()) {
        CacheWrapper cache = new CacheWrapper(mscLocalCache);
        // 1) Retrieve from the cache those ids present, gather the rest
        Pair<List<ColumnStatisticsObj>, List<String>> p = getTableColumnStatisticsCache(
            cache, req, watermark);
        List<String> colStatsMissing = p.getRight();
        List<ColumnStatisticsObj> colStats = p.getLeft();
        // 2) If they were all present in the cache, return
        if (colStatsMissing.isEmpty()) {
          return new TableStatsResult(colStats);
        }
        // 3) If they were not, gather the remaining
        TableStatsRequest newRqst = new TableStatsRequest(req);
        newRqst.setColNames(colStatsMissing);
        TableStatsResult r = super.getTableColumnStatisticsInternal(newRqst);
        // 4) Populate the cache
        List<ColumnStatisticsObj> newColStats = loadTableColumnStatisticsCache(
            cache, r, req, watermark);
        // 5) Sort result (in case there is any assumption) and return
        TableStatsResult result = computeTableColumnStatisticsFinal(req, colStats, newColStats);

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }
        return result;
      }
    }
    return super.getTableColumnStatisticsInternal(req);
  }

  @Override
  protected AggrStats getAggrStatsForInternal(PartitionsStatsRequest req) throws TException {
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          req.getValidWriteIdList(), getTable(req.getDbName(), req.getTblName()).getId());
      if (watermark.isValid()) {
        CacheKey cacheKey = new CacheKey(KeyType.AGGR_COL_STATS, watermark, req);
        AggrStats r = (AggrStats) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = super.getAggrStatsForInternal(req);
          mscLocalCache.put(cacheKey, r);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getAggrStatsForInternal, dbName={}, tblName={}, partNames={}",
              req.getDbName(), req.getTblName(), req.getPartNames());
        }

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }
        return r;
      }
    }

    return super.getAggrStatsForInternal(req);
  }

  @Override
  protected GetPartitionsByNamesResult getPartitionsByNamesInternal(GetPartitionsByNamesRequest rqst) throws TException {
    if (isCacheEnabledAndInitialized()) {
      String dbName = parseDbName(rqst.getDb_name(), conf)[1];
      TableWatermark watermark = new TableWatermark(
          rqst.getValidWriteIdList(), getTable(dbName, rqst.getTbl_name()).getId());
      if (watermark.isValid()) {
        CacheWrapper cache = new CacheWrapper(mscLocalCache);
        // 1) Retrieve from the cache those ids present, gather the rest
        Pair<Pair<List<Partition>, ObjectDictionary>, List<String>> p = getPartitionsByNamesCache(
            cache, rqst, watermark);
        List<String> partitionsMissing = p.getRight();
        Pair<List<Partition>, ObjectDictionary> partitions = p.getLeft();
        // 2) If they were all present in the cache, return
        if (partitionsMissing.isEmpty()) {
          GetPartitionsByNamesResult result = new GetPartitionsByNamesResult(partitions.getLeft());
          result.setDictionary(partitions.getRight());
          return result;
        }
        // 3) If they were not, gather the remaining
        GetPartitionsByNamesRequest newRqst = new GetPartitionsByNamesRequest(rqst);
        newRqst.setNames(partitionsMissing);
        GetPartitionsByNamesResult r = super.getPartitionsByNamesInternal(newRqst);
        // 4) Populate the cache
        Pair<List<Partition>, ObjectDictionary> newPartitions = loadPartitionsByNamesCache(
            cache, r, rqst, watermark);
        // 5) Sort result (in case there is any assumption) and return
        GetPartitionsByNamesResult result = computePartitionsByNamesFinal(rqst, partitions, newPartitions);

        if (LOG.isDebugEnabled() && recordStats) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

        return result;
      }
    }

    return super.getPartitionsByNamesInternal(rqst);
  }

  /**
   * Checks if cache is enabled and initialized
   *
   * @return boolean
   */
  private boolean isCacheEnabledAndInitialized() {
    return INITIALIZED.get();
  }


  protected final Pair<List<ColumnStatisticsObj>, List<String>> getTableColumnStatisticsCache(CacheI cache,
      TableStatsRequest rqst, TableWatermark watermark) {
    List<String> colStatsMissing = new ArrayList<>();
    List<ColumnStatisticsObj> colStats = new ArrayList<>();
    for (String colName : rqst.getColNames()) {
      CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark,
          rqst.getDbName(), rqst.getTblName(), colName,
          rqst.getCatName(), rqst.getValidWriteIdList(),
          rqst.getEngine(), rqst.getId());
      ColumnStatisticsObj v = (ColumnStatisticsObj) cache.get(cacheKey);
      if (v == null) {
        colStatsMissing.add(colName);
      } else {
        if (watermark == null) {
          LOG.debug(
              "Query level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, colName={}",
              rqst.getDbName(), rqst.getTblName(), colName);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, colName={}",
              rqst.getDbName(), rqst.getTblName(), colName);
        }
        colStats.add(v);
      }
    }
    return Pair.of(colStats, colStatsMissing);
  }

  protected final List<ColumnStatisticsObj> loadTableColumnStatisticsCache(CacheI cache,
      TableStatsResult r, TableStatsRequest rqst, TableWatermark watermark) {
    List<ColumnStatisticsObj> newColStats = new ArrayList<>();
    for (ColumnStatisticsObj colStat : r.getTableStats()) {
      CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark,
          rqst.getDbName(), rqst.getTblName(), colStat.getColName(),
          rqst.getCatName(), rqst.getValidWriteIdList(),
          rqst.getEngine(), rqst.getId());
      cache.put(cacheKey, colStat);
      newColStats.add(colStat);
    }
    return newColStats;
  }

  protected final TableStatsResult computeTableColumnStatisticsFinal(TableStatsRequest rqst,
      List<ColumnStatisticsObj> colStats, List<ColumnStatisticsObj> newColStats) {
    List<ColumnStatisticsObj> result = new ArrayList<>();
    int i = 0, j = 0;
    for (String colName : rqst.getColNames()) {
      if (i >= colStats.size() || j >= newColStats.size()) {
        break;
      }
      if (colStats.get(i).getColName().equals(colName)) {
        result.add(colStats.get(i));
        i++;
      } else if (newColStats.get(j).getColName().equals(colName)) {
        result.add(newColStats.get(j));
        j++;
      }
    }
    while (i < colStats.size()) {
      result.add(colStats.get(i));
      i++;
    }
    while (j < newColStats.size()) {
      result.add(newColStats.get(j));
      j++;
    }
    return new TableStatsResult(result);
  }


  protected final Pair<Pair<List<Partition>, ObjectDictionary>, List<String>> getPartitionsByNamesCache(CacheI cache,
      GetPartitionsByNamesRequest rqst, TableWatermark watermark) throws MetaException {
    List<String> partitionsMissing = new ArrayList<>();
    List<Partition> partitions = new ArrayList<>();
    ObjectDictionary od = null;
    for (String partitionName : rqst.getNames()) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_NAMES, watermark,
          rqst.getDb_name(), rqst.getTbl_name(), Warehouse.getPartValuesFromPartName(partitionName),
          rqst.isGet_col_stats(), rqst.getProcessorCapabilities(), rqst.getProcessorIdentifier(),
          rqst.getEngine(), rqst.getValidWriteIdList(), rqst.isGetFileMetadata());
      Pair<Partition, ObjectDictionary> v = (Pair<Partition, ObjectDictionary>) cache.get(cacheKey);
      if (v == null) {
        partitionsMissing.add(partitionName);
      } else {
        if (watermark == null) {
          LOG.debug(
              "Query level HMS cache: method=getPartitionsByNamesInternal, dbName={}, tblName={}, partitionName={}, fileMetadata={}",
              rqst.getDb_name(), rqst.getTbl_name(), partitionName, rqst.isGetFileMetadata());
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getPartitionsByNamesInternal, dbName={}, tblName={}, partitionName={}, fileMetadata={}",
              rqst.getDb_name(), rqst.getTbl_name(), partitionName, rqst.isGetFileMetadata());
        }
        partitions.add(v.getLeft());
        od = v.getRight();
      }
    }
    return Pair.of(Pair.of(partitions, od), partitionsMissing);
  }

  protected final Pair<List<Partition>, ObjectDictionary> loadPartitionsByNamesCache(CacheI cache,
      GetPartitionsByNamesResult r, GetPartitionsByNamesRequest rqst, TableWatermark watermark) {
    List<Partition> newPartitions = new ArrayList<>();
    ObjectDictionary od = null;
    for (Partition partition : r.getPartitions()) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_NAMES, watermark,
          rqst.getDb_name(), rqst.getTbl_name(), partition.getValues(),
          rqst.isGet_col_stats(), rqst.getProcessorCapabilities(), rqst.getProcessorIdentifier(),
          rqst.getEngine(), rqst.getValidWriteIdList(), rqst.isGetFileMetadata());
      cache.put(cacheKey, Pair.of(partition, r.getDictionary()));
      newPartitions.add(partition);
      od = r.getDictionary();
    }
    return Pair.of(newPartitions, od);
  }

  protected final GetPartitionsByNamesResult computePartitionsByNamesFinal(GetPartitionsByNamesRequest rqst,
      Pair<List<Partition>, ObjectDictionary> partitions, Pair<List<Partition>, ObjectDictionary> newPartitions)
      throws MetaException {
    List<Partition> p1 = partitions.getLeft();
    List<Partition> p2 = newPartitions.getLeft();
    List<Partition> resultPartitions = new ArrayList<>();
    int i = 0, j = 0;
    for (String partitionName : rqst.getNames()) {
      if (i >= p1.size() || j >= p2.size()) {
        break;
      }
      List<String> pv = Warehouse.getPartValuesFromPartName(partitionName);
      Partition pi = p1.get(i);
      Partition pj = p2.get(j);
      if (pi.getValues().equals(pv)) {
        resultPartitions.add(pi);
        i++;
      } else if (pj.getValues().equals(pv)) {
        resultPartitions.add(pj);
        j++;
      }
    }
    while (i < p1.size()) {
      resultPartitions.add(p1.get(i));
      i++;
    }
    while (j < p2.size()) {
      resultPartitions.add(p2.get(j));
      j++;
    }
    GetPartitionsByNamesResult result = new GetPartitionsByNamesResult(resultPartitions);
    if (rqst.isGetFileMetadata()) {
      // TODO: This is just choosing randomly
      if (newPartitions.getRight() != null) {
        result.setDictionary(newPartitions.getRight());
      } else if (partitions.getRight() != null) {
        result.setDictionary(partitions.getRight());
      }
    }
    return result;
  }


  protected final Pair<List<TableValidWriteIds>, List<String>> getValidWriteIdsCache(CacheI cache,
      GetValidWriteIdsRequest rqst) throws TException {
    List<String> fullTableNamesMissing = new ArrayList<>();
    List<TableValidWriteIds> tblValidWriteIds = new ArrayList<>();
    for (String fullTableName : rqst.getFullTableNames()) {
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_IDS,
          fullTableName, rqst.getValidTxnList(), rqst.getWriteId());
      TableValidWriteIds v = (TableValidWriteIds) cache.get(cacheKey);
      if (v == null) {
        fullTableNamesMissing.add(fullTableName);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getValidWriteIdsInternal, fullTableName={}",
            fullTableName);
        tblValidWriteIds.add(v);
      }
    }
    return Pair.of(tblValidWriteIds, fullTableNamesMissing);
  }

  protected final List<TableValidWriteIds> loadValidWriteIdsCache(CacheI cache,
      GetValidWriteIdsResponse r, GetValidWriteIdsRequest rqst)
      throws TException {
    List<TableValidWriteIds> newTblValidWriteIds = new ArrayList<>();
    for (TableValidWriteIds tableValidWriteIds : r.getTblValidWriteIds()) {
      newTblValidWriteIds.add(tableValidWriteIds);
      // Add to the cache
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_IDS,
          tableValidWriteIds.getFullTableName(), rqst.getValidTxnList(), rqst.getWriteId());
      cache.put(cacheKey, tableValidWriteIds);
    }
    return newTblValidWriteIds;
  }

  protected final GetValidWriteIdsResponse computeValidWriteIdsFinal(GetValidWriteIdsRequest rqst,
      List<TableValidWriteIds> tblValidWriteIds, List<TableValidWriteIds> newTblValidWriteIds) {
    List<TableValidWriteIds> result = new ArrayList<>();
    int i = 0, j = 0;
    for (String fullTableName : rqst.getFullTableNames()) {
      if (i >= tblValidWriteIds.size() || j >= newTblValidWriteIds.size()) {
        break;
      }
      if (tblValidWriteIds.get(i).getFullTableName().equals(fullTableName)) {
        result.add(tblValidWriteIds.get(i));
        i++;
      } else if (newTblValidWriteIds.get(j).getFullTableName().equals(fullTableName)) {
        result.add(newTblValidWriteIds.get(j));
        j++;
      }
    }
    while (i < tblValidWriteIds.size()) {
      result.add(tblValidWriteIds.get(i));
      i++;
    }
    while (j < newTblValidWriteIds.size()) {
      result.add(newTblValidWriteIds.get(j));
      j++;
    }
    return new GetValidWriteIdsResponse(result);
  }


  /**
   * Wrapper to create a cache around a Caffeine Cache.
   */
  protected static class CacheWrapper implements CacheI {
    final Cache<CacheKey, Object> c;

    protected CacheWrapper(Cache<CacheKey, Object> c) {
      this.c = c;
    }

    @Override
    public void put(Object k, Object v) {
      c.put((CacheKey) k, v);
    }

    @Override
    public Object get(Object k) {
      return c.getIfPresent(k);
    }
  }

  /**
   * Cache interface.
   */
  protected interface CacheI {
    void put(Object k, Object v);

    Object get(Object k);
  }


  /**
   * Internal class to identify uniquely a Table.
   */
  protected static class TableWatermark {
    final String validWriteIdList;
    final long tableId;

    protected TableWatermark(String validWriteIdList, long tableId) {
      this.validWriteIdList = validWriteIdList;
      this.tableId = tableId;
    }

    public boolean isValid() {
      return validWriteIdList != null && tableId != -1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableWatermark that = (TableWatermark) o;
      return tableId == that.tableId &&
          Objects.equals(validWriteIdList, that.validWriteIdList);
    }

    @Override
    public int hashCode() {
      return Objects.hash(validWriteIdList, tableId);
    }

    @Override
    public String toString() {
      return "TableWatermark {" + tableId + " @@ " + (validWriteIdList != null ? validWriteIdList : "null") + "}";
    }
  }

}
