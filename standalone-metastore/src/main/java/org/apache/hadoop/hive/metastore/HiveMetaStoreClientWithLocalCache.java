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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.localcache.PartitionCacheHelper;
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
    TABLE(GetTableResult.class, TableWatermark.class, String.class, boolean.class, long.class),
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
    // ConcurrentHashMap <-- String dbName, String tblName,
    //      (TableWatermark tw ?)
    TABLE_COLUMN_STATS(ConcurrentHashMap.class, String.class, ColumnStatisticsObj.class, String.class, String.class, TableWatermark.class),
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
    // Key: String db_name, String tbl_name, TableWatermark tw, boolean isGetFileMetadata
    // Value: CacheValue cacheValue
    PARTITIONS_BY_NAMES(PartitionCacheHelper.CacheValue.class, ConcurrentHashMap.class, String.class,
        Partition.class, String.class, String.class, TableWatermark.class, boolean.class),
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
        CacheKey cacheKey = new CacheKey(KeyType.TABLE, req.getDbName(),
            req.getTblName(), watermark, req.isGetFileMetadata(), req.getEngine(),
            req.isGetColumnStats(), req.getId());
        GetTableResult r = (GetTableResult) mscLocalCache.getIfPresent(cacheKey);
        if (r == null) {
          r = convertTableRequest(req, super.getTableInternal(req));
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
    return convertTableRequest(req, super.getTableInternal(req));
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
        List<String> columnNamesMissing = new ArrayList<>();
        List<ColumnStatisticsObj> columnStatsFound = new ArrayList<>();
        // 1) Retrieve from the cache those ids present, gather the rest
        getTableColumnStatisticsCache(cache, req, watermark, columnNamesMissing, columnStatsFound);
        // 2) If they were all present in the cache, return
        if (columnNamesMissing.isEmpty()) {
          return new TableStatsResult(columnStatsFound);
        }
        // 3) If they were not, gather the remaining
        TableStatsRequest newRqst = new TableStatsRequest(req);
        newRqst.setColNames(columnNamesMissing);
        TableStatsResult r = super.getTableColumnStatisticsInternal(newRqst);
        // 4) Populate the cache
        List<ColumnStatisticsObj> newColumnStats = new ArrayList<>();
        loadTableColumnStatisticsCache(cache, r, req, watermark, newColumnStats);
        // 5) Sort result (in case there is any assumption) and return
        TableStatsResult result = computeTableColumnStatisticsFinal(req, columnStatsFound, newColumnStats);

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
    String dbName = parseDbName(rqst.getDb_name(), conf)[1];
    Table table = getTable(dbName, rqst.getTbl_name());
    if (isCacheEnabledAndInitialized()) {
      TableWatermark watermark = new TableWatermark(
          rqst.getValidWriteIdList(), table.getId());
      if (watermark.isValid()) {
        CacheKey key = new CacheKey(KeyType.PARTITIONS_BY_NAMES, rqst.getDb_name(),
            rqst.getTbl_name(), watermark, rqst.isGetFileMetadata());
        // check cache for value
        PartitionCacheHelper.CacheValue cacheValue =
            (PartitionCacheHelper.CacheValue) mscLocalCache.get(key,
            k -> PartitionCacheHelper.createCacheValue(rqst, getHMSConverter(), table, true));
        PartitionCacheHelper cacheHelper = new PartitionCacheHelper(cacheValue,
            rqst, getHMSConverter(),  table, PartitionCacheHelper.Level.HS2);
        if (!cacheHelper.cacheContainsAllPartitions()) {
          // The cache didn't contain all the partitions so we need to fetch the ones it didn't have.
          GetPartitionsByNamesRequest missingNamesRqst = cacheHelper.getMissingNamesRequest();
          GetPartitionsByNamesResult missingNamesResult
              = super.getPartitionsByNamesInternal(missingNamesRqst);

          cacheHelper.addToCache(missingNamesRqst, missingNamesResult, cacheValue);

          if (LOG.isDebugEnabled() && recordStats) {
            LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
          }
        }
        return cacheHelper.fetchRequestedResultFromCache(cacheValue);
      }
    }
    // retrieve the partitions from HMS and (potentially) convert them into its
    // final response (if the converter exists).
    GetPartitionsByNamesResult requestedResult = super.getPartitionsByNamesInternal(rqst);

    PartitionCacheHelper cacheHelper =
        new PartitionCacheHelper(rqst, getHMSConverter(), table, PartitionCacheHelper.Level.HS2);
    return cacheHelper.fetchRequestedResult(requestedResult);
  }

  /**
   * Checks if cache is enabled and initialized
   *
   * @return boolean
   */
  private boolean isCacheEnabledAndInitialized() {
    return INITIALIZED.get();
  }


  protected final void getTableColumnStatisticsCache(CacheI cache,
      TableStatsRequest rqst, TableWatermark watermark, List<String> columnNamesMissing,
      List<ColumnStatisticsObj> columnStatsFound) {
    CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark,
        rqst.getDbName(), rqst.getTblName());
    Map<String, ColumnStatisticsObj> cacheValue =
        (Map<String, ColumnStatisticsObj>) cache.get(cacheKey);
    if (cacheValue == null) {
      // no cached column stats exist, so all requested column stats are missing
      columnNamesMissing.addAll(rqst.getColNames());
      return;
    }

    List<String> columnNamesFound = new ArrayList<>();
    for (String colName : rqst.getColNames()) {
      ColumnStatisticsObj cachedColStats = cacheValue.get(colName);
      if (cachedColStats == null) {
        columnNamesMissing.add(colName);
      } else {
        columnStatsFound.add(cachedColStats);
        columnNamesFound.add(colName);
      }
    }

    if (columnNamesFound.size() > 0) {
      if (watermark == null) {
        LOG.debug(
            "Query level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, columnNames={}",
            rqst.getDbName(), rqst.getTblName(), columnNamesFound);
      } else {
        LOG.debug(
            "HS2 level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, columnNames={}",
            rqst.getDbName(), rqst.getTblName(), columnNamesFound);
      }
    }
  }

  protected final void loadTableColumnStatisticsCache(CacheI cache,
      TableStatsResult r, TableStatsRequest rqst, TableWatermark watermark,
      List<ColumnStatisticsObj> newColumnStats) {
    CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark,
        rqst.getDbName(), rqst.getTblName());
    Map<String, ColumnStatisticsObj> cacheValue;
    if (cache instanceof CacheWrapper) {
      cacheValue = (Map<String, ColumnStatisticsObj>) ((CacheWrapper) cache).cache.get(cacheKey,
          k -> new ConcurrentHashMap<String, ColumnStatisticsObj>());
    } else {
      cacheValue = (Map<String, ColumnStatisticsObj>) cache.get(cacheKey);
      if (cacheValue == null) {
        cacheValue = new ConcurrentHashMap<>();
        cache.put(cacheKey, cacheValue);
      }
    }

    for (ColumnStatisticsObj colStat : r.getTableStats()) {
      cacheValue.put(colStat.getColName(), colStat);
      newColumnStats.add(colStat);
    }
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

  @Override
  protected HMSConverter getHMSConverter() {
    return null;
  }

  protected final GetTableResult convertTableRequest(
      GetTableRequest rqst, GetTableResult result) {
    if (getHMSConverter() == null || getHMSConverter().getTableConverter(rqst) == null) {
      return result;
    }

    return getHMSConverter().getTableConverter(rqst).convertTable(result);
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
    final Cache<CacheKey, Object> cache;

    protected CacheWrapper(Cache<CacheKey, Object> c) {
      this.cache = c;
    }

    @Override
    public void put(Object k, Object v) {
      cache.put((CacheKey) k, v);
    }

    @Override
    public Object get(Object k) {
      return cache.getIfPresent(k);
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
