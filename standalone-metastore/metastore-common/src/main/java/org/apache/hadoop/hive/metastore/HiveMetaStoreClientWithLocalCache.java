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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsSpecByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

/**
 * This class introduces a caching layer in HS2 for metadata for some selected query APIs. It extends
 * HiveMetaStoreClient, and overrides some of its methods to add this feature.
 * Its design is simple, relying on snapshot information being queried to cache and invalidate the metadata.
 * It helps to reduce the time spent in compilation by using HS2 memory more effectively, and it allows to
 * improve HMS throughput for multi-tenant workloads by reducing the number of calls it needs to serve.
 */
public class HiveMetaStoreClientWithLocalCache extends HiveMetaStoreClient {

  private static Cache<CacheKey, Object> mscLocalCache = null;
  private static boolean IS_CACHE_ENABLED;
  private static long MAX_SIZE;
  private static boolean RECORD_STATS;
  private static HashMap<Class<?>, ObjectEstimator> sizeEstimator = null;
  private static String cacheObjName = null;

  public static synchronized void init() {
    if (mscLocalCache != null) return; // init cache only once
    Configuration metaConf = MetastoreConf.newMetastoreConf();
    LOG.debug("Initializing local cache in HiveMetaStoreClient...");
    MAX_SIZE = MetastoreConf.getSizeVar(metaConf, MetastoreConf.ConfVars.MSC_CACHE_MAX_SIZE);
    IS_CACHE_ENABLED = MetastoreConf.getBoolVar(metaConf, MetastoreConf.ConfVars.MSC_CACHE_ENABLED);
    RECORD_STATS = MetastoreConf.getBoolVar(metaConf, MetastoreConf.ConfVars.MSC_CACHE_RECORD_STATS);
    initSizeEstimator();
    initCache();
    LOG.debug("Local cache initialized in HiveMetaStoreClient: " + mscLocalCache);
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
    Arrays.stream(KeyType.values()).forEach(e -> {
      IncrementalObjectSizeEstimator.createEstimators(e.keyClass, sizeEstimator);
      IncrementalObjectSizeEstimator.createEstimators(e.valueClass, sizeEstimator);}
    );
  }

  /**
   * KeyType is used to differentiate the request types. More types can be added in future.
   */
  public enum KeyType {
    PARTITIONS_BY_EXPR(PartitionsByExprRequest.class, PartitionsByExprResult.class),
    PARTITIONS_SPEC_BY_EXPR(PartitionsByExprRequest.class, PartitionsSpecByExprResult.class),
    AGGR_COL_STATS(PartitionsStatsRequest.class, AggrStats.class);

    private final Class<?> keyClass;
    private final Class<?> valueClass;

    KeyType(Class<?> keyClass, Class<?> valueClass) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }
  }

  /**
   * CacheKey objects are used as key for the cache.
   */
  public static class CacheKey{
    KeyType IDENTIFIER;
    Object obj;

    public CacheKey(KeyType IDENTIFIER, Object obj) {
      this.IDENTIFIER = IDENTIFIER;
      this.obj = obj;
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
  }

  public static class PartitionsStatsCustomRequest {
    PartitionsStatsRequest request;
    String validWriteIdList;
    long tableId;

    public PartitionsStatsCustomRequest(PartitionsStatsRequest req, String validWriteIdList, long tableId) {
      this.request = req;
      this.validWriteIdList = validWriteIdList;
      this.tableId = tableId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionsStatsCustomRequest that = (PartitionsStatsCustomRequest) o;
      return tableId == that.tableId &&
          Objects.equals(request, that.request) &&
          Objects.equals(validWriteIdList, that.validWriteIdList);
    }

    @Override
    public int hashCode() {
      return Objects.hash(request, validWriteIdList, tableId);
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

  private Object load(CacheKey key) {
    try {
      return getResultObject(key);
    } catch (TException e) {
      throw new UncheckedCacheException(e);
    }
  }

/**
 * Initializes the cache
 */
  private static void initCache() {
    int initSize = 100;
    Caffeine<CacheKey, Object> cacheBuilder = Caffeine.newBuilder()
            .initialCapacity(initSize)
            .maximumWeight(MAX_SIZE)
            .weigher(HiveMetaStoreClientWithLocalCache::getWeight)
            .removalListener((key, val, cause) -> {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Caffeine - ({}, {}) was removed ({})", key, val, cause);
              }});
    if (RECORD_STATS) {
      cacheBuilder.recordStats();
    }
    mscLocalCache = cacheBuilder.build();
    cacheObjName = mscLocalCache.toString().substring(mscLocalCache.toString().indexOf("Cache@"));
  }

  /**
   * This method is used to load the cache by calling relevant APIs, depending on the type of the request.
   *
   * @param cacheKey key of the cache, containing an identifier and a request object
   * @return Result object / null
   * @throws TException
   */
  private Object getResultObject(CacheKey cacheKey) throws TException {
    Object result = null;

    switch (cacheKey.IDENTIFIER) {
      case PARTITIONS_BY_EXPR:
        result = super.getPartitionsByExprResult((PartitionsByExprRequest)cacheKey.obj);
        break;
      case PARTITIONS_SPEC_BY_EXPR:
        result = super.getPartitionsSpecByExprResult((PartitionsByExprRequest)cacheKey.obj);
        break;
      case AGGR_COL_STATS:
        PartitionsStatsCustomRequest customRequest = (PartitionsStatsCustomRequest) cacheKey.obj;
        result = super.getAggrStatsFor(customRequest.request);
        break;
      default:
        break;
    }

    return result;
  }

  @Override
  protected PartitionsByExprResult getPartitionsByExprResult(PartitionsByExprRequest req) throws TException {
    PartitionsByExprResult r;

    // table should be transactional to get responses from the cache
    if (isCacheEnabledAndInitialized() && isRequestCacheable(req, KeyType.PARTITIONS_BY_EXPR)) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR, req);
      try {
        r = (PartitionsByExprResult) mscLocalCache.get(cacheKey, this::load); // get either the result or an Exception

        if (LOG.isDebugEnabled() && RECORD_STATS) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

      } catch (UncheckedCacheException e) {
        if (e.getCause() instanceof MetaException) {
          throw (MetaException) e.getCause();
        } else if (e.getCause() instanceof TException) {
          throw (TException) e.getCause();
        } else {
          throw new TException(e.getCause());
        }
      }
    } else {
      r = client.get_partitions_by_expr(req);
    }

    return r;
  }

  @Override
  protected PartitionsSpecByExprResult getPartitionsSpecByExprResult(PartitionsByExprRequest req) throws TException {
    PartitionsSpecByExprResult r;

    // table should be transactional to get responses from the cache
    if (isCacheEnabledAndInitialized() && isRequestCacheable(req, KeyType.PARTITIONS_SPEC_BY_EXPR)) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_SPEC_BY_EXPR, req);
      try {
        r = (PartitionsSpecByExprResult) mscLocalCache.get(cacheKey, this::load); // get either the result or an Exception

        if (LOG.isDebugEnabled() && RECORD_STATS) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }

      } catch (UncheckedCacheException e) {
        if (e.getCause() instanceof MetaException) {
          throw (MetaException) e.getCause();
        } else if (e.getCause() instanceof TException) {
          throw (TException) e.getCause();
        } else {
          throw new TException(e.getCause());
        }
      }
    } else {
      r = client.get_partitions_spec_by_expr(req);
    }

    return r;
  }


  @Override
  protected AggrStats getAggrStatsFor(PartitionsStatsRequest req) throws TException {
    AggrStats r;

    Table tbl = getTable(req.getDbName(), req.getTblName());
    PartitionsStatsCustomRequest customRequest = new PartitionsStatsCustomRequest(req,
        getValidWriteIdList(TableName.getDbTable(req.getDbName(), req.getTblName())), tbl.getId());

    if (isCacheEnabledAndInitialized() && isRequestCacheable(customRequest, KeyType.AGGR_COL_STATS)) {
      CacheKey cacheKey = new CacheKey(KeyType.AGGR_COL_STATS, customRequest);
      try {
        r = (AggrStats) mscLocalCache.get(cacheKey, this::load);

        if (LOG.isDebugEnabled() && RECORD_STATS) {
          LOG.debug(cacheObjName + ": " + mscLocalCache.stats().toString());
        }
      } catch (UncheckedCacheException e) {
        if (e.getCause() instanceof MetaException) {
          throw (MetaException) e.getCause();
        } else if (e.getCause() instanceof TException) {
          throw (TException) e.getCause();
        } else {
          throw new TException(e.getCause());
        }
      }
    } else {
      r = super.getAggrStatsFor(req);
    }

    return r;
  }

  /**
   * This method determines if the request should be cached.
   * @param request Request object
   * @return boolean
   */
  private boolean isRequestCacheable(Object request, KeyType keyType) {
    switch (keyType) {
      //cache only requests for transactional tables, with a valid table id
      case PARTITIONS_BY_EXPR:
      case PARTITIONS_SPEC_BY_EXPR:
        PartitionsByExprRequest req = (PartitionsByExprRequest) request;
        return req.getValidWriteIdList() != null && req.getId() != -1;
      case AGGR_COL_STATS:
        PartitionsStatsCustomRequest customRequest = (PartitionsStatsCustomRequest) request;
        return customRequest.tableId != -1 && customRequest.validWriteIdList != null;
        // Requests of other types can have different conditions and should be added here.
      default:
        return false;
    }
  }

  /**
   * Checks if cache is enabled and initialized
   *
   * @return boolean
   */
  private boolean isCacheEnabledAndInitialized() {
    return IS_CACHE_ENABLED && mscLocalCache != null;
  }
}

/**
 * This unchecked exception is thrown from the load method because checked exception is
 * not thrown from the functional interface
 */
class UncheckedCacheException extends RuntimeException {
  public UncheckedCacheException(Throwable t) {
    super(t);
  }
}
