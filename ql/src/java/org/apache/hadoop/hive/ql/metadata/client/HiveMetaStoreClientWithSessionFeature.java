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

package org.apache.hadoop.hive.ql.metadata.client;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.client.HiveMetaStoreClientUtils;
import org.apache.hadoop.hive.metastore.client.NoopHiveMetaStoreClientDelegator;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheKey;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheI;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.KeyType;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionSpecsWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionsWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.Supplier;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.createThriftPartitionsReq;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;

/**
 * This class provides two features: query-level cache and Session level transaction.
 * SG:FIXME, is "Session level transaction" correct explanation?
 */
public class HiveMetaStoreClientWithSessionFeature extends NoopHiveMetaStoreClientDelegator
    implements IMetaStoreClient {
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientWithSessionFeature.class);

  private final Configuration conf;

  public HiveMetaStoreClientWithSessionFeature(Configuration conf, IMetaStoreClient delegate) {
    super(delegate);
    this.conf = conf;
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.CONFIG_VALUE, name, defaultValue);
      String v = (String) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().getConfigValue(name, defaultValue);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getConfigValueInternal, name={}",
            name);
      }
      return v;
    }
    return getDelegate().getConfigValue(name, defaultValue);
  }

  @Override
  public Database getDatabase(String name) throws TException {
    return getDatabase(getDefaultCatalog(conf), name);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.DATABASE, catalogName, databaseName);
      Database v = (Database) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().getDatabase(catalogName, databaseName);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getDatabaseInternal, name={}",
            databaseName);
      }
      return v;
    }
    return getDelegate().getDatabase(catalogName, databaseName);
  }


  @Override
  public Table getTable(String dbname, String name) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    return getTable(req);
  }

  @Override
  public Table getTable(String dbname, String name, boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList)
      throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public Table getTable(GetTableRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKeyTableId =
          new CacheKey(KeyType.TABLE_ID, req.getCatName(), req.getDbName(), req.getTblName());
      long tableId = -1;

      if (queryCache.containsKey(cacheKeyTableId)) {
        tableId = (long) queryCache.get(cacheKeyTableId);
      }

      req.setId(tableId);
      CacheKey cacheKey = new CacheKey(KeyType.TABLE, req);
      Table table = (Table) queryCache.get(cacheKey);
      if (table == null) {
        table = getDelegate().getTable(req);
        if (tableId == -1) {
          queryCache.put(cacheKeyTableId, table.getId());
          req.setId(table.getId());
          cacheKey = new CacheKey(KeyType.TABLE, req);
        }
        queryCache.put(cacheKey, table);
      } else {
        LOG.debug("Query level HMS cache: method=getTableInternal, dbName={}, tblName={}", req.getDbName(),
            req.getTblName());
      }
      return table;
    }
    return getDelegate().getTable(req);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return tableExists(getDefaultCatalog(conf), databaseName, tableName);
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
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.PRIMARY_KEYS, req);
      PrimaryKeysResponse v = (PrimaryKeysResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new PrimaryKeysResponse(getDelegate().getPrimaryKeys(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPrimaryKeysInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getPrimaryKeys();
    }
    return getDelegate().getPrimaryKeys(req);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.FOREIGN_KEYS, req);
      ForeignKeysResponse v = (ForeignKeysResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new ForeignKeysResponse(getDelegate().getForeignKeys(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getForeignKeysInternal, dbName={}, tblName={}",
            req.getForeign_db_name(), req.getForeign_tbl_name());
      }
      return v.getForeignKeys();
    }
    return getDelegate().getForeignKeys(req);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.UNIQUE_CONSTRAINTS, req);
      UniqueConstraintsResponse v = (UniqueConstraintsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new UniqueConstraintsResponse(getDelegate().getUniqueConstraints(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getUniqueConstraintsInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getUniqueConstraints();
    }
    return getDelegate().getUniqueConstraints(req);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.NOT_NULL_CONSTRAINTS, req);
      NotNullConstraintsResponse v = (NotNullConstraintsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new NotNullConstraintsResponse(getDelegate().getNotNullConstraints(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getNotNullConstraintsInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getNotNullConstraints();
    }
    return getDelegate().getNotNullConstraints(req);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames, engine);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine) throws TException {
    return getTableColumnStatistics(catName, dbName, tableName, colNames, engine, null);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList) throws TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames,
        engine, validWriteIdList);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine, String validWriteIdList) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      MapWrapper cache = new MapWrapper(queryCache);
      // 1) Retrieve from the cache those ids present, gather the rest
      Pair<List<ColumnStatisticsObj>, List<String>> p =
          MetaStoreClientCacheUtils.getTableColumnStatisticsCache(cache, catName, dbName, tableName,
              colNames, engine, validWriteIdList, null);
      List<String> colStatsMissing = p.getRight();
      List<ColumnStatisticsObj> colStats = p.getLeft();
      // 2) If they were all present in the cache, return
      if (colStatsMissing.isEmpty()) {
        return colStats;
      }
      // 3) If they were not, gather the remaining
      List<ColumnStatisticsObj> newColStats = getDelegate().getTableColumnStatistics(catName, dbName, tableName,
          colStatsMissing, engine, validWriteIdList);
      // 4) Populate the cache
      MetaStoreClientCacheUtils.loadTableColumnStatisticsCache(cache, newColStats, catName, dbName,
          tableName, engine, validWriteIdList, null);
      // 5) Sort result (in case there is any assumption) and return
      List<ColumnStatisticsObj> result =
          MetaStoreClientCacheUtils.computeTableColumnStatisticsFinal(colNames, colStats, newColStats);
      return result;
    }

    return getDelegate().getTableColumnStatistics(catName, dbName, tableName, colNames, engine, validWriteIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList) throws TException {
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames,
        partNames, engine, writeIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName,
      List<String> colNames, List<String> partNames, String engine) throws TException {
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames, partNames, engine);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
      List<String> colNames, List<String> partNames, String engine) throws TException {
    String writeIdList = getValidWriteIdList(dbName, tblName);
    return getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
      req.setEngine(engine);
      req.setCatName(catName);
      req.setValidWriteIdList(writeIdList);
      CacheKey cacheKey = new CacheKey(KeyType.AGGR_COL_STATS, req);
      AggrStats v = (AggrStats) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getAggrStatsForInternal, dbName={}, tblName={}, partNames={}",
            req.getDbName(), req.getTblName(), req.getPartNames());
      }
      return v;
    }

    return getDelegate().getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
  }


  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
      String default_partition_name, short max_parts, List<Partition> result) throws TException {
    return listPartitionsByExpr(getDefaultCatalog(conf), db_name, tbl_name, expr, default_partition_name,
        max_parts, result);
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
    Supplier<PartitionsWrapper, TException> supplier = new Supplier<PartitionsWrapper, TException>() {
      @Override
      public MetaStoreClientCacheUtils.PartitionsWrapper get() throws TException {
        List<Partition> parts = new ArrayList<>();
        boolean hasUnknownPart = getDelegate().listPartitionsByExpr(
            catName, db_name, tbl_name, expr, default_partition_name, max_parts, parts);
        return new MetaStoreClientCacheUtils.PartitionsWrapper(parts, hasUnknownPart);
      }
    };
    PartitionsByExprRequest req =
        buildPartitionsByExprRequest(catName, db_name, tbl_name, expr, default_partition_name, max_parts);
    MetaStoreClientCacheUtils.PartitionsWrapper r = getPartitionsByExprInternal(req, supplier);
    result.addAll(r.partitions);
    return r.hasUnknownPartition;
  }

  private PartitionsByExprRequest buildPartitionsByExprRequest(String catName, String db_name,
      String tbl_name, byte[] expr, String default_partition_name, int max_parts) {
    PartitionsByExprRequest req = new PartitionsByExprRequest(db_name, tbl_name, ByteBuffer.wrap(expr));
    if (catName == null) {
      req.setCatName(getDefaultCatalog(conf));
    } else {
      req.setCatName(catName);
    }
    if (default_partition_name != null) {
      req.setDefaultPartitionName(default_partition_name);
    }
    if (max_parts >= 0) {
      req.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(max_parts));
    }
    req.setValidWriteIdList(getValidWriteIdList(db_name, tbl_name));
    return req;
  }

  // SG:FIXME, should replace lambda with concrete function
  private MetaStoreClientCacheUtils.PartitionsWrapper getPartitionsByExprInternal(
      PartitionsByExprRequest req, Supplier<PartitionsWrapper, TException> supplier) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR, req);
      PartitionsWrapper v = (PartitionsWrapper) queryCache.get(cacheKey);
      if (v == null) {
        v = supplier.get();
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPartitionsByExprInternal, dbName={}, tblName={}",
            req.getDbName(), req.getTblName());
      }
      return v;
    }
    return supplier.get();
  }


  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    Supplier<PartitionSpecsWrapper, TException> supplier = new Supplier<PartitionSpecsWrapper, TException>() {
      @Override
      public PartitionSpecsWrapper get() throws TException {
        List<PartitionSpec> parts = new ArrayList<>();
        boolean hasUnknownPart = getDelegate().listPartitionsSpecByExpr(req, parts);
        return new PartitionSpecsWrapper(parts, hasUnknownPart);
      }
    };
    PartitionSpecsWrapper r = getPartitionsSpecByExprInternal(req, supplier);
    result.addAll(r.partitionSpecs);
    return r.hasUnknownPartition;
  }

  // SG:FIXME, should replace lambda with concrete function
  private PartitionSpecsWrapper getPartitionsSpecByExprInternal(
      PartitionsByExprRequest req, Supplier<PartitionSpecsWrapper, TException> supplier) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_SPEC_BY_EXPR, req);
      PartitionSpecsWrapper v = (PartitionSpecsWrapper) queryCache.get(cacheKey);
      if (v == null) {
        v = supplier.get();
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPartitionsSpecByExprInternal, dbName={}, tblName={}",
            req.getDbName(), req.getTblName());
      }
      return v;
    }
    return supplier.get();
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short max) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), dbName, tblName, max);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_ALL, catName, dbName, tableName, maxParts);
      List<String> v = (List<String>) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionNames(catName, dbName, tableName, maxParts);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionNamesInternalAll, dbName={}, tblName={}",
            dbName, tableName);
      }
      return v;
    }
    return getDelegate().listPartitionNames(catName, dbName, tableName, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tableName, List<String> partVals,
      short maxParts) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), dbName, tableName, partVals, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName,
      List<String> partVals, int maxParts) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey =
          new CacheKey(KeyType.LIST_PARTITIONS, catName, dbName, tableName, partVals, maxParts);
      List<String> v = (List<String>) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionNames(catName, dbName, tableName, partVals, maxParts);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionNamesInternal, dbName={}, tblName={}",
            dbName, tableName);
      }
      return v;
    }
    return getDelegate().listPartitionNames(catName, dbName, tableName, partVals, maxParts);
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    if (req.getCatName() == null) {
      req.setCatName(getDefaultCatalog(conf));
    }
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_REQ, req);
      GetPartitionNamesPsResponse v = (GetPartitionNamesPsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionNamesRequest(req);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionNamesRequestInternal, dbName={}, tblName={}, partValues={}",
            req.getDbName(), req.getTblName(), req.getPartValues());
      }
      return v;
    }
    return getDelegate().listPartitionNamesRequest(req);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short maxParts,
      String userName, List<String> groupNames) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitionsWithAuthInfo(
        getDefaultCatalog(conf), dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      int maxParts, String userName,
      List<String> groupNames) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO_ALL,
          catName, dbName, tableName, maxParts, userName, groupNames);
      List<Partition> v = (List<Partition>) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoInternalAll, dbName={}, tblName={}",
            dbName, tableName);
      }
      return v;
    }
    return getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name, List<String> part_vals,
      short max_parts, String user_name, List<String> group_names) throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts,
        user_name, group_names);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts,
      String userName, List<String> groupNames)
      throws TException {
    // TODO should we add capabilities here as well as it returns Partition objects
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO,
          catName, dbName, tableName, partialPvals, maxParts, userName, groupNames);
      List<Partition> v = (List<Partition>) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals, maxParts, userName,
            groupNames);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoInternal, dbName={}, tblName={}, partVals={}",
            dbName, tableName, partialPvals);
      }
      return v;
    }
    return getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals, maxParts, userName,
        groupNames);
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
      throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    if(req.getCatName() == null) {
      req.setCatName(getDefaultCatalog(conf));
    }
    req.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(req.getMaxParts()));

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO_REQ, req);
      GetPartitionsPsWithAuthResponse v = (GetPartitionsPsWithAuthResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().listPartitionsWithAuthInfoRequest(req);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoRequestInternal, dbName={}, tblName={}, partVals={}",
            req.getDbName(), req.getTblName(), req.getPartVals());
      }
      return v;
    }
    return getDelegate().listPartitionsWithAuthInfoRequest(req);
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws TException {
    return getPartitionsByNames(getDefaultCatalog(conf), db_name, tbl_name, part_names);
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
      List<String> part_names) throws TException {
    GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(
        MetaStoreUtils.prependCatalogToDbName(catName, db_name, conf), tbl_name, part_names);
    return getPartitionsByNames(req).getPartitions();
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req)
      throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      MapWrapper cache = new MapWrapper(queryCache);
      // 1) Retrieve from the cache those ids present, gather the rest
      Pair<List<Partition>, List<String>> p =
          MetaStoreClientCacheUtils.getPartitionsByNamesCache(cache, req, null);
      List<String> partitionsMissing = p.getRight();
      List<Partition> partitions = p.getLeft();
      // 2) If they were all present in the cache, return
      if (partitionsMissing.isEmpty()) {
        return new GetPartitionsByNamesResult(partitions);
      }
      // 3) If they were not, gather the remaining
      GetPartitionsByNamesRequest newRqst = new GetPartitionsByNamesRequest(req);
      newRqst.setNames(partitionsMissing);
      GetPartitionsByNamesResult r = getDelegate().getPartitionsByNames(req);
      // 4) Populate the cache
      List<Partition> newPartitions =
          MetaStoreClientCacheUtils.loadPartitionsByNamesCache(cache, r, req, null);
      // 5) Sort result (in case there is any assumption) and return
      return MetaStoreClientCacheUtils.computePartitionsByNamesFinal(req, partitions, newPartitions);
    }
    return getDelegate().getPartitionsByNames(req);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_ID, fullTableName);
      ValidWriteIdList v = (ValidWriteIdList) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().getValidWriteIds(fullTableName);
        queryCache.put(cacheKey, v);
      }
      return v;
    }
    return getDelegate().getValidWriteIds(fullTableName);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_ID, fullTableName, writeId);
      ValidWriteIdList v = (ValidWriteIdList) queryCache.get(cacheKey);
      if (v == null) {
        v = getDelegate().getValidWriteIds(fullTableName, writeId);
        queryCache.put(cacheKey, v);
      }
      return v;
    }
    return getDelegate().getValidWriteIds(fullTableName, writeId);
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList)
      throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tablesList);
      rqst.setValidTxnList(validTxnList);

      MapWrapper cache = new MapWrapper(queryCache);
      // 1) Retrieve from the cache those ids present, gather the rest
      Pair<List<TableValidWriteIds>, List<String>> p = getValidWriteIdsCache(cache, rqst);
      List<String> fullTableNamesMissing = p.getRight();
      List<TableValidWriteIds> tblValidWriteIds = p.getLeft();
      // 2) If they were all present in the cache, return
      if (fullTableNamesMissing.isEmpty()) {
        return tblValidWriteIds;
      }
      // 3) If they were not, gather the remaining
      List<TableValidWriteIds> r = getDelegate().getValidWriteIds(fullTableNamesMissing, validTxnList);
      // 4) Populate the cache
      List<TableValidWriteIds> newTblValidWriteIds = loadValidWriteIdsCache(cache, r, rqst);
      // 5) Sort result (in case there is any assumption) and return
      return computeValidWriteIdsFinal(rqst, tblValidWriteIds, newTblValidWriteIds);
    }
    return getDelegate().getValidWriteIds(tablesList, validTxnList);
  }

  private Pair<List<TableValidWriteIds>, List<String>> getValidWriteIdsCache(CacheI cache,
      GetValidWriteIdsRequest rqst) {
    List<String> fullTableNamesMissing = new ArrayList<>();
    List<TableValidWriteIds> tblValidWriteIds = new ArrayList<>();
    for (String fullTableName : rqst.getFullTableNames()) {
      CacheKey cacheKey =
          new CacheKey(KeyType.VALID_WRITE_IDS, fullTableName, rqst.getValidTxnList(), rqst.getWriteId());
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

  private List<TableValidWriteIds> loadValidWriteIdsCache(CacheI cache, List<TableValidWriteIds> r,
      GetValidWriteIdsRequest rqst) {
    List<TableValidWriteIds> newTblValidWriteIds = new ArrayList<>();
    for (TableValidWriteIds tableValidWriteIds : r) {
      newTblValidWriteIds.add(tableValidWriteIds);
      // Add to the cache
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_IDS,
          tableValidWriteIds.getFullTableName(), rqst.getValidTxnList(), rqst.getWriteId());
      cache.put(cacheKey, tableValidWriteIds);
    }
    return newTblValidWriteIds;
  }

  private List<TableValidWriteIds> computeValidWriteIdsFinal(GetValidWriteIdsRequest rqst,
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
    return result;
  }

  /**
   * Wrapper to create a cache around a Map.
   */
  private static class MapWrapper implements CacheI {

    final Map<Object, Object> m;

    protected MapWrapper(Map<Object, Object> m) {
      this.m = m;
    }

    @Override
    public void put(Object k, Object v) {
      m.put(k, v);
    }

    @Override
    public Object get(Object k) {
      return m.get(k);
    }
  }

  private Map<Object, Object> getQueryCache() {
    String queryId = MetaStoreClientCacheUtils.getQueryId();
    if (queryId != null) {
      SessionState ss = SessionState.get();
      if (ss != null) {
        return ss.getQueryCache(queryId);
      }
    }
    return null;
  }

  /**
   * Methods from SessionHiveMetaStoreClient that does not use query-level caches.
   */

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req) throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    return getDelegate().getPartitionRequest(req);
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req) throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    return getDelegate().getPartitionsRequest(req);
  }

  // SG:FIXME, methods that use `getValidWriteIdList` should be overridden as well.
  //  List of missing methods: getPartitionColumnStatistics,
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
          new ValidTxnWriteIdList(validWriteIds) :
          SessionState.get().getTxnMgr().getValidWriteIds(ImmutableList.of(fullTableName), validTxnsList);

      ValidWriteIdList writeIdList = validTxnWriteIdList.getTableValidWriteIdList(fullTableName);
      return (writeIdList != null) ? writeIdList.toString() : null;
    } catch (Exception e) {
      throw new RuntimeException("Exception getting valid write id list", e);
    }
  }
}
