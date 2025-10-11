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

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.cache.CachedStore;
import org.apache.hadoop.hive.metastore.client.MetaStoreClientWrapper;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.utils.HiveMetaStoreClientUtils;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheKey;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheI;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.KeyType;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionSpecsWrapper;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.PartitionsWrapper;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeValsFromName;
import static org.apache.hadoop.hive.metastore.client.utils.HiveMetaStoreClientUtils.deepCopyFieldSchemas;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.compareFieldColumns;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getColumnNamesForTable;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPvals;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.isExternalTable;

/**
 * todo: This need review re: thread safety.  Various places (see callsers of
 * {@link SessionState#setCurrentSessionState(SessionState)}) pass SessionState to forked threads.
 * Currently it looks like those threads only read metadata but this is fragile.
 * Also, maps (in SessionState) where tempt table metadata is stored are concurrent and so
 * any put/get crosses a memory barrier and so does using most {@code java.util.concurrent.*}
 * so the readers of the objects in these maps should have the most recent view of the object.
 * But again, could be fragile.
 */
public class SessionHiveMetaStoreClient extends MetaStoreClientWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(SessionHiveMetaStoreClient.class);

  private volatile Warehouse wh = null;

  public static SessionHiveMetaStoreClient newClient(Configuration conf, IMetaStoreClient delegate) {
    return new SessionHiveMetaStoreClient(conf, delegate);
  }

  public SessionHiveMetaStoreClient(Configuration conf, IMetaStoreClient delegate) {
    super(delegate, conf);
  }

  private Warehouse getWh() throws MetaException {
    if (wh == null) {
      synchronized (this) {
        if (wh == null) {
          wh = new Warehouse(conf);
        }
      }
    }

    return wh;
  }

  private boolean isDefaultCatalog(String catName) {
    return catName == null || catName.isEmpty() || getDefaultCatalog(conf).equals(catName);
  }

  /**
   * Methods for supporting multiple features
   */

  @Override
  public Table getTable(GetTableRequest req) throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table tempTable = getTempTable(req.getDbName(), req.getTblName());
      if (tempTable != null) {
        // Original method used deepCopy(), do the same here.
        return HiveMetaStoreClientUtils.deepCopy(tempTable);
      }
    }

    // ThriftHiveMetaStoreClient sets processorCapabilities to GetTableRequest.
    // Since we use GetTableRequest as a cache key, we update GetTableRequest in advance
    // to make sure that the request inside the cache key and the actual request sent to HMS are identical.
    String[] processorCapabilities = ThriftHiveMetaStoreClient.getProcessorCapabilities();
    if (processorCapabilities != null) {
      req.setProcessorCapabilities(Arrays.asList(processorCapabilities));
    }
    req.setProcessorIdentifier(ThriftHiveMetaStoreClient.getProcessorIdentifier());

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
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
        table = delegate.getTable(req);
        if (tableId == -1) {
          queryCache.put(cacheKeyTableId, table.getId());
          req.setId(table.getId());
          cacheKey = new CacheKey(KeyType.TABLE, req);
        }
        queryCache.put(cacheKey, table);
      }
      return table;
    }
    return delegate.getTable(req);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern) throws TException {
    List<String> tableNames = delegate.getTables(catName, dbName, tablePattern);

    if (isDefaultCatalog(catName)) {
      // May need to merge with list of temp tables
      dbName = dbName.toLowerCase();
      tablePattern = tablePattern.toLowerCase();
      Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables =
          getTempTablesForDatabase(dbName, tablePattern);
      if (tables == null || tables.size() == 0) {
        return tableNames;
      }
      tablePattern = tablePattern.replaceAll("(?<!\\.)\\*", ".*");
      Pattern pattern = Pattern.compile(tablePattern);
      Matcher matcher = pattern.matcher("");
      Set<String> combinedTableNames = new HashSet<String>();
      for (String tableName : tables.keySet()) {
        matcher.reset(tableName);
        if (matcher.matches()) {
          combinedTableNames.add(tableName);
        }
      }

      // Combine/sort temp and normal table results
      combinedTableNames.addAll(tableNames);
      tableNames = new ArrayList<String>(combinedTableNames);
      Collections.sort(tableNames);
    }

    return tableNames;
  }

  @Override
  public List<String> getTables(String catName, String dbname, String tablePattern, TableType tableType)
      throws TException {
    List<String> tableNames = delegate.getTables(dbname, tablePattern, tableType);

    if (isDefaultCatalog(catName)) {
      if (tableType == TableType.MANAGED_TABLE || tableType == TableType.EXTERNAL_TABLE) {
        // May need to merge with list of temp tables
        dbname = dbname.toLowerCase();
        tablePattern = tablePattern.toLowerCase();
        Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables =
            getTempTablesForDatabase(dbname, tablePattern);
        if (tables == null || tables.size() == 0) {
          return tableNames;
        }
        tablePattern = tablePattern.replaceAll("(?<!\\.)\\*", ".*");
        Pattern pattern = Pattern.compile(tablePattern);
        Matcher matcher = pattern.matcher("");
        Set<String> combinedTableNames = new HashSet<String>();
        combinedTableNames.addAll(tableNames);
        for (Map.Entry<String, org.apache.hadoop.hive.ql.metadata.Table> tableData : tables.entrySet()) {
          matcher.reset(tableData.getKey());
          if (matcher.matches()) {
            if (tableData.getValue().getTableType() == tableType) {
              // If tableType is the same that we are requesting,
              // add table the the list
              combinedTableNames.add(tableData.getKey());
            } else {
              // If tableType is not the same that we are requesting,
              // remove it in case it was added before, as temp table
              // overrides original table
              combinedTableNames.remove(tableData.getKey());
            }
          }
        }
        // Combine/sort temp and normal table results
        tableNames = new ArrayList<>(combinedTableNames);
        Collections.sort(tableNames);
      }
    }

    return tableNames;
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec) throws TException {
    if (isDefaultCatalog(catName) && projectionsSpec == null) {
      // TODO: What if projectionsSpec != null
      List<Table> tables = new ArrayList<>();
      for (String tableName: tableNames) {
        tables.add(getTable(catName, dbName, tableName));
      }
      return tables;
    } else {
      return delegate.getTables(catName, dbName, tableNames, projectionsSpec);
    }
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) throws TException {
    if (isDefaultCatalog(catName)) {
      Table tempTable = getTempTable(dbName, tableName);
      if (tempTable != null) {
        return true;
      }
    }

    return delegate.tableExists(catName, dbName, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String catName, String dbName, String tableName) throws TException {
    if (isDefaultCatalog(catName)) {
      Table tempTable = getTempTable(dbName, tableName);
      if (tempTable != null) {
        return deepCopyFieldSchemas(tempTable.getSd().getCols());
      }
    }

    return delegate.getSchema(catName, dbName, tableName);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine, String validWriteIdList) throws TException {
    if (isDefaultCatalog(catName) && getTempTable(dbName, tableName) != null) {
      return getTempTableColumnStats(dbName, tableName, colNames);
    }

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
      List<ColumnStatisticsObj> newColStats =
          delegate.getTableColumnStatistics(catName, dbName, tableName, colStatsMissing,
              engine, validWriteIdList);
      // 4) Populate the cache
      MetaStoreClientCacheUtils.loadTableColumnStatisticsCache(cache, newColStats, catName, dbName,
          tableName, engine, validWriteIdList, null);
      // 5) Sort result (in case there is any assumption) and return
      List<ColumnStatisticsObj> result =
          MetaStoreClientCacheUtils.computeTableColumnStatisticsFinal(colNames, colStats, newColStats);
      return result;
    }

    return delegate
        .getTableColumnStatistics(catName, dbName, tableName, colNames, engine, validWriteIdList);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName,
      String tableName, List<String> partialPvals, int maxParts, String userName,
      List<String> groupNames) throws TException {
    if (isDefaultCatalog(catName)) {
      Table tmpTable = getTempTable(dbName, tableName);
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        List<Partition> parts =
            tt.listPartitionsByPartitionValsWithAuthInfo(partialPvals, userName, groupNames);
        return getPartitionsForMaxParts(parts, maxParts);
      }
    }

    // TODO should we add capabilities here as well as it returns Partition objects
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO,
          catName, dbName, tableName, partialPvals, maxParts, userName, groupNames);
      List<Partition> v = (List<Partition>) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals, maxParts,
            userName, groupNames);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoInternal, dbName={}, tblName={}, partVals={}",
            dbName, tableName, partialPvals);
      }
      return v;
    }
    return delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals, maxParts,
        userName, groupNames);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      int maxParts, String userName, List<String> groupNames)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table tmpTable = getTempTable(dbName, tableName);
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        List<Partition> parts = tt.listPartitionsWithAuthInfo(userName, groupNames);
        return getPartitionsForMaxParts(parts, maxParts);
      }
    }

    // TODO should we add capabilities here as well as it returns Partition objects
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO_ALL,
          catName, dbName, tableName, maxParts, userName, groupNames);
      List<Partition> v = (List<Partition>) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoInternalAll, dbName={}, tblName={}",
            dbName, tableName);
      }
      return v;
    }
    return delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(
      GetPartitionsPsWithAuthRequest req) throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table tmpTable = getTempTable(req.getDbName(), req.getTblName());
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        List<Partition> partitions = tt.listPartitionsWithAuthInfo(req.getUserName(), req.getGroupNames());
        GetPartitionsPsWithAuthResponse response = new GetPartitionsPsWithAuthResponse();
        response.setPartitions(getPartitionsForMaxParts(partitions, req.getMaxParts()));
        return response;
      }
    }

    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }

    req.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(req.getMaxParts()));

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_AUTH_INFO_REQ, req);
      GetPartitionsPsWithAuthResponse v = (GetPartitionsPsWithAuthResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.listPartitionsWithAuthInfoRequest(req);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionsWithAuthInfoRequestInternal, dbName={}, tblName={}, partVals={}",
            req.getDbName(), req.getTblName(), req.getPartVals());
      }
      return v;
    }
    return delegate.listPartitionsWithAuthInfoRequest(req);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table tmpTable = getTempTable(dbName, tableName);
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        List<Partition> partitions = tt.listPartitions();
        List<String> result = new ArrayList<>();
        int lastIndex = (maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts;
        for (int i = 0; i < lastIndex; i++) {
          result.add(makePartName(tmpTable.getPartitionKeys(), partitions.get(i).getValues()));
        }
        Collections.sort(result);
        return result;
      }
    }

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_ALL, catName, dbName, tableName, maxParts);
      List<String> v = (List<String>) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.listPartitionNames(catName, dbName, tableName, maxParts);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionNamesInternalAll, dbName={}, tableName={}",
            dbName, tableName);
      }
      return v;
    }
    return delegate.listPartitionNames(catName, dbName, tableName, maxParts);
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table tmpTable = getTempTable(req.getDbName(), req.getTblName());
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        List<Partition> partitions = tt.getPartitionsByPartitionVals(req.getPartValues());
        short maxParts = req.getMaxParts();
        int lastIndex = (maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts;

        List<String> result = new ArrayList<>();
        for (int i = 0; i < lastIndex; i++) {
          result.add(makePartName(tmpTable.getPartitionKeys(), partitions.get(i).getValues()));
        }
        Collections.sort(result);
        GetPartitionNamesPsResponse response = new GetPartitionNamesPsResponse();
        response.setNames(result);
        return response;
      }
    }

    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.LIST_PARTITIONS_REQ, req);
      GetPartitionNamesPsResponse v = (GetPartitionNamesPsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.listPartitionNamesRequest(req);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=listPartitionNamesRequestInternal, dbName={}, tblName={}, partValues={}",
            req.getDbName(), req.getTblName(), req.getPartValues());
      }
      return v;
    }
    return delegate.listPartitionNamesRequest(req);
  }

  @Override
  public boolean listPartitionsByExpr(PartitionsByExprRequest req, List<Partition> result) throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table tmpTable = getTempTable(req.getDbName(), req.getTblName());
      if (tmpTable != null) {
        assert result != null;
        result.addAll(getPartitionsForMaxParts(getPartitionedTempTable(tmpTable).listPartitionsByFilter(
            generateJDOFilter(tmpTable, req.getExpr(), req.getDefaultPartitionName())), req.getMaxParts()));
        return result.isEmpty();
      }
    }

    if (!req.isSetValidWriteIdList()) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_EXPR, req);
      PartitionsWrapper v = (PartitionsWrapper) queryCache.get(cacheKey);
      if (v == null) {
        List<Partition> parts = new ArrayList<>();
        boolean hasUnknownPart = delegate.listPartitionsByExpr(req, parts);
        v = new MetaStoreClientCacheUtils.PartitionsWrapper(parts, hasUnknownPart);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPartitionsByExprInternal, dbName={}, tblName={}",
            req.getDbName(), req.getTblName());
      }

      result.addAll(v.partitions);
      return v.hasUnknownPartition;
    }
    return delegate.listPartitionsByExpr(req, result);
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table tmpTable = getTempTable(req.getDbName(), req.getTblName());
      if (tmpTable != null) {
        assert result != null;
        result.addAll(
            MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor(tmpTable,
                getPartitionsForMaxParts(getPartitionedTempTable(tmpTable).listPartitionsByFilter(
                    generateJDOFilter(tmpTable, req.getExpr(), req.getDefaultPartitionName())),
                    req.getMaxParts())));
        return result.isEmpty();
      }
    }

    if (!req.isSetValidWriteIdList()) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }

    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_SPEC_BY_EXPR, req);
      PartitionSpecsWrapper v = (PartitionSpecsWrapper) queryCache.get(cacheKey);
      if (v == null) {
        List<PartitionSpec> parts = new ArrayList<>();
        boolean hasUnknownPart = delegate.listPartitionsSpecByExpr(req, parts);
        v = new PartitionSpecsWrapper(parts, hasUnknownPart);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPartitionsSpecByExprInternal, dbName={}, tblName={}",
            req.getDbName(), req.getTblName());
      }
      result.addAll(v.partitionSpecs);
      return v.hasUnknownPartition;
    }
    return delegate.listPartitionsSpecByExpr(req, result);
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req)
      throws TException {
    String[] parsedNames = MetaStoreUtils.parseDbName(req.getDb_name(), conf);
    if (isDefaultCatalog(parsedNames[0])) {
      Table tmpTable = getTempTable(req.getDb_name(), req.getTbl_name());
      if (tmpTable != null) {
        TempTable tt = getPartitionedTempTable(tmpTable);
        GetPartitionsByNamesResult result = new GetPartitionsByNamesResult();
        result.setPartitions(
            HiveMetaStoreClientUtils.deepCopyPartitions(tt.getPartitionsByNames(req.getNames())));

        return result;
      }
    }

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
      GetPartitionsByNamesResult r = delegate.getPartitionsByNames(newRqst);
      // 4) Populate the cache
      List<Partition> newPartitions =
          MetaStoreClientCacheUtils.loadPartitionsByNamesCache(cache, r, req, null);
      // 5) Sort result (in case there is any assumption) and return
      return MetaStoreClientCacheUtils.computePartitionsByNamesFinal(req, partitions, newPartitions);
    }
    return delegate.getPartitionsByNames(req);
  }

  /**
   * Methods for temporary table managements
   */
  @Override
  public void createTable(CreateTableRequest request) throws TException {
    Table tbl = request.getTable();
    if (tbl.isTemporary()) {
      createTempTable(tbl);
    } else {
      delegate.createTable(request);
    }
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
      List<String> tableTypes) throws TException {
    List<TableMeta> tableMetas = delegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);

    if (isDefaultCatalog(catName)) {
      Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> tmpTables =
          getTempTables("dbPatterns='" + dbPatterns + "' tablePatterns='" + tablePatterns + "'");
      if (tmpTables.isEmpty()) {
        return tableMetas;
      }

      List<Matcher> dbPatternList = new ArrayList<>();
      for (String element : dbPatterns.split("\\|")) {
        dbPatternList.add(Pattern.compile(element.replace("*", ".*")).matcher(""));
      }
      List<Matcher> tblPatternList = new ArrayList<>();
      for (String element : tablePatterns.split("\\|")) {
        tblPatternList.add(Pattern.compile(element.replace("*", ".*")).matcher(""));
      }
      for (Map.Entry<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> outer : tmpTables.entrySet()) {
        if (!matchesAny(outer.getKey(), dbPatternList)) {
          continue;
        }
        for (Map.Entry<String, org.apache.hadoop.hive.ql.metadata.Table> inner : outer.getValue().entrySet()) {
          org.apache.hadoop.hive.ql.metadata.Table table = inner.getValue();
          String tableName = table.getTableName();
          String typeString = table.getTableType().name();
          if (tableTypes != null && !tableTypes.contains(typeString)) {
            continue;
          }
          if (!matchesAny(inner.getKey(), tblPatternList)) {
            continue;
          }
          TableMeta tableMeta = new TableMeta(table.getDbName(), tableName, typeString);
          tableMeta.setComments(table.getProperty("comment"));
          tableMetas.add(tableMeta);
        }
      }
    }

    return tableMetas;
  }

  private boolean matchesAny(String string, List<Matcher> matchers) {
    for (Matcher matcher : matchers) {
      if (matcher.reset(string).matches()) {
        return true;
      }
    }
    return matchers.isEmpty();
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws TException {
    List<String> tableNames = delegate.getAllTables(catName, dbName);

    if (isDefaultCatalog(catName)) {
      Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, "?");
      if (tables != null && tables.size() != 0) {
        Set<String> tempTableNames = tables.keySet();

        Set<String> allTableNames = new HashSet<String>(tableNames.size() + tempTableNames.size());
        allTableNames.addAll(tableNames);
        allTableNames.addAll(tempTableNames);
        tableNames = new ArrayList<String>(allTableNames);
        Collections.sort(tableNames);
      }
    }

    return tableNames;
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge)
      throws TException {
    if (table.isTemporary()) {
      dropTempTable(table, deleteData, ifPurge);
    } else {
      delegate.dropTable(table, deleteData, ignoreUnknownTable, ifPurge);
    }
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, String ref,
      List<String> partNames, String validWriteIds, long writeId, boolean deleteData,
      EnvironmentContext context) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        truncateTempTable(table);
        return;
      }
    }
    delegate.truncateTable(catName, dbName, tableName, ref, partNames, validWriteIds, writeId,
        deleteData, context);
  }

  @Override
  public void alter_table(String catName, String dbName, String tbl_name, Table new_tbl,
      EnvironmentContext envContext, String validWriteIds) throws TException {
    if (isDefaultCatalog(catName)) {
      Table old_tbl = getTempTable(dbName, tbl_name);
      if (old_tbl != null) {
        // actually temp table does not support partitions, cascade is not applicable here
        alterTempTable(dbName, tbl_name, old_tbl, new_tbl);
        return;
      }
    }
    delegate.alter_table(catName, dbName, tbl_name, new_tbl, envContext, validWriteIds);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws TException {
    // If caller is looking for temp table, handle here. Otherwise pass on to underlying client.
    if (hiveObject.getObjectType() == HiveObjectType.TABLE && isDefaultCatalog(hiveObject.getCatName())) {
      Table table = getTempTable(hiveObject.getDbName(), hiveObject.getObjectName());
      if (table != null) {
        return HiveMetaStoreClientUtils.deepCopy(table.getPrivileges());
      }
    }

    return delegate.get_privilege_set(hiveObject, userName, groupNames);
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
    // This does not support catalog.
    if (request.getColStatsSize() == 1) {
      ColumnStatistics colStats = request.getColStatsIterator().next();
      ColumnStatisticsDesc desc = colStats.getStatsDesc();
      String dbName = desc.getDbName().toLowerCase();
      String tableName = desc.getTableName().toLowerCase();
      if (getTempTable(dbName, tableName) != null) {
        return updateTempTableColumnStats(dbName, tableName, colStats);
      }
    }
    return delegate.setPartitionColumnStatistics(request);
  }

  @Override
  public boolean deleteColumnStatistics(DeleteColumnStatisticsRequest req) throws TException {
    if (isDefaultCatalog(req.getCat_name())) {
      String dbName = req.getDb_name();
      String tableName = req.getTbl_name();
      Table table;
      if ((table = getTempTable(dbName, tableName)) != null) {
        List<String> colNames = req.getCol_names();
        if (table.getPartitionKeysSize() == 0) {
          if (colNames == null || colNames.isEmpty()) {
            colNames = table.getSd().getCols().stream().map(FieldSchema::getName)
                .collect(Collectors.toList());
          }
          for (String colName : colNames) {
            deleteTempTableColumnStats(dbName, tableName, colName);
          }
        } else {
          throw new UnsupportedOperationException("Not implemented yet");
        }
        return true;
      }
    }
    return delegate.deleteColumnStatistics(req);
  }

  private void createTempTable(Table tbl) throws TException {

    boolean isVirtualTable = tbl.getTableName().startsWith(SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX);

    SessionState ss = SessionState.get();
    if (ss == null) {
      throw new MetaException("No current SessionState, cannot create temporary table: "
          + Warehouse.getQualifiedName(tbl));
    }

    // We may not own the table object, create a copy
    tbl = deepCopyAndLowerCaseTable(tbl);

    String dbName = tbl.getDbName();
    String tblName = tbl.getTableName();
    Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, tblName);
    if (tables != null && tables.containsKey(tblName)) {
      throw new MetaException(
          "Temporary table " + StatsUtils.getFullyQualifiedTableName(dbName, tblName) + " already exists");
    }

    // Create temp table directory
    Warehouse wh = getWh();
    if (tbl.getSd().getLocation() == null) {
      // Temp tables that do not go through SemanticAnalyzer may not have location set - do it here.
      // For example export of acid tables generates a query plan that creates a temp table.
      tbl.getSd().setLocation(SessionState.generateTempTableLocation(conf));
    }
    Path tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
    if (tblPath == null) {
      throw new MetaException("Temp table path not set for " + tbl.getTableName());
    } else {
      if (!wh.isDir(tblPath)) {
        if (!wh.mkdirs(tblPath)) {
          throw new MetaException(tblPath
              + " is not a directory or unable to create one");
        }
      }
      // Make sure location string is in proper format
      tbl.getSd().setLocation(tblPath.toString());
    }

    // Add temp table info to current session
    org.apache.hadoop.hive.ql.metadata.Table tTable = new org.apache.hadoop.hive.ql.metadata.Table(tbl);
    if (!isVirtualTable) {
      StatsSetupConst.setStatsStateForCreateTable(tbl.getParameters(), getColumnNamesForTable(tbl),
          StatsSetupConst.TRUE);
    }
    if (tables == null) {
      tables = new HashMap<String, org.apache.hadoop.hive.ql.metadata.Table>();
      ss.getTempTables().put(dbName, tables);
    }
    tables.put(tblName, tTable);
    createPartitionedTempTable(tbl);
  }

  private Table getTempTable(String dbName, String tableName)
      throws MetaException {
    String parsedDbName = MetaStoreUtils.parseDbName(dbName, conf)[1];
    if (parsedDbName == null) {
      throw new MetaException("Db name cannot be null");
    }
    if (tableName == null) {
      throw new MetaException("Table name cannot be null");
    }
    Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables =
        getTempTablesForDatabase(parsedDbName.toLowerCase(), tableName.toLowerCase());
    if (tables != null) {
      org.apache.hadoop.hive.ql.metadata.Table table = tables.get(tableName.toLowerCase());
      if (table != null) {
        return table.getTTable();
      }
    }
    return null;
  }

  private void alterTempTable(String dbname, String tbl_name, Table oldt, Table newt) throws TException {
    dbname = dbname.toLowerCase();
    tbl_name = tbl_name.toLowerCase();
    boolean shouldDeleteColStats = false;

    // Disallow changing temp table location
    if (!newt.getSd().getLocation().equals(oldt.getSd().getLocation())) {
      throw new MetaException("Temp table location cannot be changed");
    }

    Table newtCopy = deepCopyAndLowerCaseTable(newt);
    org.apache.hadoop.hive.ql.metadata.Table newTable = new org.apache.hadoop.hive.ql.metadata.Table(newtCopy);
    String newDbName = newTable.getDbName();
    String newTableName = newTable.getTableName();
    if (!newDbName.equals(oldt.getDbName()) || !newTableName.equals(oldt.getTableName())) {
      // Table was renamed.

      // Do not allow temp table rename if the new name already exists as a temp table
      if (getTempTable(newDbName, newTableName) != null) {
        throw new MetaException("Cannot rename temporary table to " + newTableName
            + " - temporary table already exists with the same name");
      }

      // Remove old temp table entry, and add new entry to list of temp tables.
      // Note that for temp tables there is no need to rename directories
      Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbname, tbl_name);
      if (tables == null || tables.remove(tbl_name) == null) {
        throw new MetaException("Could not find temp table entry for " + dbname + "." + tbl_name);
      }
      shouldDeleteColStats = true;

      tables = getTempTablesForDatabase(newDbName, tbl_name);
      if (tables == null) {
        tables = new HashMap<String, org.apache.hadoop.hive.ql.metadata.Table>();
        SessionState.get().getTempTables().put(newDbName, tables);
      }
      tables.put(newTableName, newTable);
    } else {
      if (haveTableColumnsChanged(oldt, newt)) {
        shouldDeleteColStats = true;
      }
      getTempTablesForDatabase(dbname, tbl_name).put(tbl_name, newTable);
    }

    if (shouldDeleteColStats) {
      try {
        deleteTempTableColumnStatsForTable(dbname, tbl_name);
      } catch (NoSuchObjectException err){
        // No stats to delete, forgivable error.
        LOG.info(err.getMessage());
      }
    }
  }

  private static boolean haveTableColumnsChanged(Table oldt,
      Table newt) {
    List<FieldSchema> oldCols = oldt.getSd().getCols();
    List<FieldSchema> newCols = newt.getSd().getCols();
    if (oldCols.size() != newCols.size()) {
      return true;
    }
    Iterator<FieldSchema> oldColsIter = oldCols.iterator();
    Iterator<FieldSchema> newColsIter = newCols.iterator();
    while (oldColsIter.hasNext()) {
      // Don't use FieldSchema.equals() since it also compares comments,
      // which is unnecessary for this method.
      if (!fieldSchemaEqualsIgnoreComment(oldColsIter.next(), newColsIter.next())) {
        return true;
      }
    }
    return false;
  }

  private static boolean fieldSchemaEqualsIgnoreComment(FieldSchema left, FieldSchema right) {
    // Just check name/type for equality, don't compare comment
    if (!left.getName().equals(right.getName())) {
      return true;
    }
    if (!left.getType().equals(right.getType())) {
      return true;
    }
    return false;
  }

  private boolean needToUpdateStats(Map<String,String> props) {
    if (null == props) {
      return false;
    }
    boolean statsPresent = false;
    for (String stat : StatsSetupConst.SUPPORTED_STATS) {
      String statVal = props.get(stat);
      if (statVal != null && Long.parseLong(statVal) > 0) {
        statsPresent = true;
        //In the case of truncate table, we set the stats to be 0.
        props.put(stat, "0");
      }
    }
    //first set basic stats to true
    StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
    //then invalidate column stats
    StatsSetupConst.clearColumnStatsState(props);
    return statsPresent;
  }

  private void truncateTempTable(Table table) throws TException {
    boolean isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
    try {
      // this is not transactional
      Path location = new Path(table.getSd().getLocation());

      FileSystem fs = location.getFileSystem(conf);
      HadoopShims.HdfsEncryptionShim shim
          = ShimLoader.getHadoopShims().createHdfsEncryptionShim(fs, conf);
      if (!shim.isPathEncrypted(location)) {
        HdfsUtils.HadoopFileStatus status = HdfsUtils.HadoopFileStatus.createInstance(conf, fs, location);
        FileStatus targetStatus = fs.getFileStatus(location);
        String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
        org.apache.hadoop.hive.metastore.utils.FileUtils.deleteDir(fs, location, isSkipTrash, conf);
        fs.mkdirs(location);
        HdfsUtils.setFullFileStatus(conf, status, targetGroup, fs, location, false);
      } else {
        FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
        if ((statuses != null) && (statuses.length > 0)) {
          boolean success = Hive.trashFiles(fs, statuses, conf, isSkipTrash);
          if (!success) {
            throw new HiveException("Error in deleting the contents of " + location.toString());
          }
        }
      }

      if (needToUpdateStats(table.getParameters())) {
        alterTempTable(table.getDbName(), table.getTableName(), table, table);
      }
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  private void dropTempTable(Table table, boolean deleteData,
      boolean ifPurge) throws TException, UnsupportedOperationException {
    try {
      deleteTempTableColumnStatsForTable(table.getDbName(), table.getTableName());
    } catch (NoSuchObjectException err){
      // No stats to delete, forgivable error.
      LOG.info(err.getMessage());
    }

    String dbName = table.getDbName().toLowerCase();
    String tableName = table.getTableName().toLowerCase();

    // Determine the temp table path
    Path tablePath = null;
    String pathStr = table.getSd().getLocation();
    if (pathStr != null) {
      try {
        tablePath = new Path(table.getSd().getLocation());
        if (deleteData && !isExternalTable(table) && !getWh().isWritable(tablePath.getParent())) {
          throw new MetaException("Table metadata not deleted since " + tablePath.getParent() +
              " is not writable by " + SecurityUtils.getUser());
        }
      } catch (IOException err) {
        MetaException metaException =
            new MetaException("Error checking temp table path for " + table.getTableName());
        metaException.initCause(err);
        throw metaException;
      }
    }

    // Remove table entry from SessionState
    Map<String, org.apache.hadoop.hive.ql.metadata.Table> tables = getTempTablesForDatabase(dbName, tableName);
    if (tables == null || tables.remove(tableName) == null) {
      throw new MetaException(
          "Could not find temp table entry for " + StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    }
    removePartitionedTempTable(table);

    // Delete table data
    if (deleteData && !isExternalTable(table)) {
      try {
        getWh().deleteDir(tablePath, ifPurge, false);
      } catch (Exception err) {
        LOG.error("Failed to delete temp table directory: " + tablePath, err);
        // Forgive error
      }
    }
  }

  private Table deepCopyAndLowerCaseTable(
      Table tbl) {
    Table newCopy = HiveMetaStoreClientUtils.deepCopy(tbl);
    newCopy.setDbName(newCopy.getDbName().toLowerCase());
    newCopy.setTableName(newCopy.getTableName().toLowerCase());
    return newCopy;
  }

  /**
   * @param dbName actual database name
   * @param tblName actual table name or search pattern (for error message)
   */
  public static Map<String, org.apache.hadoop.hive.ql.metadata.Table> getTempTablesForDatabase(String dbName,
      String tblName) {
    return getTempTables(Warehouse.getQualifiedName(dbName, tblName)).get(dbName);
  }

  private static Map<String, Map<String, org.apache.hadoop.hive.ql.metadata.Table>> getTempTables(String msg) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables for " + msg);
      return Collections.emptyMap();
    }
    return ss.getTempTables();
  }

  private Map<String, ColumnStatisticsObj> getTempTableColumnStatsForTable(String dbName,
      String tableName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables for " +
          Warehouse.getQualifiedName(dbName, tableName));
      return null;
    }
    String lookupName = StatsUtils.getFullyQualifiedTableName(dbName.toLowerCase(),
        tableName.toLowerCase());
    return ss.getTempTableColStats().get(lookupName);
  }

  private List<ColumnStatisticsObj> getTempTableColumnStats(String dbName, String tableName,
      List<String> colNames) {
    Map<String, ColumnStatisticsObj> tableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    List<ColumnStatisticsObj> retval = new ArrayList<ColumnStatisticsObj>();

    if (tableColStats != null) {
      for (String colName : colNames) {
        colName = colName.toLowerCase();
        if (tableColStats.containsKey(colName)) {
          retval.add(new ColumnStatisticsObj(tableColStats.get(colName)));
        }
      }
    }
    return retval;
  }

  private boolean updateTempTableColumnStats(String dbName, String tableName,
      ColumnStatistics colStats) throws MetaException {
    SessionState ss = SessionState.get();
    if (ss == null) {
      throw new MetaException("No current SessionState, cannot update temporary table stats for "
          + StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    }
    Map<String, ColumnStatisticsObj> ssTableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (ssTableColStats == null) {
      // Add new entry for this table
      ssTableColStats = new HashMap<String, ColumnStatisticsObj>();
      ss.getTempTableColStats().put(
          StatsUtils.getFullyQualifiedTableName(dbName, tableName),
          ssTableColStats);
    }
    mergeColumnStats(ssTableColStats, colStats);

    List<String> colNames = new ArrayList<>();
    for (ColumnStatisticsObj obj : colStats.getStatsObj()) {
      colNames.add(obj.getColName());
    }
    Table table = getTempTable(dbName, tableName);
    StatsSetupConst.setColumnStatsState(table.getParameters(), colNames);
    return true;
  }

  private static void mergeColumnStats(Map<String, ColumnStatisticsObj> oldStats,
      ColumnStatistics newStats) {
    List<ColumnStatisticsObj> newColList = newStats.getStatsObj();
    if (newColList != null) {
      for (ColumnStatisticsObj colStat : newColList) {
        // This is admittedly a bit simple, StatsObjectConverter seems to allow
        // old stats attributes to be kept if the new values do not overwrite them.
        oldStats.put(colStat.getColName().toLowerCase(), colStat);
      }
    }
  }

  private boolean deleteTempTableColumnStatsForTable(String dbName, String tableName)
      throws NoSuchObjectException {
    Map<String, ColumnStatisticsObj> deletedEntry =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (deletedEntry != null) {
      SessionState.get().getTempTableColStats().remove(
          StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    } else {
      throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
          " temp table=" + tableName);
    }
    return true;
  }

  private boolean deleteTempTableColumnStats(String dbName, String tableName, String columnName)
      throws NoSuchObjectException {
    ColumnStatisticsObj deletedEntry = null;
    Map<String, ColumnStatisticsObj> ssTableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (ssTableColStats != null) {
      deletedEntry = ssTableColStats.remove(columnName.toLowerCase());
    }
    if (deletedEntry == null) {
      throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
          " temp table=" + tableName);
    }
    return true;
  }

  /**
   * Hive.loadPartition() calls this.
   * @param partition
   *          The partition to add
   * @return the partition added
   */
  @Override
  public Partition add_partition(Partition partition) throws TException {
    // First try temp table
    if (partition == null) {
      throw new MetaException("Partition cannot be null");
    }

    if (isDefaultCatalog(partition.getCatName())) {
      Table table = getTempTable(partition.getDbName(), partition.getTableName());
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        checkPartitionProperties(partition);
        Path partitionLocation = getPartitionLocation(table, partition, false);
        Partition result = tt.addPartition(HiveMetaStoreClientUtils.deepCopy(partition));
        createAndSetLocationForAddedPartition(result, partitionLocation);
        return result;
      }
    }
    return delegate.add_partition(partition);
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    if (partitionSpec == null) {
      throw new MetaException("PartitionSpec cannot be null.");
    }
    if (partitionSpec.size() == 0) {
      return 0;
    }

    if (isDefaultCatalog(partitionSpec.getCatName())) {
      Table table = getTempTable(partitionSpec.getDbName(), partitionSpec.getTableName());
      if (table != null) {
        assertTempTablePartitioned(table);
        PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpec.getPartitionIterator();
        List<Partition> partitionsToAdd = new ArrayList<>(partitionSpec.size());
        while (partitionIterator.hasNext()) {
          partitionsToAdd.add(partitionIterator.next());
        }

        List<Partition> addedPartitions = addPartitionsToTempTable(partitionsToAdd, partitionSpec.getDbName(),
            partitionSpec.getTableName(), false);
        if (addedPartitions != null) {
          return addedPartitions.size();
        }
      }
    }
    return delegate.add_partitions_pspec(partitionSpec);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws TException {
    if (partitions == null || partitions.contains(null)) {
      throw new MetaException("Partitions cannot be null");
    }

    if (partitions.isEmpty()) {
      return needResults ? new ArrayList<>() : null;
    }

    if (isDefaultCatalog(partitions.get(0).getCatName())) {
      if (getTempTable(partitions.get(0).getDbName(), partitions.get(0).getTableName()) != null) {
        List<Partition> addedPartitions = addPartitionsToTempTable(partitions, null, null, ifNotExists);
        if (addedPartitions != null) {
          return needResults ? addedPartitions : null;
        }
      }
    }

    return delegate.add_partitions(partitions, ifNotExists, needResults);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        Partition partition = tt.getPartition(name);
        if (partition == null) {
          throw new NoSuchObjectException("Partition with name " + name + " for table " + tblName +
              " in database " + dbName + " is not found.");
        }

        return HiveMetaStoreClientUtils.deepCopy(partition);
      }
    }
    return delegate.getPartition(catName, dbName, tblName, name);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        Partition partition = tt.getPartition(partVals);
        if (partition == null) {
          throw new NoSuchObjectException("Partition with partition values " +
              (partVals != null ? Arrays.toString(partVals.toArray()) : "null")+
              " for table " + tblName + " in database " + dbName + " is not found.");
        }
        return HiveMetaStoreClientUtils.deepCopy(partition);
      }
    }
    return delegate.getPartition(catName, dbName, tblName, partVals);

  }

  @Override
  public GetPartitionsResponse getPartitionsWithSpecs(GetPartitionsRequest request) throws TException {
    if (isDefaultCatalog(request.getCatName())) {
      Table table = getTempTable(request.getDbName(), request.getTblName());
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        return tt.getPartitionsWithSpecs(request);
      }
    }

    return delegate.getPartitionsWithSpecs(request);
  }

  @Override
  public List<String> listPartitionNames(PartitionsByExprRequest req) throws TException {
    if (isDefaultCatalog(req.getCatName())) {
      Table table = getTempTable(req.getDbName(), req.getTblName());
      if (table != null) {
        List<Partition> partitionList = getPartitionedTempTable(table).listPartitions();
        if (partitionList.isEmpty()) {
          return Collections.emptyList();
        }

        byte[] expr = req.getExpr();
        boolean isEmptyFilter = (expr == null || (expr.length == 1 && expr[0] == -1));
        if (!isEmptyFilter) {
          partitionList = getPartitionedTempTable(table).listPartitionsByFilter(
              generateJDOFilter(table, expr, req.getDefaultPartitionName()));
        }

        List<String> results = new ArrayList<>();
        Collections.sort(partitionList, new PartitionNamesComparator(table, req));
        short maxParts = req.getMaxParts();
        int numPartitions = maxParts < 0 || maxParts > partitionList.size() ? partitionList.size() : maxParts;
        for(int i = 0; i < numPartitions; i++) {
          results.add(Warehouse.makePartName(table.getPartitionKeys(), partitionList.get(i).getValues()));
        }
        return results;
      }
    }
    return delegate.listPartitionNames(req);
  }

  private static final class PartitionNamesComparator implements java.util.Comparator<Partition> {
    private Table table;
    private PartitionsByExprRequest req;
    PartitionNamesComparator(Table table, PartitionsByExprRequest req) {
      this.table = table;
      this.req = req;
    }
    @Override
    public int compare(Partition o1, Partition o2) {
      List<Object[]> orders = MetaStoreUtils.makeOrderSpecs(req.getOrder());
      for (Object[] order : orders) {
        int partKeyIndex = (int) order[0];
        boolean isAsc = "asc".equalsIgnoreCase((String)order[1]);
        String partVal1 = o1.getValues().get(partKeyIndex), partVal2 = o2.getValues().get(partKeyIndex);
        int val = partVal1.compareTo(partVal2);
        if (val == 0) {
          continue;
        } else if (partVal1.equals(req.getDefaultPartitionName())) {
          return isAsc ? 1 : -1;
        } else if (partVal2.equals(req.getDefaultPartitionName())) {
          return isAsc ? -1 : 1;
        } else {
          String type = table.getPartitionKeys().get(partKeyIndex).getType();
          if (org.apache.hadoop.hive.metastore.ColumnType.IntegralTypes.contains(type)) {
            val = (Double.valueOf(partVal1) - Double.valueOf(partVal2)) > 0 ? 1 : -1;
          }
          return isAsc ? val : - val;
        }
      }

      try {
        return Warehouse.makePartName(table.getPartitionKeys(), o1.getValues()).compareTo(
            Warehouse.makePartName(table.getPartitionKeys(), o2.getValues()));
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<Partition> listPartitions(String catName, String dbName, String tblName, int maxParts)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        return getPartitionsForMaxParts(tt.listPartitions(), maxParts);
      }
    }
    return delegate.listPartitions(catName, dbName, tblName, maxParts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String dbName, String tblName, List<String> partVals,
      int maxParts) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        return getPartitionsForMaxParts(tt.getPartitionsByPartitionVals(partVals), maxParts);
      }
    }
    return delegate.listPartitions(catName, dbName, tblName, partVals, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        return getPartitionSpecProxy(table, tt.listPartitions(), maxParts);
      }
    }
    return delegate.listPartitionSpecs(catName, dbName, tableName, maxParts);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        Partition partition = tt.getPartitionWithAuthInfo(pvals, userName, groupNames);
        if (partition == null) {
          throw new NoSuchObjectException("Partition with partition values " +
              (pvals != null ? Arrays.toString(pvals.toArray()) : "null") +
              " for table " + tableName + " in database " + dbName + " and for user " +
              userName + " and group names " + (groupNames != null ? Arrays.toString(groupNames.toArray()) : "null") +
              " is not found.");
        }
        return HiveMetaStoreClientUtils.deepCopy(partition);
      }
    }
    return delegate.getPartitionWithAuthInfo(catName, dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tblName, List<String> partVals,
      PartitionDropOptions options) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        if (partVals == null || partVals.isEmpty() || partVals.contains(null)) {
          throw new MetaException("Partition values cannot be null, empty or contain null values");
        }
        TempTable tt = getPartitionedTempTable(table);
        if (tt == null) {
          throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
        }
        Partition droppedPartition = tt.dropPartition(partVals);
        boolean result = droppedPartition != null ? true : false;
        boolean purgeData = options != null ? options.purgeData : true;
        boolean deleteData = options != null ? options.deleteData : true;
        if (deleteData && !tt.isExternal()) {
          result &= deletePartitionLocation(droppedPartition, purgeData);
        }

        return result;
      }
    }
    return delegate.dropPartition(catName, dbName, tblName, partVals, options);
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, String partitionName,
      boolean deleteData) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        Partition droppedPartition = tt.dropPartition(partitionName);
        boolean result = droppedPartition != null ? true : false;
        if (deleteData && !tt.isExternal()) {
          result &= deletePartitionLocation(droppedPartition, true);
        }
        return result;
      }
    }
    return delegate.dropPartition(catName, dbName, tableName, partitionName, deleteData);
  }

  @Override
  public List<Partition> dropPartitions(TableName tableName,
      RequestPartsSpec partsSpec, PartitionDropOptions options, EnvironmentContext context)
      throws TException {
    if (isDefaultCatalog(tableName.getCat())) {
      Table table = getTempTable(tableName.getDb(), tableName.getTable());
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
            List<List<String>> partValues = new ArrayList<>();
        if (partsSpec.isSetExprs()) {
          List<DropPartitionsExpr> exprs = partsSpec.getExprs();
          for (DropPartitionsExpr expr : exprs) {
            String filter = generateJDOFilter(table, expr.getExpr(),
                conf.get(HiveConf.ConfVars.DEFAULT_PARTITION_NAME.varname));
            List<Partition> partitions = tt.listPartitionsByFilter(filter);
            for (Partition p : partitions) {
              partValues.add(p.getValues());
            }
          }
        } else if (partsSpec.isSetNames()) {
          List<String> partNames = partsSpec.getNames();
          for (String partName : partNames) {
            partValues.add(CachedStore.partNameToVals(partName));
          }
        }
        boolean purgeData = options != null ? options.purgeData : true;
        boolean deleteData = options != null ? options.deleteData : true;
        List<Partition> result = new ArrayList<>();
        for (List<String> partValue : partValues) {
          Partition droppedPartition = tt.dropPartition(partValue);
          if (droppedPartition != null) {
            result.add(droppedPartition);
            if (deleteData && !tt.isExternal()) {
              deletePartitionLocation(droppedPartition, purgeData);
            }
          }
        }
        return result;
      }
    }

    return delegate.dropPartitions(tableName, partsSpec, options, context);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCatName,
      String sourceDbName, String sourceTableName, String destCatName, String destDbName,
      String destTableName) throws TException {
    Table sourceTempTable = null;
    Table destTempTable = null;
    if (isDefaultCatalog(sourceCatName)) {
      sourceTempTable = getTempTable(sourceDbName, sourceTableName);
    }
    if (isDefaultCatalog(destCatName)) {
      destTempTable = getTempTable(destDbName, destTableName);
    }

    if (sourceTempTable == null && destTempTable == null) {
      return delegate.exchange_partition(partitionSpecs, sourceCatName, sourceDbName, sourceTableName,
          destCatName, destDbName, destTableName);
    } else if (sourceTempTable != null && destTempTable != null) {
      TempTable sourceTT = getPartitionedTempTable(sourceTempTable);
      TempTable destTT = getPartitionedTempTable(destTempTable);
      List<Partition> partitions = exchangePartitions(partitionSpecs, sourceTempTable, sourceTT,
          destTempTable, destTT);
      if (!partitions.isEmpty()) {
        return partitions.get(0);
      }
    }
    throw new MetaException("Exchanging partitions between temporary and non-temporary tables is not supported.");
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCatName,
      String sourceDbName, String sourceTableName, String destCatName, String destDbName, String destTableName)
      throws TException {
    Table sourceTempTable = null;
    Table destTempTable = null;
    if (isDefaultCatalog(sourceCatName)) {
      sourceTempTable = getTempTable(sourceDbName, sourceTableName);
    }
    if (isDefaultCatalog(destCatName)) {
      destTempTable = getTempTable(destDbName, destTableName);
    }

    if (sourceTempTable == null && destTempTable == null) {
      return delegate
          .exchange_partitions(partitionSpecs, sourceCatName, sourceDbName, sourceTableName, destCatName, destDbName,
              destTableName);
    } else if (sourceTempTable != null && destTempTable != null) {
      return exchangePartitions(partitionSpecs, sourceTempTable, getPartitionedTempTable(sourceTempTable),
          destTempTable, getPartitionedTempTable(destTempTable));
    }
    throw new MetaException("Exchanging partitions between temporary and non-temporary tables is not supported.");
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        tt.alterPartition(newPart);
        return;
      }
    }

    delegate.alter_partition(catName, dbName, tblName, newPart, environmentContext, writeIdList);
  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        tt.alterPartitions(newParts);
        return;
      }
    }

    delegate.alter_partitions(catName, dbName, tblName, newParts, environmentContext, writeIdList,
        writeId);
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> partitionVals,
      Partition newPart, String validWriteIds, long txnId, boolean makeCopy) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbname, tableName);
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        tt.renamePartition(partitionVals, newPart);
        return;
      }
    }

    delegate.renamePartition(catName, dbname, tableName, partitionVals, newPart, validWriteIds, txnId,
        makeCopy);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        if (partVals == null || partVals.isEmpty()) {
          throw new MetaException("The partition values must be not null or empty.");
        }
        assertTempTablePartitioned(table);
        Partition partition = new PartitionBuilder().inTable(table).setValues(partVals).build(conf);
        return appendPartitionToTempTable(table, partition);
      }
    }

    return delegate.appendPartition(catName, dbName, tableName, partVals);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, String partitionName)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        if (partitionName == null || partitionName.isEmpty()) {
          throw new MetaException("The partition must be not null or empty.");
        }
        assertTempTablePartitioned(table);
        Map<String, String> specFromName = makeSpecFromName(partitionName);
        if (specFromName == null || specFromName.isEmpty()) {
          throw new InvalidObjectException("Invalid partition name " + partitionName);
        }
        List<String> pVals = new ArrayList<>();
        for (FieldSchema field : table.getPartitionKeys()) {
          String val = specFromName.get(field.getName());
          if (val == null) {
            throw new InvalidObjectException("Partition name " + partitionName + " and table partition keys " + Arrays
                .toString(table.getPartitionKeys().toArray()) + " does not match");
          }
          pVals.add(val);
        }
        Partition partition = new PartitionBuilder().inTable(table).setValues(pVals).build(conf);
        return appendPartitionToTempTable(table, partition);
      }
    }

    return delegate.appendPartition(catName, dbName, tableName, partitionName);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String dbName, String tableName,
      String filter, int maxParts) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        return getPartitionsForMaxParts(
            getPartitionedTempTable(table).listPartitionsByFilter(generateJDOFilter(table, filter)),
            maxParts);
      }
    }
    return delegate.listPartitionsByFilter(catName, dbName, tableName, filter, maxParts);
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
      throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tableName);
      if (table != null) {
        return getPartitionedTempTable(table).getNumPartitionsByFilter(generateJDOFilter(table, filter));
      }
    }
    return delegate.getNumPartitionsByFilter(catName, dbName, tableName, filter);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String dbName, String tblName,
      String filter, int maxParts) throws TException {
    if (isDefaultCatalog(catName)) {
      Table table = getTempTable(dbName, tblName);
      if (table != null) {
        return getPartitionSpecProxy(table,
            getPartitionedTempTable(table)
                .listPartitionsByFilter(generateJDOFilter(table, filter)), maxParts);
      }
    }
    return delegate.listPartitionSpecsByFilter(catName, dbName, tblName, filter, maxParts);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws TException {
    if (request == null || request.getPartitionKeys() == null || request.getPartitionKeys().isEmpty()) {
      return delegate.listPartitionValues(request);
    }

    if (isDefaultCatalog(request.getCatName())) {
      Table table = getTempTable(request.getDbName(), request.getTblName());
      if (table != null) {
        TempTable tt = getPartitionedTempTable(table);
        List<Partition> partitions = request.isSetFilter() ?
            tt.listPartitionsByFilter(generateJDOFilter(table, request.getFilter())) :
            tt.listPartitions();
        List<String> partitionNames = new ArrayList<>();
        for (Partition p : partitions) {
          partitionNames.add(makePartName(table.getPartitionKeys(), p.getValues()));
        }
        if (partitionNames.isEmpty() && partitions.isEmpty()) {
          throw new MetaException("Cannot obtain list of partition by filter:\"" + request.getFilter() +
              "\" for " + getCatalogQualifiedTableName(table));
        }
        if (request.isSetAscending()) {
          if (request.isAscending()) {
            Collections.sort(partitionNames);
          } else {
            Collections.sort(partitionNames, Collections.reverseOrder());
          }
        }
        PartitionValuesResponse response = new PartitionValuesResponse();
        response.setPartitionValues(new ArrayList<>(partitionNames.size()));
        for (String partName : partitionNames) {
          ArrayList<String> vals = new ArrayList<>(Collections.nCopies(table.getPartitionKeysSize(), null));
          PartitionValuesRow row = new PartitionValuesRow();
          makeValsFromName(partName, vals);
          vals.forEach(row::addToRow);
          response.addToPartitionValues(row);
        }
        return response;
      }
    }

    return delegate.listPartitionValues(request);
  }

  private PartitionSpecProxy getPartitionSpecProxy(Table table,
      List<Partition> partitions, int maxParts) throws MetaException {
    List<PartitionSpec> partitionSpecs;
    PartitionSpec partitionSpec = new PartitionSpec();
    PartitionListComposingSpec partitionListComposingSpec = new PartitionListComposingSpec(new ArrayList<>());
    for (int i = 0; i < ((maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts); i++) {
      partitionListComposingSpec.addToPartitions(HiveMetaStoreClientUtils.deepCopy(partitions.get(i)));
    }
    partitionSpec.setCatName(table.getCatName());
    partitionSpec.setDbName(table.getDbName());
    partitionSpec.setTableName(table.getTableName());
    partitionSpec.setRootPath(table.getSd().getLocation());
    partitionSpec.setPartitionList(partitionListComposingSpec);
    partitionSpecs = Arrays.asList(partitionSpec);

    return PartitionSpecProxy.Factory.get(partitionSpecs);
  }

  private List<Partition> getPartitionsForMaxParts(List<Partition> parts, int maxParts) {
    List<Partition> matchedParts = new ArrayList<>();
    for(int i = 0; i < ((maxParts < 0 || maxParts > parts.size()) ? parts.size() : maxParts); i++) {
      matchedParts.add(HiveMetaStoreClientUtils.deepCopy(parts.get(i)));
    }
    return matchedParts;
  }

  private String generateJDOFilter(Table table, String filter)
      throws MetaException {
    ExpressionTree exprTree = org.apache.commons.lang3.StringUtils.isNotEmpty(filter)
        ? PartFilterExprUtil.parseFilterTree(filter) : ExpressionTree.EMPTY_TREE;
    return generateJDOFilter(table, exprTree);
  }

  private String generateJDOFilter(Table table, byte[] expr,
      String defaultPartitionName) throws MetaException {
    ExpressionTree expressionTree = PartFilterExprUtil
        .makeExpressionTree(PartFilterExprUtil.createExpressionProxy(conf), expr, defaultPartitionName, conf);
    return generateJDOFilter(table, expressionTree == null ? ExpressionTree.EMPTY_TREE : expressionTree);
  }

  private String generateJDOFilter(Table table, ExpressionTree exprTree)
      throws MetaException {

    assert table != null;
    ExpressionTree.FilterBuilder filterBuilder = new ExpressionTree.FilterBuilder(true);
    Map<String, Object> params = new HashMap<>();
    exprTree.accept(new ExpressionTree.JDOFilterGenerator(conf,
        table.getPartitionKeys(), filterBuilder, params));
    StringBuilder stringBuilder = new StringBuilder(filterBuilder.getFilter());
    params.entrySet().stream().forEach(e -> {
      int index = stringBuilder.indexOf(e.getKey());
      stringBuilder.replace(index, index + e.getKey().length(), "\"" + e.getValue().toString() + "\"");
    });
    return stringBuilder.toString();
  }

  private Partition appendPartitionToTempTable(Table table, Partition partition)
      throws MetaException, AlreadyExistsException {
    TempTable tt = getPartitionedTempTable(table);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
    }
    Path partitionLocation = getPartitionLocation(table, partition, false);
    partition = tt.addPartition(HiveMetaStoreClientUtils.deepCopy(partition));
    createAndSetLocationForAddedPartition(partition, partitionLocation);
    return partition;
  }

  private List<Partition> exchangePartitions(Map<String, String> partitionSpecs,
      Table sourceTable, TempTable sourceTempTable,
      Table destTable, TempTable destTempTable) throws TException {
    if (partitionSpecs == null || partitionSpecs.isEmpty()) {
      throw new MetaException("PartitionSpecs cannot be null or empty.");
    }
    List<String> partitionVals = getPvals(sourceTable.getPartitionKeys(), partitionSpecs);
    if (partitionVals.stream().allMatch(String::isEmpty)) {
      throw new MetaException("Invalid partition key & values; keys " +
          Arrays.toString(sourceTable.getPartitionKeys().toArray()) + ", values " +
          Arrays.toString(partitionVals.toArray()));
    }
    List<Partition> partitionsToExchange = sourceTempTable
        .getPartitionsByPartitionVals(partitionVals);
    if (partitionSpecs == null) {
      throw new MetaException("The partition specs must be not null.");
    }
    if (partitionsToExchange.isEmpty()) {
      throw new MetaException(
          "No partition is found with the values " + partitionSpecs + " for the table " + sourceTable.getTableName());
    }

    boolean sameColumns = compareFieldColumns(sourceTable.getSd().getCols(), destTable.getSd().getCols());
    boolean samePartitions = compareFieldColumns(sourceTable.getPartitionKeys(), destTable.getPartitionKeys());
    if (!(sameColumns && samePartitions)) {
      throw new MetaException("The tables have different schemas. Their partitions cannot be exchanged.");
    }
    // Check if any of the partitions already exists in the destTable
    for (Partition partition : partitionsToExchange) {
      String partToExchangeName = makePartName(destTable.getPartitionKeys(), partition.getValues());
      if (destTempTable.getPartition(partToExchangeName) != null) {
        throw new MetaException(
            "The partition " + partToExchangeName + " already exists in the table " + destTable.getTableName());
      }
    }

    List<Partition> result = new ArrayList<>();
    for (Partition partition : partitionsToExchange) {
      Partition destPartition = new Partition(partition);
      destPartition.setCatName(destTable.getCatName());
      destPartition.setDbName(destTable.getDbName());
      destPartition.setTableName(destTable.getTableName());
      // the destPartition is created from the original partition, therefore all it's properties are copied, including
      // the location. We must force the rewrite of the location (getPartitionLocation(forceRewrite=true))
      destPartition.getSd().setLocation(getPartitionLocation(destTable, destPartition, true).toString());
      wh.renameDir(new Path(partition.getSd().getLocation()), new Path(destPartition.getSd().getLocation()), false);
      destPartition = destTempTable.addPartition(destPartition);
      dropPartition(null, sourceTable.getDbName(), sourceTable.getTableName(), partition.getValues(),
          PartitionDropOptions.instance().deleteData(true));
      result.add(destPartition);
    }
    return result;
  }

  private TempTable getPartitionedTempTable(Table t) throws MetaException {
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, failed to get temp partitions for " + qualifiedTableName);
      return null;
    }
    assertTempTablePartitioned(t);
    TempTable tt = ss.getTempPartitions().get(qualifiedTableName);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " +
          getCatalogQualifiedTableName(t));
    }
    return tt;
  }
  private void removePartitionedTempTable(Table t) {
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, skip removing temp partitions for " + qualifiedTableName);
      return;
    }
    ss.getTempPartitions().remove(Warehouse.getQualifiedName(t));
  }

  private void createPartitionedTempTable(Table t) {
    if(t.getPartitionKeysSize() <= 0) {
      //do nothing as it's not a partitioned table
      return;
    }
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, skip creating temp partitions for " + qualifiedTableName);
      return;
    }
    TempTable tt = new TempTable(t);
    if(ss.getTempPartitions().putIfAbsent(qualifiedTableName, tt) != null) {
      throw new IllegalStateException("TempTable for " + qualifiedTableName + " already exists");
    }
  }

  /**
   * Create the directory for partition and set it to the partition.
   *
   * @param partition         instance of the partition, must be not null
   * @param partitionLocation the location of the partition
   * @throws MetaException if the target directory already exists
   */
  private void createAndSetLocationForAddedPartition(Partition partition, Path partitionLocation) throws MetaException {

    if (partitionLocation != null) {
      partition.getSd().setLocation(partitionLocation.toString());

      // Check to see if the directory already exists before calling
      // mkdirs() because if the file system is read-only, mkdirs will
      // throw an exception even if the directory already exists.
      if (!getWh().isDir(partitionLocation)) {
        if (!getWh().mkdirs(partitionLocation)) {
          throw new MetaException(partitionLocation
              + " is not a directory or unable to create one");
        }
      }
    }
  }

  /**
   * Checking the validity of some partition properties (values, storage descriptor, columns, serdeinfo).
   *
   * @param partition an instance of the partition, must be not null
   * @throws MetaException if some check is failing
   */
  private void checkPartitionProperties(Partition partition) throws MetaException {
    if (partition.getDbName() == null) {
      throw new MetaException("Database name cannot be null. " + partition);
    }
    if (partition.getTableName() == null) {
      throw new MetaException("Table name cannot be null. " + partition);
    }
    if (partition.getValues() == null) {
      throw new MetaException("Partition values cannot be null. " + partition);
    }
    if (partition.getSd() == null) {
      throw new MetaException("Storage descriptor for partition cannot be null. " + partition);
    }
    if (partition.getSd().getCols() == null) {
      // not sure this is correct, but it is possible to add a partition without column information.
      return;
    }

    for (FieldSchema schema : partition.getSd().getCols()) {
      if (schema.getType() == null) {
        throw new MetaException("Storage descriptor column type for partition cannot be null. " + partition);
      }
      if (schema.getName() == null) {
        throw new MetaException("Storage descriptor column name for partition cannot be null. " + partition);
      }
    }
    if (partition.getSd().getSerdeInfo() == null) {
      throw new MetaException("Storage descriptor serde info for partition cannot be null. " + partition);
    }
  }

  /**
   * Get the partition location. If the partition location is not set the location of the parent table will be used.
   *
   * @param table     the parent table, must be not null
   * @param partition instance of the partition, must be not null
   * @param forceOverwrite force recalculation of location based on table/partition name
   * @return location of partition
   * @throws MetaException if the partition location cannot be specified or the location is invalid.
   */
  private Path getPartitionLocation(Table table, Partition partition,
      boolean forceOverwrite)
      throws MetaException {
    Path partLocation = null;
    String partLocationStr = null;
    if (partition.getSd() != null) {
      partLocationStr = partition.getSd().getLocation();
    }
    if (partLocationStr == null || partLocationStr.isEmpty() || forceOverwrite) {
      // set default location if not specified and this is
      // a physical table partition (not a view)
      if (table.getSd().getLocation() != null) {
        partLocation =
            new Path(table.getSd().getLocation(), makePartName(table.getPartitionKeys(), partition.getValues()));
      }
    } else {
      if (table.getSd().getLocation() == null) {
        throw new MetaException("Cannot specify location for a view partition");
      }
      try {
        partLocation = getWh().getDnsPath(new Path(partLocationStr));
      } catch (IllegalArgumentException e) {
        throw new MetaException("Partition path is invalid. " + e.getLocalizedMessage());
      }
    }
    return partLocation;
  }

  /**
   * Try to add the partitions to an already existing temporary table.
   *
   * @param partitions  The partitions to add. It must contain at least one item.
   * @param dbName Name of the database, can be null
   * @param tableName Name of the table, can be null
   * @param ifNotExists only add partition if they don't exist
   * @return the partitions that were added to the temp table. If the temp table is not found, null.
   * @throws MetaException
   */
  private List<Partition> addPartitionsToTempTable(List<Partition> partitions, String dbName, String tableName,
      boolean ifNotExists) throws MetaException, AlreadyExistsException {
    if (!validatePartitions(partitions, dbName, tableName)) {
      return null;
    }
    if (dbName == null) {
      dbName = partitions.get(0).getDbName();
    }
    if (tableName == null) {
      tableName = partitions.get(0).getTableName();
    }
    Table table = getTempTable(dbName, tableName);
    TempTable tt = getPartitionedTempTable(table);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
    }
    List<Partition> result = tt.addPartitions(HiveMetaStoreClientUtils.deepCopyPartitions(partitions), ifNotExists);
    for (Partition p : result) {
      createAndSetLocationForAddedPartition(p, getPartitionLocation(table, p, false));
    }
    return result;
  }

  /**
   * Validate various partition and table properties (dbName, tableName, partitionValues, table key size).
   * @param partitions the partition, must be not null
   * @param dbName name of the database, must be not null
   * @param tableName name of the table, must be not null
   * @return true, if all the validation passed. false, if the associated temporary table cannot be found
   * @throws MetaException some validations can throw it.
   */
  private boolean validatePartitions(List<Partition> partitions, String dbName, String tableName) throws MetaException {
    Set<List<String>> partitionVals = new HashSet<>();
    // do some basic validation
    for (Partition p : partitions) {
      if (p.getDbName() == null) {
        throw new MetaException("Database for partition cannot be null. " + p.toString());
      }
      if (p.getTableName() == null) {
        throw new MetaException("Table for partition cannot be null. " + p.toString());
      }

      // check that all new partitions are belonging to tables located in the same database
      if (dbName != null && !dbName.equals(p.getDbName())) {
        throw new MetaException("Partition tables doesn't belong to the same database "
            + Arrays.toString(partitions.toArray()));
      } else {
        dbName = p.getDbName();
      }
      // check if all new partitions are part of the same table
      if (tableName != null && !tableName.equals(p.getTableName())) {
        throw new MetaException("New partitions doesn't belong to the same table "
            + Arrays.toString(partitions.toArray()));
      } else {
        tableName = p.getTableName();
      }

      // validate that new partitions belonging to the same table doesn't contain duplicate partition values
      if (!partitionVals.contains(p.getValues())) {
        partitionVals.add(p.getValues());
      } else {
        throw new MetaException("Partition values contains duplicate entries. "
            + Arrays.toString(partitions.toArray()));
      }

      // check if all the tables are tmp tables
      Table table = getTempTable(p.getDbName(), p.getTableName());
      if (table == null) {
        return false;
      }

      checkPartitionProperties(p);
      // validate partition location
      getPartitionLocation(table, p, false);
    }
    return true;
  }

  /**
   * Check partition properties of temporary table.
   * @param table table instance, must be not null.
   * @throws MetaException if check fails.
   */
  private void assertTempTablePartitioned(Table table) throws MetaException {
    if(table.getPartitionKeysSize() <= 0) {
      throw new MetaException(getCatalogQualifiedTableName(table) + " is not partitioned");
    }
  }

  /**
   * Delete the directory where the partition resides.
   * @param partition instance of partition, must be not null
   * @param purgeData purge the data
   * @return true if delete was successful
   * @throws MetaException if delete fails
   */
  private boolean deletePartitionLocation(Partition partition, boolean purgeData) throws MetaException {
    String location = partition.getSd().getLocation();
    if (location != null) {
      Path path = getWh().getDnsPath(new Path(location));
      try {
        do {
          if (!getWh().deleteDir(path, purgeData, false)) {
            throw new MetaException("Unable to delete partition at " + location);
          }
          path = path.getParent();
        } while (getWh().isEmptyDir(path));
      } catch (IOException e) {
        throw new MetaException("Unable to delete partition at " + path.toString());
      }
      return true;
    }
    return false;
  }

  @Override
  public Table getTranslateTableDryrun(Table tbl) throws TException {
    if (tbl.isTemporary()) {
      Table table = getTempTable(tbl.getDbName(), tbl.getTableName());
      return table != null ? HiveMetaStoreClientUtils.deepCopy(table) : tbl;
    }
    return delegate.getTranslateTableDryrun(tbl);
  }

  /**
   * Methods for query-level caching
   */

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.CONFIG_VALUE, name, defaultValue);
      String v = (String) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.getConfigValue(name, defaultValue);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getConfigValueInternal, name={}",
            name);
      }
      return v;
    }
    return delegate.getConfigValue(name, defaultValue);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.DATABASE, catalogName, databaseName);
      Database v = (Database) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.getDatabase(catalogName, databaseName);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getDatabaseInternal, name={}",
            databaseName);
      }
      return v;
    }
    return delegate.getDatabase(catalogName, databaseName);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.PRIMARY_KEYS, req);
      PrimaryKeysResponse v = (PrimaryKeysResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new PrimaryKeysResponse(delegate.getPrimaryKeys(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getPrimaryKeysInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getPrimaryKeys();
    }
    return delegate.getPrimaryKeys(req);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.FOREIGN_KEYS, req);
      ForeignKeysResponse v = (ForeignKeysResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new ForeignKeysResponse(delegate.getForeignKeys(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getForeignKeysInternal, dbName={}, tblName={}",
            req.getForeign_db_name(), req.getForeign_tbl_name());
      }
      return v.getForeignKeys();
    }
    return delegate.getForeignKeys(req);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.UNIQUE_CONSTRAINTS, req);
      UniqueConstraintsResponse v = (UniqueConstraintsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new UniqueConstraintsResponse(delegate.getUniqueConstraints(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getUniqueConstraintsInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getUniqueConstraints();
    }
    return delegate.getUniqueConstraints(req);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest req) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      // Retrieve or populate cache
      CacheKey cacheKey = new CacheKey(KeyType.NOT_NULL_CONSTRAINTS, req);
      NotNullConstraintsResponse v = (NotNullConstraintsResponse) queryCache.get(cacheKey);
      if (v == null) {
        v = new NotNullConstraintsResponse(delegate.getNotNullConstraints(req));
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getNotNullConstraintsInternal, dbName={}, tblName={}",
            req.getDb_name(), req.getTbl_name());
      }
      return v.getNotNullConstraints();
    }
    return delegate.getNotNullConstraints(req);
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
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
      req.setEngine(engine);
      req.setCatName(catName);
      req.setValidWriteIdList(writeIdList);
      CacheKey cacheKey = new CacheKey(KeyType.AGGR_COL_STATS, req);
      AggrStats v = (AggrStats) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
        queryCache.put(cacheKey, v);
      } else {
        LOG.debug(
            "Query level HMS cache: method=getAggrStatsForInternal, dbName={}, tblName={}, partNames={}",
            req.getDbName(), req.getTblName(), req.getPartNames());
      }
      return v;
    }

    return delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_ID, fullTableName);
      ValidWriteIdList v = (ValidWriteIdList) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.getValidWriteIds(fullTableName);
        queryCache.put(cacheKey, v);
      }
      return v;
    }
    return delegate.getValidWriteIds(fullTableName);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    Map<Object, Object> queryCache = getQueryCache();
    if (queryCache != null) {
      CacheKey cacheKey = new CacheKey(KeyType.VALID_WRITE_ID, fullTableName, writeId);
      ValidWriteIdList v = (ValidWriteIdList) queryCache.get(cacheKey);
      if (v == null) {
        v = delegate.getValidWriteIds(fullTableName, writeId);
        queryCache.put(cacheKey, v);
      }
      return v;
    }
    return delegate.getValidWriteIds(fullTableName, writeId);
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
      List<TableValidWriteIds> r = delegate.getValidWriteIds(fullTableNamesMissing, validTxnList);
      // 4) Populate the cache
      List<TableValidWriteIds> newTblValidWriteIds = loadValidWriteIdsCache(cache, r, rqst);
      // 5) Sort result (in case there is any assumption) and return
      return computeValidWriteIdsFinal(rqst, tblValidWriteIds, newTblValidWriteIds);
    }
    return delegate.getValidWriteIds(tablesList, validTxnList);
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
   * Methods for setting ValidTxnList retrieved from the current session.
   */

  // TODO: Need to take catName once we extend TXN_TO_WRITE_ID.
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

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws TException {
    if (validWriteIdList == null) {
      validWriteIdList = getValidWriteIdList(dbName, tableName);
    }
    return delegate.getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine,
        validWriteIdList);
  }

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req) throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    return delegate.getPartitionRequest(req);
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req) throws TException {
    if (req.getValidWriteIdList() == null) {
      req.setValidWriteIdList(getValidWriteIdList(req.getDbName(), req.getTblName()));
    }
    return delegate.getPartitionsRequest(req);
  }
}
