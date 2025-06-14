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

import com.github.benmanes.caffeine.cache.Cache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class MetaStoreClientCacheUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreClientCacheUtils.class);

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
    // Database <-- getDatabaseInternal(String catalogName, String databaseName)
    DATABASE(Database.class, String.class, String.class),
    // Table <-- getTableInternal(GetTableRequest req)
    TABLE(Table.class, GetTableRequest.class),
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
    // SG:FIXME, understand the following code and check whether the below line correctly indicates the
    //  member of CacheKey.
    // SG:FIXME, Do we use id? Check git log.
    TABLE_COLUMN_STATS(ColumnStatisticsObj.class, String.class, long.class, TableWatermark.class),
    // AggrStats <-- getAggrStatsForInternal(PartitionsStatsRequest req), (TableWatermark tw ?)
    AGGR_COL_STATS(AggrStats.class, PartitionsStatsRequest.class, TableWatermark.class),
    // PartitionsWrapper <-- getPartitionsByExprInternal(PartitionsByExprRequest req), (TableWatermark tw ?)
    PARTITIONS_BY_EXPR(PartitionsWrapper.class, PartitionsByExprRequest.class, TableWatermark.class),
    // PartitionSpecsWrapper <-- getPartitionsSpecByExprInternal(PartitionsByExprRequest req), (TableWatermark tw ?)
    PARTITIONS_SPEC_BY_EXPR(PartitionSpecsWrapper.class, PartitionsByExprRequest.class, TableWatermark.class),
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
    LIST_PARTITIONS_AUTH_INFO_ALL(org.apache.hadoop.hive.metastore.api.Partition.class, String.class, int.class),
    // List<Partition> <- listPartitionsWithAuthInfoInternal(String catName, String dbName, String tableName,
    //      List<String> partialPvals, int maxParts, String userName, List<String> groupNames)
    LIST_PARTITIONS_AUTH_INFO(org.apache.hadoop.hive.metastore.api.Partition.class, String.class, int.class),
    // GetPartitionsPsWithAuthResponse <- listPartitionsWithAuthInfoRequestInternal(GetPartitionsPsWithAuthRequest req)
    LIST_PARTITIONS_AUTH_INFO_REQ(GetPartitionsPsWithAuthResponse.class, GetPartitionsPsWithAuthRequest.class),
    // GetPartitionsByNamesResult <-- getPartitionsByNamesInternal(GetPartitionsByNamesRequest gpbnr)
    // Stored individually as:
    // Partition <-- String db_name, String tbl_name, List<String> partValues, boolean get_col_stats,
    //      List<String> processorCapabilities, String processorIdentifier, String engine,
    //      String validWriteIdList, (TableWatermark tw ?)
    PARTITIONS_BY_NAMES(Partition.class, String.class, boolean.class, TableWatermark.class),
    // GetValidWriteIdsResponse <-- getValidWriteIdsInternal(GetValidWriteIdsRequest rqst)
    // Stored individually as:
    // TableValidWriteIds <-- String fullTableName, String validTxnList, long writeId
    VALID_WRITE_IDS(TableValidWriteIds.class, String.class, long.class),
    // SG:FIXME, I don't know what I'm doing. Should be fixed!
    // ValidWriteIdList <-- String fullTableName, (optional) long writeId
    VALID_WRITE_ID(ValidWriteIdList.class, String.class, long.class),
    // TableId <- String fullTableName
    TABLE_ID(Long.class, String.class);

    public final List<Class<?>> keyClasses;
    public final Class<?> valueClass;

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

  public static int getWeight(CacheKey key, Object val, HashMap<Class<?>, ObjectEstimator> sizeEstimator) {
    IncrementalObjectSizeEstimator.ObjectEstimator keySizeEstimator = sizeEstimator.get(key.getClass());
    IncrementalObjectSizeEstimator.ObjectEstimator valSizeEstimator = sizeEstimator.get(key.IDENTIFIER.valueClass);
    int keySize = keySizeEstimator.estimate(key, sizeEstimator);
    int valSize = valSizeEstimator.estimate(val, sizeEstimator);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache entry weight - key: {}, value: {}, total: {}", keySize, valSize, keySize + valSize);
    }
    return keySize + valSize;
  }

  public static String getQueryId() {
    try {
      return Hive.get().getConf().get(HiveConf.ConfVars.HIVE_QUERY_ID.varname);
    } catch (HiveException e) {
      LOG.error("Error getting query id. Query level and Global HMS caching will be disabled", e);
      return null;
    }
  }

  /* This class is needed so the size estimator can work properly. */
  public static class PartitionNamesWrapper {
    public final List<String> partitionNames;

    public PartitionNamesWrapper(List<String> partitionNames) {
      this.partitionNames = partitionNames;
    }
  }

  // SG:FIXME, do we really need this wrapper classes? cf. LIST_PARTITIONS
  public static class PartitionsWrapper {
    public final List<Partition> partitions;
    public final boolean hasUnknownPartition;

    public PartitionsWrapper(List<Partition> partitions, boolean hasUnknownPartition) {
      this.partitions = partitions;
      this.hasUnknownPartition = hasUnknownPartition;
    }
  }

  public static class PartitionSpecsWrapper {
    public final List<PartitionSpec> partitionSpecs;
    public final boolean hasUnknownPartition;

    public PartitionSpecsWrapper(List<PartitionSpec> partitionSpecs, boolean hasUnknownPartition) {
      this.partitionSpecs = partitionSpecs;
      this.hasUnknownPartition = hasUnknownPartition;
    }
  }

  /**
   * Wrapper to create a cache around a Caffeine Cache.
   */
  public static class CacheWrapper implements CacheI {
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
  public interface CacheI {
    void put(Object k, Object v);

    Object get(Object k);
  }

  /**
   * Internal class to identify uniquely a Table.
   */
  public static class TableWatermark {
    final String validWriteIdList;
    final long tableId;

    public TableWatermark(String validWriteIdList, long tableId) {
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

  public interface Supplier<T, E extends Throwable> {
    T get() throws E;
  }

  /**
   * Auxiliary functions
   */

  public static Pair<List<ColumnStatisticsObj>, List<String>> getTableColumnStatisticsCache(
      CacheI cache, String catName, String dbName, String tableName, List<String> colNames, String engine,
      String validWriteIdList, TableWatermark watermark) {
    List<String> colStatsMissing = new ArrayList<>();
    List<ColumnStatisticsObj> colStats = new ArrayList<>();
    for (String colName : colNames) {
      CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark, dbName, tableName, colName,
          catName, validWriteIdList, engine, -1);
      ColumnStatisticsObj v = (ColumnStatisticsObj) cache.get(cacheKey);
      if (v == null) {
        colStatsMissing.add(colName);
      } else {
        if (watermark == null) {
          LOG.debug(
              "Query level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, colName={}",
              dbName, tableName, colName);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getTableColumnStatisticsInternal, dbName={}, tblName={}, colName={}",
              dbName, tableName, colName);
        }
        colStats.add(v);
      }
    }
    return Pair.of(colStats, colStatsMissing);
  }

  public static void loadTableColumnStatisticsCache(CacheI cache, List<ColumnStatisticsObj> colStats,
      String catName, String dbName, String tableName, String engine, String validWriteIdList,
      TableWatermark watermark) {
    for (ColumnStatisticsObj colStat : colStats) {
      CacheKey cacheKey = new CacheKey(KeyType.TABLE_COLUMN_STATS, watermark, dbName, tableName,
          colStat.getColName(), catName, validWriteIdList, engine, -1);
      cache.put(cacheKey, colStat);
    }
  }

  public static List<ColumnStatisticsObj> computeTableColumnStatisticsFinal(List<String> originalColNames,
      List<ColumnStatisticsObj> colStats, List<ColumnStatisticsObj> newColStats) {
    List<ColumnStatisticsObj> result = new ArrayList<>();
    int i = 0, j = 0;
    for (String colName : originalColNames) {
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
    return result;
  }

  public static Pair<List<Partition>, List<String>> getPartitionsByNamesCache(CacheI cache,
      GetPartitionsByNamesRequest rqst, TableWatermark watermark) throws MetaException {
    List<String> partitionsMissing = new ArrayList<>();
    List<Partition> partitions = new ArrayList<>();
    for (String partitionName : rqst.getNames()) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_NAMES, watermark,
          rqst.getDb_name(), rqst.getTbl_name(), Warehouse.getPartValuesFromPartName(partitionName),
          rqst.isGet_col_stats(), rqst.getProcessorCapabilities(), rqst.getProcessorIdentifier(),
          rqst.getEngine(), rqst.getValidWriteIdList());
      Partition v = (Partition) cache.get(cacheKey);
      if (v == null) {
        partitionsMissing.add(partitionName);
      } else {
        if (watermark == null) {
          LOG.debug(
              "Query level HMS cache: method=getPartitionsByNamesInternal, dbName={}, tblName={}, partitionName={}",
              rqst.getDb_name(), rqst.getTbl_name(), partitionName);
        } else {
          LOG.debug(
              "HS2 level HMS cache: method=getPartitionsByNamesInternal, dbName={}, tblName={}, partitionName={}",
              rqst.getDb_name(), rqst.getTbl_name(), partitionName);
        }
        partitions.add(v);
      }
    }
    return Pair.of(partitions, partitionsMissing);
  }

  public static List<Partition> loadPartitionsByNamesCache(CacheI cache,
      GetPartitionsByNamesResult r, GetPartitionsByNamesRequest rqst, TableWatermark watermark) {
    List<Partition> newPartitions = new ArrayList<>();
    for (Partition partition : r.getPartitions()) {
      CacheKey cacheKey = new CacheKey(KeyType.PARTITIONS_BY_NAMES, watermark,
          rqst.getDb_name(), rqst.getTbl_name(), partition.getValues(),
          rqst.isGet_col_stats(), rqst.getProcessorCapabilities(), rqst.getProcessorIdentifier(),
          rqst.getEngine(), rqst.getValidWriteIdList());
      cache.put(cacheKey, partition);
      newPartitions.add(partition);
    }
    return newPartitions;
  }

  public static GetPartitionsByNamesResult computePartitionsByNamesFinal(GetPartitionsByNamesRequest rqst,
      List<Partition> partitions, List<Partition> newPartitions) throws MetaException {
    List<Partition> result = new ArrayList<>();
    int i = 0, j = 0;
    for (String partitionName : rqst.getNames()) {
      if (i >= partitions.size() || j >= newPartitions.size()) {
        break;
      }
      List<String> pv = Warehouse.getPartValuesFromPartName(partitionName);
      if (partitions.get(i).getValues().equals(pv)) {
        result.add(partitions.get(i));
        i++;
      } else if (newPartitions.get(j).getValues().equals(pv)) {
        result.add(newPartitions.get(j));
        j++;
      }
    }
    while (i < partitions.size()) {
      result.add(partitions.get(i));
      i++;
    }
    while (j < newPartitions.size()) {
      result.add(newPartitions.get(j));
      j++;
    }
    return new GetPartitionsByNamesResult(result);
  }
}
