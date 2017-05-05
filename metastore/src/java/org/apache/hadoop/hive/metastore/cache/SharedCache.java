/**
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
package org.apache.hadoop.hive.metastore.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.cache.CachedStore.PartitionWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.StorageDescriptorWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.TableWrapper;
import org.apache.hadoop.hive.metastore.hbase.HBaseUtils;
import org.apache.hive.common.util.HiveStringUtils;

import com.google.common.annotations.VisibleForTesting;

public class SharedCache {
  private static Map<String, Database> databaseCache = new TreeMap<String, Database>();
  private static Map<String, TableWrapper> tableCache = new TreeMap<String, TableWrapper>();
  private static Map<String, PartitionWrapper> partitionCache = new TreeMap<String, PartitionWrapper>();
  private static Map<String, ColumnStatisticsObj> partitionColStatsCache = new TreeMap<String, ColumnStatisticsObj>();
  private static Map<ByteArrayWrapper, StorageDescriptorWrapper> sdCache = new HashMap<ByteArrayWrapper, StorageDescriptorWrapper>();
  private static MessageDigest md;

  static {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  public static synchronized Database getDatabaseFromCache(String name) {
    return databaseCache.get(name)!=null?databaseCache.get(name).deepCopy():null;
  }

  public static synchronized void addDatabaseToCache(String dbName, Database db) {
    Database dbCopy = db.deepCopy();
    dbCopy.setName(HiveStringUtils.normalizeIdentifier(dbName));
    databaseCache.put(dbName, dbCopy);
  }

  public static synchronized void removeDatabaseFromCache(String dbName) {
    databaseCache.remove(dbName);
  }

  public static synchronized List<String> listCachedDatabases() {
    return new ArrayList<String>(databaseCache.keySet());
  }

  public static synchronized void alterDatabaseInCache(String dbName, Database newDb) {
    removeDatabaseFromCache(HiveStringUtils.normalizeIdentifier(dbName));
    addDatabaseToCache(HiveStringUtils.normalizeIdentifier(newDb.getName()), newDb.deepCopy());
  }

  public static synchronized int getCachedDatabaseCount() {
    return databaseCache.size();
  }

  public static synchronized Table getTableFromCache(String dbName, String tableName) {
    TableWrapper tblWrapper = tableCache.get(CacheUtils.buildKey(dbName, tableName));
    if (tblWrapper == null) {
      return null;
    }
    Table t = CacheUtils.assemble(tblWrapper);
    return t;
  }

  public static synchronized void addTableToCache(String dbName, String tblName, Table tbl) {
    Table tblCopy = tbl.deepCopy();
    tblCopy.setDbName(HiveStringUtils.normalizeIdentifier(dbName));
    tblCopy.setTableName(HiveStringUtils.normalizeIdentifier(tblName));
    for (FieldSchema fs : tblCopy.getPartitionKeys()) {
      fs.setName(HiveStringUtils.normalizeIdentifier(fs.getName()));
    }
    TableWrapper wrapper;
    if (tbl.getSd()!=null) {
      byte[] sdHash = HBaseUtils.hashStorageDescriptor(tbl.getSd(), md);
      StorageDescriptor sd = tbl.getSd();
      increSd(sd, sdHash);
      tblCopy.setSd(null);
      wrapper = new TableWrapper(tblCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new TableWrapper(tblCopy, null, null, null);
    }
    tableCache.put(CacheUtils.buildKey(dbName, tblName), wrapper);
  }

  public static synchronized void removeTableFromCache(String dbName, String tblName) {
    TableWrapper tblWrapper = tableCache.remove(CacheUtils.buildKey(dbName, tblName));
    byte[] sdHash = tblWrapper.getSdHash();
    if (sdHash!=null) {
      decrSd(sdHash);
    }
  }

  public static synchronized void alterTableInCache(String dbName, String tblName, Table newTable) {
    removeTableFromCache(dbName, tblName);
    addTableToCache(HiveStringUtils.normalizeIdentifier(newTable.getDbName()),
        HiveStringUtils.normalizeIdentifier(newTable.getTableName()), newTable);
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      List<Partition> partitions = listCachedPartitions(dbName, tblName, -1);
      for (Partition part : partitions) {
        removePartitionFromCache(part.getDbName(), part.getTableName(), part.getValues());
        part.setDbName(HiveStringUtils.normalizeIdentifier(newTable.getDbName()));
        part.setTableName(HiveStringUtils.normalizeIdentifier(newTable.getTableName()));
        addPartitionToCache(HiveStringUtils.normalizeIdentifier(newTable.getDbName()),
            HiveStringUtils.normalizeIdentifier(newTable.getTableName()), part);
      }
    }
  }

  public static synchronized int getCachedTableCount() {
    return tableCache.size();
  }

  public static synchronized List<Table> listCachedTables(String dbName) {
    List<Table> tables = new ArrayList<Table>();
    for (TableWrapper wrapper : tableCache.values()) {
      if (wrapper.getTable().getDbName().equals(dbName)) {
        tables.add(CacheUtils.assemble(wrapper));
      }
    }
    return tables;
  }

  public static synchronized void updateTableColumnStatistics(String dbName, String tableName,
      List<ColumnStatisticsObj> statsObjs) {
    Table tbl = getTableFromCache(dbName, tableName);
    tbl.getSd().getParameters();
    List<String> colNames = new ArrayList<>();
    for (ColumnStatisticsObj statsObj:statsObjs) {
      colNames.add(statsObj.getColName());
    }
    StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);
    alterTableInCache(dbName, tableName, tbl);
  }

  public static synchronized List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes) {
    List<TableMeta> tableMetas = new ArrayList<TableMeta>();
    for (String dbName : listCachedDatabases()) {
      if (CacheUtils.matches(dbName, dbNames)) {
        for (Table table : listCachedTables(dbName)) {
          if (CacheUtils.matches(table.getTableName(), tableNames)) {
            if (tableTypes==null || tableTypes.contains(table.getTableType())) {
              TableMeta metaData = new TableMeta(
                  dbName, table.getTableName(), table.getTableType());
                metaData.setComments(table.getParameters().get("comment"));
                tableMetas.add(metaData);
            }
          }
        }
      }
    }
    return tableMetas;
  }

  public static synchronized void addPartitionToCache(String dbName, String tblName, Partition part) {
    Partition partCopy = part.deepCopy();
    PartitionWrapper wrapper;
    if (part.getSd()!=null) {
      byte[] sdHash = HBaseUtils.hashStorageDescriptor(part.getSd(), md);
      StorageDescriptor sd = part.getSd();
      increSd(sd, sdHash);
      partCopy.setSd(null);
      wrapper = new PartitionWrapper(partCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new PartitionWrapper(partCopy, null, null, null);
    }
    partitionCache.put(CacheUtils.buildKey(dbName, tblName, part.getValues()), wrapper);
  }

  public static synchronized Partition getPartitionFromCache(String key) {
    PartitionWrapper wrapper = partitionCache.get(key);
    if (wrapper == null) {
      return null;
    }
    Partition p = CacheUtils.assemble(wrapper);
    return p;
  }

  public static synchronized Partition getPartitionFromCache(String dbName, String tblName, List<String> part_vals) {
    return getPartitionFromCache(CacheUtils.buildKey(dbName, tblName, part_vals));
  }

  public static synchronized boolean existPartitionFromCache(String dbName, String tblName, List<String> part_vals) {
    return partitionCache.containsKey(CacheUtils.buildKey(dbName, tblName, part_vals));
  }

  public static synchronized Partition removePartitionFromCache(String dbName, String tblName, List<String> part_vals) {
    PartitionWrapper wrapper = partitionCache.remove(CacheUtils.buildKey(dbName, tblName, part_vals));
    if (wrapper.getSdHash()!=null) {
      decrSd(wrapper.getSdHash());
    }
    return wrapper.getPartition();
  }

  public static synchronized List<Partition> listCachedPartitions(String dbName, String tblName, int max) {
    List<Partition> partitions = new ArrayList<Partition>();
    int count = 0;
    for (PartitionWrapper wrapper : partitionCache.values()) {
      if (wrapper.getPartition().getDbName().equals(dbName)
          && wrapper.getPartition().getTableName().equals(tblName)
          && (max == -1 || count < max)) {
        partitions.add(CacheUtils.assemble(wrapper));
        count++;
      }
    }
    return partitions;
  }

  public static synchronized void alterPartitionInCache(String dbName, String tblName, List<String> partVals, Partition newPart) {
    removePartitionFromCache(dbName, tblName, partVals);
    addPartitionToCache(HiveStringUtils.normalizeIdentifier(newPart.getDbName()),
        HiveStringUtils.normalizeIdentifier(newPart.getTableName()), newPart);
  }

  public static synchronized void updatePartitionColumnStatistics(String dbName, String tableName,
      List<String> partVals, List<ColumnStatisticsObj> statsObjs) {
    Partition part = getPartitionFromCache(dbName, tableName, partVals);
    part.getSd().getParameters();
    List<String> colNames = new ArrayList<>();
    for (ColumnStatisticsObj statsObj:statsObjs) {
      colNames.add(statsObj.getColName());
    }
    StatsSetupConst.setColumnStatsState(part.getParameters(), colNames);
    alterPartitionInCache(dbName, tableName, partVals, part);
  }

  public static synchronized int getCachedPartitionCount() {
    return partitionCache.size();
  }

  public static synchronized ColumnStatisticsObj getCachedPartitionColStats(String key) {
    return partitionColStatsCache.get(key);
  }

  public static synchronized void addPartitionColStatsToCache(Map<String, ColumnStatisticsObj> aggrStatsPerPartition) {
    partitionColStatsCache.putAll(aggrStatsPerPartition);
  }


  public static void increSd(StorageDescriptor sd, byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    if (sdCache.containsKey(byteArray)) {
      sdCache.get(byteArray).refCount++;
    } else {
      StorageDescriptor sdToCache = sd.deepCopy();
      sdToCache.setLocation(null);
      sdToCache.setParameters(null);
      sdCache.put(byteArray, new StorageDescriptorWrapper(sdToCache, 1));
    }
  }

  public static void decrSd(byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    StorageDescriptorWrapper sdWrapper = sdCache.get(byteArray);
    sdWrapper.refCount--;
    if (sdWrapper.getRefCount() == 0) {
      sdCache.remove(byteArray);
    }
  }

  public static StorageDescriptor getSdFromCache(byte[] sdHash) {
    StorageDescriptorWrapper sdWrapper = sdCache.get(new ByteArrayWrapper(sdHash));
    return sdWrapper.getSd();
  }

  // Replace databases in databaseCache with the new list
  public static synchronized void refreshDatabases(List<Database> databases) {
    for (String dbName : listCachedDatabases()) {
      removeDatabaseFromCache(dbName);
    }
    for (Database db : databases) {
      addDatabaseToCache(db.getName(), db);
    }
  }

  // Replace tables in tableCache with the new list
  public static synchronized void refreshTables(String dbName, List<Table> tables) {
    for (Table tbl : listCachedTables(dbName)) {
      removeTableFromCache(dbName, tbl.getTableName());
    }
    for (Table tbl : tables) {
      addTableToCache(dbName, tbl.getTableName(), tbl);
    }
  }

  public static void refreshPartitions(String dbName, String tblName, List<Partition> partitions) {
    List<String> keysToRemove = new ArrayList<String>();
    for (Map.Entry<String, PartitionWrapper> entry : partitionCache.entrySet()) {
      if (entry.getValue().getPartition().getDbName().equals(dbName)
          && entry.getValue().getPartition().getTableName().equals(tblName)) {
        keysToRemove.add(entry.getKey());
      }
    }
    for (String key : keysToRemove) {
      partitionCache.remove(key);
    }
    for (Partition part : partitions) {
      addPartitionToCache(dbName, tblName, part);
    }
  }

  @VisibleForTesting
  static Map<String, Database> getDatabaseCache() {
    return databaseCache;
  }

  @VisibleForTesting
  static Map<String, TableWrapper> getTableCache() {
    return tableCache;
  }

  @VisibleForTesting
  static Map<String, PartitionWrapper> getPartitionCache() {
    return partitionCache;
  }

  @VisibleForTesting
  static Map<ByteArrayWrapper, StorageDescriptorWrapper> getSdCache() {
    return sdCache;
  }

  @VisibleForTesting
  static Map<String, ColumnStatisticsObj> getPartitionColStatsCache() {
    return partitionColStatsCache;
  }
}
