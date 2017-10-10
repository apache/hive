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
package org.apache.hadoop.hive.metastore.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.cache.CachedStore.PartitionWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.StorageDescriptorWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.TableWrapper;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class SharedCache {
  private Map<String, Database> databaseCache = new TreeMap<>();
  private Map<String, TableWrapper> tableCache = new TreeMap<>();
  private Map<String, PartitionWrapper> partitionCache = new TreeMap<>();
  private Map<String, ColumnStatisticsObj> partitionColStatsCache = new TreeMap<>();
  private Map<String, ColumnStatisticsObj> tableColStatsCache = new TreeMap<>();
  private Map<ByteArrayWrapper, StorageDescriptorWrapper> sdCache = new HashMap<>();
  private static MessageDigest md;

  private static final Logger LOG = LoggerFactory.getLogger(SharedCache.class);

  static {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  public synchronized Database getDatabaseFromCache(String name) {
    return databaseCache.get(name)!=null?databaseCache.get(name).deepCopy():null;
  }

  public synchronized void addDatabaseToCache(String dbName, Database db) {
    Database dbCopy = db.deepCopy();
    dbCopy.setName(StringUtils.normalizeIdentifier(dbName));
    databaseCache.put(dbName, dbCopy);
  }

  public synchronized void removeDatabaseFromCache(String dbName) {
    databaseCache.remove(dbName);
  }

  public synchronized List<String> listCachedDatabases() {
    return new ArrayList<>(databaseCache.keySet());
  }

  public synchronized void alterDatabaseInCache(String dbName, Database newDb) {
    removeDatabaseFromCache(StringUtils.normalizeIdentifier(dbName));
    addDatabaseToCache(StringUtils.normalizeIdentifier(newDb.getName()), newDb.deepCopy());
  }

  public synchronized int getCachedDatabaseCount() {
    return databaseCache.size();
  }

  public synchronized Table getTableFromCache(String dbName, String tableName) {
    TableWrapper tblWrapper = tableCache.get(CacheUtils.buildKey(dbName, tableName));
    if (tblWrapper == null) {
      return null;
    }
    Table t = CacheUtils.assemble(tblWrapper, this);
    return t;
  }

  public synchronized void addTableToCache(String dbName, String tblName, Table tbl) {
    Table tblCopy = tbl.deepCopy();
    tblCopy.setDbName(StringUtils.normalizeIdentifier(dbName));
    tblCopy.setTableName(StringUtils.normalizeIdentifier(tblName));
    if (tblCopy.getPartitionKeys() != null) {
      for (FieldSchema fs : tblCopy.getPartitionKeys()) {
        fs.setName(StringUtils.normalizeIdentifier(fs.getName()));
      }
    }
    TableWrapper wrapper;
    if (tbl.getSd() != null) {
      byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(tbl.getSd(), md);
      StorageDescriptor sd = tbl.getSd();
      increSd(sd, sdHash);
      tblCopy.setSd(null);
      wrapper = new TableWrapper(tblCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new TableWrapper(tblCopy, null, null, null);
    }
    tableCache.put(CacheUtils.buildKey(dbName, tblName), wrapper);
  }

  public synchronized void removeTableFromCache(String dbName, String tblName) {
    TableWrapper tblWrapper = tableCache.remove(CacheUtils.buildKey(dbName, tblName));
    byte[] sdHash = tblWrapper.getSdHash();
    if (sdHash!=null) {
      decrSd(sdHash);
    }
  }

  public synchronized ColumnStatisticsObj getCachedTableColStats(String colStatsCacheKey) {
    return tableColStatsCache.get(colStatsCacheKey)!=null?tableColStatsCache.get(colStatsCacheKey).deepCopy():null;
  }

  public synchronized void removeTableColStatsFromCache(String dbName, String tblName) {
    String partialKey = CacheUtils.buildKeyWithDelimit(dbName, tblName);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        tableColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  public synchronized void removeTableColStatsFromCache(String dbName, String tblName,
      String colName) {
    tableColStatsCache.remove(CacheUtils.buildKey(dbName, tblName, colName));
  }

  public synchronized void updateTableColStatsInCache(String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    for (ColumnStatisticsObj colStatObj : colStatsForTable) {
      // Get old stats object if present
      String key = CacheUtils.buildKey(dbName, tableName, colStatObj.getColName());
      ColumnStatisticsObj oldStatsObj = tableColStatsCache.get(key);
      if (oldStatsObj != null) {
        LOG.debug("CachedStore: updating table column stats for column: " + colStatObj.getColName()
            + ", of table: " + tableName + " and database: " + dbName);
        // Update existing stat object's field
        StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
      } else {
        // No stats exist for this key; add a new object to the cache
        tableColStatsCache.put(key, colStatObj);
      }
    }
  }

  public synchronized void alterTableInCache(String dbName, String tblName, Table newTable) {
    removeTableFromCache(dbName, tblName);
    addTableToCache(StringUtils.normalizeIdentifier(newTable.getDbName()),
        StringUtils.normalizeIdentifier(newTable.getTableName()), newTable);
  }

  public synchronized void alterTableInPartitionCache(String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      List<Partition> partitions = listCachedPartitions(dbName, tblName, -1);
      for (Partition part : partitions) {
        removePartitionFromCache(part.getDbName(), part.getTableName(), part.getValues());
        part.setDbName(StringUtils.normalizeIdentifier(newTable.getDbName()));
        part.setTableName(StringUtils.normalizeIdentifier(newTable.getTableName()));
        addPartitionToCache(StringUtils.normalizeIdentifier(newTable.getDbName()),
            StringUtils.normalizeIdentifier(newTable.getTableName()), part);
      }
    }
  }

  public synchronized void alterTableInTableColStatsCache(String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      String oldPartialTableStatsKey = CacheUtils.buildKeyWithDelimit(dbName, tblName);
      Iterator<Entry<String, ColumnStatisticsObj>> iterator =
          tableColStatsCache.entrySet().iterator();
      Map<String, ColumnStatisticsObj> newTableColStats =
          new HashMap<>();
      while (iterator.hasNext()) {
        Entry<String, ColumnStatisticsObj> entry = iterator.next();
        String key = entry.getKey();
        ColumnStatisticsObj colStatObj = entry.getValue();
        if (key.toLowerCase().startsWith(oldPartialTableStatsKey.toLowerCase())) {
          String[] decomposedKey = CacheUtils.splitTableColStats(key);
          String newKey = CacheUtils.buildKey(decomposedKey[0], decomposedKey[1], decomposedKey[2]);
          newTableColStats.put(newKey, colStatObj);
          iterator.remove();
        }
      }
      tableColStatsCache.putAll(newTableColStats);
    }
  }

  public synchronized void alterTableInPartitionColStatsCache(String dbName, String tblName,
      Table newTable) {
    if (!dbName.equals(newTable.getDbName()) || !tblName.equals(newTable.getTableName())) {
      List<Partition> partitions = listCachedPartitions(dbName, tblName, -1);
      Map<String, ColumnStatisticsObj> newPartitionColStats = new HashMap<>();
      for (Partition part : partitions) {
        String oldPartialPartitionKey =
            CacheUtils.buildKeyWithDelimit(dbName, tblName, part.getValues());
        Iterator<Entry<String, ColumnStatisticsObj>> iterator =
            partitionColStatsCache.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, ColumnStatisticsObj> entry = iterator.next();
          String key = entry.getKey();
          ColumnStatisticsObj colStatObj = entry.getValue();
          if (key.toLowerCase().startsWith(oldPartialPartitionKey.toLowerCase())) {
            Object[] decomposedKey = CacheUtils.splitPartitionColStats(key);
            String newKey =
                CacheUtils.buildKey((String) decomposedKey[0], (String) decomposedKey[1],
                    (List<String>) decomposedKey[2], (String) decomposedKey[3]);
            newPartitionColStats.put(newKey, colStatObj);
            iterator.remove();
          }
        }
      }
      partitionColStatsCache.putAll(newPartitionColStats);
    }
  }

  public synchronized int getCachedTableCount() {
    return tableCache.size();
  }

  public synchronized List<Table> listCachedTables(String dbName) {
    List<Table> tables = new ArrayList<>();
    for (TableWrapper wrapper : tableCache.values()) {
      if (wrapper.getTable().getDbName().equals(dbName)) {
        tables.add(CacheUtils.assemble(wrapper, this));
      }
    }
    return tables;
  }

  public synchronized List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes) {
    List<TableMeta> tableMetas = new ArrayList<>();
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

  public synchronized void addPartitionToCache(String dbName, String tblName, Partition part) {
    Partition partCopy = part.deepCopy();
    PartitionWrapper wrapper;
    if (part.getSd()!=null) {
      byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(part.getSd(), md);
      StorageDescriptor sd = part.getSd();
      increSd(sd, sdHash);
      partCopy.setSd(null);
      wrapper = new PartitionWrapper(partCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new PartitionWrapper(partCopy, null, null, null);
    }
    partitionCache.put(CacheUtils.buildKey(dbName, tblName, part.getValues()), wrapper);
  }

  public synchronized Partition getPartitionFromCache(String key) {
    PartitionWrapper wrapper = partitionCache.get(key);
    if (wrapper == null) {
      return null;
    }
    Partition p = CacheUtils.assemble(wrapper, this);
    return p;
  }

  public synchronized Partition getPartitionFromCache(String dbName, String tblName, List<String> part_vals) {
    return getPartitionFromCache(CacheUtils.buildKey(dbName, tblName, part_vals));
  }

  public synchronized boolean existPartitionFromCache(String dbName, String tblName, List<String> part_vals) {
    return partitionCache.containsKey(CacheUtils.buildKey(dbName, tblName, part_vals));
  }

  public synchronized Partition removePartitionFromCache(String dbName, String tblName,
      List<String> part_vals) {
    PartitionWrapper wrapper =
        partitionCache.remove(CacheUtils.buildKey(dbName, tblName, part_vals));
    if (wrapper.getSdHash() != null) {
      decrSd(wrapper.getSdHash());
    }
    return wrapper.getPartition();
  }

  // Remove cached column stats for all partitions of a table
  public synchronized void removePartitionColStatsFromCache(String dbName, String tblName) {
    String partialKey = CacheUtils.buildKeyWithDelimit(dbName, tblName);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  // Remove cached column stats for a particular partition of a table
  public synchronized void removePartitionColStatsFromCache(String dbName, String tblName,
      List<String> partVals) {
    String partialKey = CacheUtils.buildKeyWithDelimit(dbName, tblName, partVals);
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
        iterator.remove();
      }
    }
  }

  // Remove cached column stats for a particular partition and a particular column of a table
  public synchronized void removePartitionColStatsFromCache(String dbName, String tblName,
      List<String> partVals, String colName) {
    partitionColStatsCache.remove(CacheUtils.buildKey(dbName, tblName, partVals, colName));
  }

  public synchronized List<Partition> listCachedPartitions(String dbName, String tblName, int max) {
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (PartitionWrapper wrapper : partitionCache.values()) {
      if (wrapper.getPartition().getDbName().equals(dbName)
          && wrapper.getPartition().getTableName().equals(tblName)
          && (max == -1 || count < max)) {
        partitions.add(CacheUtils.assemble(wrapper, this));
        count++;
      }
    }
    return partitions;
  }

  public synchronized void alterPartitionInCache(String dbName, String tblName,
      List<String> partVals, Partition newPart) {
    removePartitionFromCache(dbName, tblName, partVals);
    addPartitionToCache(StringUtils.normalizeIdentifier(newPart.getDbName()),
        StringUtils.normalizeIdentifier(newPart.getTableName()), newPart);
  }

  public synchronized void alterPartitionInColStatsCache(String dbName, String tblName,
      List<String> partVals, Partition newPart) {
    String oldPartialPartitionKey = CacheUtils.buildKeyWithDelimit(dbName, tblName, partVals);
    Map<String, ColumnStatisticsObj> newPartitionColStats = new HashMap<>();
    Iterator<Entry<String, ColumnStatisticsObj>> iterator =
        partitionColStatsCache.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, ColumnStatisticsObj> entry = iterator.next();
      String key = entry.getKey();
      ColumnStatisticsObj colStatObj = entry.getValue();
      if (key.toLowerCase().startsWith(oldPartialPartitionKey.toLowerCase())) {
        Object[] decomposedKey = CacheUtils.splitPartitionColStats(key);
        String newKey =
            CacheUtils.buildKey(StringUtils.normalizeIdentifier(newPart.getDbName()),
                StringUtils.normalizeIdentifier(newPart.getTableName()), newPart.getValues(),
                (String) decomposedKey[3]);
        newPartitionColStats.put(newKey, colStatObj);
        iterator.remove();
      }
    }
    partitionColStatsCache.putAll(newPartitionColStats);
  }

  public synchronized void updatePartitionColStatsInCache(String dbName, String tableName,
      List<String> partVals, List<ColumnStatisticsObj> colStatsObjs) {
    for (ColumnStatisticsObj colStatObj : colStatsObjs) {
      // Get old stats object if present
      String key = CacheUtils.buildKey(dbName, tableName, partVals, colStatObj.getColName());
      ColumnStatisticsObj oldStatsObj = partitionColStatsCache.get(key);
      if (oldStatsObj != null) {
        // Update existing stat object's field
        LOG.debug("CachedStore: updating partition column stats for column: "
            + colStatObj.getColName() + ", of table: " + tableName + " and database: " + dbName);
        StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
      } else {
        // No stats exist for this key; add a new object to the cache
        partitionColStatsCache.put(key, colStatObj);
      }
    }
  }

  public synchronized int getCachedPartitionCount() {
    return partitionCache.size();
  }

  public synchronized ColumnStatisticsObj getCachedPartitionColStats(String key) {
    return partitionColStatsCache.get(key)!=null?partitionColStatsCache.get(key).deepCopy():null;
  }

  public synchronized void addPartitionColStatsToCache(String dbName, String tableName,
      Map<String, List<ColumnStatisticsObj>> colStatsPerPartition) {
    for (Map.Entry<String, List<ColumnStatisticsObj>> entry : colStatsPerPartition.entrySet()) {
      String partName = entry.getKey();
      try {
        List<String> partVals = Warehouse.getPartValuesFromPartName(partName);
        for (ColumnStatisticsObj colStatObj : entry.getValue()) {
          String key = CacheUtils.buildKey(dbName, tableName, partVals, colStatObj.getColName());
          partitionColStatsCache.put(key, colStatObj);
        }
      } catch (MetaException e) {
        LOG.info("Unable to add partition: " + partName + " to SharedCache", e);
      }
    }
  }

  public synchronized void refreshPartitionColStats(String dbName, String tableName,
      Map<String, List<ColumnStatisticsObj>> newColStatsPerPartition) {
    LOG.debug("CachedStore: updating cached partition column stats objects for database: " + dbName
        + " and table: " + tableName);
    removePartitionColStatsFromCache(dbName, tableName);
    addPartitionColStatsToCache(dbName, tableName, newColStatsPerPartition);
  }

  public synchronized void addTableColStatsToCache(String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    for (ColumnStatisticsObj colStatObj : colStatsForTable) {
      String key = CacheUtils.buildKey(dbName, tableName, colStatObj.getColName());
      tableColStatsCache.put(key, colStatObj);
    }
  }

  public synchronized void refreshTableColStats(String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    LOG.debug("CachedStore: updating cached table column stats objects for database: " + dbName
        + " and table: " + tableName);
    // Remove all old cache entries for this table
    removeTableColStatsFromCache(dbName, tableName);
    // Add new entries to cache
    addTableColStatsToCache(dbName, tableName, colStatsForTable);
  }

  public void increSd(StorageDescriptor sd, byte[] sdHash) {
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

  public void decrSd(byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    StorageDescriptorWrapper sdWrapper = sdCache.get(byteArray);
    sdWrapper.refCount--;
    if (sdWrapper.getRefCount() == 0) {
      sdCache.remove(byteArray);
    }
  }

  public StorageDescriptor getSdFromCache(byte[] sdHash) {
    StorageDescriptorWrapper sdWrapper = sdCache.get(new ByteArrayWrapper(sdHash));
    return sdWrapper.getSd();
  }

  // Replace databases in databaseCache with the new list
  public synchronized void refreshDatabases(List<Database> databases) {
    LOG.debug("CachedStore: updating cached database objects");
    for (String dbName : listCachedDatabases()) {
      removeDatabaseFromCache(dbName);
    }
    for (Database db : databases) {
      addDatabaseToCache(db.getName(), db);
    }
  }

  // Replace tables in tableCache with the new list
  public synchronized void refreshTables(String dbName, List<Table> tables) {
    LOG.debug("CachedStore: updating cached table objects for database: " + dbName);
    for (Table tbl : listCachedTables(dbName)) {
      removeTableFromCache(dbName, tbl.getTableName());
    }
    for (Table tbl : tables) {
      addTableToCache(dbName, tbl.getTableName(), tbl);
    }
  }

  public synchronized void refreshPartitions(String dbName, String tblName,
      List<Partition> partitions) {
    LOG.debug("CachedStore: updating cached partition objects for database: " + dbName
        + " and table: " + tblName);
    Iterator<Entry<String, PartitionWrapper>> iterator = partitionCache.entrySet().iterator();
    while (iterator.hasNext()) {
      PartitionWrapper partitionWrapper = iterator.next().getValue();
      if (partitionWrapper.getPartition().getDbName().equals(dbName)
          && partitionWrapper.getPartition().getTableName().equals(tblName)) {
        iterator.remove();
      }
    }
    for (Partition part : partitions) {
      addPartitionToCache(dbName, tblName, part);
    }
  }

  @VisibleForTesting
  Map<String, Database> getDatabaseCache() {
    return databaseCache;
  }

  @VisibleForTesting
  Map<String, TableWrapper> getTableCache() {
    return tableCache;
  }

  @VisibleForTesting
  Map<String, PartitionWrapper> getPartitionCache() {
    return partitionCache;
  }

  @VisibleForTesting
  Map<ByteArrayWrapper, StorageDescriptorWrapper> getSdCache() {
    return sdCache;
  }

  @VisibleForTesting
  Map<String, ColumnStatisticsObj> getPartitionColStatsCache() {
    return partitionColStatsCache;
  }
}
