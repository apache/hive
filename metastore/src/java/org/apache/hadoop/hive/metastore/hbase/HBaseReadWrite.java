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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Class to manage storing object in and reading them from HBase.
 */
class HBaseReadWrite {

  @VisibleForTesting final static String DB_TABLE = "DBS";
  @VisibleForTesting final static String PART_TABLE = "PARTITIONS";
  @VisibleForTesting final static String ROLE_TABLE = "ROLES";
  @VisibleForTesting final static String SD_TABLE = "SDS";
  @VisibleForTesting final static String TABLE_TABLE = "TBLS";
  @VisibleForTesting final static byte[] CATALOG_CF = "c".getBytes(HBaseUtils.ENCODING);
  @VisibleForTesting final static byte[] STATS_CF = "s".getBytes(HBaseUtils.ENCODING);
  @VisibleForTesting final static String NO_CACHE_CONF = "no.use.cache";
  private final static byte[] CATALOG_COL = "cat".getBytes(HBaseUtils.ENCODING);
  private final static byte[] REF_COUNT_COL = "ref".getBytes(HBaseUtils.ENCODING);
  private final static int TABLES_TO_CACHE = 10;

  // TODO Add privileges as a second column in the CATALOG_CF

  private final static String[] tableNames = { DB_TABLE, PART_TABLE, ROLE_TABLE, SD_TABLE,
      TABLE_TABLE  };
  static final private Log LOG = LogFactory.getLog(HBaseReadWrite.class.getName());

  private static ThreadLocal<HBaseReadWrite> self = new ThreadLocal<HBaseReadWrite>() {
    @Override
    protected HBaseReadWrite initialValue() {
      if (staticConf == null) {
        throw new RuntimeException("Attempt to create HBaseReadWrite with no configuration set");
      }
      return new HBaseReadWrite(staticConf);
    }
  };

  private static boolean tablesCreated = false;
  private static Configuration staticConf = null;

  private Configuration conf;
  private HConnection conn;
  private Map<String, HTableInterface> tables;
  private MessageDigest md;
  private ObjectCache<ObjectPair<String, String>, Table> tableCache;
  private ObjectCache<ByteArrayWrapper, StorageDescriptor> sdCache;
  private PartitionCache partCache;
  private StatsCache statsCache;
  private Counter tableHits;
  private Counter tableMisses;
  private Counter tableOverflows;
  private Counter partHits;
  private Counter partMisses;
  private Counter partOverflows;
  private Counter sdHits;
  private Counter sdMisses;
  private Counter sdOverflows;
  private List<Counter> counters;

  /**
   * Get the instance of HBaseReadWrite for the current thread.  This is intended to be used by
   * {@link org.apache.hadoop.hive.metastore.hbase.HBaseStore} since it creates the thread local
   * version of this class.
   * @param configuration Configuration object
   * @return thread's instance of HBaseReadWrite
   */
  static HBaseReadWrite getInstance(Configuration configuration) {
    staticConf = configuration;
    return self.get();
  }

  /**
   * Get the instance of HBaseReadWrite for the current thread.  This is inteded to be used after
   * the thread has been initialized.  Woe betide you if that's not the case.
   * @return thread's instance of HBaseReadWrite
   */
  static HBaseReadWrite getInstance() {
    return self.get();
  }

  private HBaseReadWrite(Configuration configuration) {
    conf = configuration;
    HBaseConfiguration.addHbaseResources(conf);

    try {
      conn = HConnectionManager.createConnection(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    tables = new HashMap<String, HTableInterface>();

    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    int totalObjectsToCache =
        HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_CACHE_SIZE);

    tableHits = new Counter("table cache hits");
    tableMisses = new Counter("table cache misses");
    tableOverflows = new Counter("table cache overflows");
    partHits = new Counter("partition cache hits");
    partMisses = new Counter("partition cache misses");
    partOverflows = new Counter("partition cache overflows");
    sdHits = new Counter("storage descriptor cache hits");
    sdMisses = new Counter("storage descriptor cache misses");
    sdOverflows = new Counter("storage descriptor cache overflows");
    counters = new ArrayList<Counter>();
    counters.add(tableHits);
    counters.add(tableMisses);
    counters.add(tableOverflows);
    counters.add(partHits);
    counters.add(partMisses);
    counters.add(partOverflows);
    counters.add(sdHits);
    counters.add(sdMisses);
    counters.add(sdOverflows);

    // Divide 50/50 between catalog and stats, then give 1% of catalog space to storage
    // descriptors (storage descriptors are shared, so 99% should be the same for a
    // given table).
    int sdsCacheSize = totalObjectsToCache / 100;
    if (conf.getBoolean(NO_CACHE_CONF, false)) {
      tableCache = new BogusObjectCache<ObjectPair<String, String>, Table>();
      sdCache = new BogusObjectCache<ByteArrayWrapper, StorageDescriptor>();
      partCache = new BogusPartitionCache();
      statsCache = StatsCache.getBogusStatsCache();
    } else {
      tableCache = new ObjectCache<ObjectPair<String, String>, Table>(TABLES_TO_CACHE, tableHits,
          tableMisses, tableOverflows);
      sdCache = new ObjectCache<ByteArrayWrapper, StorageDescriptor>(sdsCacheSize, sdHits,
          sdMisses, sdOverflows);
      partCache = new PartitionCache(totalObjectsToCache / 2, partHits, partMisses, partOverflows);
      statsCache = StatsCache.getInstance(conf);
    }
  }

  // Synchronize this so not everyone's doing it at once.
  static synchronized void createTablesIfNotExist() throws IOException {
    if (!tablesCreated) {
      HBaseAdmin admin = new HBaseAdmin(self.get().conn);
      for (String name : tableNames) {
        if (self.get().getHTable(name) == null) {
          LOG.info("Creating HBase table " + name);
          HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(name));
          tableDesc.addFamily(new HColumnDescriptor(CATALOG_CF));
          // Only table and partitions need stats
          if (TABLE_TABLE.equals(name) || PART_TABLE.equals(name)) {
            tableDesc.addFamily(new HColumnDescriptor(STATS_CF));
          }
          admin.createTable(tableDesc);
        }
      }
      admin.close();
      tablesCreated = true;
    }
  }

  /**
   * Begin a transaction
   */
  void begin() {
    // NOP for now
  }

  /**
   * Commit a transaction
   */
  void commit() {
    // NOP for now
  }

  void rollback() {
    // NOP for now
  }

  void close() throws IOException {
    for (HTableInterface htab : tables.values()) htab.close();
    conn.close();
  }

  /**
   * Fetch a database object
   * @param name name of the database to fetch
   * @return the database object, or null if there is no such database
   * @throws IOException
   */
  Database getDb(String name) throws IOException {
    byte[] key = HBaseUtils.buildKey(name);
    byte[] serialized = read(DB_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    DatabaseWritable db = new DatabaseWritable();
    HBaseUtils.deserialize(db, serialized);
    return db.db;
  }

  /**
   * Get a list of databases.
   * @param regex Regular expression to use in searching for database names.  It is expected to
   *              be a Java regular expression.  If it is null then all databases will be returned.
   * @return list of databases matching the regular expression.
   * @throws IOException
   */
  List<Database> scanDatabases(String regex) throws IOException {
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    Iterator<Result> iter =
        scanWithFilter(DB_TABLE, null, CATALOG_CF, CATALOG_COL, filter);
    List<Database> databases = new ArrayList<Database>();
    while (iter.hasNext()) {
      DatabaseWritable db = new DatabaseWritable();
      HBaseUtils.deserialize(db, iter.next().getValue(CATALOG_CF, CATALOG_COL));
      databases.add(db.db);
    }
    return databases;
  }

  /**
   * Store a database object
   * @param database database object to store
   * @throws IOException
   */
  void putDb(Database database) throws IOException {
    DatabaseWritable db = new DatabaseWritable(database);
    byte[] key = HBaseUtils.buildKey(db.db.getName());
    byte[] serialized = HBaseUtils.serialize(db);
    store(DB_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    flush();
  }

  /**
   * Drop a database
   * @param name name of db to drop
   * @throws IOException
   */
  void deleteDb(String name) throws IOException {
    byte[] key = HBaseUtils.buildKey(name);
    delete(DB_TABLE, key, null, null);
    flush();
  }

  /**
   * Fetch one partition
   * @param dbName database table is in
   * @param tableName table partition is in
   * @param partVals list of values that specify the partition, given in the same order as the
   *                 columns they belong to
   * @return The partition objec,t or null if there is no such partition
   * @throws IOException
   */
  Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws IOException {
    return getPartition(dbName, tableName, partVals, true);
  }

  /**
   * Add a partition
   * @param partition partition object to add
   * @throws IOException
   */
  void putPartition(Partition partition) throws IOException {
    PartitionWritable part = new PartitionWritable(partition);
    byte[] key = buildPartitionKey(part);
    byte[] serialized = HBaseUtils.serialize(part);
    store(PART_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    flush();
    partCache.put(partition.getDbName(), partition.getTableName(), partition);
  }

  /**
   * Add a group of partitions
   * @param partitions list of partitions to add
   * @throws IOException
   */
  void putPartitions(List<Partition> partitions) throws IOException {
    List<Put> puts = new ArrayList<Put>(partitions.size());
    for (Partition partition : partitions) {
      PartitionWritable part = new PartitionWritable(partition);
      byte[] key = buildPartitionKey(part);
      byte[] serialized = HBaseUtils.serialize(part);
      Put p = new Put(key);
      p.add(CATALOG_CF, CATALOG_COL, serialized);
      puts.add(p);
      partCache.put(partition.getDbName(), partition.getTableName(), partition);
    }
    getHTable(PART_TABLE).put(puts);
    flush();
  }

  /**
   * Find all the partitions in a table.
   * @param dbName name of the database the table is in
   * @param tableName table name
   * @param maxPartitions max partitions to fetch.  If negative all partitions will be returned.
   * @return List of partitions that match the criteria.
   * @throws IOException
   */
  List<Partition> scanPartitionsInTable(String dbName, String tableName, int maxPartitions)
      throws IOException {
    if (maxPartitions < 0) maxPartitions = Integer.MAX_VALUE;
    Collection<Partition> cached = partCache.getAllForTable(dbName, tableName);
    if (cached != null) {
      return maxPartitions < cached.size()
          ? new ArrayList<Partition>(cached).subList(0, maxPartitions)
          : new ArrayList<Partition>(cached);
    }
    byte[] keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName, tableName);
    List<Partition> parts = scanPartitions(keyPrefix, CATALOG_CF, CATALOG_COL, -1);
    partCache.put(dbName, tableName, parts, true);
    return maxPartitions < parts.size() ? parts.subList(0, maxPartitions) : parts;
  }

  /**
   * Scan partitions based on partial key information.
   * @param dbName name of database, required
   * @param tableName name of table, required
   * @param partVals partial specification of values.  Any values that are unknown can instead be
   *                 a '*'.  For example, if a table had two partition columns date
   *                 and region (in that order), and partitions ('today', 'na'), ('today', 'eu'),
   *                 ('tomorrow', 'na'), ('tomorrow', 'eu') then passing ['today', '*'] would return
   *                 ('today', 'na') and ('today', 'eu') while passing ['*', 'eu'] would return
   *                 ('today', 'eu') and ('tomorrow', 'eu').  Also the list can terminate early,
   *                 which will be the equivalent of adding '*' for all non-included values.
   *                 I.e. ['today'] is the same as ['today', '*'].
   * @param maxPartitions Maximum number of entries to return.
   * @return list of partitions that match the specified information
   * @throws IOException
   * @throws org.apache.hadoop.hive.metastore.api.NoSuchObjectException if the table containing
   * the partitions can't be found.
   */
  List<Partition> scanPartitions(String dbName, String tableName, List<String> partVals,
                                 int maxPartitions) throws IOException, NoSuchObjectException {
    // First, build as much of the key as we can so that we make the scan as tight as possible.
    List<String> keyElements = new ArrayList<String>();
    keyElements.add(dbName);
    keyElements.add(tableName);

    int firstStar = -1;
    for (int i = 0; i < partVals.size(); i++) {
      if ("*".equals(partVals.get(i))) {
        firstStar = i;
        break;
      } else {
        keyElements.add(partVals.get(i));
      }
    }

    byte[] keyPrefix;
    // We need to fetch the table to determine if the user fully specified the partitions or
    // not, as it affects how we build the key.
    Table table = getTable(dbName, tableName);
    if (table == null) {
      throw new NoSuchObjectException("Unable to find table " + dbName + "." + tableName);
    }
    if (partVals.size() == table.getPartitionKeys().size()) {
      keyPrefix = HBaseUtils.buildKey(keyElements.toArray(new String[keyElements.size()]));
    } else {
      keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(keyElements.toArray(
          new String[keyElements.size()]));
    }

    // Now, build a filter out of the remaining keys
    String regex = null;
    if (!(partVals.size() == table.getPartitionKeys().size() && firstStar == -1)) {
      StringBuilder buf = new StringBuilder(".*");
      for (int i = Math.max(0, firstStar);
           i < table.getPartitionKeys().size() && i < partVals.size(); i++) {
        buf.append(HBaseUtils.KEY_SEPARATOR);
        if ("*".equals(partVals.get(i))) {
          buf.append("[^");
          buf.append(HBaseUtils.KEY_SEPARATOR);
          buf.append("]+");
        } else {
          buf.append(partVals.get(i));
        }
      }
      if (partVals.size() < table.getPartitionKeys().size()) {
        buf.append(HBaseUtils.KEY_SEPARATOR);
        buf.append(".*");
      }
      regex = buf.toString();
    }

    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning partitions with prefix <" + new String(keyPrefix) + "> and filter <" +
          regex + ">");
    }

    List<Partition> parts =
        scanPartitionsWithFilter(keyPrefix, CATALOG_CF, CATALOG_COL, maxPartitions, filter);
    partCache.put(dbName, tableName, parts, false);
    return parts;
  }

  /**
   * Delete a partition
   * @param dbName database name that table is in
   * @param tableName table partition is in
   * @param partVals partition values that define this partition, in the same order as the
   *                 partition columns they are values for
   * @throws IOException
   */
  void deletePartition(String dbName, String tableName, List<String> partVals) throws IOException {
    // Find the partition so I can get the storage descriptor and drop it
    partCache.remove(dbName, tableName, partVals);
    Partition p = getPartition(dbName, tableName, partVals, false);
    decrementStorageDescriptorRefCount(p.getSd());
    byte[] key = buildPartitionKey(dbName, tableName, partVals);
    delete(PART_TABLE, key, null, null);
    flush();
  }

  /**
   * Fetch a role
   * @param roleName name of the role
   * @return role object, or null if no such role
   * @throws IOException
   */
  Role getRole(String roleName) throws IOException {
    byte[] key = HBaseUtils.buildKey(roleName);
    byte[] serialized = read(ROLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    RoleWritable role = new RoleWritable();
    HBaseUtils.deserialize(role, serialized);
    return role.role;
  }

  /**
   * Get a list of roles.
   * @return list of all known roles.
   * @throws IOException
   */
  List<Role> scanRoles() throws IOException {
    Iterator<Result> iter = scanWithFilter(ROLE_TABLE, null, CATALOG_CF, CATALOG_COL, null);
    List<Role> roles = new ArrayList<Role>();
    while (iter.hasNext()) {
      RoleWritable role = new RoleWritable();
      HBaseUtils.deserialize(role, iter.next().getValue(CATALOG_CF, CATALOG_COL));
      roles.add(role.role);
    }
    return roles;
  }

  /**
   * Add a new role
   * @param role role object
   * @throws IOException
   */
  void putRole(Role role) throws IOException {
    byte[] key = HBaseUtils.buildKey(role.getRoleName());
    byte[] serialized = HBaseUtils.serialize(new RoleWritable(role));
    store(ROLE_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    flush();
  }

  /**
   * Drop a role
   * @param roleName name of role to drop
   * @throws IOException
   */
  void deleteRole(String roleName) throws IOException {
    byte[] key = HBaseUtils.buildKey(roleName);
    delete(ROLE_TABLE, key, null, null);
    flush();
  }

  /**
   * Fetch a table object
   * @param dbName database the table is in
   * @param tableName table name
   * @return Table object, or null if no such table
   * @throws IOException
   */
  Table getTable(String dbName, String tableName) throws IOException {
    return getTable(dbName, tableName, true);
  }

  /**
   * Fetch a list of table objects.
   * @param dbName Database that all fetched tables are in
   * @param tableNames list of table names
   * @return list of tables, in the same order as the provided names.
   * @throws IOException
   */
  List<Table> getTables(String dbName, List<String> tableNames) throws IOException {
    // I could implement getTable in terms of this method.  But it is such a core function
    // that I don't want to slow it down for the much less common fetching of multiple tables.
    List<Table> results = new ArrayList<Table>(tableNames.size());
    ObjectPair<String, String>[] hashKeys = new ObjectPair[tableNames.size()];
    boolean atLeastOneMissing = false;
    for (int i = 0; i < tableNames.size(); i++) {
      hashKeys[i] = new ObjectPair<String, String>(dbName, tableNames.get(i));
      // The result may be null, but we still want to add it so that we have a slot in the list
      // for it.
      results.add(tableCache.get(hashKeys[i]));
      if (results.get(i) == null) atLeastOneMissing = true;
    }
    if (!atLeastOneMissing) return results;

    // Now build a single get that will fetch the remaining tables
    List<Get> gets = new ArrayList<Get>();
    HTableInterface htab = getHTable(TABLE_TABLE);
    for (int i = 0; i < tableNames.size(); i++) {
      if (results.get(i) != null) continue;
      byte[] key = HBaseUtils.buildKey(dbName, tableNames.get(i));
      Get g = new Get(key);
      g.addColumn(CATALOG_CF, CATALOG_COL);
      gets.add(g);
    }
    Result[] res = htab.get(gets);
    for (int i = 0, nextGet = 0; i < tableNames.size(); i++) {
      if (results.get(i) != null) continue;
      byte[] serialized = res[nextGet++].getValue(CATALOG_CF, CATALOG_COL);
      if (serialized != null) {
        TableWritable table = new TableWritable();
        HBaseUtils.deserialize(table, serialized);
        tableCache.put(hashKeys[i], table.table);
        results.set(i, table.table);
      }
    }
    return results;
  }

  /**
   * Get a list of tables.
   * @param dbName Database these tables are in
   * @param regex Regular expression to use in searching for table names.  It is expected to
   *              be a Java regular expression.  If it is null then all tables in the indicated
   *              database will be returned.
   * @return list of tables matching the regular expression.
   * @throws IOException
   */
  List<Table> scanTables(String dbName, String regex) throws IOException {
    // There's no way to know whether all the tables we are looking for are
    // in the cache, so we would need to scan one way or another.  Thus there's no value in hitting
    // the cache for this function.
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    byte[] keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName);
    Iterator<Result> iter =
        scanWithFilter(TABLE_TABLE, keyPrefix, CATALOG_CF, CATALOG_COL, filter);
    List<Table> tables = new ArrayList<Table>();
    while (iter.hasNext()) {
      TableWritable table = new TableWritable();
      HBaseUtils.deserialize(table, iter.next().getValue(CATALOG_CF, CATALOG_COL));
      tables.add(table.table);
    }
    return tables;
  }

  /**
   * Put a table object
   * @param table table object
   * @throws IOException
   */
  void putTable(Table table) throws IOException {
    byte[] key = HBaseUtils.buildKey(table.getDbName(), table.getTableName());
    byte[] serialized = HBaseUtils.serialize(new TableWritable(table));
    store(TABLE_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
    flush();
    tableCache.put(new ObjectPair<String, String>(table.getDbName(), table.getTableName()), table);
  }

  /**
   * Delete a table
   * @param dbName name of database table is in
   * @param tableName table to drop
   * @throws IOException
   */
  void deleteTable(String dbName, String tableName) throws IOException {
    tableCache.remove(new ObjectPair<String, String>(dbName, tableName));
    // Find the table so I can get the storage descriptor and drop it
    Table t = getTable(dbName, tableName, false);
    decrementStorageDescriptorRefCount(t.getSd());
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    delete(TABLE_TABLE, key, null, null);
    flush();
  }

  /**
   * If this serde has already been read, then return it from the cache.  If not, read it, then
   * return it.
   * @param hash
   * @return
   * @throws IOException
   */
  StorageDescriptor getStorageDescriptor(byte[] hash) throws IOException {
    ByteArrayWrapper hashKey = new ByteArrayWrapper(hash);
    StorageDescriptor cached = sdCache.get(hashKey);
    if (cached != null) return cached;
    byte[] serialized = read(SD_TABLE, hash, CATALOG_CF, CATALOG_COL);
    if (serialized == null) {
      throw new RuntimeException("Woh, bad!  Trying to fetch a non-existent storage descriptor " +
          "from hash " + hash);
    }
    StorageDescriptor sd = new StorageDescriptor();
    HBaseUtils.deserializeStorageDescriptor(sd, serialized);
    sdCache.put(hashKey, sd);
    return sd;
  }

  /**
   * Lower the reference count on the storage descriptor by one.  If it goes to zero, then it
   * will be deleted.
   * @param sd Storage descriptor
   * @throws IOException
   */
  void decrementStorageDescriptorRefCount(StorageDescriptor sd) throws IOException {
    byte[] serialized = HBaseUtils.serializeStorageDescriptor(sd);
    byte[] key = hash(serialized);
    for (int i = 0; i < 10; i++) {
      byte[] serializedRefCnt = read(SD_TABLE, key, CATALOG_CF, REF_COUNT_COL);
      if (serializedRefCnt == null) {
        // Someone deleted it before we got to it, no worries
        return;
      }
      int refCnt = Integer.valueOf(new String(serializedRefCnt, HBaseUtils.ENCODING));
      HTableInterface htab = getHTable(SD_TABLE);
      if (refCnt-- < 1) {
        Delete d = new Delete(key);
        if (htab.checkAndDelete(key, CATALOG_CF, REF_COUNT_COL, serializedRefCnt, d)) {
          sdCache.remove(new ByteArrayWrapper(key));
          return;
        }
      } else {
        Put p = new Put(key);
        p.add(CATALOG_CF, REF_COUNT_COL, Integer.toString(refCnt).getBytes(HBaseUtils.ENCODING));
        if (htab.checkAndPut(key, CATALOG_CF, REF_COUNT_COL, serializedRefCnt, p)) {
          return;
        }
      }
    }
    throw new IOException("Too many unsuccessful attepts to decrement storage counter");
  }

  /**
   * Place the common parts of a storage descriptor into the cache.
   * @param storageDescriptor storage descriptor to store.
   * @return id of the entry in the cache, to be written in for the storage descriptor
   */
  byte[] putStorageDescriptor(StorageDescriptor storageDescriptor) throws IOException {
    byte[] sd = HBaseUtils.serializeStorageDescriptor(storageDescriptor);
    byte[] key = hash(sd);
    for (int i = 0; i < 10; i++) {
      byte[] serializedRefCnt = read(SD_TABLE, key, CATALOG_CF, REF_COUNT_COL);
      HTableInterface htab = getHTable(SD_TABLE);
      if (serializedRefCnt == null) {
        // We are the first to put it in the DB
        Put p = new Put(key);
        p.add(CATALOG_CF, CATALOG_COL, sd);
        p.add(CATALOG_CF, REF_COUNT_COL, "0".getBytes(HBaseUtils.ENCODING));
        if (htab.checkAndPut(key, CATALOG_CF, REF_COUNT_COL, null, p)) {
          sdCache.put(new ByteArrayWrapper(key), storageDescriptor);
          return key;
        }
      } else {
        // Just increment the reference count
        int refCnt = Integer.valueOf(new String(serializedRefCnt, HBaseUtils.ENCODING)) + 1;
        Put p = new Put(key);
        p.add(CATALOG_CF, REF_COUNT_COL, Integer.toString(refCnt).getBytes(HBaseUtils.ENCODING));
        if (htab.checkAndPut(key, CATALOG_CF, REF_COUNT_COL, serializedRefCnt, p)) {
          return key;
        }
      }
    }
    throw new IOException("Too many unsuccessful attepts to increment storage counter");
  }

  /**
   * Update statistics for one or more columns for a table or a partition.
   * @param dbName database the table is in
   * @param tableName table to update statistics for
   * @param partName name of the partition, can be null if these are table level statistics.
   * @param partVals partition values that define partition to update statistics for.  If this is
   *                 null, then these will be assumed to be table level statistics.
   * @param stats Stats object with stats for one or more columns.
   * @throws IOException
   */
  void updateStatistics(String dbName, String tableName, String partName, List<String> partVals,
                        ColumnStatistics stats) throws IOException {
    byte[] key = getStatisticsKey(dbName, tableName, partVals);
    String hbaseTable = getStatisticsTable(partVals);

    byte[][] colnames = new byte[stats.getStatsObjSize()][];
    byte[][] serializeds = new byte[stats.getStatsObjSize()][];
    for (int i = 0; i < stats.getStatsObjSize(); i++) {
      ColumnStatisticsObj obj = stats.getStatsObj().get(i);
      serializeds[i] = HBaseUtils.serializeStatsForOneColumn(stats, obj);
      String colname = obj.getColName();
      colnames[i] = HBaseUtils.buildKey(colname);
      statsCache.put(dbName, tableName, partName, colname, obj,
          stats.getStatsDesc().getLastAnalyzed());
    }
    store(hbaseTable, key, STATS_CF, colnames, serializeds);
    flush();
  }

  /**
   * Get Statistics for a table
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param colNames list of column names to get statistics for
   * @return column statistics for indicated table
   * @throws IOException
   */
  ColumnStatistics getTableStatistics(String dbName, String tableName, List<String> colNames)
      throws IOException {
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    ColumnStatistics stats = new ColumnStatistics();
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
    desc.setIsTblLevel(true);
    desc.setDbName(dbName);
    desc.setTableName(tableName);
    stats.setStatsDesc(desc);

    // First we have to go through and see what's in the cache and fetch what we can from there.
    // Then we'll fetch the rest from HBase
    List<String> stillLookingFor = new ArrayList<String>();
    for (int i = 0; i < colNames.size(); i++) {
      StatsCache.StatsInfo info =
          statsCache.getTableStatistics(dbName, tableName, colNames.get(i));
      if (info == null) {
        stillLookingFor.add(colNames.get(i));
      } else {
        info.stats.setColName(colNames.get(i));
        stats.addToStatsObj(info.stats);
        stats.getStatsDesc().setLastAnalyzed(Math.max(stats.getStatsDesc().getLastAnalyzed(),
            info.lastAnalyzed));
      }
    }
    if (stillLookingFor.size() == 0) return stats;

    byte[][] colKeys = new byte[stillLookingFor.size()][];
    for (int i = 0; i < colKeys.length; i++) {
      colKeys[i] = HBaseUtils.buildKey(stillLookingFor.get(i));
    }
    Result res = read(TABLE_TABLE, key, STATS_CF, colKeys);
    for (int i = 0; i < colKeys.length; i++) {
      byte[] serialized = res.getValue(STATS_CF, colKeys[i]);
      if (serialized == null) {
        // There were no stats for this column, so skip it
        continue;
      }
      ColumnStatisticsObj obj = HBaseUtils.deserializeStatsForOneColumn(stats, serialized);
      statsCache.put(dbName, tableName, null, stillLookingFor.get(i), obj,
          stats.getStatsDesc().getLastAnalyzed());
      obj.setColName(stillLookingFor.get(i));
      stats.addToStatsObj(obj);
    }
    return stats;
  }

  /**
   * Get statistics for a set of partitions
   * @param dbName name of database table is in
   * @param tableName table partitions are in
   * @param partNames names of the partitions, used only to set values inside the return stats
   *                  objects.
   * @param partVals partition values for each partition, needed because this class doesn't know
   *                 how to translate from partName to partVals
   * @param colNames column names to fetch stats for.  These columns will be fetched for all
   *                 requested partitions.
   * @return list of ColumnStats, one for each partition.  The values will be in the same order
   * as the partNames list that was passed in.
   * @throws IOException
   */
  List<ColumnStatistics> getPartitionStatistics(String dbName, String tableName,
                                                List<String> partNames,
                                                List<List<String>> partVals,
                                                List<String> colNames) throws IOException {
    // Go through the cache first, see what we can fetch from there.  This is complicated because
    // we may have different columns for different partitions
    List<ColumnStatistics> statsList = new ArrayList<ColumnStatistics>(partNames.size());
    List<PartStatsInfo> stillLookingFor = new ArrayList<PartStatsInfo>();
    for (int pOff = 0; pOff < partVals.size(); pOff++) {
      // Add an entry for this partition in the list
      ColumnStatistics stats = new ColumnStatistics();
      ColumnStatisticsDesc desc = new ColumnStatisticsDesc();
      desc.setIsTblLevel(false);
      desc.setDbName(dbName);
      desc.setTableName(tableName);
      desc.setPartName(partNames.get(pOff));
      stats.setStatsDesc(desc);
      statsList.add(stats);
      PartStatsInfo missing = null;

      for (int cOff = 0; cOff < colNames.size(); cOff++) {
        StatsCache.StatsInfo info = statsCache.getPartitionStatistics(dbName, tableName,
            partNames.get(pOff), colNames.get(cOff));
        if (info == null) {
          if (missing == null) {
            // We haven't started an entry for this one yet
            missing = new PartStatsInfo(stats, partVals.get(pOff), partNames.get(pOff));
            stillLookingFor.add(missing);
          }
          missing.colNames.add(colNames.get(cOff));
        } else {
          info.stats.setColName(colNames.get(cOff));
          stats.addToStatsObj(info.stats);
          stats.getStatsDesc().setLastAnalyzed(Math.max(stats.getStatsDesc().getLastAnalyzed(),
              info.lastAnalyzed));
        }
      }
    }
    if (stillLookingFor.size() == 0) return statsList;

    // Build the list of gets. It may be different for each partition now depending on what we
    // found in the cache.
    List<Get> gets = new ArrayList<Get>();
    for (PartStatsInfo pi : stillLookingFor) {
      byte[][] colKeys = new byte[pi.colNames.size()][];
      for (int i = 0; i < colKeys.length; i++) {
        colKeys[i] = HBaseUtils.buildKey(pi.colNames.get(i));
      }
      pi.colKeys = colKeys;

      byte[] key = buildPartitionKey(dbName, tableName, pi.partVals);
      Get g = new Get(key);
      for (byte[] colName : colKeys) g.addColumn(STATS_CF, colName);
      gets.add(g);
    }
    HTableInterface htab = getHTable(PART_TABLE);
    Result[] results = htab.get(gets);

    for (int pOff = 0; pOff < results.length; pOff++) {
      PartStatsInfo pi = stillLookingFor.get(pOff);
      for (int cOff = 0; cOff < pi.colNames.size(); cOff++) {
        byte[] serialized = results[pOff].getValue(STATS_CF, pi.colKeys[cOff]);
        if (serialized == null) {
          // There were no stats for this column, so skip it
          continue;
        }
        ColumnStatisticsObj obj = HBaseUtils.deserializeStatsForOneColumn(pi.stats, serialized);
        statsCache.put(dbName, tableName, pi.partName, pi.colNames.get(cOff), obj,
            pi.stats.getStatsDesc().getLastAnalyzed());
        obj.setColName(pi.colNames.get(cOff));
        pi.stats.addToStatsObj(obj);
      }
    }
    return statsList;
  }

  /**
   * This should be called whenever a new query is started.
   */
  void flushCatalogCache() {
    for (Counter counter : counters) {
      LOG.debug(counter.dump());
      counter.clear();
    }
    tableCache.flush();
    sdCache.flush();
    partCache.flush();
  }

  @VisibleForTesting
  int countStorageDescriptor() throws IOException {
    ResultScanner scanner = getHTable(SD_TABLE).getScanner(new Scan());
    int cnt = 0;
    while (scanner.next() != null) cnt++;
    return cnt;
  }

  private Table getTable(String dbName, String tableName, boolean populateCache)
      throws IOException {
    ObjectPair<String, String> hashKey = new ObjectPair<String, String>(dbName, tableName);
    Table cached = tableCache.get(hashKey);
    if (cached != null) return cached;
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    byte[] serialized = read(TABLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    TableWritable table = new TableWritable();
    HBaseUtils.deserialize(table, serialized);
    if (populateCache) tableCache.put(hashKey, table.table);
    return table.table;
  }

  private Partition getPartition(String dbName, String tableName, List<String> partVals,
                                 boolean populateCache) throws IOException {
    Partition cached = partCache.get(dbName, tableName, partVals);
    if (cached != null) return cached;
    byte[] key = buildPartitionKey(dbName, tableName, partVals);
    byte[] serialized = read(PART_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    PartitionWritable part = new PartitionWritable();
    HBaseUtils.deserialize(part, serialized);
    if (populateCache) partCache.put(dbName, tableName, part.part);
    return part.part;
  }

  private void store(String table, byte[] key, byte[] colFam, byte[] colName, byte[] obj)
      throws IOException {
    HTableInterface htab = getHTable(table);
    Put p = new Put(key);
    p.add(colFam, colName, obj);
    htab.put(p);
  }

  private void store(String table, byte[] key, byte[] colFam, byte[][] colName, byte[][] obj)
      throws IOException {
    HTableInterface htab = getHTable(table);
    Put p = new Put(key);
    for (int i = 0; i < colName.length; i++) {
      p.add(colFam, colName[i], obj[i]);
    }
    htab.put(p);
  }

  private byte[] read(String table, byte[] key, byte[] colFam, byte[] colName) throws IOException {
    HTableInterface htab = getHTable(table);
    Get g = new Get(key);
    g.addColumn(colFam, colName);
    Result res = htab.get(g);
    return res.getValue(colFam, colName);
  }

  private Result read(String table, byte[] key, byte[] colFam, byte[][] colNames)
      throws IOException {
    HTableInterface htab = getHTable(table);
    Get g = new Get(key);
    for (byte[] colName : colNames) g.addColumn(colFam, colName);
    return htab.get(g);
  }

  // Delete a row.  If colFam and colName are not null, then only the named column will be
  // deleted.  If colName is null and colFam is not, only the named family will be deleted.  If
  // both are null the entire row will be deleted.
  private void delete(String table, byte[] key, byte[] colFam, byte[] colName) throws IOException {
    HTableInterface htab = getHTable(table);
    Delete d = new Delete(key);
    if (colName != null) d.deleteColumn(colFam, colName);
    else if (colFam != null) d.deleteFamily(colFam);
    htab.delete(d);
  }

  private List<Partition> scanPartitions(byte[] keyPrefix, byte[] colFam, byte[] colName,
                                         int maxResults) throws IOException {
    return scanPartitionsWithFilter(keyPrefix, colFam, colName, maxResults, null);
  }

  private List<Partition> scanPartitionsWithFilter(byte[] keyPrefix, byte[] colFam, byte[] colName,
                                                   int maxResults, Filter filter)
      throws IOException {
    Iterator<Result> iter =
        scanWithFilter(PART_TABLE, keyPrefix, colFam, colName, filter);
    List<Partition> parts = new ArrayList<Partition>();
    int numToFetch = maxResults < 0 ? Integer.MAX_VALUE : maxResults;
    for (int i = 0; i < numToFetch && iter.hasNext(); i++) {
      PartitionWritable p = new PartitionWritable();
      HBaseUtils.deserialize(p, iter.next().getValue(colFam, colName));
      parts.add(p.part);
    }
    return parts;
  }

  private Iterator<Result> scanWithFilter(String table, byte[] keyPrefix, byte[] colFam,
                                          byte[] colName, Filter filter) throws IOException {
    HTableInterface htab = getHTable(table);
    Scan s;
    if (keyPrefix == null) {
      s = new Scan();
    } else {
      byte[] stop = Arrays.copyOf(keyPrefix, keyPrefix.length);
      stop[stop.length - 1]++;
      s = new Scan(keyPrefix, stop);
    }
    s.addColumn(colFam, colName);
    if (filter != null) s.setFilter(filter);
    ResultScanner scanner = htab.getScanner(s);
    return scanner.iterator();
  }

  private HTableInterface getHTable(String table) throws IOException {
    HTableInterface htab = tables.get(table);
    if (htab == null) {
      LOG.debug("Trying to connect to table " + table);
      try {
        htab = conn.getTable(table);
        // Calling gettable doesn't actually connect to the region server, it's very light
        // weight, so call something else so we actually reach out and touch the region server
        // and see if the table is there.
        Result r = htab.get(new Get("nosuchkey".getBytes(HBaseUtils.ENCODING)));
      } catch (IOException e) {
        LOG.info("Caught exception when table was missing");
        return null;
      }
      htab.setAutoFlushTo(false);
      tables.put(table, htab);
    }
    return htab;
  }

  private void flush() throws IOException {
    for (HTableInterface htab : tables.values()) htab.flushCommits();
  }

  private byte[] buildPartitionKey(String dbName, String tableName, List<String> partVals) {
    Deque<String> keyParts = new ArrayDeque<String>(partVals);
    keyParts.addFirst(tableName);
    keyParts.addFirst(dbName);
    return HBaseUtils.buildKey(keyParts.toArray(new String[keyParts.size()]));
  }

  private byte[] buildPartitionKey(PartitionWritable part) throws IOException {
    Deque<String> keyParts = new ArrayDeque<String>(part.part.getValues());
    keyParts.addFirst(part.part.getTableName());
    keyParts.addFirst(part.part.getDbName());
    return HBaseUtils.buildKey(keyParts.toArray(new String[keyParts.size()]));
  }

  private byte[] hash(byte[] serialized) throws IOException {
    md.update(serialized);
    return md.digest();
  }

  private byte[] getStatisticsKey(String dbName, String tableName, List<String> partVals) {
    return partVals == null ?
        HBaseUtils.buildKey(dbName, tableName) :
        buildPartitionKey(dbName, tableName, partVals);
  }

  private String getStatisticsTable(List<String> partVals) {
    return partVals == null ? TABLE_TABLE : PART_TABLE;
  }

  /**
   * Use this for unit testing only, so that a mock connection object can be passed in.
   * @param connection Mock connection objecct
   */
  @VisibleForTesting
  void setConnection(HConnection connection) {
    conn = connection;
  }

  private static class ByteArrayWrapper {
    byte[] wrapped;

    ByteArrayWrapper(byte[] b) {
      wrapped = b;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof ByteArrayWrapper) {
        return Arrays.equals(((ByteArrayWrapper)other).wrapped, wrapped);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(wrapped);
    }
  }

  private static class PartStatsInfo {
    ColumnStatistics stats;
    String partName;
    List<String> colNames;
    List<String> partVals;
    byte[][] colKeys;

    PartStatsInfo(ColumnStatistics s, List<String> pv, String pn) {
      stats = s; partVals = pv; partName = pn;
      colNames = new ArrayList<String>();
      colKeys = null;
    }
  }

  // For testing without the cache
  private static class BogusObjectCache<K, V> extends ObjectCache<K, V> {
    static Counter bogus = new Counter("bogus");

   BogusObjectCache() {
      super(1, bogus, bogus, bogus);
    }

    @Override
    V get(K key) {
      return null;
    }
  }

  private static class BogusPartitionCache extends PartitionCache {
    static Counter bogus = new Counter("bogus");

    BogusPartitionCache() {
      super(1, bogus, bogus, bogus);
    }

    @Override
    Collection<Partition> getAllForTable(String dbName, String tableName) {
      return null;
    }

    @Override
    Partition get(String dbName, String tableName, List<String> partVals) {
      return null;
    }
  }
}
