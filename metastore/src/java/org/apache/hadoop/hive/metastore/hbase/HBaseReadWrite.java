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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Class to manage storing object in and reading them from HBase.
 */
class HBaseReadWrite {

  @VisibleForTesting final static String DB_TABLE = "HBMS_DBS";
  @VisibleForTesting final static String FUNC_TABLE = "HBMS_FUNCS";
  @VisibleForTesting final static String GLOBAL_PRIVS_TABLE = "HBMS_GLOBAL_PRIVS";
  @VisibleForTesting final static String PART_TABLE = "HBMS_PARTITIONS";
  @VisibleForTesting final static String ROLE_TABLE = "HBMS_ROLES";
  @VisibleForTesting final static String SD_TABLE = "HBMS_SDS";
  @VisibleForTesting final static String TABLE_TABLE = "HBMS_TBLS";
  @VisibleForTesting final static String USER_TO_ROLE_TABLE = "HBMS_USER_TO_ROLE";
  @VisibleForTesting final static byte[] CATALOG_CF = "c".getBytes(HBaseUtils.ENCODING);
  @VisibleForTesting final static byte[] STATS_CF = "s".getBytes(HBaseUtils.ENCODING);
  @VisibleForTesting final static String NO_CACHE_CONF = "no.use.cache";
  private final static byte[] CATALOG_COL = "cat".getBytes(HBaseUtils.ENCODING);
  private final static byte[] ROLES_COL = "roles".getBytes(HBaseUtils.ENCODING);
  private final static byte[] REF_COUNT_COL = "ref".getBytes(HBaseUtils.ENCODING);
  private final static byte[] GLOBAL_PRIVS_KEY = "globalprivs".getBytes(HBaseUtils.ENCODING);
  private final static int TABLES_TO_CACHE = 10;

  @VisibleForTesting final static String TEST_CONN = "test_connection";
  private static HBaseConnection testConn;

  private final static String[] tableNames = { DB_TABLE, FUNC_TABLE, GLOBAL_PRIVS_TABLE, PART_TABLE,
      USER_TO_ROLE_TABLE, ROLE_TABLE, SD_TABLE, TABLE_TABLE  };
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

  private final Configuration conf;
  private HBaseConnection conn;
  private MessageDigest md;
  private ObjectCache<ObjectPair<String, String>, Table> tableCache;
  private ObjectCache<ByteArrayWrapper, StorageDescriptor> sdCache;
  private PartitionCache partCache;
  private StatsCache statsCache;
  private final Counter tableHits;
  private final Counter tableMisses;
  private final Counter tableOverflows;
  private final Counter partHits;
  private final Counter partMisses;
  private final Counter partOverflows;
  private final Counter sdHits;
  private final Counter sdMisses;
  private final Counter sdOverflows;
  private final List<Counter> counters;
  // roleCache doesn't use ObjectCache because I don't want to limit the size.  I am assuming
  // that the number of roles will always be small (< 100) so caching the whole thing should not
  // be painful.
  private final Map<String, HbaseMetastoreProto.RoleGrantInfoList> roleCache;
  boolean entireRoleTableInCache;

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
      String connClass = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_HBASE_CONNECTION_CLASS);
      if (TEST_CONN.equals(connClass)) {
        conn = testConn;
        LOG.debug("Using test connection.");
      } else {
        LOG.debug("Instantiating connection class " + connClass);
        Class c = Class.forName(connClass);
        Object o = c.newInstance();
        if (HBaseConnection.class.isAssignableFrom(o.getClass())) {
          conn = (HBaseConnection) o;
        } else {
          throw new IOException(connClass + " is not an instance of HBaseConnection.");
        }
        conn.setConf(conf);
        conn.connect();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

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

    roleCache = new HashMap<String, HbaseMetastoreProto.RoleGrantInfoList>();
    entireRoleTableInCache = false;
  }

  // Synchronize this so not everyone's doing it at once.
  static synchronized void createTablesIfNotExist() throws IOException {
    if (!tablesCreated) {
      for (String name : tableNames) {
        if (self.get().conn.getHBaseTable(name, true) == null) {
          List<byte[]> columnFamilies = new ArrayList<byte[]>();
          columnFamilies.add(CATALOG_CF);
          if (TABLE_TABLE.equals(name) || PART_TABLE.equals(name)) {
            columnFamilies.add(STATS_CF);
          }
          self.get().conn.createHBaseTable(name, columnFamilies);
        }
      }
      tablesCreated = true;
    }
  }

  /**********************************************************************************************
   * Transaction related methods
   *********************************************************************************************/

  /**
   * Begin a transaction
   */
  void begin() {
    try {
      conn.beginTransaction();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Commit a transaction
   */
  void commit() {
    try {
      conn.commitTransaction();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void rollback() {
    try {
      conn.rollbackTransaction();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void close() throws IOException {
    conn.close();
  }

  /**********************************************************************************************
   * Database related methods
   *********************************************************************************************/

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
    return HBaseUtils.deserializeDatabase(name, serialized);
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
        scan(DB_TABLE, CATALOG_CF, CATALOG_COL, filter);
    List<Database> databases = new ArrayList<Database>();
    while (iter.hasNext()) {
      Result result = iter.next();
      databases.add(HBaseUtils.deserializeDatabase(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL)));
    }
    return databases;
  }

  /**
   * Store a database object
   * @param database database object to store
   * @throws IOException
   */
  void putDb(Database database) throws IOException {
    byte[][] serialized = HBaseUtils.serializeDatabase(database);
    store(DB_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
  }

  /**
   * Drop a database
   * @param name name of db to drop
   * @throws IOException
   */
  void deleteDb(String name) throws IOException {
    byte[] key = HBaseUtils.buildKey(name);
    delete(DB_TABLE, key, null, null);
  }

  /**********************************************************************************************
   * Function related methods
   *********************************************************************************************/

  /**
   * Fetch a function object
   * @param dbName name of the database the function is in
   * @param functionName name of the function to fetch
   * @return the function object, or null if there is no such function
   * @throws IOException
   */
  Function getFunction(String dbName, String functionName) throws IOException {
    byte[] key = HBaseUtils.buildKey(dbName, functionName);
    byte[] serialized = read(FUNC_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeFunction(dbName, functionName, serialized);
  }

  /**
   * Get a list of functions.
   * @param dbName Name of the database to search in.
   * @param regex Regular expression to use in searching for function names.  It is expected to
   *              be a Java regular expression.  If it is null then all functions will be returned.
   * @return list of functions matching the regular expression.
   * @throws IOException
   */
  List<Function> scanFunctions(String dbName, String regex) throws IOException {
    byte[] keyPrefix = null;
    if (dbName != null) {
      keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName);
    }
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    Iterator<Result> iter =
        scan(FUNC_TABLE, keyPrefix, HBaseUtils.getEndPrefix(keyPrefix), CATALOG_CF, CATALOG_COL, filter);
    List<Function> functions = new ArrayList<Function>();
    while (iter.hasNext()) {
      Result result = iter.next();
      functions.add(HBaseUtils.deserializeFunction(result.getRow(),
                                                   result.getValue(CATALOG_CF, CATALOG_COL)));
    }
    return functions;
  }

  /**
   * Store a function object
   * @param function function object to store
   * @throws IOException
   */
  void putFunction(Function function) throws IOException {
    byte[][] serialized = HBaseUtils.serializeFunction(function);
    store(FUNC_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
  }

  /**
   * Drop a function
   * @param dbName name of database the function is in
   * @param functionName name of function to drop
   * @throws IOException
   */
  void deleteFunction(String dbName, String functionName) throws IOException {
    byte[] key = HBaseUtils.buildKey(dbName, functionName);
    delete(FUNC_TABLE, key, null, null);
  }

  /**********************************************************************************************
   * Global privilege related methods
   *********************************************************************************************/

  /**
   * Fetch the global privileges object
   * @return
   * @throws IOException
   */
  PrincipalPrivilegeSet getGlobalPrivs() throws IOException {
    byte[] key = GLOBAL_PRIVS_KEY;
    byte[] serialized = read(GLOBAL_PRIVS_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializePrincipalPrivilegeSet(serialized);
  }

  /**
   * Store the global privileges object
   * @throws IOException
   */
  void putGlobalPrivs(PrincipalPrivilegeSet privs) throws IOException {
    byte[] key = GLOBAL_PRIVS_KEY;
    byte[] serialized = HBaseUtils.serializePrincipalPrivilegeSet(privs);
    store(GLOBAL_PRIVS_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
  }

  /**********************************************************************************************
   * Partition related methods
   *********************************************************************************************/

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
   * Get a set of specific partitions.  This cannot be used to do a scan, each partition must be
   * completely specified.  This does not use the partition cache.
   * @param dbName database table is in
   * @param tableName table partitions are in
   * @param partValLists list of list of values, each list should uniquely identify one partition
   * @return a list of partition objects.
   * @throws IOException
   */
   List<Partition> getPartitions(String dbName, String tableName, List<List<String>> partValLists)
       throws IOException {
     List<Partition> parts = new ArrayList<Partition>(partValLists.size());
     List<Get> gets = new ArrayList<Get>(partValLists.size());
     for (List<String> partVals : partValLists) {
       byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, partVals);
       Get get = new Get(key);
       get.addColumn(CATALOG_CF, CATALOG_COL);
       gets.add(get);
     }
     HTableInterface htab = conn.getHBaseTable(PART_TABLE);
     Result[] results = htab.get(gets);
     for (int i = 0; i < results.length; i++) {
       HBaseUtils.StorageDescriptorParts sdParts =
           HBaseUtils.deserializePartition(dbName, tableName, partValLists.get(i),
               results[i].getValue(CATALOG_CF, CATALOG_COL));
       StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
       HBaseUtils.assembleStorageDescriptor(sd, sdParts);
       parts.add(sdParts.containingPartition);
     }

     return parts;
  }

  /**
   * Add a partition.  This should only be called for new partitions.  For altering existing
   * partitions this should not be called as it will blindly increment the ref counter for the
   * storage descriptor.
   * @param partition partition object to add
   * @throws IOException
   */
  void putPartition(Partition partition) throws IOException {
    byte[] hash = putStorageDescriptor(partition.getSd());
    byte[][] serialized = HBaseUtils.serializePartition(partition, hash);
    store(PART_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    partCache.put(partition.getDbName(), partition.getTableName(), partition);
  }

  /**
   * Replace an existing partition.
   * @param oldPart partition to be replaced
   * @param newPart partitiion to replace it with
   * @throws IOException
   */
  void replacePartition(Partition oldPart, Partition newPart) throws IOException {
    byte[] hash;
    byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldPart.getSd(), md);
    byte[] newHash = HBaseUtils.hashStorageDescriptor(newPart.getSd(), md);
    if (Arrays.equals(oldHash, newHash)) {
      hash = oldHash;
    } else {
      decrementStorageDescriptorRefCount(oldPart.getSd());
      hash = putStorageDescriptor(newPart.getSd());
    }
    byte[][] serialized = HBaseUtils.serializePartition(newPart, hash);
    store(PART_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    partCache.put(newPart.getDbName(), newPart.getTableName(), newPart);
  }

  /**
   * Add a group of partitions.  This should only be used when all partitions are new.  It
   * blindly increments the ref count on the storage descriptor.
   * @param partitions list of partitions to add
   * @throws IOException
   */
  void putPartitions(List<Partition> partitions) throws IOException {
    List<Put> puts = new ArrayList<Put>(partitions.size());
    for (Partition partition : partitions) {
      byte[] hash = putStorageDescriptor(partition.getSd());
      byte[][] serialized = HBaseUtils.serializePartition(partition, hash);
      Put p = new Put(serialized[0]);
      p.add(CATALOG_CF, CATALOG_COL, serialized[1]);
      puts.add(p);
      partCache.put(partition.getDbName(), partition.getTableName(), partition);
    }
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    htab.put(puts);
    htab.flushCommits();
  }

  void replacePartitions(List<Partition> oldParts, List<Partition> newParts) throws IOException {
    if (oldParts.size() != newParts.size()) {
      throw new RuntimeException("Number of old and new partitions must match.");
    }
    List<Put> puts = new ArrayList<Put>(newParts.size());
    for (int i = 0; i < newParts.size(); i++) {
      byte[] hash;
      byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldParts.get(i).getSd(), md);
      byte[] newHash = HBaseUtils.hashStorageDescriptor(newParts.get(i).getSd(), md);
      if (Arrays.equals(oldHash, newHash)) {
        hash = oldHash;
      } else {
        decrementStorageDescriptorRefCount(oldParts.get(i).getSd());
        hash = putStorageDescriptor(newParts.get(i).getSd());
      }
      byte[][] serialized = HBaseUtils.serializePartition(newParts.get(i), hash);
      Put p = new Put(serialized[0]);
      p.add(CATALOG_CF, CATALOG_COL, serialized[1]);
      puts.add(p);
      partCache.put(newParts.get(i).getDbName(), newParts.get(i).getTableName(), newParts.get(i));
    }
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
    htab.put(puts);
    htab.flushCommits();
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
    List<Partition> parts = scanPartitionsWithFilter(keyPrefix, HBaseUtils.getEndPrefix(keyPrefix), -1, null);
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

    List<Partition> parts = scanPartitionsWithFilter(keyPrefix, HBaseUtils.getEndPrefix(keyPrefix), maxPartitions, filter);
    partCache.put(dbName, tableName, parts, false);
    return parts;
  }

  List<Partition> scanPartitions(String dbName, String tableName, byte[] keyStart, byte[] keyEnd,
      Filter filter, int maxPartitions) throws IOException, NoSuchObjectException {
    List<String> keyElements = new ArrayList<String>();
    keyElements.add(dbName);
    keyElements.add(tableName);

    byte[] keyPrefix =
        HBaseUtils.buildKeyWithTrailingSeparator(keyElements.toArray(new String[keyElements.size()]));
    byte[] startRow = ArrayUtils.addAll(keyPrefix, keyStart);
    byte[] endRow;
    if (keyEnd == null || keyEnd.length == 0) {
      // stop when current db+table entries are over
      endRow = HBaseUtils.getEndPrefix(keyPrefix);
    } else {
      endRow = ArrayUtils.addAll(keyPrefix, keyEnd);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning partitions with start row <" + new String(startRow) + "> and end row <"
          + new String(endRow) + ">");
    }
    return scanPartitionsWithFilter(startRow, endRow, maxPartitions, filter);
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
    byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, partVals);
    delete(PART_TABLE, key, null, null);
  }

  private Partition getPartition(String dbName, String tableName, List<String> partVals,
                                 boolean populateCache) throws IOException {
    Partition cached = partCache.get(dbName, tableName, partVals);
    if (cached != null) return cached;
    byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, partVals);
    byte[] serialized = read(PART_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    HBaseUtils.StorageDescriptorParts sdParts =
        HBaseUtils.deserializePartition(dbName, tableName, partVals, serialized);
    StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
    HBaseUtils.assembleStorageDescriptor(sd, sdParts);
    if (populateCache) partCache.put(dbName, tableName, sdParts.containingPartition);
    return sdParts.containingPartition;
  }

  private List<Partition> scanPartitionsWithFilter(byte[] startRow, byte [] endRow,
      int maxResults, Filter filter)
      throws IOException {
    Iterator<Result> iter =
        scan(PART_TABLE, startRow, endRow, CATALOG_CF, CATALOG_COL, filter);
    List<Partition> parts = new ArrayList<Partition>();
    int numToFetch = maxResults < 0 ? Integer.MAX_VALUE : maxResults;
    for (int i = 0; i < numToFetch && iter.hasNext(); i++) {
      Result result = iter.next();
      HBaseUtils.StorageDescriptorParts sdParts = HBaseUtils.deserializePartition(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL));
      StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
      HBaseUtils.assembleStorageDescriptor(sd, sdParts);
      parts.add(sdParts.containingPartition);
    }
    return parts;
  }

  /**********************************************************************************************
   * Role related methods
   *********************************************************************************************/

  /**
   * Fetch the list of all roles for a user
   * @param userName name of the user
   * @return the list of all roles this user participates in
   * @throws IOException
   */
  List<String> getUserRoles(String userName) throws IOException {
    byte[] key = HBaseUtils.buildKey(userName);
    byte[] serialized = read(USER_TO_ROLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    return HBaseUtils.deserializeRoleList(serialized);
  }

  /**
   * Find all roles directly participated in by a given principal.  This builds the role cache
   * because it assumes that subsequent calls may be made to find roles participated in indirectly.
   * @param name username or role name
   * @param type user or role
   * @return map of role name to grant info for all roles directly participated in.
   */
  List<Role> getPrincipalDirectRoles(String name, PrincipalType type)
      throws IOException {
    buildRoleCache();

    Set<String> rolesFound = new HashSet<String>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo giw : e.getValue().getGrantInfoList()) {
        if (HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()) == type &&
            giw.getPrincipalName().equals(name)) {
          rolesFound.add(e.getKey());
          break;
        }
      }
    }
    List<Role> directRoles = new ArrayList<Role>(rolesFound.size());
    List<Get> gets = new ArrayList<Get>();
    HTableInterface htab = conn.getHBaseTable(ROLE_TABLE);
    for (String roleFound : rolesFound) {
      byte[] key = HBaseUtils.buildKey(roleFound);
      Get g = new Get(key);
      g.addColumn(CATALOG_CF, CATALOG_COL);
      gets.add(g);
    }

    Result[] results = htab.get(gets);
    for (int i = 0; i < results.length; i++) {
      byte[] serialized = results[i].getValue(CATALOG_CF, CATALOG_COL);
      if (serialized != null) {
        directRoles.add(HBaseUtils.deserializeRole(results[i].getRow(), serialized));
      }
    }

    return directRoles;
  }

  /**
   * Fetch all roles and users included directly in a given role.
   * @param roleName name of the principal
   * @return a list of all roles included in this role
   * @throws IOException
   */
  HbaseMetastoreProto.RoleGrantInfoList getRolePrincipals(String roleName)
      throws IOException, NoSuchObjectException {
    HbaseMetastoreProto.RoleGrantInfoList rolePrincipals = roleCache.get(roleName);
    if (rolePrincipals != null) return rolePrincipals;
    byte[] key = HBaseUtils.buildKey(roleName);
    byte[] serialized = read(ROLE_TABLE, key, CATALOG_CF, ROLES_COL);
    if (serialized == null) return null;
    rolePrincipals = HbaseMetastoreProto.RoleGrantInfoList.parseFrom(serialized);
    roleCache.put(roleName, rolePrincipals);
    return rolePrincipals;
  }

  /**
   * Given a role, find all users who are either directly or indirectly participate in this role.
   * This is expensive, it should be used sparingly.  It scan the entire userToRole table and
   * does a linear search on each entry.
   * @param roleName name of the role
   * @return set of all users in the role
   * @throws IOException
   */
  Set<String> findAllUsersInRole(String roleName) throws IOException {
    // Walk the userToRole table and collect every user that matches this role.
    Set<String> users = new HashSet<String>();
    Iterator<Result> iter = scan(USER_TO_ROLE_TABLE, CATALOG_CF, CATALOG_COL);
    while (iter.hasNext()) {
      Result result = iter.next();
      List<String> roleList =
          HBaseUtils.deserializeRoleList(result.getValue(CATALOG_CF, CATALOG_COL));
      for (String rn : roleList) {
        if (rn.equals(roleName)) {
          users.add(new String(result.getRow(), HBaseUtils.ENCODING));
          break;
        }
      }
    }
    return users;
  }

  /**
   * Add a principal to a role.
   * @param roleName name of the role to add principal to
   * @param grantInfo grant information for this principal.
   * @throws java.io.IOException
   * @throws NoSuchObjectException
   *
   */
  void addPrincipalToRole(String roleName, HbaseMetastoreProto.RoleGrantInfo grantInfo)
      throws IOException, NoSuchObjectException {
    HbaseMetastoreProto.RoleGrantInfoList proto = getRolePrincipals(roleName);
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals =
        new ArrayList<HbaseMetastoreProto.RoleGrantInfo>();
    if (proto != null) {
      rolePrincipals.addAll(proto.getGrantInfoList());
    }

    rolePrincipals.add(grantInfo);
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    byte[] key = HBaseUtils.buildKey(roleName);
    store(ROLE_TABLE, key, CATALOG_CF, ROLES_COL, proto.toByteArray());
    roleCache.put(roleName, proto);
  }

  /**
   * Drop a principal from a role.
   * @param roleName Name of the role to drop the principal from
   * @param principalName name of the principal to drop from the role
   * @param type user or role
   * @param grantOnly if this is true, just remove the grant option, don't actually remove the
   *                  user from the role.
   * @throws NoSuchObjectException
   * @throws IOException
   */
  void dropPrincipalFromRole(String roleName, String principalName, PrincipalType type,
                             boolean grantOnly)
      throws NoSuchObjectException, IOException {
    HbaseMetastoreProto.RoleGrantInfoList proto = getRolePrincipals(roleName);
    if (proto == null) return;
    List<HbaseMetastoreProto.RoleGrantInfo> rolePrincipals =
        new ArrayList<HbaseMetastoreProto.RoleGrantInfo>();
    rolePrincipals.addAll(proto.getGrantInfoList());

    for (int i = 0; i < rolePrincipals.size(); i++) {
      if (HBaseUtils.convertPrincipalTypes(rolePrincipals.get(i).getPrincipalType()) == type &&
          rolePrincipals.get(i).getPrincipalName().equals(principalName)) {
        if (grantOnly) {
          rolePrincipals.set(i,
              HbaseMetastoreProto.RoleGrantInfo.newBuilder(rolePrincipals.get(i))
                  .setGrantOption(false)
                  .build());
        } else {
          rolePrincipals.remove(i);
        }
        break;
      }
    }
    byte[] key = HBaseUtils.buildKey(roleName);
    proto = HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
        .addAllGrantInfo(rolePrincipals)
        .build();
    store(ROLE_TABLE, key, CATALOG_CF, ROLES_COL, proto.toByteArray());
    roleCache.put(roleName, proto);
  }

  /**
   * Rebuild the row for a given user in the USER_TO_ROLE table.  This is expensive.  It
   * should be called as infrequently as possible.
   * @param userName name of the user
   * @throws IOException
   */
  void buildRoleMapForUser(String userName) throws IOException, NoSuchObjectException {
    // This is mega ugly.  Hopefully we don't have to do this too often.
    // First, scan the role table and put it all in memory
    buildRoleCache();
    LOG.debug("Building role map for " + userName);

    // Second, find every role the user participates in directly.
    Set<String> rolesToAdd = new HashSet<String>();
    Set<String> rolesToCheckNext = new HashSet<String>();
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      for (HbaseMetastoreProto.RoleGrantInfo grantInfo : e.getValue().getGrantInfoList()) {
        if (HBaseUtils.convertPrincipalTypes(grantInfo.getPrincipalType()) == PrincipalType.USER &&
            userName .equals(grantInfo.getPrincipalName())) {
          rolesToAdd.add(e.getKey());
          rolesToCheckNext.add(e.getKey());
          LOG.debug("Adding " + e.getKey() + " to list of roles user is in directly");
          break;
        }
      }
    }

    // Third, find every role the user participates in indirectly (that is, they have been
    // granted into role X and role Y has been granted into role X).
    while (rolesToCheckNext.size() > 0) {
      Set<String> tmpRolesToCheckNext = new HashSet<String>();
      for (String roleName : rolesToCheckNext) {
        HbaseMetastoreProto.RoleGrantInfoList grantInfos = roleCache.get(roleName);
        if (grantInfos == null) continue;  // happens when a role contains no grants
        for (HbaseMetastoreProto.RoleGrantInfo grantInfo : grantInfos.getGrantInfoList()) {
          if (HBaseUtils.convertPrincipalTypes(grantInfo.getPrincipalType()) == PrincipalType.ROLE &&
              rolesToAdd.add(grantInfo.getPrincipalName())) {
            tmpRolesToCheckNext.add(grantInfo.getPrincipalName());
            LOG.debug("Adding " + grantInfo.getPrincipalName() +
                " to list of roles user is in indirectly");
          }
        }
      }
      rolesToCheckNext = tmpRolesToCheckNext;
    }

    byte[] key = HBaseUtils.buildKey(userName);
    byte[] serialized = HBaseUtils.serializeRoleList(new ArrayList<String>(rolesToAdd));
    store(USER_TO_ROLE_TABLE, key, CATALOG_CF, CATALOG_COL, serialized);
  }

  /**
   * Remove all of the grants for a role.  This is not cheap.
   * @param roleName Role to remove from all other roles and grants
   * @throws IOException
   */
  void removeRoleGrants(String roleName) throws IOException {
    buildRoleCache();

    List<Put> puts = new ArrayList<Put>();
    // First, walk the role table and remove any references to this role
    for (Map.Entry<String, HbaseMetastoreProto.RoleGrantInfoList> e : roleCache.entrySet()) {
      boolean madeAChange = false;
      List<HbaseMetastoreProto.RoleGrantInfo> rgil =
          new ArrayList<HbaseMetastoreProto.RoleGrantInfo>();
      rgil.addAll(e.getValue().getGrantInfoList());
      for (int i = 0; i < rgil.size(); i++) {
        if (HBaseUtils.convertPrincipalTypes(rgil.get(i).getPrincipalType()) == PrincipalType.ROLE &&
            rgil.get(i).getPrincipalName().equals(roleName)) {
          rgil.remove(i);
          madeAChange = true;
          break;
        }
      }
      if (madeAChange) {
        Put put = new Put(HBaseUtils.buildKey(e.getKey()));
        HbaseMetastoreProto.RoleGrantInfoList proto =
            HbaseMetastoreProto.RoleGrantInfoList.newBuilder()
            .addAllGrantInfo(rgil)
            .build();
        put.add(CATALOG_CF, ROLES_COL, proto.toByteArray());
        puts.add(put);
        roleCache.put(e.getKey(), proto);
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(ROLE_TABLE);
      htab.put(puts);
      htab.flushCommits();
    }

    // Remove any global privileges held by this role
    PrincipalPrivilegeSet global = getGlobalPrivs();
    if (global != null &&
        global.getRolePrivileges() != null &&
        global.getRolePrivileges().remove(roleName) != null) {
      putGlobalPrivs(global);
    }

    // Now, walk the db table
    puts.clear();
    List<Database> dbs = scanDatabases(null);
    if (dbs == null) dbs = new ArrayList<Database>(); // rare, but can happen
    for (Database db : dbs) {
      if (db.getPrivileges() != null &&
          db.getPrivileges().getRolePrivileges() != null &&
          db.getPrivileges().getRolePrivileges().remove(roleName) != null) {
        byte[][] serialized = HBaseUtils.serializeDatabase(db);
        Put put = new Put(serialized[0]);
        put.add(CATALOG_CF, CATALOG_COL, serialized[1]);
        puts.add(put);
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(DB_TABLE);
      htab.put(puts);
      htab.flushCommits();
    }

    // Finally, walk the table table
    puts.clear();
    for (Database db : dbs) {
      List<Table> tables = scanTables(db.getName(), null);
      if (tables != null) {
        for (Table table : tables) {
          if (table.getPrivileges() != null &&
              table.getPrivileges().getRolePrivileges() != null &&
              table.getPrivileges().getRolePrivileges().remove(roleName) != null) {
            byte[][] serialized = HBaseUtils.serializeTable(table,
                HBaseUtils.hashStorageDescriptor(table.getSd(), md));
            Put put = new Put(serialized[0]);
            put.add(CATALOG_CF, CATALOG_COL, serialized[1]);
            puts.add(put);
          }
        }
      }
    }

    if (puts.size() > 0) {
      HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
      htab.put(puts);
      htab.flushCommits();
    }
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
    return HBaseUtils.deserializeRole(roleName, serialized);
  }

  /**
   * Get a list of roles.
   * @return list of all known roles.
   * @throws IOException
   */
  List<Role> scanRoles() throws IOException {
    Iterator<Result> iter = scan(ROLE_TABLE, CATALOG_CF, CATALOG_COL);
    List<Role> roles = new ArrayList<Role>();
    while (iter.hasNext()) {
      Result result = iter.next();
      roles.add(HBaseUtils.deserializeRole(result.getRow(),
          result.getValue(CATALOG_CF, CATALOG_COL)));
    }
    return roles;
  }

  /**
   * Add a new role
   * @param role role object
   * @throws IOException
   */
  void putRole(Role role) throws IOException {
    byte[][] serialized = HBaseUtils.serializeRole(role);
    store(ROLE_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
  }

  /**
   * Drop a role
   * @param roleName name of role to drop
   * @throws IOException
   */
  void deleteRole(String roleName) throws IOException {
    byte[] key = HBaseUtils.buildKey(roleName);
    delete(ROLE_TABLE, key, null, null);
    roleCache.remove(roleName);
  }

  private void buildRoleCache() throws IOException {
    if (!entireRoleTableInCache) {
      Iterator<Result> roles = scan(ROLE_TABLE, CATALOG_CF, ROLES_COL);
      while (roles.hasNext()) {
        Result res = roles.next();
        String roleName = new String(res.getRow(), HBaseUtils.ENCODING);
        HbaseMetastoreProto.RoleGrantInfoList grantInfos =
            HbaseMetastoreProto.RoleGrantInfoList.parseFrom(res.getValue(CATALOG_CF, ROLES_COL));
        roleCache.put(roleName, grantInfos);
      }
      entireRoleTableInCache = true;
    }
  }

  /**********************************************************************************************
   * Table related methods
   *********************************************************************************************/

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
    HTableInterface htab = conn.getHBaseTable(TABLE_TABLE);
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
        HBaseUtils.StorageDescriptorParts sdParts =
            HBaseUtils.deserializeTable(dbName, tableNames.get(i), serialized);
        StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
        HBaseUtils.assembleStorageDescriptor(sd, sdParts);
        tableCache.put(hashKeys[i], sdParts.containingTable);
        results.set(i, sdParts.containingTable);
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
    byte[] keyPrefix = null;
    if (dbName != null) {
      keyPrefix = HBaseUtils.buildKeyWithTrailingSeparator(dbName);
    }
    Filter filter = null;
    if (regex != null) {
      filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    }
    Iterator<Result> iter =
        scan(TABLE_TABLE, keyPrefix, HBaseUtils.getEndPrefix(keyPrefix),
            CATALOG_CF, CATALOG_COL, filter);
    List<Table> tables = new ArrayList<Table>();
    while (iter.hasNext()) {
      Result result = iter.next();
      HBaseUtils.StorageDescriptorParts sdParts =
          HBaseUtils.deserializeTable(result.getRow(), result.getValue(CATALOG_CF, CATALOG_COL));
      StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
      HBaseUtils.assembleStorageDescriptor(sd, sdParts);
      tables.add(sdParts.containingTable);
    }
    return tables;
  }

  /**
   * Put a table object.  This should only be called when the table is new (create table) as it
   * will blindly add/increment the storage descriptor.  If you are altering an existing table
   * call {@link #replaceTable} instead.
   * @param table table object
   * @throws IOException
   */
  void putTable(Table table) throws IOException {
    byte[] hash = putStorageDescriptor(table.getSd());
    byte[][] serialized = HBaseUtils.serializeTable(table, hash);
    store(TABLE_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    tableCache.put(new ObjectPair<String, String>(table.getDbName(), table.getTableName()), table);
  }

  /**
   * Replace an existing table.  This will also compare the storage descriptors and see if the
   * reference count needs to be adjusted
   * @param oldTable old version of the table
   * @param newTable new version of the table
   */
  void replaceTable(Table oldTable, Table newTable) throws IOException {
    byte[] hash;
    byte[] oldHash = HBaseUtils.hashStorageDescriptor(oldTable.getSd(), md);
    byte[] newHash = HBaseUtils.hashStorageDescriptor(newTable.getSd(), md);
    if (Arrays.equals(oldHash, newHash)) {
      hash = oldHash;
    } else {
      decrementStorageDescriptorRefCount(oldTable.getSd());
      hash = putStorageDescriptor(newTable.getSd());
    }
    byte[][] serialized = HBaseUtils.serializeTable(newTable, hash);
    store(TABLE_TABLE, serialized[0], CATALOG_CF, CATALOG_COL, serialized[1]);
    tableCache.put(new ObjectPair<String, String>(newTable.getDbName(), newTable.getTableName()),
        newTable);
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
  }

  private Table getTable(String dbName, String tableName, boolean populateCache)
      throws IOException {
    ObjectPair<String, String> hashKey = new ObjectPair<String, String>(dbName, tableName);
    Table cached = tableCache.get(hashKey);
    if (cached != null) return cached;
    byte[] key = HBaseUtils.buildKey(dbName, tableName);
    byte[] serialized = read(TABLE_TABLE, key, CATALOG_CF, CATALOG_COL);
    if (serialized == null) return null;
    HBaseUtils.StorageDescriptorParts sdParts =
        HBaseUtils.deserializeTable(dbName, tableName, serialized);
    StorageDescriptor sd = getStorageDescriptor(sdParts.sdHash);
    HBaseUtils.assembleStorageDescriptor(sd, sdParts);
    if (populateCache) tableCache.put(hashKey, sdParts.containingTable);
    return sdParts.containingTable;
  }

  /**********************************************************************************************
   * StorageDescriptor related methods
   *********************************************************************************************/

  /**
   * If this serde has already been read, then return it from the cache.  If not, read it, then
   * return it.
   * @param hash hash of the storage descriptor to read
   * @return the storage descriptor
   * @throws IOException
   */
  StorageDescriptor getStorageDescriptor(byte[] hash) throws IOException {
    ByteArrayWrapper hashKey = new ByteArrayWrapper(hash);
    StorageDescriptor cached = sdCache.get(hashKey);
    if (cached != null) return cached;
    LOG.debug("Not found in cache, looking in hbase");
    byte[] serialized = read(SD_TABLE, hash, CATALOG_CF, CATALOG_COL);
    if (serialized == null) {
      throw new RuntimeException("Woh, bad!  Trying to fetch a non-existent storage descriptor " +
          "from hash " + Base64.encodeBase64String(hash));
    }
    StorageDescriptor sd = HBaseUtils.deserializeStorageDescriptor(serialized);
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
    byte[] key = HBaseUtils.hashStorageDescriptor(sd, md);
    byte[] serializedRefCnt = read(SD_TABLE, key, CATALOG_CF, REF_COUNT_COL);
    if (serializedRefCnt == null) {
      // Someone deleted it before we got to it, no worries
      return;
    }
    int refCnt = Integer.valueOf(new String(serializedRefCnt, HBaseUtils.ENCODING));
    HTableInterface htab = conn.getHBaseTable(SD_TABLE);
    if (--refCnt < 1) {
      Delete d = new Delete(key);
      // We don't use checkAndDelete here because it isn't compatible with the transaction
      // managers.  If the transaction managers are doing their jobs then we should not need it
      // anyway.
      htab.delete(d);
      sdCache.remove(new ByteArrayWrapper(key));
    } else {
      Put p = new Put(key);
      p.add(CATALOG_CF, REF_COUNT_COL, Integer.toString(refCnt).getBytes(HBaseUtils.ENCODING));
      htab.put(p);
      htab.flushCommits();
    }
  }

  /**
   * Place the common parts of a storage descriptor into the cache and write the storage
   * descriptor out to HBase.  This should only be called if you are sure that the storage
   * descriptor needs to be added.  If you have changed a table or partition but not it's storage
   * descriptor do not call this method, as it will increment the reference count of the storage
   * descriptor.
   * @param storageDescriptor storage descriptor to store.
   * @return id of the entry in the cache, to be written in for the storage descriptor
   */
  byte[] putStorageDescriptor(StorageDescriptor storageDescriptor) throws IOException {
    byte[] sd = HBaseUtils.serializeStorageDescriptor(storageDescriptor);
    byte[] key = HBaseUtils.hashStorageDescriptor(storageDescriptor, md);
    byte[] serializedRefCnt = read(SD_TABLE, key, CATALOG_CF, REF_COUNT_COL);
    HTableInterface htab = conn.getHBaseTable(SD_TABLE);
    if (serializedRefCnt == null) {
      // We are the first to put it in the DB
      Put p = new Put(key);
      p.add(CATALOG_CF, CATALOG_COL, sd);
      p.add(CATALOG_CF, REF_COUNT_COL, "1".getBytes(HBaseUtils.ENCODING));
      htab.put(p);
      sdCache.put(new ByteArrayWrapper(key), storageDescriptor);
    } else {
      // Just increment the reference count
      int refCnt = Integer.valueOf(new String(serializedRefCnt, HBaseUtils.ENCODING)) + 1;
      Put p = new Put(key);
      p.add(CATALOG_CF, REF_COUNT_COL, Integer.toString(refCnt).getBytes(HBaseUtils.ENCODING));
      htab.put(p);
    }
    htab.flushCommits();
    return key;
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

  /**********************************************************************************************
   * Statistics related methods
   *********************************************************************************************/

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

      byte[] key = HBaseUtils.buildPartitionKey(dbName, tableName, pi.partVals);
      Get g = new Get(key);
      for (byte[] colName : colKeys) g.addColumn(STATS_CF, colName);
      gets.add(g);
    }
    HTableInterface htab = conn.getHBaseTable(PART_TABLE);
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

  private byte[] getStatisticsKey(String dbName, String tableName, List<String> partVals) {
    return partVals == null ?
        HBaseUtils.buildKey(dbName, tableName) :
        HBaseUtils.buildPartitionKey(dbName, tableName, partVals);
  }

  private String getStatisticsTable(List<String> partVals) {
    return partVals == null ? TABLE_TABLE : PART_TABLE;
  }

  /**********************************************************************************************
   * Cache methods
   *********************************************************************************************/

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
    flushRoleCache();
  }

  private void flushRoleCache() {
    roleCache.clear();
    entireRoleTableInCache = false;
  }

  /**********************************************************************************************
   * General access methods
   *********************************************************************************************/

  private void store(String table, byte[] key, byte[] colFam, byte[] colName, byte[] obj)
      throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Put p = new Put(key);
    p.add(colFam, colName, obj);
    htab.put(p);
    htab.flushCommits();
  }

  private void store(String table, byte[] key, byte[] colFam, byte[][] colName, byte[][] obj)
      throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Put p = new Put(key);
    for (int i = 0; i < colName.length; i++) {
      p.add(colFam, colName[i], obj[i]);
    }
    htab.put(p);
    htab.flushCommits();
  }

  private byte[] read(String table, byte[] key, byte[] colFam, byte[] colName) throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Get g = new Get(key);
    g.addColumn(colFam, colName);
    Result res = htab.get(g);
    return res.getValue(colFam, colName);
  }

  private Result read(String table, byte[] key, byte[] colFam, byte[][] colNames)
      throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Get g = new Get(key);
    for (byte[] colName : colNames) g.addColumn(colFam, colName);
    return htab.get(g);
  }

  // Delete a row.  If colFam and colName are not null, then only the named column will be
  // deleted.  If colName is null and colFam is not, only the named family will be deleted.  If
  // both are null the entire row will be deleted.
  private void delete(String table, byte[] key, byte[] colFam, byte[] colName) throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Delete d = new Delete(key);
    if (colName != null) d.deleteColumn(colFam, colName);
    else if (colFam != null) d.deleteFamily(colFam);
    htab.delete(d);
  }

  private Iterator<Result> scan(String table, byte[] colFam,
      byte[] colName) throws IOException {
    return scan(table, null, null, colFam, colName, null);
  }

  private Iterator<Result> scan(String table, byte[] colFam, byte[] colName,
      Filter filter) throws IOException {
    return scan(table, null, null, colFam, colName, filter);
  }

  private Iterator<Result> scan(String table, byte[] keyStart, byte[] keyEnd, byte[] colFam,
                                          byte[] colName, Filter filter) throws IOException {
    HTableInterface htab = conn.getHBaseTable(table);
    Scan s = new Scan();
    if (keyStart != null) {
      s.setStartRow(keyStart);
    }
    if (keyEnd != null) {
      s.setStopRow(keyEnd);
    }
    s.addColumn(colFam, colName);
    if (filter != null) {
      s.setFilter(filter);
    }
    ResultScanner scanner = htab.getScanner(s);
    return scanner.iterator();
  }



  /**********************************************************************************************
   * Testing methods and classes
   *********************************************************************************************/

  @VisibleForTesting
  int countStorageDescriptor() throws IOException {
    ResultScanner scanner = conn.getHBaseTable(SD_TABLE).getScanner(new Scan());
    int cnt = 0;
    Result r;
    do {
      r = scanner.next();
      if (r != null) {
        LOG.debug("Saw record with hash " + Base64.encodeBase64String(r.getRow()));
        cnt++;
      }
    } while (r != null);

    return cnt;
  }

  /**
   * Use this for unit testing only, so that a mock connection object can be passed in.
   * @param connection Mock connection objecct
   */
  @VisibleForTesting
  static void setTestConnection(HBaseConnection connection) {
    testConn = connection;
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
