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

package org.apache.hadoop.hive.metastore;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Hive Metastore Client.
 */
public class HiveMetaStoreClient implements IMetaStoreClient {
  ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean open = false;
  private URI metastoreUris[];
  private final boolean standAloneClient = false;
  private final HiveMetaHookLoader hookLoader;

  // for thrift connects
  private int retries = 5;

  static final private Log LOG = LogFactory.getLog("hive.metastore");

  public HiveMetaStoreClient(HiveConf conf)
    throws MetaException {
    this(conf, null);
  }

  public HiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader)
    throws MetaException {

    this.hookLoader = hookLoader;
    if (conf == null) {
      conf = new HiveConf(HiveMetaStoreClient.class);
    }


    boolean localMetaStore = conf.getBoolean("hive.metastore.local", false);
    if (localMetaStore) {
      // instantiate the metastore server handler directly instead of connecting
      // through the network
      client = new HiveMetaStore.HMSHandler("hive client", conf);
      open = true;
      return;
    }

    // get the number retries
    retries = HiveConf.getIntVar(conf, HiveConf.ConfVars.METATORETHRIFTRETRIES);

    // user wants file store based configuration
    if (conf.getVar(HiveConf.ConfVars.METASTOREURIS) != null) {
      String metastoreUrisString[] = conf.getVar(
          HiveConf.ConfVars.METASTOREURIS).split(",");
      metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for (String s : metastoreUrisString) {
          URI tmpUri = new URI(s);
          if (tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: " + s
                + " does not have a scheme");
          }
          metastoreUris[i++] = tmpUri;

        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch (Exception e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else if (conf.getVar(HiveConf.ConfVars.METASTOREDIRECTORY) != null) {
      metastoreUris = new URI[1];
      try {
        metastoreUris[0] = new URI(conf
            .getVar(HiveConf.ConfVars.METASTOREDIRECTORY));
      } catch (URISyntaxException e) {
        MetaStoreUtils.logAndThrowMetaException(e);
      }
    } else {
      LOG.error("NOT getting uris from conf");
      throw new MetaException("MetaStoreURIs not found in conf file");
    }
    // finally open the store
    open();
  }

  /**
   * @param dbname
   * @param tbl_name
   * @param new_tbl
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#alter_table(java.lang.String,
   *      java.lang.String, org.apache.hadoop.hive.metastore.api.Table)
   */
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table(dbname, tbl_name, new_tbl);
  }

  private void open() throws MetaException {
    for (URI store : metastoreUris) {
      LOG.info("Trying to connect to metastore with URI " + store);
      try {
        openStore(store);
      } catch (MetaException e) {
        LOG.warn(e.getStackTrace());
        LOG.warn("Unable to connect metastore with URI " + store);
      }
      if (open) {
        break;
      }
    }
    if (!open) {
      throw new MetaException(
          "Could not connect to meta store using any of the URIs provided");
    }
    LOG.info("Connected to metastore.");
  }

  private void openStore(URI store) throws MetaException {
    open = false;
    transport = new TSocket(store.getHost(), store.getPort());
    ((TSocket) transport).setTimeout(20000);
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);

    for (int i = 0; i < retries && !open; ++i) {
      try {
        transport.open();
        open = true;
      } catch (TTransportException e) {
        LOG.warn("failed to connect to MetaStore, re-trying...");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }
      }
    }
    if (!open) {
      throw new MetaException("could not connect to meta store");
    }
  }

  public void close() {
    open = false;
    if ((transport != null) && transport.isOpen()) {
      transport.close();
    }
    if (standAloneClient) {
      try {
        client.shutdown();
      } catch (TException e) {
        // TODO:pc cleanup the exceptions
        LOG.error("Unable to shutdown local metastore client");
        LOG.error(e.getStackTrace());
        // throw new RuntimeException(e.getMessage());
      }
    }
  }

  /**
   * @param new_part
   * @return the added partition
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#add_partition(org.apache.hadoop.hive.metastore.api.Partition)
   */
  public Partition add_partition(Partition new_part)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return deepCopy(client.add_partition(new_part));
  }

  /**
   * @param table_name
   * @param db_name
   * @param part_vals
   * @return the appended partition
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  public Partition appendPartition(String db_name, String table_name,
      List<String> part_vals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    return deepCopy(client.append_partition(db_name, table_name, part_vals));
  }

  public Partition appendPartition(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException,
             MetaException, TException {
    return deepCopy(
        client.append_partition_by_name(dbName, tableName, partName));
  }

  /**
   * Create a new Database
   * @param db
   * @return true or false
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_database(java.lang.String,
   *      java.lang.String)
   */
  public void createDatabase(Database db)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    client.create_database(db);
  }

  /**
   * @param tbl
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */
  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(tbl);
    }
    boolean success = false;
    try {
      client.create_table(tbl);
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackCreateTable(tbl);
      }
    }
  }

  /**
   * @param type
   * @return true or false
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_type(org.apache.hadoop.hive.metastore.api.Type)
   */
  public boolean createType(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return client.create_type(type);
  }

  /**
   * @param name
   * @return true or false
   * @throws NoSuchObjectException
   * @throws InvalidOperationException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_database(java.lang.String)
   */
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(name, true, false);
  }


  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getDatabase(name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownDb) {
        throw e;
      }
      return;
    }
    client.drop_database(name, deleteData);
  }


  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals) throws NoSuchObjectException, MetaException,
      TException {
    return dropPartition(db_name, tbl_name, part_vals, true);
  }

  public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return client.drop_partition_by_name(dbName, tableName, partName, deleteData);
  }
  /**
   * @param db_name
   * @param tbl_name
   * @param part_vals
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_partition(db_name, tbl_name, part_vals, deleteData);
  }

  /**
   * @param name
   * @param dbname
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void dropTable(String dbname, String name)
      throws NoSuchObjectException, MetaException, TException {
    dropTable(dbname, name, true, true);
  }

  /** {@inheritDoc} */
  @Deprecated
  public void dropTable(String tableName, boolean deleteData)
      throws MetaException, UnknownTableException, TException, NoSuchObjectException {
    dropTable(DEFAULT_DATABASE_NAME, tableName, deleteData, false);
  }

  /**
   * @param dbname
   * @param name
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @throws NoSuchObjectException
   * @throws ExistingDependentsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_table(java.lang.String,
   *      java.lang.String, boolean)
   */
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUknownTab) throws MetaException, TException,
      NoSuchObjectException {

    Table tbl;
    try {
      tbl = getTable(dbname, name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUknownTab) {
        throw e;
      }
      return;
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preDropTable(tbl);
    }
    boolean success = false;
    try {
      client.drop_table(dbname, name, deleteData);
      if (hook != null) {
        hook.commitDropTable(tbl, deleteData);
      }
    } catch (NoSuchObjectException e) {
      if (!ignoreUknownTab) {
        throw e;
      }
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackDropTable(tbl);
      }
    }
  }

  /**
   * @param type
   * @return true if the type is dropped
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_type(java.lang.String)
   */
  public boolean dropType(String type) throws NoSuchObjectException, MetaException, TException {
    return client.drop_type(type);
  }

  /**
   * @param name
   * @return map of types
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type_all(java.lang.String)
   */
  public Map<String, Type> getTypeAll(String name) throws MetaException,
      TException {
    Map<String, Type> result = null;
    Map<String, Type> fromClient = client.get_type_all(name);
    if (fromClient != null) {
      result = new LinkedHashMap<String, Type>();
      for (String key : fromClient.keySet()) {
        result.put(key, deepCopy(fromClient.get(key)));
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  public List<String> getDatabases(String databasePattern)
    throws MetaException {
    try {
      return client.get_databases(databasePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /** {@inheritDoc} */
  public List<String> getAllDatabases() throws MetaException {
    try {
      return client.get_all_databases();
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param max_parts
   * @return list of partitions
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  public List<Partition> listPartitions(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(
        client.get_partitions(db_name, tbl_name, max_parts));
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return deepCopyPartitions(
        client.get_partitions_ps(db_name, tbl_name, part_vals, max_parts));
  }

  /**
   * Get list of partitions matching specified filter
   * @param db_name the database name
   * @param tbl_name the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 <= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @return list of partitions
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws MetaException,
         NoSuchObjectException, TException {
    return deepCopyPartitions(
        client.get_partitions_by_filter(db_name, tbl_name, filter, max_parts));
  }

  /**
   * @param name
   * @return the database
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_database(java.lang.String)
   */
  public Database getDatabase(String name) throws NoSuchObjectException,
      MetaException, TException {
    return deepCopy(client.get_database(name));
  }

  /**
   * @param tbl_name
   * @param db_name
   * @param part_vals
   * @return the partition
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  public Partition getPartition(String db_name, String tbl_name,
      List<String> part_vals) throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_partition(db_name, tbl_name, part_vals));
  }

  /**
   * @param name
   * @param dbname
   * @return the table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_table(java.lang.String,
   *      java.lang.String)
   */
  public Table getTable(String dbname, String name) throws MetaException,
      TException, NoSuchObjectException {
    return deepCopy(client.get_table(dbname, name));
  }

  /** {@inheritDoc} */
  @Deprecated
  public Table getTable(String tableName) throws MetaException, TException,
      NoSuchObjectException {
    return getTable(DEFAULT_DATABASE_NAME, tableName);
  }

  /**
   * @param name
   * @return the type
   * @throws MetaException
   * @throws TException
   * @throws NoSuchObjectException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_type(java.lang.String)
   */
  public Type getType(String name) throws NoSuchObjectException, MetaException, TException {
    return deepCopy(client.get_type(name));
  }

  /** {@inheritDoc} */
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return client.get_tables(dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  /** {@inheritDoc} */
  public List<String> getAllTables(String dbname) throws MetaException {
    try {
      return client.get_all_tables(dbname);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public boolean tableExists(String databaseName, String tableName) throws MetaException,
      TException, UnknownDBException {
    try {
      client.get_table(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  @Deprecated
  public boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException {
    return tableExists(DEFAULT_DATABASE_NAME, tableName);
  }

  public List<String> listPartitionNames(String dbName, String tblName,
      short max) throws MetaException, TException {
    return client.get_partition_names(dbName, tblName, max);
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws MetaException, TException {
    return client.get_partition_names_ps(db_name, tbl_name, part_vals, max_parts);
  }

  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(dbName, tblName, newPart);
  }

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String,
   *      java.lang.String)
   */
  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    return deepCopyFieldSchemas(client.get_fields(db, tableName));
  }

  /**
   * create an index
   * @param index the index object
   * @param index table which stores the index data
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @throws AlreadyExistsException
   */
  public void createIndex(Index index, Table indexTable) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.add_index(index, indexTable);
  }

  /**
   * @param dbName
   * @param tblName
   * @param indexName
   * @return
   * @throws MetaException
   * @throws UnknownTableException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public Index getIndex(String dbName, String tblName, String indexName)
      throws MetaException, UnknownTableException, NoSuchObjectException,
      TException {
    return client.get_index_by_name(dbName, tblName, indexName);
  }

  /**
   * list indexes of the give base table
   * @param db_name
   * @param tbl_name
   * @param max
   * @return
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  public List<String> listIndexNames(String dbName, String tblName, short max)
      throws MetaException, TException {
    return client.get_index_names(dbName, tblName, max);
  }

  /**
   * list all the index names of the give base table.
   *
   * @param db_name
   * @param tbl_name
   * @param max
   * @return
   * @throws MetaException
   * @throws TException
   */
  public List<Index> listIndexes(String dbName, String tblName, short max)
      throws NoSuchObjectException, MetaException, TException {
    return client.get_indexes(dbName, tblName, max);
  }

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String,
   *      java.lang.String)
   */
  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    return deepCopyFieldSchemas(client.get_schema(db, tableName));
  }

  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return client.get_config_value(name, defaultValue);
  }

  public Partition getPartition(String db, String tableName, String partName)
      throws MetaException, TException, UnknownTableException, NoSuchObjectException {
    return deepCopy(client.get_partition_by_name(db, tableName, partName));
  }

  public Partition appendPartitionByName(String dbName, String tableName, String partName)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return deepCopy(
        client.append_partition_by_name(dbName, tableName, partName));
  }

  public boolean dropPartitionByName(String dbName, String tableName, String partName, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return client.drop_partition_by_name(dbName, tableName, partName, deleteData);
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return client.partition_name_to_vals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException{
    return client.partition_name_to_spec(name);
  }

  /**
   * @param partition
   * @return
   */
  private Partition deepCopy(Partition partition) {
    Partition copy = null;
    if (partition != null) {
      copy = new Partition(partition);
    }
    return copy;
  }

  private Database deepCopy(Database database) {
    Database copy = null;
    if (database != null) {
      copy = new Database(database);
    }
    return copy;
  }

  private Table deepCopy(Table table) {
    Table copy = null;
    if (table != null) {
      copy = new Table(table);
    }
    return copy;
  }

  private Type deepCopy(Type type) {
    Type copy = null;
    if (type != null) {
      copy = new Type(type);
    }
    return copy;
  }

  private FieldSchema deepCopy(FieldSchema schema) {
    FieldSchema copy = null;
    if (schema != null) {
      copy = new FieldSchema(schema);
    }
    return copy;
  }

  private List<Partition> deepCopyPartitions(List<Partition> partitions) {
    List<Partition> copy = null;
    if (partitions != null) {
      copy = new ArrayList<Partition>();
      for (Partition part : partitions) {
        copy.add(deepCopy(part));
      }
    }
    return copy;
  }

  private List<Table> deepCopyTables(List<Table> tables) {
    List<Table> copy = null;
    if (tables != null) {
      copy = new ArrayList<Table>();
      for (Table tab : tables) {
        copy.add(deepCopy(tab));
      }
    }
    return copy;
  }

  private List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
    List<FieldSchema> copy = null;
    if (schemas != null) {
      copy = new ArrayList<FieldSchema>();
      for (FieldSchema schema : schemas) {
        copy.add(deepCopy(schema));
      }
    }
    return copy;
  }

  @Override
  public boolean dropIndex(String dbName, String tblName, String name,
      boolean deleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.drop_index_by_name(dbName, tblName, name, deleteData);
  }

}
