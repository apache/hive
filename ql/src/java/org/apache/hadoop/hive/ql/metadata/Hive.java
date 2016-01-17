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

package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionTask;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.InPlaceUpdates;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * This class has functions that implement meta data/DDL operations using calls
 * to the metastore.
 * It has a metastore client instance it uses to communicate with the metastore.
 *
 * It is a thread local variable, and the instances is accessed using static
 * get methods in this class.
 */

@SuppressWarnings({"deprecation", "rawtypes"})
public class Hive {

  static final private Logger LOG = LoggerFactory.getLogger("hive.ql.metadata.Hive");

  private HiveConf conf = null;
  private IMetaStoreClient metaStoreClient;
  private UserGroupInformation owner;

  // metastore calls timing information
  private final Map<String, Long> metaCallTimeMap = new HashMap<String, Long>();

  private static ThreadLocal<Hive> hiveDB = new ThreadLocal<Hive>() {
    @Override
    protected Hive initialValue() {
      return null;
    }

    @Override
    public synchronized void remove() {
      if (this.get() != null) {
        this.get().close();
      }
      super.remove();
    }
  };

  // Note that while this is an improvement over static initialization, it is still not,
  // technically, valid, cause nothing prevents us from connecting to several metastores in
  // the same process. This will still only get the functions from the first metastore.
  private final static AtomicInteger didRegisterAllFuncs = new AtomicInteger(0);
  private final static int REG_FUNCS_NO = 0, REG_FUNCS_DONE = 2, REG_FUNCS_PENDING = 1;

  // register all permanent functions. need improvement
  private void registerAllFunctionsOnce() {
    boolean breakLoop = false;
    while (!breakLoop) {
      int val = didRegisterAllFuncs.get();
      switch (val) {
      case REG_FUNCS_NO: {
        if (didRegisterAllFuncs.compareAndSet(val, REG_FUNCS_PENDING)) {
          breakLoop = true;
          break;
        }
        continue;
      }
      case REG_FUNCS_PENDING: {
        synchronized (didRegisterAllFuncs) {
          try {
            didRegisterAllFuncs.wait(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        continue;
      }
      case REG_FUNCS_DONE: return;
      default: throw new AssertionError(val);
      }
    }
    try {
      reloadFunctions();
    } catch (Exception e) {
      LOG.warn("Failed to register all functions.", e);
    } finally {
      boolean result = didRegisterAllFuncs.compareAndSet(REG_FUNCS_PENDING, REG_FUNCS_DONE);
      assert result;
      synchronized (didRegisterAllFuncs) {
        didRegisterAllFuncs.notifyAll();
      }
    }
  }

  public void reloadFunctions() throws HiveException {
    for (Function function : getAllFunctions()) {
      String functionName = function.getFunctionName();
      try {
        LOG.info("Registering function " + functionName + " " + function.getClassName());
        FunctionRegistry.registerPermanentFunction(FunctionUtils.qualifyFunctionName(
                    functionName, function.getDbName()), function.getClassName(), false,
                    FunctionTask.toFunctionResource(function.getResourceUris()));
      } catch (Exception e) {
        LOG.warn("Failed to register persistent function " +
                functionName + ":" + function.getClassName() + ". Ignore and continue.");
      }
    }
  }

  public static Hive get(Configuration c, Class<?> clazz) throws HiveException {
    return get(c instanceof HiveConf ? (HiveConf)c : new HiveConf(c, clazz));
  }

  /**
   * Gets hive object for the current thread. If one is not initialized then a
   * new one is created If the new configuration is different in metadata conf
   * vars, or the owner will be different then a new one is created.
   *
   * @param c
   *          new Hive Configuration
   * @return Hive object for current thread
   * @throws HiveException
   *
   */
  public static Hive get(HiveConf c) throws HiveException {
    return getInternal(c, false);
  }

  /**
   * Same as {@link #get(HiveConf)}, except that it checks only the object identity of existing
   * MS client, assuming the relevant settings would be unchanged within the same conf object.
   */
  public static Hive getWithFastCheck(HiveConf c) throws HiveException {
    return getInternal(c, true);
  }

  private static Hive getInternal(HiveConf c, boolean isFastCheck) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || !db.isCurrentUserOwner() ||
        (db.metaStoreClient != null && !isCompatible(db, c, isFastCheck))) {
      return get(c, true);
    }
    db.conf = c;
    return db;
  }

  private static boolean isCompatible(Hive db, HiveConf c, boolean isFastCheck) {
    return isFastCheck
        ? db.metaStoreClient.isSameConfObj(c) : db.metaStoreClient.isCompatibleWith(c);
  }

  /**
   * get a connection to metastore. see get(HiveConf) function for comments
   *
   * @param c
   *          new conf
   * @param needsRefresh
   *          if true then creates a new one
   * @return The connection to the metastore
   * @throws HiveException
   */
  public static Hive get(HiveConf c, boolean needsRefresh) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || needsRefresh || !db.isCurrentUserOwner()) {
      if (db != null) {
        LOG.debug("Creating new db. db = " + db + ", needsRefresh = " + needsRefresh +
          ", db.isCurrentUserOwner = " + db.isCurrentUserOwner());
      }
      closeCurrent();
      c.set("fs.scheme.class", "dfs");
      Hive newdb = new Hive(c);
      hiveDB.set(newdb);
      return newdb;
    }
    db.conf = c;
    return db;
  }

  public static Hive get() throws HiveException {
    Hive db = hiveDB.get();
    if (db != null && !db.isCurrentUserOwner()) {
      LOG.debug("Creating new db. db.isCurrentUserOwner = " + db.isCurrentUserOwner());
      db.close();
      db = null;
    }
    if (db == null) {
      SessionState session = SessionState.get();
      db = new Hive(session == null ? new HiveConf(Hive.class) : session.getConf());
      hiveDB.set(db);
    }
    return db;
  }

  public static void set(Hive hive) {
    hiveDB.set(hive);
  }

  public static void closeCurrent() {
    hiveDB.remove();
  }

  /**
   * Hive
   *
   * @param c
   *
   */
  private Hive(HiveConf c) throws HiveException {
    conf = c;
    registerAllFunctionsOnce();
  }


  private boolean isCurrentUserOwner() throws HiveException {
    try {
      return owner == null || owner.equals(UserGroupInformation.getCurrentUser());
    } catch(IOException e) {
      throw new HiveException("Error getting current user: " + e.getMessage(), e);
    }
  }



  /**
   * closes the connection to metastore for the calling thread
   */
  private void close() {
    LOG.debug("Closing current thread's connection to Hive Metastore.");
    if (metaStoreClient != null) {
      metaStoreClient.close();
      metaStoreClient = null;
    }
    if (owner != null) {
      owner = null;
    }
  }

  /**
   * Create a database
   * @param db
   * @param ifNotExist if true, will ignore AlreadyExistsException exception
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db, boolean ifNotExist)
      throws AlreadyExistsException, HiveException {
    try {
      getMSC().createDatabase(db);
    } catch (AlreadyExistsException e) {
      if (!ifNotExist) {
        throw e;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Create a Database. Raise an error if a database with the same name already exists.
   * @param db
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db) throws AlreadyExistsException, HiveException {
    createDatabase(db, false);
  }

  /**
   * Drop a database.
   * @param name
   * @throws NoSuchObjectException
   * @throws HiveException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
   */
  public void dropDatabase(String name) throws HiveException, NoSuchObjectException {
    dropDatabase(name, true, false, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws HiveException, NoSuchObjectException {
    dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @param cascade         if true, delete all tables on the DB if exists. Otherwise, the query
   *                        will fail if table still exists.
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  /**
   * Creates a table metadata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partCols, fileInputFormat,
        fileOutputFormat, -1, null);
  }

  /**
   * Creates a table metadata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @param bucketCount
   *          number of buckets that each partition (or the table itself) should
   *          be divided into
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols)
      throws HiveException {
    createTable(tableName, columns, partCols, fileInputFormat, fileOutputFormat, bucketCount,
        bucketCols, null);
  }

  /**
   * Create a table metadata and the directory for the table data
   * @param tableName table name
   * @param columns list of fields of the table
   * @param partCols partition keys of the table
   * @param fileInputFormat Class of the input format of the table data file
   * @param fileOutputFormat Class of the output format of the table data file
   * @param bucketCount number of buckets that each partition (or the table itself) should be
   *                    divided into
   * @param bucketCols Bucket columns
   * @param parameters Parameters for the table
   * @throws HiveException
   */
  public void createTable(String tableName, List<String> columns, List<String> partCols,
                          Class<? extends InputFormat> fileInputFormat,
                          Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols,
                          Map<String, String> parameters) throws HiveException {
    if (columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = newTable(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col : columns) {
      FieldSchema field = new FieldSchema(col, STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if (partCols != null) {
      for (String partCol : partCols) {
        FieldSchema part = new FieldSchema();
        part.setName(partCol);
        part.setType(STRING_TYPE_NAME); // default partition key
        tbl.getPartCols().add(part);
      }
    }
    tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    tbl.setBucketCols(bucketCols);
    if (parameters != null) {
      tbl.setParameters(parameters);
    }
    createTable(tbl);
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newTbl
   *          new name of the table. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterTable(String tblName, Table newTbl)
      throws InvalidOperationException, HiveException {
    alterTable(tblName, newTbl, false);
  }

  public void alterTable(String tblName, Table newTbl, boolean cascade)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newTbl.getParameters() != null) {
        newTbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      newTbl.checkValidity(conf);
      getMSC().alter_table(names[0], names[1], newTbl.getTTable(), cascade);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table. " + e.getMessage(), e);
    }
  }

  public void alterIndex(String baseTableName, String indexName, Index newIdx)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(baseTableName);
    alterIndex(names[0], names[1], indexName, newIdx);
  }

  /**
   * Updates the existing index metadata with the new metadata.
   *
   * @param idxName
   *          name of the existing index
   * @param newIdx
   *          new name of the index. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterIndex(String dbName, String baseTblName, String idxName, Index newIdx)
      throws InvalidOperationException, HiveException {
    try {
      getMSC().alter_index(dbName, baseTblName, idxName, newIdx);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter index. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter index. " + e.getMessage(), e);
    }
  }

  /**
   * Updates the existing partition metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String tblName, Partition newPart)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    alterPartition(names[0], names[1], newPart);
  }

  /**
   * Updates the existing partition metadata with the new metadata.
   *
   * @param dbName
   *          name of the exiting table's database
   * @param tblName
   *          name of the existing table
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, HiveException {
    try {
      // Remove the DDL time so that it gets refreshed
      if (newPart.getParameters() != null) {
        newPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      newPart.checkValidity();
      getMSC().alter_partition(dbName, tblName, newPart.getTPartition());

    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    }
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newParts
   *          new partitions
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartitions(String tblName, List<Partition> newParts)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    List<org.apache.hadoop.hive.metastore.api.Partition> newTParts =
      new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    try {
      // Remove the DDL time so that it gets refreshed
      for (Partition tmpPart: newParts) {
        if (tmpPart.getParameters() != null) {
          tmpPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
        }
        newTParts.add(tmpPart.getTPartition());
      }
      getMSC().alter_partitions(names[0], names[1], newTParts);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    }
  }
  /**
   * Rename a old partition to new partition
   *
   * @param tbl
   *          existing table
   * @param oldPartSpec
   *          spec of old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void renamePartition(Table tbl, Map<String, String> oldPartSpec, Partition newPart)
      throws HiveException {
    try {
      Map<String, String> newPartSpec = newPart.getSpec();
      if (oldPartSpec.keySet().size() != tbl.getPartCols().size()
          || newPartSpec.keySet().size() != tbl.getPartCols().size()) {
        throw new HiveException("Unable to rename partition to the same name: number of partition cols don't match. ");
      }
      if (!oldPartSpec.keySet().equals(newPartSpec.keySet())){
        throw new HiveException("Unable to rename partition to the same name: old and new partition cols don't match. ");
      }
      List<String> pvals = new ArrayList<String>();

      for (FieldSchema field : tbl.getPartCols()) {
        String val = oldPartSpec.get(field.getName());
        if (val == null || val.length() == 0) {
          throw new HiveException("get partition: Value for key "
              + field.getName() + " is null or empty");
        } else if (val != null){
          pvals.add(val);
        }
      }
      getMSC().renamePartition(tbl.getDbName(), tbl.getTableName(), pvals,
          newPart.getTPartition());

    } catch (InvalidOperationException e){
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    }
  }

  public void alterDatabase(String dbName, Database db)
      throws HiveException {
    try {
      getMSC().alterDatabase(dbName, db);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter database " + dbName + ". " + e.getMessage(), e);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Database " + dbName + " does not exists.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter database " + dbName + ". " + e.getMessage(), e);
    }
  }
  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @throws HiveException
   */
  public void createTable(Table tbl) throws HiveException {
    createTable(tbl, false);
  }

  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @param ifNotExists
   *          if true, ignore AlreadyExistsException
   * @throws HiveException
   */
  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
    try {
      if (tbl.getDbName() == null || "".equals(tbl.getDbName().trim())) {
        tbl.setDbName(SessionState.get().getCurrentDatabase());
      }
      if (tbl.getCols().size() == 0 || tbl.getSd().getColsSize() == 0) {
        tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(),
            tbl.getDeserializer()));
      }
      tbl.checkValidity(conf);
      if (tbl.getParameters() != null) {
        tbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      org.apache.hadoop.hive.metastore.api.Table tTbl = tbl.getTTable();
      PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
      SessionState ss = SessionState.get();
      if (ss != null) {
        CreateTableAutomaticGrant grants = ss.getCreateTableGrants();
        if (grants != null) {
          principalPrivs.setUserPrivileges(grants.getUserGrants());
          principalPrivs.setGroupPrivileges(grants.getGroupGrants());
          principalPrivs.setRolePrivileges(grants.getRoleGrants());
          tTbl.setPrivileges(principalPrivs);
        }
      }
      getMSC().createTable(tTbl);
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static List<FieldSchema> getFieldsFromDeserializerForMsStorage(
      Table tbl, Deserializer deserializer) throws SerDeException, MetaException {
    List<FieldSchema> schema = MetaStoreUtils.getFieldsFromDeserializer(
        tbl.getTableName(), deserializer);
    for (FieldSchema field : schema) {
      field.setType(MetaStoreUtils.TYPE_FROM_DESERIALIZER);
    }
    return schema;
  }

  /**
   *
   * @param tableName
   *          table name
   * @param indexName
   *          index name
   * @param indexHandlerClass
   *          index handler class
   * @param indexedCols
   *          index columns
   * @param indexTblName
   *          index table's name
   * @param deferredRebuild
   *          referred build index table's data
   * @param inputFormat
   *          input format
   * @param outputFormat
   *          output format
   * @param serde
   * @param storageHandler
   *          index table's storage handler
   * @param location
   *          location
   * @param idxProps
   *          idx
   * @param serdeProps
   *          serde properties
   * @param collItemDelim
   * @param fieldDelim
   * @param fieldEscape
   * @param lineDelim
   * @param mapKeyDelim
   * @throws HiveException
   */
  public void createIndex(String tableName, String indexName, String indexHandlerClass,
      List<String> indexedCols, String indexTblName, boolean deferredRebuild,
      String inputFormat, String outputFormat, String serde,
      String storageHandler, String location,
      Map<String, String> idxProps, Map<String, String> tblProps, Map<String, String> serdeProps,
      String collItemDelim, String fieldDelim, String fieldEscape,
      String lineDelim, String mapKeyDelim, String indexComment)
      throws HiveException {

    try {
      String tdname = Utilities.getDatabaseName(tableName);
      String idname = Utilities.getDatabaseName(indexTblName);
      if (!idname.equals(tdname)) {
        throw new HiveException("Index on different database (" + idname
          + ") from base table (" + tdname + ") is not supported.");
      }

      Index old_index = null;
      try {
        old_index = getIndex(tableName, indexName);
      } catch (Exception e) {
      }
      if (old_index != null) {
        throw new HiveException("Index " + indexName + " already exists on table " + tableName);
      }

      org.apache.hadoop.hive.metastore.api.Table baseTbl = getTable(tableName).getTTable();
      if (TableType.VIRTUAL_VIEW.toString().equals(baseTbl.getTableType())) {
        throw new HiveException("tableName="+ tableName +" is a VIRTUAL VIEW. Index on VIRTUAL VIEW is not supported.");
      }
      if (baseTbl.isTemporary()) {
        throw new HiveException("tableName=" + tableName
            + " is a TEMPORARY TABLE. Index on TEMPORARY TABLE is not supported.");
      }

      org.apache.hadoop.hive.metastore.api.Table temp = null;
      try {
        temp = getTable(indexTblName).getTTable();
      } catch (Exception e) {
      }
      if (temp != null) {
        throw new HiveException("Table name " + indexTblName + " already exists. Choose another name.");
      }

      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setName(indexTblName);

      if(serde != null) {
        serdeInfo.setSerializationLib(serde);
      } else {
        if (storageHandler == null) {
          serdeInfo.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        } else {
          HiveStorageHandler sh = HiveUtils.getStorageHandler(getConf(), storageHandler);
          String serDeClassName = sh.getSerDeClass().getName();
          serdeInfo.setSerializationLib(serDeClassName);
        }
      }

      serdeInfo.setParameters(new HashMap<String, String>());
      if (fieldDelim != null) {
        serdeInfo.getParameters().put(FIELD_DELIM, fieldDelim);
        serdeInfo.getParameters().put(SERIALIZATION_FORMAT, fieldDelim);
      }
      if (fieldEscape != null) {
        serdeInfo.getParameters().put(ESCAPE_CHAR, fieldEscape);
      }
      if (collItemDelim != null) {
        serdeInfo.getParameters().put(COLLECTION_DELIM, collItemDelim);
      }
      if (mapKeyDelim != null) {
        serdeInfo.getParameters().put(MAPKEY_DELIM, mapKeyDelim);
      }
      if (lineDelim != null) {
        serdeInfo.getParameters().put(LINE_DELIM, lineDelim);
      }

      if (serdeProps != null) {
        Iterator<Entry<String, String>> iter = serdeProps.entrySet()
          .iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = iter.next();
          serdeInfo.getParameters().put(m.getKey(), m.getValue());
        }
      }

      List<FieldSchema> indexTblCols = new ArrayList<FieldSchema>();
      List<Order> sortCols = new ArrayList<Order>();
      int k = 0;
      Table metaBaseTbl = new Table(baseTbl);
      // Even though we are storing these in metastore, get regular columns. Indexes on lengthy
      // types from e.g. Avro schema will just fail to create the index table (by design).
      List<FieldSchema> cols = metaBaseTbl.getCols();
      for (int i = 0; i < cols.size(); i++) {
        FieldSchema col = cols.get(i);
        if (indexedCols.contains(col.getName())) {
          indexTblCols.add(col);
          sortCols.add(new Order(col.getName(), 1));
          k++;
        }
      }
      if (k != indexedCols.size()) {
        throw new RuntimeException(
            "Check the index columns, they should appear in the table being indexed.");
      }

      int time = (int) (System.currentTimeMillis() / 1000);
      org.apache.hadoop.hive.metastore.api.Table tt = null;
      HiveIndexHandler indexHandler = HiveUtils.getIndexHandler(this.getConf(), indexHandlerClass);

      String itname = Utilities.getTableName(indexTblName);
      if (indexHandler.usesIndexTable()) {
        tt = new org.apache.hadoop.hive.ql.metadata.Table(idname, itname).getTTable();
        List<FieldSchema> partKeys = baseTbl.getPartitionKeys();
        tt.setPartitionKeys(partKeys);
        tt.setTableType(TableType.INDEX_TABLE.toString());
        if (tblProps != null) {
          for (Entry<String, String> prop : tblProps.entrySet()) {
            tt.putToParameters(prop.getKey(), prop.getValue());
          }
        }
        SessionState ss = SessionState.get();
        CreateTableAutomaticGrant grants;
        if (ss != null && ((grants = ss.getCreateTableGrants()) != null)) {
            PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
            principalPrivs.setUserPrivileges(grants.getUserGrants());
            principalPrivs.setGroupPrivileges(grants.getGroupGrants());
            principalPrivs.setRolePrivileges(grants.getRoleGrants());
            tt.setPrivileges(principalPrivs);
          }
      }

      if(!deferredRebuild) {
        throw new RuntimeException("Please specify deferred rebuild using \" WITH DEFERRED REBUILD \".");
      }

      StorageDescriptor indexSd = new StorageDescriptor(
          indexTblCols,
          location,
          inputFormat,
          outputFormat,
          false/*compressed - not used*/,
          -1/*numBuckets - default is -1 when the table has no buckets*/,
          serdeInfo,
          null/*bucketCols*/,
          sortCols,
          null/*parameters*/);

      String ttname = Utilities.getTableName(tableName);
      Index indexDesc = new Index(indexName, indexHandlerClass, tdname, ttname, time, time, itname,
          indexSd, new HashMap<String,String>(), deferredRebuild);
      if (indexComment != null) {
        indexDesc.getParameters().put("comment", indexComment);
      }

      if (idxProps != null)
      {
        indexDesc.getParameters().putAll(idxProps);
      }

      indexHandler.analyzeIndexDefinition(baseTbl, indexDesc, tt);

      this.getMSC().createIndex(indexDesc, tt);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public Index getIndex(String baseTableName, String indexName) throws HiveException {
    String[] names = Utilities.getDbTableName(baseTableName);
    return this.getIndex(names[0], names[1], indexName);
  }

  public Index getIndex(String dbName, String baseTableName,
      String indexName) throws HiveException {
    try {
      return this.getMSC().getIndex(dbName, baseTableName, indexName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean dropIndex(String baseTableName, String index_name,
      boolean throwException, boolean deleteData) throws HiveException {
    String[] names = Utilities.getDbTableName(baseTableName);
    return dropIndex(names[0], names[1], index_name, throwException, deleteData);
  }

  public boolean dropIndex(String db_name, String tbl_name, String index_name,
      boolean throwException, boolean deleteData) throws HiveException {
    try {
      return getMSC().dropIndex(db_name, tbl_name, index_name, deleteData);
    } catch (NoSuchObjectException e) {
      if (throwException) {
        throw new HiveException("Index " + index_name + " doesn't exist. ", e);
      }
      return false;
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op. If ifPurge option is specified it is passed to the
   * hdfs command that removes table data from warehouse to make it skip trash.
   *
   * @param tableName
   *          table to drop
   * @param ifPurge
   *          completely purge the table (skipping trash) while removing data from warehouse
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String tableName, boolean ifPurge) throws HiveException {
    String[] names = Utilities.getDbTableName(tableName);
    dropTable(names[0], names[1], true, true, ifPurge);
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String tableName) throws HiveException {
    dropTable(tableName, false);
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param dbName
   *          database where the table lives
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String dbName, String tableName) throws HiveException {
    dropTable(dbName, tableName, true, true, false);
  }

  /**
   * Drops the table.
   *
   * @param dbName
   * @param tableName
   * @param deleteData
   *          deletes the underlying data along with metadata
   * @param ignoreUnknownTab
   *          an exception is thrown if this is false and the table doesn't exist
   * @throws HiveException
   */
  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws HiveException {
    dropTable(dbName, tableName, deleteData, ignoreUnknownTab, false);
  }

  /**
   * Drops the table.
   *
   * @param dbName
   * @param tableName
   * @param deleteData
   *          deletes the underlying data along with metadata
   * @param ignoreUnknownTab
   *          an exception is thrown if this is false and the table doesn't exist
   * @param ifPurge
   *          completely purge the table skipping trash while removing data from warehouse
   * @throws HiveException
   */
  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws HiveException {
    try {
      getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab, ifPurge);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public HiveConf getConf() {
    return (conf);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName) throws HiveException {
    return this.getTable(tableName, true);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @param throwException controls whether an exception is thrown or a returns a null
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName, boolean throwException) throws HiveException {
    String[] names = Utilities.getDbTableName(tableName);
    return this.getTable(names[0], names[1], throwException);
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @return the table
   * @exception HiveException
   *              if there's an internal error or if the table doesn't exist
   */
  public Table getTable(final String dbName, final String tableName) throws HiveException {
    if (tableName.contains(".")) {
      String[] names = Utilities.getDbTableName(tableName);
      return this.getTable(names[0], names[1], true);
    } else {
      return this.getTable(dbName, tableName, true);
    }
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName,
      boolean throwException) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable = getMSC().getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error("Table " + tableName + " not found: " + e.getMessage());
        throw new InvalidTableException(tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName + ". " + e.getMessage(), e);
    }

    // For non-views, we need to do some extra fixes
    if (!TableType.VIRTUAL_VIEW.toString().equals(tTable.getTableType())) {
      // Fix the non-printable chars
      Map<String, String> parameters = tTable.getSd().getParameters();
      String sf = parameters.get(SERIALIZATION_FORMAT);
      if (sf != null) {
        char[] b = sf.toCharArray();
        if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
          parameters.put(SERIALIZATION_FORMAT, Integer.toString(b[0]));
        }
      }

      // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
      // NOTE: LazySimpleSerDe does not support tables with a single column of
      // col
      // of type "array<string>". This happens when the table is created using
      // an
      // earlier version of Hive.
      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName().equals(
            tTable.getSd().getSerdeInfo().getSerializationLib())
          && tTable.getSd().getColsSize() > 0
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        tTable.getSd().getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }
    }

    return new Table(tTable);
  }

  /**
   * Get all table names for the current database.
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables() throws HiveException {
    return getAllTables(SessionState.get().getCurrentDatabase());
  }

  /**
   * Get all table names for the specified database.
   * @param dbName
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables(String dbName) throws HiveException {
    return getTablesByPattern(dbName, ".*");
  }

  /**
   * Returns all existing tables from default database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String tablePattern) throws HiveException {
    return getTablesByPattern(SessionState.get().getCurrentDatabase(),
        tablePattern);
  }

  /**
   * Returns all existing tables from the specified database which match the given
   * pattern. The matching occurs as per Java regular expressions.
   * @param dbName
   * @param tablePattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String dbName, String tablePattern) throws HiveException {
    try {
      return getMSC().getTables(dbName, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Returns all existing tables from the given database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param database
   *          the database name
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesForDb(String database, String tablePattern)
      throws HiveException {
    try {
      return getMSC().getTables(database, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing database names.
   *
   * @return List of database names.
   * @throws HiveException
   */
  public List<String> getAllDatabases() throws HiveException {
    try {
      return getMSC().getAllDatabases();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing databases that match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param databasePattern
   *          java re pattern
   * @return list of database names
   * @throws HiveException
   */
  public List<String> getDatabasesByPattern(String databasePattern) throws HiveException {
    try {
      return getMSC().getDatabases(databasePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean grantPrivileges(PrivilegeBag privileges)
      throws HiveException {
    try {
      return getMSC().grant_privileges(privileges);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param privileges
   *          a bag of privileges
   * @return true on success
   * @throws HiveException
   */
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws HiveException {
    try {
      return getMSC().revoke_privileges(privileges, grantOption);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Query metadata to see if a database with the given name already exists.
   *
   * @param dbName
   * @return true if a database with the given name already exists, false if
   *         does not exist.
   * @throws HiveException
   */
  public boolean databaseExists(String dbName) throws HiveException {
    return getDatabase(dbName) != null;
  }

  /**
   * Get the database by name.
   * @param dbName the name of the database.
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabase(String dbName) throws HiveException {
    try {
      return getMSC().getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get the Database object for current database
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabaseCurrent() throws HiveException {
    String currentDb = SessionState.get().getCurrentDatabase();
    return getDatabase(currentDb);
  }

  public void loadPartition(Path loadPath, String tableName,
      Map<String, String> partSpec, boolean replace,
      boolean inheritTableSpecs, boolean isSkewedStoreAsSubdir,
      boolean isSrcLocal, boolean isAcid) throws HiveException {
    Table tbl = getTable(tableName);
    loadPartition(loadPath, tbl, partSpec, replace, inheritTableSpecs,
        isSkewedStoreAsSubdir, isSrcLocal, isAcid);
  }

  /**
   * Load a directory into a Hive Table Partition - Alters existing content of
   * the partition with the contents of loadPath. - If the partition does not
   * exist - one is created - files in loadPath are moved into Hive. But the
   * directory itself is not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param  tbl
   *          name of table to be loaded.
   * @param partSpec
   *          defines which partition needs to be loaded
   * @param replace
   *          if true - replace files in the partition, otherwise add files to
   *          the partition
   * @param inheritTableSpecs if true, on [re]creating the partition, take the
   *          location/inputformat/outputformat/serde details from table spec
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param isAcid true if this is an ACID operation
   * @throws JSONException 
   */
  public Partition loadPartition(Path loadPath, Table tbl,
      Map<String, String> partSpec, boolean replace,
      boolean inheritTableSpecs, boolean isSkewedStoreAsSubdir,
      boolean isSrcLocal, boolean isAcid) throws HiveException {
    Path tblDataLocationPath =  tbl.getDataLocation();
    Partition newTPart = null;
    try {
      /**
       * Move files before creating the partition since down stream processes
       * check for existence of partition in metadata before accessing the data.
       * If partition is created before data is moved, downstream waiting
       * processes might move forward with partial data
       */

      Partition oldPart = getPartition(tbl, partSpec, false);
      Path oldPartPath = null;
      if(oldPart != null) {
        oldPartPath = oldPart.getDataLocation();
      }

      Path newPartPath = null;

      if (inheritTableSpecs) {
        Path partPath = new Path(tbl.getDataLocation(),
            Warehouse.makePartPath(partSpec));
        newPartPath = new Path(tblDataLocationPath.toUri().getScheme(), tblDataLocationPath.toUri().getAuthority(),
            partPath.toUri().getPath());

        if(oldPart != null) {
          /*
           * If we are moving the partition across filesystem boundaries
           * inherit from the table properties. Otherwise (same filesystem) use the
           * original partition location.
           *
           * See: HIVE-1707 and HIVE-2117 for background
           */
          FileSystem oldPartPathFS = oldPartPath.getFileSystem(getConf());
          FileSystem loadPathFS = loadPath.getFileSystem(getConf());
          if (FileUtils.equalsFileSystem(oldPartPathFS,loadPathFS)) {
            newPartPath = oldPartPath;
          }
        }
      } else {
        newPartPath = oldPartPath;
      }

      List<Path> newFiles = null;
      if (replace) {
        Hive.replaceFiles(tbl.getPath(), loadPath, newPartPath, oldPartPath, getConf(),
            isSrcLocal);
      } else {
        newFiles = new ArrayList<Path>();
        FileSystem fs = tbl.getDataLocation().getFileSystem(conf);
        Hive.copyFiles(conf, loadPath, newPartPath, fs, isSrcLocal, isAcid, newFiles);
      }

      newTPart = getPartition(tbl, partSpec, true, newPartPath.toString(),
          inheritTableSpecs, newFiles);
      //column stats will be inaccurate
      StatsSetupConst.clearColumnStatsState(newTPart.getParameters());

      // recreate the partition if it existed before
      if (isSkewedStoreAsSubdir) {
        org.apache.hadoop.hive.metastore.api.Partition newCreatedTpart = newTPart.getTPartition();
        SkewedInfo skewedInfo = newCreatedTpart.getSd().getSkewedInfo();
        /* Construct list bucketing location mappings from sub-directory name. */
        Map<List<String>, String> skewedColValueLocationMaps = constructListBucketingLocationMap(
            newPartPath, skewedInfo);
        /* Add list bucketing location mappings. */
        skewedInfo.setSkewedColValueLocationMaps(skewedColValueLocationMaps);
        newCreatedTpart.getSd().setSkewedInfo(skewedInfo);
        if(!this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          StatsSetupConst.setBasicStatsState(newTPart.getParameters(), StatsSetupConst.FALSE);
        }
        alterPartition(tbl.getDbName(), tbl.getTableName(), new Partition(tbl, newCreatedTpart));
        newTPart = getPartition(tbl, partSpec, true, newPartPath.toString(), inheritTableSpecs,
            newFiles);
        return new Partition(tbl, newCreatedTpart);
      }
      if(!this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        StatsSetupConst.setBasicStatsState(newTPart.getParameters(), StatsSetupConst.FALSE);
      }
      alterPartition(tbl.getDbName(), tbl.getTableName(), new Partition(tbl, newTPart.getTPartition()));
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (InvalidOperationException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return newTPart;
  }

  /**
 * Walk through sub-directory tree to construct list bucketing location map.
 *
 * @param fSta
 * @param fSys
 * @param skewedColValueLocationMaps
 * @param newPartPath
 * @param skewedInfo
 * @throws IOException
 */
private void walkDirTree(FileStatus fSta, FileSystem fSys,
    Map<List<String>, String> skewedColValueLocationMaps, Path newPartPath, SkewedInfo skewedInfo)
    throws IOException {
  /* Base Case. It's leaf. */
  if (!fSta.isDir()) {
    /* construct one location map if not exists. */
    constructOneLBLocationMap(fSta, skewedColValueLocationMaps, newPartPath, skewedInfo);
    return;
  }

  /* dfs. */
  FileStatus[] children = fSys.listStatus(fSta.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
  if (children != null) {
    for (FileStatus child : children) {
      walkDirTree(child, fSys, skewedColValueLocationMaps, newPartPath, skewedInfo);
    }
  }
}

/**
 * Construct a list bucketing location map
 * @param fSta
 * @param skewedColValueLocationMaps
 * @param newPartPath
 * @param skewedInfo
 */
private void constructOneLBLocationMap(FileStatus fSta,
    Map<List<String>, String> skewedColValueLocationMaps,
    Path newPartPath, SkewedInfo skewedInfo) {
  Path lbdPath = fSta.getPath().getParent();
  List<String> skewedValue = new ArrayList<String>();
  String lbDirName = FileUtils.unescapePathName(lbdPath.toString());
  String partDirName = FileUtils.unescapePathName(newPartPath.toString());
  String lbDirSuffix = lbDirName.replace(partDirName, "");
  String[] dirNames = lbDirSuffix.split(Path.SEPARATOR);
  for (String dirName : dirNames) {
    if ((dirName != null) && (dirName.length() > 0)) {
      // Construct skewed-value to location map except default directory.
      // why? query logic knows default-dir structure and don't need to get from map
        if (!dirName
            .equalsIgnoreCase(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME)) {
        String[] kv = dirName.split("=");
        if (kv.length == 2) {
          skewedValue.add(kv[1]);
        }
      }
    }
  }
  if ((skewedValue.size() > 0) && (skewedValue.size() == skewedInfo.getSkewedColNames().size())
      && !skewedColValueLocationMaps.containsKey(skewedValue)) {
    skewedColValueLocationMaps.put(skewedValue, lbdPath.toString());
  }
}

  /**
   * Construct location map from path
   *
   * @param newPartPath
   * @param skewedInfo
   * @return
   * @throws IOException
   * @throws FileNotFoundException
   */
  private Map<List<String>, String> constructListBucketingLocationMap(Path newPartPath,
      SkewedInfo skewedInfo) throws IOException, FileNotFoundException {
    Map<List<String>, String> skewedColValueLocationMaps = new HashMap<List<String>, String>();
    FileSystem fSys = newPartPath.getFileSystem(conf);
    walkDirTree(fSys.getFileStatus(newPartPath), fSys, skewedColValueLocationMaps, newPartPath,
        skewedInfo);
    return skewedColValueLocationMaps;
  }


  /**
   * Given a source directory name of the load path, load all dynamically generated partitions
   * into the specified table and return a list of strings that represent the dynamic partition
   * paths.
   * @param loadPath
   * @param tableName
   * @param partSpec
   * @param replace
   * @param numDP number of dynamic partitions
   * @param listBucketingEnabled
   * @param isAcid true if this is an ACID operation
   * @param txnId txnId, can be 0 unless isAcid == true
   * @return partition map details (PartitionSpec and Partition)
   * @throws HiveException
   * @throws JSONException 
   */
  public Map<Map<String, String>, Partition> loadDynamicPartitions(Path loadPath,
      String tableName, Map<String, String> partSpec, boolean replace,
      int numDP, boolean listBucketingEnabled, boolean isAcid, long txnId)
      throws HiveException {

    Set<Path> validPartitions = new HashSet<Path>();
    try {
      Map<Map<String, String>, Partition> partitionsMap = new
          LinkedHashMap<Map<String, String>, Partition>();

      FileSystem fs = loadPath.getFileSystem(conf);
      FileStatus[] leafStatus = HiveStatsUtils.getFileStatusRecurse(loadPath, numDP+1, fs);
      // Check for empty partitions
      for (FileStatus s : leafStatus) {
        try {
          validatePartitionNameCharacters(
            Warehouse.getPartValuesFromPartName(s.getPath().getParent().toString()));
        } catch (MetaException e) {
          throw new HiveException(e);
        }
        validPartitions.add(s.getPath().getParent());
      }

      int partsToLoad = validPartitions.size();
      if (partsToLoad == 0) {
        LOG.warn("No partition is generated by dynamic partitioning");
      }

      if (partsToLoad > conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)) {
        throw new HiveException("Number of dynamic partitions created is " + partsToLoad
            + ", which is more than "
            + conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)
            +". To solve this try to set " + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
            + " to at least " + partsToLoad + '.');
      }

      Table tbl = getTable(tableName);
      // for each dynamically created DP directory, construct a full partition spec
      // and load the partition based on that
      Iterator<Path> iter = validPartitions.iterator();
      LOG.info("Going to load " + partsToLoad + " partitions.");
      PrintStream ps = null;
      boolean inPlaceEligible = conf.getLong("fs.trash.interval", 0) <= 0
          && InPlaceUpdates.inPlaceEligible(conf);
      if(inPlaceEligible) {
        ps = SessionState.getConsole().getInfoStream();
      }
      int partitionsLoaded = 0;
      while (iter.hasNext()) {
        // get the dynamically created directory
        Path partPath = iter.next();
        assert fs.getFileStatus(partPath).isDir():
          "partitions " + partPath + " is not a directory !";

        // generate a full partition specification
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>(partSpec);
        Warehouse.makeSpecFromName(fullPartSpec, partPath);
        Partition newPartition = loadPartition(partPath, tbl, fullPartSpec, replace,
            true, listBucketingEnabled, false, isAcid);
        partitionsMap.put(fullPartSpec, newPartition);
        if (inPlaceEligible) {
          InPlaceUpdates.rePositionCursor(ps);
          InPlaceUpdates.reprintLine(ps, "Loaded : " + ++partitionsLoaded + "/" + partsToLoad +" partitions.");
        }
        LOG.info("New loading path = " + partPath + " with partSpec " + fullPartSpec);
      }
      if (isAcid) {
        List<String> partNames = new ArrayList<>(partitionsMap.size());
        for (Partition p : partitionsMap.values()) {
          partNames.add(p.getName());
        }
        metaStoreClient.addDynamicPartitions(txnId, tbl.getDbName(), tbl.getTableName(), partNames);
      }
      return partitionsMap;
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  /**
   * Load a directory into a Hive Table. - Alters existing content of table with
   * the contents of loadPath. - If table does not exist - an exception is
   * thrown - files in loadPath are moved into Hive. But the directory itself is
   * not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tableName
   *          name of table to be loaded.
   * @param replace
   *          if true - replace files in the table, otherwise add files to table
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param isSkewedStoreAsSubdir
   *          if list bucketing enabled
   * @param isAcid true if this is an ACID based write
   */
  public void loadTable(Path loadPath, String tableName, boolean replace,
      boolean isSrcLocal, boolean isSkewedStoreAsSubdir, boolean isAcid)
      throws HiveException {
    List<Path> newFiles = new ArrayList<Path>();
    Table tbl = getTable(tableName);
    HiveConf sessionConf = SessionState.getSessionConf();
    if (replace) {
      Path tableDest = tbl.getPath();
      replaceFiles(tableDest, loadPath, tableDest, tableDest, sessionConf, isSrcLocal);
    } else {
      FileSystem fs;
      try {
        fs = tbl.getDataLocation().getFileSystem(sessionConf);
        copyFiles(sessionConf, loadPath, tbl.getPath(), fs, isSrcLocal, isAcid, newFiles);
      } catch (IOException e) {
        throw new HiveException("addFiles: filesystem error in check phase", e);
      }
    }
    if(!this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
    }  else {
      tbl.getParameters().put(StatsSetupConst.STATS_GENERATED_VIA_STATS_TASK, "true");
    }

    //column stats will be inaccurate
    StatsSetupConst.clearColumnStatsState(tbl.getParameters());

    try {
      if (isSkewedStoreAsSubdir) {
        SkewedInfo skewedInfo = tbl.getSkewedInfo();
        // Construct list bucketing location mappings from sub-directory name.
        Map<List<String>, String> skewedColValueLocationMaps = constructListBucketingLocationMap(
            tbl.getPath(), skewedInfo);
        // Add list bucketing location mappings.
        skewedInfo.setSkewedColValueLocationMaps(skewedColValueLocationMaps);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

    try {
      alterTable(tableName, tbl);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    }

    fireInsertEvent(tbl, null, newFiles);
  }

  /**
   * Creates a partition.
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec) throws HiveException {
    try {
      return new Partition(tbl, getMSC().add_partition(
          Partition.createMetaPartitionObject(tbl, partSpec, null)));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public List<Partition> createPartitions(AddPartitionDesc addPartitionDesc) throws HiveException {
    Table tbl = getTable(addPartitionDesc.getDbName(), addPartitionDesc.getTableName());
    int size = addPartitionDesc.getPartitionCount();
    List<org.apache.hadoop.hive.metastore.api.Partition> in =
        new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>(size);
    for (int i = 0; i < size; ++i) {
      in.add(convertAddSpecToMetaPartition(tbl, addPartitionDesc.getPartition(i)));
    }
    List<Partition> out = new ArrayList<Partition>();
    try {
      if (!addPartitionDesc.getReplaceMode()){
        // TODO: normally, the result is not necessary; might make sense to pass false
        for (org.apache.hadoop.hive.metastore.api.Partition outPart
            : getMSC().add_partitions(in, addPartitionDesc.isIfNotExists(), true)) {
          out.add(new Partition(tbl, outPart));
        }
      } else {
        getMSC().alter_partitions(addPartitionDesc.getDbName(), addPartitionDesc.getTableName(), in);
        List<String> part_names = new ArrayList<String>();
        for (org.apache.hadoop.hive.metastore.api.Partition p: in){
          part_names.add(Warehouse.makePartName(tbl.getPartitionKeys(), p.getValues()));
        }
        for ( org.apache.hadoop.hive.metastore.api.Partition outPart :
        getMSC().getPartitionsByNames(addPartitionDesc.getDbName(), addPartitionDesc.getTableName(),part_names)){
          out.add(new Partition(tbl,outPart));
        }
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return out;
  }

  private org.apache.hadoop.hive.metastore.api.Partition convertAddSpecToMetaPartition(
      Table tbl, AddPartitionDesc.OnePartitionDesc addSpec) throws HiveException {
    Path location = addSpec.getLocation() != null
        ? new Path(tbl.getPath(), addSpec.getLocation()) : null;
    if (location !=null && !Utilities.isDefaultNameNode(conf)) {
      // Ensure that it is a full qualified path (in most cases it will be since tbl.getPath() is full qualified)
      location = new Path(Utilities.getQualifiedPath(conf, location));
    }
    org.apache.hadoop.hive.metastore.api.Partition part =
        Partition.createMetaPartitionObject(tbl, addSpec.getPartSpec(), location);
    if (addSpec.getPartParams() != null) {
      part.setParameters(addSpec.getPartParams());
    }
    if (addSpec.getInputFormat() != null) {
      part.getSd().setInputFormat(addSpec.getInputFormat());
    }
    if (addSpec.getOutputFormat() != null) {
      part.getSd().setOutputFormat(addSpec.getOutputFormat());
    }
    if (addSpec.getNumBuckets() != -1) {
      part.getSd().setNumBuckets(addSpec.getNumBuckets());
    }
    if (addSpec.getCols() != null) {
      part.getSd().setCols(addSpec.getCols());
    }
    if (addSpec.getSerializationLib() != null) {
        part.getSd().getSerdeInfo().setSerializationLib(addSpec.getSerializationLib());
    }
    if (addSpec.getSerdeParams() != null) {
      part.getSd().getSerdeInfo().setParameters(addSpec.getSerdeParams());
    }
    if (addSpec.getBucketCols() != null) {
      part.getSd().setBucketCols(addSpec.getBucketCols());
    }
    if (addSpec.getSortCols() != null) {
      part.getSd().setSortCols(addSpec.getSortCols());
    }
    return part;
  }

  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate) throws HiveException {
    return getPartition(tbl, partSpec, forceCreate, null, true, null);
  }

  /**
   * Returns partition metadata
   *
   * @param tbl
   *          the partition's table
   * @param partSpec
   *          partition keys and values
   * @param forceCreate
   *          if this is true and partition doesn't exist then a partition is
   *          created
   * @param partPath the path where the partition data is located
   * @param inheritTableSpecs whether to copy over the table specs for if/of/serde
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, Map<String, String> partSpec, boolean forceCreate,
                                String partPath, boolean inheritTableSpecs)
      throws HiveException {
    return getPartition(tbl, partSpec, forceCreate, partPath, inheritTableSpecs, null);
  }

  /**
   * Returns partition metadata
   *
   * @param tbl
   *          the partition's table
   * @param partSpec
   *          partition keys and values
   * @param forceCreate
   *          if this is true and partition doesn't exist then a partition is
   *          created
   * @param partPath the path where the partition data is located
   * @param inheritTableSpecs whether to copy over the table specs for if/of/serde
   * @param newFiles An optional list of new files that were moved into this partition.  If
   *                 non-null these will be included in the DML event sent to the metastore.
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate, String partPath, boolean inheritTableSpecs, List<Path> newFiles)
      throws HiveException {
    tbl.validatePartColumnNames(partSpec, true);
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      // enable dynamic partitioning
      if ((val == null && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING))
          || (val != null && val.length() == 0)) {
        throw new HiveException("get partition: Value for key "
            + field.getName() + " is null or empty");
      } else if (val != null){
        pvals.add(val);
      }
    }
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      tpart = getMSC().getPartitionWithAuthInfo(tbl.getDbName(),
          tbl.getTableName(), pvals, getUserName(), getGroupNames());
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given partition
      // key value pairs - thrift cannot handle null return values, hence
      // getPartition() throws NoSuchObjectException to indicate null partition
      tpart = null;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    try {
      if (forceCreate) {
        if (tpart == null) {
          LOG.debug("creating partition for table " + tbl.getTableName()
                    + " with partition spec : " + partSpec);
          try {
            tpart = getMSC().appendPartition(tbl.getDbName(), tbl.getTableName(), pvals);
          } catch (AlreadyExistsException aee) {
            LOG.debug("Caught already exists exception, trying to alter partition instead");
            tpart = getMSC().getPartitionWithAuthInfo(tbl.getDbName(),
              tbl.getTableName(), pvals, getUserName(), getGroupNames());
            alterPartitionSpec(tbl, partSpec, tpart, inheritTableSpecs, partPath);
          } catch (Exception e) {
            if (CheckJDOException.isJDODataStoreException(e)) {
              // Using utility method above, so that JDODataStoreException doesn't
              // have to be used here. This helps avoid adding jdo dependency for
              // hcatalog client uses
              LOG.debug("Caught JDO exception, trying to alter partition instead");
              tpart = getMSC().getPartitionWithAuthInfo(tbl.getDbName(),
                tbl.getTableName(), pvals, getUserName(), getGroupNames());
              if (tpart == null) {
                // This means the exception was caused by something other than a race condition
                // in creating the partition, since the partition still doesn't exist.
                throw e;
              }
              alterPartitionSpec(tbl, partSpec, tpart, inheritTableSpecs, partPath);
            } else {
              throw e;
            }
          }
        }
        else {
          alterPartitionSpec(tbl, partSpec, tpart, inheritTableSpecs, partPath);
          fireInsertEvent(tbl, partSpec, newFiles);
        }
      }
      if (tpart == null) {
        return null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }

  private void alterPartitionSpec(Table tbl,
                                  Map<String, String> partSpec,
                                  org.apache.hadoop.hive.metastore.api.Partition tpart,
                                  boolean inheritTableSpecs,
                                  String partPath) throws HiveException, InvalidOperationException {
    LOG.debug("altering partition for table " + tbl.getTableName() + " with partition spec : "
        + partSpec);
    if (inheritTableSpecs) {
      tpart.getSd().setOutputFormat(tbl.getTTable().getSd().getOutputFormat());
      tpart.getSd().setInputFormat(tbl.getTTable().getSd().getInputFormat());
      tpart.getSd().getSerdeInfo().setSerializationLib(tbl.getSerializationLib());
      tpart.getSd().getSerdeInfo().setParameters(
          tbl.getTTable().getSd().getSerdeInfo().getParameters());
      tpart.getSd().setBucketCols(tbl.getBucketCols());
      tpart.getSd().setNumBuckets(tbl.getNumBuckets());
      tpart.getSd().setSortCols(tbl.getSortCols());
    }
    if (partPath == null || partPath.trim().equals("")) {
      throw new HiveException("new partition path should not be null or empty.");
    }
    tpart.getSd().setLocation(partPath);
    String fullName = tbl.getTableName();
    if (!org.apache.commons.lang.StringUtils.isEmpty(tbl.getDbName())) {
      fullName = tbl.getDbName() + "." + tbl.getTableName();
    }
    alterPartition(fullName, new Partition(tbl, tpart));
  }

  private void fireInsertEvent(Table tbl, Map<String, String> partitionSpec, List<Path> newFiles)
      throws HiveException {
    if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML)) {
      LOG.debug("Firing dml insert event");
      if (tbl.isTemporary()) {
        LOG.debug("Not firing dml insert event as " + tbl.getTableName() + " is temporary");
        return;
      }
      FireEventRequestData data = new FireEventRequestData();
      InsertEventRequestData insertData = new InsertEventRequestData();
      data.setInsertData(insertData);
      if (newFiles != null && newFiles.size() > 0) {
        for (Path p : newFiles) {
          insertData.addToFilesAdded(p.toString());
        }
      } else {
        insertData.setFilesAdded(new ArrayList<String>());
      }
      FireEventRequest rqst = new FireEventRequest(true, data);
      rqst.setDbName(tbl.getDbName());
      rqst.setTableName(tbl.getTableName());
      if (partitionSpec != null && partitionSpec.size() > 0) {
        List<String> partVals = new ArrayList<String>(partitionSpec.size());
        for (FieldSchema fs : tbl.getPartitionKeys()) {
          partVals.add(partitionSpec.get(fs.getName()));
        }
        rqst.setPartitionVals(partVals);
      }
      try {
        getMSC().fireListenerEvent(rqst);
      } catch (TException e) {
        throw new HiveException(e);
      }
    }
  }

  public boolean dropPartition(String tblName, List<String> part_vals, boolean deleteData)
      throws HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    return dropPartition(names[0], names[1], part_vals, deleteData);
  }

  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws HiveException {
    return dropPartition(db_name, tbl_name, part_vals,
                         PartitionDropOptions.instance().deleteData(deleteData));
  }

  public boolean dropPartition(String dbName, String tableName, List<String> partVals, PartitionDropOptions options)
      throws HiveException {
    try {
      return getMSC().dropPartition(dbName, tableName, partVals, options);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public List<Partition> dropPartitions(String tblName, List<DropTableDesc.PartSpec> partSpecs,
      boolean deleteData, boolean ifExists) throws HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    return dropPartitions(names[0], names[1], partSpecs, deleteData, ifExists);
  }

  public List<Partition> dropPartitions(String dbName, String tblName,
      List<DropTableDesc.PartSpec> partSpecs,  boolean deleteData,
      boolean ifExists) throws HiveException {
    return dropPartitions(dbName, tblName, partSpecs,
                          PartitionDropOptions.instance()
                                              .deleteData(deleteData)
                                              .ifExists(ifExists));
  }

  public List<Partition> dropPartitions(String tblName, List<DropTableDesc.PartSpec> partSpecs,
                                        PartitionDropOptions dropOptions) throws HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    return dropPartitions(names[0], names[1], partSpecs, dropOptions);
  }

  public List<Partition> dropPartitions(String dbName, String tblName,
      List<DropTableDesc.PartSpec> partSpecs, PartitionDropOptions dropOptions) throws HiveException {
    try {
      Table tbl = getTable(dbName, tblName);
      List<ObjectPair<Integer, byte[]>> partExprs =
          new ArrayList<ObjectPair<Integer,byte[]>>(partSpecs.size());
      for (DropTableDesc.PartSpec partSpec : partSpecs) {
        partExprs.add(new ObjectPair<Integer, byte[]>(partSpec.getPrefixLength(),
            SerializationUtilities.serializeExpressionToKryo(partSpec.getPartSpec())));
      }
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts = getMSC().dropPartitions(
          dbName, tblName, partExprs, dropOptions);
      return convertFromMetastore(tbl, tParts);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public List<String> getPartitionNames(String tblName, short max) throws HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    return getPartitionNames(names[0], names[1], max);
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max)
      throws HiveException {
    List<String> names = null;
    try {
      names = getMSC().listPartitionNames(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  public List<String> getPartitionNames(String dbName, String tblName,
      Map<String, String> partSpec, short max) throws HiveException {
    List<String> names = null;
    Table t = getTable(dbName, tblName);

    List<String> pvals = MetaStoreUtils.getPvals(t.getPartCols(), partSpec);

    try {
      names = getMSC().listPartitionNames(dbName, tblName, pvals, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has
   *
   * @param tbl
   *          object for which partition is needed
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    if (tbl.isPartitioned()) {
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
      try {
        tParts = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
            (short) -1, getUserName(), getGroupNames());
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
      List<Partition> parts = new ArrayList<Partition>(tParts.size());
      for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
        parts.add(new Partition(tbl, tpart));
      }
      return parts;
    } else {
      Partition part = new Partition(tbl);
      ArrayList<Partition> parts = new ArrayList<Partition>(1);
      parts.add(part);
      return parts;
    }
  }

  /**
   * Get all the partitions; unlike {@link #getPartitions(Table)}, does not include auth.
   * @param tbl table for which partitions are needed
   * @return list of partition objects
   */
  public Set<Partition> getAllPartitionsOf(Table tbl) throws HiveException {
    if (!tbl.isPartitioned()) {
      return Sets.newHashSet(new Partition(tbl));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
    try {
      tParts = getMSC().listPartitions(tbl.getDbName(), tbl.getTableName(), (short)-1);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    Set<Partition> parts = new LinkedHashSet<Partition>(tParts.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
      parts.add(new Partition(tbl, tpart));
    }
    return parts;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param limit number of partitions to return
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec,
      short limit)
  throws HiveException {
    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }

    List<String> partialPvals = MetaStoreUtils.getPvals(tbl.getPartCols(), partialPartSpec);

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = null;
    try {
      partitions = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
          partialPvals, limit, getUserName(), getGroupNames());
    } catch (Exception e) {
      throw new HiveException(e);
    }

    List<Partition> qlPartitions = new ArrayList<Partition>();
    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      qlPartitions.add( new Partition(tbl, p));
    }

    return qlPartitions;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec)
  throws HiveException {
    return getPartitions(tbl, partialPartSpec, (short)-1);
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partialPartSpec
   *          partial partition specification (some subpartitions can be empty).
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl,
      Map<String, String> partialPartSpec)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }

    List<String> names = getPartitionNames(tbl.getDbName(), tbl.getTableName(),
        partialPartSpec, (short)-1);

    List<Partition> partitions = getPartitionsByNames(tbl, names);
    return partitions;
  }

  /**
   * Get all partitions of the table that matches the list of given partition names.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partNames
   *          list of partition names
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl, List<String> partNames)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }
    List<Partition> partitions = new ArrayList<Partition>(partNames.size());

    int batchSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX);
    // TODO: might want to increase the default batch size. 1024 is viable; MS gets OOM if too high.
    int nParts = partNames.size();
    int nBatches = nParts / batchSize;

    try {
      for (int i = 0; i < nBatches; ++i) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(i*batchSize, (i+1)*batchSize));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }

      if (nParts > nBatches * batchSize) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(nBatches*batchSize, nParts));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return partitions;
  }

  /**
   * Get a list of Partitions by filter.
   * @param tbl The table containing the partitions.
   * @param filter A string represent partition predicates.
   * @return a list of partitions satisfying the partition predicates.
   * @throws HiveException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public List<Partition> getPartitionsByFilter(Table tbl, String filter)
      throws HiveException, MetaException, NoSuchObjectException, TException {

    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts = getMSC().listPartitionsByFilter(
        tbl.getDbName(), tbl.getTableName(), filter, (short)-1);
    return convertFromMetastore(tbl, tParts);
  }

  private static List<Partition> convertFromMetastore(Table tbl,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions) throws HiveException {
    if (partitions == null) {
      return new ArrayList<Partition>();
    }

    List<Partition> results = new ArrayList<Partition>(partitions.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tPart : partitions) {
      results.add(new Partition(tbl, tPart));
    }
    return results;
  }

  /**
   * Get a list of Partitions by expr.
   * @param tbl The table containing the partitions.
   * @param expr A serialized expression for partition predicates.
   * @param conf Hive config.
   * @param result the resulting list of partitions
   * @return whether the resulting list contains partitions which may or may not match the expr
   */
  public boolean getPartitionsByExpr(Table tbl, ExprNodeGenericFuncDesc expr, HiveConf conf,
      List<Partition> result) throws HiveException, TException {
    assert result != null;
    byte[] exprBytes = SerializationUtilities.serializeExpressionToKryo(expr);
    String defaultPartitionName = HiveConf.getVar(conf, ConfVars.DEFAULTPARTITIONNAME);
    List<org.apache.hadoop.hive.metastore.api.Partition> msParts =
        new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    boolean hasUnknownParts = getMSC().listPartitionsByExpr(tbl.getDbName(),
        tbl.getTableName(), exprBytes, defaultPartitionName, (short)-1, msParts);
    result.addAll(convertFromMetastore(tbl, msParts));
    return hasUnknownParts;
  }

  public void validatePartitionNameCharacters(List<String> partVals) throws HiveException {
    try {
      getMSC().validatePartitionNameCharacters(partVals);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public void createRole(String roleName, String ownerName)
      throws HiveException {
    try {
      getMSC().create_role(new Role(roleName, -1, ownerName));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropRole(String roleName) throws HiveException {
    try {
      getMSC().drop_role(roleName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing role names.
   *
   * @return List of role names.
   * @throws HiveException
   */
  public List<String> getAllRoleNames() throws HiveException {
    try {
      return getMSC().listRoleNames();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public  List<RolePrincipalGrant> getRoleGrantInfoForPrincipal(String principalName, PrincipalType principalType) throws HiveException {
    try {
      GetRoleGrantsForPrincipalRequest req = new GetRoleGrantsForPrincipalRequest(principalName, principalType);
      GetRoleGrantsForPrincipalResponse resp = getMSC().get_role_grants_for_principal(req);
      return resp.getPrincipalGrants();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public boolean grantRole(String roleName, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws HiveException {
    try {
      return getMSC().grant_role(roleName, userName, principalType, grantor,
        grantorType, grantOption);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean revokeRole(String roleName, String userName,
      PrincipalType principalType, boolean grantOption)  throws HiveException {
    try {
      return getMSC().revoke_role(roleName, userName, principalType, grantOption);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Role> listRoles(String userName,  PrincipalType principalType)
      throws HiveException {
    try {
      return getMSC().list_roles(userName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param db_name
   *          database name
   * @param table_name
   *          table name
   * @param part_values
   *          partition values
   * @param column_name
   *          column name
   * @param user_name
   *          user name
   * @param group_names
   *          group names
   * @return the privilege set
   * @throws HiveException
   */
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectType objectType,
      String db_name, String table_name, List<String> part_values,
      String column_name, String user_name, List<String> group_names)
      throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, db_name,
          table_name, part_values, column_name);
      return getMSC().get_privilege_set(hiveObj, user_name, group_names);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param principalName
   * @param principalType
   * @param dbName
   * @param tableName
   * @param partValues
   * @param columnName
   * @return list of privileges
   * @throws HiveException
   */
  public List<HiveObjectPrivilege> showPrivilegeGrant(
      HiveObjectType objectType, String principalName,
      PrincipalType principalType, String dbName, String tableName,
      List<String> partValues, String columnName) throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, dbName, tableName,
          partValues, columnName);
      return getMSC().list_privileges(principalName, principalType, hiveObj);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  // for each file or directory in 'srcs', make mapping for every file in src to safe name in dest
  private static List<List<Path[]>> checkPaths(HiveConf conf, FileSystem fs,
      FileStatus[] srcs, FileSystem srcFs, Path destf, boolean replace)
      throws HiveException {

    List<List<Path[]>> result = new ArrayList<List<Path[]>>();
    try {
      FileStatus destStatus = !replace ? FileUtils.getFileStatusOrNull(fs, destf) : null;
      if (destStatus != null && !destStatus.isDir()) {
        throw new HiveException("checkPaths: destination " + destf
            + " should be a directory");
      }
      for (FileStatus src : srcs) {
        FileStatus[] items;
        if (src.isDir()) {
          items = srcFs.listStatus(src.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
          Arrays.sort(items);
        } else {
          items = new FileStatus[] {src};
        }

        List<Path[]> srcToDest = new ArrayList<Path[]>();
        for (FileStatus item : items) {

          Path itemSource = item.getPath();

          if (Utilities.isTempPath(item)) {
            // This check is redundant because temp files are removed by
            // execution layer before
            // calling loadTable/Partition. But leaving it in just in case.
            srcFs.delete(itemSource, true);
            continue;
          }

          // Strip off the file type, if any so we don't make:
          // 000000_0.gz -> 000000_0.gz_copy_1
          String name = itemSource.getName();
          String filetype;
          int index = name.lastIndexOf('.');
          if (index >= 0) {
            filetype = name.substring(index);
            name = name.substring(0, index);
          } else {
            filetype = "";
          }

          Path itemDest = new Path(destf, itemSource.getName());

          if (!replace) {
            // It's possible that the file we're copying may have the same
            // relative name as an existing file in the "destf" directory.
            // So let's make a quick check to see if we can rename any
            // potential offenders so as to allow them to move into the
            // "destf" directory. The scheme is dead simple: simply tack
            // on "_copy_N" where N starts at 1 and works its way up until
            // we find a free space.

            // removed source file staging.. it's more confusing when failed.
            for (int counter = 1; fs.exists(itemDest) || destExists(result, itemDest); counter++) {
              itemDest = new Path(destf, name + ("_copy_" + counter) + filetype);
            }
          }
          srcToDest.add(new Path[]{itemSource, itemDest});
        }
        result.add(srcToDest);
      }
    } catch (IOException e) {
      throw new HiveException("checkPaths: filesystem error in check phase. " + e.getMessage(), e);
    }
    return result;
  }

  private static boolean destExists(List<List<Path[]>> result, Path proposed) {
    for (List<Path[]> sdpairs : result) {
      for (Path[] sdpair : sdpairs) {
        if (sdpair[1].equals(proposed)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isSubDir(Path srcf, Path destf, FileSystem srcFs, FileSystem destFs, boolean isSrcLocal) {
    if (srcf == null) {
      LOG.debug("The source path is null for isSubDir method.");
      return false;
    }

    String fullF1 = getQualifiedPathWithoutSchemeAndAuthority(srcf, srcFs);
    String fullF2 = getQualifiedPathWithoutSchemeAndAuthority(destf, destFs);

    boolean isInTest = Boolean.valueOf(HiveConf.getBoolVar(srcFs.getConf(), ConfVars.HIVE_IN_TEST));
    // In the automation, the data warehouse is the local file system based.
    LOG.debug("The source path is " + fullF1 + " and the destination path is " + fullF2);
    if (isInTest) {
      return fullF1.startsWith(fullF2);
    }

    // schema is diff, return false
    String schemaSrcf = srcf.toUri().getScheme();
    String schemaDestf = destf.toUri().getScheme();

    // if the schemaDestf is null, it means the destination is not in the local file system
    if (schemaDestf == null && isSrcLocal) {
      LOG.debug("The source file is in the local while the dest not.");
      return false;
    }

    // If both schema information are provided, they should be the same.
    if (schemaSrcf != null && schemaDestf != null && !schemaSrcf.equals(schemaDestf)) {
      LOG.debug("The source path's schema is " + schemaSrcf +
        " and the destination path's schema is " + schemaDestf + ".");
      return false;
    }

    LOG.debug("The source path is " + fullF1 + " and the destination path is " + fullF2);
    return fullF1.startsWith(fullF2);
  }

  private static String getQualifiedPathWithoutSchemeAndAuthority(Path srcf, FileSystem fs) {
    Path currentWorkingDir = fs.getWorkingDirectory();
    Path path = srcf.makeQualified(srcf.toUri(), currentWorkingDir);
    return ShimLoader.getHadoopShims().getPathWithoutSchemeAndAuthority(path).toString();
  }

  //it is assumed that parent directory of the destf should already exist when this
  //method is called. when the replace value is true, this method works a little different
  //from mv command if the destf is a directory, it replaces the destf instead of moving under
  //the destf. in this case, the replaced destf still preserves the original destf's permission
  public static boolean moveFile(HiveConf conf, Path srcf, Path destf,
      boolean replace, boolean isSrcLocal) throws HiveException {
    boolean success = false;
    FileSystem srcFs, destFs;
    try {
      destFs = destf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }
    try {
      srcFs = srcf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }

    //needed for perm inheritance.
    boolean inheritPerms = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    HadoopShims shims = ShimLoader.getHadoopShims();
    HadoopShims.HdfsFileStatus destStatus = null;

    // If source path is a subdirectory of the destination path:
    //   ex: INSERT OVERWRITE DIRECTORY 'target/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;
    //   where the staging directory is a subdirectory of the destination directory
    // (1) Do not delete the dest dir before doing the move operation.
    // (2) It is assumed that subdir and dir are in same encryption zone.
    // (3) Move individual files from scr dir to dest dir.
    boolean destIsSubDir = isSubDir(srcf, destf, srcFs, destFs, isSrcLocal);
    try {
      if (inheritPerms || replace) {
        try{
          destStatus = shims.getFullFileStatus(conf, destFs, destf.getParent());
          //if destf is an existing directory:
          //if replace is true, delete followed by rename(mv) is equivalent to replace
          //if replace is false, rename (mv) actually move the src under dest dir
          //if destf is an existing file, rename is actually a replace, and do not need
          // to delete the file first
          if (replace && !destIsSubDir) {
            LOG.debug("The path " + destf.toString() + " is deleted");
            destFs.delete(destf, true);
          }
        } catch (FileNotFoundException ignore) {
          //if dest dir does not exist, any re
          if (inheritPerms) {
            destStatus = shims.getFullFileStatus(conf, destFs, destf.getParent());
          }
        }
      }
      if (isSrcLocal) {
        // For local src file, copy to hdfs
        destFs.copyFromLocalFile(srcf, destf);
        success = true;
      } else {
        if (needToCopy(srcf, destf, srcFs, destFs)) {
          //copy if across file system or encryption zones.
          LOG.info("Copying source " + srcf + " to " + destf + " because HDFS encryption zones are different.");
          success = FileUtils.copy(srcf.getFileSystem(conf), srcf, destf.getFileSystem(conf), destf,
              true,    // delete source
              replace, // overwrite destination
              conf);
        } else {
          if (destIsSubDir) {
            FileStatus[] srcs = destFs.listStatus(srcf, FileUtils.HIDDEN_FILES_PATH_FILTER);
            if (srcs.length == 0) {
              success = true; // Nothing to move.
            }

            /* Move files one by one because source is a subdirectory of destination */
            for (FileStatus status : srcs) {
              Path destFile;

              /* Append the source filename to the destination directory */
              if (destFs.isDirectory(destf)) {
                destFile = new Path(destf, status.getPath().getName());
              } else {
                destFile = destf;
              }

              // Destination should be replaced, so we delete it first
              if (destFs.exists(destFile)) {
                if (!destFs.delete(destFile, true)) {
                  throw new HiveException(String.format("File to replace could not be deleted: %s", destFile));
                }
              }

              if (!(destFs.rename(status.getPath(), destFile))) {
                throw new HiveException("Unable to move source " + status.getPath() + " to destination " + destf);
              }
            }

            success = true;
          } else {
            success = destFs.rename(srcf, destf);
          }
        }
      }

      LOG.info((replace ? "Replacing src:" : "Renaming src: ") + srcf.toString()
          + ", dest: " + destf.toString()  + ", Status:" + success);
    } catch (IOException ioe) {
      throw new HiveException("Unable to move source " + srcf + " to destination " + destf, ioe);
    }

    if (success && inheritPerms) {
      try {
        ShimLoader.getHadoopShims().setFullFileStatus(conf, destStatus, destFs, destf);
      } catch (IOException e) {
        LOG.warn("Error setting permission of file " + destf + ": "+ e.getMessage(), e);
      }
    }
    return success;
  }

  /**
   * If moving across different FileSystems or differnent encryption zone, need to do a File copy instead of rename.
   * TODO- consider if need to do this for different file authority.
   */
  static protected boolean needToCopy(Path srcf, Path destf, FileSystem srcFs, FileSystem destFs) throws HiveException, IOException {
    //Check if different FileSystems
    if (!srcFs.getClass().equals(destFs.getClass())) {
      return true;
    }

    //Check if different encryption zones
    HadoopShims.HdfsEncryptionShim hdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim();
    return hdfsEncryptionShim != null && (hdfsEncryptionShim.isPathEncrypted(srcf) || hdfsEncryptionShim.isPathEncrypted(destf))
      && !hdfsEncryptionShim.arePathsOnSameEncryptionZone(srcf, destf);
  }

  /**
   * Copy files.  This handles building the mapping for buckets and such between the source and
   * destination
   * @param conf Configuration object
   * @param srcf source directory, if bucketed should contain bucket files
   * @param destf directory to move files into
   * @param fs Filesystem
   * @param isSrcLocal true if source is on local file system
   * @param isAcid true if this is an ACID based write
   * @param newFiles if this is non-null, a list of files that were created as a result of this
   *                 move will be returned.
   * @throws HiveException
   */
  static protected void copyFiles(HiveConf conf, Path srcf, Path destf,
      FileSystem fs, boolean isSrcLocal, boolean isAcid, List<Path> newFiles) throws HiveException {
    boolean inheritPerms = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS);
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        FileUtils.mkdir(fs, destf, inheritPerms, conf);
      }
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    FileSystem srcFs;
    try {
      srcFs = srcf.getFileSystem(conf);
      srcs = srcFs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase. " + e.getMessage(), e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }

    // If we're moving files around for an ACID write then the rules and paths are all different.
    // You can blame this on Owen.
    if (isAcid) {
      moveAcidFiles(srcFs, srcs, destf, newFiles);
    } else {
    // check that source and target paths exist
      List<List<Path[]>> result = checkPaths(conf, fs, srcs, srcFs, destf, false);
      // move it, move it
      try {
        for (List<Path[]> sdpairs : result) {
          for (Path[] sdpair : sdpairs) {
            if (!moveFile(conf, sdpair[0], sdpair[1], false, isSrcLocal)) {
              throw new IOException("Cannot move " + sdpair[0] + " to "
                  + sdpair[1]);
            }
            if (newFiles != null) newFiles.add(sdpair[1]);
          }
        }
      } catch (IOException e) {
        throw new HiveException("copyFiles: error while moving files!!! " + e.getMessage(), e);
      }
    }
  }

  private static void moveAcidFiles(FileSystem fs, FileStatus[] stats, Path dst,
                                    List<Path> newFiles) throws HiveException {
    // The layout for ACID files is table|partname/base|delta/bucket
    // We will always only be writing delta files.  In the buckets created by FileSinkOperator
    // it will look like bucket/delta/bucket.  So we need to move that into the above structure.
    // For the first mover there will be no delta directory, so we can move the whole directory.
    // For everyone else we will need to just move the buckets under the existing delta
    // directory.

    Set<Path> createdDeltaDirs = new HashSet<Path>();
    // Open the original path we've been given and find the list of original buckets
    for (FileStatus stat : stats) {
      Path srcPath = stat.getPath();

      LOG.debug("Acid move Looking for original buckets in " + srcPath);

      FileStatus[] origBucketStats = null;
      try {
        origBucketStats = fs.listStatus(srcPath, AcidUtils.originalBucketFilter);
      } catch (IOException e) {
        String msg = "Unable to look for bucket files in src path " + srcPath.toUri().toString();
        LOG.error(msg);
        throw new HiveException(msg, e);
      }
      LOG.debug("Acid move found " + origBucketStats.length + " original buckets");

      for (FileStatus origBucketStat : origBucketStats) {
        Path origBucketPath = origBucketStat.getPath();
        LOG.debug("Acid move looking for delta files in bucket " + origBucketPath);

        FileStatus[] deltaStats = null;
        try {
          deltaStats = fs.listStatus(origBucketPath, AcidUtils.deltaFileFilter);
        } catch (IOException e) {
          throw new HiveException("Unable to look for delta files in original bucket " +
              origBucketPath.toUri().toString(), e);
        }
        LOG.debug("Acid move found " + deltaStats.length + " delta files");

        for (FileStatus deltaStat : deltaStats) {
          Path deltaPath = deltaStat.getPath();
          // Create the delta directory.  Don't worry if it already exists,
          // as that likely means another task got to it first.  Then move each of the buckets.
          // it would be more efficient to try to move the delta with it's buckets but that is
          // harder to make race condition proof.
          Path deltaDest = new Path(dst, deltaPath.getName());
          try {
            if (!createdDeltaDirs.contains(deltaDest)) {
              try {
                fs.mkdirs(deltaDest);
                createdDeltaDirs.add(deltaDest);
              } catch (IOException swallowIt) {
                // Don't worry about this, as it likely just means it's already been created.
                LOG.info("Unable to create delta directory " + deltaDest +
                    ", assuming it already exists: " + swallowIt.getMessage());
              }
            }
            FileStatus[] bucketStats = fs.listStatus(deltaPath, AcidUtils.bucketFileFilter);
            LOG.debug("Acid move found " + bucketStats.length + " bucket files");
            for (FileStatus bucketStat : bucketStats) {
              Path bucketSrc = bucketStat.getPath();
              Path bucketDest = new Path(deltaDest, bucketSrc.getName());
              LOG.info("Moving bucket " + bucketSrc.toUri().toString() + " to " +
                  bucketDest.toUri().toString());
              fs.rename(bucketSrc, bucketDest);
              if (newFiles != null) newFiles.add(bucketDest);
            }
          } catch (IOException e) {
            throw new HiveException("Error moving acid files " + e.getMessage(), e);
          }
        }
      }
    }
  }


  /**
   * Replaces files in the partition with new data set specified by srcf. Works
   * by renaming directory of srcf to the destination file.
   * srcf, destf, and tmppath should resident in the same DFS, but the oldPath can be in a
   * different DFS.
   *
   * @param tablePath path of the table.  Used to identify permission inheritance.
   * @param srcf
   *          Source directory to be renamed to tmppath. It should be a
   *          leaf directory where the final data files reside. However it
   *          could potentially contain subdirectories as well.
   * @param destf
   *          The directory where the final data needs to go
   * @param oldPath
   *          The directory where the old data location, need to be cleaned up.  Most of time, will be the same
   *          as destf, unless its across FileSystem boundaries.
   * @param isSrcLocal
   *          If the source directory is LOCAL
   */
  protected static void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
          boolean isSrcLocal) throws HiveException {
    try {

      FileSystem destFs = destf.getFileSystem(conf);

      // check if srcf contains nested sub-directories
      FileStatus[] srcs;
      FileSystem srcFs;
      try {
        srcFs = srcf.getFileSystem(conf);
        srcs = srcFs.globStatus(srcf);
      } catch (IOException e) {
        throw new HiveException("Getting globStatus " + srcf.toString(), e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
      }
      List<List<Path[]>> result = checkPaths(conf, destFs, srcs, srcFs, destf, true);

      if (oldPath != null) {
        boolean oldPathDeleted = false;
        boolean isOldPathUnderDestf = false;
        try {
          FileSystem fs2 = oldPath.getFileSystem(conf);
          if (fs2.exists(oldPath)) {
            // Do not delete oldPath if:
            //  - destf is subdir of oldPath
            //if ( !(fs2.equals(destf.getFileSystem(conf)) && FileUtils.isSubDir(oldPath, destf, fs2)))
            isOldPathUnderDestf = FileUtils.isSubDir(oldPath, destf, fs2);
            if (isOldPathUnderDestf) {
              // if oldPath is destf or its subdir, its should definitely be deleted, otherwise its
              // existing content might result in incorrect (extra) data.
              // But not sure why we changed not to delete the oldPath in HIVE-8750 if it is
              // not the destf or its subdir?
              oldPathDeleted = FileUtils.trashFilesUnderDir(fs2, oldPath, conf);
            }
          }
        } catch (IOException e) {
          if (isOldPathUnderDestf) {
            // if oldPath is a subdir of destf but it could not be cleaned
            throw new HiveException("Directory " + oldPath.toString()
                + " could not be cleaned up.", e);
          } else {
            //swallow the exception since it won't affect the final result
            LOG.warn("Directory " + oldPath.toString() + " cannot be cleaned: " + e, e);
          }
        }
        if (isOldPathUnderDestf && !oldPathDeleted) {
          throw new HiveException("Destination directory " + destf + " has not be cleaned up.");
        }
      }

      // first call FileUtils.mkdir to make sure that destf directory exists, if not, it creates
      // destf with inherited permissions
      boolean destfExist = FileUtils.mkdir(destFs, destf, true, conf);
      if(!destfExist) {
        throw new IOException("Directory " + destf.toString()
            + " does not exist and could not be created.");
      }

      // Two cases:
      // 1. srcs has only a src directory, if rename src directory to destf, we also need to
      // Copy/move each file under the source directory to avoid to delete the destination
      // directory if it is the root of an HDFS encryption zone.
      // 2. srcs must be a list of files -- ensured by LoadSemanticAnalyzer
      // in both cases, we move the file under destf
      for (List<Path[]> sdpairs : result) {
        for (Path[] sdpair : sdpairs) {
          if (!moveFile(conf, sdpair[0], sdpair[1], true, isSrcLocal)) {
            throw new IOException("Error moving: " + sdpair[0] + " into: " + sdpair[1]);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public static boolean isHadoop1() {
    return ShimLoader.getMajorVersion().startsWith("0.20");
  }

  public List<Partition> exchangeTablePartitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destDb,
      String destinationTableName) throws HiveException {
    try {
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
        getMSC().exchange_partitions(partitionSpecs, sourceDb, sourceTable, destDb,
        destinationTableName);

      return convertFromMetastore(getTable(destDb, destinationTableName), partitions);
    } catch (Exception ex) {
      LOG.error(StringUtils.stringifyException(ex));
      throw new HiveException(ex);
    }
  }

  /**
   * Creates a metastore client. Currently it creates only JDBC based client as
   * File based store support is removed
   *
   * @returns a Meta Store Client
   * @throws HiveMetaException
   *           if a working client can't be created
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException {

    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
        @Override
        public HiveMetaHook getHook(
          org.apache.hadoop.hive.metastore.api.Table tbl)
          throws MetaException {

          try {
            if (tbl == null) {
              return null;
            }
            HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(conf,
                tbl.getParameters().get(META_TABLE_STORAGE));
            if (storageHandler == null) {
              return null;
            }
            return storageHandler.getMetaHook();
          } catch (HiveException ex) {
            LOG.error(StringUtils.stringifyException(ex));
            throw new MetaException(
              "Failed to load storage handler:  " + ex.getMessage());
          }
        }
      };

    if (conf.getBoolVar(ConfVars.METASTORE_FASTPATH)) {
      return new SessionHiveMetaStoreClient(conf, hookLoader);
    } else {
      return RetryingMetaStoreClient.getProxy(conf, hookLoader, metaCallTimeMap,
          SessionHiveMetaStoreClient.class.getName());
    }
  }

  /**
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  @LimitedPrivate(value = {"Hive"})
  @Unstable
  public synchronized IMetaStoreClient getMSC() throws MetaException {
    if (metaStoreClient == null) {
      try {
        owner = UserGroupInformation.getCurrentUser();
      } catch(IOException e) {
        String msg = "Error getting current user: " + e.getMessage();
        LOG.error(msg, e);
        throw new MetaException(msg + "\n" + StringUtils.stringifyException(e));
      }
      metaStoreClient = createMetaStoreClient();
      String metaStoreUris = conf.getVar(HiveConf.ConfVars.METASTOREURIS);
      if (!org.apache.commons.lang3.StringUtils.isEmpty(metaStoreUris)) {
        // get a synchronized wrapper if the meta store is remote.
        metaStoreClient = HiveMetaStoreClient.newSynchronizedClient(metaStoreClient);
      }
    }
    return metaStoreClient;
  }

  private String getUserName() {
    return SessionState.getUserFromAuthenticator();
  }

  private List<String> getGroupNames() {
    SessionState ss = SessionState.get();
    if (ss != null && ss.getAuthenticator() != null) {
      return ss.getAuthenticator().getGroupNames();
    }
    return null;
  }

  public static List<FieldSchema> getFieldsFromDeserializer(String name,
      Deserializer serde) throws HiveException {
    try {
      return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. "
          + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde."
          + e.getMessage(), e);
    }
  }

  public List<Index> getIndexes(String dbName, String tblName, short max) throws HiveException {
    List<Index> indexes = null;
    try {
      indexes = getMSC().listIndexes(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return indexes;
  }

  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updateTableColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updatePartitionColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws HiveException {
    try {
      return getMSC().setPartitionColumnStatistics(request);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(
      String dbName, String tableName, List<String> colNames) throws HiveException {
    try {
      return getMSC().getTableColumnStatistics(dbName, tableName, colNames);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNames, List<String> colNames) throws HiveException {
      try {
      return getMSC().getPartitionColumnStatistics(dbName, tableName, partNames, colNames);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public AggrStats getAggrColStatsFor(String dbName, String tblName,
    List<String> colNames, List<String> partName) {
    try {
      return getMSC().getAggrColStatsFor(dbName, tblName, colNames, partName);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      return new AggrStats(new ArrayList<ColumnStatisticsObj>(),0);
    }
  }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().deleteTableColumnStatistics(dbName, tableName, colName);
    } catch(Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws HiveException {
      try {
        return getMSC().deletePartitionColumnStatistics(dbName, tableName, partName, colName);
      } catch(Exception e) {
        LOG.debug(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
    }

  public Table newTable(String tableName) throws HiveException {
    String[] names = Utilities.getDbTableName(tableName);
    return new Table(names[0], names[1]);
  }

  public String getDelegationToken(String owner, String renewer)
    throws HiveException{
    try {
      return getMSC().getDelegationToken(owner, renewer);
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public void cancelDelegationToken(String tokenStrForm)
    throws HiveException {
    try {
      getMSC().cancelDelegationToken(tokenStrForm);
    }  catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  /**
   * Enqueue a compaction request.
   * @param dbname name of the database, if null default will be used.
   * @param tableName name of the table, cannot be null
   * @param partName name of the partition, if null table will be compacted (valid only for
   *                 non-partitioned tables).
   * @param compactType major or minor
   * @throws HiveException
   */
  public void compact(String dbname, String tableName, String partName,  String compactType)
      throws HiveException {
    try {
      CompactionType cr = null;
      if ("major".equals(compactType)) cr = CompactionType.MAJOR;
      else if ("minor".equals(compactType)) cr = CompactionType.MINOR;
      else throw new RuntimeException("Unknown compaction type " + compactType);
      getMSC().compact(dbname, tableName, partName, cr);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public ShowCompactResponse showCompactions() throws HiveException {
    try {
      return getMSC().showCompactions();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public GetOpenTxnsInfoResponse showTransactions() throws HiveException {
    try {
      return getMSC().showTxns();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public void createFunction(Function func) throws HiveException {
    try {
      getMSC().createFunction(func);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws HiveException {
    try {
      getMSC().alterFunction(dbName, funcName, newFunction);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public void dropFunction(String dbName, String funcName)
      throws HiveException {
    try {
      getMSC().dropFunction(dbName, funcName);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public Function getFunction(String dbName, String funcName) throws HiveException {
    try {
      return getMSC().getFunction(dbName, funcName);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public List<Function> getAllFunctions() throws HiveException {
    try {
      List<Function> functions = getMSC().getAllFunctions().getFunctions();
      return functions == null ? new ArrayList<Function>() : functions;
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public List<String> getFunctions(String dbName, String pattern) throws HiveException {
    try {
      return getMSC().getFunctions(dbName, pattern);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public void setMetaConf(String propName, String propValue) throws HiveException {
    try {
      getMSC().setMetaConf(propName, propValue);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public String getMetaConf(String propName) throws HiveException {
    try {
      return getMSC().getMetaConf(propName);
    } catch (TException te) {
      throw new HiveException(te);
    }
  }

  public void clearMetaCallTiming() {
    metaCallTimeMap.clear();
  }

  public void dumpAndClearMetaCallTiming(String phase) {
    boolean phaseInfoLogged = false;
    if (LOG.isDebugEnabled()) {
      phaseInfoLogged = logDumpPhase(phase);
      LOG.debug("Total time spent in each metastore function (ms): " + metaCallTimeMap);
    }

    if (LOG.isInfoEnabled()) {
      // print information about calls that took longer time at INFO level
      for (Entry<String, Long> callTime : metaCallTimeMap.entrySet()) {
        // dump information if call took more than 1 sec (1000ms)
        if (callTime.getValue() > 1000) {
          if (!phaseInfoLogged) {
            phaseInfoLogged = logDumpPhase(phase);
          }
          LOG.info("Total time spent in this metastore function was greater than 1000ms : "
              + callTime);
        }
      }
    }
    metaCallTimeMap.clear();
  }

  private boolean logDumpPhase(String phase) {
    LOG.info("Dumping metastore api call timing information for : " + phase + " phase");
    return true;
  }

  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(
      List<Long> fileIds, Configuration conf) throws HiveException {
    try {
      return getMSC().getFileMetadata(fileIds);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public void clearFileMetadata(List<Long> fileIds) throws HiveException {
    try {
      getMSC().clearFileMetadata(fileIds);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws HiveException {
    try {
      getMSC().putFileMetadata(fileIds, metadata);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }
};
