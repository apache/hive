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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.hive.conf.Constants.MATERIALIZED_VIEW_REWRITING_TIME_WINDOW;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer.makeBinaryPredicate;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.jdo.JDODataStoreException;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.*;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionTask;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.TableSnapshot;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveAugmentMaterializationRule;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DropTableDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.TxnIdUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private SynchronizedMetaStoreClient syncMetaStoreClient;
  private UserGroupInformation owner;

  // metastore calls timing information
  private final ConcurrentHashMap<String, Long> metaCallTimeMap = new ConcurrentHashMap<>();

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
  private void registerAllFunctionsOnce() throws HiveException {
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
      didRegisterAllFuncs.compareAndSet(REG_FUNCS_PENDING, REG_FUNCS_DONE);
    } catch (Exception e) {
      LOG.warn("Failed to register all functions.", e);
      didRegisterAllFuncs.compareAndSet(REG_FUNCS_PENDING, REG_FUNCS_NO);
      throw new HiveException(e);
    } finally {
      synchronized (didRegisterAllFuncs) {
        didRegisterAllFuncs.notifyAll();
      }
    }
  }


  public void reloadFunctions() throws HiveException {
    HashSet<String> registryFunctions = new HashSet<String>(
        FunctionRegistry.getFunctionNames(".+\\..+"));
    for (Function function : getAllFunctions()) {
      String functionName = function.getFunctionName();
      try {
        LOG.info("Registering function " + functionName + " " + function.getClassName());
        String qualFunc = FunctionUtils.qualifyFunctionName(functionName, function.getDbName());
        FunctionRegistry.registerPermanentFunction(qualFunc, function.getClassName(), false,
                    FunctionTask.toFunctionResource(function.getResourceUris()));
        registryFunctions.remove(qualFunc);
      } catch (Exception e) {
        LOG.warn("Failed to register persistent function " +
                functionName + ":" + function.getClassName() + ". Ignore and continue.");
      }
    }
    // unregister functions from local system registry that are not in getAllFunctions()
    for (String functionName : registryFunctions) {
      try {
        FunctionRegistry.unregisterPermanentFunction(functionName);
      } catch (Exception e) {
        LOG.warn("Failed to unregister persistent function " +
            functionName + "on reload. Ignore and continue.");
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
    return getInternal(c, false, false, true);
  }

  /**
   * Same as {@link #get(HiveConf)}, except that it checks only the object identity of existing
   * MS client, assuming the relevant settings would be unchanged within the same conf object.
   */
  public static Hive getWithFastCheck(HiveConf c) throws HiveException {
    return getWithFastCheck(c, true);
  }

  /**
   * Same as {@link #get(HiveConf)}, except that it checks only the object identity of existing
   * MS client, assuming the relevant settings would be unchanged within the same conf object.
   */
  public static Hive getWithFastCheck(HiveConf c, boolean doRegisterAllFns) throws HiveException {
    return getInternal(c, false, true, doRegisterAllFns);
  }

  private static Hive getInternal(HiveConf c, boolean needsRefresh, boolean isFastCheck,
      boolean doRegisterAllFns) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || !db.isCurrentUserOwner() || needsRefresh
        || (c != null && !isCompatible(db, c, isFastCheck))) {
      db = create(c, false, db, doRegisterAllFns);
    }
    if (c != null) {
      db.conf = c;
    }
    return db;
  }

  private static Hive create(HiveConf c, boolean needsRefresh, Hive db, boolean doRegisterAllFns)
      throws HiveException {
    if (db != null) {
      LOG.debug("Creating new db. db = " + db + ", needsRefresh = " + needsRefresh +
        ", db.isCurrentUserOwner = " + db.isCurrentUserOwner());
      db.close();
    }
    closeCurrent();
    if (c == null) {
      c = createHiveConf();
    }
    c.set("fs.scheme.class", "dfs");
    Hive newdb = new Hive(c, doRegisterAllFns);
    hiveDB.set(newdb);
    return newdb;
  }


  private static HiveConf createHiveConf() {
    SessionState session = SessionState.get();
    return (session == null) ? new HiveConf(Hive.class) : session.getConf();
  }

  private static boolean isCompatible(Hive db, HiveConf c, boolean isFastCheck) {
    if (isFastCheck) {
      return (db.metaStoreClient == null || db.metaStoreClient.isSameConfObj(c))
          && (db.syncMetaStoreClient == null || db.syncMetaStoreClient.isSameConfObj(c));
    } else {
      return (db.metaStoreClient == null || db.metaStoreClient.isCompatibleWith(c))
          && (db.syncMetaStoreClient == null || db.syncMetaStoreClient.isCompatibleWith(c));
    }
  }

  public static Hive get() throws HiveException {
    return get(true);
  }

  public static Hive get(boolean doRegisterAllFns) throws HiveException {
    return getInternal(null, false, false, doRegisterAllFns);
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
    return getInternal(c, needsRefresh, false, true);
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
  private Hive(HiveConf c, boolean doRegisterAllFns) throws HiveException {
    conf = c;
    if (doRegisterAllFns) {
      registerAllFunctionsOnce();
    }
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
    // syncMetaStoreClient is wrapped on metaStoreClient. So, it is enough to close it once.
    syncMetaStoreClient = null;
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


  public void alterTable(Table newTbl, boolean cascade, EnvironmentContext environmentContext,
      boolean transactional) throws HiveException {
    alterTable(newTbl.getCatName(), newTbl.getDbName(),
        newTbl.getTableName(), newTbl, cascade, environmentContext, transactional);
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param fullyQlfdTblName
   *          name of the existing table
   * @param newTbl
   *          new name of the table. could be the old name
   * @param transactional
   *          Need to generate and save a table snapshot into the metastore?
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterTable(String fullyQlfdTblName, Table newTbl, EnvironmentContext environmentContext,
                         boolean transactional)
      throws HiveException {
    String[] names = Utilities.getDbTableName(fullyQlfdTblName);
    alterTable(null, names[0], names[1], newTbl, false, environmentContext, transactional);
  }

  public void alterTable(String fullyQlfdTblName, Table newTbl, boolean cascade,
      EnvironmentContext environmentContext, boolean transactional)
      throws HiveException {
    String[] names = Utilities.getDbTableName(fullyQlfdTblName);
    alterTable(null, names[0], names[1], newTbl, cascade, environmentContext, transactional);
  }

  public void alterTable(String catName, String dbName, String tblName, Table newTbl, boolean cascade,
      EnvironmentContext environmentContext, boolean transactional)
      throws HiveException {

    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newTbl.getParameters() != null) {
        newTbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      newTbl.checkValidity(conf);
      if (environmentContext == null) {
        environmentContext = new EnvironmentContext();
      }
      if (cascade) {
        environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
      }

      // Take a table snapshot and set it to newTbl.
      AcidUtils.TableSnapshot tableSnapshot = null;
      if (transactional) {
        // Make sure we pass in the names, so we can get the correct snapshot for rename table.
        tableSnapshot = AcidUtils.getTableSnapshot(conf, newTbl, dbName, tblName, true);
        if (tableSnapshot != null) {
          newTbl.getTTable().setWriteId(tableSnapshot.getWriteId());
        } else {
          LOG.warn("Cannot get a table snapshot for " + tblName);
        }
      }

      // Why is alter_partitions synchronized while this isn't?
      getMSC().alter_table(
          catName, dbName, tblName, newTbl.getTTable(), environmentContext,
          tableSnapshot == null ? null : tableSnapshot.getValidWriteIdList());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table. " + e.getMessage(), e);
    }
  }

  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws HiveException {
    try {
      getMSC().updateCreationMetadata(dbName, tableName, cm);
    } catch (TException e) {
      throw new HiveException("Unable to update creation metadata " + e.getMessage(), e);
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
  public void alterPartition(String tblName, Partition newPart,
      EnvironmentContext environmentContext, boolean transactional)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    alterPartition(names[0], names[1], newPart, environmentContext, transactional);
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
   * @param environmentContext
   *          environment context for the method
   * @param transactional
   *          indicates this call is for transaction stats
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String dbName, String tblName, Partition newPart,
                             EnvironmentContext environmentContext, boolean transactional)
      throws InvalidOperationException, HiveException {
    try {
      validatePartition(newPart);
      String location = newPart.getLocation();
      if (location != null) {
        location = Utilities.getQualifiedPath(conf, new Path(location));
        newPart.setLocation(location);
      }
      if (environmentContext == null) {
        environmentContext = new EnvironmentContext();
      }
      AcidUtils.TableSnapshot tableSnapshot = null;
      if (transactional) {
        tableSnapshot = AcidUtils.getTableSnapshot(conf, newPart.getTable(), true);
        if (tableSnapshot != null) {
          newPart.getTPartition().setWriteId(tableSnapshot.getWriteId());
        } else {
          LOG.warn("Cannot get a table snapshot for " + tblName);
        }
      }
      getSynchronizedMSC().alter_partition(
          dbName, tblName, newPart.getTPartition(), environmentContext,
          tableSnapshot == null ? null : tableSnapshot.getValidWriteIdList());

    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition. " + e.getMessage(), e);
    }
  }

  private void validatePartition(Partition newPart) throws HiveException {
    // Remove the DDL time so that it gets refreshed
    if (newPart.getParameters() != null) {
      newPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
    }
    newPart.checkValidity();
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newParts
   *          new partitions
   * @param transactional
   *          Need to generate and save a table snapshot into the metastore?
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartitions(String tblName, List<Partition> newParts,
                              EnvironmentContext environmentContext, boolean transactional)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    List<org.apache.hadoop.hive.metastore.api.Partition> newTParts =
      new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    try {
      AcidUtils.TableSnapshot tableSnapshot = null;
      if (transactional) {
        tableSnapshot = AcidUtils.getTableSnapshot(conf, newParts.get(0).getTable(), true);
      }
      // Remove the DDL time so that it gets refreshed
      for (Partition tmpPart: newParts) {
        if (tmpPart.getParameters() != null) {
          tmpPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
        }
        String location = tmpPart.getLocation();
        if (location != null) {
          location = Utilities.getQualifiedPath(conf, new Path(location));
          tmpPart.setLocation(location);
        }
        newTParts.add(tmpPart.getTPartition());
      }
      getMSC().alter_partitions(names[0], names[1], newTParts, environmentContext,
          tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null,
          tableSnapshot != null ? tableSnapshot.getWriteId() : -1);
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
      String validWriteIds = null;
      if (AcidUtils.isTransactionalTable(tbl)) {
        // Set table snapshot to api.Table to make it persistent.
        TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, true);
        if (tableSnapshot != null) {
          newPart.getTPartition().setWriteId(tableSnapshot.getWriteId());
          validWriteIds = tableSnapshot.getValidWriteIdList();
        }
      }

      getMSC().renamePartition(tbl.getCatName(), tbl.getDbName(), tbl.getTableName(), pvals,
          newPart.getTPartition(), validWriteIds);

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
   * Creates the table with the given objects. It takes additional arguments for
   * primary keys and foreign keys associated with the table.
   *
   * @param tbl
   *          a table object
   * @param ifNotExists
   *          if true, ignore AlreadyExistsException
   * @param primaryKeys
   *          primary key columns associated with the table
   * @param foreignKeys
   *          foreign key columns associated with the table
   * @param uniqueConstraints
   *          UNIQUE constraints associated with the table
   * @param notNullConstraints
   *          NOT NULL constraints associated with the table
   * @param defaultConstraints
   *          DEFAULT constraints associated with the table
   * @param checkConstraints
   *          CHECK constraints associated with the table
   * @throws HiveException
   */
  public void createTable(Table tbl, boolean ifNotExists,
    List<SQLPrimaryKey> primaryKeys,
    List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints,
    List<SQLCheckConstraint> checkConstraints)
            throws HiveException {
    try {
      if (tbl.getDbName() == null || "".equals(tbl.getDbName().trim())) {
        tbl.setDbName(SessionState.get().getCurrentDatabase());
      }
      if (tbl.getCols().size() == 0 || tbl.getSd().getColsSize() == 0) {
        tbl.setFields(HiveMetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(),
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
      // Set table snapshot to api.Table to make it persistent.
      TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, true);
      if (tableSnapshot != null) {
        tbl.getTTable().setWriteId(tableSnapshot.getWriteId());
      }

      if (primaryKeys == null && foreignKeys == null
              && uniqueConstraints == null && notNullConstraints == null && defaultConstraints == null
          && checkConstraints == null) {
        getMSC().createTable(tTbl);
      } else {
        getMSC().createTableWithConstraints(tTbl, primaryKeys, foreignKeys,
            uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
      }

    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
   createTable(tbl, ifNotExists, null, null, null, null,
               null, null);
 }

  public static List<FieldSchema> getFieldsFromDeserializerForMsStorage(
      Table tbl, Deserializer deserializer) throws SerDeException, MetaException {
    List<FieldSchema> schema = HiveMetaStoreUtils.getFieldsFromDeserializer(
        tbl.getTableName(), deserializer);
    for (FieldSchema field : schema) {
      field.setType(MetaStoreUtils.TYPE_FROM_DESERIALIZER);
    }
    return schema;
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



  /**
   * Truncates the table/partition as per specifications. Just trash the data files
   *
   * @param dbDotTableName
   *          name of the table
   * @throws HiveException
   */
  public void truncateTable(String dbDotTableName, Map<String, String> partSpec) throws HiveException {
    try {
      Table table = getTable(dbDotTableName, true);
      // TODO: we should refactor code to make sure snapshot is always obtained in the same layer e.g. Hive.java
      AcidUtils.TableSnapshot snapshot = null;
      if (AcidUtils.isTransactionalTable(table)) {
        snapshot = AcidUtils.getTableSnapshot(conf, table, true);
      }

      List<String> partNames = ((null == partSpec)
        ? null : getPartitionNames(table.getDbName(), table.getTableName(), partSpec, (short) -1));
      if (snapshot == null) {
        getMSC().truncateTable(table.getDbName(), table.getTableName(), partNames);
      } else {
        getMSC().truncateTable(table.getDbName(), table.getTableName(), partNames,
            snapshot.getValidWriteIdList(), snapshot.getWriteId());
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
    return this.getTable(dbName, tableName, throwException, false);
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
   * @param checkTransactional
   *          checks whether the metadata table stats are valid (or
   *          compilant with the snapshot isolation of) for the current transaction.
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName,
      boolean throwException, boolean checkTransactional) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      // Note: this is currently called w/true from StatsOptimizer only.
      if (checkTransactional) {
        ValidWriteIdList validWriteIdList = null;
        long txnId = SessionState.get().getTxnMgr() != null ?
            SessionState.get().getTxnMgr().getCurrentTxnId() : 0;
        if (txnId > 0) {
          validWriteIdList = AcidUtils.getTableValidWriteIdListWithTxnList(conf,
              dbName, tableName);
        }
        tTable = getMSC().getTable(getDefaultCatalog(conf), dbName, tableName,
            validWriteIdList != null ? validWriteIdList.toString() : null);
      } else {
        tTable = getMSC().getTable(dbName, tableName);
      }
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error("Table " + dbName + "." + tableName + " not found: " + e.getMessage());
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
      String sf = parameters!=null?parameters.get(SERIALIZATION_FORMAT) : null;
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
    return getTablesByType(SessionState.get().getCurrentDatabase(), null, null);
  }

  /**
   * Get all table names for the specified database.
   * @param dbName
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables(String dbName) throws HiveException {
    return getTablesByType(dbName, ".*", null);
  }

  /**
   * Get all tables for the specified database.
   * @param dbName
   * @return List of all tables
   * @throws HiveException
   */
  public List<Table> getAllTableObjects(String dbName) throws HiveException {
    return getTableObjects(dbName, ".*", null);
  }

  /**
   * Get all materialized view names for the specified database.
   * @param dbName
   * @return List of materialized view table names
   * @throws HiveException
   */
  public List<String> getAllMaterializedViews(String dbName) throws HiveException {
    return getTablesByType(dbName, ".*", TableType.MATERIALIZED_VIEW);
  }

  /**
   * Get all materialized views for the specified database.
   * @param dbName
   * @return List of materialized view table objects
   * @throws HiveException
   */
  public List<Table> getAllMaterializedViewObjects(String dbName) throws HiveException {
    return getTableObjects(dbName, ".*", TableType.MATERIALIZED_VIEW);
  }

  private List<Table> getTableObjects(String dbName, String pattern, TableType tableType) throws HiveException {
    try {
      return Lists.transform(getMSC().getTableObjectsByName(dbName, getTablesByType(dbName, pattern, tableType)),
        new com.google.common.base.Function<org.apache.hadoop.hive.metastore.api.Table, Table>() {
          @Override
          public Table apply(org.apache.hadoop.hive.metastore.api.Table table) {
            return new Table(table);
          }
        }
      );
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private List<Table> getTableObjects(String dbName, List<String> tableNames) throws HiveException {
    try {
      return Lists.transform(getMSC().getTableObjectsByName(dbName, tableNames),
        new com.google.common.base.Function<org.apache.hadoop.hive.metastore.api.Table, Table>() {
          @Override
          public Table apply(org.apache.hadoop.hive.metastore.api.Table table) {
            return new Table(table);
          }
        }
      );
    } catch (Exception e) {
      throw new HiveException(e);
    }
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
    return getTablesByType(SessionState.get().getCurrentDatabase(),
        tablePattern, null);
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
    return getTablesByType(dbName, tablePattern, null);
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
    return getTablesByType(database, tablePattern, null);
  }

  /**
   * Returns all existing tables of a type (VIRTUAL_VIEW|EXTERNAL_TABLE|MANAGED_TABLE) from the specified
   * database which match the given pattern. The matching occurs as per Java regular expressions.
   * @param dbName Database name to find the tables in. if null, uses the current database in this session.
   * @param pattern A pattern to match for the table names.If null, returns all names from this DB.
   * @param type The type of tables to return. VIRTUAL_VIEWS for views. If null, returns all tables and views.
   * @return list of table names that match the pattern.
   * @throws HiveException
   */
  public List<String> getTablesByType(String dbName, String pattern, TableType type)
      throws HiveException {
    if (dbName == null) {
      dbName = SessionState.get().getCurrentDatabase();
    }

    try {
      if (type != null) {
        if (pattern != null) {
          return getMSC().getTables(dbName, pattern, type);
        } else {
          return getMSC().getTables(dbName, ".*", type);
        }
      } else {
        if (pattern != null) {
          return getMSC().getTables(dbName, pattern);
        } else {
          return getMSC().getTables(dbName, ".*");
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get the materialized views that have been enabled for rewriting from the
   * metastore. If the materialized view is in the cache, we do not need to
   * parse it to generate a logical plan for the rewriting. Instead, we
   * return the version present in the cache. Further, information provided
   * by the invalidation cache is useful to know whether a materialized view
   * can be used for rewriting or not.
   *
   * @return the list of materialized views available for rewriting
   * @throws HiveException
   */
  public List<RelOptMaterialization> getAllValidMaterializedViews(List<String> tablesUsed, boolean forceMVContentsUpToDate)
      throws HiveException {
    // Final result
    List<RelOptMaterialization> result = new ArrayList<>();
    try {
      for (String dbName : getMSC().getAllDatabases()) {
        // From metastore (for security)
        List<String> materializedViewNames = getMaterializedViewsForRewriting(dbName);
        if (materializedViewNames.isEmpty()) {
          // Bail out: empty list
          continue;
        }
        result.addAll(getValidMaterializedViews(dbName, materializedViewNames, tablesUsed, forceMVContentsUpToDate));
      }
      return result;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<RelOptMaterialization> getValidMaterializedView(String dbName, String materializedViewName,
      List<String> tablesUsed, boolean forceMVContentsUpToDate) throws HiveException {
    return getValidMaterializedViews(dbName, ImmutableList.of(materializedViewName), tablesUsed, forceMVContentsUpToDate);
  }

  private List<RelOptMaterialization> getValidMaterializedViews(String dbName, List<String> materializedViewNames,
      List<String> tablesUsed, boolean forceMVContentsUpToDate) throws HiveException {
    final String validTxnsList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    final ValidTxnWriteIdList currentTxnWriteIds =
        SessionState.get().getTxnMgr().getValidWriteIds(tablesUsed, validTxnsList);
    final boolean tryIncrementalRewriting =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_INCREMENTAL);
    final boolean tryIncrementalRebuild =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL);
    final long defaultTimeWindow =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW,
            TimeUnit.MILLISECONDS);
    try {
      // Final result
      List<RelOptMaterialization> result = new ArrayList<>();
      List<Table> materializedViewTables = getTableObjects(dbName, materializedViewNames);
      for (Table materializedViewTable : materializedViewTables) {
        final Boolean outdated = isOutdatedMaterializedView(materializedViewTable, currentTxnWriteIds,
            defaultTimeWindow, tablesUsed, forceMVContentsUpToDate);
        if (outdated == null) {
          continue;
        }

        final CreationMetadata creationMetadata = materializedViewTable.getCreationMetadata();
        if (outdated) {
          // The MV is outdated, see whether we should consider it for rewriting or not
          boolean ignore = false;
          if (forceMVContentsUpToDate && !tryIncrementalRebuild) {
            // We will not try partial rewriting for rebuild if incremental rebuild is disabled
            ignore = true;
          } else if (!forceMVContentsUpToDate && !tryIncrementalRewriting) {
            // We will not try partial rewriting for non-rebuild if incremental rewriting is disabled
            ignore = true;
          } else {
            // Obtain additional information if we should try incremental rewriting / rebuild
            // We will not try partial rewriting if there were update/delete operations on source tables
            Materialization invalidationInfo = getMSC().getMaterializationInvalidationInfo(
                creationMetadata, conf.get(ValidTxnList.VALID_TXNS_KEY));
            ignore = invalidationInfo == null || invalidationInfo.isSourceTablesUpdateDeleteModified();
          }
          if (ignore) {
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as its contents are outdated");
            continue;
          }
        }

        // It passed the test, load
        RelOptMaterialization materialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(
                dbName, materializedViewTable.getTableName());
        if (materialization != null) {
          RelNode viewScan = materialization.tableRel;
          RelOptHiveTable cachedMaterializedViewTable;
          if (viewScan instanceof Project) {
            // There is a Project on top (due to nullability)
            cachedMaterializedViewTable = (RelOptHiveTable) viewScan.getInput(0).getTable();
          } else {
            cachedMaterializedViewTable = (RelOptHiveTable) viewScan.getTable();
          }
          if (cachedMaterializedViewTable.getHiveTableMD().getCreateTime() ==
              materializedViewTable.getCreateTime()) {
            // It is in the cache and up to date
            if (outdated) {
              // We will rewrite it to include the filters on transaction list
              // so we can produce partial rewritings
              materialization = augmentMaterializationWithTimeInformation(
                  materialization, validTxnsList, new ValidTxnWriteIdList(
                      creationMetadata.getValidTxnList()));
            }
            result.add(materialization);
            continue;
          }
        }

        // It was not present in the cache (maybe because it was added by another HS2)
        // or it is not up to date.
        if (HiveMaterializedViewsRegistry.get().isInitialized()) {
          // But the registry was fully initialized, thus we need to add it
          if (LOG.isDebugEnabled()) {
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " was not in the cache");
          }
          materialization = HiveMaterializedViewsRegistry.get().createMaterializedView(
              conf, materializedViewTable);
          if (materialization != null) {
            if (outdated) {
              // We will rewrite it to include the filters on transaction list
              // so we can produce partial rewritings
              materialization = augmentMaterializationWithTimeInformation(
                  materialization, validTxnsList, new ValidTxnWriteIdList(
                      creationMetadata.getValidTxnList()));
            }
            result.add(materialization);
          }
        } else {
          // Otherwise the registry has not been initialized, skip for the time being
          if (LOG.isWarnEnabled()) {
            LOG.info("Materialized view " + materializedViewTable.getFullyQualifiedName() + " was skipped "
                + "because cache has not been loaded yet");
          }
        }
      }
      return result;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Utility method that returns whether a materialized view is outdated (true), not outdated
   * (false), or it cannot be determined (null). The latest case may happen e.g. when the
   * materialized view definition uses external tables.
   */
  public static Boolean isOutdatedMaterializedView(Table materializedViewTable, final ValidTxnWriteIdList currentTxnWriteIds,
      long defaultTimeWindow, List<String> tablesUsed, boolean forceMVContentsUpToDate) {
    // Check if materialization defined its own invalidation time window
    String timeWindowString = materializedViewTable.getProperty(MATERIALIZED_VIEW_REWRITING_TIME_WINDOW);
    long timeWindow = org.apache.commons.lang.StringUtils.isEmpty(timeWindowString) ? defaultTimeWindow :
        HiveConf.toTime(timeWindowString,
            HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW),
            TimeUnit.MILLISECONDS);
    CreationMetadata creationMetadata = materializedViewTable.getCreationMetadata();
    boolean outdated = false;
    if (timeWindow < 0L) {
      // We only consider the materialized view to be outdated if forceOutdated = true, i.e.,
      // if it is a rebuild. Otherwise, it passed the test and we use it as it is.
      outdated = forceMVContentsUpToDate;
    } else {
      // Check whether the materialized view is invalidated
      if (forceMVContentsUpToDate || timeWindow == 0L || creationMetadata.getMaterializationTime() < System.currentTimeMillis() - timeWindow) {
        if (currentTxnWriteIds == null) {
          LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
              " ignored for rewriting as we could not obtain current txn ids");
          return null;
        }
        if (creationMetadata.getValidTxnList() == null ||
            creationMetadata.getValidTxnList().isEmpty()) {
          LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
              " ignored for rewriting as we could not obtain materialization txn ids");
          return null;
        }
        boolean ignore = false;
        ValidTxnWriteIdList mvTxnWriteIds = new ValidTxnWriteIdList(
            creationMetadata.getValidTxnList());
        for (String qName : tablesUsed) {
          // Note. If the materialized view does not contain a table that is contained in the query,
          // we do not need to check whether that specific table is outdated or not. If a rewriting
          // is produced in those cases, it is because that additional table is joined with the
          // existing tables with an append-columns only join, i.e., PK-FK + not null.
          if (!creationMetadata.getTablesUsed().contains(qName)) {
            continue;
          }
          ValidWriteIdList tableCurrentWriteIds = currentTxnWriteIds.getTableValidWriteIdList(qName);
          if (tableCurrentWriteIds == null) {
            // Uses non-transactional table, cannot be considered
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as it is outdated and cannot be considered for " +
                " rewriting because it uses non-transactional table " + qName);
            ignore = true;
            break;
          }
          ValidWriteIdList tableWriteIds = mvTxnWriteIds.getTableValidWriteIdList(qName);
          if (tableWriteIds == null) {
            // This should not happen, but we ignore for safety
            LOG.warn("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as details about txn ids for table " + qName +
                " could not be found in " + mvTxnWriteIds);
            ignore = true;
            break;
          }
          if (!outdated && !TxnIdUtils.checkEquivalentWriteIds(tableCurrentWriteIds, tableWriteIds)) {
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " contents are outdated");
            outdated = true;
          }
        }
        if (ignore) {
          return null;
        }
      }
    }
    return outdated;
  }

  /**
   * Method to enrich the materialization query contained in the input with
   * its invalidation.
   */
  private static RelOptMaterialization augmentMaterializationWithTimeInformation(
      RelOptMaterialization materialization, String validTxnsList,
      ValidTxnWriteIdList materializationTxnList) throws LockException {
    // Extract tables used by the query which will in turn be used to generate
    // the corresponding txn write ids
    List<String> tablesUsed = new ArrayList<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          TableScan ts = (TableScan) node;
          tablesUsed.add(((RelOptHiveTable) ts.getTable()).getHiveTableMD().getFullyQualifiedName());
        }
        super.visit(node, ordinal, parent);
      }
    }.go(materialization.queryRel);
    ValidTxnWriteIdList currentTxnList =
        SessionState.get().getTxnMgr().getValidWriteIds(tablesUsed, validTxnsList);
    // Augment
    final RexBuilder rexBuilder = materialization.queryRel.getCluster().getRexBuilder();
    final HepProgramBuilder augmentMaterializationProgram = new HepProgramBuilder()
        .addRuleInstance(new HiveAugmentMaterializationRule(rexBuilder, currentTxnList, materializationTxnList));
    final HepPlanner augmentMaterializationPlanner = new HepPlanner(
        augmentMaterializationProgram.build());
    augmentMaterializationPlanner.setRoot(materialization.queryRel);
    final RelNode modifiedQueryRel = augmentMaterializationPlanner.findBestExp();
    return new RelOptMaterialization(materialization.tableRel, modifiedQueryRel,
        null, materialization.qualifiedTableName);
  }

  /**
   * Get materialized views for the specified database that have enabled rewriting.
   * @param dbName
   * @return List of materialized view table objects
   * @throws HiveException
   */
  private List<String> getMaterializedViewsForRewriting(String dbName) throws HiveException {
    try {
      return getMSC().getMaterializedViewsForRewriting(dbName);
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
   * Get the database by name.
   * @param catName catalog name
   * @param dbName the name of the database.
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabase(String catName, String dbName) throws HiveException {
    try {
      return getMSC().getDatabase(catName, dbName);
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
   * @param loadFileType
   *          if REPLACE_ALL - replace files in the table,
   *          otherwise add files to table (KEEP_EXISTING, OVERWRITE_EXISTING)
   * @param inheritTableSpecs if true, on [re]creating the partition, take the
   *          location/inputformat/outputformat/serde details from table spec
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param isAcidIUDoperation
   *          true if this is an ACID operation Insert/Update/Delete operation
   * @param hasFollowingStatsTask
   *          true if there is a following task which updates the stats, so, this method need not update.
   * @param writeId write ID allocated for the current load operation
   * @param stmtId statement ID of the current load statement
   * @param isInsertOverwrite
   * @return Partition object being loaded with data
   */
  public Partition loadPartition(Path loadPath, Table tbl, Map<String, String> partSpec,
      LoadFileType loadFileType, boolean inheritTableSpecs, boolean inheritLocation,
      boolean isSkewedStoreAsSubdir,
      boolean isSrcLocal, boolean isAcidIUDoperation, boolean hasFollowingStatsTask, Long writeId,
      int stmtId, boolean isInsertOverwrite) throws HiveException {
    Path tblDataLocationPath =  tbl.getDataLocation();
    boolean isMmTableWrite = AcidUtils.isInsertOnlyTable(tbl.getParameters());
    assert tbl.getPath() != null : "null==getPath() for " + tbl.getTableName();
    boolean isFullAcidTable = AcidUtils.isFullAcidTable(tbl);
    boolean isTxnTable = AcidUtils.isTransactionalTable(tbl);
    try {
      PerfLogger perfLogger = SessionState.getPerfLogger();
      perfLogger.PerfLogBegin("MoveTask", PerfLogger.LOAD_PARTITION);

      // Get the partition object if it already exists
      Partition oldPart = getPartition(tbl, partSpec, false);
      /**
       * Move files before creating the partition since down stream processes
       * check for existence of partition in metadata before accessing the data.
       * If partition is created before data is moved, downstream waiting
       * processes might move forward with partial data
       */

      Path oldPartPath = (oldPart != null) ? oldPart.getDataLocation() : null;
      Path newPartPath = null;

      if (inheritLocation) {
        newPartPath = genPartPathFromTable(tbl, partSpec, tblDataLocationPath);

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
        newPartPath = oldPartPath == null
          ? newPartPath = genPartPathFromTable(tbl, partSpec, tblDataLocationPath) : oldPartPath;
      }
      List<Path> newFiles = Collections.synchronizedList(new ArrayList<Path>());

      perfLogger.PerfLogBegin("MoveTask", PerfLogger.FILE_MOVES);

      // If config is set, table is not temporary and partition being inserted exists, capture
      // the list of files added. For not yet existing partitions (insert overwrite to new partition
      // or dynamic partition inserts), the add partition event will capture the list of files added.
      if (areEventsForDmlNeeded(tbl, oldPart)) {
        newFiles = Collections.synchronizedList(new ArrayList<Path>());
      }

      // Note: the stats for ACID tables do not have any coordination with either Hive ACID logic
      //       like txn commits, time outs, etc.; nor the lower level sync in metastore pertaining
      //       to ACID updates. So the are not themselves ACID.

      // Note: this assumes both paths are qualified; which they are, currently.
      if ((isMmTableWrite || isFullAcidTable) && loadPath.equals(newPartPath)) {
        // MM insert query, move itself is a no-op.
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("not moving " + loadPath + " to " + newPartPath + " (MM)");
        }
        assert !isAcidIUDoperation;
        if (newFiles != null) {
          listFilesCreatedByQuery(loadPath, writeId, stmtId, isMmTableWrite ? isInsertOverwrite : false, newFiles);
        }
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("maybe deleting stuff from " + oldPartPath
              + " (new " + newPartPath + ") for replace");
        }
      } else {
        // Either a non-MM query, or a load into MM table from an external source.
        Path destPath = newPartPath;
        if (isMmTableWrite) {
          assert !isAcidIUDoperation;
          // We will load into MM directory, and hide previous directories if needed.
          destPath = new Path(destPath, isInsertOverwrite
              ? AcidUtils.baseDir(writeId) : AcidUtils.deltaSubdir(writeId, writeId, stmtId));
        }
        if (!isAcidIUDoperation && isFullAcidTable) {
          destPath = fixFullAcidPathForLoadData(loadFileType, destPath, writeId, stmtId, tbl);
        }
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("moving " + loadPath + " to " + destPath);
        }

        boolean isManaged = tbl.getTableType() == TableType.MANAGED_TABLE;
        // TODO: why is "&& !isAcidIUDoperation" needed here?
        if (!isTxnTable && ((loadFileType == LoadFileType.REPLACE_ALL) || (oldPart == null && !isAcidIUDoperation))) {
          //for fullAcid tables we don't delete files for commands with OVERWRITE - we create a new
          // base_x.  (there is Insert Overwrite and Load Data Overwrite)
          boolean isAutoPurge = "true".equalsIgnoreCase(tbl.getProperty("auto.purge"));
          boolean needRecycle = !tbl.isTemporary()
                  && ReplChangeManager.isSourceOfReplication(Hive.get().getDatabase(tbl.getDbName()));
          replaceFiles(tbl.getPath(), loadPath, destPath, oldPartPath, getConf(), isSrcLocal,
              isAutoPurge, newFiles, FileUtils.HIDDEN_FILES_PATH_FILTER, needRecycle, isManaged);
        } else {
          FileSystem fs = tbl.getDataLocation().getFileSystem(conf);
          copyFiles(conf, loadPath, destPath, fs, isSrcLocal, isAcidIUDoperation,
              (loadFileType == LoadFileType.OVERWRITE_EXISTING), newFiles,
              tbl.getNumBuckets() > 0, isFullAcidTable, isManaged);
        }
      }
      perfLogger.PerfLogEnd("MoveTask", PerfLogger.FILE_MOVES);
      Partition newTPart = oldPart != null ? oldPart : new Partition(tbl, partSpec, newPartPath);
      alterPartitionSpecInMemory(tbl, partSpec, newTPart.getTPartition(), inheritTableSpecs, newPartPath.toString());
      validatePartition(newTPart);
      AcidUtils.TableSnapshot tableSnapshot = null;
      tableSnapshot = AcidUtils.getTableSnapshot(conf, newTPart.getTable(), true);
      if (tableSnapshot != null) {
        newTPart.getTPartition().setWriteId(tableSnapshot.getWriteId());
      }

      // If config is set, table is not temporary and partition being inserted exists, capture
      // the list of files added. For not yet existing partitions (insert overwrite to new partition
      // or dynamic partition inserts), the add partition event will capture the list of files added.
      // Generate an insert event only if inserting into an existing partition
      // When inserting into a new partition, the add partition event takes care of insert event
      if ((null != oldPart) && (null != newFiles)) {
        if (isTxnTable) {
          addWriteNotificationLog(tbl, partSpec, newFiles, writeId);
        } else {
          fireInsertEvent(tbl, partSpec, (loadFileType == LoadFileType.REPLACE_ALL), newFiles);
        }
      } else {
        LOG.debug("No new files were created, and is not a replace, or we're inserting into a "
                + "partition that does not exist yet. Skipping generating INSERT event.");
      }

      // column stats will be inaccurate
      if (!hasFollowingStatsTask) {
        StatsSetupConst.clearColumnStatsState(newTPart.getParameters());
      }

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
      }
      if (!this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        StatsSetupConst.setBasicStatsState(newTPart.getParameters(), StatsSetupConst.FALSE);
      }
      if (oldPart == null) {
        newTPart.getTPartition().setParameters(new HashMap<String,String>());
        if (this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          StatsSetupConst.setStatsStateForCreateTable(newTPart.getParameters(),
              MetaStoreUtils.getColumnNames(tbl.getCols()), StatsSetupConst.TRUE);
        }
        // Note: we are creating a brand new the partition, so this is going to be valid for ACID.
        List<FileStatus> filesForStats = null;
        if (isTxnTable) {
          filesForStats = AcidUtils.getAcidFilesForStats(
              newTPart.getTable(), newPartPath, conf, null);
        } else {
          filesForStats = HiveStatsUtils.getFileStatusRecurse(
              newPartPath, -1, newPartPath.getFileSystem(conf));
        }
        if (filesForStats != null) {
          MetaStoreUtils.populateQuickStats(filesForStats, newTPart.getParameters());
        } else {
          // The ACID state is probably absent. Warning is logged in the get method.
          MetaStoreUtils.clearQuickStats(newTPart.getParameters());
        }
        try {
          LOG.debug("Adding new partition " + newTPart.getSpec());
          getSynchronizedMSC().add_partition(newTPart.getTPartition());
        } catch (AlreadyExistsException aee) {
          // With multiple users concurrently issuing insert statements on the same partition has
          // a side effect that some queries may not see a partition at the time when they're issued,
          // but will realize the partition is actually there when it is trying to add such partition
          // to the metastore and thus get AlreadyExistsException, because some earlier query just created it (race condition).
          // For example, imagine such a table is created:
          //  create table T (name char(50)) partitioned by (ds string);
          // and the following two queries are launched at the same time, from different sessions:
          //  insert into table T partition (ds) values ('Bob', 'today'); -- creates the partition 'today'
          //  insert into table T partition (ds) values ('Joe', 'today'); -- will fail with AlreadyExistsException
          // In that case, we want to retry with alterPartition.
          LOG.debug("Caught AlreadyExistsException, trying to alter partition instead");
          setStatsPropAndAlterPartition(hasFollowingStatsTask, tbl, newTPart, tableSnapshot);
        } catch (Exception e) {
          try {
            final FileSystem newPathFileSystem = newPartPath.getFileSystem(this.getConf());
            boolean isAutoPurge = "true".equalsIgnoreCase(tbl.getProperty("auto.purge"));
            final FileStatus status = newPathFileSystem.getFileStatus(newPartPath);
            Hive.trashFiles(newPathFileSystem, new FileStatus[] {status}, this.getConf(), isAutoPurge);
          } catch (IOException io) {
            LOG.error("Could not delete partition directory contents after failed partition creation: ", io);
          }
          throw e;
        }

        // For acid table, add the acid_write event with file list at the time of load itself. But
        // it should be done after partition is created.
        if (isTxnTable && (null != newFiles)) {
          addWriteNotificationLog(tbl, partSpec, newFiles, writeId);
        }
      } else {
        setStatsPropAndAlterPartition(hasFollowingStatsTask, tbl, newTPart, tableSnapshot);
      }

      perfLogger.PerfLogEnd("MoveTask", PerfLogger.LOAD_PARTITION);
      return newTPart;
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (InvalidOperationException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (TException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }


  private static Path genPartPathFromTable(Table tbl, Map<String, String> partSpec,
      Path tblDataLocationPath) throws MetaException {
    Path partPath = new Path(tbl.getDataLocation(), Warehouse.makePartPath(partSpec));
    return new Path(tblDataLocationPath.toUri().getScheme(),
        tblDataLocationPath.toUri().getAuthority(), partPath.toUri().getPath());
  }

  /**
   * Load Data commands for fullAcid tables write to base_x (if there is overwrite clause) or
   * delta_x_x directory - same as any other Acid write.  This method modifies the destPath to add
   * this path component.
   * @param writeId - write id of the operated table from current transaction (in which this operation is running)
   * @param stmtId - see {@link DbTxnManager#getStmtIdAndIncrement()}
   * @return appropriately modified path
   */
  private Path fixFullAcidPathForLoadData(LoadFileType loadFileType, Path destPath, long writeId, int stmtId, Table tbl) throws HiveException {
    switch (loadFileType) {
      case REPLACE_ALL:
        destPath = new Path(destPath, AcidUtils.baseDir(writeId));
        break;
      case KEEP_EXISTING:
        destPath = new Path(destPath, AcidUtils.deltaSubdir(writeId, writeId, stmtId));
        break;
      case OVERWRITE_EXISTING:
        //should not happen here - this is for replication
      default:
        throw new IllegalArgumentException("Unexpected " + LoadFileType.class.getName() + " " + loadFileType);
    }
    try {
      FileSystem fs = tbl.getDataLocation().getFileSystem(SessionState.getSessionConf());
      if(!FileUtils.mkdir(fs, destPath, conf)) {
        LOG.warn(destPath + " already exists?!?!");
      }
    } catch (IOException e) {
      throw new HiveException("load: error while creating " + destPath + ";loadFileType=" + loadFileType, e);
    }
    return destPath;
  }

  private boolean areEventsForDmlNeeded(Table tbl, Partition oldPart) {
    // For Acid IUD, add partition is a meta data only operation. So need to add the new files added
    // information into the TXN_WRITE_NOTIFICATION_LOG table.
    return conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML) && !tbl.isTemporary() &&
            ((null != oldPart) || AcidUtils.isTransactionalTable(tbl));
  }

  private void listFilesInsideAcidDirectory(Path acidDir, FileSystem srcFs, List<Path> newFiles) throws IOException {
    // list out all the files/directory in the path
    FileStatus[] acidFiles;
    acidFiles = srcFs.listStatus(acidDir);
    if (acidFiles == null) {
      LOG.debug("No files added by this query in: " + acidDir);
      return;
    }
    for (FileStatus acidFile : acidFiles) {
      // need to list out only files, ignore folders.
      if (!acidFile.isDirectory()) {
        newFiles.add(acidFile.getPath());
      } else {
        listFilesInsideAcidDirectory(acidFile.getPath(), srcFs, newFiles);
      }
    }
  }

  private void listFilesCreatedByQuery(Path loadPath, long writeId, int stmtId,
                                             boolean isInsertOverwrite, List<Path> newFiles) throws HiveException {
    Path acidDir = new Path(loadPath, AcidUtils.baseOrDeltaSubdir(isInsertOverwrite, writeId, writeId, stmtId));
    try {
      FileSystem srcFs = loadPath.getFileSystem(conf);
      if (srcFs.exists(acidDir) && srcFs.isDirectory(acidDir)){
        // list out all the files in the path
        listFilesInsideAcidDirectory(acidDir, srcFs, newFiles);
      } else {
        LOG.info("directory does not exist: " + acidDir);
        return;
      }
    } catch (IOException e) {
      LOG.error("Error listing files", e);
      throw new HiveException(e);
    }
    return;
  }

  private void setStatsPropAndAlterPartition(boolean hasFollowingStatsTask, Table tbl,
      Partition newTPart, TableSnapshot tableSnapshot) throws MetaException, TException {
    EnvironmentContext ec = new EnvironmentContext();
    if (hasFollowingStatsTask) {
      ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }
    LOG.debug("Altering existing partition " + newTPart.getSpec());
    getSynchronizedMSC().alter_partition(
        tbl.getDbName(), tbl.getTableName(), newTPart.getTPartition(), new EnvironmentContext(),
        tableSnapshot == null ? null : tableSnapshot.getValidWriteIdList());
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
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Processing LB leaf " + fSta.getPath());
    }
    /* construct one location map if not exists. */
    constructOneLBLocationMap(fSta, skewedColValueLocationMaps, newPartPath, skewedInfo);
    return;
  }

  /* dfs. */
  FileStatus[] children = fSys.listStatus(fSta.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
  if (children != null) {
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Processing LB dir " + fSta.getPath());
    }
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
  String lbDirSuffix = lbDirName.replace(partDirName, ""); // TODO: should it rather do a prefix?
  if (lbDirSuffix.startsWith(Path.SEPARATOR)) {
    lbDirSuffix = lbDirSuffix.substring(1);
  }
  String[] dirNames = lbDirSuffix.split(Path.SEPARATOR);
  int keysFound = 0, dirsToTake = 0;
  int colCount = skewedInfo.getSkewedColNames().size();
  while (dirsToTake < dirNames.length && keysFound < colCount) {
    String dirName = dirNames[dirsToTake++];
    // Construct skewed-value to location map except default directory.
    // why? query logic knows default-dir structure and don't need to get from map
    if (dirName.equalsIgnoreCase(ListBucketingPrunerUtils.HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME)) {
      ++keysFound;
    } else {
      String[] kv = dirName.split("=");
      if (kv.length == 2) {
        skewedValue.add(kv[1]);
        ++keysFound;
      } else {
        // TODO: we should really probably throw. Keep the existing logic for now.
        LOG.warn("Skipping unknown directory " + dirName
            + " when expecting LB keys or default directory (from " + lbDirName + ")");
      }
    }
  }
  for (int i = 0; i < (dirNames.length - dirsToTake); ++i) {
    lbdPath = lbdPath.getParent();
  }
  if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
    Utilities.FILE_OP_LOGGER.trace("Saving LB location " + lbdPath + " based on "
      + colCount + " keys and " + fSta.getPath());
  }
  if ((skewedValue.size() > 0) && (skewedValue.size() == colCount)
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
    walkDirTree(fSys.getFileStatus(newPartPath),
        fSys, skewedColValueLocationMaps, newPartPath, skewedInfo);
    return skewedColValueLocationMaps;
  }

  /**
   * Get the valid partitions from the path
   * @param numDP number of dynamic partitions
   * @param loadPath
   * @return Set of valid partitions
   * @throws HiveException
   */
  private Set<Path> getValidPartitionsInPath(
      int numDP, int numLB, Path loadPath, Long writeId, int stmtId,
      boolean isMmTable, boolean isInsertOverwrite) throws HiveException {
    Set<Path> validPartitions = new HashSet<Path>();
    try {
      FileSystem fs = loadPath.getFileSystem(conf);
      if (!isMmTable) {
        List<FileStatus> leafStatus = HiveStatsUtils.getFileStatusRecurse(loadPath, numDP, fs);
        // Check for empty partitions
        for (FileStatus s : leafStatus) {
          if (!s.isDirectory()) {
            throw new HiveException("partition " + s.getPath() + " is not a directory!");
          }
          Path dpPath = s.getPath();
          validPartitions.add(dpPath);
        }
      } else {
        // The non-MM path only finds new partitions, as it is looking at the temp path.
        // To produce the same effect, we will find all the partitions affected by this txn ID.
        // Note: we ignore the statement ID here, because it's currently irrelevant for MoveTask
        //       where this is used; we always want to load everything; also the only case where
        //       we have multiple statements anyway is union.
        Utilities.FILE_OP_LOGGER.trace(
            "Looking for dynamic partitions in {} ({} levels)", loadPath, numDP);
        Path[] leafStatus = Utilities.getMmDirectoryCandidates(
            fs, loadPath, numDP, null, writeId, -1, conf, isInsertOverwrite);
        for (Path p : leafStatus) {
          Path dpPath = p.getParent(); // Skip the MM directory that we have found.
          if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
            Utilities.FILE_OP_LOGGER.trace("Found DP " + dpPath);
          }
          validPartitions.add(dpPath);
        }
      }
    } catch (IOException e) {
      throw new HiveException(e);
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
    return validPartitions;
  }

  /**
   * Given a source directory name of the load path, load all dynamically generated partitions
   * into the specified table and return a list of strings that represent the dynamic partition
   * paths.
   * @param loadPath
   * @param tableName
   * @param partSpec
   * @param loadFileType
   * @param numDP number of dynamic partitions
   * @param isAcid true if this is an ACID operation
   * @param writeId writeId, can be 0 unless isAcid == true
   * @return partition map details (PartitionSpec and Partition)
   * @throws HiveException
   */
  public Map<Map<String, String>, Partition> loadDynamicPartitions(final Path loadPath,
      final String tableName, final Map<String, String> partSpec, final LoadFileType loadFileType,
      final int numDP, final int numLB, final boolean isAcid, final long writeId, final int stmtId,
      final boolean hasFollowingStatsTask, final AcidUtils.Operation operation,
      boolean isInsertOverwrite) throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin("MoveTask", PerfLogger.LOAD_DYNAMIC_PARTITIONS);

    final Map<Map<String, String>, Partition> partitionsMap =
        Collections.synchronizedMap(new LinkedHashMap<Map<String, String>, Partition>());

    int poolSize = conf.getInt(ConfVars.HIVE_LOAD_DYNAMIC_PARTITIONS_THREAD_COUNT.varname, 1);
    final ExecutorService pool = Executors.newFixedThreadPool(poolSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("load-dynamic-partitions-%d")
                .build());

    // Get all valid partition paths and existing partitions for them (if any)
    final Table tbl = getTable(tableName);
    final Set<Path> validPartitions = getValidPartitionsInPath(numDP, numLB, loadPath, writeId, stmtId,
        AcidUtils.isInsertOnlyTable(tbl.getParameters()), isInsertOverwrite);

    final int partsToLoad = validPartitions.size();
    final AtomicInteger partitionsLoaded = new AtomicInteger(0);

    final boolean inPlaceEligible = conf.getLong("fs.trash.interval", 0) <= 0
        && InPlaceUpdate.canRenderInPlace(conf) && !SessionState.getConsole().getIsSilent();
    final PrintStream ps = (inPlaceEligible) ? SessionState.getConsole().getInfoStream() : null;
    final SessionState parentSession = SessionState.get();

    final List<Future<Void>> futures = Lists.newLinkedList();
    try {
      // for each dynamically created DP directory, construct a full partition spec
      // and load the partition based on that
      final Map<Long, RawStore> rawStoreMap = new ConcurrentHashMap<>();
      for(final Path partPath : validPartitions) {
        // generate a full partition specification
        final LinkedHashMap<String, String> fullPartSpec = Maps.newLinkedHashMap(partSpec);
        if (!Warehouse.makeSpecFromName(
            fullPartSpec, partPath, new HashSet<String>(partSpec.keySet()))) {
          Utilities.FILE_OP_LOGGER.warn("Ignoring invalid DP directory " + partPath);
          continue;
        }
        futures.add(pool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            try {
              // move file would require session details (needCopy() invokes SessionState.get)
              SessionState.setCurrentSessionState(parentSession);
              LOG.info("New loading path = " + partPath + " with partSpec " + fullPartSpec);

              // load the partition
              Partition newPartition = loadPartition(partPath, tbl, fullPartSpec, loadFileType,
                  true, false, numLB > 0, false, isAcid, hasFollowingStatsTask, writeId, stmtId,
                  isInsertOverwrite);
              partitionsMap.put(fullPartSpec, newPartition);

              if (inPlaceEligible) {
                synchronized (ps) {
                  InPlaceUpdate.rePositionCursor(ps);
                  partitionsLoaded.incrementAndGet();
                  InPlaceUpdate.reprintLine(ps, "Loaded : " + partitionsLoaded.get() + "/"
                      + partsToLoad + " partitions.");
                }
              }
              // Add embedded rawstore, so we can cleanup later to avoid memory leak
              if (getMSC().isLocalMetaStore()) {
                if (!rawStoreMap.containsKey(Thread.currentThread().getId())) {
                  rawStoreMap.put(Thread.currentThread().getId(), HiveMetaStore.HMSHandler.getRawStore());
                }
              }
              return null;
            } catch (Exception t) {
              LOG.error("Exception when loading partition with parameters "
                  + " partPath=" + partPath + ", "
                  + " table=" + tbl.getTableName() + ", "
                  + " partSpec=" + fullPartSpec + ", "
                  + " loadFileType=" + loadFileType.toString() + ", "
                  + " listBucketingLevel=" + numLB + ", "
                  + " isAcid=" + isAcid + ", "
                  + " hasFollowingStatsTask=" + hasFollowingStatsTask, t);
              throw t;
            }
          }
        }));
      }
      pool.shutdown();
      LOG.debug("Number of partitions to be added is " + futures.size());

      for (Future future : futures) {
        future.get();
      }

      rawStoreMap.forEach((k, rs) -> rs.shutdown());
    } catch (InterruptedException | ExecutionException e) {
      LOG.debug("Cancelling " + futures.size() + " dynamic loading tasks");
      //cancel other futures
      for (Future future : futures) {
        future.cancel(true);
      }
      throw new HiveException("Exception when loading "
          + partsToLoad + " in table " + tbl.getTableName()
          + " with loadPath=" + loadPath, e);
    }

    try {
      if (isAcid) {
        List<String> partNames = new ArrayList<>(partitionsMap.size());
        for (Partition p : partitionsMap.values()) {
          partNames.add(p.getName());
        }
        getMSC().addDynamicPartitions(parentSession.getTxnMgr().getCurrentTxnId(), writeId,
                tbl.getDbName(), tbl.getTableName(), partNames,
                AcidUtils.toDataOperationType(operation));
      }
      LOG.info("Loaded " + partitionsMap.size() + " partitions");

      perfLogger.PerfLogEnd("MoveTask", PerfLogger.LOAD_DYNAMIC_PARTITIONS);

      return partitionsMap;
    } catch (TException te) {
      throw new HiveException("Exception updating metastore for acid table "
          + tableName + " with partitions " + partitionsMap.values(), te);
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
   * @param loadFileType
   *          if REPLACE_ALL - replace files in the table,
   *          otherwise add files to table (KEEP_EXISTING, OVERWRITE_EXISTING)
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param isSkewedStoreAsSubdir
   *          if list bucketing enabled
   * @param hasFollowingStatsTask
   *          if there is any following stats task
   * @param isAcidIUDoperation true if this is an ACID based Insert [overwrite]/update/delete
   * @param writeId write ID allocated for the current load operation
   * @param stmtId statement ID of the current load statement
   */
  public void loadTable(Path loadPath, String tableName, LoadFileType loadFileType, boolean isSrcLocal,
      boolean isSkewedStoreAsSubdir, boolean isAcidIUDoperation, boolean hasFollowingStatsTask,
      Long writeId, int stmtId, boolean isInsertOverwrite) throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.PerfLogBegin("MoveTask", PerfLogger.LOAD_TABLE);

    List<Path> newFiles = null;
    Table tbl = getTable(tableName);
    assert tbl.getPath() != null : "null==getPath() for " + tbl.getTableName();
    boolean isTxnTable = AcidUtils.isTransactionalTable(tbl);
    boolean isMmTable = AcidUtils.isInsertOnlyTable(tbl);
    boolean isFullAcidTable = AcidUtils.isFullAcidTable(tbl);

    if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML) && !tbl.isTemporary()) {
      newFiles = Collections.synchronizedList(new ArrayList<Path>());
    }

    // Note: this assumes both paths are qualified; which they are, currently.
    if ((isMmTable || isFullAcidTable) && loadPath.equals(tbl.getPath())) {
      /**
       * some operations on Transactional tables (e.g. Import) write directly to the final location
       * and avoid the 'move' operation.  Since MoveTask does other things, setting 'loadPath' to be
       * the table/partition path indicates that the 'file move' part of MoveTask is not needed.
       */
      if (Utilities.FILE_OP_LOGGER.isDebugEnabled()) {
        Utilities.FILE_OP_LOGGER.debug(
            "not moving " + loadPath + " to " + tbl.getPath() + " (MM)");
      }

      //new files list is required only for event notification.
      if (newFiles != null) {
        listFilesCreatedByQuery(loadPath, writeId, stmtId, isMmTable ? isInsertOverwrite : false, newFiles);
      }
    } else {
      // Either a non-MM query, or a load into MM table from an external source.
      Path tblPath = tbl.getPath();
      Path destPath = tblPath;
      if (isMmTable) {
        assert !isAcidIUDoperation;
        // We will load into MM directory, and hide previous directories if needed.
        destPath = new Path(destPath, isInsertOverwrite
            ? AcidUtils.baseDir(writeId) : AcidUtils.deltaSubdir(writeId, writeId, stmtId));
      }
      if (!isAcidIUDoperation && isFullAcidTable) {
        destPath = fixFullAcidPathForLoadData(loadFileType, destPath, writeId, stmtId, tbl);
      }
      Utilities.FILE_OP_LOGGER.debug("moving " + loadPath + " to " + tblPath
          + " (replace = " + loadFileType + ")");

      perfLogger.PerfLogBegin("MoveTask", PerfLogger.FILE_MOVES);

      boolean isManaged = tbl.getTableType() == TableType.MANAGED_TABLE;

      if (loadFileType == LoadFileType.REPLACE_ALL && !isTxnTable) {
        //for fullAcid we don't want to delete any files even for OVERWRITE see HIVE-14988/HIVE-17361
        boolean isAutopurge = "true".equalsIgnoreCase(tbl.getProperty("auto.purge"));
        boolean needRecycle = !tbl.isTemporary()
                && ReplChangeManager.isSourceOfReplication(Hive.get().getDatabase(tbl.getDbName()));
        replaceFiles(tblPath, loadPath, destPath, tblPath, conf, isSrcLocal, isAutopurge,
            newFiles, FileUtils.HIDDEN_FILES_PATH_FILTER, needRecycle, isManaged);
      } else {
        try {
          FileSystem fs = tbl.getDataLocation().getFileSystem(conf);
          copyFiles(conf, loadPath, destPath, fs, isSrcLocal, isAcidIUDoperation,
              loadFileType == LoadFileType.OVERWRITE_EXISTING, newFiles,
              tbl.getNumBuckets() > 0, isFullAcidTable, isManaged);
        } catch (IOException e) {
          throw new HiveException("addFiles: filesystem error in check phase", e);
        }
      }
      perfLogger.PerfLogEnd("MoveTask", PerfLogger.FILE_MOVES);
    }
    if (!this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
    }

    //column stats will be inaccurate
    if (!hasFollowingStatsTask) {
      StatsSetupConst.clearColumnStatsState(tbl.getParameters());
    }

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

    EnvironmentContext environmentContext = null;
    if (hasFollowingStatsTask) {
      environmentContext = new EnvironmentContext();
      environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }

    alterTable(tbl, false, environmentContext, true);

    if (AcidUtils.isTransactionalTable(tbl)) {
      addWriteNotificationLog(tbl, null, newFiles, writeId);
    } else {
      fireInsertEvent(tbl, null, (loadFileType == LoadFileType.REPLACE_ALL), newFiles);
    }

    perfLogger.PerfLogEnd("MoveTask", PerfLogger.LOAD_TABLE);
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
  @VisibleForTesting
  public Partition createPartition(Table tbl, Map<String, String> partSpec) throws HiveException {
    try {
      org.apache.hadoop.hive.metastore.api.Partition part =
          Partition.createMetaPartitionObject(tbl, partSpec, null);
      AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
      part.setWriteId(tableSnapshot != null ? tableSnapshot.getWriteId() : 0);
      return new Partition(tbl, getMSC().add_partition(part));
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
    AcidUtils.TableSnapshot tableSnapshot =
        AcidUtils.getTableSnapshot(conf, tbl);
    for (int i = 0; i < size; ++i) {
      org.apache.hadoop.hive.metastore.api.Partition tmpPart =
          convertAddSpecToMetaPartition(tbl, addPartitionDesc.getPartition(i), conf);
      if (tmpPart != null && tableSnapshot != null && tableSnapshot.getWriteId() > 0) {
        tmpPart.setWriteId(tableSnapshot.getWriteId());
      }
      in.add(tmpPart);
    }
    List<Partition> out = new ArrayList<Partition>();
    try {
      if (!addPartitionDesc.getReplicationSpec().isInReplicationScope()){
        // TODO: normally, the result is not necessary; might make sense to pass false
        for (org.apache.hadoop.hive.metastore.api.Partition outPart
            : getMSC().add_partitions(in, addPartitionDesc.isIfNotExists(), true)) {
          out.add(new Partition(tbl, outPart));
        }
      } else {

        // For replication add-ptns, we need to follow a insert-if-not-exist, alter-if-exists scenario.
        // TODO : ideally, we should push this mechanism to the metastore, because, otherwise, we have
        // no choice but to iterate over the partitions here.

        List<org.apache.hadoop.hive.metastore.api.Partition> partsToAdd = new ArrayList<>();
        List<org.apache.hadoop.hive.metastore.api.Partition> partsToAlter = new ArrayList<>();
        List<String> part_names = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.Partition p: in){
          part_names.add(Warehouse.makePartName(tbl.getPartitionKeys(), p.getValues()));
          try {
            org.apache.hadoop.hive.metastore.api.Partition ptn =
                getMSC().getPartition(addPartitionDesc.getDbName(), addPartitionDesc.getTableName(), p.getValues());
            if (addPartitionDesc.getReplicationSpec().allowReplacementInto(ptn.getParameters())){
              partsToAlter.add(p);
            } // else ptn already exists, but we do nothing with it.
          } catch (NoSuchObjectException nsoe){
            // if the object does not exist, we want to add it.
            partsToAdd.add(p);
          }
        }
        for (org.apache.hadoop.hive.metastore.api.Partition outPart
            : getMSC().add_partitions(partsToAdd, addPartitionDesc.isIfNotExists(), true)) {
          out.add(new Partition(tbl, outPart));
        }
        getMSC().alter_partitions(addPartitionDesc.getDbName(), addPartitionDesc.getTableName(),
            partsToAlter, new EnvironmentContext(), null, -1);

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

  public static org.apache.hadoop.hive.metastore.api.Partition convertAddSpecToMetaPartition(
    Table tbl, AddPartitionDesc.OnePartitionDesc addSpec, final HiveConf conf) throws HiveException {
    Path location = addSpec.getLocation() != null
        ? new Path(tbl.getPath(), addSpec.getLocation()) : null;
    if (location != null) {
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
    return getPartition(tbl, partSpec, forceCreate, null, true);
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
  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate, String partPath, boolean inheritTableSpecs) throws HiveException {
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
      tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
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
            tpart = getSynchronizedMSC().appendPartition(tbl.getDbName(), tbl.getTableName(), pvals);
          } catch (AlreadyExistsException aee) {
            LOG.debug("Caught already exists exception, trying to alter partition instead");
            tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
              tbl.getTableName(), pvals, getUserName(), getGroupNames());
            alterPartitionSpec(tbl, partSpec, tpart, inheritTableSpecs, partPath);
          } catch (Exception e) {
            if (CheckJDOException.isJDODataStoreException(e)) {
              // Using utility method above, so that JDODataStoreException doesn't
              // have to be used here. This helps avoid adding jdo dependency for
              // hcatalog client uses
              LOG.debug("Caught JDO exception, trying to alter partition instead");
              tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
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
          fireInsertEvent(tbl, partSpec, true, null);
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

    alterPartitionSpecInMemory(tbl, partSpec, tpart, inheritTableSpecs, partPath);
    String fullName = tbl.getTableName();
    if (!org.apache.commons.lang.StringUtils.isEmpty(tbl.getDbName())) {
      fullName = tbl.getFullyQualifiedName();
    }
    alterPartition(fullName, new Partition(tbl, tpart), null, true);
  }

  private void alterPartitionSpecInMemory(Table tbl,
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
  }

  private void addWriteNotificationLog(Table tbl, Map<String, String> partitionSpec,
                                       List<Path> newFiles, Long writeId) throws HiveException {
    if (!conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML)) {
      LOG.debug("write notification log is ignored as dml event logging is disabled");
      return;
    }

    if (tbl.isTemporary()) {
      LOG.debug("write notification log is ignored as " + tbl.getTableName() + " is temporary : " + writeId);
      return;
    }

    if (newFiles == null || newFiles.isEmpty()) {
      LOG.debug("write notification log is ignored as file list is empty");
      return;
    }

    LOG.debug("adding write notification log for operation " + writeId + " table " + tbl.getCompleteName() +
                        "partition " + partitionSpec + " list of files " + newFiles);

    try {
      FileSystem fileSystem = tbl.getDataLocation().getFileSystem(conf);
      Long txnId = SessionState.get().getTxnMgr().getCurrentTxnId();

      InsertEventRequestData insertData = new InsertEventRequestData();
      insertData.setReplace(true);

      WriteNotificationLogRequest rqst = new WriteNotificationLogRequest(txnId, writeId,
              tbl.getDbName(), tbl.getTableName(), insertData);
      addInsertFileInformation(newFiles, fileSystem, insertData);

      if (partitionSpec != null && !partitionSpec.isEmpty()) {
        for (FieldSchema fs : tbl.getPartitionKeys()) {
          rqst.addToPartitionVals(partitionSpec.get(fs.getName()));
        }
      }
      getSynchronizedMSC().addWriteNotificationLog(rqst);
    } catch (IOException | TException e) {
      throw new HiveException(e);
    }
  }

  private void fireInsertEvent(Table tbl, Map<String, String> partitionSpec, boolean replace, List<Path> newFiles)
      throws HiveException {
    if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML)) {
      LOG.debug("Firing dml insert event");
      if (tbl.isTemporary()) {
        LOG.debug("Not firing dml insert event as " + tbl.getTableName() + " is temporary");
        return;
      }
      try {
        FileSystem fileSystem = tbl.getDataLocation().getFileSystem(conf);
        FireEventRequestData data = new FireEventRequestData();
        InsertEventRequestData insertData = new InsertEventRequestData();
        insertData.setReplace(replace);
        data.setInsertData(insertData);
        if (newFiles != null && !newFiles.isEmpty()) {
          addInsertFileInformation(newFiles, fileSystem, insertData);
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
        getSynchronizedMSC().fireListenerEvent(rqst);
      } catch (IOException | TException e) {
        throw new HiveException(e);
      }
    }
  }


  private static void addInsertFileInformation(List<Path> newFiles, FileSystem fileSystem,
      InsertEventRequestData insertData) throws IOException {
    LinkedList<Path> directories = null;
    for (Path p : newFiles) {
      if (fileSystem.isDirectory(p)) {
        if (directories == null) {
          directories = new LinkedList<>();
        }
        directories.add(p);
        continue;
      }
      addInsertNonDirectoryInformation(p, fileSystem, insertData);
    }
    if (directories == null) {
      return;
    }
    // We don't expect any nesting in most cases, or a lot of it if it is present; union and LB
    // are some examples where we would have 1, or few, levels respectively.
    while (!directories.isEmpty()) {
      Path dir = directories.poll();
      FileStatus[] contents = fileSystem.listStatus(dir);
      if (contents == null) {
        continue;
      }
      for (FileStatus status : contents) {
        if (status.isDirectory()) {
          directories.add(status.getPath());
          continue;
        }
        addInsertNonDirectoryInformation(status.getPath(), fileSystem, insertData);
      }
    }
  }


  private static void addInsertNonDirectoryInformation(Path p, FileSystem fileSystem,
      InsertEventRequestData insertData) throws IOException {
    insertData.addToFilesAdded(p.toString());
    FileChecksum cksum = fileSystem.getFileChecksum(p);
    String acidDirPath = AcidUtils.getFirstLevelAcidDirPath(p.getParent(), fileSystem);
    // File checksum is not implemented for local filesystem (RawLocalFileSystem)
    if (cksum != null) {
      String checksumString =
          StringUtils.byteToHexString(cksum.getBytes(), 0, cksum.getLength());
      insertData.addToFilesAddedChecksum(checksumString);
    } else {
      // Add an empty checksum string for filesystems that don't generate one
      insertData.addToFilesAddedChecksum("");
    }

    // acid dir will be present only for acid write operations.
    if (acidDirPath != null) {
      insertData.addToSubDirectoryList(acidDirPath);
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

  /**
   * drop the partitions specified as directory names associated with the table.
   *
   * @param table object for which partition is needed
   * @param partDirNames partition directories that need to be dropped
   * @param deleteData whether data should be deleted from file system
   * @param ifExists check for existence before attempting delete
   *
   * @return list of partition objects that were deleted
   *
   * @throws HiveException
   */
  public List<Partition> dropPartitions(Table table, List<String>partDirNames,
      boolean deleteData, boolean ifExists) throws HiveException {
    // partitions to be dropped in this batch
    List<DropTableDesc.PartSpec> partSpecs = new ArrayList<>(partDirNames.size());

    // parts of the partition
    String[] parts = null;

    // Expression splits of each part of the partition
    String[] partExprParts = null;

    // Column Types of all partitioned columns.  Used for generating partition specification
    Map<String, String> colTypes = new HashMap<String, String>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      colTypes.put(fs.getName(), fs.getType());
    }

    // Key to be used to save the partition to be dropped in partSpecs
    int partSpecKey = 0;

    for (String partDir : partDirNames) {
      // The expression to identify the partition to be dropped
      ExprNodeGenericFuncDesc expr = null;

      // Split by "/" to identify partition parts
      parts = partDir.split("/");

      // Loop through the partitions and form the expression
      for (String part : parts) {
        // Split the partition predicate to identify column and value
        partExprParts = part.split("=");

        // Only two elements expected in partExprParts partition column name and partition value
        assert partExprParts.length == 2;

        // Partition Column
        String partCol = partExprParts[0];

        // Column Type
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(colTypes.get(partCol));

        // Form the expression node corresponding to column
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, partCol, null, true);

        // Build the expression based on the partition predicate
        ExprNodeGenericFuncDesc op =
            makeBinaryPredicate("=", column, new ExprNodeConstantDesc(pti, partExprParts[1]));

        // the multiple parts to partition predicate are joined using and
        expr = (expr == null) ? op : makeBinaryPredicate("and", expr, op);
      }

      // Add the expression to partition specification
      partSpecs.add(new DropTableDesc.PartSpec(expr, partSpecKey));

      // Increment dropKey to get a new key for hash map
      ++partSpecKey;
    }

    String[] names = Utilities.getDbTableName(table.getFullyQualifiedName());
    return dropPartitions(names[0], names[1], partSpecs, deleteData, ifExists);
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
      List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs =
          new ArrayList<>(partSpecs.size());
      for (DropTableDesc.PartSpec partSpec : partSpecs) {
        partExprs.add(new org.apache.hadoop.hive.metastore.utils.ObjectPair<>(partSpec.getPrefixLength(),
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

  /**
   * Get a number of Partitions by filter.
   * @param tbl The table containing the partitions.
   * @param filter A string represent partition predicates.
   * @return the number of partitions satisfying the partition predicates.
   * @throws HiveException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public int getNumPartitionsByFilter(Table tbl, String filter)
    throws HiveException, MetaException, NoSuchObjectException, TException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
        "partitioned table");
    }

    int numParts = getMSC().getNumPartitionsByFilter(
      tbl.getDbName(), tbl.getTableName(), filter);

    return numParts;
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

  private static void copyFiles(final HiveConf conf, final FileSystem destFs,
            FileStatus[] srcs, final FileSystem srcFs, final Path destf,
            final boolean isSrcLocal, boolean isOverwrite,
            final List<Path> newFiles, boolean acidRename, boolean isManaged) throws HiveException {

    final HdfsUtils.HadoopFileStatus fullDestStatus;
    try {
      fullDestStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
    } catch (IOException e1) {
      throw new HiveException(e1);
    }

    if (!fullDestStatus.getFileStatus().isDirectory()) {
      throw new HiveException(destf + " is not a directory.");
    }
    final List<Future<ObjectPair<Path, Path>>> futures = new LinkedList<>();
    final ExecutorService pool = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
        Executors.newFixedThreadPool(conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
    // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
    // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
    // The extension is only maintained for files which are compressed.
    int taskId = 0;
    // Sort the files
    Arrays.sort(srcs);
    String configuredOwner = HiveConf.getVar(conf, ConfVars.HIVE_LOAD_DATA_OWNER);
    for (FileStatus src : srcs) {
      FileStatus[] files;
      if (src.isDirectory()) {
        try {
          files = srcFs.listStatus(src.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        } catch (IOException e) {
          if (null != pool) {
            pool.shutdownNow();
          }
          throw new HiveException(e);
        }
      } else {
        files = new FileStatus[] {src};
      }

      final SessionState parentSession = SessionState.get();
      // Sort the files
      Arrays.sort(files);
      for (final FileStatus srcFile : files) {
        final Path srcP = srcFile.getPath();
        final boolean needToCopy = needToCopy(srcP, destf, srcFs, destFs, configuredOwner, isManaged);

        final boolean isRenameAllowed = !needToCopy && !isSrcLocal;

        final String msg = "Unable to move source " + srcP + " to destination " + destf;

        // If we do a rename for a non-local file, we will be transfering the original
        // file permissions from source to the destination. Else, in case of mvFile() where we
        // copy from source to destination, we will inherit the destination's parent group ownership.
        if (null == pool) {
          try {
            Path destPath = mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite, isRenameAllowed,
                    acidRename ? taskId++ : -1);

            if (null != newFiles) {
              newFiles.add(destPath);
            }
          } catch (Exception e) {
            throw getHiveException(e, msg, "Failed to move: {}");
          }
        } else {
          // future only takes final or seemingly final values. Make a final copy of taskId
          final int finalTaskId = acidRename ? taskId++ : -1;
          futures.add(pool.submit(new Callable<ObjectPair<Path, Path>>() {
            @Override
            public ObjectPair<Path, Path> call() throws HiveException {
              SessionState.setCurrentSessionState(parentSession);

              try {
                Path destPath =
                    mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite, isRenameAllowed, finalTaskId);

                if (null != newFiles) {
                  newFiles.add(destPath);
                }
                return ObjectPair.create(srcP, destPath);
              } catch (Exception e) {
                throw getHiveException(e, msg);
              }
            }
          }));
        }
      }
    }
    if (null != pool) {
      pool.shutdown();
      for (Future<ObjectPair<Path, Path>> future : futures) {
        try {
          ObjectPair<Path, Path> pair = future.get();
          LOG.debug("Moved src: {}, to dest: {}", pair.getFirst().toString(), pair.getSecond().toString());
        } catch (Exception e) {
          throw handlePoolException(pool, e);
        }
      }
    }
  }

  private static boolean isSubDir(Path srcf, Path destf, FileSystem srcFs, FileSystem destFs, boolean isSrcLocal) {
    if (srcf == null) {
      LOG.debug("The source path is null for isSubDir method.");
      return false;
    }

    String fullF1 = getQualifiedPathWithoutSchemeAndAuthority(srcf, srcFs).toString() + Path.SEPARATOR;
    String fullF2 = getQualifiedPathWithoutSchemeAndAuthority(destf, destFs).toString() + Path.SEPARATOR;

    boolean isInTest = HiveConf.getBoolVar(srcFs.getConf(), ConfVars.HIVE_IN_TEST);
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

  private static Path getQualifiedPathWithoutSchemeAndAuthority(Path srcf, FileSystem fs) {
    Path currentWorkingDir = fs.getWorkingDirectory();
    Path path = srcf.makeQualified(srcf.toUri(), currentWorkingDir);
    return ShimLoader.getHadoopShims().getPathWithoutSchemeAndAuthority(path);
  }

  private static String getPathName(int taskId) {
    return Utilities.replaceTaskId("000000", taskId) + "_0";
  }

  /**
   * <p>
   *   Moves a file from one {@link Path} to another. If {@code isRenameAllowed} is true then the
   *   {@link FileSystem#rename(Path, Path)} method is used to move the file. If its false then the data is copied, if
   *   {@code isSrcLocal} is true then the {@link FileSystem#copyFromLocalFile(Path, Path)} method is used, else
   *   {@link FileUtils#copy(FileSystem, Path, FileSystem, Path, boolean, boolean, HiveConf)} is used.
   * </p>
   *
   * <p>
   *   If the destination file already exists, then {@code _copy_[counter]} is appended to the file name, where counter
   *   is an integer starting from 1.
   * </p>
   *
   * @param conf the {@link HiveConf} to use if copying data
   * @param sourceFs the {@link FileSystem} where the source file exists
   * @param sourcePath the {@link Path} to move
   * @param destFs the {@link FileSystem} to move the file to
   * @param destDirPath the {@link Path} to move the file to
   * @param isSrcLocal if the source file is on the local filesystem
   * @param isOverwrite if true, then overwrite destination file if exist else make a duplicate copy
   * @param isRenameAllowed true if the data should be renamed and not copied, false otherwise
   *
   * @return the {@link Path} the source file was moved to
   *
   * @throws IOException if there was an issue moving the file
   */
  private static Path mvFile(HiveConf conf, FileSystem sourceFs, Path sourcePath, FileSystem destFs, Path destDirPath,
                             boolean isSrcLocal, boolean isOverwrite, boolean isRenameAllowed,
                             int taskId) throws IOException {

    // Strip off the file type, if any so we don't make:
    // 000000_0.gz -> 000000_0.gz_copy_1
    final String fullname = sourcePath.getName();
    final String name;
    if (taskId == -1) { // non-acid
      name = FilenameUtils.getBaseName(sourcePath.getName());
    } else { // acid
      name = getPathName(taskId);
    }
    final String type = FilenameUtils.getExtension(sourcePath.getName());

    // Incase of ACID, the file is ORC so the extension is not relevant and should not be inherited.
    Path destFilePath = new Path(destDirPath, taskId == -1 ? fullname : name);

    /*
    * The below loop may perform bad when the destination file already exists and it has too many _copy_
    * files as well. A desired approach was to call listFiles() and get a complete list of files from
    * the destination, and check whether the file exists or not on that list. However, millions of files
    * could live on the destination directory, and on concurrent situations, this can cause OOM problems.
    *
    * I'll leave the below loop for now until a better approach is found.
    */
    for (int counter = 1; destFs.exists(destFilePath); counter++) {
      if (isOverwrite) {
        destFs.delete(destFilePath, false);
        break;
      }
      destFilePath =  new Path(destDirPath, name + (Utilities.COPY_KEYWORD + counter) +
              ((taskId == -1 && !type.isEmpty()) ? "." + type : ""));
    }

    if (isRenameAllowed) {
      destFs.rename(sourcePath, destFilePath);
    } else if (isSrcLocal) {
      destFs.copyFromLocalFile(sourcePath, destFilePath);
    } else {
      FileUtils.copy(sourceFs, sourcePath, destFs, destFilePath,
          true,   // delete source
          false,  // overwrite destination
          conf);
    }
    return destFilePath;
  }

  // Clears the dest dir when src is sub-dir of dest.
  public static void clearDestForSubDirSrc(final HiveConf conf, Path dest,
      Path src, boolean isSrcLocal) throws IOException {
    FileSystem destFS = dest.getFileSystem(conf);
    FileSystem srcFS = src.getFileSystem(conf);
    if (isSubDir(src, dest, srcFS, destFS, isSrcLocal)) {
      final Path fullSrcPath = getQualifiedPathWithoutSchemeAndAuthority(src, srcFS);
      final Path fullDestPath = getQualifiedPathWithoutSchemeAndAuthority(dest, destFS);
      if (fullSrcPath.equals(fullDestPath)) {
        return;
      }
      Path parent = fullSrcPath;
      while (!parent.getParent().equals(fullDestPath)) {
        parent = parent.getParent();
      }
      FileStatus[] existingFiles = destFS.listStatus(
          dest, FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus fileStatus : existingFiles) {
        if (!fileStatus.getPath().getName().equals(parent.getName())) {
          destFS.delete(fileStatus.getPath(), true);
        }
      }
    }
  }

  // List the new files in destination path which gets copied from source.
  public static void listNewFilesRecursively(final FileSystem destFs, Path dest,
                                             List<Path> newFiles) throws HiveException {
    try {
      for (FileStatus fileStatus : destFs.listStatus(dest, FileUtils.HIDDEN_FILES_PATH_FILTER)) {
        if (fileStatus.isDirectory()) {
          // If it is a sub-directory, then recursively list the files.
          listNewFilesRecursively(destFs, fileStatus.getPath(), newFiles);
        } else {
          newFiles.add(fileStatus.getPath());
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get source file statuses", e);
      throw new HiveException(e.getMessage(), e);
    }
  }

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param dataPath Path of the data files to be recycled to cmroot
   * @param isPurge
   *          When set to true files which needs to be recycled are not moved to Trash
   */
  public void recycleDirToCmPath(Path dataPath, boolean isPurge) throws HiveException {
    try {
      CmRecycleRequest request = new CmRecycleRequest(dataPath.toString(), isPurge);
      getMSC().recycleDirToCmPath(request);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  //it is assumed that parent directory of the destf should already exist when this
  //method is called. when the replace value is true, this method works a little different
  //from mv command if the destf is a directory, it replaces the destf instead of moving under
  //the destf. in this case, the replaced destf still preserves the original destf's permission
  public static boolean moveFile(final HiveConf conf, Path srcf, final Path destf, boolean replace,
                                 boolean isSrcLocal, boolean isManaged) throws HiveException {
    final FileSystem srcFs, destFs;
    try {
      destFs = destf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }
    try {
      srcFs = srcf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get src fs", e);
      throw new HiveException(e.getMessage(), e);
    }

    HdfsUtils.HadoopFileStatus destStatus = null;
    String configuredOwner = HiveConf.getVar(conf, ConfVars.HIVE_LOAD_DATA_OWNER);

    // If source path is a subdirectory of the destination path (or the other way around):
    //   ex: INSERT OVERWRITE DIRECTORY 'target/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;
    //   where the staging directory is a subdirectory of the destination directory
    // (1) Do not delete the dest dir before doing the move operation.
    // (2) It is assumed that subdir and dir are in same encryption zone.
    // (3) Move individual files from scr dir to dest dir.
    boolean srcIsSubDirOfDest = isSubDir(srcf, destf, srcFs, destFs, isSrcLocal),
        destIsSubDirOfSrc = isSubDir(destf, srcf, destFs, srcFs, false);
    final String msg = "Unable to move source " + srcf + " to destination " + destf;
    try {
      if (replace) {
        try{
          destStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
          //if destf is an existing directory:
          //if replace is true, delete followed by rename(mv) is equivalent to replace
          //if replace is false, rename (mv) actually move the src under dest dir
          //if destf is an existing file, rename is actually a replace, and do not need
          // to delete the file first
          if (replace && !srcIsSubDirOfDest) {
            destFs.delete(destf, true);
            LOG.debug("The path " + destf.toString() + " is deleted");
          }
        } catch (FileNotFoundException ignore) {
        }
      }
      final HdfsUtils.HadoopFileStatus desiredStatus = destStatus;
      final SessionState parentSession = SessionState.get();
      if (isSrcLocal) {
        // For local src file, copy to hdfs
        destFs.copyFromLocalFile(srcf, destf);
        return true;
      } else {
        if (needToCopy(srcf, destf, srcFs, destFs, configuredOwner, isManaged)) {
          //copy if across file system or encryption zones.
          LOG.debug("Copying source " + srcf + " to " + destf + " because HDFS encryption zones are different.");
          return FileUtils.copy(srcf.getFileSystem(conf), srcf, destf.getFileSystem(conf), destf,
              true,    // delete source
              replace, // overwrite destination
              conf);
        } else {
          if (srcIsSubDirOfDest || destIsSubDirOfSrc) {
            FileStatus[] srcs = destFs.listStatus(srcf, FileUtils.HIDDEN_FILES_PATH_FILTER);

            List<Future<Void>> futures = new LinkedList<>();
            final ExecutorService pool = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
                Executors.newFixedThreadPool(conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
            if (destIsSubDirOfSrc && !destFs.exists(destf)) {
              if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
                Utilities.FILE_OP_LOGGER.trace("Creating " + destf);
              }
              destFs.mkdirs(destf);
            }
            /* Move files one by one because source is a subdirectory of destination */
            for (final FileStatus srcStatus : srcs) {

              final Path destFile = new Path(destf, srcStatus.getPath().getName());

              final String poolMsg =
                  "Unable to move source " + srcStatus.getPath() + " to destination " + destFile;

              if (null == pool) {
                boolean success = false;
                if (destFs instanceof DistributedFileSystem) {
                  ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                  success = true;
                } else {
                  destFs.delete(destFile, false);
                  success = destFs.rename(srcStatus.getPath(), destFile);
                }
                if(!success) {
                  throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest:"
                      + destf + " returned false");
                }
              } else {
                futures.add(pool.submit(new Callable<Void>() {
                  @Override
                  public Void call() throws HiveException {
                    SessionState.setCurrentSessionState(parentSession);
                    try {
                      boolean success = false;
                      if (destFs instanceof DistributedFileSystem) {
                        ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                        success = true;
                      } else {
                        destFs.delete(destFile, false);
                        success = destFs.rename(srcStatus.getPath(), destFile);
                      }
                      if (!success) {
                        throw new IOException(
                            "rename for src path: " + srcStatus.getPath() + " to dest path:"
                                + destFile + " returned false");
                      }
                    } catch (Exception e) {
                      throw getHiveException(e, poolMsg);
                    }
                    return null;
                  }
                }));
              }
            }
            if (null != pool) {
              pool.shutdown();
              for (Future<Void> future : futures) {
                try {
                  future.get();
                } catch (Exception e) {
                  throw handlePoolException(pool, e);
                }
              }
            }
            return true;
          } else {
            if (destFs.rename(srcf, destf)) {
              return true;
            }
            return false;
          }
        }
      }
    } catch (Exception e) {
      throw getHiveException(e, msg);
    }
  }

  static private HiveException getHiveException(Exception e, String msg) {
    return getHiveException(e, msg, null);
  }

  static private HiveException handlePoolException(ExecutorService pool, Exception e) {
    HiveException he = null;

    if (e instanceof HiveException) {
      he = (HiveException) e;
      if (he.getCanonicalErrorMsg() != ErrorMsg.GENERIC_ERROR) {
        if (he.getCanonicalErrorMsg() == ErrorMsg.UNRESOLVED_RT_EXCEPTION) {
          LOG.error("Failed to move: {}", he.getMessage());
        } else {
          LOG.error("Failed to move: {}", he.getRemoteErrorMsg());
        }
      }
    } else {
      LOG.error("Failed to move: {}", e.getMessage());
      he = new HiveException(e.getCause());
    }
    pool.shutdownNow();
    return he;
  }

  static private HiveException getHiveException(Exception e, String msg, String logMsg) {
    // The message from remote exception includes the entire stack.  The error thrown from
    // hive based on the remote exception needs only the first line.
    String hiveErrMsg = null;

    if (e.getMessage() != null) {
      hiveErrMsg = String.format("%s%s%s", msg, ": ",
          Splitter.on(System.getProperty("line.separator")).split(e.getMessage()).iterator()
              .next());
    } else {
      hiveErrMsg = msg;
    }

    ErrorMsg errorMsg = ErrorMsg.getErrorMsg(e);

    if (logMsg != null) {
      LOG.info(String.format(logMsg, e.getMessage()));
    }

    if (errorMsg != ErrorMsg.UNRESOLVED_RT_EXCEPTION) {
      return new HiveException(e, e.getMessage(), errorMsg, hiveErrMsg);
    } else {
      return new HiveException(msg, e);
    }
  }

  /**
   * If moving across different FileSystems or differnent encryption zone, need to do a File copy instead of rename.
   * TODO- consider if need to do this for different file authority.
   * @throws HiveException
   */
  static private boolean needToCopy(Path srcf, Path destf, FileSystem srcFs,
                                      FileSystem destFs, String configuredOwner, boolean isManaged) throws HiveException {
    //Check if different FileSystems
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      return true;
    }

    if (isManaged && !configuredOwner.isEmpty() && srcFs instanceof DistributedFileSystem) {
      // Need some extra checks
      // Get the running owner
      FileStatus srcs;

      try {
        srcs = srcFs.getFileStatus(srcf);
        String runningUser = UserGroupInformation.getLoginUser().getShortUserName();
        boolean isOwned = FileUtils.isOwnerOfFileHierarchy(srcFs, srcs, configuredOwner, false);
        if (configuredOwner.equals(runningUser)) {
          // Check if owner has write permission, else it will have to copy
          if (!(isOwned &&
              FileUtils.isActionPermittedForFileHierarchy(
                  srcFs, srcs, configuredOwner, FsAction.WRITE, false))) {
            return true;
          }
        } else {
          // If the configured owner does not own the file, throw
          if (!isOwned) {
            throw new HiveException("Load Data failed for " + srcf + " as the file is not owned by "
            + configuredOwner + " and load data is also not ran as " + configuredOwner);
          } else {
            return true;
          }
        }
      } catch (IOException e) {
        throw new HiveException("Could not fetch FileStatus for source file");
      } catch (HiveException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(" Failed in looking up Permissions on file + " + srcf);
      }
    }

    //Check if different encryption zones
    HadoopShims.HdfsEncryptionShim srcHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(srcFs);
    HadoopShims.HdfsEncryptionShim destHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(destFs);
    try {
      return srcHdfsEncryptionShim != null
          && destHdfsEncryptionShim != null
          && (srcHdfsEncryptionShim.isPathEncrypted(srcf) || destHdfsEncryptionShim.isPathEncrypted(destf))
          && !srcHdfsEncryptionShim.arePathsOnSameEncryptionZone(srcf, destf, destHdfsEncryptionShim);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Copy files.  This handles building the mapping for buckets and such between the source and
   * destination
   * @param conf Configuration object
   * @param srcf source directory, if bucketed should contain bucket files
   * @param destf directory to move files into
   * @param fs Filesystem
   * @param isSrcLocal true if source is on local file system
   * @param isAcidIUD true if this is an ACID based Insert/Update/Delete
   * @param isOverwrite if true, then overwrite if destination file exist, else add a duplicate copy
   * @param newFiles if this is non-null, a list of files that were created as a result of this
   *                 move will be returned.
   * @param isManaged if table is managed.
   * @throws HiveException
   */
  static protected void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs,
                                  boolean isSrcLocal, boolean isAcidIUD,
                                  boolean isOverwrite, List<Path> newFiles, boolean isBucketed,
                                  boolean isFullAcidTable, boolean isManaged) throws HiveException {
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        FileUtils.mkdir(fs, destf, conf);
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
    if (isAcidIUD) {
      moveAcidFiles(srcFs, srcs, destf, newFiles);
    } else {
      // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
      // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
      // The extension is only maintained for files which are compressed.
      copyFiles(conf, fs, srcs, srcFs, destf, isSrcLocal, isOverwrite,
              newFiles, isFullAcidTable && !isBucketed, isManaged);
    }
  }

  public static void moveAcidFiles(FileSystem fs, FileStatus[] stats, Path dst,
                                    List<Path> newFiles) throws HiveException {
    // The layout for ACID files is table|partname/base|delta|delete_delta/bucket
    // We will always only be writing delta files ( except IOW which writes base_X/ ).
    // In the buckets created by FileSinkOperator
    // it will look like original_bucket/delta|delete_delta/bucket
    // (e.g. .../-ext-10004/000000_0/delta_0000014_0000014_0000/bucket_00000).  So we need to
    // move that into the above structure. For the first mover there will be no delta directory,
    // so we can move the whole directory.
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
        if(origBucketStats == null || origBucketStats.length == 0) {
          /**
           check if we are dealing with data with non-standard layout. For example a write
           produced by a (optimized) Union All query
           which looks like
          └── -ext-10000
            ├── HIVE_UNION_SUBDIR_1
            │   └── 000000_0
            │       └── delta_0000019_0000019_0001
            │           ├── _orc_acid_version
            │           └── bucket_00000
            ├── HIVE_UNION_SUBDIR_2
            │   └── 000000_0
            │       └── delta_0000019_0000019_0002
            │           ├── _orc_acid_version
            │           └── bucket_00000
           The assumption is that we either have all data in subdirs or root of srcPath
           but not both.
           For Union case, we expect delta dirs to have unique names which is assured by
           {@link org.apache.hadoop.hive.ql.optimizer.QueryPlanPostProcessor}
          */
          FileStatus[] unionSubdirs = fs.globStatus(new Path(srcPath,
            AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + "[0-9]*"));
          List<FileStatus> buckets = new ArrayList<>();
          for(FileStatus unionSubdir : unionSubdirs) {
            Collections.addAll(buckets,
              fs.listStatus(unionSubdir.getPath(), AcidUtils.originalBucketFilter));
          }
          origBucketStats = buckets.toArray(new FileStatus[buckets.size()]);
        }
      } catch (IOException e) {
        String msg = "Unable to look for bucket files in src path " + srcPath.toUri().toString();
        LOG.error(msg);
        throw new HiveException(msg, e);
      }
      LOG.debug("Acid move found " + origBucketStats.length + " original buckets");

      for (FileStatus origBucketStat : origBucketStats) {
        Path origBucketPath = origBucketStat.getPath();
        moveAcidFiles(AcidUtils.DELTA_PREFIX, AcidUtils.deltaFileFilter,
                fs, dst, origBucketPath, createdDeltaDirs, newFiles);
        moveAcidFiles(AcidUtils.DELETE_DELTA_PREFIX, AcidUtils.deleteEventDeltaDirFilter,
                fs, dst,origBucketPath, createdDeltaDirs, newFiles);
        moveAcidFiles(AcidUtils.BASE_PREFIX, AcidUtils.baseFileFilter,//for Insert Overwrite
                fs, dst, origBucketPath, createdDeltaDirs, newFiles);
      }
    }
  }

  private static void moveAcidFiles(String deltaFileType, PathFilter pathFilter, FileSystem fs,
                                    Path dst, Path origBucketPath, Set<Path> createdDeltaDirs,
                                    List<Path> newFiles) throws HiveException {
    LOG.debug("Acid move looking for " + deltaFileType + " files in bucket " + origBucketPath);

    FileStatus[] deltaStats = null;
    try {
      deltaStats = fs.listStatus(origBucketPath, pathFilter);
    } catch (IOException e) {
      throw new HiveException("Unable to look for " + deltaFileType + " files in original bucket " +
          origBucketPath.toUri().toString(), e);
    }
    LOG.debug("Acid move found " + deltaStats.length + " " + deltaFileType + " files");

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
            if(fs.mkdirs(deltaDest)) {
              fs.rename(AcidUtils.OrcAcidVersion.getVersionFilePath(deltaStat.getPath()),
                  AcidUtils.OrcAcidVersion.getVersionFilePath(deltaDest));
            }
            createdDeltaDirs.add(deltaDest);
          } catch (IOException swallowIt) {
            // Don't worry about this, as it likely just means it's already been created.
            LOG.info("Unable to create " + deltaFileType + " directory " + deltaDest +
                ", assuming it already exists: " + swallowIt.getMessage());
          }
        }
        FileStatus[] bucketStats = fs.listStatus(deltaPath, AcidUtils.bucketFileFilter);
        LOG.debug("Acid move found " + bucketStats.length + " bucket files");
        for (FileStatus bucketStat : bucketStats) {
          Path bucketSrc = bucketStat.getPath();
          Path bucketDest = new Path(deltaDest, bucketSrc.getName());
          final String msg = "Unable to move source " + bucketSrc + " to destination " +
              bucketDest;
          LOG.info("Moving bucket " + bucketSrc.toUri().toString() + " to " +
              bucketDest.toUri().toString());
          try {
            fs.rename(bucketSrc, bucketDest);
            if (newFiles != null) {
              newFiles.add(bucketDest);
            }
          } catch (Exception e) {
            throw getHiveException(e, msg);
          }
        }
      } catch (IOException e) {
        throw new HiveException("Error moving acid files " + e.getMessage(), e);
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
   * @param purge
   *          When set to true files which needs to be deleted are not moved to Trash
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param newFiles
   *          Output the list of new files replaced in the destination path
   * @param isManaged
   *          If the table is managed.
   */
  protected void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
          boolean isSrcLocal, boolean purge, List<Path> newFiles, PathFilter deletePathFilter,
          boolean isNeedRecycle, boolean isManaged) throws HiveException {
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

      if (oldPath != null) {
        deleteOldPathForReplace(destf, oldPath, conf, purge, deletePathFilter, isNeedRecycle);
      }

      // first call FileUtils.mkdir to make sure that destf directory exists, if not, it creates
      // destf
      boolean destfExist = FileUtils.mkdir(destFs, destf, conf);
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
      if (srcs.length == 1 && srcs[0].isDirectory()) {
        if (!moveFile(conf, srcs[0].getPath(), destf, true, isSrcLocal, isManaged)) {
          throw new IOException("Error moving: " + srcf + " into: " + destf);
        }

        // Add file paths of the files that will be moved to the destination if the caller needs it
        if (null != newFiles) {
          listNewFilesRecursively(destFs, destf, newFiles);
        }
      } else {
        // its either a file or glob
        for (FileStatus src : srcs) {
          Path destFile = new Path(destf, src.getPath().getName());
          if (!moveFile(conf, src.getPath(), destFile, true, isSrcLocal, isManaged)) {
            throw new IOException("Error moving: " + srcf + " into: " + destf);
          }

          // Add file paths of the files that will be moved to the destination if the caller needs it
          if (null != newFiles) {
            newFiles.add(destFile);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  private void deleteOldPathForReplace(Path destPath, Path oldPath, HiveConf conf, boolean purge,
      PathFilter pathFilter, boolean isNeedRecycle) throws HiveException {
    Utilities.FILE_OP_LOGGER.debug("Deleting old paths for replace in " + destPath
        + " and old path " + oldPath);
    boolean isOldPathUnderDestf = false;
    try {
      FileSystem oldFs = oldPath.getFileSystem(conf);
      FileSystem destFs = destPath.getFileSystem(conf);
      // if oldPath is destf or its subdir, its should definitely be deleted, otherwise its
      // existing content might result in incorrect (extra) data.
      // But not sure why we changed not to delete the oldPath in HIVE-8750 if it is
      // not the destf or its subdir?
      isOldPathUnderDestf = isSubDir(oldPath, destPath, oldFs, destFs, false);
      if (isOldPathUnderDestf) {
        cleanUpOneDirectoryForReplace(oldPath, oldFs, pathFilter, conf, purge, isNeedRecycle);
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
  }


  private void cleanUpOneDirectoryForReplace(Path path, FileSystem fs,
      PathFilter pathFilter, HiveConf conf, boolean purge, boolean isNeedRecycle) throws IOException, HiveException {
    if (isNeedRecycle && conf.getBoolVar(HiveConf.ConfVars.REPLCMENABLED)) {
      recycleDirToCmPath(path, purge);
    }
    FileStatus[] statuses = fs.listStatus(path, pathFilter);
    if (statuses == null || statuses.length == 0) {
      return;
    }
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      String s = "Deleting files under " + path + " for replace: ";
      for (FileStatus file : statuses) {
        s += file.getPath().getName() + ", ";
      }
      Utilities.FILE_OP_LOGGER.trace(s);
    }

    if (!trashFiles(fs, statuses, conf, purge)) {
      throw new HiveException("Old path " + path + " has not been cleaned up.");
    }
  }


  /**
   * Trashes or deletes all files under a directory. Leaves the directory as is.
   * @param fs FileSystem to use
   * @param statuses fileStatuses of files to be deleted
   * @param conf hive configuration
   * @return true if deletion successful
   * @throws IOException
   */
  public static boolean trashFiles(final FileSystem fs, final FileStatus[] statuses,
      final Configuration conf, final boolean purge)
      throws IOException {
    boolean result = true;

    if (statuses == null || statuses.length == 0) {
      return false;
    }
    final List<Future<Boolean>> futures = new LinkedList<>();
    final ExecutorService pool = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
        Executors.newFixedThreadPool(conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Delete-Thread-%d").build()) : null;
    final SessionState parentSession = SessionState.get();
    for (final FileStatus status : statuses) {
      if (null == pool) {
        result &= FileUtils.moveToTrash(fs, status.getPath(), conf, purge);
      } else {
        futures.add(pool.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            SessionState.setCurrentSessionState(parentSession);
            return FileUtils.moveToTrash(fs, status.getPath(), conf, purge);
          }
        }));
      }
    }
    if (null != pool) {
      pool.shutdown();
      for (Future<Boolean> future : futures) {
        try {
          result &= future.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("Failed to delete: ",e);
          pool.shutdownNow();
          throw new IOException(e);
        }
      }
    }
    return result;
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
  private IMetaStoreClient createMetaStoreClient(boolean allowEmbedded) throws MetaException {

    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(
              org.apache.hadoop.hive.metastore.api.Table tbl)
              throws MetaException {
        HiveStorageHandler storageHandler = createStorageHandler(tbl);
        return storageHandler == null ? null : storageHandler.getMetaHook();
      }
    };

    if (conf.getBoolVar(ConfVars.METASTORE_FASTPATH)) {
      return new SessionHiveMetaStoreClient(conf, hookLoader, allowEmbedded);
    } else {
      return RetryingMetaStoreClient.getProxy(conf, hookLoader, metaCallTimeMap,
          SessionHiveMetaStoreClient.class.getName(), allowEmbedded);
    }
  }

  @Nullable
  private HiveStorageHandler createStorageHandler(org.apache.hadoop.hive.metastore.api.Table tbl) throws MetaException {
    try {
      if (tbl == null) {
        return null;
      }
      HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(conf, tbl.getParameters().get(META_TABLE_STORAGE));
      return storageHandler;
    } catch (HiveException ex) {
      LOG.error(StringUtils.stringifyException(ex));
      throw new MetaException(
              "Failed to load storage handler:  " + ex.getMessage());
    }
  }

  public static class SchemaException extends MetaException {
    private static final long serialVersionUID = 1L;
    public SchemaException(String message) {
      super(message);
    }
  }

  /**
   * @return synchronized metastore client
   * @throws MetaException
   */
  @LimitedPrivate(value = {"Hive"})
  @Unstable
  public synchronized SynchronizedMetaStoreClient getSynchronizedMSC() throws MetaException {
    if (syncMetaStoreClient == null) {
      syncMetaStoreClient = new SynchronizedMetaStoreClient(getMSC(true, false));
    }
    return syncMetaStoreClient;
  }

  /**
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  @LimitedPrivate(value = {"Hive"})
  @Unstable
  public synchronized IMetaStoreClient getMSC() throws MetaException {
    return getMSC(true, false);
  }

  /**
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  @LimitedPrivate(value = {"Hive"})
  @Unstable
  public synchronized IMetaStoreClient getMSC(
      boolean allowEmbedded, boolean forceCreate) throws MetaException {
    if (metaStoreClient == null || forceCreate) {
      try {
        owner = UserGroupInformation.getCurrentUser();
      } catch(IOException e) {
        String msg = "Error getting current user: " + e.getMessage();
        LOG.error(msg, e);
        throw new MetaException(msg + "\n" + StringUtils.stringifyException(e));
      }
      try {
        metaStoreClient = createMetaStoreClient(allowEmbedded);
      } catch (RuntimeException ex) {
        Throwable t = ex.getCause();
        while (t != null) {
          if (t instanceof JDODataStoreException && t.getMessage() != null
              && t.getMessage().contains("autoCreate")) {
            LOG.error("Cannot initialize metastore due to autoCreate error", t);
            // DataNucleus wants us to auto-create, but we shall do no such thing.
            throw new SchemaException("Hive metastore database is not initialized. Please use "
              + "schematool (e.g. ./schematool -initSchema -dbType ...) to create the schema. If "
              + "needed, don't forget to include the option to auto-create the underlying database"
              + " in your JDBC connection string (e.g. ?createDatabaseIfNotExist=true for mysql)");
          }
          t = t.getCause();
        }
        throw ex;
      }
      String metaStoreUris = conf.getVar(HiveConf.ConfVars.METASTOREURIS);
      if (!org.apache.commons.lang3.StringUtils.isEmpty(metaStoreUris)) {
        // get a synchronized wrapper if the meta store is remote.
        metaStoreClient = HiveMetaStoreClient.newSynchronizedClient(metaStoreClient);
      }
    }
    return metaStoreClient;
  }

  private static String getUserName() {
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
      return HiveMetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. "
          + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde."
          + e.getMessage(), e);
    }
  }

  public boolean setPartitionColumnStatistics(
      SetPartitionsStatsRequest request) throws HiveException {
    try {
      ColumnStatistics colStat = request.getColStats().get(0);
      ColumnStatisticsDesc statsDesc = colStat.getStatsDesc();
      Table tbl = getTable(statsDesc.getDbName(), statsDesc.getTableName());

      AcidUtils.TableSnapshot tableSnapshot  = AcidUtils.getTableSnapshot(conf, tbl, true);
      request.setValidWriteIdList(tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null);
      request.setWriteId(tableSnapshot != null ? tableSnapshot.getWriteId() : 0);
      return getMSC().setPartitionColumnStatistics(request);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(
      String dbName, String tableName, List<String> colNames, boolean checkTransactional)
      throws HiveException {

    List<ColumnStatisticsObj> retv = null;
    try {
      if (checkTransactional) {
        Table tbl = getTable(dbName, tableName);
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
        retv = getMSC().getTableColumnStatistics(dbName, tableName, colNames,
            tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null);
      } else {
        retv = getMSC().getTableColumnStatistics(dbName, tableName, colNames);
      }
      return retv;
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName, String tableName, List<String> partNames, List<String> colNames,
      boolean checkTransactional)
      throws HiveException {
    String writeIdList = null;
    try {
      if (checkTransactional) {
        Table tbl = getTable(dbName, tableName);
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
        writeIdList = tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null;
      }

      return getMSC().getPartitionColumnStatistics(
          dbName, tableName, partNames, colNames, writeIdList);
    } catch (Exception e) {
      LOG.debug(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public AggrStats getAggrColStatsFor(String dbName, String tblName,
     List<String> colNames, List<String> partName, boolean checkTransactional) {
    String writeIdList = null;
    try {
      if (checkTransactional) {
        Table tbl = getTable(dbName, tblName);
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
        writeIdList = tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null;
      }
      return getMSC().getAggrColStatsFor(dbName, tblName, colNames, partName, writeIdList);
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
   * @deprecated use {@link #compact2(String, String, String, String, Map)}
   */
  @Deprecated
  public void compact(String dbname, String tableName, String partName, String compactType,
                      Map<String, String> tblproperties) throws HiveException {
    compact2(dbname, tableName, partName, compactType, tblproperties);
  }
  /**
   * Enqueue a compaction request.  Only 1 compaction for a given resource (db/table/partSpec) can
   * be scheduled/running at any given time.
   * @param dbname name of the database, if null default will be used.
   * @param tableName name of the table, cannot be null
   * @param partName name of the partition, if null table will be compacted (valid only for
   *                 non-partitioned tables).
   * @param compactType major or minor
   * @param tblproperties the list of tblproperties to overwrite for this compaction
   * @return id of new request or id already existing request for specified resource
   * @throws HiveException
   */
  public CompactionResponse compact2(String dbname, String tableName, String partName, String compactType,
                                     Map<String, String> tblproperties)
      throws HiveException {
    try {
      CompactionType cr = null;
      if ("major".equalsIgnoreCase(compactType)) {
        cr = CompactionType.MAJOR;
      } else if ("minor".equalsIgnoreCase(compactType)) {
        cr = CompactionType.MINOR;
      } else {
        throw new RuntimeException("Unknown compaction type " + compactType);
      }
      return getMSC().compact2(dbname, tableName, partName, cr, tblproperties);
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

  public void abortTransactions(List<Long> txnids) throws HiveException {
    try {
      getMSC().abortTxns(txnids);
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

  public ImmutableMap<String, Long> dumpAndClearMetaCallTiming(String phase) {
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

    ImmutableMap<String, Long> result = ImmutableMap.copyOf(metaCallTimeMap);
    metaCallTimeMap.clear();
    return result;
  }

  private boolean logDumpPhase(String phase) {
    LOG.info("Dumping metastore api call timing information for : " + phase + " phase");
    return true;
  }

  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(
      List<Long> fileIds) throws HiveException {
    try {
      return getMSC().getFileMetadata(fileIds);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataByExpr(
      List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws HiveException {
    try {
      return getMSC().getFileMetadataBySarg(fileIds, sarg, doGetFooters);
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

  public void cacheFileMetadata(
      String dbName, String tableName, String partName, boolean allParts) throws HiveException {
    try {
      boolean willCache = getMSC().cacheFileMetadata(dbName, tableName, partName, allParts);
      if (!willCache) {
        throw new HiveException(
            "Caching file metadata is not supported by metastore or for this file format");
      }
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public void dropConstraint(String dbName, String tableName, String constraintName)
    throws HiveException, NoSuchObjectException {
    try {
      getMSC().dropConstraint(dbName, tableName, constraintName);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLPrimaryKey> getPrimaryKeyList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getPrimaryKeys(new PrimaryKeysRequest(dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLForeignKey> getForeignKeyList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getForeignKeys(new ForeignKeysRequest(null, null, dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLUniqueConstraint> getUniqueConstraintList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getUniqueConstraints(new UniqueConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLNotNullConstraint> getNotNullConstraintList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getNotNullConstraints(new NotNullConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLDefaultConstraint> getDefaultConstraintList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getDefaultConstraints(new DefaultConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SQLCheckConstraint> getCheckConstraintList(String dbName, String tblName) throws HiveException, NoSuchObjectException {
    try {
      return getMSC().getCheckConstraints(new CheckConstraintsRequest(getDefaultCatalog(conf),
          dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all primary key columns associated with the table.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Primary Key associated with the table.
   * @throws HiveException
   */
  public PrimaryKeyInfo getPrimaryKeys(String dbName, String tblName) throws HiveException {
    return getPrimaryKeys(dbName, tblName, false);
  }

  /**
   * Get primary key columns associated with the table that are available for optimization.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Primary Key associated with the table.
   * @throws HiveException
   */
  public PrimaryKeyInfo getReliablePrimaryKeys(String dbName, String tblName) throws HiveException {
    return getPrimaryKeys(dbName, tblName, true);
  }

  private PrimaryKeyInfo getPrimaryKeys(String dbName, String tblName, boolean onlyReliable)
      throws HiveException {
    try {
      List<SQLPrimaryKey> primaryKeys = getMSC().getPrimaryKeys(new PrimaryKeysRequest(dbName, tblName));
      if (onlyReliable && primaryKeys != null && !primaryKeys.isEmpty()) {
        primaryKeys = primaryKeys.stream()
          .filter(pk -> pk.isRely_cstr())
          .collect(Collectors.toList());
      }
      return new PrimaryKeyInfo(primaryKeys, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all foreign keys associated with the table.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Foreign keys associated with the table.
   * @throws HiveException
   */
  public ForeignKeyInfo getForeignKeys(String dbName, String tblName) throws HiveException {
    return getForeignKeys(dbName, tblName, false);
  }

  /**
   * Get foreign keys associated with the table that are available for optimization.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Foreign keys associated with the table.
   * @throws HiveException
   */
  public ForeignKeyInfo getReliableForeignKeys(String dbName, String tblName) throws HiveException {
    return getForeignKeys(dbName, tblName, true);
  }

  private ForeignKeyInfo getForeignKeys(String dbName, String tblName, boolean onlyReliable)
      throws HiveException {
    try {
      List<SQLForeignKey> foreignKeys = getMSC().getForeignKeys(new ForeignKeysRequest(null, null, dbName, tblName));
      if (onlyReliable && foreignKeys != null && !foreignKeys.isEmpty()) {
        foreignKeys = foreignKeys.stream()
          .filter(fk -> fk.isRely_cstr())
          .collect(Collectors.toList());
      }
      return new ForeignKeyInfo(foreignKeys, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all unique constraints associated with the table.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Unique constraints associated with the table.
   * @throws HiveException
   */
  public UniqueConstraint getUniqueConstraints(String dbName, String tblName) throws HiveException {
    return getUniqueConstraints(dbName, tblName, false);
  }

  /**
   * Get unique constraints associated with the table that are available for optimization.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Unique constraints associated with the table.
   * @throws HiveException
   */
  public UniqueConstraint getReliableUniqueConstraints(String dbName, String tblName) throws HiveException {
    return getUniqueConstraints(dbName, tblName, true);
  }

  private UniqueConstraint getUniqueConstraints(String dbName, String tblName, boolean onlyReliable)
      throws HiveException {
    try {
      List<SQLUniqueConstraint> uniqueConstraints = getMSC().getUniqueConstraints(
              new UniqueConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (onlyReliable && uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
        uniqueConstraints = uniqueConstraints.stream()
          .filter(uk -> uk.isRely_cstr())
          .collect(Collectors.toList());
      }
      return new UniqueConstraint(uniqueConstraints, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all not null constraints associated with the table.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Not null constraints associated with the table.
   * @throws HiveException
   */
  public NotNullConstraint getNotNullConstraints(String dbName, String tblName) throws HiveException {
    return getNotNullConstraints(dbName, tblName, false);
  }

  /**
   * Get not null constraints associated with the table that are available for optimization.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Not null constraints associated with the table.
   * @throws HiveException
   */
  public NotNullConstraint getReliableNotNullConstraints(String dbName, String tblName) throws HiveException {
    return getNotNullConstraints(dbName, tblName, true);
  }

  /**
   * Get not null constraints associated with the table that are enabled/enforced.
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Not null constraints associated with the table.
   * @throws HiveException
   */
  public NotNullConstraint getEnabledNotNullConstraints(String dbName, String tblName)
      throws HiveException {
    try {
      List<SQLNotNullConstraint> notNullConstraints = getMSC().getNotNullConstraints(
              new NotNullConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (notNullConstraints != null && !notNullConstraints.isEmpty()) {
        notNullConstraints = notNullConstraints.stream()
          .filter(nnc -> nnc.isEnable_cstr())
          .collect(Collectors.toList());
      }
      return new NotNullConstraint(notNullConstraints, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get CHECK constraints associated with the table that are enabled
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return CHECK constraints associated with the table.
   * @throws HiveException
   */
  public CheckConstraint getEnabledCheckConstraints(String dbName, String tblName)
      throws HiveException {
    try {
      List<SQLCheckConstraint> checkConstraints = getMSC().getCheckConstraints(
          new CheckConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (checkConstraints != null && !checkConstraints.isEmpty()) {
        checkConstraints = checkConstraints.stream()
            .filter(nnc -> nnc.isEnable_cstr())
            .collect(Collectors.toList());
      }
      return new CheckConstraint(checkConstraints);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
  /**
   * Get Default constraints associated with the table that are enabled
   *
   * @param dbName Database Name
   * @param tblName Table Name
   * @return Default constraints associated with the table.
   * @throws HiveException
   */
  public DefaultConstraint getEnabledDefaultConstraints(String dbName, String tblName)
      throws HiveException {
    try {
      List<SQLDefaultConstraint> defaultConstraints = getMSC().getDefaultConstraints(
          new DefaultConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (defaultConstraints != null && !defaultConstraints.isEmpty()) {
        defaultConstraints = defaultConstraints.stream()
            .filter(nnc -> nnc.isEnable_cstr())
            .collect(Collectors.toList());
      }
      return new DefaultConstraint(defaultConstraints, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private NotNullConstraint getNotNullConstraints(String dbName, String tblName, boolean onlyReliable)
      throws HiveException {
    try {
      List<SQLNotNullConstraint> notNullConstraints = getMSC().getNotNullConstraints(
              new NotNullConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (onlyReliable && notNullConstraints != null && !notNullConstraints.isEmpty()) {
        notNullConstraints = notNullConstraints.stream()
          .filter(nnc -> nnc.isRely_cstr())
          .collect(Collectors.toList());
      }
      return new NotNullConstraint(notNullConstraints, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public DefaultConstraint getDefaultConstraints(String dbName, String tblName)
      throws HiveException {
    try {
      List<SQLDefaultConstraint> defaultConstraints = getMSC().getDefaultConstraints(
          new DefaultConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (defaultConstraints != null && !defaultConstraints.isEmpty()) {
        defaultConstraints = defaultConstraints.stream()
            .collect(Collectors.toList());
      }
      return new DefaultConstraint(defaultConstraints, tblName, dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public CheckConstraint getCheckConstraints(String dbName, String tblName)
      throws HiveException {
    try {
      List<SQLCheckConstraint> checkConstraints = getMSC().getCheckConstraints(
          new CheckConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
      if (checkConstraints != null && !checkConstraints.isEmpty()) {
        checkConstraints = checkConstraints.stream()
            .collect(Collectors.toList());
      }
      return new CheckConstraint(checkConstraints);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
    throws HiveException, NoSuchObjectException {
    try {
      getMSC().addPrimaryKey(primaryKeyCols);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
    throws HiveException, NoSuchObjectException {
    try {
      getMSC().addForeignKey(foreignKeyCols);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
    throws HiveException, NoSuchObjectException {
    try {
      getMSC().addUniqueConstraint(uniqueConstraintCols);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
    throws HiveException, NoSuchObjectException {
    try {
      getMSC().addNotNullConstraint(notNullConstraintCols);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().addDefaultConstraint(defaultConstraints);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().addCheckConstraint(checkConstraints);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws HiveException {
    try {
      getMSC().createResourcePlan(resourcePlan, copyFromName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan getResourcePlan(String rpName) throws HiveException {
    try {
      return getMSC().getResourcePlan(rpName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<WMResourcePlan> getAllResourcePlans() throws HiveException {
    try {
      return getMSC().getAllResourcePlans();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropResourcePlan(String rpName) throws HiveException {
    try {
      getMSC().dropResourcePlan(rpName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan alterResourcePlan(String rpName, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws HiveException {
    try {
      return getMSC().alterResourcePlan(rpName, resourcePlan, canActivateDisabled,
          isForceDeactivate, isReplace);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan getActiveResourcePlan() throws HiveException {
    try {
      return getMSC().getActiveResourcePlan();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMValidateResourcePlanResponse validateResourcePlan(String rpName) throws HiveException {
    try {
      return getMSC().validateResourcePlan(rpName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createWMTrigger(WMTrigger trigger) throws HiveException {
    try {
      getMSC().createWMTrigger(trigger);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void alterWMTrigger(WMTrigger trigger) throws HiveException {
    try {
      getMSC().alterWMTrigger(trigger);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMTrigger(String rpName, String triggerName) throws HiveException {
    try {
      getMSC().dropWMTrigger(rpName, triggerName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createWMPool(WMPool pool) throws HiveException {
    try {
      getMSC().createWMPool(pool);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void alterWMPool(WMNullablePool pool, String poolPath) throws HiveException {
    try {
      getMSC().alterWMPool(pool, poolPath);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMPool(String resourcePlanName, String poolPath) throws HiveException {
    try {
      getMSC().dropWMPool(resourcePlanName, poolPath);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws HiveException {
    try {
      getMSC().createOrUpdateWMMapping(mapping, isUpdate);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMMapping(WMMapping mapping) throws HiveException {
    try {
      getMSC().dropWMMapping(mapping);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop) throws HiveException {
    try {
      getMSC().createOrDropTriggerToPoolMapping(resourcePlanName, triggerName, poolPath, shouldDrop);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  @Nullable
  public StorageHandlerInfo getStorageHandlerInfo(Table table)
      throws HiveException {
    try {
      HiveStorageHandler storageHandler = createStorageHandler(table.getTTable());
      return storageHandler == null ? null : storageHandler.getStorageHandlerInfo(table.getTTable());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
