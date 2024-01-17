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
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;

import static org.apache.hadoop.hive.conf.Constants.MATERIALIZED_VIEW_REWRITING_TIME_WINDOW;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_LOAD_DYNAMIC_PARTITIONS_SCAN_SPECIFIC_PARTITIONS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_WRITE_NOTIFICATION_MAX_BATCH_SIZE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.CTAS_LEGACY_CONFIG;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.ql.ddl.DDLUtils.isIcebergStatsSource;
import static org.apache.hadoop.hive.ql.ddl.DDLUtils.isIcebergTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.getFullTableName;
import static org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm.CALCITE;
import static org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm.ALL;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils.extractTable;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.DataCopyStatistics;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.RetryUtilities;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.database.drop.DropDatabaseDesc;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.PartitionDetails;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.TableSnapshot;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPrunerUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
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
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.thrift.TException;
import org.apache.thrift.TApplicationException;
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
  private final String CLASS_NAME = Hive.class.getName();

  private HiveConf conf = null;
  private IMetaStoreClient metaStoreClient;
  private SynchronizedMetaStoreClient syncMetaStoreClient;
  private UserGroupInformation owner;
  private boolean isAllowClose = true;
  private final static int DEFAULT_BATCH_DECAYING_FACTOR = 2;

  // metastore calls timing information
  private final ConcurrentHashMap<String, Long> metaCallTimeMap = new ConcurrentHashMap<>();

  // Static class to store thread local Hive object.
  private static class ThreadLocalHive extends ThreadLocal<Hive> {
    @Override
    protected Hive initialValue() {
      return null;
    }

    @Override
    public synchronized void set(Hive hiveObj) {
      Hive currentHive = this.get();
      if (currentHive != hiveObj) {
        // Remove/close current thread-local Hive object before overwriting with new Hive object.
        remove();
        super.set(hiveObj);
      }
    }

    @Override
    public synchronized void remove() {
      Hive currentHive = this.get();
      if (currentHive != null) {
        // Close the metastore connections before removing it from thread local hiveDB.
        currentHive.close(false);
        super.remove();
      }
    }
  }

  private static ThreadLocalHive hiveDB = new ThreadLocalHive();

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
    } catch (Exception | Error e) {
      LOG.warn("Failed to register all functions.", e);
      didRegisterAllFuncs.compareAndSet(REG_FUNCS_PENDING, REG_FUNCS_NO);
      if (e instanceof Exception) {
        throw new HiveException(e);
      } else {
        throw e;
      }
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
                    FunctionUtils.toFunctionResource(function.getResourceUris()));
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

  public static Hive createHiveForSession(HiveConf c) throws HiveException {
    return create(c, true);
  }

  public void setConf(HiveConf c) {
    this.conf = c;
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

  /**
   * Same as {@link #get(HiveConf)}, except that it does not register all functions.
   */
  public static Hive getWithoutRegisterFns(HiveConf c) throws HiveException {
    return getInternal(c, false, false, false);
  }

  private static Hive getInternal(HiveConf c, boolean needsRefresh, boolean isFastCheck,
      boolean doRegisterAllFns) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || !db.isCurrentUserOwner() || needsRefresh
        || (c != null && !isCompatible(db, c, isFastCheck))) {
      if (db != null) {
        LOG.debug("Creating new db. db = " + db + ", needsRefresh = " + needsRefresh +
                ", db.isCurrentUserOwner = " + db.isCurrentUserOwner());
        closeCurrent();
      }
      db = create(c, doRegisterAllFns);
    }
    if (c != null) {
      db.conf = c;
    }
    return db;
  }

  private static Hive create(HiveConf c, boolean doRegisterAllFns) throws HiveException {
    if (c == null) {
      c = createHiveConf();
    }
    c.set("fs.scheme.class", "dfs");
    Hive newdb = new Hive(c, doRegisterAllFns);
    if (newdb.getHMSClientCapabilities() == null || newdb.getHMSClientCapabilities().length == 0) {
      if (c.get(HiveConf.ConfVars.METASTORE_CLIENT_CAPABILITIES.varname) != null) {
        String[] capabilities = c.get(HiveConf.ConfVars.METASTORE_CLIENT_CAPABILITIES.varname).split(",");
        newdb.setHMSClientCapabilities(capabilities);
        String hostName = "unknown";
        try {
          hostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ue) {
        }
        newdb.setHMSClientIdentifier("Hiveserver2#" + HiveVersionInfo.getVersion() + "@" + hostName);
      }
    }
    hiveDB.set(newdb);
    return newdb;
  }

  private static HiveConf createHiveConf() {
    SessionState session = SessionState.get();
    return (session == null) ? new HiveConf(Hive.class) : session.getConf();
  }

  public void setHMSClientCapabilities(String[] capabilities) {
    HiveMetaStoreClient.setProcessorCapabilities(capabilities);
  }

  public void setHMSClientIdentifier(final String id) {
    HiveMetaStoreClient.setProcessorIdentifier(id);
  }

  public String[] getHMSClientCapabilities() {
    return HiveMetaStoreClient.getProcessorCapabilities();
  }

  public String getHMSClientIdentifier() {
    return HiveMetaStoreClient.getProcessorIdentifier();
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

  private boolean isCurrentUserOwner() throws HiveException {
    try {
      return owner == null || owner.equals(UserGroupInformation.getCurrentUser());
    } catch(IOException e) {
      throw new HiveException("Error getting current user: " + e.getMessage(), e);
    }
  }

  public static Hive getThreadLocal() {
    return hiveDB.get();
  }

  public static Hive get() throws HiveException {
    return get(true);
  }

  @VisibleForTesting
  public static Hive get(IMetaStoreClient msc) throws HiveException, MetaException {
    Hive hive = get(true);
    hive.setMSC(msc);
    return hive;
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
    // turn off calcite rexnode normalization
    System.setProperty("calcite.enable.rexnode.digest.normalize", "false");
    if (doRegisterAllFns) {
      registerAllFunctionsOnce();
    }
  }

  /**
   * GC is attempting to destroy the object.
   * No one references this Hive anymore, so HMS connection from this Hive object can be closed.
   * @throws Throwable
   */
  @Override
  protected void finalize() throws Throwable {
    close(true);
    super.finalize();
  }

  /**
   * Marks if the given Hive object is allowed to close metastore connections.
   * @param allowClose
   */
  public void setAllowClose(boolean allowClose) {
    isAllowClose = allowClose;
  }

  /**
   * Gets the allowClose flag which determines if it is allowed to close metastore connections.
   * @return allowClose flag
   */
  public boolean allowClose() {
    return isAllowClose;
  }

  /**
   * Closes the connection to metastore for the calling thread if allow to close.
   * @param forceClose - Override the isAllowClose flag to forcefully close the MS connections.
   */
  public void close(boolean forceClose) {
    if (allowClose() || forceClose) {
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
    dropDatabase(new DropDatabaseDesc(name, ignoreUnknownDb, cascade, deleteData));
  }

  public void dropDatabase(DropDatabaseDesc desc) 
      throws HiveException, NoSuchObjectException {
    boolean isSoftDelete = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);
    
    long txnId = Optional.ofNullable(SessionState.get())
      .map(SessionState::getTxnMgr)
      .map(HiveTxnManager::getCurrentTxnId).orElse(0L);
    
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setCatalogName(getDefaultCatalog(conf));
    req.setName(desc.getDatabaseName());
    req.setIgnoreUnknownDb(desc.getIfExists());
    req.setDeleteData(desc.isDeleteData());
    req.setCascade(desc.isCasdade());
    req.setSoftDelete(isSoftDelete);
    req.setTxnId(txnId);
    
    try {
      getMSC().dropDatabase(req);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Dry run that translates table
   *
   * @param tbl
   *          a table object
   * @throws HiveException
   */
  public Table getTranslateTableDryrun(org.apache.hadoop.hive.metastore.api.Table tbl) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable  = getMSC().getTranslateTableDryrun(tbl);
    } catch (AlreadyExistsException e) {
      throw new HiveException(e);
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return new Table(tTable);
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
   * @throws HiveException
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

  public void alterTable(String fullyQlfdTblName, Table newTbl, boolean cascade,
                         EnvironmentContext environmentContext, boolean transactional, long writeId)
          throws HiveException {
    String[] names = Utilities.getDbTableName(fullyQlfdTblName);
    alterTable(null, names[0], names[1], newTbl, cascade, environmentContext, transactional,
                writeId);
  }

  public void alterTable(String catName, String dbName, String tblName, Table newTbl, boolean cascade,
                         EnvironmentContext environmentContext, boolean transactional) throws HiveException {
    alterTable(catName, dbName, tblName, newTbl, cascade, environmentContext, transactional, 0);
  }

  public void alterTable(String catName, String dbName, String tblName, Table newTbl, boolean cascade,
      EnvironmentContext environmentContext, boolean transactional, long replWriteId)
      throws HiveException {

    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newTbl.getParameters() != null) {
        newTbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      if (environmentContext == null) {
        environmentContext = new EnvironmentContext();
      }
      if (isRename(environmentContext)) {
        newTbl.validateName(conf);
        environmentContext.putToProperties(HiveMetaHook.OLD_TABLE_NAME, tblName);
        environmentContext.putToProperties(HiveMetaHook.OLD_DB_NAME, dbName);
      } else {
        newTbl.checkValidity(conf);
      }
      if (cascade) {
        environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
      }

      // Take a table snapshot and set it to newTbl.
      AcidUtils.TableSnapshot tableSnapshot = null;
      if (transactional) {
        if (replWriteId > 0) {
          // We need a valid writeId list for a transactional table modification. During
          // replication we do not have a valid writeId list which was used to modify the table
          // on the source. But we know for sure that the writeId associated with it was valid
          // then (otherwise modification would have failed on the source). So use a valid
          // transaction list with only that writeId.
          ValidWriteIdList writeIds = new ValidReaderWriteIdList(TableName.getDbTable(dbName, tblName),
                                                                  new long[0], new BitSet(),
                                                                  replWriteId);
          tableSnapshot = new TableSnapshot(replWriteId, writeIds.writeToString());
        } else {
          // Make sure we pass in the names, so we can get the correct snapshot for rename table.
          tableSnapshot = AcidUtils.getTableSnapshot(conf, newTbl, dbName, tblName, true);
        }
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

  private static boolean isRename(EnvironmentContext environmentContext) {
    if (environmentContext.isSetProperties()) {
      String operation = environmentContext.getProperties().get(HiveMetaHook.ALTER_TABLE_OPERATION_TYPE);
      return operation != null && AlterTableType.RENAME == AlterTableType.valueOf(operation);
    }
    return false;
  }

  /**
   * Create a dataconnector
   * @param connector
   * @param ifNotExist if true, will ignore AlreadyExistsException exception
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDataConnector(DataConnector connector, boolean ifNotExist)
      throws AlreadyExistsException, HiveException {
    try {
      getMSC().createDataConnector(connector);
    } catch (AlreadyExistsException e) {
      if (!ifNotExist) {
        throw e;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Create a DataConnector. Raise an error if a dataconnector with the same name already exists.
   * @param connector
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDataConnector(DataConnector connector) throws AlreadyExistsException, HiveException {
    createDataConnector(connector, false);
  }

  /**
   * Drop a dataconnector.
   * @param name
   * @throws NoSuchObjectException
   * @throws HiveException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDataConnector(java.lang.String, boolean, boolean)
   */
  public void dropDataConnector(String name, boolean ifNotExists) throws HiveException, NoSuchObjectException {
    dropDataConnector(name, ifNotExists, true);
  }

  /**
   * Drop a dataconnector
   * @param name
   * @param checkReferences drop only if there are no dbs referencing this connector
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDataConnector(String name, boolean ifNotExists, boolean checkReferences)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().dropDataConnector(name, ifNotExists, checkReferences);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get the dataconnector by name.
   * @param dcName the name of the dataconnector.
   * @return a DataConnector object if this dataconnector exists, null otherwise.
   * @throws HiveException
   */
  public DataConnector getDataConnector(String dcName) throws HiveException {
    try {
      return getMSC().getDataConnector(dcName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all dataconnector names.
   * @return List of all dataconnector names.
   * @throws HiveException
   */
  public List<String> getAllDataConnectorNames() throws HiveException {
    try {
      return getMSC().getAllDataConnectorNames();
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void alterDataConnector(String dcName, DataConnector connector)
      throws HiveException {
    try {
      getMSC().alterDataConnector(dcName, connector);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter dataconnector " + dcName + ". " + e.getMessage(), e);
    } catch (NoSuchObjectException e) {
      throw new HiveException("DataConnector " + dcName + " does not exists.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter dataconnector " + dcName + ". " + e.getMessage(), e);
    }
  }

  public void updateCreationMetadata(String dbName, String tableName, MaterializedViewMetadata metadata)
      throws HiveException {
    try {
      getMSC().updateCreationMetadata(dbName, tableName, metadata.creationMetadata);
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
   * @throws HiveException
   */
  @Deprecated
  public void alterPartition(String tblName, Partition newPart,
      EnvironmentContext environmentContext, boolean transactional)
      throws InvalidOperationException, HiveException {
    String[] names = Utilities.getDbTableName(tblName);
    alterPartition(null, names[0], names[1], newPart, environmentContext, transactional);
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
   * @throws HiveException
   */
  public void alterPartition(String catName, String dbName, String tblName, Partition newPart,
                             EnvironmentContext environmentContext, boolean transactional)
      throws InvalidOperationException, HiveException {
    try {
      if (catName == null) {
        catName = getDefaultCatalog(conf);
      }
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
      getSynchronizedMSC().alter_partition(catName,
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
   * @throws HiveException
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
   * @throws HiveException
   */
  public void renamePartition(Table tbl, Map<String, String> oldPartSpec, Partition newPart,
                              long replWriteId)
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
      boolean clonePart = false;
      long txnId = 0;
      
      if (AcidUtils.isTransactionalTable(tbl)) {
        TableSnapshot tableSnapshot;
        if (replWriteId > 0) {
          // We need a valid writeId list for a transactional table modification. During
          // replication we do not have a valid writeId list which was used to modify the table
          // on the source. But we know for sure that the writeId associated with it was valid
          // then (otherwise modification would have failed on the source). So use a valid
          // transaction list with only that writeId.
          ValidWriteIdList writeIds = new ValidReaderWriteIdList(TableName.getDbTable(tbl.getDbName(),
                  tbl.getTableName()), new long[0], new BitSet(), replWriteId);
          tableSnapshot = new TableSnapshot(replWriteId, writeIds.writeToString());
        } else {
          // Set table snapshot to api.Table to make it persistent.
          tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, true);
        }

        if (tableSnapshot != null) {
          newPart.getTPartition().setWriteId(tableSnapshot.getWriteId());
          validWriteIds = tableSnapshot.getValidWriteIdList();
        }
        clonePart = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_RENAME_PARTITION_MAKE_COPY)
          || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

        txnId = Optional.ofNullable(SessionState.get())
          .map(ss -> ss.getTxnMgr().getCurrentTxnId()).orElse(0L);
      }

      String catName = (tbl.getCatalogName() != null) ? tbl.getCatalogName() : getDefaultCatalog(conf);
      getMSC().renamePartition(catName, tbl.getDbName(), tbl.getTableName(), pvals,
          newPart.getTPartition(), validWriteIds, txnId, clonePart);

    } catch (InvalidOperationException e){
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    } catch (TException e) {
      throw new HiveException("Unable to rename partition. " + e.getMessage(), e);
    }
  }

  // TODO: this whole path won't work with catalogs
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

  // TODO: from here down dozens of methods do not support catalog. I got tired marking them.

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
      if (org.apache.commons.lang3.StringUtils.isBlank(tbl.getDbName())) {
        tbl.setDbName(SessionState.get().getCurrentDatabase());
      }
      if (tbl.getCols().size() == 0 || tbl.getSd().getColsSize() == 0) {
        tbl.setFields(HiveMetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(), tbl.getDeserializer(), conf));
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
        if (AcidUtils.isTransactionalTable(tbl)) {
          boolean createTableUseSuffix = HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
            || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

          if (createTableUseSuffix 
                && (tbl.getSd().getLocation() == null || tbl.getSd().getLocation().isEmpty())) {
            tbl.setProperty(SOFT_DELETE_TABLE, Boolean.TRUE.toString());
          }
          tTbl.setTxnId(ss.getTxnMgr().getCurrentTxnId());
        }
      }
      // Set table snapshot to api.Table to make it persistent. A transactional table being
      // replicated may have a valid write Id copied from the source. Use that instead of
      // crafting one on the replica.
      if (tTbl.getWriteId() <= 0) {
        TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, true);
        if (tableSnapshot != null) {
          tTbl.setWriteId(tableSnapshot.getWriteId());
        }
      }

      CreateTableRequest request = new CreateTableRequest(tTbl);

      if (isIcebergTable(tbl)) {
        EnvironmentContext envContext = new EnvironmentContext();
        if (TableType.MANAGED_TABLE.equals(tbl.getTableType())) {
          envContext.putToProperties(CTAS_LEGACY_CONFIG, Boolean.TRUE.toString());
        }
        if (isIcebergStatsSource(conf)) {
          envContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
        }
        request.setEnvContext(envContext);
      }

      request.setPrimaryKeys(primaryKeys);
      request.setForeignKeys(foreignKeys);
      request.setUniqueConstraints(uniqueConstraints);
      request.setNotNullConstraints(notNullConstraints);
      request.setDefaultConstraints(defaultConstraints);
      request.setCheckConstraints(checkConstraints);

      getMSC().createTable(request);

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
      Table tbl, Deserializer deserializer, Configuration conf) throws SerDeException, MetaException {
    List<FieldSchema> schema = HiveMetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(), deserializer, conf);
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

  public void dropTable(Table table, boolean ifPurge) throws HiveException {
    boolean tableWithSuffix = AcidUtils.isTableSoftDeleteEnabled(table, conf);
    long txnId = Optional.ofNullable(SessionState.get())
      .map(ss -> ss.getTxnMgr().getCurrentTxnId()).orElse(0L);
    table.getTTable().setTxnId(txnId);

    dropTable(table.getTTable(), !tableWithSuffix, true, ifPurge);
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

  public void dropTable(org.apache.hadoop.hive.metastore.api.Table table, 
      boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws HiveException {
    try {
      getMSC().dropTable(table, deleteData, ignoreUnknownTab, ifPurge);
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      AcidUtils.tryInvalidateDirCache(table);
    }
  }

  /**
   * Truncates the table/partition as per specifications. Just trash the data files
   *
   * @param dbDotTableName
   *          name of the table
   * @throws HiveException
   */
  public void truncateTable(String dbDotTableName, Map<String, String> partSpec, Long writeId) throws HiveException {
    try {
      Table table = getTable(dbDotTableName, true);
      AcidUtils.TableSnapshot snapshot = null;
      if (AcidUtils.isTransactionalTable(table)) {
        if (writeId <= 0) {
          snapshot = AcidUtils.getTableSnapshot(conf, table, true);
        } else {
          String fullTableName = getFullTableName(table.getDbName(), table.getTableName());
          ValidWriteIdList writeIdList = getMSC().getValidWriteIds(fullTableName, writeId);
          snapshot = new TableSnapshot(writeId, writeIdList.writeToString());
        }
      }

      // TODO: APIs with catalog names
      List<String> partNames = ((null == partSpec)
              ? null : getPartitionNames(table.getDbName(), table.getTableName(), partSpec, (short) -1));
      if (snapshot == null) {
        getMSC().truncateTable(table.getDbName(), table.getTableName(), partNames);
      } else {
        boolean truncateUseBase = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE)
          || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);
        getMSC().truncateTable(table.getDbName(), table.getTableName(), partNames,
            snapshot.getValidWriteIdList(), snapshot.getWriteId(), !truncateUseBase);
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
    String[] nameParts = tableName.split("\\.");
    if (nameParts.length == 3) {
      Table table = this.getTable(nameParts[0], nameParts[1], nameParts[2], throwException);
      return table;
    } else {
      String[] names = Utilities.getDbTableName(tableName);
      Table table = this.getTable(names[0], names[1], null, throwException);
      return table;
    }
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
     // TODO: catalog... etc everywhere
    if (tableName.contains(".")) {
      String[] names = Utilities.getDbTableName(tableName);
      return this.getTable(names[0], names[1], null, true);
    } else {
      return this.getTable(dbName, tableName, null, true);
    }
  }

  /**
   * Returns metadata of the table
   *
   * @param tableName
   *          the tableName object
   * @return the table
   * @exception HiveException
   *              if there's an internal error or if the table doesn't exist
   */
  public Table getTable(TableName tableName) throws HiveException {
    return this.getTable(ObjectUtils.firstNonNull(tableName.getDb(), SessionState.get().getCurrentDatabase()),
        tableName.getTable(), tableName.getTableMetaRef(), true);
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param tableMetaRef
   *          the name of the table meta ref, e.g. iceberg metadata table or branch
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName,
                        final String tableMetaRef, boolean throwException) throws HiveException {
    return this.getTable(dbName, tableName, tableMetaRef, throwException, false);
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
  public Table getTable(final String dbName, final String tableName, boolean throwException) throws HiveException {
    return this.getTable(dbName, tableName, null, throwException);
  }

  /**
   * Returns metadata of the table.
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
  public Table getTable(final String dbName, final String tableName, boolean throwException, boolean checkTransactional)
      throws HiveException {
    return getTable(dbName, tableName, null, throwException, checkTransactional, false);
  }

  /**
   * Returns metadata of the table.
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param tableMetaRef
   *          the name of the table meta ref, e.g. iceberg metadata table or branch
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @param checkTransactional
   *          checks whether the metadata table stats are valid (or
   *          compilant with the snapshot isolation of) for the current transaction.
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName, String tableMetaRef, boolean throwException,
                        boolean checkTransactional) throws HiveException {
    return getTable(dbName, tableName, tableMetaRef, throwException, checkTransactional, false);
  }

  /**
   * Returns metadata of the table.
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param tableMetaRef
   *          the name of the table meta ref, e.g. iceberg metadata table or branch
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @param checkTransactional
   *          checks whether the metadata table stats are valid (or
   *          compilant with the snapshot isolation of) for the current transaction.
   * @param getColumnStats
   *          get column statistics if available
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName, String tableMetaRef, boolean throwException,
                        boolean checkTransactional, boolean getColumnStats) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      // Note: this is currently called w/true from StatsOptimizer only.
      GetTableRequest request = new GetTableRequest(dbName, tableName);
      request.setCatName(getDefaultCatalog(conf));
      request.setGetColumnStats(getColumnStats);
      request.setEngine(Constants.HIVE_ENGINE);
      if (checkTransactional) {
        ValidWriteIdList validWriteIdList = null;
        long txnId = SessionState.get() != null && SessionState.get().getTxnMgr() != null ?
            SessionState.get().getTxnMgr().getCurrentTxnId() : 0;
        if (txnId > 0) {
          validWriteIdList = AcidUtils.getTableValidWriteIdListWithTxnList(conf, dbName, tableName);
        }
        request.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
      }
      tTable = getMSC().getTable(request);
    } catch (NoSuchObjectException e) {
      if (throwException) {
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

    Table t = new Table(tTable);
    if (tableMetaRef != null) {
      if (t.getStorageHandler() == null || !t.getStorageHandler().isTableMetaRefSupported()) {
        throw new SemanticException(ErrorMsg.TABLE_META_REF_NOT_SUPPORTED, t.getTableName());
      }
      t = t.getStorageHandler().checkAndSetTableMetaRef(t, tableMetaRef);
    }
    return t;
  }

  /**
   * Get ValidWriteIdList for the current transaction.
   * This fetches the ValidWriteIdList from the metastore for a given table if txnManager has an open transaction.
   *
   * @param dbName
   * @param tableName
   * @return
   * @throws LockException
   */
  private ValidWriteIdList getValidWriteIdList(String dbName, String tableName) throws LockException {
    ValidWriteIdList validWriteIdList = null;
    SessionState sessionState = SessionState.get();
    HiveTxnManager txnMgr = sessionState != null? sessionState.getTxnMgr() : null;
    long txnId = txnMgr != null ? txnMgr.getCurrentTxnId() : 0;
    if (txnId > 0) {
      validWriteIdList = AcidUtils.getTableValidWriteIdListWithTxnList(conf, dbName, tableName);
    } else {
      String fullTableName = getFullTableName(dbName, tableName);
      validWriteIdList = new ValidReaderWriteIdList(fullTableName, new long[0], new BitSet(), Long.MAX_VALUE);
    }
    return validWriteIdList;
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

  /**
   * Get materialized views for the specified database that match the provided regex pattern.
   * @param dbName
   * @param pattern
   * @return List of materialized view table objects
   * @throws HiveException
   */
  public List<Table> getMaterializedViewObjectsByPattern(String dbName, String pattern) throws HiveException {
    return getTableObjects(dbName, pattern, TableType.MATERIALIZED_VIEW);
  }

  public List<Table> getTableObjects(String dbName, String pattern, TableType tableType) throws HiveException {
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
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_TABLE);

    if (dbName == null) {
      dbName = SessionState.get().getCurrentDatabase();
    }

    try {
      List<String> result;
      if (type != null) {
        if (pattern != null) {
          result = getMSC().getTables(dbName, pattern, type);
        } else {
          result = getMSC().getTables(dbName, ".*", type);
        }
      } else {
        if (pattern != null) {
          result = getMSC().getTables(dbName, pattern);
        } else {
          result = getMSC().getTables(dbName, ".*");
        }
      }
      return result;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_TABLE, "HS2-cache");
    }
  }

  /**
   * Get the materialized views that have been enabled for rewriting from the
   * cache (registry). It will preprocess them to discard those that are
   * outdated and augment those that need to be augmented, e.g., if incremental
   * rewriting is enabled.
   *
   * @return the list of materialized views available for rewriting from the registry
   * @throws HiveException
   */
  public List<HiveRelOptMaterialization> getPreprocessedMaterializedViewsFromRegistry(
      Set<TableName> tablesUsed, HiveTxnManager txnMgr) throws HiveException {
    // From cache
    List<HiveRelOptMaterialization> materializedViews =
        HiveMaterializedViewsRegistry.get().getRewritingMaterializedViews();
    if (materializedViews.isEmpty()) {
      return Collections.emptyList();
    }
    // Add to final result
    return filterAugmentMaterializedViews(materializedViews, tablesUsed, txnMgr);
  }

  private List<HiveRelOptMaterialization> filterAugmentMaterializedViews(List<HiveRelOptMaterialization> materializedViews,
        Set<TableName> tablesUsed, HiveTxnManager txnMgr) throws HiveException {
    final String validTxnsList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    final boolean tryIncrementalRewriting =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_INCREMENTAL);
    try {
      // Final result
      List<HiveRelOptMaterialization> result = new ArrayList<>();
      for (HiveRelOptMaterialization materialization : materializedViews) {
        final Table materializedViewTable = extractTable(materialization);
        final Boolean outdated = isOutdatedMaterializedView(
            materializedViewTable, tablesUsed, false, txnMgr);
        if (outdated == null) {
          continue;
        }

        if (outdated) {
          // The MV is outdated, see whether we should consider it for rewriting or not
          if (!tryIncrementalRewriting) {
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as its contents are outdated");
            continue;
          }
          // We will rewrite it to include the filters on transaction list
          // so we can produce partial rewritings.
          // This would be costly since we are doing it for every materialized view
          // that is outdated, but it only happens for more than one materialized view
          // if rewriting with outdated materialized views is enabled (currently
          // disabled by default).
          materialization = HiveMaterializedViewUtils.augmentMaterializationWithTimeInformation(
              materialization, validTxnsList, materializedViewTable.getMVMetadata().getSnapshot());
        }
        result.addAll(HiveMaterializedViewUtils.deriveGroupingSetsMaterializedViews(materialization));
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
   * This method checks invalidation time window defined in materialization.
   */
  public Boolean isOutdatedMaterializedView(
          Table materializedViewTable, Set<TableName> tablesUsed,
          boolean forceMVContentsUpToDate, HiveTxnManager txnMgr) throws HiveException {

    String validTxnsList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if (validTxnsList == null) {
      return null;
    }
    long defaultTimeWindow = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW, TimeUnit.MILLISECONDS);

    // Check if materialization defined its own invalidation time window
    String timeWindowString = materializedViewTable.getProperty(MATERIALIZED_VIEW_REWRITING_TIME_WINDOW);
    long timeWindow = org.apache.commons.lang3.StringUtils.isEmpty(timeWindowString) ? defaultTimeWindow :
            HiveConf.toTime(timeWindowString,
                    HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_TIME_WINDOW),
                    TimeUnit.MILLISECONDS);
    MaterializedViewMetadata mvMetadata = materializedViewTable.getMVMetadata();
    boolean outdated = false;
    if (timeWindow < 0L) {
      // We only consider the materialized view to be outdated if forceOutdated = true, i.e.,
      // if it is a rebuild. Otherwise, it passed the test and we use it as it is.
      outdated = forceMVContentsUpToDate;
    } else {
      // Check whether the materialized view is invalidated
      if (forceMVContentsUpToDate || timeWindow == 0L ||
              mvMetadata.getMaterializationTime() < System.currentTimeMillis() - timeWindow) {
        return HiveMaterializedViewUtils.isOutdatedMaterializedView(
                validTxnsList, txnMgr, this, tablesUsed, materializedViewTable);
      }
    }
    return outdated;
  }

  /**
   * Utility method that returns whether a materialized view is outdated (true), not outdated
   * (false), or it cannot be determined (null). The latest case may happen e.g. when the
   * materialized view definition uses external tables.
   */
  public Boolean isOutdatedMaterializedView(HiveTxnManager txnManager, Table table) throws HiveException {

    String validTxnsList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    if (validTxnsList == null) {
      return null;
    }

    return HiveMaterializedViewUtils.isOutdatedMaterializedView(
        validTxnsList, txnManager, this, table.getMVMetadata().getSourceTableNames(), table);
  }

  /**
   * Validate that the materialized views retrieved from registry are still up-to-date.
   * For those that are not, the method loads them from the metastore into the registry.
   *
   * @return true if they are up-to-date, otherwise false
   * @throws HiveException
   */
  public boolean validateMaterializedViewsFromRegistry(List<Table> cachedMaterializedViewTables,
      Set<TableName> tablesUsed, HiveTxnManager txnMgr) throws HiveException {
    try {
      // Final result
      boolean result = true;
      for (Table cachedMaterializedViewTable : cachedMaterializedViewTables) {
        // Retrieve the materialized view table from the metastore
        final Table materializedViewTable = getTable(
            cachedMaterializedViewTable.getDbName(), cachedMaterializedViewTable.getTableName());
        if (materializedViewTable == null || !materializedViewTable.isRewriteEnabled()) {
          // This could happen if materialized view has been deleted or rewriting has been disabled.
          // We remove it from the registry and set result to false.
          HiveMaterializedViewsRegistry.get().dropMaterializedView(cachedMaterializedViewTable);
          result = false;
        } else {
          final Boolean outdated = isOutdatedMaterializedView(cachedMaterializedViewTable, tablesUsed, false, txnMgr);
          if (outdated == null) {
            result = false;
            continue;
          }
          // If the cached materialized view was not outdated wrt the query snapshot,
          // then we know that the metastore version should be either the same or
          // more recent. If it is more recent, snapshot isolation will shield us
          // from the reading its contents after snapshot was acquired, but we will
          // update the registry so we have most recent version.
          // On the other hand, if the materialized view in the cache was outdated,
          // we can only use it if the version that was in the cache is the same one
          // that we can find in the metastore.
          if (outdated) {
            if (!cachedMaterializedViewTable.equals(materializedViewTable)) {
              // We ignore and update the registry
              HiveMaterializedViewsRegistry.get().refreshMaterializedView(conf, cachedMaterializedViewTable, materializedViewTable);
              result = false;
            } else {
              // Obtain additional information if we should try incremental rewriting / rebuild
              // We will not try partial rewriting if there were update/delete/compaction operations on source tables
              Materialization invalidationInfo = getMaterializationInvalidationInfo(materializedViewTable.getMVMetadata());
              if (invalidationInfo == null || invalidationInfo.isSourceTablesUpdateDeleteModified() ||
                  invalidationInfo.isSourceTablesCompacted()) {
                // We ignore (as it did not meet the requirements), but we do not need to update it in the
                // registry, since it is up-to-date
                result = false;
              }
            }
          } else if (!cachedMaterializedViewTable.equals(materializedViewTable)) {
            // Update the registry
            HiveMaterializedViewsRegistry.get().refreshMaterializedView(conf, cachedMaterializedViewTable, materializedViewTable);
          }
        }
      }
      return result;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private Materialization getMaterializationInvalidationInfo(MaterializedViewMetadata metadata)
      throws TException, HiveException {
    Optional<SourceTable> first = metadata.getSourceTables().stream().findFirst();
    if (!first.isPresent()) {
      // This is unexpected: all MV must have at least one source
      Materialization materialization = new Materialization();
      materialization.setSourceTablesCompacted(true);
      materialization.setSourceTablesUpdateDeleteModified(true);
      return new Materialization();
    } else {
      Table table = getTable(first.get().getTable().getDbName(), first.get().getTable().getTableName());
      if (!(table.isNonNative() && table.getStorageHandler().areSnapshotsSupported())) {
        // Mixing native and non-native acid source tables are not supported. If the first source is native acid
        // the rest is expected to be native acid
        return getMSC().getMaterializationInvalidationInfo(
                metadata.creationMetadata, conf.get(ValidTxnList.VALID_TXNS_KEY));
      }
    }

    MaterializationSnapshot mvSnapshot = MaterializationSnapshot.fromJson(metadata.creationMetadata.getValidTxnList());

    boolean hasAppendsOnly = true;
    for (SourceTable sourceTable : metadata.getSourceTables()) {
      Table table = getTable(sourceTable.getTable().getDbName(), sourceTable.getTable().getTableName());
      HiveStorageHandler storageHandler = table.getStorageHandler();
      if (storageHandler == null) {
        Materialization materialization = new Materialization();
        materialization.setSourceTablesCompacted(true);
        return materialization;
      }
      Boolean b = storageHandler.hasAppendsOnly(
          table, mvSnapshot.getTableSnapshots().get(table.getFullyQualifiedName()));
      if (b == null) {
        Materialization materialization = new Materialization();
        materialization.setSourceTablesCompacted(true);
        return materialization;
      } else if (!b) {
        hasAppendsOnly = false;
        break;
      }
    }
    Materialization materialization = new Materialization();
    // TODO: delete operations are not supported yet.
    // Set setSourceTablesCompacted to false when delete is supported
    materialization.setSourceTablesCompacted(!hasAppendsOnly);
    materialization.setSourceTablesUpdateDeleteModified(!hasAppendsOnly);
    return materialization;
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
  public List<HiveRelOptMaterialization> getPreprocessedMaterializedViews(
      Set<TableName> tablesUsed, HiveTxnManager txnMgr)
      throws HiveException {
    // From metastore
    List<Table> materializedViewTables =
        getAllMaterializedViewObjectsForRewriting();
    if (materializedViewTables.isEmpty()) {
      return Collections.emptyList();
    }
    // Return final result
    return getValidMaterializedViews(materializedViewTables, tablesUsed, false, true, txnMgr, EnumSet.of(CALCITE));
  }

  /**
   * Get the target materialized view from the metastore. Although it may load the plan
   * from the registry, it is guaranteed that it will always return an up-to-date version
   * wrt metastore.
   *
   * @return the materialized view for rebuild
   * @throws HiveException
   */
  public HiveRelOptMaterialization getMaterializedViewForRebuild(String dbName, String materializedViewName,
      Set<TableName> tablesUsed, HiveTxnManager txnMgr) throws HiveException {
    List<HiveRelOptMaterialization> validMaterializedViews = getValidMaterializedViews(
            ImmutableList.of(getTable(dbName, materializedViewName)), tablesUsed, true, false, txnMgr, ALL);
    if (validMaterializedViews.isEmpty()) {
      return null;
    }
    Preconditions.checkState(validMaterializedViews.size() == 1,
        "Returned more than a materialized view for rebuild");
    return validMaterializedViews.get(0);
  }

  private List<HiveRelOptMaterialization> getValidMaterializedViews(List<Table> materializedViewTables,
      Set<TableName> tablesUsed, boolean forceMVContentsUpToDate, boolean expandGroupingSets,
      HiveTxnManager txnMgr, EnumSet<RewriteAlgorithm> scope)
      throws HiveException {
    final String validTxnsList = conf.get(ValidTxnList.VALID_TXNS_KEY);
    final boolean tryIncrementalRewriting =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REWRITING_INCREMENTAL);
    final boolean tryIncrementalRebuild =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_MATERIALIZED_VIEW_REBUILD_INCREMENTAL);
    try {
      // Final result
      List<HiveRelOptMaterialization> result = new ArrayList<>();
      for (Table materializedViewTable : materializedViewTables) {
        final Boolean outdated = isOutdatedMaterializedView(
                materializedViewTable, tablesUsed, forceMVContentsUpToDate, txnMgr);
        if (outdated == null) {
          continue;
        }

        final MaterializedViewMetadata metadata = materializedViewTable.getMVMetadata();
        Materialization invalidationInfo = null;
        if (outdated) {
          // The MV is outdated, see whether we should consider it for rewriting or not
          boolean ignore;
          if (forceMVContentsUpToDate && !tryIncrementalRebuild) {
            // We will not try partial rewriting for rebuild if incremental rebuild is disabled
            ignore = true;
          } else if (!forceMVContentsUpToDate && !tryIncrementalRewriting) {
            // We will not try partial rewriting for non-rebuild if incremental rewriting is disabled
            ignore = true;
          } else {
            // Obtain additional information if we should try incremental rewriting / rebuild
            // We will not try partial rewriting if there were update/delete/compaction operations on source tables
            invalidationInfo = getMaterializationInvalidationInfo(metadata);
            ignore = invalidationInfo == null || invalidationInfo.isSourceTablesCompacted();
          }
          if (ignore) {
            LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as its contents are outdated");
            continue;
          }
        }

        // It passed the test, load
        HiveRelOptMaterialization relOptMaterialization =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedView(
                materializedViewTable.getDbName(), materializedViewTable.getTableName(), scope);
        if (relOptMaterialization != null) {
          Table cachedMaterializedViewTable = extractTable(relOptMaterialization);
          if (cachedMaterializedViewTable.equals(materializedViewTable)) {
            // It is in the cache and up to date
            if (outdated) {
              // We will rewrite it to include the filters on transaction list
              // so we can produce partial rewritings
              relOptMaterialization = HiveMaterializedViewUtils.augmentMaterializationWithTimeInformation(
                  relOptMaterialization, validTxnsList, metadata.getSnapshot());
            }
            addToMaterializationList(expandGroupingSets, invalidationInfo, relOptMaterialization, result);
            continue;
          }
        }

        // It was not present in the cache (maybe because it was added by another HS2)
        // or it is not up to date. We need to add it
        if (LOG.isDebugEnabled()) {
          LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
              " was not in the cache or it is not supported by specified rewrite algorithm {}", scope);
        }
        HiveRelOptMaterialization hiveRelOptMaterialization =
                HiveMaterializedViewsRegistry.get().createMaterialization(conf, materializedViewTable);
        if (hiveRelOptMaterialization != null && hiveRelOptMaterialization.isSupported(scope)) {
          relOptMaterialization = hiveRelOptMaterialization;
          HiveMaterializedViewsRegistry.get().refreshMaterializedView(conf, null, materializedViewTable);
          if (outdated) {
            // We will rewrite it to include the filters on transaction list
            // so we can produce partial rewritings
            relOptMaterialization = HiveMaterializedViewUtils.augmentMaterializationWithTimeInformation(
                    hiveRelOptMaterialization, validTxnsList, metadata.getSnapshot());
          }
          addToMaterializationList(expandGroupingSets, invalidationInfo, relOptMaterialization, result);
        }
      }
      return result;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void addToMaterializationList(
          boolean expandGroupingSets, Materialization invalidationInfo, HiveRelOptMaterialization relOptMaterialization,
          List<HiveRelOptMaterialization> result) {
    if (expandGroupingSets) {
      List<HiveRelOptMaterialization> hiveRelOptMaterializationList =
              HiveMaterializedViewUtils.deriveGroupingSetsMaterializedViews(relOptMaterialization);
      if (invalidationInfo != null) {
        for (HiveRelOptMaterialization materialization : hiveRelOptMaterializationList) {
          result.add(materialization.updateInvalidation(invalidationInfo));
        }
      } else {
        result.addAll(hiveRelOptMaterializationList);
      }
    } else {
      result.add(invalidationInfo == null ? relOptMaterialization : relOptMaterialization.updateInvalidation(invalidationInfo));
    }
  }

  public List<Table> getAllMaterializedViewObjectsForRewriting() throws HiveException {
    try {
      return getMSC().getAllMaterializedViewObjectsForRewriting()
          .stream()
          .map(Table::new)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get the materialized views from the metastore or from the registry which has the same query definition as the
   * specified sql query text. It is guaranteed that it will always return an up-to-date version wrt metastore.
   * This method filters out outdated Materialized views. It compares the transaction ids of the passed usedTables and
   * the materialized view using the txnMgr.
   * @param tablesUsed List of tables to verify whether materialized view is outdated
   * @param txnMgr Transaction manager to get open transactions affects used tables.
   * @return List of materialized views has matching query definition with querySql
   * @throws HiveException - an exception is thrown during validation or unable to pull transaction ids
   */
  public List<HiveRelOptMaterialization> getMaterializedViewsByAST(
          ASTNode astNode, Set<TableName> tablesUsed, HiveTxnManager txnMgr) throws HiveException {

    List<HiveRelOptMaterialization> materializedViews =
            HiveMaterializedViewsRegistry.get().getRewritingMaterializedViews(astNode);
    if (materializedViews.isEmpty()) {
      return Collections.emptyList();
    }

    try {
      // Final result
      List<HiveRelOptMaterialization> result = new ArrayList<>();
      for (HiveRelOptMaterialization materialization : materializedViews) {
        Table materializedViewTable = extractTable(materialization);
        final Boolean outdated = isOutdatedMaterializedView(materializedViewTable, tablesUsed, false, txnMgr);
        if (outdated == null) {
          LOG.debug("Unable to determine if Materialized view " + materializedViewTable.getFullyQualifiedName() +
                  " contents are outdated. It may uses external tables?");
          continue;
        }

        if (outdated) {
          LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                  " ignored for rewriting as its contents are outdated");
          continue;
        }

        result.add(materialization);
      }
      return result;
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

  public void validateDatabaseExists(String databaseName) throws SemanticException {
    boolean exists;
    try {
      exists = databaseExists(databaseName);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName), e);
    }

    if (!exists) {
      throw new SemanticException(ErrorMsg.DATABASE_NOT_EXISTS.getMsg(databaseName));
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
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_DATABASE);
    try {
      return getMSC().getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_DATABASE, "HS2-cache");
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
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_DATABASE_2);
    try {
      return getMSC().getDatabase(catName, dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_DATABASE_2, "HS2-cache");
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

  private TableSnapshot getTableSnapshot(Table tbl, Long writeId) throws LockException {
    TableSnapshot tableSnapshot = null;
    if ((writeId != null) && (writeId > 0)) {
      ValidWriteIdList writeIds = AcidUtils.getTableValidWriteIdListWithTxnList(
              conf, tbl.getDbName(), tbl.getTableName());
      tableSnapshot = new TableSnapshot(writeId, writeIds.writeToString());
    } else {
      // Make sure we pass in the names, so we can get the correct snapshot for rename table.
      tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, tbl.getDbName(), tbl.getTableName(),
                                                  true);
    }
    return tableSnapshot;
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
   * @param resetStatistics
   *          if true, reset the statistics. If false, do not reset statistics.
   * @param writeId write ID allocated for the current load operation
   * @param stmtId statement ID of the current load statement
   * @param isInsertOverwrite
   * @return Partition object being loaded with data
   */
  public Partition loadPartition(Path loadPath, Table tbl, Map<String, String> partSpec,
                                 LoadFileType loadFileType, boolean inheritTableSpecs,
                                 boolean inheritLocation,
                                 boolean isSkewedStoreAsSubdir,
                                 boolean isSrcLocal, boolean isAcidIUDoperation,
                                 boolean resetStatistics, Long writeId,
                                 int stmtId, boolean isInsertOverwrite, boolean isDirectInsert) throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin("MoveTask", PerfLogger.LOAD_PARTITION);

    // Get the partition object if it already exists
    Partition oldPart = getPartition(tbl, partSpec, false);
    boolean isTxnTable = AcidUtils.isTransactionalTable(tbl);

    // If config is set, table is not temporary and partition being inserted exists, capture
    // the list of files added. For not yet existing partitions (insert overwrite to new partition
    // or dynamic partition inserts), the add partition event will capture the list of files added.
    List<FileStatus> newFiles = null;
    if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML) && !tbl.isTemporary()) {
      newFiles = Collections.synchronizedList(new ArrayList<>());
    }

    Partition newTPart = loadPartitionInternal(loadPath, tbl, partSpec, oldPart,
            loadFileType, inheritTableSpecs,
            inheritLocation, isSkewedStoreAsSubdir, isSrcLocal, isAcidIUDoperation,
            resetStatistics, writeId, stmtId, isInsertOverwrite, isTxnTable, newFiles, isDirectInsert);

    AcidUtils.TableSnapshot tableSnapshot = isTxnTable ? getTableSnapshot(tbl, writeId) : null;
    if (tableSnapshot != null) {
      newTPart.getTPartition().setWriteId(tableSnapshot.getWriteId());
    }

    if (oldPart == null) {
      addPartitionToMetastore(newTPart, resetStatistics, tbl, tableSnapshot);
      // For acid table, add the acid_write event with file list at the time of load itself. But
      // it should be done after partition is created.
      if (isTxnTable && (null != newFiles)) {
        addWriteNotificationLog(tbl, partSpec, newFiles, writeId, null);
      }
    } else {
      try {
        setStatsPropAndAlterPartition(resetStatistics, tbl, newTPart, tableSnapshot);
      } catch (TException e) {
        LOG.error("Error loading partitions", e);
        throw new HiveException(e);
      }
    }

    perfLogger.perfLogEnd("MoveTask", PerfLogger.LOAD_PARTITION);

    return newTPart;
  }

  /**
   * Move all the files from loadPath into Hive. If the partition
   * does not exist - one is created - files in loadPath are moved into Hive. But the
   * directory itself is not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tbl
   *          name of table to be loaded.
   * @param partSpec
   *          defines which partition needs to be loaded
   * @param oldPart
   *          already existing partition object, can be null
   * @param loadFileType
   *          if REPLACE_ALL - replace files in the table,
   *          otherwise add files to table (KEEP_EXISTING, OVERWRITE_EXISTING)
   * @param inheritTableSpecs if true, on [re]creating the partition, take the
   *          location/inputformat/outputformat/serde details from table spec
   * @param inheritLocation
   *          if true, partition path is generated from table
   * @param isSkewedStoreAsSubdir
   *          if true, skewed is stored as sub-directory
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param isAcidIUDoperation
   *          true if this is an ACID operation Insert/Update/Delete operation
   * @param resetStatistics
   *          if true, reset the statistics. Do not reset statistics if false.
   * @param writeId
   *          write ID allocated for the current load operation
   * @param stmtId
   *          statement ID of the current load statement
   * @param isInsertOverwrite
   * @param isTxnTable
   *
   * @return Partition object being loaded with data
   * @throws HiveException
   */
  private Partition loadPartitionInternal(Path loadPath, Table tbl, Map<String, String> partSpec,
                        Partition oldPart, LoadFileType loadFileType, boolean inheritTableSpecs,
                        boolean inheritLocation, boolean isSkewedStoreAsSubdir,
                        boolean isSrcLocal, boolean isAcidIUDoperation, boolean resetStatistics,
                        Long writeId, int stmtId, boolean isInsertOverwrite,
                        boolean isTxnTable, List<FileStatus> newFiles, boolean isDirectInsert) throws HiveException {
    Path tblDataLocationPath =  tbl.getDataLocation();
    boolean isMmTableWrite = AcidUtils.isInsertOnlyTable(tbl.getParameters());
    assert tbl.getPath() != null : "null==getPath() for " + tbl.getTableName();
    boolean isFullAcidTable = AcidUtils.isFullAcidTable(tbl);
    List<FileStatus> newFileStatuses = null;
    try {
      PerfLogger perfLogger = SessionState.getPerfLogger();

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
          FileSystem tblPathFS = tblDataLocationPath.getFileSystem(getConf());
          if (FileUtils.isEqualFileSystemAndSameOzoneBucket(oldPartPathFS, tblPathFS, oldPartPath, tblDataLocationPath)) {
            newPartPath = oldPartPath;
          }
        }
      } else {
        newPartPath = oldPartPath == null
          ? genPartPathFromTable(tbl, partSpec, tblDataLocationPath) : oldPartPath;
      }

      perfLogger.perfLogBegin("MoveTask", PerfLogger.FILE_MOVES);

      // Note: the stats for ACID tables do not have any coordination with either Hive ACID logic
      //       like txn commits, time outs, etc.; nor the lower level sync in metastore pertaining
      //       to ACID updates. So the are not themselves ACID.

      // Note: this assumes both paths are qualified; which they are, currently.
      if (((isMmTableWrite || isDirectInsert || isFullAcidTable) && loadPath.equals(newPartPath)) ||
              (loadFileType == LoadFileType.IGNORE)) {
        // MM insert query or direct insert; move itself is a no-op.
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("not moving " + loadPath + " to " + newPartPath + " (MM = " + isMmTableWrite
              + ", Direct insert = " + isDirectInsert + ")");
        }
        if (newFiles != null) {
          if (!newFiles.isEmpty()) {
            newFileStatuses = new ArrayList<>();
            newFileStatuses.addAll(newFiles);
          } else {
            newFileStatuses = listFilesCreatedByQuery(loadPath, writeId, stmtId);
            newFiles.addAll(newFileStatuses);
          }
        }
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("maybe deleting stuff from " + oldPartPath
              + " (new " + newPartPath + ") for replace");
        }
      } else {
        // Either a non-MM query, or a load into MM table from an external source.
        Path destPath = newPartPath;
        if (isMmTableWrite) {
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
          boolean isSkipTrash = MetaStoreUtils.isSkipTrash(tbl.getParameters());
          boolean needRecycle = !tbl.isTemporary()
              && ReplChangeManager.shouldEnableCm(getDatabase(tbl.getDbName()), tbl.getTTable());
          replaceFiles(tbl.getPath(), loadPath, destPath, oldPartPath, getConf(), isSrcLocal,
              isSkipTrash, newFiles, FileUtils.HIDDEN_FILES_PATH_FILTER, needRecycle, isManaged, isInsertOverwrite);
        } else {
          FileSystem fs = destPath.getFileSystem(conf);
          copyFiles(conf, loadPath, destPath, fs, isSrcLocal, isAcidIUDoperation,
              (loadFileType == LoadFileType.OVERWRITE_EXISTING), newFiles,
              tbl.getNumBuckets() > 0, isFullAcidTable, isManaged, false);
        }
      }
      perfLogger.perfLogEnd("MoveTask", PerfLogger.FILE_MOVES);
      Partition newTPart = oldPart != null ? oldPart : new Partition(tbl, partSpec, newPartPath);
      alterPartitionSpecInMemory(tbl, partSpec, newTPart.getTPartition(), inheritTableSpecs, newPartPath.toString());
      validatePartition(newTPart);

      // If config is set, table is not temporary and partition being inserted exists, capture
      // the list of files added. For not yet existing partitions (insert overwrite to new partition
      // or dynamic partition inserts), the add partition event will capture the list of files added.
      // Generate an insert event only if inserting into an existing partition
      // When inserting into a new partition, the add partition event takes care of insert event
      if ((null != oldPart) && (null != newFiles)) {
        if (isTxnTable) {
          addWriteNotificationLog(tbl, partSpec, newFiles, writeId, null);
        } else {
          fireInsertEvent(tbl, partSpec, (loadFileType == LoadFileType.REPLACE_ALL), newFiles);
        }
      } else {
        LOG.debug("No new files were created, and is not a replace, or we're inserting into a "
                + "partition that does not exist yet. Skipping generating INSERT event.");
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

      // If there is no column stats gather stage present in the plan. So we don't know the accuracy of the stats or
      // auto gather stats is turn off explicitly. We need to reset the stats in both cases.
      if (resetStatistics || !this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
        LOG.debug(
            "Clear partition column statistics by setting basic stats to false for " + newTPart.getCompleteName());
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
        if (newFileStatuses != null && !newFileStatuses.isEmpty()) {
          filesForStats = newFileStatuses;
        } else {
          if (isTxnTable) {
            filesForStats = AcidUtils.getAcidFilesForStats(newTPart.getTable(), newPartPath, conf, null);
          } else {
            filesForStats = HiveStatsUtils.getFileStatusRecurse(newPartPath, -1, newPartPath.getFileSystem(conf));
          }
        }
        if (filesForStats != null) {
          MetaStoreServerUtils.populateQuickStats(filesForStats, newTPart.getParameters());
        } else {
          // The ACID state is probably absent. Warning is logged in the get method.
          MetaStoreServerUtils.clearQuickStats(newTPart.getParameters());
        }
      }
      return newTPart;
    } catch (IOException | MetaException | InvalidOperationException e) {
      LOG.error("Error in loadPartitionInternal", e);
      throw new HiveException(e);
    }
  }

  private void addPartitionToMetastore(Partition newTPart, boolean resetStatistics,
                                       Table tbl, TableSnapshot tableSnapshot) throws HiveException{
    try {
      LOG.debug("Adding new partition " + newTPart.getSpec());
      getSynchronizedMSC().add_partition(newTPart.getTPartition());
    } catch (AlreadyExistsException aee) {
      // With multiple users concurrently issuing insert statements on the same partition has
      // a side effect that some queries may not see a partition at the time when they're issued,
      // but will realize the partition is actually there when it is trying to add such partition
      // to the metastore and thus get AlreadyExistsException, because some earlier query just
      // created it (race condition).
      // For example, imagine such a table is created:
      //  create table T (name char(50)) partitioned by (ds string);
      // and the following two queries are launched at the same time, from different sessions:
      //  insert into table T partition (ds) values ('Bob', 'today'); -- creates the partition 'today'
      //  insert into table T partition (ds) values ('Joe', 'today'); -- will fail with AlreadyExistsException
      // In that case, we want to retry with alterPartition.
      LOG.debug("Caught AlreadyExistsException, trying to alter partition instead");
      try {
        setStatsPropAndAlterPartition(resetStatistics, tbl, newTPart, tableSnapshot);
      } catch (TException e) {
        LOG.error("Error setStatsPropAndAlterPartition", e);
        throw new HiveException(e);
      }
    } catch (Exception e) {
      try {
        final FileSystem newPathFileSystem = newTPart.getPartitionPath().getFileSystem(this.getConf());
        boolean isSkipTrash = MetaStoreUtils.isSkipTrash(tbl.getParameters());
        final FileStatus status = newPathFileSystem.getFileStatus(newTPart.getPartitionPath());
        Hive.trashFiles(newPathFileSystem, new FileStatus[]{status}, this.getConf(), isSkipTrash);
      } catch (IOException io) {
        LOG.error("Could not delete partition directory contents after failed partition creation: ", io);
      }
      LOG.error("Error addPartitionToMetastore", e);
      throw new HiveException(e);
    }
  }

  private void addPartitionsToMetastore(List<Partition> partitions,
                                        boolean resetStatistics, Table tbl,
                                        List<AcidUtils.TableSnapshot> tableSnapshots)
                                        throws HiveException {
    try {
      if (partitions.isEmpty() || tableSnapshots.isEmpty()) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        StringBuffer debugMsg = new StringBuffer("Adding new partitions ");
        partitions.forEach(partition -> debugMsg.append(partition.getSpec() + " "));
        LOG.debug(debugMsg.toString());
      }
      getSynchronizedMSC().add_partitions(partitions.stream().map(Partition::getTPartition)
              .collect(Collectors.toList()));
    } catch(AlreadyExistsException aee) {
      // With multiple users concurrently issuing insert statements on the same partition has
      // a side effect that some queries may not see a partition at the time when they're issued,
      // but will realize the partition is actually there when it is trying to add such partition
      // to the metastore and thus get AlreadyExistsException, because some earlier query just
      // created it (race condition).
      // For example, imagine such a table is created:
      //  create table T (name char(50)) partitioned by (ds string);
      // and the following two queries are launched at the same time, from different sessions:
      //  insert into table T partition (ds) values ('Bob', 'today'); -- creates the partition 'today'
      //  insert into table T partition (ds) values ('Joe', 'today'); -- will fail with AlreadyExistsException
      // In that case, we want to retry with alterPartition.
      LOG.debug("Caught AlreadyExistsException, trying to add partitions one by one.");
      assert partitions.size() == tableSnapshots.size();
      for (int i = 0; i < partitions.size(); i++) {
        addPartitionToMetastore(partitions.get(i), resetStatistics, tbl,
                tableSnapshots.get(i));
      }
    } catch (Exception e) {
      try {
        for (Partition partition : partitions) {
          final FileSystem newPathFileSystem = partition.getPartitionPath().getFileSystem(this.getConf());
          boolean isSkipTrash = MetaStoreUtils.isSkipTrash(tbl.getParameters());
          final FileStatus status = newPathFileSystem.getFileStatus(partition.getPartitionPath());
          Hive.trashFiles(newPathFileSystem, new FileStatus[]{status}, this.getConf(), isSkipTrash);
        }
      } catch (IOException io) {
        LOG.error("Could not delete partition directory contents after failed partition creation: ", io);
      }
      LOG.error("Failed addPartitionsToMetastore", e);
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

  public static void listFilesInsideAcidDirectory(Path acidDir, FileSystem srcFs, List<Path> newFiles, PathFilter filter)
          throws IOException {
    // list out all the files/directory in the path
    FileStatus[] acidFiles = null;
    if (filter != null) {
      acidFiles = srcFs.listStatus(acidDir, filter);
    } else {
      acidFiles = srcFs.listStatus(acidDir);
    }

    if (acidFiles == null) {
      LOG.debug("No files added by this query in: " + acidDir);
      return;
    }
    LOG.debug("Listing files under " + acidDir);
    for (FileStatus acidFile : acidFiles) {
      // need to list out only files, ignore folders.
      if (!acidFile.isDirectory()) {
        newFiles.add(acidFile.getPath());
      } else {
        listFilesInsideAcidDirectory(acidFile.getPath(), srcFs, newFiles, null);
      }
    }
  }

  private List<FileStatus> listFilesCreatedByQuery(Path loadPath, long writeId, int stmtId) throws HiveException {
    try {
      FileSystem srcFs = loadPath.getFileSystem(conf);
      PathFilter filter = new AcidUtils.IdFullPathFiler(writeId, stmtId, loadPath);
      return HdfsUtils.listLocatedFileStatus(srcFs, loadPath, filter, true);
    } catch (FileNotFoundException e) {
      LOG.info("directory does not exist: " + loadPath);
    } catch (IOException e) {
      LOG.error("Error listing files", e);
      throw new HiveException(e);
    }
    return Collections.EMPTY_LIST;
  }

  private void setStatsPropAndAlterPartition(boolean resetStatistics, Table tbl,
                                             Partition newTPart, TableSnapshot tableSnapshot) throws TException {
    EnvironmentContext ec = new EnvironmentContext();
    if (!resetStatistics) {
      ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }
    LOG.debug("Altering existing partition " + newTPart.getSpec());
    getSynchronizedMSC().alter_partition(tbl.getCatName(),
        tbl.getDbName(), tbl.getTableName(), newTPart.getTPartition(), new EnvironmentContext(),
        tableSnapshot == null ? null : tableSnapshot.getValidWriteIdList());
  }

  private void setStatsPropAndAlterPartitions(boolean resetStatistics, Table tbl,
                                              List<Partition> partitions,
                                              AcidUtils.TableSnapshot tableSnapshot)
          throws TException {
    if (partitions.isEmpty() || conf.getBoolVar(ConfVars.HIVESTATSAUTOGATHER)) {
      return;
    }
    EnvironmentContext ec = new EnvironmentContext();
    if (!resetStatistics) {
      ec.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }
    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("Altering existing partitions ");
      partitions.forEach(p -> sb.append(p.getSpec()));
      LOG.debug(sb.toString());
    }

    String validWriteIdList = null;
    long writeId = 0L;
    if (tableSnapshot != null) {
      validWriteIdList = tableSnapshot.getValidWriteIdList();
      writeId = tableSnapshot.getWriteId();
    }
    getSynchronizedMSC().alter_partitions(tbl.getCatName(), tbl.getDbName(), tbl.getTableName(),
            partitions.stream().map(Partition::getTPartition).collect(Collectors.toList()),
            ec, validWriteIdList, writeId);
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
   * Given a source directory name of the load path, load all dynamically generated partitions
   * into the specified table and return a list of strings that represent the dynamic partition
   * paths.
   * @param tbd table descriptor
   * @param numLB number of buckets
   * @param isAcid true if this is an ACID operation
   * @param writeId writeId, can be 0 unless isAcid == true
   * @param stmtId statementId
   * @param resetStatistics if true, reset statistics. Do not reset statistics otherwise.
   * @param operation ACID operation type
   * @param partitionDetailsMap full dynamic partition specification
   * @return partition map details (PartitionSpec and Partition)
   * @throws HiveException
   */
  public Map<Map<String, String>, Partition> loadDynamicPartitions(final LoadTableDesc tbd, final int numLB,
      final boolean isAcid, final long writeId, final int stmtId, final boolean resetStatistics,
      final AcidUtils.Operation operation, Map<Path, PartitionDetails> partitionDetailsMap) throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin("MoveTask", PerfLogger.LOAD_DYNAMIC_PARTITIONS);

    final Path loadPath = tbd.getSourcePath();
    final Table tbl = getTable(tbd.getTable().getTableName());
    final Map<String, String> partSpec = tbd.getPartitionSpec();

    final AtomicInteger partitionsLoaded = new AtomicInteger(0);
    final boolean inPlaceEligible = conf.getLong("fs.trash.interval", 0) <= 0
        && InPlaceUpdate.canRenderInPlace(conf) && !SessionState.getConsole().getIsSilent();
    final PrintStream ps = (inPlaceEligible) ? SessionState.getConsole().getInfoStream() : null;

    final SessionState parentSession = SessionState.get();
    List<Callable<Partition>> tasks = Lists.newLinkedList();

    boolean fetchPartitionInfo = true;
    final boolean scanPartitionsByName =
        HiveConf.getBoolVar(conf, HIVE_LOAD_DYNAMIC_PARTITIONS_SCAN_SPECIFIC_PARTITIONS);

    // ACID table can be a bigger change. Filed HIVE-25817 for an appropriate fix for ACID tables
    // For now, for ACID tables, skip getting all partitions for a table from HMS (since that
    // can degrade performance for large partitioned tables) and instead make getPartition() call
    // for every dynamic partition
    if (scanPartitionsByName && !tbd.isDirectInsert() && !AcidUtils.isTransactionalTable(tbl)) {
      //Fetch only relevant partitions from HMS for checking old partitions
      List<String> partitionNames = new LinkedList<>();
      for(PartitionDetails details : partitionDetailsMap.values()) {
        if (details.fullSpec != null && !details.fullSpec.isEmpty()) {
          partitionNames.add(Warehouse.makeDynamicPartNameNoTrailingSeperator(details.fullSpec));
        }
      }
      List<Partition> partitions = Hive.get().getPartitionsByNames(tbl, partitionNames);
      for(Partition partition : partitions) {
        LOG.debug("HMS partition spec: {}", partition.getSpec());
        partitionDetailsMap.entrySet().parallelStream()
            .filter(entry -> entry.getValue().fullSpec.equals(partition.getSpec()))
            .findAny().ifPresent(entry -> {
          entry.getValue().partition = partition;
          entry.getValue().hasOldPartition = true;
        });
      }
      // no need to fetch partition again in tasks since we have already fetched partitions
      // info in getPartitionsByNames()
      fetchPartitionInfo = false;
    }

    boolean isTxnTable = AcidUtils.isTransactionalTable(tbl);
    AcidUtils.TableSnapshot tableSnapshot = isTxnTable ? getTableSnapshot(tbl, writeId) : null;

    for (Entry<Path, PartitionDetails> entry : partitionDetailsMap.entrySet()) {
      boolean getPartitionFromHms = fetchPartitionInfo;
      tasks.add(() -> {
        PartitionDetails partitionDetails = entry.getValue();
        Map<String, String> fullPartSpec = partitionDetails.fullSpec;
        try {
          SessionState.setCurrentSessionState(parentSession);
          if (getPartitionFromHms) {
            // didn't fetch partition info from HMS. Getting from HMS now.
            Partition existing = getPartition(tbl, fullPartSpec, false);
            if (existing != null) {
              partitionDetails.partition = existing;
              partitionDetails.hasOldPartition = true;
            }
          }
          LOG.info("New loading path = " + entry.getKey() + " withPartSpec " + fullPartSpec);
          Partition oldPartition = partitionDetails.partition;
          List<FileStatus> newFiles = null;
          if (partitionDetails.newFiles != null) {
            // If we already know the files from the direct insert manifest, use them
            newFiles = partitionDetails.newFiles;
          } else if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML) && !tbl.isTemporary()) {
            // Otherwise only collect them, if we are going to fire write notifications
            newFiles = Collections.synchronizedList(new ArrayList<>());
          }
          // load the partition
          Partition partition = loadPartitionInternal(entry.getKey(), tbl,
                  fullPartSpec, oldPartition, tbd.getLoadFileType(), true, false, numLB > 0, false, isAcid,
                  resetStatistics, writeId, stmtId, tbd.isInsertOverwrite(), isTxnTable, newFiles, tbd.isDirectInsert());
          // if the partition already existed before the loading, no need to add it again to the
          // metastore
          if (tableSnapshot != null) {
            partition.getTPartition().setWriteId(tableSnapshot.getWriteId());
          }
          partitionDetails.tableSnapshot = tableSnapshot;
          if (oldPartition == null) {
            partitionDetails.newFiles = newFiles;
            partitionDetails.partition = partition;
          }

          if (inPlaceEligible) {
            synchronized (ps) {
              InPlaceUpdate.rePositionCursor(ps);
              partitionsLoaded.incrementAndGet();
              InPlaceUpdate.reprintLine(ps, "Loaded : " + partitionsLoaded.get() + "/"
                  + partitionDetailsMap.size() + " partitions.");
            }
          }

          return partition;
        } catch (Exception e) {
          LOG.error("Exception when loading partition with parameters "
                  + " partPath=" + entry.getKey() + ", "
                  + " table=" + tbl.getTableName() + ", "
                  + " partSpec=" + fullPartSpec + ", "
                  + " loadFileType=" + tbd.getLoadFileType().toString() + ", "
                  + " listBucketingLevel=" + numLB + ", "
                  + " isAcid=" + isAcid + ", "
                  + " resetStatistics=" + resetStatistics, e);
          throw e;
        } finally {
          // get(conf).getMSC can be called in this task, Close the HMS connection right after use, do not wait for finalizer to close it.
          closeCurrent();
        }
      });
    }

    int poolSize = conf.getInt(ConfVars.HIVE_LOAD_DYNAMIC_PARTITIONS_THREAD_COUNT.varname, 1);
    ExecutorService executor = Executors.newFixedThreadPool(poolSize,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-dynamic-partitionsToAdd-%d").build());

    List<Future<Partition>> futures = Lists.newLinkedList();
    Map<Map<String, String>, Partition> result = Maps.newLinkedHashMap();
    try {
      futures = executor.invokeAll(tasks);
      LOG.info("Number of partitionsToAdd to be added is " + futures.size());
      for (Future<Partition> future : futures) {
        Partition partition = future.get();
        result.put(partition.getSpec(), partition);
      }
      // add new partitions in batch

      addPartitionsToMetastore(
              partitionDetailsMap.entrySet()
                      .stream()
                      .filter(entry -> !entry.getValue().hasOldPartition)
                      .map(entry -> entry.getValue().partition)
                      .collect(Collectors.toList()),
              resetStatistics,
              tbl,
              partitionDetailsMap.entrySet()
                      .stream()
                      .filter(entry -> !entry.getValue().hasOldPartition)
                      .map(entry -> entry.getValue().tableSnapshot)
                      .collect(Collectors.toList()));
      // For acid table, add the acid_write event with file list at the time of load itself. But
      // it should be done after partition is created.

      List<WriteNotificationLogRequest> requestList = new ArrayList<>();
      int maxBatchSize = conf.getIntVar(HIVE_WRITE_NOTIFICATION_MAX_BATCH_SIZE);
      for (Entry<Path, PartitionDetails> entry : partitionDetailsMap.entrySet()) {
        PartitionDetails partitionDetails = entry.getValue();
        if (isTxnTable && partitionDetails.newFiles != null) {
          addWriteNotificationLog(tbl, partitionDetails.fullSpec, partitionDetails.newFiles,
                  writeId, requestList);
          if (requestList != null && requestList.size() >= maxBatchSize) {
            // If the first call returns that the HMS does not supports batching, avoid batching
            // for later requests.
            boolean batchSupported = addWriteNotificationLogInBatch(tbl, requestList);
            if (batchSupported) {
              requestList.clear();
            } else {
              requestList = null;
            }
          }
        }
      }

      if (requestList != null && requestList.size() > 0) {
        addWriteNotificationLogInBatch(tbl, requestList);
      }

      setStatsPropAndAlterPartitions(resetStatistics, tbl,
              partitionDetailsMap.entrySet().stream()
                      .filter(entry -> entry.getValue().hasOldPartition)
                      .map(entry -> entry.getValue().partition)
                      .collect(Collectors.toList()), tableSnapshot);

    } catch (InterruptedException | ExecutionException e) {
      throw new HiveException("Exception when loading " + partitionDetailsMap.size() + " partitions"
              + " in table " + tbl.getTableName()
              + " with loadPath=" + loadPath, e);
    } catch (TException e) {
      LOG.error("Failed loadDynamicPartitions", e);
      throw new HiveException(e);
    } catch (Exception e) {

      StringBuffer logMsg = new StringBuffer();
      logMsg.append("Exception when loading partitionsToAdd with parameters ");
      logMsg.append("partPaths=");
      partitionDetailsMap.keySet().forEach(path -> logMsg.append(path + ", "));
      logMsg.append("table=" + tbl.getTableName() + ", ").
              append("partSpec=" + partSpec + ", ").
              append("loadFileType=" + tbd.getLoadFileType().toString() + ", ").
              append("listBucketingLevel=" + numLB + ", ").
              append("isAcid=" + isAcid + ", ").
              append("resetStatistics=" + resetStatistics);

      LOG.error(logMsg.toString(), e);
      throw e;
    } finally {
      LOG.debug("Cancelling " + futures.size() + " dynamic loading tasks");
      executor.shutdownNow();
    }
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) && HiveConf.getBoolVar(conf, ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION)) {
      throw new HiveException(HiveConf.ConfVars.HIVE_TEST_MODE_FAIL_LOAD_DYNAMIC_PARTITION.name() + "=true");
    }
    try {
      if (isTxnTable) {
        List<String> partNames =
                result.values().stream().map(Partition::getName).collect(Collectors.toList());
        getMSC().addDynamicPartitions(parentSession.getTxnMgr().getCurrentTxnId(), writeId,
                tbl.getDbName(), tbl.getTableName(), partNames,
                AcidUtils.toDataOperationType(operation));
      }
      LOG.info("Loaded " + result.size() + "partitionsToAdd");

      perfLogger.perfLogEnd("MoveTask", PerfLogger.LOAD_DYNAMIC_PARTITIONS);

      return result;
    } catch (TException te) {
      LOG.error("Failed loadDynamicPartitions", te);
      throw new HiveException("Exception updating metastore for acid table "
          + tbd.getTable().getTableName() + " with partitions " + result.values(), te);
    }
  }

  private boolean addWriteNotificationLogInBatch(Table tbl, List<WriteNotificationLogRequest> requestList)
          throws HiveException,MetaException,TException {
    long start = System. currentTimeMillis();
    boolean supported = true;
    WriteNotificationLogBatchRequest rqst = new WriteNotificationLogBatchRequest(tbl.getCatName(), tbl.getDbName(),
            tbl.getTableName(), requestList);
    try {
      get(conf).getSynchronizedMSC().addWriteNotificationLogInBatch(rqst);
    } catch (TApplicationException e) {
      int type = e.getType();
      if (type == TApplicationException.UNKNOWN_METHOD || type == TApplicationException.WRONG_METHOD_NAME) {
        // For older HMS, if the batch API is not supported, fall back to older API.
        LOG.info("addWriteNotificationLogInBatch failed with ", e);
        for (WriteNotificationLogRequest request : requestList) {
          get(conf).getSynchronizedMSC().addWriteNotificationLog(request);
        }
        supported = false;
      } else {
	// Rethrow the exception, so failures are visible. Missing a write notification can be very difficult
	// to debug otherwise.
	throw e;
      }
    }
    long end = System.currentTimeMillis();
    LOG.info("Time taken to add " + requestList.size() + " write notifications: " + ((end - start)/1000F) + " seconds");
    return supported;
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
   * @param isAcidIUDoperation true if this is an ACID based Insert [overwrite]/update/delete
   * @param resetStatistics should reset statistics as part of move.
   * @param writeId write ID allocated for the current load operation
   * @param stmtId statement ID of the current load statement
   */
  public void loadTable(Path loadPath, String tableName, LoadFileType loadFileType, boolean isSrcLocal,
      boolean isSkewedStoreAsSubdir, boolean isAcidIUDoperation, boolean resetStatistics,
      Long writeId, int stmtId, boolean isInsertOverwrite, boolean isDirectInsert) throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin("MoveTask", PerfLogger.LOAD_TABLE);

    List<FileStatus> newFiles = null;
    Table tbl = getTable(tableName);
    assert tbl.getPath() != null : "null==getPath() for " + tbl.getTableName();
    boolean isTxnTable = AcidUtils.isTransactionalTable(tbl);
    boolean isMmTable = AcidUtils.isInsertOnlyTable(tbl);
    boolean isFullAcidTable = AcidUtils.isFullAcidTable(tbl);
    boolean isCompactionTable = AcidUtils.isCompactionTable(tbl.getParameters());

    if (conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML) && !tbl.isTemporary()) {
      newFiles = Collections.synchronizedList(new ArrayList<FileStatus>());
    }

    // Note: this assumes both paths are qualified; which they are, currently.
    if (((isMmTable || isDirectInsert || isFullAcidTable) && loadPath.equals(tbl.getPath())) || (loadFileType == LoadFileType.IGNORE)) {
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
        newFiles.addAll(listFilesCreatedByQuery(loadPath, writeId, stmtId));
      }
    } else {
      // Either a non-MM query, or a load into MM table from an external source.
      Path tblPath = tbl.getPath();
      Path destPath = tblPath;
      if (isMmTable) {
        // We will load into MM directory, and hide previous directories if needed.
        destPath = new Path(destPath, isInsertOverwrite
            ? AcidUtils.baseDir(writeId) : AcidUtils.deltaSubdir(writeId, writeId, stmtId));
      }
      if (!isAcidIUDoperation && isFullAcidTable) {
        destPath = fixFullAcidPathForLoadData(loadFileType, destPath, writeId, stmtId, tbl);
      }
      Utilities.FILE_OP_LOGGER.debug("moving " + loadPath + " to " + tblPath
          + " (replace = " + loadFileType + ")");

      perfLogger.perfLogBegin("MoveTask", PerfLogger.FILE_MOVES);

      boolean isManaged = tbl.getTableType() == TableType.MANAGED_TABLE;

      if (loadFileType == LoadFileType.REPLACE_ALL && !isTxnTable) {
        //for fullAcid we don't want to delete any files even for OVERWRITE see HIVE-14988/HIVE-17361
        boolean isSkipTrash = MetaStoreUtils.isSkipTrash(tbl.getParameters());
        boolean needRecycle = !tbl.isTemporary()
                && ReplChangeManager.shouldEnableCm(getDatabase(tbl.getDbName()), tbl.getTTable());
        replaceFiles(tblPath, loadPath, destPath, tblPath, conf, isSrcLocal, isSkipTrash,
            newFiles, FileUtils.HIDDEN_FILES_PATH_FILTER, needRecycle, isManaged, isInsertOverwrite);
      } else {
        try {
          FileSystem fs = tbl.getDataLocation().getFileSystem(conf);
          copyFiles(conf, loadPath, destPath, fs, isSrcLocal, isAcidIUDoperation,
              loadFileType == LoadFileType.OVERWRITE_EXISTING, newFiles,
              tbl.getNumBuckets() > 0, isFullAcidTable, isManaged, isCompactionTable);
        } catch (IOException e) {
          throw new HiveException("addFiles: filesystem error in check phase", e);
        }
      }
      perfLogger.perfLogEnd("MoveTask", PerfLogger.FILE_MOVES);
    }

    // If there is no column stats gather stage present in the plan. So we don't know the accuracy of the stats or
    // auto gather stats is turn off explicitly. We need to reset the stats in both cases.
    if (resetStatistics || !this.getConf().getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      LOG.debug("Clear table column statistics and set basic statistics to false for " + tbl.getCompleteName());
      StatsSetupConst.setBasicStatsState(tbl.getParameters(), StatsSetupConst.FALSE);
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
      LOG.error("Failed loadTable", e);
      throw new HiveException(e);
    }

    EnvironmentContext environmentContext = null;
    if (!resetStatistics) {
      environmentContext = new EnvironmentContext();
      environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    }

    alterTable(tbl.getCatName(), tbl.getDbName(), tbl.getTableName(), tbl, false, environmentContext,
            true, ((writeId == null) ? 0 : writeId));

    if (AcidUtils.isTransactionalTable(tbl)) {
      addWriteNotificationLog(tbl, null, newFiles, writeId, null);
    } else {
      fireInsertEvent(tbl, null, (loadFileType == LoadFileType.REPLACE_ALL), newFiles);
    }

    perfLogger.perfLogEnd("MoveTask", PerfLogger.LOAD_TABLE);
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
      LOG.error("Failed createPartition", e);
      throw new HiveException(e);
    }
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> addPartitions(
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions, boolean ifNotExists, boolean needResults)
          throws HiveException {
    try {
      return getMSC().add_partitions(partitions, ifNotExists, needResults);
    } catch (Exception e) {
      LOG.error("Failed addPartitions", e);
      throw new HiveException(e);
    }
  }

  public org.apache.hadoop.hive.metastore.api.Partition getPartition(Table t, String dbName, String tableName,
      List<String> params) throws HiveException {
    try {
      GetPartitionRequest req = new GetPartitionRequest();
      req.setDbName(dbName);
      req.setTblName(tableName);
      req.setPartVals(params);
      if (AcidUtils.isTransactionalTable(t)) {
        ValidWriteIdList validWriteIdList = getValidWriteIdList(dbName, tableName);
        req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
        req.setId(t.getTTable().getId());
      }
      GetPartitionResponse res = getMSC().getPartitionRequest(req);
      return res.getPartition();
    } catch (Exception e) {
      LOG.error("Failed getPartition", e);
      throw new HiveException(e);
    }
  }

  public void alterPartitions(String dbName, String tableName,
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions, EnvironmentContext ec, String validWriteIdList,
      long writeId) throws HiveException {
    try {
      getMSC().alter_partitions(dbName, tableName, partitions, ec, validWriteIdList, writeId);
    } catch (Exception e) {
      LOG.error("Failed alterPartitions", e);
      throw new HiveException(e);
    }
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String dbName, String tableName,
      List<String> partitionNames, Table t) throws HiveException {
    try {
      GetPartitionsByNamesRequest req = new GetPartitionsByNamesRequest();
      req.setDb_name(dbName);
      req.setTbl_name(tableName);
      req.setNames(partitionNames);
      return getPartitionsByNames(req, t);
    } catch (Exception e) {
      LOG.error("Failed getPartitionsByNames", e);
      throw new HiveException(e);
    }
  }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(GetPartitionsByNamesRequest req,
      Table table)
        throws HiveException {
    try {
      if (table !=null && AcidUtils.isTransactionalTable(table)) {
        ValidWriteIdList validWriteIdList = getValidWriteIdList(req.getDb_name(), req.getTbl_name());
        req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
        req.setId(table.getTTable().getId());
      }
      return (getMSC().getPartitionsByNames(req)).getPartitions();
    } catch (Exception e) {
      LOG.error("Failed getPartitionsByNames", e);
      throw new HiveException(e);
    }
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
      if ((val == null && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING))
          || (val != null && val.length() == 0)) {
        throw new HiveException("get partition: Value for key "
            + field.getName() + " is null or empty");
      } else if (val != null){
        pvals.add(val);
      }
    }
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      String userName = getUserName();
      tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
          tbl.getTableName(), pvals, userName, getGroupNames());
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given partition
      // key value pairs - thrift cannot handle null return values, hence
      // getPartition() throws NoSuchObjectException to indicate null partition
      tpart = null;
    } catch (Exception e) {
      LOG.error("Failed getPartitionWithAuthInfo", e);
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
            String userName = getUserName();
            tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
              tbl.getTableName(), pvals, userName, getGroupNames());
            alterPartitionSpec(tbl, partSpec, tpart, inheritTableSpecs, partPath);
          } catch (Exception e) {
            if (CheckJDOException.isJDODataStoreException(e)) {
              // Using utility method above, so that JDODataStoreException doesn't
              // have to be used here. This helps avoid adding jdo dependency for
              // hcatalog client uses
              LOG.debug("Caught JDO exception, trying to alter partition instead");
              String userName = getUserName();
              tpart = getSynchronizedMSC().getPartitionWithAuthInfo(tbl.getDbName(),
                tbl.getTableName(), pvals, userName, getGroupNames());
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
      LOG.error("Failed getPartition", e);
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
    alterPartition(tbl.getCatalogName(), tbl.getDbName(), tbl.getTableName(),
        new Partition(tbl, tpart), null, true);
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

  public void addWriteNotificationLog(Table tbl, Map<String, String> partitionSpec,
                                       List<FileStatus> newFiles, Long writeId,
                                       List<WriteNotificationLogRequest> requestList) throws HiveException {
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
      Long txnId = SessionState.get().getTxnMgr().getCurrentTxnId();
      List<String> partitionVals = null;
      if (partitionSpec != null && !partitionSpec.isEmpty()) {
        partitionVals = new ArrayList<>();
        for (FieldSchema fs : tbl.getPartitionKeys()) {
          partitionVals.add(partitionSpec.get(fs.getName()));
        }
      }

      addWriteNotificationLog(conf, tbl, partitionVals, txnId, writeId, newFiles, requestList);
    } catch (IOException | TException e) {
      throw new HiveException(e);
    }
  }

  public static void addWriteNotificationLog(HiveConf conf, Table tbl, List<String> partitionVals,
                                             Long txnId, Long writeId, List<FileStatus> newFiles,
                                             List<WriteNotificationLogRequest> requestList)
          throws IOException, HiveException, TException {
    FileSystem fileSystem = tbl.getDataLocation().getFileSystem(conf);
    InsertEventRequestData insertData = new InsertEventRequestData();
    insertData.setReplace(true);

    WriteNotificationLogRequest rqst = new WriteNotificationLogRequest(txnId, writeId,
            tbl.getDbName(), tbl.getTableName(), insertData);
    addInsertFileInformation(newFiles, fileSystem, insertData);
    rqst.setPartitionVals(partitionVals);

    if (requestList == null) {
      get(conf).getSynchronizedMSC().addWriteNotificationLog(rqst);
    } else {
      requestList.add(rqst);
    }
  }

  /**
   * This method helps callers trigger an INSERT event for DML queries without having to deal with
   * HMS objects. This takes java object types as arguments.
   * @param dbName Name of the hive database this table belongs to.
   * @param tblName Name of the hive table this event is for.
   * @param partitionSpec Map containing key/values for each partition column. Can be null if the event is for a table
   * @param replace boolean to indicate whether the filelist is replacement of existing files. Treated as additions otherwise
   * @param newFiles List of file paths affected (added/replaced) by this DML query. Can be null
   * @throws HiveException if the table or partition does not exist or other internal errors in fetching them
   */
  public void fireInsertEvent(String dbName, String tblName,
      Map<String, String> partitionSpec, boolean replace, List<String> newFiles)
      throws HiveException {
    if (!conf.getBoolVar(ConfVars.FIRE_EVENTS_FOR_DML)) {
      LOG.info("DML Events not enabled. Set " + ConfVars.FIRE_EVENTS_FOR_DML.varname);
      return;
    }
    Table table = getTable(dbName, tblName);
    if (table != null && !table.isTemporary()) {
      List<FileStatus> newFileStatusObject = null;
      String parentDir = null;
      if (newFiles != null && newFiles.size() > 0) {
        newFileStatusObject = new ArrayList<>(newFiles.size());
        if (partitionSpec != null && partitionSpec.size() > 0) {
          // fetch the partition object to determine its location
          Partition part = getPartition(table, partitionSpec, false);
          parentDir = part.getLocation();
        } else {
          // fetch the table location
          parentDir = table.getSd().getLocation();
        }
        for (String fileName: newFiles) {
          FileStatus fStatus = new FileStatus();
          fStatus.setPath(new Path(parentDir, fileName));
          newFileStatusObject.add(fStatus);
        }
      }
      fireInsertEvent(table, partitionSpec, replace, newFileStatusObject);
    }
  }

  private void fireInsertEvent(Table tbl, Map<String, String> partitionSpec, boolean replace, List<FileStatus> newFiles)
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


  private static void addInsertFileInformation(List<FileStatus> newFiles, FileSystem fileSystem,
      InsertEventRequestData insertData) throws IOException {
    LinkedList<Path> directories = null;
    for (FileStatus status : newFiles) {
      if (status.isDirectory()) {
        if (directories == null) {
          directories = new LinkedList<>();
        }
        directories.add(status.getPath());
        continue;
      }
      addInsertNonDirectoryInformation(status.getPath(), fileSystem, insertData);
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

  public boolean dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData)
      throws HiveException {
    return dropPartition(dbName, tableName, partitionValues, PartitionDropOptions.instance().deleteData(deleteData));
  }

  public boolean dropPartition(String dbName, String tableName, List<String> partitionValues,
      PartitionDropOptions options) throws HiveException {
    try {
      return getMSC().dropPartition(dbName, tableName, partitionValues, options);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public List<Partition> dropPartitions(String dbName, String tableName,
      List<Pair<Integer, byte[]>> partitionExpressions,
      PartitionDropOptions dropOptions) throws HiveException {
    try {
      Table table = getTable(dbName, tableName);
      if (!dropOptions.deleteData) {
        AcidUtils.TableSnapshot snapshot = AcidUtils.getTableSnapshot(conf, table, true);
        if (snapshot != null) {
          dropOptions.setWriteId(snapshot.getWriteId());
        }
        long txnId = Optional.ofNullable(SessionState.get())
          .map(ss -> ss.getTxnMgr().getCurrentTxnId()).orElse(0L);
        dropOptions.setTxnId(txnId);
      }
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getMSC().dropPartitions(dbName, tableName,
          partitionExpressions, dropOptions);
      return convertFromMetastore(table, partitions);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max)
      throws HiveException {
    List<String> names = null;
    try {
      names = getMSC().listPartitionNames(dbName, tblName, max);
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given dbName and tblName
      // key value pairs - thrift cannot handle null return values, hence
      // listPartitionNames() throws NoSuchObjectException to indicate null partitions
      return Lists.newArrayList();
    } catch (Exception e) {
      LOG.error("Failed getPartitionNames", e);
      throw new HiveException(e);
    }
    return names;
  }

  public List<String> getPartitionNames(String dbName, String tblName,
      Map<String, String> partSpec, short max) throws HiveException {
    List<String> names = null;
    Table t = getTable(dbName, tblName);
    if (t.getStorageHandler() != null && t.getStorageHandler().alwaysUnpartitioned()) {
      return t.getStorageHandler().getPartitionNames(t, partSpec);
    }

    List<String> pvals = MetaStoreUtils.getPvals(t.getPartCols(), partSpec);

    try {
      GetPartitionNamesPsRequest req = new GetPartitionNamesPsRequest();
      req.setTblName(tblName);
      req.setDbName(dbName);
      req.setPartValues(pvals);
      req.setMaxParts(max);
      if (AcidUtils.isTransactionalTable(t)) {
        ValidWriteIdList validWriteIdList = getValidWriteIdList(dbName, tblName);
        req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
        req.setId(t.getTTable().getId());
      }
      GetPartitionNamesPsResponse res = getMSC().listPartitionNamesRequest(req);
      names = res.getNames();
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given partition spec
      // key value pairs - thrift cannot handle null return values, hence
      // listPartitionNames() throws NoSuchObjectException to indicate null partitions
      return Lists.newArrayList();
    } catch (Exception e) {
      LOG.error("Failed getPartitionNames", e);
      throw new HiveException(e);
    }
    return names;
  }

  public List<String> getPartitionNames(Table tbl, ExprNodeGenericFuncDesc expr, String order,
       short maxParts) throws HiveException {
    List<String> names = null;
    // the exprBytes should not be null by thrift definition
    byte[] exprBytes = {(byte)-1};
    if (expr != null) {
      exprBytes = SerializationUtilities.serializeObjectWithTypeInformation(expr);
    }
    try {
      String defaultPartitionName = HiveConf.getVar(conf, ConfVars.DEFAULT_PARTITION_NAME);
      PartitionsByExprRequest req =
          new PartitionsByExprRequest(tbl.getDbName(), tbl.getTableName(), ByteBuffer.wrap(exprBytes));
      if (defaultPartitionName != null) {
        req.setDefaultPartitionName(defaultPartitionName);
      }
      if (maxParts >= 0) {
        req.setMaxParts(maxParts);
      }
      req.setOrder(order);
      req.setCatName(tbl.getCatalogName());
      if (AcidUtils.isTransactionalTable(tbl)) {
        ValidWriteIdList validWriteIdList = getValidWriteIdList(tbl.getDbName(), tbl.getTableName());
        req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
        req.setId(tbl.getTTable().getId());
      }
      names = getMSC().listPartitionNames(req);

    } catch (NoSuchObjectException nsoe) {
      return Lists.newArrayList();
    } catch (Exception e) {
      LOG.error("Failed getPartitionNames", e);
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has along with auth info
   *
   * @param tbl
   *          object for which partition is needed
   * @return list of partition objects along with auth info
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS);
    try {
      int batchSize= MetastoreConf.getIntVar(Hive.get().getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
      return new ArrayList<>(getAllPartitionsInBatches(tbl, batchSize, DEFAULT_BATCH_DECAYING_FACTOR, MetastoreConf
                      .getIntVar(Hive.get().getConf(), MetastoreConf.ConfVars.GETPARTITIONS_BATCH_MAX_RETRIES),
              null, true, getUserName(), getGroupNames()));
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS, "HS2-cache");
    }
  }

  /**
   * Get all the partitions; unlike {@link #getPartitions(Table)}, does not include auth.
   * @param tbl table for which partitions are needed
   * @return list of partition objects
   */
  public Set<Partition> getAllPartitions(Table tbl) throws HiveException {
    if (!tbl.isPartitioned()) {
      return Sets.newHashSet(new Partition(tbl));
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
    try {
      tParts = getMSC().listPartitions(tbl.getDbName(), tbl.getTableName(), (short)-1);
    } catch (Exception e) {
      LOG.error("Failed getAllPartitionsOf", e);
      throw new HiveException(e);
    }
    Set<Partition> parts = new LinkedHashSet<Partition>(tParts.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
      parts.add(new Partition(tbl, tpart));
    }
    return parts;
  }

  /**
   * Get all the partitions. Do it in batches if batchSize is more than 0 else get it in one call.
   * @param tbl table for which partitions are needed
   * @return list of partition objects
   */
  public Set<Partition> getAllPartitionsOf(Table tbl) throws HiveException {
    int batchSize= MetastoreConf.getIntVar(
            Hive.get().getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    if (batchSize > 0) {
      return getAllPartitionsInBatches(tbl, batchSize, DEFAULT_BATCH_DECAYING_FACTOR, MetastoreConf.getIntVar(
              Hive.get().getConf(), MetastoreConf.ConfVars.GETPARTITIONS_BATCH_MAX_RETRIES), null, false);
    } else {
      return getAllPartitions(tbl);
    }
  }

  public Set<Partition> getAllPartitionsInBatches(Table tbl, int batchSize, int decayingFactor,
       int maxRetries, Map<String, String> partialPartitionSpec, boolean isAuthRequired) throws HiveException {
    return getAllPartitionsInBatches(tbl, batchSize, decayingFactor, maxRetries, partialPartitionSpec, isAuthRequired,
            null, null);
  }

  /**
   * Main method which fetches the partitions in batches
   * @param tbl table for which partitions are needed
   * @param batchSize Number of partitions to be fectehd in one batched call
   * @param decayingFactor the value by which batchSize decays in the next retry in case it faces an exception
   * @param maxRetries Number of retries allowed for this operation
   * @param partialPartitionSpec partialPartitionSpec for the table
   * @param isAuthRequired If auth information is required along with partitions
   * @param userName name of the calling user
   * @param groupNames groups the call
   * @return list of partition objects
   */
  public Set<Partition> getAllPartitionsInBatches(Table tbl, int batchSize, int decayingFactor,
       int maxRetries, Map<String, String> partialPartitionSpec, boolean isAuthRequired,
       String userName, List<String> groupNames) throws HiveException {
    if (!tbl.isPartitioned()) {
      return Sets.newHashSet(new Partition(tbl));
    }
    Set<Partition> result = new LinkedHashSet<>();
    RetryUtilities.ExponentiallyDecayingBatchWork batchTask = new RetryUtilities
            .ExponentiallyDecayingBatchWork<Void>(batchSize, decayingFactor, maxRetries) {
      @Override
      public Void execute(int size) throws HiveException {
        result.clear();
        PartitionIterable partitionIterable = new PartitionIterable(Hive.get(), tbl, partialPartitionSpec, size,
            isAuthRequired, userName, groupNames);
        partitionIterable.forEach(result::add);
        return null;
      }
    };
    try {
      batchTask.run();
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return result;
  }

  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec,
       short limit) throws HiveException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS_2);
    try {
      // TODO: Implement Batching when limit is >=0
      if (limit >= 0) {
        return getPartitionsWithAuth(tbl, partialPartSpec, limit);
      } else {
        int batchSize = MetastoreConf.getIntVar(Hive.get().getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
        return new ArrayList<>(getAllPartitionsInBatches(tbl, batchSize, DEFAULT_BATCH_DECAYING_FACTOR,
                MetastoreConf.getIntVar(Hive.get().getConf(), MetastoreConf.ConfVars.GETPARTITIONS_BATCH_MAX_RETRIES),
                partialPartSpec, true, getUserName(), getGroupNames()));
      }
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS_2, "HS2-cache");
    }
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
  private List<Partition> getPartitionsWithAuth(Table tbl, Map<String, String> partialPartSpec,
                                                short limit)
          throws HiveException {
    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }

    List<String> partialPvals = MetaStoreUtils.getPvals(tbl.getPartCols(), partialPartSpec);

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = null;
    try {
      String userName = getUserName();
      partitions = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
              partialPvals, limit, userName, getGroupNames());
    } catch (Exception e) {
      throw new HiveException(e);
    }

    List<Partition> qlPartitions = new ArrayList<Partition>();
    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      qlPartitions.add(new Partition(tbl, p));
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
    return getPartitionsByNames(tbl, partNames, false);
  }

  /**
   * Get all partitions of the table that matches the list of given partition names.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partNames
   *          list of partition names
   * @param getColStats
   *          if true, Partition object includes column statistics for that partition.
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl, List<String> partNames, boolean getColStats)
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
        GetPartitionsByNamesRequest req = new GetPartitionsByNamesRequest();
        req.setDb_name(tbl.getDbName());
        req.setTbl_name(tbl.getTableName());
        req.setNames(partNames.subList(i*batchSize, (i+1)*batchSize));
        req.setGet_col_stats(false);
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts = getPartitionsByNames(req, tbl);

        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }

      if (nParts > nBatches * batchSize) {
        String validWriteIdList = null;
        Long tableId = null;
        if (AcidUtils.isTransactionalTable(tbl)) {
          ValidWriteIdList vWriteIdList = getValidWriteIdList(tbl.getDbName(), tbl.getTableName());
          validWriteIdList = vWriteIdList != null ? vWriteIdList.toString() : null;
          tableId = tbl.getTTable().getId();
        }
        GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(tbl.getDbName(), tbl.getTableName(),
            partNames.subList(nBatches*batchSize, nParts), getColStats, Constants.HIVE_ENGINE, validWriteIdList,
            tableId);
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
            getMSC().getPartitionsByNames(req).getPartitions();
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

  public List<Partition> getPartitionsAuthByNames(Table tbl, List<String> partNames, String userName,
      List<String> groupNames) throws HiveException {
    if (!tbl.isPartitioned()) {
      throw new HiveException(ErrorMsg.TABLE_NOT_PARTITIONED, tbl.getTableName());
    }
    GetPartitionsPsWithAuthRequest req = new GetPartitionsPsWithAuthRequest();
    req.setTblName(tbl.getTableName());
    req.setDbName(tbl.getDbName());
    req.setUserName(userName);
    req.setGroupNames(groupNames);
    req.setPartNames(partNames);
    if (AcidUtils.isTransactionalTable(tbl)) {
      ValidWriteIdList validWriteIdList = getValidWriteIdList(tbl.getDbName(), tbl.getTableName());
      req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
      req.setId(tbl.getTTable().getId());
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
    try {
      GetPartitionsPsWithAuthResponse res = getMSC().listPartitionsWithAuthInfoRequest(req);
      tParts = res.getPartitions();
    } catch (Exception e) {
      throw new HiveException(e);
    }
    List<Partition> parts = new ArrayList<>(tParts.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
      parts.add(new Partition(tbl, tpart));
    }
    return parts;
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
      return Collections.emptyList();
    }

    List<Partition> results = new ArrayList<Partition>(partitions.size());
    for (org.apache.hadoop.hive.metastore.api.Partition tPart : partitions) {
      results.add(new Partition(tbl, tPart));
    }
    return results;
  }

  // This method converts PartitionSpec to Partiton.
  // This is required because listPartitionsSpecByExpr return set of PartitionSpec but hive
  // require Partition
  private static List<Partition> convertFromPartSpec(Iterator<PartitionSpec> iterator, Table tbl)
      throws HiveException, TException {
    if(!iterator.hasNext()) {
      return Collections.emptyList();
    }
    List<Partition> results = new ArrayList<>();

    while (iterator.hasNext()) {
      PartitionSpec partitionSpec = iterator.next();
      if (partitionSpec.getPartitionList() != null) {
        // partitions outside table location
        Iterator<org.apache.hadoop.hive.metastore.api.Partition> externalPartItr =
            partitionSpec.getPartitionList().getPartitions().iterator();
        while(externalPartItr.hasNext()) {
          org.apache.hadoop.hive.metastore.api.Partition msPart =
              externalPartItr.next();
          results.add(new Partition(tbl, msPart));
        }
      } else {
        // partitions within table location
        for(PartitionWithoutSD partitionWithoutSD:partitionSpec.getSharedSDPartitionSpec().getPartitions()) {
          org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
          part.setTableName(partitionSpec.getTableName());
          part.setDbName(partitionSpec.getDbName());
          part.setCatName(partitionSpec.getCatName());
          part.setCreateTime(partitionWithoutSD.getCreateTime());
          part.setLastAccessTime(partitionWithoutSD.getLastAccessTime());
          part.setParameters(partitionWithoutSD.getParameters());
          part.setPrivileges(partitionWithoutSD.getPrivileges());
          part.setSd(partitionSpec.getSharedSDPartitionSpec().getSd().deepCopy());
          String partitionLocation = null;
          if(partitionWithoutSD.getRelativePath() == null
              || partitionWithoutSD.getRelativePath().isEmpty()) {
            if (tbl.getDataLocation() != null) {
              Path partPath = new Path(tbl.getDataLocation(),
                  Warehouse.makePartName(tbl.getPartCols(),
                      partitionWithoutSD.getValues()));
              partitionLocation = partPath.toString();
            }
          } else {
            partitionLocation = tbl.getSd().getLocation();
            partitionLocation += partitionWithoutSD.getRelativePath();
          }
          part.getSd().setLocation(partitionLocation);
          part.setValues(partitionWithoutSD.getValues());
          part.setWriteId(partitionSpec.getWriteId());
          Partition hivePart = new Partition(tbl, part);
          results.add(hivePart);
        }
      }
    }
    return results;
  }

  /**
   * Get a list of Partitions by expr.
   * @param tbl The table containing the partitions.
   * @param expr A serialized expression for partition predicates.
   * @param conf Hive config.
   * @param partitions the resulting list of partitions
   * @return whether the resulting list contains partitions which may or may not match the expr
   */
  public boolean getPartitionsByExpr(Table tbl, ExprNodeDesc expr, HiveConf conf,
      List<Partition> partitions) throws HiveException, TException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS_BY_EXPR);
    try {
      Preconditions.checkNotNull(partitions);
      String defaultPartitionName = HiveConf.getVar(conf, ConfVars.DEFAULT_PARTITION_NAME);
      if (tbl.getStorageHandler() != null && tbl.getStorageHandler().alwaysUnpartitioned()) {
        partitions.addAll(tbl.getStorageHandler().getPartitionsByExpr(tbl, expr));
        return false;
      } else {
        byte[] exprBytes = SerializationUtilities.serializeObjectWithTypeInformation(expr);
        List<org.apache.hadoop.hive.metastore.api.PartitionSpec> msParts =
                new ArrayList<>();
        ValidWriteIdList validWriteIdList = null;

        PartitionsByExprRequest req = buildPartitionByExprRequest(tbl, exprBytes, defaultPartitionName, conf,
                null);

        if (AcidUtils.isTransactionalTable(tbl)) {
          validWriteIdList = getValidWriteIdList(tbl.getDbName(), tbl.getTableName());
          req.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.toString() : null);
          req.setId(tbl.getTTable().getId());
        }

        boolean hasUnknownParts = getMSC().listPartitionsSpecByExpr(req, msParts);
        partitions.addAll(convertFromPartSpec(msParts.iterator(), tbl));

        return hasUnknownParts;
      }
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_PARTITIONS_BY_EXPR, "HS2-cache");
    }
  }

  private PartitionsByExprRequest buildPartitionByExprRequest(Table tbl, byte[] exprBytes, String defaultPartitionName,
                                                              HiveConf conf, String validWriteIdList) {
    PartitionsByExprRequest req = new PartitionsByExprRequest(tbl.getDbName(), tbl.getTableName(),
            ByteBuffer.wrap(exprBytes));
    if (defaultPartitionName != null) {
      req.setDefaultPartitionName(defaultPartitionName);
    }
    req.setCatName(getDefaultCatalog(conf));
    req.setValidWriteIdList(validWriteIdList);
    req.setId(tbl.getTTable().getId());

    return req;
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
      LOG.error("Failed validatePartitionNameCharacters", e);
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
            final List<Path> newFiles, boolean acidRename, boolean isManaged,
            boolean isCompactionTable) throws HiveException {

    try {
      FileStatus fullDestStatus = destFs.getFileStatus(destf);
      if (!fullDestStatus.isDirectory()) {
        throw new HiveException(destf + " is not a directory.");
      }
    } catch (IOException e1) {
      throw new HiveException(e1);
    }

    final List<Future<Pair<Path, Path>>> futures = new LinkedList<>();
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
    FileStatus[] files;
    for (FileStatus src : srcs) {
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

      if (isCompactionTable && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_WRITE_ACID_VERSION_FILE)) {
        try {
          AcidUtils.OrcAcidVersion.writeVersionFile(destf, destFs);
        } catch (IOException e) {
          if (null != pool) {
            pool.shutdownNow();
          }
          throw new HiveException(e);
        }
      }

      final SessionState parentSession = SessionState.get();
      // Sort the files
      Arrays.sort(files);
      for (final FileStatus srcFile : files) {
        final Path srcP = srcFile.getPath();
        final boolean needToCopy = needToCopy(conf, srcP, destf, srcFs, destFs, configuredOwner, isManaged);

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
          futures.add(pool.submit(new Callable<Pair<Path, Path>>() {
            @Override
            public Pair<Path, Path> call() throws HiveException {
              SessionState.setCurrentSessionState(parentSession);

              try {
                Path destPath =
                    mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite, isRenameAllowed, finalTaskId);

                if (null != newFiles) {
                  newFiles.add(destPath);
                }
                return Pair.of(srcP, destPath);
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
      for (Future<Pair<Path, Path>> future : futures) {
        try {
          Pair<Path, Path> pair = future.get();
          LOG.debug("Moved src: {}, to dest: {}", pair.getLeft().toString(), pair.getRight().toString());
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
      if (!FileUtils.copy(sourceFs, sourcePath, destFs, destFilePath,
          false,   // delete source
          false,  // overwrite destination
          conf,
          new DataCopyStatistics())) {
        LOG.error("Copy failed for source: " + sourcePath + " to destination: " + destFilePath);
        throw new IOException("File copy failed.");
      }

      // Source file delete may fail because of permission issue as executing user might not
      // have permission to delete the files in the source path. Ignore this failure.
      try {
        if (!sourceFs.delete(sourcePath, true)) {
          LOG.warn("Delete source failed for source: " + sourcePath + " during copy to destination: " + destFilePath);
        }
      } catch (Exception e) {
        LOG.warn("Delete source failed for source: " + sourcePath + " during copy to destination: " + destFilePath, e);
      }
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
      getSynchronizedMSC().recycleDirToCmPath(request);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static void deleteAndRename(FileSystem destFs, Path destFile, FileStatus srcStatus, Path destPath)
          throws IOException {
    try {
      // rename cannot overwrite non empty destination directory, so deleting the destination before renaming.
      destFs.delete(destFile);
      LOG.info("Deleted destination file" + destFile.toUri());
    } catch (FileNotFoundException e) {
      // no worries
    }
    if(!destFs.rename(srcStatus.getPath(), destFile)) {
      throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest:"
              + destPath + " returned false");
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
      final SessionState parentSession = SessionState.get();
      if (isSrcLocal) {
        // For local src file, copy to hdfs
        destFs.copyFromLocalFile(srcf, destf);
        return true;
      } else {
        if (needToCopy(conf, srcf, destf, srcFs, destFs, configuredOwner, isManaged)) {
          //copy if across file system or encryption zones.
          LOG.debug("Copying source " + srcf + " to " + destf + " because HDFS encryption zones are different.");
          return FileUtils.copy(srcf.getFileSystem(conf), srcf, destf.getFileSystem(conf), destf,
              true,    // delete source
              replace, // overwrite destination
              conf,
              new DataCopyStatistics());
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
                deleteAndRename(destFs, destFile, srcStatus, destf);
              } else {
                futures.add(pool.submit(new Callable<Void>() {
                  @Override
                  public Void call() throws HiveException {
                    SessionState.setCurrentSessionState(parentSession);
                    try {
                      deleteAndRename(destFs, destFile, srcStatus, destf);
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
  static private boolean needToCopy(final HiveConf conf, Path srcf, Path destf, FileSystem srcFs,
                                      FileSystem destFs, String configuredOwner, boolean isManaged) throws HiveException {
    //Check if different FileSystems
    if (!FileUtils.isEqualFileSystemAndSameOzoneBucket(srcFs, destFs, srcf, destf)) {
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
          UserGroupInformation proxyUser = null;
          try {
            proxyUser = FileUtils.getProxyUser(configuredOwner);
            FileSystem fsAsUser = FileUtils.getFsAsUser(srcFs, proxyUser);
            if (!(isOwned && FileUtils.isActionPermittedForFileHierarchy(srcFs, srcs, configuredOwner, FsAction.WRITE,
                false, fsAsUser))) {
              return true;
            }
          }
          finally {
            FileUtils.closeFs(proxyUser);
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

    // if Encryption not enabled, no copy needed
    if (!DFSUtilClient.isHDFSEncryptionEnabled(conf)) {
      return false;
    }
    //Check if different encryption zones
    HadoopShims.HdfsEncryptionShim srcHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(srcFs, conf);
    HadoopShims.HdfsEncryptionShim destHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(destFs, conf);
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
   * @param newFilesStatus if this is non-null, a list of files that were created as a result of this
   *                 move will be returned.
   * @param isManaged if table is managed.
   * @param isCompactionTable is table used in query-based compaction
   * @throws HiveException
   */
  static protected void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs,
      boolean isSrcLocal, boolean isAcidIUD, boolean isOverwrite, List<FileStatus> newFilesStatus, boolean isBucketed,
      boolean isFullAcidTable, boolean isManaged, boolean isCompactionTable) throws HiveException {
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
      LOG.error("addFiles: filesystem error in check phase", e);
      throw new HiveException("addFiles: filesystem error in check phase. " + e.getMessage(), e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }

    List<Path> newFiles = null;
    if (newFilesStatus != null) {
      newFiles = Collections.synchronizedList(new ArrayList<Path>());
    }

    // If we're moving files around for an ACID write then the rules and paths are all different.
    // You can blame this on Owen.
    if (isAcidIUD) {
      moveAcidFiles(srcFs, srcs, destf, newFiles, conf);
    } else {
      // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
      // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
      // The extension is only maintained for files which are compressed.
      copyFiles(conf, fs, srcs, srcFs, destf, isSrcLocal, isOverwrite,
              newFiles, isFullAcidTable && !isBucketed, isManaged, isCompactionTable);
    }

    if (newFilesStatus != null) {
      for (Path filePath : newFiles) {
        try {
          newFilesStatus.add(fs.getFileStatus(filePath));
        } catch (Exception e) {
          LOG.error("Failed to get getFileStatus", e);
          throw new HiveException(e.getMessage());
        }
      }
    }
  }

  public static void moveAcidFiles(FileSystem fs, FileStatus[] stats, Path dst,
                                    List<Path> newFiles, HiveConf conf) throws HiveException {
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
                fs, dst, origBucketPath, createdDeltaDirs, newFiles, conf);
        moveAcidFiles(AcidUtils.DELETE_DELTA_PREFIX, AcidUtils.deleteEventDeltaDirFilter,
                fs, dst,origBucketPath, createdDeltaDirs, newFiles, conf);
        moveAcidFiles(AcidUtils.BASE_PREFIX, AcidUtils.baseFileFilter,//for Insert Overwrite
                fs, dst, origBucketPath, createdDeltaDirs, newFiles, conf);
      }
    }
  }

  private static void moveAcidFiles(String deltaFileType, PathFilter pathFilter, FileSystem fs,
                                    Path dst, Path origBucketPath, Set<Path> createdDeltaDirs,
                                    List<Path> newFiles, HiveConf conf) throws HiveException {

    try{
      LOG.debug("Acid move looking for " + deltaFileType + " files in bucket " + origBucketPath);

      FileStatus[] deltaStats = null;
      try {
        deltaStats = fs.listStatus(origBucketPath, pathFilter);
      } catch (IOException e) {
        throw new HiveException("Unable to look for " + deltaFileType + " files in original bucket " +
                origBucketPath.toUri().toString(), e);
      }
      LOG.debug("Acid move found " + deltaStats.length + " " + deltaFileType + " files");

      List<Future<Void>> futures = new LinkedList<>();
      final ExecutorService pool = conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
              Executors.newFixedThreadPool(conf.getInt(ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
                      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Acid-Files-Thread-%d").build()) : null;

      Set<Path> createdDeltaDirsSync = Collections.synchronizedSet(createdDeltaDirs);
      
      for (FileStatus deltaStat : deltaStats) {

        if (null == pool) {
          moveAcidFilesForDelta(deltaFileType, fs, dst, createdDeltaDirsSync, newFiles, deltaStat);
        } else {
          futures.add(pool.submit(new Callable<Void>() {
            @Override
            public Void call() throws HiveException {
              try {
                moveAcidFilesForDelta(deltaFileType, fs, dst, createdDeltaDirsSync, newFiles, deltaStat);
              } catch (Exception e) {
                final String poolMsg =
                        "Unable to move source " + deltaStat.getPath().getName() + " to destination " + dst.getName();
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
          } catch (InterruptedException | ExecutionException e) {
            pool.shutdownNow();
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            }
            if (e.getCause() instanceof HiveException) {
              throw (HiveException) e.getCause();
            }
            throw handlePoolException(pool, e);
          }
        }
      }
    }
    catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  private static void moveAcidFilesForDelta(String deltaFileType, FileSystem fs,
                                            Path dst, Set<Path> createdDeltaDirs,
                                            List<Path> newFiles, FileStatus deltaStat) throws HiveException {

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
            try {
              fs.rename(AcidUtils.OrcAcidVersion.getVersionFilePath(deltaStat.getPath()),
                      AcidUtils.OrcAcidVersion.getVersionFilePath(deltaDest));
            } catch (FileNotFoundException fnf) {
              // There might be no side file. Skip in this case.
            }
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
        LOG.info("Moving bucket " + bucketSrc.toUri().toString() + " to " +
                bucketDest.toUri().toString());
        try {
          fs.rename(bucketSrc, bucketDest);
          if (newFiles != null) {
            newFiles.add(bucketDest);
          }
        } catch (Exception e) {
          throw getHiveException(e, "Unable to move source " + bucketSrc + " to destination " + bucketDest);
        }
      }
    } catch (IOException e) {
      throw new HiveException("Error moving acid files " + e.getMessage(), e);
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
  private void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
          boolean isSrcLocal, boolean purge, List<FileStatus> newFiles, PathFilter deletePathFilter,
      boolean isNeedRecycle, boolean isManaged, boolean isInsertOverwrite) throws HiveException {
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

      // For insert/load overwrite cases, where external.table.purge is disabled for the table, there may be stale
      // partitions present in the table location after Alter table drop partition operation. In such cases, oldPath will be
      // null, since those partitions will not be present in metastore. Added below check to clean up those stale partitions.
      if (oldPath == null && isInsertOverwrite) {
        deleteOldPathForReplace(destf, destf, conf, purge, deletePathFilter, isNeedRecycle);
      }

      // the extra check is required to make ALTER TABLE ... CONCATENATE work
      if (oldPath != null && (srcs != null || isInsertOverwrite)) {
        deleteOldPathForReplace(destf, oldPath, conf, purge, deletePathFilter, isNeedRecycle);
      }

      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
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
        if (newFiles != null) {
          newFiles.addAll(HdfsUtils.listLocatedFileStatus(destFs, destf, null, true));
        }
      } else {
        final Map<Future<Boolean>, Path> moveFutures = Maps.newLinkedHashMapWithExpectedSize(srcs.length);
        final int moveFilesThreadCount = HiveConf.getIntVar(conf, ConfVars.HIVE_MOVE_FILES_THREAD_COUNT);
        final ExecutorService pool = moveFilesThreadCount > 0
            ? Executors.newFixedThreadPool(
                moveFilesThreadCount,
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Replace-Thread-%d").build())
            : MoreExecutors.newDirectExecutorService();
        final SessionState parentSession = SessionState.get();
        // its either a file or glob
        for (FileStatus src : srcs) {
          Path destFile = new Path(destf, src.getPath().getName());
          moveFutures.put(
              pool.submit(
                  new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                      SessionState.setCurrentSessionState(parentSession);
                      return moveFile(
                          conf, src.getPath(), destFile, true, isSrcLocal, isManaged);
                    }
                  }),
              destFile);
        }

        pool.shutdown();
        for (Map.Entry<Future<Boolean>, Path> moveFuture : moveFutures.entrySet()) {
          boolean moveFailed;
          try {
            moveFailed = !moveFuture.getKey().get();
          } catch (InterruptedException | ExecutionException e) {
            pool.shutdownNow();
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            }
            if (e.getCause() instanceof HiveException) {
              throw (HiveException) e.getCause();
            }
            throw handlePoolException(pool, e);
          }
          if (moveFailed) {
            throw new IOException("Error moving: " + srcf + " into: " + destf);
          }

          // Add file paths of the files that will be moved to the destination if the caller needs it
          if (null != newFiles) {
            newFiles.add(destFs.getFileStatus(moveFuture.getValue()));
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
      if (isOldPathUnderDestf && oldFs.exists(oldPath)) {
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


  public void cleanUpOneDirectoryForReplace(Path path, FileSystem fs,
      PathFilter pathFilter, HiveConf conf, boolean purge, boolean isNeedRecycle) throws IOException, HiveException {
    if (isNeedRecycle && conf.getBoolVar(HiveConf.ConfVars.REPL_CM_ENABLED)) {
      recycleDirToCmPath(path, purge);
    }
    if (!fs.exists(path)) {
      return;
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
      LOG.error("Failed exchangeTablePartitions", ex);
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
      LOG.error("Failed createStorageHandler", ex);
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
   * Sets the metastore client for the current thread
   * @throws MetaException
   */
  @VisibleForTesting
  public synchronized void setMSC(IMetaStoreClient client)
      throws MetaException {
    metaStoreClient = client;
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
      String metaStoreUris = conf.getVar(HiveConf.ConfVars.METASTORE_URIS);
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

  public static List<FieldSchema> getFieldsFromDeserializer(String name, Deserializer serde, Configuration conf)
      throws HiveException {
    try {
      return HiveMetaStoreUtils.getFieldsFromDeserializer(name, serde, conf);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. " + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde." + e.getMessage(), e);
    }
  }

  public boolean setPartitionColumnStatistics(
      SetPartitionsStatsRequest request) throws HiveException {
    try {
      ColumnStatistics colStat = request.getColStats().get(0);
      ColumnStatisticsDesc statsDesc = colStat.getStatsDesc();

      // In case of replication, the request already has valid writeId and valid transaction id
      // list obtained from the source. Just use it.
      if (request.getWriteId() <= 0 || request.getValidWriteIdList() == null) {
        Table tbl = getTable(statsDesc.getDbName(), statsDesc.getTableName());
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl, true);
        request.setValidWriteIdList(tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null);
        request.setWriteId(tableSnapshot != null ? tableSnapshot.getWriteId() : 0);
      }

      return getMSC().setPartitionColumnStatistics(request);
    } catch (Exception e) {
      LOG.debug("Failed setPartitionColumnStatistics", e);
      throw new HiveException(e);
    }
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(
      String dbName, String tableName, List<String> colNames, boolean checkTransactional)
      throws HiveException {

    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_TABLE_COLUMN_STATS);
    List<ColumnStatisticsObj> retv = null;
    try {
      if (checkTransactional) {
        Table tbl = getTable(dbName, tableName);
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
        retv = getMSC().getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE,
            tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null);
      } else {
        retv = getMSC().getTableColumnStatistics(dbName, tableName, colNames, Constants.HIVE_ENGINE);
      }

      return retv;
    } catch (Exception e) {
      LOG.debug("Failed getTableColumnStatistics", e);
      throw new HiveException(e);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_TABLE_COLUMN_STATS, "HS2-cache");
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
          dbName, tableName, partNames, colNames, Constants.HIVE_ENGINE, writeIdList);
    } catch (Exception e) {
      LOG.debug("Failed getPartitionColumnStatistics", e);
      throw new HiveException(e);
    }
  }

  public AggrStats getAggrColStatsFor(String dbName, String tblName,
     List<String> colNames, List<String> partName, boolean checkTransactional) {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_AGGR_COL_STATS);
    String writeIdList = null;
    try {
      if (checkTransactional) {
        Table tbl = getTable(dbName, tblName);
        AcidUtils.TableSnapshot tableSnapshot = AcidUtils.getTableSnapshot(conf, tbl);
        writeIdList = tableSnapshot != null ? tableSnapshot.getValidWriteIdList() : null;
      }
      AggrStats result = getMSC().getAggrColStatsFor(dbName, tblName, colNames, partName, Constants.HIVE_ENGINE,
              writeIdList);

      return result;
    } catch (Exception e) {
      LOG.debug("Failed getAggrColStatsFor", e);
      return new AggrStats(new ArrayList<ColumnStatisticsObj>(),0);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_AGGR_COL_STATS, "HS2-cache");
    }
  }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().deleteTableColumnStatistics(dbName, tableName, colName, Constants.HIVE_ENGINE);
    } catch(Exception e) {
      LOG.debug("Failed deleteTableColumnStatistics", e);
      throw new HiveException(e);
    }
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws HiveException {
      try {
        return getMSC().deletePartitionColumnStatistics(dbName, tableName, partName, colName, Constants.HIVE_ENGINE);
      } catch(Exception e) {
        LOG.debug("Failed deletePartitionColumnStatistics", e);
        throw new HiveException(e);
      }
    }

  public void updateTransactionalStatistics(UpdateTransactionalStatsRequest req) throws HiveException {
    try {
      getMSC().updateTransactionalStatistics(req);
    } catch(Exception e) {
      LOG.debug("Failed updateTransactionalStatistics", e);
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
      LOG.error("Failed getDelegationToken", e);
      throw new HiveException(e);
    }
  }

  public void cancelDelegationToken(String tokenStrForm)
    throws HiveException {
    try {
      getMSC().cancelDelegationToken(tokenStrForm);
    }  catch(Exception e) {
      LOG.error("Failed cancelDelegationToken", e);
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
   * @deprecated use {@link #compact(CompactionRequest)}
   */
  @Deprecated
  public CompactionResponse compact2(String dbname, String tableName, String partName, String compactType,
                                     Map<String, String> tblproperties)
      throws HiveException {
      CompactionType cr;
      if ("major".equalsIgnoreCase(compactType)) {
        cr = CompactionType.MAJOR;
      } else if ("minor".equalsIgnoreCase(compactType)) {
        cr = CompactionType.MINOR;
      } else {
        throw new RuntimeException("Unknown compaction type " + compactType);
      }
      CompactionRequest request = new CompactionRequest(dbname, tableName, cr);
      request.setPartitionname(partName);
      request.setProperties(tblproperties);
      return compact(request);
  }

  /**
   * Enqueue a compaction request.  Only 1 compaction for a given resource (db/table/partSpec) can
   * be scheduled/running at any given time.
   * @param request The {@link CompactionRequest} object containing the details required to enqueue
   *                a compaction request.
   * @throws HiveException
   */
  public CompactionResponse compact(CompactionRequest request)
      throws HiveException {
    try {
      return getMSC().compact2(request);
    } catch (Exception e) {
      LOG.error("Failed compact3", e);
      throw new HiveException(e);
    }
  }

  public ShowCompactResponse showCompactions() throws HiveException {
    try {
      return getMSC().showCompactions();
    } catch (Exception e) {
      LOG.error("Failed showCompactions", e);
      throw new HiveException(e);
    }
  }

  public ShowCompactResponse showCompactions(ShowCompactRequest request) throws HiveException {
    try {
      return getMSC().showCompactions(request);
    } catch (Exception e) {
      LOG.error("Failed showCompactions", e);
      throw new HiveException(e);
    }
  }

  public GetOpenTxnsInfoResponse showTransactions() throws HiveException {
    try {
      return getMSC().showTxns();
    } catch (Exception e) {
      LOG.error("Failed showTransactions", e);
      throw new HiveException(e);
    }
  }

  public void abortTransactions(List<Long> txnids, long errorCode) throws HiveException {
    AbortTxnsRequest abortTxnsRequest = new AbortTxnsRequest(txnids);
    abortTxnsRequest.setErrorCode(errorCode);
    try {
      getMSC().abortTxns(abortTxnsRequest);
    } catch (Exception e) {
      LOG.error("Failed abortTransactions", e);
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

  public static ImmutableMap<String, Long> dumpMetaCallTimingWithoutEx(String phase) {
    try {
      return get().dumpAndClearMetaCallTiming(phase);
    } catch (HiveException he) {
      LOG.warn("Caught exception attempting to write metadata call information " + he, he);
    }
    return null;
  }

  public ImmutableMap<String, Long> dumpAndClearMetaCallTiming(String phase) {
    if (LOG.isInfoEnabled()) {
      boolean phaseInfoLogged = logDumpPhase(phase);
      LOG.info("Total time spent in each metastore function (ms): " + metaCallTimeMap);
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
      return getMSC().getCheckConstraints(new CheckConstraintsRequest(getDefaultCatalog(conf), dbName, tblName));
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public SQLAllTableConstraints getTableConstraints(String dbName, String tblName, long tableId)
      throws HiveException, NoSuchObjectException {
    try {
      ValidWriteIdList validWriteIdList = getValidWriteIdList(dbName, tblName);
      AllTableConstraintsRequest request = new AllTableConstraintsRequest(dbName, tblName, getDefaultCatalog(conf));
      request.setTableId(tableId);
      request.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.writeToString() : null);
      return getMSC().getAllTableConstraints(request);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public TableConstraintsInfo getTableConstraints(String dbName, String tblName, boolean fetchReliable,
      boolean fetchEnabled, long tableId) throws HiveException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.HIVE_GET_TABLE_CONSTRAINTS);

    try {

      ValidWriteIdList validWriteIdList = getValidWriteIdList(dbName,tblName);
      AllTableConstraintsRequest request = new AllTableConstraintsRequest(dbName, tblName, getDefaultCatalog(conf));
      request.setValidWriteIdList(validWriteIdList != null ? validWriteIdList.writeToString() : null);
      request.setTableId(tableId);

      SQLAllTableConstraints tableConstraints = getMSC().getAllTableConstraints(request);
      if (fetchReliable && tableConstraints != null) {
        if (CollectionUtils.isNotEmpty(tableConstraints.getPrimaryKeys())) {
          tableConstraints.setPrimaryKeys(
              tableConstraints.getPrimaryKeys().stream().filter(SQLPrimaryKey::isRely_cstr)
                  .collect(Collectors.toList()));
        }
        if (CollectionUtils.isNotEmpty(tableConstraints.getForeignKeys())) {
          tableConstraints.setForeignKeys(
              tableConstraints.getForeignKeys().stream().filter(SQLForeignKey::isRely_cstr)
                  .collect(Collectors.toList()));
        }
        if (CollectionUtils.isNotEmpty(tableConstraints.getUniqueConstraints())) {
          tableConstraints.setUniqueConstraints(tableConstraints.getUniqueConstraints().stream()
              .filter(SQLUniqueConstraint::isRely_cstr).collect(Collectors.toList()));
        }
        if (CollectionUtils.isNotEmpty(tableConstraints.getNotNullConstraints())) {
          tableConstraints.setNotNullConstraints(tableConstraints.getNotNullConstraints().stream()
              .filter(SQLNotNullConstraint::isRely_cstr).collect(Collectors.toList()));
        }
      }

      if (fetchEnabled && tableConstraints != null) {
        if (CollectionUtils.isNotEmpty(tableConstraints.getCheckConstraints())) {
          tableConstraints.setCheckConstraints(
              tableConstraints.getCheckConstraints().stream().filter(SQLCheckConstraint::isEnable_cstr)
                  .collect(Collectors.toList()));
        }
        if (CollectionUtils.isNotEmpty(tableConstraints.getDefaultConstraints())) {
          tableConstraints.setDefaultConstraints(tableConstraints.getDefaultConstraints().stream()
              .filter(SQLDefaultConstraint::isEnable_cstr).collect(Collectors.toList()));
        }
      }
      return new TableConstraintsInfo(new PrimaryKeyInfo(tableConstraints.getPrimaryKeys(), tblName, dbName),
          new ForeignKeyInfo(tableConstraints.getForeignKeys(), tblName, dbName),
          new UniqueConstraint(tableConstraints.getUniqueConstraints(), tblName, dbName),
          new DefaultConstraint(tableConstraints.getDefaultConstraints(), tblName, dbName),
          new CheckConstraint(tableConstraints.getCheckConstraints()),
          new NotNullConstraint(tableConstraints.getNotNullConstraints(), tblName, dbName));
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.HIVE_GET_TABLE_CONSTRAINTS, "HS2-cache");
    }
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

  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName, boolean ifNotExists)
      throws HiveException {
    String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
    if (resourcePlan.isSetNs() && !ns.equals(resourcePlan.getNs())) {
      throw new HiveException("Cannot create a plan in a different NS; was "
          + resourcePlan.getNs() + ", configured " + ns);
    }
    resourcePlan.setNs(ns);

    try {
      getMSC().createResourcePlan(resourcePlan, copyFromName);
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e, ErrorMsg.RESOURCE_PLAN_ALREADY_EXISTS, resourcePlan.getName());
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan getResourcePlan(String rpName) throws HiveException {
    try {
      return getMSC().getResourcePlan(rpName, conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE));
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<WMResourcePlan> getAllResourcePlans() throws HiveException {
    try {
      return getMSC().getAllResourcePlans(conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropResourcePlan(String rpName, boolean ifExists) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      getMSC().dropResourcePlan(rpName, ns);
    } catch (NoSuchObjectException e) {
      if (!ifExists) {
        throw new HiveException(e, ErrorMsg.RESOURCE_PLAN_NOT_EXISTS, rpName);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan alterResourcePlan(String rpName, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (resourcePlan.isSetNs() && !ns.equals(resourcePlan.getNs())) {
        throw new HiveException("Cannot modify a plan in a different NS; was "
            + resourcePlan.getNs() + ", configured " + ns);
      }
      resourcePlan.setNs(ns);
      return getMSC().alterResourcePlan(rpName, ns, resourcePlan, canActivateDisabled,
          isForceDeactivate, isReplace);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMFullResourcePlan getActiveResourcePlan() throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      return getMSC().getActiveResourcePlan(ns);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public WMValidateResourcePlanResponse validateResourcePlan(String rpName) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      return getMSC().validateResourcePlan(rpName, ns);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createWMTrigger(WMTrigger trigger) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (trigger.isSetNs() && !ns.equals(trigger.getNs())) {
        throw new HiveException("Cannot create a trigger in a different NS; was "
            + trigger.getNs() + ", configured " + ns);
      }
      trigger.setNs(ns);
      getMSC().createWMTrigger(trigger);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void alterWMTrigger(WMTrigger trigger) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (trigger.isSetNs() && !ns.equals(trigger.getNs())) {
        throw new HiveException("Cannot modify a trigger in a different NS; was "
            + trigger.getNs() + ", configured " + ns);
      }
      trigger.setNs(ns);
      getMSC().alterWMTrigger(trigger);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMTrigger(String rpName, String triggerName) throws HiveException {
    try {
      getMSC().dropWMTrigger(rpName, triggerName, conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createWMPool(WMPool pool) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (pool.isSetNs() && !ns.equals(pool.getNs())) {
        throw new HiveException("Cannot create a pool in a different NS; was "
            + pool.getNs() + ", configured " + ns);
      }
      pool.setNs(ns);
      getMSC().createWMPool(pool);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void alterWMPool(WMNullablePool pool, String poolPath) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (pool.isSetNs() && !ns.equals(pool.getNs())) {
        throw new HiveException("Cannot modify a pool in a different NS; was "
            + pool.getNs() + ", configured " + ns);
      }
      pool.setNs(ns);
      getMSC().alterWMPool(pool, poolPath);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMPool(String resourcePlanName, String poolPath) throws HiveException {
    try {
      getMSC().dropWMPool(resourcePlanName, poolPath,
          conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (mapping.isSetNs() && !ns.equals(mapping.getNs())) {
        throw new HiveException("Cannot create a mapping in a different NS; was "
            + mapping.getNs() + ", configured " + ns);
      }
      mapping.setNs(ns);
      getMSC().createOrUpdateWMMapping(mapping, isUpdate);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropWMMapping(WMMapping mapping) throws HiveException {
    try {
      String ns = conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE);
      if (mapping.isSetNs() && !ns.equals(mapping.getNs())) {
        throw new HiveException("Cannot modify a mapping in a different NS; was "
            + mapping.getNs() + ", configured " + ns);
      }
      mapping.setNs(ns);
      getMSC().dropWMMapping(mapping);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  // TODO: eh
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop) throws HiveException {
    try {
      getMSC().createOrDropTriggerToPoolMapping(resourcePlanName, triggerName, poolPath,
          shouldDrop, conf.getVar(ConfVars.HIVE_SERVER2_WM_NAMESPACE));
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

  public void alterTableExecuteOperation(Table table, AlterTableExecuteSpec executeSpec) throws HiveException {
    try {
      HiveStorageHandler storageHandler = Optional.ofNullable(createStorageHandler(table.getTTable())).orElseThrow(() ->
          new UnsupportedOperationException(String.format("ALTER EXECUTE is not supported for table %s", table.getTableName())));
      storageHandler.executeOperation(table, executeSpec);
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  public void alterTableSnapshotRefOperation(Table table, AlterTableSnapshotRefSpec alterTableSnapshotRefSpec) throws HiveException {
    try {
      HiveStorageHandler storageHandler = createStorageHandler(table.getTTable());
      storageHandler.alterTableSnapshotRefOperation(table, alterTableSnapshotRefSpec);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public AbortCompactResponse abortCompactions(AbortCompactionRequest request) throws HiveException {
    try {
      return getMSC().abortCompactions(request);
    } catch (Exception e) {
      LOG.error("Failed abortCompactions", e);
      throw new HiveException(e);
    }
  }
}
