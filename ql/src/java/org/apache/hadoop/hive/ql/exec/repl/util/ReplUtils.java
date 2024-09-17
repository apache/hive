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
package org.apache.hadoop.hive.ql.exec.repl.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableOperation;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableSetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplStateLogWork;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.lockmgr.DbLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.BootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.IncrementalDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.BootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.IncrementalLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.ColumnStatsUpdateWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.parse.repl.load.UpdatedMetaDataTracker;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Base64;
import java.util.Set;

import static org.apache.hadoop.hive.conf.Constants.SCHEDULED_QUERY_EXECUTIONID;
import static org.apache.hadoop.hive.conf.Constants.SCHEDULED_QUERY_SCHEDULENAME;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.NON_RECOVERABLE_MARKER;

public class ReplUtils {

  public static final String LAST_REPL_ID_KEY = "hive.repl.last.repl.id";

  public static final String FUNCTIONS_ROOT_DIR_NAME = "_functions";
  public static final String CONSTRAINTS_ROOT_DIR_NAME = "_constraints";

  // Root directory for dumping bootstrapped tables along with incremental events dump.
  public static final String INC_BOOTSTRAP_ROOT_DIR_NAME = "_bootstrap";

  // Root base directory name for hive.
  public static final String REPL_HIVE_BASE_DIR = "hive";

  // Root base directory name for ranger.
  public static final String REPL_RANGER_BASE_DIR = "ranger";

  // Root base directory name for atlas.
  public static final String REPL_ATLAS_BASE_DIR = "atlas";

  // Atlas meta data export file.
  public static final String REPL_ATLAS_EXPORT_FILE_NAME = "atlas_export.zip";

  // Config for hadoop default file system.
  public static final String DEFAULT_FS_CONFIG = "fs.defaultFS";


  // Name of the directory which stores the list of tables included in the policy in case of table level replication.
  // One file per database, named after the db name. The directory is not created for db level replication.
  public static final String REPL_TABLE_LIST_DIR_NAME = "_tables";

  // Configuration to enable/disable dumping ACID tables. Used only for testing and shouldn't be
  // seen in production or in case of tests other than the ones where it's required.
  public static final String REPL_DUMP_INCLUDE_ACID_TABLES = "hive.repl.dump.include.acid.tables";

  // HDFS Config to define the maximum number of items a directory may contain.
  public static final String DFS_MAX_DIR_ITEMS_CONFIG = "dfs.namenode.fs-limits.max-directory-items";

  // Reserved number of items to accommodate operational files in the dump root dir.
  public static final int RESERVED_DIR_ITEMS_COUNT = 10;

  public static final String RANGER_AUTHORIZER = "ranger";

  public static final String HIVE_RANGER_POLICIES_FILE_NAME = "ranger_policies.json";

  public static final String RANGER_REST_URL = "ranger.plugin.hive.policy.rest.url";

  public static final String RANGER_HIVE_SERVICE_NAME = "ranger.plugin.hive.service.name";

  public static final String RANGER_CONFIGURATION_RESOURCE_NAME = "ranger-hive-security.xml";

  // Service name for hive.
  public static final String REPL_HIVE_SERVICE = "hive";

  // Service name for ranger.
  public static final String REPL_RANGER_SERVICE = "ranger";

  // Service name for atlas.
  public static final String REPL_ATLAS_SERVICE = "atlas";
  public static final String INC_EVENTS_BATCH = "events_batch_%d";

  /**
   * Bootstrap REPL LOAD operation type on the examined object based on ckpt state.
   */
  public enum ReplLoadOpType {
    LOAD_NEW, LOAD_SKIP, LOAD_REPLACE
  }

  /**
   * Replication Metrics.
   */
  public enum MetricName {
    TABLES, FUNCTIONS, EVENTS, POLICIES, ENTITIES
  }

  public static final String DISTCP_JOB_ID_CONF = "distcp.job.id";
  public static final String DISTCP_JOB_ID_CONF_DEFAULT = "UNAVAILABLE";

  private static transient Logger LOG = LoggerFactory.getLogger(ReplUtils.class);

  public static Map<Integer, List<ExprNodeGenericFuncDesc>> genPartSpecs(
          Table table, List<Map<String, String>> partitions) throws SemanticException {
    Map<Integer, List<ExprNodeGenericFuncDesc>> partSpecs = new HashMap<>();
    int partPrefixLength = 0;
    if (partitions.size() > 0) {
      partPrefixLength = partitions.get(0).size();
      // pick the length of the first ptn, we expect all ptns listed to have the same number of
      // key-vals.
    }
    List<ExprNodeGenericFuncDesc> partitionDesc = new ArrayList<>();
    for (Map<String, String> ptn : partitions) {
      // convert each key-value-map to appropriate expression.
      ExprNodeGenericFuncDesc expr = null;
      for (Map.Entry<String, String> kvp : ptn.entrySet()) {
        String key = kvp.getKey();
        Object val = kvp.getValue();
        String type = table.getPartColByName(key).getType();
        PrimitiveTypeInfo pti = TypeInfoFactory.getPrimitiveTypeInfo(type);
        ExprNodeColumnDesc column = new ExprNodeColumnDesc(pti, key, null, true);
        ExprNodeGenericFuncDesc op = PartitionUtils.makeBinaryPredicate(
                "=", column, new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, val));
        expr = (expr == null) ? op : PartitionUtils.makeBinaryPredicate("and", expr, op);
      }
      if (expr != null) {
        partitionDesc.add(expr);
      }
    }
    if (partitionDesc.size() > 0) {
      partSpecs.put(partPrefixLength, partitionDesc);
    }
    return partSpecs;
  }

  public static void unsetDbPropIfSet(Database db, String prop, Hive hiveDb) throws HiveException {
    if (db == null) {
      return;
    }
    Map<String, String> dbProps = db.getParameters();
    if (dbProps == null || !dbProps.containsKey(prop)) {
      return;
    }
    LOG.info("Removing property: {} from database: {}", prop, db.getName());
    dbProps.remove(prop);
    hiveDb.alterDatabase(db.getName(), db);
  }

  public static Task<?> getTableReplLogTask(ImportTableDesc tableDesc, ReplLogger replLogger, HiveConf conf,
                                            ReplicationMetricCollector metricCollector,
                                            String dumpRoot)
          throws SemanticException {
    TableType tableType = tableDesc.isExternal() ? TableType.EXTERNAL_TABLE : tableDesc.tableType();
    ReplStateLogWork replLogWork = new ReplStateLogWork(replLogger, metricCollector,
            tableDesc.getTableName(), tableType, dumpRoot);
    return TaskFactory.get(replLogWork, conf);
  }

  public static Task<?> getTableCheckpointTask(ImportTableDesc tableDesc, HashMap<String, String> partSpec,
                                               String dumpRoot, ReplicationMetricCollector metricCollector,
                                               HiveConf conf) throws SemanticException {
    HashMap<String, String> mapProp = new HashMap<>();
    mapProp.put(ReplConst.REPL_TARGET_DB_PROPERTY, dumpRoot);

    final TableName tName = TableName.fromString(tableDesc.getTableName(), null, tableDesc.getDatabaseName());
    AlterTableSetPropertiesDesc alterTblDesc =  new AlterTableSetPropertiesDesc(tName, partSpec, null, false,
            mapProp, false, false, null);
    return TaskFactory.get(new DDLWork(new HashSet<>(), new HashSet<>(), alterTblDesc,
            true, (new Path(dumpRoot)).getParent().toString(), metricCollector), conf);
  }

  public static boolean replCkptStatus(String dbName, Map<String, String> props, String dumpRoot)
          throws InvalidOperationException {
    // If ckpt property not set or empty means, bootstrap is not run on this object.
    if ((props != null) && props.containsKey(ReplConst.REPL_TARGET_DB_PROPERTY)
            && !props.get(ReplConst.REPL_TARGET_DB_PROPERTY).isEmpty()) {
      if (props.get(ReplConst.REPL_TARGET_DB_PROPERTY).equals(dumpRoot)) {
        return true;
      }
      throw new InvalidOperationException(ErrorMsg.REPL_BOOTSTRAP_LOAD_PATH_NOT_VALID.format(dumpRoot,
              props.get(ReplConst.REPL_TARGET_DB_PROPERTY)));
    }
    return false;
  }

  public static String getNonEmpty(String configParam, HiveConf hiveConf, String errorMsgFormat)
          throws SemanticException {
    String val = hiveConf.get(configParam);
    if (StringUtils.isEmpty(val)) {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format(String.format(
        errorMsgFormat, configParam), ReplUtils.REPL_ATLAS_SERVICE));
    }
    return val;
  }


  public static List<Task<?>> addChildTask(Task<?> childTask) {
    List<Task<?>> taskList = new ArrayList<>();
    taskList.add(childTask);
    return taskList;
  }

  public static List<Task<?>> addTasksForLoadingColStats(ColumnStatistics colStats,
                                                         HiveConf conf,
                                                         UpdatedMetaDataTracker updatedMetadata,
                                                         org.apache.hadoop.hive.metastore.api.Table tableObj,
                                                         long writeId,
                                                         String nonRecoverableMarkPath,
                                                         ReplicationMetricCollector metricCollector)
          throws IOException, TException {
    List<Task<?>> taskList = new ArrayList<>();
    ColumnStatsUpdateWork work = new ColumnStatsUpdateWork(colStats, nonRecoverableMarkPath, metricCollector, true);
    work.setWriteId(writeId);
    Task<?> task = TaskFactory.get(work, conf);
    taskList.add(task);
    return taskList;

  }

  // Path filters to filter only events (directories) excluding "_bootstrap"
  public static PathFilter getEventsDirectoryFilter(final FileSystem fs) {
    return p -> {
      try {
        return fs.isDirectory(p) && !p.getName().equalsIgnoreCase(ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME)
                && !p.getName().equalsIgnoreCase(ReplUtils.REPL_TABLE_LIST_DIR_NAME)
                && !p.getName().equalsIgnoreCase(EximUtil.METADATA_PATH_NAME);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static PathFilter getBootstrapDirectoryFilter(final FileSystem fs) {
    return p -> {
      try {
        return fs.isDirectory(p) && !p.getName().equalsIgnoreCase(ReplUtils.REPL_TABLE_LIST_DIR_NAME)
                && !p.getName().equalsIgnoreCase(EximUtil.METADATA_PATH_NAME);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static int handleException(boolean isReplication, Throwable e, String nonRecoverablePath,
                                    ReplicationMetricCollector metricCollector, String stageName, HiveConf conf){
    int errorCode;
    if (isReplication && e instanceof SnapshotException) {
      errorCode = ErrorMsg.getErrorMsg("SNAPSHOT_ERROR").getErrorCode();
    } else {
      errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
    if(isReplication){
      try {
        if (nonRecoverablePath != null) {
          final int recoverableLimit = ErrorMsg.GENERIC_ERROR.getErrorCode();
          String metricStage = getMetricStageName(stageName, metricCollector);
          if(errorCode > recoverableLimit){
            Path nonRecoverableMarker = new Path(new Path(nonRecoverablePath), ReplAck.NON_RECOVERABLE_MARKER.toString());
            Utils.writeStackTrace(e, nonRecoverableMarker, conf);
            metricCollector.reportStageEnd(metricStage, Status.FAILED_ADMIN, nonRecoverableMarker.toString());
          }
          else {
            metricCollector.reportStageEnd(metricStage, Status.FAILED);
          }
        }
      } catch (Exception ex) {
        LOG.error("Failed to collect Metrics ", ex);
      }
    }
    return errorCode;
  }

  public static boolean shouldIgnoreOnError(DDLOperation<?> ddlOperation, Throwable e) {
    return ReplUtils.isCreateOperation(ddlOperation) && e.getMessage().contains("java.lang.NumberFormatException");
  }

  public static boolean isCreateOperation(DDLOperation<?> ddlOperation) {
    return ddlOperation instanceof CreateTableOperation;
  }

  private static String getMetricStageName(String stageName, ReplicationMetricCollector metricCollector) {
    if( stageName == "REPL_DUMP" || stageName == "REPL_LOAD" || stageName == "ATLAS_DUMP" || stageName == "ATLAS_LOAD"
            || stageName == "RANGER_DUMP" || stageName == "RANGER_LOAD" || stageName == "RANGER_DENY"){
      return stageName;
    }
    if(isDumpMetricCollector(metricCollector)){
        return "REPL_DUMP";
    } else {
      return "REPL_LOAD";
    }
  }

  private static boolean isDumpMetricCollector(ReplicationMetricCollector metricCollector) {
    return metricCollector instanceof BootstrapDumpMetricCollector || 
            metricCollector instanceof IncrementalDumpMetricCollector;
  }

  private static boolean isLoadMetricCollector(ReplicationMetricCollector metricCollector) {
    return metricCollector instanceof BootstrapLoadMetricCollector ||
            metricCollector instanceof IncrementalLoadMetricCollector;
  }

  public static boolean isFirstIncPending(Map<String, String> parameters) {
    // If flag is not set, then we assume first incremental load is done as the database/table may be created by user
    // and not through replication.
    return parameters != null && ReplConst.TRUE.equalsIgnoreCase(parameters.get(ReplConst.REPL_FIRST_INC_PENDING_FLAG));
  }

  public static List<Long> getOpenTxns(ValidTxnList validTxnList) {
    long[] invalidTxns = validTxnList.getInvalidTransactions();
    List<Long> openTxns = new ArrayList<>();
    for (long invalidTxn : invalidTxns) {
      if (!validTxnList.isTxnAborted(invalidTxn)) {
        openTxns.add(invalidTxn);
      }
    }
    return openTxns;
  }

  public static List<Long> getOpenTxns(HiveTxnManager hiveTxnManager, ValidTxnList validTxnList, String dbName) throws LockException {
    HiveLockManager lockManager = hiveTxnManager.getLockManager();
    long[] invalidTxns = validTxnList.getInvalidTransactions();
    List<Long> openTxns = new ArrayList<>();
    Set<Long> dbTxns = new HashSet<>();
    if (lockManager instanceof DbLockManager) {
      ShowLocksRequest request = new ShowLocksRequest();
      request.setDbname(dbName.toLowerCase());
      ShowLocksResponse showLocksResponse = ((DbLockManager)lockManager).getLocks(request);
      for (ShowLocksResponseElement showLocksResponseElement : showLocksResponse.getLocks()) {
        dbTxns.add(showLocksResponseElement.getTxnid());
      }
      for (long invalidTxn : invalidTxns) {
        if (dbTxns.contains(invalidTxn) && !validTxnList.isTxnAborted(invalidTxn)) {
          openTxns.add(invalidTxn);
        }
      }
    } else {
      for (long invalidTxn : invalidTxns) {
        if (!validTxnList.isTxnAborted(invalidTxn)) {
          openTxns.add(invalidTxn);
        }
      }
    }
    return openTxns;
  }

  public static MessageDeserializer getEventDeserializer(NotificationEvent event) {
    try {
      return MessageFactory.getInstance(event.getMessageFormat()).getDeserializer();
    } catch (Exception e) {
      String message =
              "could not create appropriate messageFactory for format " + event.getMessageFormat();
      LOG.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  public static EnvironmentContext setReplDataLocationChangedFlag(EnvironmentContext envContext) {
    if (envContext == null) {
      envContext = new EnvironmentContext();
    }
    envContext.putToProperties(ReplConst.REPL_DATA_LOCATION_CHANGED, ReplConst.TRUE);
    return envContext;
  }

  // Only for testing, we do not include ACID tables in the dump (and replicate) if config says so.
  public static boolean includeAcidTableInDump(HiveConf conf) {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)) {
      return conf.getBoolean(REPL_DUMP_INCLUDE_ACID_TABLES, true);
    }

    return true;
  }

  public static boolean tableIncludedInReplScope(ReplScope replScope, String tableName) {
    return ((replScope == null) || replScope.tableIncludedInReplScope(tableName));
  }

  public static boolean failedWithNonRecoverableError(Path dumpRoot, HiveConf conf) throws SemanticException {
    if (dumpRoot == null) {
      return false;
    }
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class).build();
    try {
      return retryable.executeCallable(() -> {
        FileSystem fs = dumpRoot.getFileSystem(conf);
        if (fs.exists(new Path(dumpRoot, NON_RECOVERABLE_MARKER.toString()))) {
          return true;
        }
        return false;
      });
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  public static Path getEncodedDumpRootPath(HiveConf conf, String dbname) throws UnsupportedEncodingException {
    return new Path(conf.getVar(HiveConf.ConfVars.REPL_DIR),
      Base64.getEncoder().encodeToString(dbname
        .getBytes(StandardCharsets.UTF_8.name())));
  }

  public static Path getLatestDumpPath(Path dumpRoot, HiveConf conf) throws IOException {
    FileSystem fs = dumpRoot.getFileSystem(conf);
    if (fs.exists(dumpRoot)) {
      FileStatus[] statuses = fs.listStatus(dumpRoot);
      if (statuses.length > 0) {
        FileStatus latestValidStatus = statuses[0];
        for (FileStatus status : statuses) {
          LOG.info("Evaluating previous dump dir path:{}", status.getPath());
          if (status.getModificationTime() > latestValidStatus.getModificationTime()) {
            latestValidStatus = status;
          }
        }
        return latestValidStatus.getPath();
      }
    }
    return null;
  }

  public static String getDistCpCustomName(HiveConf conf, String dbName) {
    String userChosenName = conf.get(JobContext.JOB_NAME);
    if (StringUtils.isEmpty(userChosenName)) {
      String policyName = conf.get(SCHEDULED_QUERY_SCHEDULENAME, "");
      if (policyName.isEmpty()) {
        userChosenName = "Repl#" + dbName;
      } else {
        String executionId = conf.get(SCHEDULED_QUERY_EXECUTIONID, "");

        userChosenName = "Repl#" + policyName + "#" + executionId + "#" + dbName;
      }
      LOG.info("Using {} as job name for map-reduce jobs.", userChosenName);
    } else {
      LOG.info("Job Name is explicitly configured as {}, not using " + "replication job custom name.", userChosenName);
    }
    return userChosenName;
  }

  /**
   * Convert to a human time of minutes:seconds.millis.
   * @param time time to humanize.
   * @return a printable value.
   */
  public static String convertToHumanReadableTime(long time) {
    long seconds = (time / 1000);
    long minutes = (seconds / 60);
    return String.format("%d:%02d.%03ds", minutes, seconds % 60, time % 1000);
  }

  /**
   * Adds a logger task at the end of the tasks passed.
   */
  public static void addLoggerTask(ReplLogger replLogger, List<Task<?>> tasks, HiveConf conf) {
    String message = "Completed all external table copy tasks.";
    ReplStateLogWork replStateLogWork = new ReplStateLogWork(replLogger, message);
    Task<ReplStateLogWork> task = TaskFactory.get(replStateLogWork, conf);
    if (tasks.isEmpty()) {
      tasks.add(task);
    } else {
      DAGTraversal.traverse(tasks, new AddDependencyToLeaves(Collections.singletonList(task)));
    }
  }

  /**
   * Used to report status of replication stage which is skipped or has some error
   * @param stageName Name of replication stage
   * @param status Status skipped or FAILED etc
   * @param errorLogPath path of error log file
   * @param conf handle configuration parameter
   * @param dbName name of database
   * @param replicationType type of replication incremental, bootstrap, etc
   * @throws SemanticException
   */
  public static void reportStatusInReplicationMetrics(String stageName, Status status, String errorLogPath,
                                                      HiveConf conf, String dbName, Metadata.ReplicationType replicationType)
          throws SemanticException {
    ReplicationMetricCollector metricCollector = new ReplicationMetricCollector(dbName, replicationType, null, 0, conf) {};
    metricCollector.reportStageStart(stageName, new HashMap<>());
    metricCollector.reportStageEnd(stageName, status, errorLogPath);
  }

  public static boolean isErrorRecoverable(Throwable e) {
    int errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    return errorCode > ErrorMsg.GENERIC_ERROR.getErrorCode();
  }

  // True if REPL DUMP should do transaction optimization
  public static boolean filterTransactionOperations(HiveConf conf) {
    return (conf.getBoolVar(HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS));
  }
  public  static class TimeSerializer extends JsonSerializer<Long> {

    @Override
    public void serialize(Long epoch, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(Instant.ofEpochSecond(epoch).toString());
    }
  }
}
