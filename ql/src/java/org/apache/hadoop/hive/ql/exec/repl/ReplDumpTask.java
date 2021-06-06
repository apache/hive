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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.CatalogFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.ReplEventFilter;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.util.AddDependencyToLeaves;
import org.apache.hadoop.hive.ql.exec.repl.util.FileList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.TaskTracker;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.apache.hadoop.hive.ql.exec.util.Retryable;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.DbLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.EventUtils;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.HiveWrapper;
import org.apache.hadoop.hive.ql.parse.repl.dump.TableExport;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandler;
import org.apache.hadoop.hive.ql.parse.repl.dump.events.EventHandlerFactory;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.ConstraintsSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.FunctionSerializer;
import org.apache.hadoop.hive.ql.parse.repl.dump.io.JsonWriter;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.BootstrapDumpLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.log.IncrementalDumpLogger;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.BootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.IncrementalDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.conf.Constants.SCHEDULED_QUERY_SCHEDULENAME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.getReplPolicyIdString;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_AUTHORIZER;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.cleanupSnapshots;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.getDFS;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.getListFromFileList;

public class ReplDumpTask extends Task<ReplDumpWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";
  private static final String FUNCTION_METADATA_FILE_NAME = EximUtil.METADATA_NAME;
  private static final long SLEEP_TIME = 5 * 60000;
  private static final long SLEEP_TIME_FOR_TESTS = 30000;
  private Set<String> tablesForBootstrap = new HashSet<>();

  public enum ConstraintFileType {COMMON("common", "c_"), FOREIGNKEY("fk", "f_");
    private final String name;
    private final String prefix;
    ConstraintFileType(String name, String prefix) {
      this.name = name;
      this.prefix = prefix;
    }
    public String getName() {
      return this.name;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  private Logger LOG = LoggerFactory.getLogger(ReplDumpTask.class);

  @Override
  public String getName() {
    return "REPL_DUMP";
  }

  @Override
  public int execute() {
    try {
      SecurityUtils.reloginExpiringKeytabUser();
      if (work.dataCopyIteratorsInitialized()) {
        initiateDataCopyTasks();
      } else {
        Path dumpRoot = ReplUtils.getEncodedDumpRootPath(conf, work.dbNameOrPattern.toLowerCase());
        if (ReplUtils.failedWithNonRecoverableError(ReplUtils.getLatestDumpPath(dumpRoot, conf), conf)) {
          LOG.error("Previous dump failed with non recoverable error. Needs manual intervention. ");
          setException(new SemanticException(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.format()));
          return ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode();
        }
        Path previousValidHiveDumpPath = getPreviousValidDumpMetadataPath(dumpRoot);
        boolean isBootstrap = (previousValidHiveDumpPath == null);
        work.setBootstrap(isBootstrap);
        if (previousValidHiveDumpPath != null) {
          work.setOldReplScope(new DumpMetaData(previousValidHiveDumpPath, conf).getReplScope());
        }
        //If no previous dump is present or previous dump is already loaded, proceed with the dump operation.
        if (shouldDump(previousValidHiveDumpPath)) {
          Path currentDumpPath = getCurrentDumpPath(dumpRoot, isBootstrap);
          Path hiveDumpRoot = new Path(currentDumpPath, ReplUtils.REPL_HIVE_BASE_DIR);
          // Set distCp custom name corresponding to the replication policy.
          String mapRedCustomName = ReplUtils.getDistCpCustomName(conf, work.dbNameOrPattern);
          conf.set(JobContext.JOB_NAME, mapRedCustomName);
          work.setCurrentDumpPath(currentDumpPath);
          work.setMetricCollector(initMetricCollection(isBootstrap, hiveDumpRoot));
          if (shouldDumpAtlasMetadata()) {
            addAtlasDumpTask(isBootstrap, previousValidHiveDumpPath);
            LOG.info("Added task to dump atlas metadata.");
          }
          if (shouldDumpAuthorizationMetadata()) {
            initiateAuthorizationDumpTask();
          }
          DumpMetaData dmd = new DumpMetaData(hiveDumpRoot, conf);
          // Initialize ReplChangeManager instance since we will require it to encode file URI.
          ReplChangeManager.getInstance(conf);
          Path cmRoot = new Path(conf.getVar(HiveConf.ConfVars.REPLCMDIR));
          Long lastReplId;
          LOG.info("Data copy at load enabled : {}", conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET));
          if (isBootstrap) {
            lastReplId = bootStrapDump(hiveDumpRoot, dmd, cmRoot, getHive());
          } else {
            work.setEventFrom(getEventFromPreviousDumpMetadata(previousValidHiveDumpPath));
            lastReplId = incrementalDump(hiveDumpRoot, dmd, cmRoot, getHive());
          }
          work.setResultValues(Arrays.asList(currentDumpPath.toUri().toString(), String.valueOf(lastReplId)));
          initiateDataCopyTasks();
        } else {
          LOG.info("Previous Dump is not yet loaded");
        }
      }
    } catch (RuntimeException e) {
      LOG.error("replication failed with run time exception", e);
      setException(e);
      try{
        ReplUtils.handleException(true, e, work.getCurrentDumpPath().toString(),
                work.getMetricCollector(), getName(), conf);
      } catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
      }
      throw e;
    } catch (Exception e) {
      setException(e);
      int errorCode;
      if (e instanceof SnapshotException) {
        errorCode = ErrorMsg.getErrorMsg("SNAPSHOT_ERROR").getErrorCode();
      } else {
        errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
      }
      try{
        return ReplUtils.handleException(true, e, work.getCurrentDumpPath().toString(),
                work.getMetricCollector(), getName(), conf);
      }
      catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
        return errorCode;        
      }
    }
    return 0;
  }

  private void initiateAuthorizationDumpTask() throws SemanticException {
    if (RANGER_AUTHORIZER.equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE))) {
      Path rangerDumpRoot = new Path(work.getCurrentDumpPath(), ReplUtils.REPL_RANGER_BASE_DIR);
      LOG.info("Exporting Authorization Metadata from {} at {} ", RANGER_AUTHORIZER, rangerDumpRoot);
      RangerDumpWork rangerDumpWork = new RangerDumpWork(rangerDumpRoot, work.dbNameOrPattern,
          work.getMetricCollector());
      Task<RangerDumpWork> rangerDumpTask = TaskFactory.get(rangerDumpWork, conf);
      if (childTasks == null) {
        childTasks = new ArrayList<>();
      }
      childTasks.add(rangerDumpTask);
    } else {
      throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.format("Authorizer "
        + conf.getVar(HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE)
              + " not supported for replication ", ReplUtils.REPL_RANGER_SERVICE));
    }
  }

  private boolean shouldDumpAuthorizationMetadata() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_AUTHORIZATION_METADATA);
  }

  private boolean shouldDumpAtlasMetadata() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA);
  }

  private Path getCurrentDumpPath(Path dumpRoot, boolean isBootstrap) throws IOException {
    Path lastDumpPath = ReplUtils.getLatestDumpPath(dumpRoot, conf);
    if (lastDumpPath != null && shouldResumePreviousDump(lastDumpPath, isBootstrap)) {
      //Resume previous dump
      LOG.info("Resuming the dump with existing dump directory {}", lastDumpPath);
      work.setShouldOverwrite(true);
      return lastDumpPath;
    } else {
      return new Path(dumpRoot, getNextDumpDir());
    }
  }

  private void initiateDataCopyTasks() throws SemanticException, IOException {
    TaskTracker taskTracker = new TaskTracker(conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS));
    if (childTasks == null) {
      childTasks = new ArrayList<>();
    }
    List<Task<?>> externalTableCopyTasks = work.externalTableCopyTasks(taskTracker, conf);
    childTasks.addAll(externalTableCopyTasks);
    LOG.debug("Scheduled {} external table copy tasks", externalTableCopyTasks.size());
    // If external table data copy tasks are present add a task to mark the end of data copy
    if (!externalTableCopyTasks.isEmpty() && !work.getExternalTblCopyPathIterator().hasNext()) {
      ReplUtils.addLoggerTask(work.getReplLogger(), childTasks, conf);
    }
    childTasks.addAll(work.managedTableCopyTasks(taskTracker, conf));
    childTasks.addAll(work.functionsBinariesCopyTasks(taskTracker, conf));
    if (childTasks.isEmpty()) {
      //All table data copy work finished.
      finishRemainingTasks();
    } else {
      DAGTraversal.traverse(childTasks, new AddDependencyToLeaves(TaskFactory.get(work, conf)));
    }
  }

  private void addAtlasDumpTask(boolean bootstrap, Path prevHiveDumpDir) {
    Path atlasDumpDir = new Path(work.getCurrentDumpPath(), ReplUtils.REPL_ATLAS_BASE_DIR);
    Path prevAtlasDumpDir = prevHiveDumpDir == null ? null
                            : new Path(prevHiveDumpDir.getParent(), ReplUtils.REPL_ATLAS_BASE_DIR);
    Path tableListLoc = null;
    if (!work.replScope.includeAllTables()) {
      Path tableListDir = new Path(work.getCurrentDumpPath(), ReplUtils.REPL_HIVE_BASE_DIR + "/" + ReplUtils.REPL_TABLE_LIST_DIR_NAME);
      tableListLoc = new Path(tableListDir, work.dbNameOrPattern.toLowerCase());
    }
    AtlasDumpWork atlasDumpWork = new AtlasDumpWork(work.dbNameOrPattern, atlasDumpDir, bootstrap, prevAtlasDumpDir,
           tableListLoc, work.getMetricCollector());
    Task<?> atlasDumpTask = TaskFactory.get(atlasDumpWork, conf);
    childTasks = new ArrayList<>();
    childTasks.add(atlasDumpTask);
  }


  private void finishRemainingTasks() throws SemanticException {
    Path dumpAckFile = new Path(work.getCurrentDumpPath(),
            ReplUtils.REPL_HIVE_BASE_DIR + File.separator
                    + ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Utils.create(dumpAckFile, conf);
    prepareReturnValues(work.getResultValues());
    work.getMetricCollector().reportEnd(Status.SUCCESS);
    deleteAllPreviousDumpMeta(work.getCurrentDumpPath());
  }

  private void prepareReturnValues(List<String> values) throws SemanticException {
    LOG.debug("prepareReturnValues : " + dumpSchema);
    for (String s : values) {
      LOG.debug("    > " + s);
    }
    Utils.writeOutput(Collections.singletonList(values), new Path(work.resultTempPath), conf);
  }

  private void deleteAllPreviousDumpMeta(Path currentDumpPath) {
    try {
      Path dumpRoot = getDumpRoot(currentDumpPath);
      if(dumpRoot == null) {
        return;
      }
      FileSystem fs = dumpRoot.getFileSystem(conf);
      if (fs.exists(dumpRoot)) {
        FileStatus[] statuses = fs.listStatus(dumpRoot,
          path -> !path.equals(currentDumpPath) && !path.toUri().getPath().equals(currentDumpPath.toString()));

        int retainPrevDumpDirCount = conf.getIntVar(HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT);
        int numDumpDirs = statuses.length;
        if(shouldRetainPrevDumpDirs()) {
          Arrays.sort(statuses, (Comparator.<FileStatus>
                  comparingLong(fileStatus1 -> fileStatus1.getModificationTime())
                  .thenComparingLong(fileStatus2 -> fileStatus2.getModificationTime())));
        }
        for (FileStatus status : statuses) {
          //based on config, either delete all previous dump-dirs
          //or delete a minimum number of oldest dump-directories
          if(!shouldRetainPrevDumpDirs() || numDumpDirs > retainPrevDumpDirCount){
            fs.delete(status.getPath(), true);
            numDumpDirs--;

          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Possible leak on disk, could not delete the previous dump directory:" + currentDumpPath, ex);
    }
  }

  private Path getDumpRoot(Path currentDumpPath) {
    if (ReplDumpWork.testDeletePreviousDumpMetaPath
            && (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)
                || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL))) {
      //testDeleteDumpMetaDumpPath to be used only for test.
      return null;
    } else {
      return currentDumpPath.getParent();
    }
  }

  private Long getEventFromPreviousDumpMetadata(Path previousDumpPath) throws SemanticException {
    if (previousDumpPath != null) {
      DumpMetaData dmd = new DumpMetaData(previousDumpPath, conf);
      if (dmd.isIncrementalDump()) {
        return dmd.getEventTo();
      }
      //bootstrap case return event from
      return dmd.getEventFrom();
    }
    return 0L;
  }

  private Path getPreviousValidDumpMetadataPath(Path dumpRoot) throws IOException {
    FileStatus latestValidStatus = null;
    FileSystem fs = dumpRoot.getFileSystem(conf);
    if (fs.exists(dumpRoot)) {
      FileStatus[] statuses = fs.listStatus(dumpRoot);
      for (FileStatus status : statuses) {
        LOG.info("Evaluating previous dump dir path:{}", status.getPath());
        if (latestValidStatus == null) {
          latestValidStatus = validDump(status.getPath()) ? status : null;
        } else if (validDump(status.getPath())
                && status.getModificationTime() > latestValidStatus.getModificationTime()) {
          latestValidStatus = status;
        }
      }
    }
    Path latestDumpDir = (latestValidStatus == null)
         ? null : new Path(latestValidStatus.getPath(), ReplUtils.REPL_HIVE_BASE_DIR);
    LOG.info("Selecting latest valid dump dir as {}", (latestDumpDir == null) ? "null" : latestDumpDir.toString());
    return latestDumpDir;
  }

  private boolean validDump(Path dumpDir) throws IOException {
    //Check if it was a successful dump
    if (dumpDir != null) {
      FileSystem fs = dumpDir.getFileSystem(conf);
      Path hiveDumpDir = new Path(dumpDir, ReplUtils.REPL_HIVE_BASE_DIR);
      return fs.exists(new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString()));
    }
    return false;
  }

  private boolean shouldDump(Path previousDumpPath) throws IOException {
    //If no previous dump means bootstrap. So return true as there was no
    //previous dump to load
    if (previousDumpPath == null) {
      return true;
    } else {
      FileSystem fs = previousDumpPath.getFileSystem(conf);
      return fs.exists(new Path(previousDumpPath, LOAD_ACKNOWLEDGEMENT.toString()));
    }
  }

  /**
   * Decide whether to examine all the tables to dump. We do this if
   * 1. External tables are going to be part of the dump : In which case we need to list their
   * locations.
   * 2. External or ACID tables are being bootstrapped for the first time : so that we can dump
   * those tables as a whole.
   * 3. If replication policy is changed/replaced, then need to examine all the tables to see if
   * any of them need to be bootstrapped as old policy doesn't include it but new one does.
   * 4. Some tables are renamed and the new name satisfies the table list filter while old name was not.
   * @return true if need to examine tables for dump and false if not.
   */
  private boolean shouldExamineTablesToDump() {
    return (previousReplScopeModified())
            || !tablesForBootstrap.isEmpty()
            || conf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES)
            || conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES);
  }

  private boolean previousReplScopeModified() {
    return work.oldReplScope != null && !work.oldReplScope.equals(work.replScope);
  }

  /**
   * Decide whether to dump external tables data. If external tables are enabled for replication,
   * then need to dump it's data in all the incremental dumps.
   * @param conf Hive Configuration.
   * @return true if need to dump external table data and false if not.
   */
  public static boolean shouldDumpExternalTableLocation(HiveConf conf) {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES)
            && (!conf.getBoolVar(REPL_DUMP_METADATA_ONLY) &&
            !conf.getBoolVar(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE));
  }

  /**
   * Decide whether to dump external tables.
   * @param tableName - Name of external table to be replicated
   * @return true if need to bootstrap dump external table and false if not.
   */
  private boolean shouldBootstrapDumpExternalTable(String tableName) {
    // Note: If repl policy is replaced, then need to dump external tables if table is getting replicated
    // for the first time in current dump. So, need to check if table is included in old policy.
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES)
            && (conf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES)
            || !ReplUtils.tableIncludedInReplScope(work.oldReplScope, tableName));
  }

  /**
   * Decide whether to dump materialized views.
   */
  private boolean isMaterializedViewsReplEnabled() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_MATERIALIZED_VIEWS);
  }

  /**
   * Decide whether to retain previous dump-directories after repl-dump
   */
  private boolean shouldRetainPrevDumpDirs() {
    return conf.getBoolVar(HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR);
  }

  /**
   * Decide whether to dump ACID tables.
   * @param tableName - Name of ACID table to be replicated
   * @return true if need to bootstrap dump ACID table and false if not.
   */
  private boolean shouldBootstrapDumpAcidTable(String tableName) {
    // Note: If repl policy is replaced, then need to dump ACID tables if table is getting replicated
    // for the first time in current dump. So, need to check if table is included in old policy.
    return ReplUtils.includeAcidTableInDump(conf)
            && (conf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES)
            || !ReplUtils.tableIncludedInReplScope(work.oldReplScope, tableName));
  }

  private boolean shouldBootstrapDumpTable(Table table) {
    // Note: If control reaches here, it means, table is already included in new replication policy.
    if (TableType.EXTERNAL_TABLE.equals(table.getTableType())
            && shouldBootstrapDumpExternalTable(table.getTableName())) {
      return true;
    }

    if (AcidUtils.isTransactionalTable(table)
            && shouldBootstrapDumpAcidTable(table.getTableName())) {
      return true;
    }

    // If the table is renamed and the new name satisfies the filter but the old name does not then the table needs to
    // be bootstrapped.
    if (tablesForBootstrap.contains(table.getTableName().toLowerCase())) {
      return true;
    }

    // If replication policy is changed with new included/excluded tables list, then tables which
    // are not included in old policy but included in new policy should be bootstrapped along with
    // the current incremental replication dump.
    // Control reaches for Non-ACID tables.
    return !ReplUtils.tableIncludedInReplScope(work.oldReplScope, table.getTableName());
  }

  private boolean isTableSatifiesConfig(Table table) {
    if (table == null) {
      return false;
    }

    if (TableType.EXTERNAL_TABLE.equals(table.getTableType())
            && !conf.getBoolVar(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES)) {
      return false;
    }

    if (AcidUtils.isTransactionalTable(table)
            && !ReplUtils.includeAcidTableInDump(conf)) {
      return false;
    }

    return true;
  }

  private Long incrementalDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot, Hive hiveDb) throws Exception {
    Long lastReplId;// get list of events matching dbPattern & tblPattern
    // go through each event, and dump out each event to a event-level dump dir inside dumproot
    String validTxnList = null;
    long waitUntilTime = 0;
    long bootDumpBeginReplId = -1;

    List<String> tableList = work.replScope.includeAllTables() ? null : new ArrayList<>();
    SnapshotUtils.ReplSnapshotCount snapshotCount = null;

    // If we are bootstrapping ACID tables, we need to perform steps similar to a regular
    // bootstrap (See bootstrapDump() for more details. Only difference here is instead of
    // waiting for the concurrent transactions to finish, we start dumping the incremental events
    // and wait only for the remaining time if any.
    if (needBootstrapAcidTablesDuringIncrementalDump()) {
      work.setBootstrap(true);
      bootDumpBeginReplId = queryState.getConf().getLong(ReplUtils.LAST_REPL_ID_KEY, -1L);
      assert (bootDumpBeginReplId >= 0);
      LOG.info("Dump for bootstrapping ACID tables during an incremental dump for db {}",
              work.dbNameOrPattern);
      long timeoutInMs = HiveConf.getTimeVar(conf,
              HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
      waitUntilTime = System.currentTimeMillis() + timeoutInMs;
    }
    // TODO : instead of simply restricting by message format, we should eventually
    // move to a jdbc-driver-stype registering of message format, and picking message
    // factory per event to decode. For now, however, since all messages have the
    // same factory, restricting by message format is effectively a guard against
    // older leftover data that would cause us problems.
    work.overrideLastEventToDump(hiveDb, bootDumpBeginReplId);
    IMetaStoreClient.NotificationFilter evFilter = new AndFilter(
        new ReplEventFilter(work.replScope),
        new CatalogFilter(MetaStoreUtils.getDefaultCatalog(conf)),
        new EventBoundaryFilter(work.eventFrom, work.eventTo));
    EventUtils.MSClientNotificationFetcher evFetcher
        = new EventUtils.MSClientNotificationFetcher(hiveDb);
    int maxEventLimit  = getMaxEventAllowed(work.maxEventLimit());
    EventUtils.NotificationEventIterator evIter = new EventUtils.NotificationEventIterator(
        evFetcher, work.eventFrom, maxEventLimit, evFilter);
    lastReplId = work.eventTo;
    Path ackFile = new Path(dumpRoot, ReplAck.EVENTS_DUMP.toString());
    long resumeFrom = Utils.fileExists(ackFile, conf) ? getResumeFrom(ackFile) : work.eventFrom;

    // Right now the only pattern allowed to be specified is *, which matches all the database
    // names. So passing dbname as is works since getDbNotificationEventsCount can exclude filter
    // on database name when it's *. In future, if we support more elaborate patterns, we will
    // have to pass DatabaseAndTableFilter created above to getDbNotificationEventsCount() to get
    // correct event count.
    String dbName = (null != work.dbNameOrPattern && !work.dbNameOrPattern.isEmpty())
        ? work.dbNameOrPattern
        : "?";
    Database db = hiveDb.getDatabase(dbName);
    if (db != null && !HiveConf.getBoolVar(conf, REPL_DUMP_METADATA_ONLY)) {
      setReplSourceFor(hiveDb, dbName, db);
    }

    long estimatedNumEvents = evFetcher.getDbNotificationEventsCount(work.eventFrom, dbName, work.eventTo,
        maxEventLimit);
    try {
      IncrementalDumpLogger replLogger =
          new IncrementalDumpLogger(dbName, dumpRoot.toString(), estimatedNumEvents, work.eventFrom, work.eventTo,
              maxEventLimit);
      work.setReplLogger(replLogger);
      replLogger.startLog();
      Map<String, Long> metricMap = new HashMap<>();
      metricMap.put(ReplUtils.MetricName.EVENTS.name(), estimatedNumEvents);
      work.getMetricCollector().reportStageStart(getName(), metricMap);
      long dumpedCount = resumeFrom - work.eventFrom;
      if (dumpedCount > 0) {
        LOG.info("Event id {} to {} are already dumped, skipping {} events", work.eventFrom, resumeFrom, dumpedCount);
      }
      cleanFailedEventDirIfExists(dumpRoot, resumeFrom);
      while (evIter.hasNext()) {
        NotificationEvent ev = evIter.next();
        lastReplId = ev.getEventId();
        if (ev.getEventId() <= resumeFrom) {
          continue;
        }

        //disable materialized-view replication if not configured
        if (!isMaterializedViewsReplEnabled()) {
          String tblName = ev.getTableName();
          if (tblName != null) {
            try {
              Table table = hiveDb.getTable(dbName, tblName);
              if (table != null && TableType.MATERIALIZED_VIEW.equals(table.getTableType())) {
                LOG.info("Attempt to dump materialized view : " + tblName);
                continue;
              }
            } catch (InvalidTableException te) {
              LOG.debug(te.getMessage());
            }
          }
        }

        Path evRoot = new Path(dumpRoot, String.valueOf(lastReplId));
        dumpEvent(ev, evRoot, dumpRoot, cmRoot, hiveDb);
        Utils.writeOutput(String.valueOf(lastReplId), ackFile, conf);
      }
      replLogger.endLog(lastReplId.toString());
      LOG.info("Done dumping events, preparing to return {},{}", dumpRoot.toUri(), lastReplId);
    } finally {
      //write the dmd always irrespective of success/failure to enable checkpointing in table level replication
      long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
      dmd.setDump(DumpType.INCREMENTAL, work.eventFrom, lastReplId, cmRoot, executionId,
        previousReplScopeModified());
      // If repl policy is changed (oldReplScope is set), then pass the current replication policy,
      // so that REPL LOAD would drop the tables which are not included in current policy.
      dmd.setReplScope(work.replScope);
      dmd.write(true);
    }

    // Get snapshot related configurations for external data copy.
    boolean isSnapshotEnabled = conf.getBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY);
    String snapshotPrefix = dbName.toLowerCase();
    ArrayList<String> prevSnaps = new ArrayList<>();
    try (FileList managedTblList = createTableFileList(dumpRoot, EximUtil.FILE_LIST, conf);
        FileList extTableFileList = createTableFileList(dumpRoot, EximUtil.FILE_LIST_EXTERNAL, conf);
        FileList snapPathFileList = isSnapshotEnabled ? createTableFileList(
        SnapshotUtils.getSnapshotFileListPath(dumpRoot), EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT, conf) : null) {
      // Examine all the tables if required.
      if (shouldExamineTablesToDump() || (tableList != null)) {
        // If required wait more for any transactions open at the time of starting the ACID bootstrap.
        if (needBootstrapAcidTablesDuringIncrementalDump()) {
          assert (waitUntilTime > 0);
          validTxnList = getValidTxnListForReplDump(hiveDb, waitUntilTime);
        }
      /* When same dump dir is resumed because of check-pointing, we need to clear the existing metadata.
      We need to rewrite the metadata as the write id list will be changed.
      We can't reuse the previous write id as it might be invalid due to compaction. */
        Path bootstrapRoot = new Path(dumpRoot, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
        Path metadataPath = new Path(bootstrapRoot, EximUtil.METADATA_PATH_NAME);
        FileSystem fs = FileSystem.get(metadataPath.toUri(), conf);
        try {
          fs.delete(metadataPath, true);
        } catch (FileNotFoundException e) {
          // no worries
        }
        Path dbRootMetadata = new Path(metadataPath, dbName);
        Path dbRootData = new Path(bootstrapRoot, EximUtil.DATA_PATH_NAME + File.separator + dbName);
        boolean dataCopyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
        ReplExternalTables externalTablesWriter = new ReplExternalTables(conf);
        boolean isSingleTaskForExternalDb =
            conf.getBoolVar(REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK) && work.replScope.includeAllTables();
        HashMap<String, Boolean> singleCopyPaths = getNonTableLevelCopyPaths(db, isSingleTaskForExternalDb);
        boolean isExternalTablePresent = false;
        if (isSnapshotEnabled) {
          snapshotCount = new SnapshotUtils.ReplSnapshotCount();
          if (snapPathFileList.hasNext()) {
            prevSnaps = getListFromFileList(snapPathFileList);
          }
        }
        for(String matchedDbName : Utils.matchesDb(hiveDb, work.dbNameOrPattern)) {
          for (String tableName : Utils.matchesTbl(hiveDb, matchedDbName, work.replScope)) {
            try {
              Table table = hiveDb.getTable(matchedDbName, tableName);

              // Dump external table locations if required.
              if (TableType.EXTERNAL_TABLE.equals(table.getTableType()) && shouldDumpExternalTableLocation(conf)) {
                externalTablesWriter
                    .dataLocationDump(table, extTableFileList, singleCopyPaths, !isSingleTaskForExternalDb, conf);
                isExternalTablePresent = true;
              }

              // Dump the table to be bootstrapped if required.
              if (shouldBootstrapDumpTable(table)) {
                HiveWrapper.Tuple<Table> tableTuple = new HiveWrapper(hiveDb, matchedDbName).table(table);
                dumpTable(matchedDbName, tableName, validTxnList, dbRootMetadata, dbRootData, bootDumpBeginReplId,
                        hiveDb, tableTuple, managedTblList, dataCopyAtLoad);
              }
              if (tableList != null && isTableSatifiesConfig(table)) {
                tableList.add(tableName);
              }
            } catch (InvalidTableException te) {
              // Repl dump shouldn't fail if the table is dropped/renamed while dumping it.
              // Just log a debug message and skip it.
              LOG.debug(te.getMessage());
            }
          }
          // if it is not a table level replication, add a single task for
          // the database default location and the paths configured.
          if (isExternalTablePresent && shouldDumpExternalTableLocation(conf) && isSingleTaskForExternalDb) {
            externalTablesWriter
                .dumpNonTableLevelCopyPaths(singleCopyPaths, extTableFileList, conf, isSnapshotEnabled, snapshotPrefix,
                    snapshotCount, snapPathFileList, prevSnaps, false);
          }
        }
        dumpTableListToDumpLocation(tableList, dumpRoot, dbName, conf);
      }
      setDataCopyIterators(extTableFileList, managedTblList);
      work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS, lastReplId, snapshotCount, null);
      // Clean-up snapshots
      if (isSnapshotEnabled) {
        cleanupSnapshots(SnapshotUtils.getSnapshotFileListPath(dumpRoot), work.dbNameOrPattern.toLowerCase(), conf,
            snapshotCount, false);
      }
      return lastReplId;
    }
  }

  @NotNull
  private HashMap<String, Boolean> getNonTableLevelCopyPaths(Database db, boolean isSingleCopyTaskForExternalTables) {
    HashMap<String, Boolean> singleCopyPaths = new HashMap<String, Boolean>();
    if (db != null && isSingleCopyTaskForExternalTables) {
      List<String> paths = Arrays.asList(conf.getVar(REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS).split(","));
      for (String path : paths) {
        if (!StringUtils.isEmpty(path)) {
          singleCopyPaths.put(path, false);
        }
      }
      singleCopyPaths.put(db.getLocationUri(), false);
    }
    return singleCopyPaths;
  }

  private void setDataCopyIterators(FileList extTableFileList, FileList managedTableFileList) {
    boolean dataCopyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
    if (dataCopyAtLoad) {
      work.setManagedTableCopyPathIterator(Collections.<String>emptyList().iterator());
      work.setExternalTblCopyPathIterator(Collections.<String>emptyList().iterator());
      LOG.info("Deferring table/partition data copy during dump. It should be done at load.");
    } else {
      work.setManagedTableCopyPathIterator(managedTableFileList);
      work.setExternalTblCopyPathIterator(extTableFileList);
    }
  }

  private ReplicationMetricCollector initMetricCollection(boolean isBootstrap, Path dumpRoot) {
    ReplicationMetricCollector collector;
    if (isBootstrap) {
      collector = new BootstrapDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf);
    } else {
      collector = new IncrementalDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf);
    }
    return collector;
  }

  private int getMaxEventAllowed(int currentEventMaxLimit) {
    int maxDirItems = Integer.parseInt(conf.get(ReplUtils.DFS_MAX_DIR_ITEMS_CONFIG, "0"));
    if (maxDirItems > 0) {
      maxDirItems = maxDirItems - ReplUtils.RESERVED_DIR_ITEMS_COUNT;
      if (maxDirItems < currentEventMaxLimit) {
        LOG.warn("Changing the maxEventLimit from {} to {} as the '" + ReplUtils.DFS_MAX_DIR_ITEMS_CONFIG
                        + "' limit encountered. Set this config appropriately to increase the maxEventLimit",
                currentEventMaxLimit, maxDirItems);
        currentEventMaxLimit = maxDirItems;
      }
    }
    return currentEventMaxLimit;
  }

  private void cleanFailedEventDirIfExists(Path dumpDir, long resumeFrom) throws SemanticException {
    Path nextEventRoot = new Path(dumpDir, String.valueOf(resumeFrom + 1));
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class).build();
    try {
      retryable.executeCallable((Callable<Void>) () -> {
        FileSystem fs = FileSystem.get(nextEventRoot.toUri(), conf);
        try {
          fs.delete(nextEventRoot, true);
        } catch (FileNotFoundException e) {
          // no worries
        }
        return null;
      });
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private long getResumeFrom(Path ackFile) throws SemanticException {
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(Exception.class).build();
    try {
      return retryable.executeCallable(() -> {
        BufferedReader br = null;
        try {
          FileSystem fs = ackFile.getFileSystem(conf);
          br = new BufferedReader(new InputStreamReader(fs.open(ackFile), Charset.defaultCharset()));
          long lastEventID = Long.parseLong(br.readLine());
          return lastEventID;
        } finally {
          if (br != null) {
            try {
              br.close();
            } catch (Exception e) {
              //Do nothing
            }
          }
        }
      });
    } catch (Exception e) {
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }

  private boolean needBootstrapAcidTablesDuringIncrementalDump() {
    // If acid table dump is not enabled, then no need to check further.
    if (!ReplUtils.includeAcidTableInDump(conf)) {
      return false;
    }

    // If old table level policy is available or the policy has filter based on table name then it is possible that some
    // of the ACID tables might be included for bootstrap during incremental dump. For old policy, its because the table
    // may not satisfying the old policy but satisfying the new policy. For filter, it may happen that the table
    // is renamed and started satisfying the policy.
    return ((!work.replScope.includeAllTables())
            || (previousReplScopeModified())
            || conf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES));
  }

  private void dumpEvent(NotificationEvent ev, Path evRoot, Path dumpRoot, Path cmRoot, Hive db) throws Exception {
    EventHandler.Context context = new EventHandler.Context(
        evRoot,
        dumpRoot,
        cmRoot,
        db,
        conf,
        getNewEventOnlyReplicationSpec(ev.getEventId()),
        work.replScope,
        work.oldReplScope,
        tablesForBootstrap
    );
    EventHandler eventHandler = EventHandlerFactory.handlerFor(ev);
    eventHandler.handle(context);
    work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.EVENTS.name(), 1);
    work.getReplLogger().eventLog(String.valueOf(ev.getEventId()), eventHandler.dumpType().toString());
  }

  private ReplicationSpec getNewEventOnlyReplicationSpec(Long eventId) {
    ReplicationSpec rspec =
        getNewReplicationSpec(eventId.toString(), eventId.toString(), conf.getBoolean(
            REPL_DUMP_METADATA_ONLY.varname, false));
    rspec.setReplSpecType(ReplicationSpec.Type.INCREMENTAL_DUMP);
    return rspec;
  }

  private void dumpTableListToDumpLocation(List<String> tableList, Path dbRoot, String dbName,
                                           HiveConf hiveConf) throws Exception {
    // Empty list will create an empty file to distinguish it from db level replication. If no file is there, that means
    // db level replication. If empty file is there, means no table satisfies the policy.
    if (tableList == null) {
      LOG.debug("Table list file is not created for db level replication.");
      return;
    }

    // The table list is dumped in _tables/dbname file
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class).build();
    try {
      retryable.executeCallable((Callable<Void>) () -> {
        Path tableListFile = new Path(dbRoot, ReplUtils.REPL_TABLE_LIST_DIR_NAME);
        tableListFile = new Path(tableListFile, dbName.toLowerCase());
        FSDataOutputStream writer = tableListFile.getFileSystem(hiveConf).create(tableListFile);
        for (String tableName : tableList) {
          String line = tableName.toLowerCase().concat("\n");
          writer.write(line.getBytes(StandardCharsets.UTF_8));
        }
        // Close is called explicitly as close also calls the actual file system write,
        // so there is chance of i/o exception thrown by close.
        writer.close();
        LOG.info("Table list file " + tableListFile.toUri() + " is created for table list - " + tableList);
        return null;
      });
    } catch (Exception e) {
      FileSystem.closeAllForUGI(org.apache.hadoop.hive.shims.Utils.getUGI());
      throw new SemanticException(ErrorMsg.REPL_RETRY_EXHAUSTED.format(e.getMessage()), e);
    }
  }

  Long bootStrapDump(Path dumpRoot, DumpMetaData dmd, Path cmRoot, Hive hiveDb)
          throws Exception {
    // bootstrap case
    // Last repl id would've been captured during compile phase in queryState configs before opening txn.
    // This is needed as we dump data on ACID/MM tables based on read snapshot or else we may lose data from
    // concurrent txns when bootstrap dump in progress. If it is not available, then get it from metastore.
    Long bootDumpBeginReplId = queryState.getConf().getLong(ReplUtils.LAST_REPL_ID_KEY, -1L);
    assert (bootDumpBeginReplId >= 0L);
    List<String> tableList;
    SnapshotUtils.ReplSnapshotCount replSnapshotCount = null;

    LOG.info("Bootstrap Dump for db {}", work.dbNameOrPattern);
    long timeoutInMs = HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT, TimeUnit.MILLISECONDS);
    long waitUntilTime = System.currentTimeMillis() + timeoutInMs;
    String validTxnList = getValidTxnListForReplDump(hiveDb, waitUntilTime);
    Path metadataPath = new Path(dumpRoot, EximUtil.METADATA_PATH_NAME);
    if (shouldResumePreviousDump(dmd)) {
      //clear the metadata. We need to rewrite the metadata as the write id list will be changed
      //We can't reuse the previous write id as it might be invalid due to compaction
      metadataPath.getFileSystem(conf).delete(metadataPath, true);
    }
    List<EximUtil.DataCopyPath> functionsBinaryCopyPaths = Collections.emptyList();
    boolean isSnapshotEnabled = conf.getBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY);
    // Create SnapPathFileList only if snapshots are enabled.
    try (FileList managedTblList = createTableFileList(dumpRoot, EximUtil.FILE_LIST, conf);
        FileList extTableFileList = createTableFileList(dumpRoot, EximUtil.FILE_LIST_EXTERNAL, conf);
        FileList snapPathFileList = isSnapshotEnabled ? createTableFileList(
            SnapshotUtils.getSnapshotFileListPath(dumpRoot), EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT, conf) : null) {
      for (String dbName : Utils.matchesDb(hiveDb, work.dbNameOrPattern)) {
        LOG.debug("Dumping db: " + dbName);
        // TODO : Currently we don't support separate table list for each database.
        tableList = work.replScope.includeAllTables() ? null : new ArrayList<>();
        Database db = hiveDb.getDatabase(dbName);
        if ((db != null) && (ReplUtils.isFirstIncPending(db.getParameters()))) {
          // For replicated (target) database, until after first successful incremental load, the database will not be
          // in a consistent state. Avoid allowing replicating this database to a new target.
          throw new HiveException("Replication dump not allowed for replicated database" +
                  " with first incremental dump pending : " + dbName);
        }

        if (db != null && !HiveConf.getBoolVar(conf, REPL_DUMP_METADATA_ONLY)) {
          setReplSourceFor(hiveDb, dbName, db);
        }

        int estimatedNumTables = Utils.getAllTables(hiveDb, dbName, work.replScope).size();
        int estimatedNumFunctions = hiveDb.getFunctions(dbName, "*").size();
        BootstrapDumpLogger replLogger =
            new BootstrapDumpLogger(dbName, dumpRoot.toString(), estimatedNumTables, estimatedNumFunctions);
        work.setReplLogger(replLogger);
        replLogger.startLog();
        Map<String, Long> metricMap = new HashMap<>();
        metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) estimatedNumTables);
        metricMap.put(ReplUtils.MetricName.FUNCTIONS.name(), (long) estimatedNumFunctions);
        work.getMetricCollector().reportStageStart(getName(), metricMap);
        Path dbRoot = dumpDbMetadata(dbName, metadataPath, bootDumpBeginReplId, hiveDb);
        Path dbDataRoot = new Path(new Path(dumpRoot, EximUtil.DATA_PATH_NAME), dbName);
        boolean dataCopyAtLoad = conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET);
        functionsBinaryCopyPaths = dumpFunctionMetadata(dbName, dbRoot, dbDataRoot, hiveDb, dataCopyAtLoad);

        String uniqueKey = Utils.setDbBootstrapDumpState(hiveDb, dbName);
        Exception caught = null;
        try {
          ReplExternalTables externalTablesWriter = new ReplExternalTables(conf);
          boolean isSingleTaskForExternalDb =
              conf.getBoolVar(REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK) && work.replScope.includeAllTables();
          // Generate snapshot related configurations for external table data copy.
          HashMap<String, Boolean> singleCopyPaths = getNonTableLevelCopyPaths(db, isSingleTaskForExternalDb);
          boolean isExternalTablePresent = false;

          String snapshotPrefix = dbName.toLowerCase();
          ArrayList<String> prevSnaps = new ArrayList<>(); // Will stay empty in case of bootstrap
          if (isSnapshotEnabled) {
            // Delete any old existing snapshot file, We always start fresh in case of bootstrap.
            FileUtils.deleteIfExists(getDFS(SnapshotUtils.getSnapshotFileListPath(dumpRoot), conf),
                new Path(SnapshotUtils.getSnapshotFileListPath(dumpRoot),
                    EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_CURRENT));
            FileUtils.deleteIfExists(getDFS(SnapshotUtils.getSnapshotFileListPath(dumpRoot), conf),
                new Path(SnapshotUtils.getSnapshotFileListPath(dumpRoot),
                    EximUtil.FILE_LIST_EXTERNAL_SNAPSHOT_OLD));
            // Get the counter to store the snapshots created & deleted at source.
            replSnapshotCount = new SnapshotUtils.ReplSnapshotCount();
          }
          for (String tblName : Utils.matchesTbl(hiveDb, dbName, work.replScope)) {
            Table table = null;
            try {
              HiveWrapper.Tuple<Table> tableTuple = new HiveWrapper(hiveDb, dbName).table(tblName, conf);
              table = tableTuple != null ? tableTuple.object : null;

              //disable materialized-view replication if not configured
              if(tableTuple != null && !isMaterializedViewsReplEnabled()
                      && TableType.MATERIALIZED_VIEW.equals(tableTuple.object.getTableType())){
                LOG.info("Attempt to dump materialized view : " + tblName);
                continue;
              }

              LOG.debug("Dumping table: " + tblName + " to db root " + dbRoot.toUri());
              if (shouldDumpExternalTableLocation(conf)
                      && TableType.EXTERNAL_TABLE.equals(tableTuple.object.getTableType())) {
                LOG.debug("Adding table {} to external tables list", tblName);
                externalTablesWriter
                    .dataLocationDump(tableTuple.object, extTableFileList, singleCopyPaths, !isSingleTaskForExternalDb,
                        conf);
                isExternalTablePresent = true;
              }
              dumpTable(dbName, tblName, validTxnList, dbRoot, dbDataRoot,
                      bootDumpBeginReplId,
                      hiveDb, tableTuple, managedTblList, dataCopyAtLoad);
            } catch (InvalidTableException te) {
              // Bootstrap dump shouldn't fail if the table is dropped/renamed while dumping it.
              // Just log a debug message and skip it.
              LOG.debug(te.getMessage());
            }
            dumpConstraintMetadata(dbName, tblName, dbRoot, hiveDb);
            if (tableList != null && isTableSatifiesConfig(table)) {
              tableList.add(tblName);
            }
          }

          // if it is not a table level replication, add a single task for
          // the database default location and for the configured paths for external tables.
          if (isExternalTablePresent && shouldDumpExternalTableLocation(conf) && isSingleTaskForExternalDb) {
            externalTablesWriter
                .dumpNonTableLevelCopyPaths(singleCopyPaths, extTableFileList, conf, isSnapshotEnabled, snapshotPrefix,
                    replSnapshotCount, snapPathFileList, prevSnaps, true);
          }
          dumpTableListToDumpLocation(tableList, dumpRoot, dbName, conf);
        } catch (Exception e) {
          caught = e;
        } finally {
          try {
            Utils.resetDbBootstrapDumpState(hiveDb, dbName, uniqueKey);
          } catch (Exception e) {
            if (caught == null) {
              throw e;
            } else {
              LOG.error("failed to reset the db state for " + uniqueKey
                      + " on failure of repl dump", e);
              throw caught;
            }
          }
          if (caught != null) {
            throw caught;
          }
        }
        replLogger.endLog(bootDumpBeginReplId.toString());
        work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS, bootDumpBeginReplId, replSnapshotCount,
            replLogger.getReplStatsTracker());
      }
      work.setFunctionCopyPathIterator(functionsBinaryCopyPaths.iterator());
      setDataCopyIterators(extTableFileList, managedTblList);
      LOG.info("Preparing to return {},{}->{}",
        dumpRoot.toUri(), bootDumpBeginReplId, currentNotificationId(hiveDb));
      return bootDumpBeginReplId;
    } finally {
      //write the dmd always irrespective of success/failure to enable checkpointing in table level replication
      Long bootDumpEndReplId = currentNotificationId(hiveDb);
      long executorId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
      dmd.setDump(DumpType.BOOTSTRAP, bootDumpBeginReplId, bootDumpEndReplId, cmRoot, executorId,
        previousReplScopeModified());
      dmd.setReplScope(work.replScope);
      dmd.write(true);
    }
  }

  private void setReplSourceFor(Hive hiveDb, String dbName, Database db) throws HiveException {
    if (!ReplChangeManager.isSourceOfReplication(db)) {
      // Check if the schedule name is available else set the query value
      // as default.
      String value = conf.get(SCHEDULED_QUERY_SCHEDULENAME,
              "default_" + getQueryState().getQueryString());
      updateReplSourceFor(hiveDb, dbName, db, value);
    } else {
      // If a schedule name is available and that isn't part of the
      // existing conf, append the schedule name to the conf.
      String scheduleQuery = conf.get(SCHEDULED_QUERY_SCHEDULENAME);
      if (!StringUtils.isEmpty(scheduleQuery)) {
        if (!getReplPolicyIdString(db).contains(scheduleQuery)) {
          updateReplSourceFor(hiveDb, dbName, db,
                  getReplPolicyIdString(db) + ", " + scheduleQuery);
        }
      }
    }
  }

  private void updateReplSourceFor(Hive hiveDb, String dbName, Database db, String value) throws HiveException {
    Map<String, String> params = db.getParameters();
    if (params != null) {
      params.put("repl.source.for", value);
      db.setParameters(params);
    } else {
      db.setParameters(Collections.singletonMap("repl.source.for", value));
    }
    hiveDb.alterDatabase(dbName, db);
  }

  public static FileList createTableFileList(Path dumpRoot, String fileName, HiveConf conf) {
    Path backingFile = new Path(dumpRoot, fileName);
    return new FileList(backingFile, conf);
  }


  private boolean shouldResumePreviousDump(DumpMetaData dumpMetaData) {
    try {
      return dumpMetaData.getEventFrom() != null;
    } catch (Exception e) {
      LOG.info("No previous dump present");
      return false;
    }
  }

  private boolean shouldResumePreviousDump(Path lastDumpPath, boolean isBootStrap) throws IOException {
    if (validDump(lastDumpPath)) {
      return false;
    }
    Path hiveDumpPath = new Path(lastDumpPath, ReplUtils.REPL_HIVE_BASE_DIR);
    DumpMetaData dumpMetaData = new DumpMetaData(hiveDumpPath, conf);
    if (tableExpressionModified(dumpMetaData)) {
      return false;
    }
    if (isBootStrap) {
      return shouldResumePreviousDump(dumpMetaData);
    }
    // In case of incremental we should resume if _events_dump file is present and is valid
    Path lastEventFile = new Path(hiveDumpPath, ReplAck.EVENTS_DUMP.toString());
    long resumeFrom = 0;
    try {
      resumeFrom = getResumeFrom(lastEventFile);
    } catch (SemanticException ex) {
      LOG.info("Could not get last repl id from {}, because of:", lastEventFile, ex.getMessage());
    }
    return resumeFrom > 0L;
  }

  private boolean tableExpressionModified(DumpMetaData dumpMetaData) {
    try {
      //Check if last dump was with same repl scope. If not table expression was modified. So restart the dump
      //Dont use checkpointing if repl scope if modified
      return !dumpMetaData.getReplScope().equals(work.replScope);
    } catch (Exception e) {
      LOG.info("No previous dump present");
      return false;
    }
  }

  long currentNotificationId(Hive hiveDb) throws TException {
    return hiveDb.getMSC().getCurrentNotificationEventId().getEventId();
  }

  Path dumpDbMetadata(String dbName, Path metadataRoot, long lastReplId, Hive hiveDb) throws Exception {
    // TODO : instantiating FS objects are generally costly. Refactor
    Path dbRoot = new Path(metadataRoot, dbName);
    FileSystem fs = dbRoot.getFileSystem(conf);
    Path dumpPath = new Path(dbRoot, EximUtil.METADATA_NAME);
    HiveWrapper.Tuple<Database> database = new HiveWrapper(hiveDb, dbName, lastReplId).database();
    EximUtil.createDbExportDump(fs, dumpPath, database.object, database.replicationSpec, context.getConf());
    return dbRoot;
  }

  void dumpTable(String dbName, String tblName, String validTxnList, Path dbRootMetadata,
                                       Path dbRootData, long lastReplId, Hive hiveDb,
                                       HiveWrapper.Tuple<Table> tuple, FileList managedTbleList, boolean dataCopyAtLoad)
          throws Exception {
    LOG.info("Bootstrap Dump for table " + tblName);
    TableSpec tableSpec = new TableSpec(tuple.object);
    TableExport.Paths exportPaths =
        new TableExport.Paths(work.astRepresentationForErrorMsg, dbRootMetadata, dbRootData, tblName, conf, true);
    String distCpDoAsUser = conf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
    tuple.replicationSpec.setIsReplace(true);  // by default for all other objects this is false
    if (AcidUtils.isTransactionalTable(tableSpec.tableHandle)) {
      tuple.replicationSpec.setValidTxnList(validTxnList);
      tuple.replicationSpec.setValidWriteIdList(getValidWriteIdList(dbName, tblName, validTxnList));

      // For transactional table, data would be valid snapshot for current txn and doesn't include data
      // added/modified by concurrent txns which are later than current txn. So, need to set last repl Id of this table
      // as bootstrap dump's last repl Id.
      tuple.replicationSpec.setCurrentReplicationState(String.valueOf(lastReplId));
    }
    MmContext mmCtx = MmContext.createIfNeeded(tableSpec.tableHandle);
    tuple.replicationSpec.setRepl(true);
    new TableExport(exportPaths, tableSpec, tuple.replicationSpec, hiveDb, distCpDoAsUser, conf, mmCtx).write(
            false, managedTbleList, dataCopyAtLoad);
    work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.TABLES.name(), 1);
    work.getReplLogger().tableLog(tblName, tableSpec.tableHandle.getTableType());
  }

  private String getValidWriteIdList(String dbName, String tblName, String validTxnString) throws LockException {
    if ((validTxnString == null) || validTxnString.isEmpty()) {
      return null;
    }
    String fullTableName = AcidUtils.getFullTableName(dbName, tblName);
    ValidWriteIdList validWriteIds = getTxnMgr()
            .getValidWriteIds(Collections.singletonList(fullTableName), validTxnString)
            .getTableValidWriteIdList(fullTableName);
    return ((validWriteIds != null) ? validWriteIds.toString() : null);
  }

  private List<Long> getTxnsNotPresentInHiveLocksTable(List<Long> openTxnList) throws LockException {
    List<Long> txnsNotPresentInHiveLocks = new ArrayList<>();
    for (long openTxnId : openTxnList) {
      if (!isTxnPresentInHiveLocks(openTxnId)) {
        txnsNotPresentInHiveLocks.add(openTxnId);
      }
    }
    return txnsNotPresentInHiveLocks;
  }

  /**
   * Get if there is an entry for the txn id in the hive locks table. It can be in waiting state or acquired state.
   * @param txnId
   * @return true if the entry for the txn id is present in hive locks.
   * @throws LockException
   */
  private boolean isTxnPresentInHiveLocks(long txnId) throws LockException {
    ShowLocksRequest request = new ShowLocksRequest();
    request.setTxnid(txnId);
    HiveLockManager lockManager = getTxnMgr().getLockManager();
    ShowLocksResponse showLocksResponse = ((DbLockManager) lockManager).getLocks(request);
    return !showLocksResponse.getLocks().isEmpty();
  }

  List<Long> getOpenTxns(ValidTxnList validTxnList, String dbName) throws LockException {
    HiveLockManager lockManager = getTxnMgr().getLockManager();
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

  // Get list of valid transactions for Repl Dump. Also wait for a given amount of time for the
  // open transactions to finish. Abort any open transactions after the wait is over.
  String getValidTxnListForReplDump(Hive hiveDb, long waitUntilTime) throws HiveException {
    // Key design point for REPL DUMP is to not have any txns older than current txn in which
    // dump runs. This is needed to ensure that Repl dump doesn't copy any data files written by
    // any open txns mainly for streaming ingest case where one delta file shall have data from
    // committed/aborted/open txns. It may also have data inconsistency if the on-going txns
    // doesn't have corresponding open/write events captured which means, catch-up incremental
    // phase won't be able to replicate those txns. So, the logic is to wait for the given amount
    // of time to see if all open txns < current txn is getting aborted/committed. If not, then
    // we forcefully abort those txns just like AcidHouseKeeperService.
    //Exclude readonly and repl created tranasactions
    List<TxnType> excludedTxns = Arrays.asList(TxnType.READ_ONLY, TxnType.REPL_CREATED);
    ValidTxnList validTxnList = getTxnMgr().getValidTxns(excludedTxns);
    while (System.currentTimeMillis() < waitUntilTime) {
      //check if no open txns at all
      List<Long> openTxnListForAllDbs = getOpenTxns(validTxnList);
      if (openTxnListForAllDbs.isEmpty()) {
        return validTxnList.toString();
      }
      //check if all transactions that are open are inserted into the hive locks table. If not wait and check again.
      //Transactions table don't contain the db information. DB information is present only in the hive locks table.
      //Transactions are inserted into the hive locks table after compilation. We need to make sure all transactions
      //that are open have a entry in hive locks which can give us the db information and then we only wait for open
      //transactions for the db under replication and not for all open transactions.
      if (getTxnsNotPresentInHiveLocksTable(openTxnListForAllDbs).isEmpty()) {
        //If all open txns have been inserted in the hive locks table, we just need to check for the db under replication
        // If there are no txns which are open for the given db under replication, then just return it.
        if (getOpenTxns(validTxnList, work.dbNameOrPattern).isEmpty()) {
          return validTxnList.toString();
        }
      }
      // Wait for 5 minutes and check again.
      try {
        Thread.sleep(getSleepTime());
      } catch (InterruptedException e) {
        LOG.info("REPL DUMP thread sleep interrupted", e);
      }
      validTxnList = getTxnMgr().getValidTxns(excludedTxns);
    }

    // After the timeout just force abort the open txns
    if (conf.getBoolVar(REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT)) {
      List<Long> openTxns = getOpenTxns(validTxnList, work.dbNameOrPattern);
      if (!openTxns.isEmpty()) {
        //abort only write transactions for the db under replication if abort transactions is enabled.
        hiveDb.abortTransactions(openTxns);
        validTxnList = getTxnMgr().getValidTxns(excludedTxns);
        openTxns = getOpenTxns(validTxnList, work.dbNameOrPattern);
        if (!openTxns.isEmpty()) {
          LOG.warn("REPL DUMP unable to force abort all the open txns: {} after timeout due to unknown reasons. " +
            "However, this is rare case that shouldn't happen.", openTxns);
          throw new IllegalStateException("REPL DUMP triggered abort txns failed for unknown reasons.");
        }
      }
    } else {
      LOG.warn("Force abort all the open txns is disabled after timeout");
      throw new IllegalStateException("REPL DUMP cannot proceed. Force abort all the open txns is disabled. Enable " +
        "hive.repl.bootstrap.dump.abort.write.txn.after.timeout to proceed.");
    }
    return validTxnList.toString();
  }

  private long getSleepTime() {
    return (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)
      || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)) ? SLEEP_TIME_FOR_TESTS : SLEEP_TIME;
  }

  private List<Long> getOpenTxns(ValidTxnList validTxnList) {
    long[] invalidTxns = validTxnList.getInvalidTransactions();
    List<Long> openTxns = new ArrayList<>();
    for (long invalidTxn : invalidTxns) {
      if (!validTxnList.isTxnAborted(invalidTxn)) {
        openTxns.add(invalidTxn);
      }
    }
    return openTxns;
  }

  private ReplicationSpec getNewReplicationSpec(String evState, String objState,
      boolean isMetadataOnly) {
    return new ReplicationSpec(true, isMetadataOnly, evState, objState, false, true);
  }

  private String getNextDumpDir() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      // make it easy to write .q unit tests, instead of unique id generation.
      // however, this does mean that in writing tests, we have to be aware that
      // repl dump will clash with prior dumps, and thus have to clean up properly.
      String nextDump = ReplDumpWork.getInjectNextDumpDirForTest();
      if (nextDump == null) {
        return "next";
      } else {
        return nextDump;
      }
    } else {
      return UUID.randomUUID().toString();
      // TODO: time good enough for now - we'll likely improve this.
      // We may also work in something the equivalent of pid, thrid and move to nanos to ensure
      // uniqueness.
    }
  }

  List<EximUtil.DataCopyPath> dumpFunctionMetadata(String dbName, Path dbMetadataRoot, Path dbDataRoot,
                                                             Hive hiveDb, boolean copyAtLoad) throws Exception {
    List<EximUtil.DataCopyPath> functionsBinaryCopyPaths = new ArrayList<>();
    Path functionsMetaRoot = new Path(dbMetadataRoot, ReplUtils.FUNCTIONS_ROOT_DIR_NAME);
    Path functionsDataRoot = new Path(dbDataRoot, ReplUtils.FUNCTIONS_ROOT_DIR_NAME);
    List<String> functionNames = hiveDb.getFunctions(dbName, "*");
    for (String functionName : functionNames) {
      HiveWrapper.Tuple<Function> tuple = functionTuple(functionName, dbName, hiveDb);
      if (tuple == null) {
        continue;
      }
      Path functionMetaRoot = new Path(functionsMetaRoot, functionName);
      Path functionMetadataFile = new Path(functionMetaRoot, FUNCTION_METADATA_FILE_NAME);
      Path functionDataRoot = new Path(functionsDataRoot, functionName);
      try (JsonWriter jsonWriter =
          new JsonWriter(functionMetadataFile.getFileSystem(conf), functionMetadataFile)) {
        FunctionSerializer serializer = new FunctionSerializer(tuple.object, functionDataRoot, copyAtLoad, conf);
        serializer.writeTo(jsonWriter, tuple.replicationSpec);
        functionsBinaryCopyPaths.addAll(serializer.getFunctionBinaryCopyPaths());
      }
      work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.FUNCTIONS.name(), 1);
      work.getReplLogger().functionLog(functionName);
    }
    return functionsBinaryCopyPaths;
  }

  void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot, Hive hiveDb) throws Exception {
    try {
      Path constraintsRoot = new Path(dbRoot, ReplUtils.CONSTRAINTS_ROOT_DIR_NAME);
      Path commonConstraintsFile = new Path(constraintsRoot, ConstraintFileType.COMMON.getPrefix() + tblName);
      Path fkConstraintsFile = new Path(constraintsRoot, ConstraintFileType.FOREIGNKEY.getPrefix() + tblName);
      SQLAllTableConstraints tableConstraints = hiveDb.getTableConstraints(dbName, tblName);
      if (CollectionUtils.isNotEmpty(tableConstraints.getPrimaryKeys())
          || CollectionUtils.isNotEmpty(tableConstraints.getUniqueConstraints())
          || CollectionUtils.isNotEmpty(tableConstraints.getNotNullConstraints())
          || CollectionUtils.isNotEmpty(tableConstraints.getCheckConstraints())
          || CollectionUtils.isNotEmpty(tableConstraints.getDefaultConstraints())) {
        try (JsonWriter jsonWriter = new JsonWriter(commonConstraintsFile.getFileSystem(conf), commonConstraintsFile)) {
          ConstraintsSerializer serializer = new ConstraintsSerializer(tableConstraints.getPrimaryKeys(), null,
              tableConstraints.getUniqueConstraints(), tableConstraints.getNotNullConstraints(),
              tableConstraints.getDefaultConstraints(), tableConstraints.getCheckConstraints(), conf);
          serializer.writeTo(jsonWriter, null);
        }
      }
      if (CollectionUtils.isNotEmpty(tableConstraints.getForeignKeys())) {
        try (JsonWriter jsonWriter = new JsonWriter(fkConstraintsFile.getFileSystem(conf), fkConstraintsFile)) {
          ConstraintsSerializer serializer =
              new ConstraintsSerializer(null, tableConstraints.getForeignKeys(), null, null, null, null, conf);
          serializer.writeTo(jsonWriter, null);
        }
      }
    } catch (NoSuchObjectException e) {
      // Bootstrap constraint dump shouldn't fail if the table is dropped/renamed while dumping it.
      // Just log a debug message and skip it.
      LOG.debug(e.getMessage());
    }
  }

  private HiveWrapper.Tuple<Function> functionTuple(String functionName, String dbName, Hive hiveDb) {
    try {
      HiveWrapper.Tuple<Function> tuple = new HiveWrapper(hiveDb, dbName).function(functionName);
      if (tuple.object.getResourceUris().isEmpty()) {
        LOG.warn("Not replicating function: " + functionName + " as it seems to have been created "
                + "without USING clause");
        return null;
      }
      return tuple;
    } catch (HiveException e) {
      //This can happen as we are querying the getFunctions before we are getting the actual function
      //in between there can be a drop function by a user in which case our call will fail.
      LOG.info("Function " + functionName
          + " could not be found, we are ignoring it as it can be a valid state ", e);
      return null;
    }
  }

  @Override
  public StageType getType() {
    return StageType.REPL_DUMP;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
