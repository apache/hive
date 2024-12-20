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
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.repl.ReplConst;
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
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.CatalogFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.ReplEventFilter;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
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
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.EventUtils;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.ExportService;
import org.apache.hadoop.hive.ql.parse.repl.dump.EventsDumpMetadata;
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
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.OptimizedBootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.PreOptimizedBootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata.ReplicationType;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.ExportWork.MmContext;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_RESUME_STARTED_AFTER_FAILOVER;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DB_PROPERTY;
import static org.apache.hadoop.hive.common.repl.ReplConst.TARGET_OF_REPLICATION;
import static org.apache.hadoop.hive.conf.Constants.SCHEDULED_QUERY_SCHEDULENAME;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.getReplPolicyIdString;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_COMPLETE_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.checkFileExists;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.createAndGetEventAckFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.createBootstrapTableList;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getEventIdFromFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getReplEventIdFromDatabase;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTablesFromTableDiffFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTargetEventId;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.isDbTargetOfFailover;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.isFirstIncrementalPending;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.NON_RECOVERABLE_MARKER;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_AUTHORIZER;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.getOpenTxns;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.cleanupSnapshots;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.getDFS;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.getListFromFileList;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_SOURCE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_TARGET;

public class ReplDumpTask extends Task<ReplDumpWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final String dumpSchema = "dump_dir,last_repl_id#string,string";
  private static final String FUNCTION_METADATA_FILE_NAME = EximUtil.METADATA_NAME;
  private static final long SLEEP_TIME = 5 * 60000;
  private static final long SLEEP_TIME_FOR_TESTS = 30000;
  private Set<String> tablesForBootstrap = new HashSet<>();
  private List<TxnType> excludedTxns = Arrays.asList(TxnType.READ_ONLY, TxnType.REPL_CREATED);
  private boolean createEventMarker = false;

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
        Path latestDumpPath = ReplUtils.getLatestDumpPath(dumpRoot, conf);
        if (ReplUtils.failedWithNonRecoverableError(latestDumpPath, conf)) {
          LOG.error("Previous dump failed with non recoverable error. Needs manual intervention. ");
          Path nonRecoverableFile = new Path(latestDumpPath, NON_RECOVERABLE_MARKER.toString());
          ReplUtils.reportStatusInReplicationMetrics(getName(), Status.SKIPPED, nonRecoverableFile.toString(), conf,  work.dbNameOrPattern, null);
          setException(new SemanticException(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.format()));
          return ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode();
        }
        Path previousValidHiveDumpPath = getPreviousValidDumpMetadataPath(dumpRoot);
        boolean isFailoverMarkerPresent = false;
        boolean isFailoverTarget = isDbTargetOfFailover(work.dbNameOrPattern, getHive());
        LOG.debug("Database {} is {} going through failover", work.dbNameOrPattern, isFailoverTarget ? "" : "not");
        if (previousValidHiveDumpPath == null && !isFailoverTarget) {
          work.setBootstrap(true);
        } else {
          work.setOldReplScope(isFailoverTarget ? null : new DumpMetaData(previousValidHiveDumpPath, conf).getReplScope());
          isFailoverMarkerPresent = !isFailoverTarget && isDumpFailoverReady(previousValidHiveDumpPath);
        }
        //Proceed with dump operation in following cases:
        //1. No previous dump is present.
        //2. Previous dump is already loaded and it is not in failover ready status.
        if (shouldDump(previousValidHiveDumpPath, isFailoverMarkerPresent, isFailoverTarget)) {
          Path currentDumpPath = getCurrentDumpPath(dumpRoot, work.isBootstrap());
          Path hiveDumpRoot = new Path(currentDumpPath, ReplUtils.REPL_HIVE_BASE_DIR);
          if (!work.isBootstrap() && !isFailoverTarget) {
            preProcessFailoverIfRequired(previousValidHiveDumpPath, isFailoverMarkerPresent);
          }
          // check if we need to create event marker
          if (previousValidHiveDumpPath == null) {
            createEventMarker = isFailoverTarget;
          } else {
            if (isFailoverTarget) {
              boolean isEventAckFilePresent = checkFileExists(previousValidHiveDumpPath.getParent(), conf, EVENT_ACK_FILE);
              if (!isEventAckFilePresent) {
                // If this is optimised bootstrap failover cycle and _event_ack file is not present, then create it
                createEventMarker = true;
              }
            }
          }
          // Set distCp custom name corresponding to the replication policy.
          String mapRedCustomName = ReplUtils.getDistCpCustomName(conf, work.dbNameOrPattern);
          conf.set(JobContext.JOB_NAME, mapRedCustomName);
          work.setCurrentDumpPath(currentDumpPath);
          // Initialize repl dump metric collector for all replication stage (Bootstrap, incremental, pre-optimised and optimised bootstrap)
          ReplicationMetricCollector dumpMetricCollector = initReplicationDumpMetricCollector(hiveDumpRoot, work.isBootstrap(), createEventMarker /*isPreOptimisedBootstrap*/, isFailoverTarget);
          work.setMetricCollector(dumpMetricCollector);
          if (shouldDumpAtlasMetadata()) {
            addAtlasDumpTask(work.isBootstrap(), previousValidHiveDumpPath);
            LOG.info("Added task to dump atlas metadata.");
          }
          if (shouldDumpAuthorizationMetadata()) {
            initiateAuthorizationDumpTask();
          }
          DumpMetaData dmd = new DumpMetaData(hiveDumpRoot, conf);
          // Initialize ReplChangeManager instance since we will require it to encode file URI.
          ReplChangeManager.getInstance(conf);
          Path cmRoot = new Path(conf.getVar(HiveConf.ConfVars.REPL_CM_DIR));
          Long lastReplId;
          LOG.info("Data copy at load enabled : {}", conf.getBoolVar(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET));
          if (isFailoverTarget) {
            if (createEventMarker) {
              LOG.info("Optimised Bootstrap Dump triggered for {}.", work.dbNameOrPattern);
              // Before starting optimised bootstrap, check if the first incremental is done to ensure database is in
              // consistent state.
              isFirstIncrementalPending(work.dbNameOrPattern, getHive());
              Database database = getHive().getDatabase(work.dbNameOrPattern);
              if (database != null) {
                HashMap<String, String> params = new HashMap<>(database.getParameters());
                long failbackStartTime = System.currentTimeMillis();
                params.put(ReplConst.REPL_METRICS_LAST_FAILBACK_STARTTIME, Long.toString(failbackStartTime));
                LOG.info("Replication Metrics: Setting replication metrics failback start time for database: {} to: {} ", work.dbNameOrPattern, failbackStartTime);
                if (!MetaStoreUtils.isDbBeingPlannedFailedOver(database)) { // if this is failback due to unplanned failover
                  LOG.info("Replication Metrics: Setting last failover type for database: {} to: {} ", work.dbNameOrPattern, ReplConst.FailoverType.UNPLANNED.toString());
                  params.put(ReplConst.REPL_METRICS_LAST_FAILOVER_TYPE, ReplConst.FailoverType.UNPLANNED.toString());

                  int failoverCount = 1 + NumberUtils.toInt(params.getOrDefault(ReplConst.REPL_METRICS_FAILOVER_COUNT, "0"), 0);
                  LOG.info("Replication Metrics: Setting replication metrics failover count for database: {} to: {} ", work.dbNameOrPattern, failoverCount);
                  params.put(ReplConst.REPL_METRICS_FAILOVER_COUNT, Integer.toString(failoverCount));
                }
                database.setParameters(params);
                getHive().alterDatabase(work.dbNameOrPattern, database);
              } else {
                LOG.debug("Database {} does not exist. Cannot set replication failover failback metrics", work.dbNameOrPattern);
              }
              // Get the last replicated event id from the database.
              String dbEventId = getReplEventIdFromDatabase(work.dbNameOrPattern, getHive());
              // Get the last replicated event id from the database with respect to target.
              String targetDbEventId = getTargetEventId(work.dbNameOrPattern, getHive());

              LOG.info("Creating event_ack file for database {} with event id {}.", work.dbNameOrPattern, dbEventId);
              Map<String, Long> metricMap = new HashMap<>();
              metricMap.put(ReplUtils.MetricName.EVENTS.name(), 0L);
              work.getMetricCollector().reportStageStart(getName(), metricMap);
              lastReplId =
                  createAndGetEventAckFile(currentDumpPath, dmd, cmRoot, dbEventId, targetDbEventId, conf, work);
              finishRemainingTasks();
              work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS);
            } else {
              // We should be here only if TableDiff is Present.
              boolean isTableDiffDirectoryPresent =
                      checkFileExists(previousValidHiveDumpPath.getParent(), conf, TABLE_DIFF_COMPLETE_DIRECTORY);
              boolean isAbortTxnsListPresent =
                      checkFileExists(previousValidHiveDumpPath.getParent(), conf, OptimisedBootstrapUtils.ABORT_TXNS_FILE);

              assert isTableDiffDirectoryPresent;

              work.setSecondDumpAfterFailover(true);
              long fromEventId = Long.parseLong(getEventIdFromFile(previousValidHiveDumpPath.getParent(), conf)[1]);
              LOG.info("Starting optimised bootstrap from event id {} for database {}", fromEventId,
                  work.dbNameOrPattern);
              work.setEventFrom(fromEventId);

              // Get the tables to be bootstrapped from the table diff
              tablesForBootstrap = getTablesFromTableDiffFile(previousValidHiveDumpPath.getParent(), conf);
              if (isAbortTxnsListPresent) {
                abortReplCreatedTxnsPriorToFailover(previousValidHiveDumpPath.getParent(), conf);
              }

              // Generate the bootstrapped table list and put it in the new dump directory for the load to consume.
              createBootstrapTableList(currentDumpPath, tablesForBootstrap, conf);

              dumpDbMetadata(work.dbNameOrPattern, new Path(hiveDumpRoot, EximUtil.METADATA_PATH_NAME),
                      fromEventId, getHive());
              // Call the normal dump with the tablesForBootstrap set.
              lastReplId =  incrementalDump(hiveDumpRoot, dmd, cmRoot, getHive());
            }
          }
          else if (work.isBootstrap()) {
            lastReplId = bootStrapDump(hiveDumpRoot, dmd, cmRoot, getHive());
          } else {
            work.setEventFrom(getEventFromPreviousDumpMetadata(previousValidHiveDumpPath));
            lastReplId = incrementalDump(hiveDumpRoot, dmd, cmRoot, getHive());
          }
          // The datacopy doesn't need to be initialised in case of optimised bootstrap first dump.
          if (lastReplId >= 0) {
            work.setResultValues(Arrays.asList(currentDumpPath.toUri().toString(), String.valueOf(lastReplId)));
            initiateDataCopyTasks();
          }
        } else {
          if (isFailoverMarkerPresent) {
            LOG.info("Previous Dump is failover ready. Skipping this iteration.");
          } else {
            LOG.info("Previous Dump is not yet loaded. Skipping this iteration.");
          }
          ReplUtils.reportStatusInReplicationMetrics(getName(), Status.SKIPPED, null, conf,
                  work.dbNameOrPattern, work.isBootstrap() ? ReplicationType.BOOTSTRAP: ReplicationType.INCREMENTAL);
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

  private void abortReplCreatedTxnsPriorToFailover(Path dumpPath, HiveConf conf) throws LockException, IOException {
    List<Long> replCreatedTxnsToAbort = OptimisedBootstrapUtils.getTxnIdFromAbortTxnsFile(dumpPath, conf);
    String replPolicy = HiveUtils.getReplPolicy(work.dbNameOrPattern);
    HiveTxnManager hiveTxnManager = getTxnMgr();
    for (Long txnId : replCreatedTxnsToAbort) {
      LOG.info("Rolling back Repl_Created txns:" + replCreatedTxnsToAbort.toString() + " opened prior to failover.");
      hiveTxnManager.replRollbackTxn(replPolicy, txnId);
    }
  }

  private void preProcessFailoverIfRequired(Path previousValidHiveDumpDir, boolean isPrevFailoverReadyMarkerPresent)
          throws HiveException, IOException {
    FileSystem fs = previousValidHiveDumpDir.getFileSystem(conf);
    Database db = getHive().getDatabase(work.dbNameOrPattern);
    if (isPrevFailoverReadyMarkerPresent) {
      if (MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.SOURCE)) {
        //Since previous valid dump is failover ready and repl.failover.endpoint is set for source, just rollback
        // the failover process initiated in the previous iteration.
        LOG.info("Rolling back failover initiated in previous dump iteration.");
        fs.delete(new Path(previousValidHiveDumpDir, ReplAck.FAILOVER_READY_MARKER.toString()), true);
      } else if (MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET)) {
        //Since previous valid dump is failover ready and repl.failover.endpoint is set for target,
        // this means it is first dump operation in the reverse direction.
        LOG.info("Switching to bootstrap dump as this is the first dump execution after failover.");
        work.setFirstDumpAfterFailover(true);
      }
    }
    if (!shouldFailover() && !work.isFirstDumpAfterFailover()) {
      //If this is first dump operation, don't unset this property until first incremental dump.
      ReplUtils.unsetDbPropIfSet(db, ReplConst.REPL_FAILOVER_ENDPOINT, getHive());
    }
  }

  private boolean isDumpFailoverReady(Path previousValidHiveDumpPath) throws IOException {
    FileSystem fs = previousValidHiveDumpPath.getFileSystem(conf);
    Path failoverReadyMarkerFile = new Path(previousValidHiveDumpPath, ReplAck.FAILOVER_READY_MARKER.toString());
    return fs.exists(failoverReadyMarkerFile);
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
      if (!shouldFailover()) {
        //If failoverReadyMarker was created in previous dump iteration, Just delete and proceed.
        Path hiveDumpRoot = new Path(lastDumpPath, ReplUtils.REPL_HIVE_BASE_DIR);
        Path failoverReadyMarkerFile = new Path(hiveDumpRoot, ReplAck.FAILOVER_READY_MARKER.toString());
        FileSystem fs = failoverReadyMarkerFile.getFileSystem(conf);
        if (fs.exists(failoverReadyMarkerFile)) {
          LOG.info("Deleting previous failover ready marker file: {}.", failoverReadyMarkerFile);
          fs.delete(failoverReadyMarkerFile, true);
        }
      }
      work.setShouldOverwrite(true);
      return lastDumpPath;
    } else {
      return new Path(dumpRoot, getNextDumpDir());
    }
  }

  private void initiateDataCopyTasks() throws HiveException, IOException {
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


  private void finishRemainingTasks() throws HiveException {
    Database database = getHive().getDatabase(work.dbNameOrPattern);
    boolean isFailoverInProgress = shouldFailover() && !work.isBootstrap() && !createEventMarker;
    if (isFailoverInProgress) {
      Utils.create(new Path(work.getCurrentDumpPath(), ReplUtils.REPL_HIVE_BASE_DIR + File.separator
              + ReplAck.FAILOVER_READY_MARKER), conf);
      LOG.info("Dump marked as failover ready.");
    }
    Path dumpAckFile = new Path(work.getCurrentDumpPath(), ReplUtils.REPL_HIVE_BASE_DIR + File.separator
                    + ReplAck.DUMP_ACKNOWLEDGEMENT);

    // Check if we need to unset database properties after successful optimised bootstrap.
    if (work.isSecondDumpAfterFailover()) {
      if (database != null) {
        HashMap<String, String> dbParams = new HashMap<>(database.getParameters());
        LOG.debug("Database {} params before removal {}", work.dbNameOrPattern, dbParams);
        dbParams.remove(TARGET_OF_REPLICATION);
        dbParams.remove(CURR_STATE_ID_TARGET.toString());
        dbParams.remove(CURR_STATE_ID_SOURCE.toString());
        dbParams.remove(REPL_TARGET_DB_PROPERTY);
        dbParams.remove(ReplConst.REPL_ENABLE_BACKGROUND_THREAD);
        dbParams.remove(REPL_RESUME_STARTED_AFTER_FAILOVER);
        if (!isFailoverInProgress) {
          // if we have failover endpoint from controlled failover remove it.
          dbParams.remove(ReplConst.REPL_FAILOVER_ENDPOINT);
        }

        LOG.info("Removing {} property from the database {} after successful optimised bootstrap dump", String.join(",",
            new String[]{TARGET_OF_REPLICATION, CURR_STATE_ID_TARGET.toString(), CURR_STATE_ID_SOURCE.toString(),
                REPL_TARGET_DB_PROPERTY}), work.dbNameOrPattern);

        int failbackCount = 1 + NumberUtils.toInt(dbParams.getOrDefault(ReplConst.REPL_METRICS_FAILBACK_COUNT, "0"), 0);
        LOG.info("Replication Metrics: Setting replication metrics failback count for database: {} to: {} ", work.dbNameOrPattern, failbackCount);
        dbParams.put(ReplConst.REPL_METRICS_FAILBACK_COUNT, Integer.toString(failbackCount));

        long failbackEndTime = System.currentTimeMillis();
        dbParams.put(ReplConst.REPL_METRICS_LAST_FAILBACK_ENDTIME, Long.toString(failbackEndTime));
        LOG.info("Replication Metrics: Setting replication metrics failback end time for database: {} to: {} ", work.dbNameOrPattern, failbackEndTime);

        database.setParameters(dbParams);
        getHive().alterDatabase(work.dbNameOrPattern, database);
        LOG.debug("Database {} params after removal {}", work.dbNameOrPattern, dbParams);
      } else {
        LOG.debug("Database {} does not exist. Cannot set replication failover and failback metrics", work.dbNameOrPattern);
      }
    }
    Utils.create(dumpAckFile, conf);
    prepareReturnValues(work.getResultValues());
    if (isFailoverInProgress) {
      work.getMetricCollector().reportEnd(Status.FAILOVER_READY);
    } else {
      work.getMetricCollector().reportEnd(isFirstCycleOfResume(database) ?
                                          Status.RESUME_READY :
                                          Status.SUCCESS);
    }
    deleteAllPreviousDumpMeta(work.getCurrentDumpPath());
  }

  private boolean isFirstCycleOfResume(Database database) {
    return createEventMarker && database.getParameters().containsKey(REPL_RESUME_STARTED_AFTER_FAILOVER);
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
          if (numDumpDirs == 1 && work.isFirstDumpAfterFailover()) {
            LOG.info("Skipping deletion of last failover ready dump dir: ", status.getPath());
            break;
          }
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

  private boolean shouldDump(Path previousDumpPath, boolean isFailoverMarkerPresent, boolean isFailover)
      throws IOException, HiveException {
    /** a) If there is no previous dump dir found, the current run is bootstrap case.
     * b) If the previous dump was successful and it contains failover marker file as well as
     * HiveConf.ConfVars.HIVE_REPL_FAILOVER_START == true, last dump was a controlled failover dump,
     * skip doing any further dump.
     */
    if (previousDumpPath == null) {
      return true;
    } else if (isFailoverMarkerPresent && shouldFailover()) {
      return false;
    } else if (isFailover) {
      // In case of OptimisedBootstrap Failover, We need to do a dump in case:
      // 1. No EVENT_ACK file is there.
      // 2. EVENT_ACK file and TABLE_DIFF_COMPLETE file is also there and the current database id is same as that in
      // the EVENT_ACK file
      boolean isEventAckFilePresent = checkFileExists(previousDumpPath.getParent(), conf, EVENT_ACK_FILE);
      if (!isEventAckFilePresent) {
        // If in the previous valid dump path, Event_Ack isn't there that means the previous one was a normal dump,
        // we need to trigger the failover dump
        LOG.debug("EVENT_ACK file not found in {}. Proceeding with OptimisedBootstrap Failover",
            previousDumpPath.getParent());
        return true;
      }
      // Event_ACK file is present check if it contains correct value or not.
      String fileEventId = getEventIdFromFile(previousDumpPath.getParent(), conf)[0];
      String dbEventId = getReplEventIdFromDatabase(work.dbNameOrPattern, getHive()).trim();
      if (!dbEventId.equalsIgnoreCase(fileEventId)) {
        // In case the database event id changed post table_diff_complete generation, that means both forward &
        // backward policies are operational, We fail in that case with non-recoverable error.
        LOG.error("The database eventID {} and the event id in the EVENT_ACK file {} both mismatch. FilePath {}",
            dbEventId, fileEventId, previousDumpPath.getParent());
        throw new RuntimeException("Database event id changed post table diff generation.");
      } else {
        // Check table_diff_complete and Load_ACK
        return checkFileExists(previousDumpPath.getParent(), conf, TABLE_DIFF_COMPLETE_DIRECTORY) && checkFileExists(previousDumpPath,
            conf, LOAD_ACKNOWLEDGEMENT.toString());
      }
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

  private boolean shouldFailover() {
    return conf.getBoolVar(HiveConf.ConfVars.HIVE_REPL_FAILOVER_START);
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

  private boolean doesTableSatisfyConfig(Table table) {
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

  private void fetchFailoverMetadata(Hive hiveDb) throws HiveException, IOException, TException {
    Path hiveDumpDir = new Path(work.getCurrentDumpPath(), ReplUtils.REPL_HIVE_BASE_DIR);
    FailoverMetaData fmd = new FailoverMetaData(hiveDumpDir, conf);
    FileSystem fs = hiveDumpDir.getFileSystem(conf);
    if (fs.exists(new Path(hiveDumpDir, FailoverMetaData.FAILOVER_METADATA)) && fmd.isValidMetadata()) {
      work.setFailoverMetadata(fmd);
      return;
    }
    HiveTxnManager hiveTxnManager = getTxnMgr();
    List<Long> txnsForDb = getOpenTxns(hiveTxnManager, hiveTxnManager.getValidTxns(excludedTxns), work.dbNameOrPattern);
    if (!txnsForDb.isEmpty()) {
      LOG.debug("Going to abort transactions: {} for database: {}.", txnsForDb, work.dbNameOrPattern);
      hiveDb.abortTransactions(txnsForDb, TxnErrorMsg.ABORT_FETCH_FAILOVER_METADATA.getErrorCode());
    }
    fmd.setAbortedTxns(txnsForDb);
    fmd.setCursorPoint(currentNotificationId(hiveDb));
    ValidTxnList allValidTxns = getTxnMgr().getValidTxns(excludedTxns);
    List<Long> openTxns = getOpenTxns(allValidTxns);
    fmd.setOpenTxns(openTxns);
    fmd.setTxnsWithoutLock(getTxnsNotPresentInHiveLocksTable(openTxns));
    txnsForDb = getOpenTxns(hiveTxnManager, allValidTxns, work.dbNameOrPattern);
    if (!txnsForDb.isEmpty()) {
      LOG.debug("Going to abort transactions: {} for database: {}.", txnsForDb, work.dbNameOrPattern);
      hiveDb.abortTransactions(txnsForDb, TxnErrorMsg.ABORT_FETCH_FAILOVER_METADATA.getErrorCode());
      fmd.addToAbortedTxns(txnsForDb);
    }
    fmd.setFailoverEventId(currentNotificationId(hiveDb));
    fmd.write();
    work.setFailoverMetadata(fmd);
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

    String dbName = work.dbNameOrPattern;
    Database db = hiveDb.getDatabase(dbName);
    if (!HiveConf.getBoolVar(conf, REPL_DUMP_METADATA_ONLY)) {
      setReplSourceFor(hiveDb, dbName, db);
    }
    if (shouldFailover()) {
      if (!MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.SOURCE)) {
        // set repl failover enabled at source
        HashMap<String, String> params = new HashMap<>(db.getParameters());
        params.put(ReplConst.REPL_FAILOVER_ENDPOINT, MetaStoreUtils.FailoverEndpoint.SOURCE.toString());

        params.put(ReplConst.REPL_METRICS_LAST_FAILOVER_TYPE, ReplConst.FailoverType.PLANNED.toString());
        LOG.info("Replication Metrics: Setting last failover type for database: {} to: {} ", dbName, ReplConst.FailoverType.PLANNED.toString());

        int failoverCount = 1 + NumberUtils.toInt(params.getOrDefault(ReplConst.REPL_METRICS_FAILOVER_COUNT, "0"), 0);
        LOG.info("Replication Metrics: Setting replication metrics failover count for target database: {} to: {} ", dbName, failoverCount);
        params.put(ReplConst.REPL_METRICS_FAILOVER_COUNT, Integer.toString(failoverCount));

        db.setParameters(params);
        getHive().alterDatabase(work.dbNameOrPattern, db);
      }
      fetchFailoverMetadata(hiveDb);
      assert work.getFailoverMetadata().isValidMetadata();
      work.overrideLastEventToDump(hiveDb, bootDumpBeginReplId, work.getFailoverMetadata().getFailoverEventId());
    } else {
      work.overrideLastEventToDump(hiveDb, bootDumpBeginReplId, -1);
    }
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
    boolean shouldBatch = conf.getBoolVar(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS);

    EventsDumpMetadata eventsDumpMetadata =
            Utils.fileExists(ackFile, conf) ? EventsDumpMetadata.deserialize(ackFile, conf)
                    : new EventsDumpMetadata(work.eventFrom, 0, shouldBatch);

    long resumeFrom = eventsDumpMetadata.getLastReplId();

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
      int size = tablesForBootstrap.size();
      if (db != null && db.getParameters()!=null &&
        Boolean.parseBoolean(db.getParameters().get(REPL_RESUME_STARTED_AFTER_FAILOVER))) {
        Collection<String> allTables = Utils.getAllTables(hiveDb, dbName, work.replScope);
        allTables.retainAll(tablesForBootstrap);
        size = allTables.size();
      }
      if (size > 0) {
        metricMap.put(ReplUtils.MetricName.TABLES.name(), (long) size);
      }
      if (shouldFailover()) {
        Map<String, String> params = db.getParameters();
        String dbFailoverEndPoint = "";
        if (params != null) {
          dbFailoverEndPoint = params.get(ReplConst.REPL_FAILOVER_ENDPOINT);
          LOG.debug("Replication Metrics: setting failover endpoint to {} ", dbFailoverEndPoint);
        } else {
          LOG.warn("Replication Metrics: Cannot obtained failover endpoint info, setting failover endpoint to null ");
        }
        work.getMetricCollector().reportFailoverStart(getName(), metricMap, work.getFailoverMetadata(), dbFailoverEndPoint, ReplConst.FailoverType.PLANNED.toString());
      } else {
        work.getMetricCollector().reportStageStart(getName(), metricMap);
      }
      long dumpedCount = resumeFrom - work.eventFrom;
      if (dumpedCount > 0) {
        LOG.info("Event id {} to {} are already dumped, skipping {} events", work.eventFrom, resumeFrom, dumpedCount);
      }
      boolean isStagingDirCheckedForFailedEvents = false;

      int batchNo = 0, eventCount = 0;
      final int maxEventsPerBatch = conf.getIntVar(HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS);
      Path eventRootDir = dumpRoot;

      if (shouldBatch && maxEventsPerBatch == 0) {
        throw new SemanticException(String.format(
                "batch size configured via %s cannot be set to zero since batching is enabled",
                HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS.varname));
      }
      if (eventsDumpMetadata.isEventsBatched() != shouldBatch) {
        LOG.error("Failed to resume from previous dump. {} was set to {} in previous dump but currently it's" +
                        " set to {}. Cannot dump events in {} manner because they were {} batched in " +
                        "the previous incomplete run",
                HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS.varname, eventsDumpMetadata.isEventsBatched(),
                shouldBatch, shouldBatch ? "batched" : "sequential", shouldBatch ? "not" : ""
        );

        throw new HiveException(
                String.format("Failed to resume from previous dump. %s must be set to %s, but currently it's set to %s",
                        HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS,
                        eventsDumpMetadata.isEventsBatched(), shouldBatch)
        );
      }

      while (evIter.hasNext()) {
        NotificationEvent ev = evIter.next();
        lastReplId = ev.getEventId();

        if (shouldBatch && eventCount++ % maxEventsPerBatch == 0) {
          eventRootDir = new Path(dumpRoot, String.format(ReplUtils.INC_EVENTS_BATCH, ++batchNo));
        }
        if (ev.getEventId() <= resumeFrom) {
          continue;
        }
        // Checking and removing remnant file from staging directory if previous incremental repl dump is failed
        if (!isStagingDirCheckedForFailedEvents) {
          cleanFailedEventDirIfExists(eventRootDir, ev.getEventId());
          isStagingDirCheckedForFailedEvents = true;
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

        Path eventDir = new Path(eventRootDir, String.valueOf(lastReplId));
        dumpEvent(ev, eventDir, dumpRoot, cmRoot, hiveDb, eventsDumpMetadata);
        eventsDumpMetadata.setLastReplId(lastReplId);
        Utils.writeOutput(eventsDumpMetadata.serialize(), ackFile, conf);
      }
      replLogger.endLog(lastReplId.toString());
      LOG.info("Done dumping events, preparing to return {},{}", dumpRoot.toUri(), lastReplId);
    } finally {
      //write the dmd always irrespective of success/failure to enable checkpointing in table level replication
      long executionId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
      if (work.isSecondDumpAfterFailover()){
        dmd.setDump(DumpType.OPTIMIZED_BOOTSTRAP, work.eventFrom, lastReplId, cmRoot, executionId,
                previousReplScopeModified());
      }
      else {
        dmd.setDump(DumpType.INCREMENTAL, work.eventFrom, lastReplId, cmRoot, executionId,
                previousReplScopeModified());
      }
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
        ExportService exportService = new ExportService(conf);
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
                dumpTable(exportService, matchedDbName, tableName, validTxnList, dbRootMetadata, dbRootData, bootDumpBeginReplId,
                        hiveDb, tableTuple, managedTblList, dataCopyAtLoad);
              }
              if (tableList != null && doesTableSatisfyConfig(table)) {
                tableList.add(tableName);
              }
            } catch (InvalidTableException te) {
              // Repl dump shouldn't fail if the table is dropped/renamed while dumping it.
              // Just log a debug message and skip it.
              LOG.debug(te.getMessage());
            }
          }

          if (exportService != null && exportService.isExportServiceRunning()) {
            try {
              exportService.waitForTasksToFinishAndShutdown();
            } catch (SemanticException e) {
              LOG.error("ExportService thread failed to perform table dump operation ", e.getCause());
              throw new SemanticException(e.getMessage(), e);
            }
            try {
              exportService.await(60, TimeUnit.SECONDS);
            } catch (Exception e) {
              LOG.error("Error while shutting down ExportService ", e);
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

  private ReplicationMetricCollector initReplicationDumpMetricCollector(Path dumpRoot, boolean isBootstrap, boolean isPreOptimisedBootstrap, boolean isFailover) throws HiveException {
    ReplicationMetricCollector collector;
    long executorId = conf.getLong(Constants.SCHEDULED_QUERY_EXECUTIONID, 0L);
    if (isBootstrap) {
      collector = new BootstrapDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf, executorId);
    } else if (isFailover) {
      // db property ReplConst.FAILOVER_ENDPOINT is only set during planned failover.
      String failoverType = MetaStoreUtils.isDbBeingPlannedFailedOver(getHive().getDatabase(work.dbNameOrPattern)) ?
          ReplConst.FailoverType.PLANNED.toString() : ReplConst.FailoverType.UNPLANNED.toString();
      if (isPreOptimisedBootstrap) {
        collector = new PreOptimizedBootstrapDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf, executorId,
            MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), failoverType);
      } else {
        collector = new OptimizedBootstrapDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf, executorId,
            MetaStoreUtils.FailoverEndpoint.SOURCE.toString(), failoverType);
      }
    } else {
      collector = new IncrementalDumpMetricCollector(work.dbNameOrPattern, dumpRoot.toString(), conf, executorId);
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

  private void cleanFailedEventDirIfExists(Path dumpDir, long eventId) throws SemanticException {
    Path eventRoot = new Path(dumpDir, String.valueOf(eventId));
    Retryable retryable = Retryable.builder()
      .withHiveConf(conf)
      .withRetryOnException(IOException.class).build();
    try {
      retryable.executeCallable((Callable<Void>) () -> {
        try {
          FileSystem fs = FileSystem.get(eventRoot.toUri(), conf);
          if (fs.exists(eventRoot))  {
            fs.delete(eventRoot, true);
          }
        } catch (FileNotFoundException e) {
          // no worries
        }
        return null;
      });
    } catch (Exception e) {
      throw new SemanticException(e);
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
    return !work.replScope.includeAllTables() || previousReplScopeModified() || !tablesForBootstrap.isEmpty()
            || conf.getBoolVar(HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES);
  }

  private void dumpEvent(NotificationEvent ev, Path evRoot, Path dumpRoot, Path cmRoot, Hive db,
                         EventsDumpMetadata eventsDumpMetadata) throws Exception {
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
    if (context.isDmdCreated()) {
      eventsDumpMetadata.incrementEventsDumpedCount();
      work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.EVENTS.name(), 1);
    }
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
      ExportService exportService = new ExportService(conf);
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
              HiveWrapper.Tuple<Table> tableTuple = createHiveWrapper(hiveDb, dbName).table(tblName, conf);
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
              dumpTable(exportService, dbName, tblName, validTxnList, dbRoot, dbDataRoot,
                      bootDumpBeginReplId,
                      hiveDb, tableTuple, managedTblList, dataCopyAtLoad);
            } catch (InvalidTableException te) {
              // Bootstrap dump shouldn't fail if the table is dropped/renamed while dumping it.
              // Just log a debug message and skip it.
              LOG.debug(te.getMessage());
            }
            dumpConstraintMetadata(dbName, tblName, dbRoot, hiveDb, table != null ? table.getTTable().getId() : -1);
            if (tableList != null && doesTableSatisfyConfig(table)) {
              tableList.add(tblName);
            }
          }

          if (exportService != null && exportService.isExportServiceRunning()) {
            try {
              exportService.waitForTasksToFinishAndShutdown();
            } catch (SemanticException e) {
              LOG.error("ExportService thread failed to perform table dump operation ", e.getCause());
              throw new SemanticException(e.getMessage(), e);
            }
            try {
              exportService.await(60, TimeUnit.SECONDS);
            } catch (Exception e) {
              LOG.error("Error while shutting down ExportService ", e);
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
      params.put(SOURCE_OF_REPLICATION, value);
    } else {
      db.setParameters(Collections.singletonMap(SOURCE_OF_REPLICATION, value));
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
      resumeFrom = EventsDumpMetadata.deserialize(lastEventFile, conf).getLastReplId();
    } catch (HiveException ex) {
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

  void dumpTable(ExportService exportService, String dbName, String tblName, String validTxnList, Path dbRootMetadata,
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
    TableExport tableExport = new TableExport(exportPaths, tableSpec, tuple.replicationSpec, hiveDb, distCpDoAsUser, conf, mmCtx);
    if (exportService != null && exportService.isExportServiceRunning()) {
      tableExport.parallelWrite(exportService,false, managedTbleList, dataCopyAtLoad);
    } else {
      tableExport.serialWrite(false, managedTbleList, dataCopyAtLoad);
    }
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
    //Exclude readonly and repl created transactions
    HiveTxnManager hiveTxnManager = getTxnMgr();
    ValidTxnList validTxnList = hiveTxnManager.getValidTxns(excludedTxns);
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
        if (getOpenTxns(hiveTxnManager, validTxnList, work.dbNameOrPattern).isEmpty()) {
          return validTxnList.toString();
        }
      }
      // Wait for 5 minutes and check again.
      try {
        Thread.sleep(getSleepTime());
      } catch (InterruptedException e) {
        LOG.info("REPL DUMP thread sleep interrupted", e);
      }
      validTxnList = hiveTxnManager.getValidTxns(excludedTxns);
    }

    // After the timeout just force abort the open txns
    if (conf.getBoolVar(REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT)) {
      List<Long> openTxns = getOpenTxns(hiveTxnManager, validTxnList, work.dbNameOrPattern);
      if (!openTxns.isEmpty()) {
        //abort only write transactions for the db under replication if abort transactions is enabled.
        hiveDb.abortTransactions(openTxns, TxnErrorMsg.ABORT_WRITE_TXN_AFTER_TIMEOUT.getErrorCode());
        validTxnList = hiveTxnManager.getValidTxns(excludedTxns);
        openTxns = getOpenTxns(hiveTxnManager, validTxnList, work.dbNameOrPattern);
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

  void dumpConstraintMetadata(String dbName, String tblName, Path dbRoot, Hive hiveDb, long tableId) throws Exception {
    try {
      Path constraintsRoot = new Path(dbRoot, ReplUtils.CONSTRAINTS_ROOT_DIR_NAME);
      Path commonConstraintsFile = new Path(constraintsRoot, ConstraintFileType.COMMON.getPrefix() + tblName);
      Path fkConstraintsFile = new Path(constraintsRoot, ConstraintFileType.FOREIGNKEY.getPrefix() + tblName);
      SQLAllTableConstraints tableConstraints = hiveDb.getTableConstraints(dbName, tblName, tableId);
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

  HiveWrapper createHiveWrapper(Hive hiveDb, String dbName){
    return new HiveWrapper(hiveDb, dbName);
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
