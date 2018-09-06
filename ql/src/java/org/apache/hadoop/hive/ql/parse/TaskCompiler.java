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

package org.apache.hadoop.hive.ql.parse;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import org.apache.commons.collections.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.MaterializedViewDesc;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.BasicStatsNoJobTask;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.DefaultFetchFormatter;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftFormatter;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TaskCompiler is a the base class for classes that compile
 * operator pipelines into tasks.
 */
public abstract class TaskCompiler {

  protected final Logger LOG = LoggerFactory.getLogger(TaskCompiler.class);

  // Assumes one instance of this + single-threaded compilation for each query.
  protected Hive db;
  protected LogHelper console;
  protected QueryState queryState;
  protected HiveConf conf;

  public void init(QueryState queryState, LogHelper console, Hive db) {
    this.queryState = queryState;
    this.conf = queryState.getConf();
    this.db = db;
    this.console = console;
  }

  @SuppressWarnings({"nls", "unchecked"})
  public void compile(final ParseContext pCtx,
      final List<Task<? extends Serializable>> rootTasks,
      final HashSet<ReadEntity> inputs, final HashSet<WriteEntity> outputs) throws SemanticException {

    Context ctx = pCtx.getContext();
    GlobalLimitCtx globalLimitCtx = pCtx.getGlobalLimitCtx();
    List<Task<MoveWork>> mvTask = new ArrayList<>();

    List<LoadTableDesc> loadTableWork = pCtx.getLoadTableWork();
    List<LoadFileDesc> loadFileWork = pCtx.getLoadFileWork();

    boolean isCStats = pCtx.getQueryProperties().isAnalyzeRewrite();
    int outerQueryLimit = pCtx.getQueryProperties().getOuterQueryLimit();

    if (pCtx.getFetchTask() != null) {
      if (pCtx.getFetchTask().getTblDesc() == null) {
        return;
      }
      pCtx.getFetchTask().getWork().setHiveServerQuery(SessionState.get().isHiveServerQuery());
      TableDesc resultTab = pCtx.getFetchTask().getTblDesc();
      // If the serializer is ThriftJDBCBinarySerDe, then it requires that NoOpFetchFormatter be used. But when it isn't,
      // then either the ThriftFormatter or the DefaultFetchFormatter should be used.
      if (!resultTab.getSerdeClassName().equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName())) {
        if (SessionState.get().isHiveServerQuery()) {
          conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER,ThriftFormatter.class.getName());
        } else {
          String formatterName = conf.get(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER);
          if (formatterName == null || formatterName.isEmpty()) {
            conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, DefaultFetchFormatter.class.getName());
          }
        }
      }

      return;
    }

    optimizeOperatorPlan(pCtx, inputs, outputs);

    /*
     * In case of a select, use a fetch task instead of a move task.
     * If the select is from analyze table column rewrite, don't create a fetch task. Instead create
     * a column stats task later.
     */
    if (pCtx.getQueryProperties().isQuery() && !isCStats) {
      if ((!loadTableWork.isEmpty()) || (loadFileWork.size() != 1)) {
        throw new SemanticException(ErrorMsg.INVALID_LOAD_TABLE_FILE_WORK.getMsg());
      }

      LoadFileDesc loadFileDesc = loadFileWork.get(0);

      String cols = loadFileDesc.getColumns();
      String colTypes = loadFileDesc.getColumnTypes();

      String resFileFormat;
      TableDesc resultTab = pCtx.getFetchTableDesc();
      if (resultTab == null) {
        resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
        if (SessionState.get().getIsUsingThriftJDBCBinarySerDe()
            && (resFileFormat.equalsIgnoreCase("SequenceFile"))) {
          resultTab =
              PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat,
                  ThriftJDBCBinarySerDe.class);
          // Set the fetch formatter to be a no-op for the ListSinkOperator, since we'll
          // read formatted thrift objects from the output SequenceFile written by Tasks.
          conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, NoOpFetchFormatter.class.getName());
        } else {
          resultTab =
              PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat,
                  LazySimpleSerDe.class);
        }
      } else {
        if (resultTab.getProperties().getProperty(serdeConstants.SERIALIZATION_LIB)
            .equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName())) {
          // Set the fetch formatter to be a no-op for the ListSinkOperator, since we'll
          // read formatted thrift objects from the output SequenceFile written by Tasks.
          conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, NoOpFetchFormatter.class.getName());
        }
      }

      FetchWork fetch = new FetchWork(loadFileDesc.getSourcePath(), resultTab, outerQueryLimit);
      boolean isHiveServerQuery = SessionState.get().isHiveServerQuery();
      fetch.setHiveServerQuery(isHiveServerQuery);
      fetch.setSource(pCtx.getFetchSource());
      fetch.setSink(pCtx.getFetchSink());
      if (isHiveServerQuery &&
        null != resultTab &&
        resultTab.getSerdeClassName().equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName()) &&
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS)) {
          fetch.setIsUsingThriftJDBCBinarySerDe(true);
      } else {
          fetch.setIsUsingThriftJDBCBinarySerDe(false);
      }

      pCtx.setFetchTask((FetchTask) TaskFactory.get(fetch));

      // For the FetchTask, the limit optimization requires we fetch all the rows
      // in memory and count how many rows we get. It's not practical if the
      // limit factor is too big
      int fetchLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVELIMITOPTMAXFETCH);
      if (globalLimitCtx.isEnable() && globalLimitCtx.getGlobalLimit() > fetchLimit) {
        LOG.info("For FetchTask, LIMIT " + globalLimitCtx.getGlobalLimit() + " > " + fetchLimit
            + ". Doesn't qualify limit optimization.");
        globalLimitCtx.disableOpt();

      }
      if (outerQueryLimit == 0) {
        // Believe it or not, some tools do generate queries with limit 0 and than expect
        // query to run quickly. Lets meet their requirement.
        LOG.info("Limit 0. No query execution needed.");
        return;
      }
    } else if (!isCStats) {
      for (LoadTableDesc ltd : loadTableWork) {
        Task<MoveWork> tsk = TaskFactory
            .get(new MoveWork(null, null, ltd, null, false));
        mvTask.add(tsk);
      }

      boolean oneLoadFileForCtas = true;
      for (LoadFileDesc lfd : loadFileWork) {
        if (pCtx.getQueryProperties().isCTAS() || pCtx.getQueryProperties().isMaterializedView()) {
          if (!oneLoadFileForCtas) { // should not have more than 1 load file for CTAS.
            throw new SemanticException(
                "One query is not expected to contain multiple CTAS loads statements");
          }
          setLoadFileLocation(pCtx, lfd);
          oneLoadFileForCtas = false;
        }
        mvTask.add(TaskFactory
            .get(new MoveWork(null, null, null, lfd, false)));
      }
    }

    generateTaskTree(rootTasks, pCtx, mvTask, inputs, outputs);

    // For each task, set the key descriptor for the reducer
    for (Task<? extends Serializable> rootTask : rootTasks) {
      GenMapRedUtils.setKeyAndValueDescForTaskTree(rootTask);
    }

    // If a task contains an operator which instructs bucketizedhiveinputformat
    // to be used, please do so
    for (Task<? extends Serializable> rootTask : rootTasks) {
      setInputFormat(rootTask);
    }

    optimizeTaskPlan(rootTasks, pCtx, ctx);

    /*
     * If the query was the result of analyze table column compute statistics rewrite, create
     * a column stats task instead of a fetch task to persist stats to the metastore.
     * As per HIVE-15903, we will also collect table stats when user computes column stats.
     * That means, if isCStats || !pCtx.getColumnStatsAutoGatherContexts().isEmpty()
     * We need to collect table stats
     * if isCStats, we need to include a basic stats task
     * else it is ColumnStatsAutoGather, which should have a move task with a stats task already.
     */
    if (isCStats || !pCtx.getColumnStatsAutoGatherContexts().isEmpty()) {
      // map from tablename to task (ColumnStatsTask which includes a BasicStatsTask)
      Map<String, StatsTask> map = new LinkedHashMap<>();
      if (isCStats) {
        if (rootTasks == null || rootTasks.size() != 1 || pCtx.getTopOps() == null
            || pCtx.getTopOps().size() != 1) {
          throw new SemanticException("Can not find correct root task!");
        }
        try {
          Task<? extends Serializable> root = rootTasks.iterator().next();
          StatsTask tsk = (StatsTask) genTableStats(pCtx, pCtx.getTopOps().values()
              .iterator().next(), root, outputs);
          root.addDependentTask(tsk);
          map.put(extractTableFullName(tsk), tsk);
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        genColumnStatsTask(pCtx.getAnalyzeRewrite(), loadFileWork, map, outerQueryLimit, 0);
      } else {
        Set<Task<? extends Serializable>> leafTasks = new LinkedHashSet<Task<? extends Serializable>>();
        getLeafTasks(rootTasks, leafTasks);
        List<Task<? extends Serializable>> nonStatsLeafTasks = new ArrayList<>();
        for (Task<? extends Serializable> tsk : leafTasks) {
          // map table name to the correct ColumnStatsTask
          if (tsk instanceof StatsTask) {
            map.put(extractTableFullName((StatsTask) tsk), (StatsTask) tsk);
          } else {
            nonStatsLeafTasks.add(tsk);
          }
        }
        // add cStatsTask as a dependent of all the nonStatsLeafTasks
        for (Task<? extends Serializable> tsk : nonStatsLeafTasks) {
          for (Task<? extends Serializable> cStatsTask : map.values()) {
            tsk.addDependentTask(cStatsTask);
          }
        }
        for (ColumnStatsAutoGatherContext columnStatsAutoGatherContext : pCtx
            .getColumnStatsAutoGatherContexts()) {
          if (!columnStatsAutoGatherContext.isInsertInto()) {
            genColumnStatsTask(columnStatsAutoGatherContext.getAnalyzeRewrite(),
                columnStatsAutoGatherContext.getLoadFileWork(), map, outerQueryLimit, 0);
          } else {
            int numBitVector;
            try {
              numBitVector = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
            } catch (Exception e) {
              throw new SemanticException(e.getMessage());
            }
            genColumnStatsTask(columnStatsAutoGatherContext.getAnalyzeRewrite(),
                columnStatsAutoGatherContext.getLoadFileWork(), map, outerQueryLimit, numBitVector);
          }
        }
      }
    }

    decideExecMode(rootTasks, ctx, globalLimitCtx);

    if (pCtx.getQueryProperties().isCTAS() && !pCtx.getCreateTable().isMaterialization()) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateTableDesc crtTblDesc = pCtx.getCreateTable();
      crtTblDesc.validate(conf);
      Task<? extends Serializable> crtTblTask = TaskFactory.get(new DDLWork(
          inputs, outputs, crtTblDesc));
      patchUpAfterCTASorMaterializedView(rootTasks, outputs, crtTblTask, CollectionUtils.isEmpty(crtTblDesc.getPartColNames()));
    } else if (pCtx.getQueryProperties().isMaterializedView()) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateViewDesc viewDesc = pCtx.getCreateViewDesc();
      Task<? extends Serializable> crtViewTask = TaskFactory.get(new DDLWork(
          inputs, outputs, viewDesc));
      patchUpAfterCTASorMaterializedView(rootTasks, outputs, crtViewTask, CollectionUtils.isEmpty(viewDesc.getPartColNames()));
    } else if (pCtx.getMaterializedViewUpdateDesc() != null) {
      // If there is a materialized view update desc, we create introduce it at the end
      // of the tree.
      MaterializedViewDesc materializedViewDesc = pCtx.getMaterializedViewUpdateDesc();
      Set<Task<? extends Serializable>> leafTasks = new LinkedHashSet<Task<? extends Serializable>>();
      getLeafTasks(rootTasks, leafTasks);
      Task<? extends Serializable> materializedViewTask = TaskFactory.get(materializedViewDesc, conf);
      for (Task<? extends Serializable> task : leafTasks) {
        task.addDependentTask(materializedViewTask);
      }
    }

    if (globalLimitCtx.isEnable() && pCtx.getFetchTask() != null) {
      LOG.info("set least row check for FetchTask: " + globalLimitCtx.getGlobalLimit());
      pCtx.getFetchTask().getWork().setLeastNumRows(globalLimitCtx.getGlobalLimit());
    }

    if (globalLimitCtx.isEnable() && globalLimitCtx.getLastReduceLimitDesc() != null) {
      LOG.info("set least row check for LimitDesc: " + globalLimitCtx.getGlobalLimit());
      globalLimitCtx.getLastReduceLimitDesc().setLeastRows(globalLimitCtx.getGlobalLimit());
    }

    Interner<TableDesc> interner = Interners.newStrongInterner();
    for (Task<? extends Serializable> rootTask : rootTasks) {
      GenMapRedUtils.internTableDesc(rootTask, interner);
      GenMapRedUtils.deriveFinalExplainAttributes(rootTask, pCtx.getConf());
    }
  }

  private String extractTableFullName(StatsTask tsk) throws SemanticException {
    return tsk.getWork().getFullTableName();
  }

  private Task<?> genTableStats(ParseContext parseContext, TableScanOperator tableScan, Task currentTask, final HashSet<WriteEntity> outputs)
      throws HiveException {
    Class<? extends InputFormat> inputFormat = tableScan.getConf().getTableMetadata()
        .getInputFormatClass();
    Table table = tableScan.getConf().getTableMetadata();
    List<Partition> partitions = new ArrayList<>();
    if (table.isPartitioned()) {
      partitions.addAll(parseContext.getPrunedPartitions(tableScan).getPartitions());
      for (Partition partn : partitions) {
        LOG.trace("adding part: " + partn);
        outputs.add(new WriteEntity(partn, WriteEntity.WriteType.DDL_NO_LOCK));
      }
    }
    TableSpec tableSpec = new TableSpec(table, partitions);
    tableScan.getConf().getTableMetadata().setTableSpec(tableSpec);

    // Note: this should probably use BasicStatsNoJobTask.canUseFooterScan, but it doesn't check
    //       Parquet for some reason. I'm keeping the existing behavior for now.
    if (inputFormat.equals(OrcInputFormat.class) && !AcidUtils.isTransactionalTable(table)) {
      // For ORC, there is no Tez Job for table stats.
      StatsWork columnStatsWork = new StatsWork(table, parseContext.getConf());
      columnStatsWork.setFooterScan();
      // If partition is specified, get pruned partition list
      if (partitions.size() > 0) {
        columnStatsWork.addInputPartitions(parseContext.getPrunedPartitions(tableScan).getPartitions());
      }
      return TaskFactory.get(columnStatsWork);
    } else {
      BasicStatsWork statsWork = new BasicStatsWork(tableScan.getConf().getTableMetadata().getTableSpec());
      statsWork.setIsExplicitAnalyze(true);
      StatsWork columnStatsWork = new StatsWork(table, statsWork, parseContext.getConf());
      columnStatsWork.collectStatsFromAggregator(tableScan.getConf());
      columnStatsWork.setSourceTask(currentTask);
      return TaskFactory.get(columnStatsWork);
    }
  }

  private void setLoadFileLocation(
      final ParseContext pCtx, LoadFileDesc lfd) throws SemanticException {
    // CTAS; make the movetask's destination directory the table's destination.
    Long txnIdForCtas = null;
    int stmtId = 0; // CTAS cannot be part of multi-txn stmt
    FileSinkDesc dataSinkForCtas = null;
    String loc = null;
    if (pCtx.getQueryProperties().isCTAS()) {
      CreateTableDesc ctd = pCtx.getCreateTable();
      dataSinkForCtas = ctd.getAndUnsetWriter();
      txnIdForCtas = ctd.getInitialMmWriteId();
      loc = ctd.getLocation();
    } else {
      loc = pCtx.getCreateViewDesc().getLocation();
    }
    Path location = (loc == null) ? getDefaultCtasLocation(pCtx) : new Path(loc);
    if (txnIdForCtas != null) {
      dataSinkForCtas.setDirName(location);
      location = new Path(location, AcidUtils.deltaSubdir(txnIdForCtas, txnIdForCtas, stmtId));
      lfd.setSourcePath(location);
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Setting MM CTAS to " + location);
      }
    }
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Location for LFD is being set to "
        + location + "; moving from " + lfd.getSourcePath());
    }
    lfd.setTargetDir(location);
  }

  private Path getDefaultCtasLocation(final ParseContext pCtx) throws SemanticException {
    try {
      String protoName = null;
      boolean isExternal = false;
      if (pCtx.getQueryProperties().isCTAS()) {
        protoName = pCtx.getCreateTable().getTableName();
        isExternal = pCtx.getCreateTable().isExternal();
      } else if (pCtx.getQueryProperties().isMaterializedView()) {
        protoName = pCtx.getCreateViewDesc().getViewName();
      }
      String[] names = Utilities.getDbTableName(protoName);
      if (!db.databaseExists(names[0])) {
        throw new SemanticException("ERROR: The database " + names[0] + " does not exist.");
      }
      Warehouse wh = new Warehouse(conf);
      return wh.getDefaultTablePath(db.getDatabase(names[0]), names[1], isExternal);
    } catch (HiveException e) {
      throw new SemanticException(e);
    } catch (MetaException e) {
      throw new SemanticException(e);
    }
  }

  private void patchUpAfterCTASorMaterializedView(final List<Task<? extends Serializable>> rootTasks,
                                                  final HashSet<WriteEntity> outputs,
                                                  Task<? extends Serializable> createTask,
                                                  boolean createTaskAfterMoveTask) {
    // clear the mapredWork output file from outputs for CTAS
    // DDLWork at the tail of the chain will have the output
    Iterator<WriteEntity> outIter = outputs.iterator();
    while (outIter.hasNext()) {
      switch (outIter.next().getType()) {
      case DFS_DIR:
      case LOCAL_DIR:
        outIter.remove();
        break;
      default:
        break;
      }
    }

    // find all leaf tasks and make the DDLTask as a dependent task on all of them
    HashSet<Task<? extends Serializable>> leaves = new LinkedHashSet<>();
    getLeafTasks(rootTasks, leaves);
    assert (leaves.size() > 0);
    // Target task is supposed to be the last task
    Task<? extends Serializable> targetTask = createTask;
    for (Task<? extends Serializable> task : leaves) {
      if (task instanceof StatsTask) {
        // StatsTask require table to already exist
        for (Task<? extends Serializable> parentOfStatsTask : task.getParentTasks()) {
          if (parentOfStatsTask instanceof MoveTask && !createTaskAfterMoveTask) {
            // For partitioned CTAS, we need to create the table before the move task
            // as we need to create the partitions in metastore and for that we should
            // have already registered the table
            interleaveTask(parentOfStatsTask, createTask);
          } else {
            parentOfStatsTask.addDependentTask(createTask);
          }
        }
        for (Task<? extends Serializable> parentOfCrtTblTask : createTask.getParentTasks()) {
          parentOfCrtTblTask.removeDependentTask(task);
        }
        createTask.addDependentTask(task);
        targetTask = task;
      } else if (task instanceof MoveTask && !createTaskAfterMoveTask) {
        // For partitioned CTAS, we need to create the table before the move task
        // as we need to create the partitions in metastore and for that we should
        // have already registered the table
        interleaveTask(task, createTask);
        targetTask = task;
      } else {
        task.addDependentTask(createTask);
      }
    }

    // Add task to insert / delete materialized view from registry if needed
    if (createTask instanceof DDLTask) {
      DDLTask ddlTask = (DDLTask) createTask;
      DDLWork work = ddlTask.getWork();
      String tableName = null;
      boolean retrieveAndInclude = false;
      boolean disableRewrite = false;
      if (work.getCreateViewDesc() != null && work.getCreateViewDesc().isMaterialized()) {
        tableName = work.getCreateViewDesc().getViewName();
        retrieveAndInclude = work.getCreateViewDesc().isRewriteEnabled();
      } else if (work.getAlterMaterializedViewDesc() != null) {
        tableName = work.getAlterMaterializedViewDesc().getMaterializedViewName();
        if (work.getAlterMaterializedViewDesc().isRewriteEnable()) {
          retrieveAndInclude = true;
        } else {
          disableRewrite = true;
        }
      } else {
        return;
      }
      targetTask.addDependentTask(
          TaskFactory.get(
              new MaterializedViewDesc(tableName, retrieveAndInclude, disableRewrite, false), conf));
    }
  }

  /**
   * Makes dependentTask dependent of task.
   */
  private void interleaveTask(Task<? extends Serializable> dependentTask, Task<? extends Serializable> task) {
    for (Task<? extends Serializable> parentOfStatsTask : dependentTask.getParentTasks()) {
      parentOfStatsTask.addDependentTask(task);
    }
    for (Task<? extends Serializable> parentOfCrtTblTask : task.getParentTasks()) {
      parentOfCrtTblTask.removeDependentTask(dependentTask);
    }
    task.addDependentTask(dependentTask);
  }

  /**
   * A helper function to generate a column stats task on top of map-red task. The column stats
   * task fetches from the output of the map-red task, constructs the column stats object and
   * persists it to the metastore.
   *
   * This method generates a plan with a column stats task on top of map-red task and sets up the
   * appropriate metadata to be used during execution.
   *
   */
  @SuppressWarnings("unchecked")
  protected void genColumnStatsTask(AnalyzeRewriteContext analyzeRewrite,
      List<LoadFileDesc> loadFileWork, Map<String, StatsTask> map,
      int outerQueryLimit, int numBitVector) throws SemanticException {
    FetchWork fetch;
    String tableName = analyzeRewrite.getTableName();
    List<String> colName = analyzeRewrite.getColName();
    List<String> colType = analyzeRewrite.getColType();
    boolean isTblLevel = analyzeRewrite.isTblLvl();

    String cols = loadFileWork.get(0).getColumns();
    String colTypes = loadFileWork.get(0).getColumnTypes();

    String resFileFormat;
    TableDesc resultTab;
    if (SessionState.get().isHiveServerQuery() && conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS)) {
      resFileFormat = "SequenceFile";
      resultTab =
          PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat,
              ThriftJDBCBinarySerDe.class);
    } else {
      resFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
      resultTab =
          PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, resFileFormat,
              LazySimpleSerDe.class);
    }

    fetch = new FetchWork(loadFileWork.get(0).getSourcePath(), resultTab, outerQueryLimit);

    ColumnStatsDesc cStatsDesc = new ColumnStatsDesc(tableName,
        colName, colType, isTblLevel, numBitVector, fetch);
    StatsTask columnStatsTask = map.get(tableName);
    if (columnStatsTask == null) {
      throw new SemanticException("Can not find " + tableName + " in genColumnStatsTask");
    } else {
      columnStatsTask.getWork().setColStats(cStatsDesc);
    }
  }


  /**
   * Find all leaf tasks of the list of root tasks.
   */
  private void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
      Set<Task<? extends Serializable>> leaves) {

    for (Task<? extends Serializable> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<? extends Serializable> task,
      Set<Task<? extends Serializable>> leaves) {
    if (task.getDependentTasks() == null) {
      if (!leaves.contains(task)) {
        leaves.add(task);
      }
    } else {
      getLeafTasks(task.getDependentTasks(), leaves);
    }
  }

  /*
   * Called to transform tasks into local tasks where possible/desirable
   */
  protected abstract void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx) throws SemanticException;

  /*
   * Called at the beginning of the compile phase to have another chance to optimize the operator plan
   */
  protected void optimizeOperatorPlan(ParseContext pCtxSet, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {
  }

  /*
   * Called after the tasks have been generated to run another round of optimization
   */
  protected abstract void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks,
      ParseContext pCtx, Context ctx) throws SemanticException;

  /*
   * Called to set the appropriate input format for tasks
   */
  protected abstract void setInputFormat(Task<? extends Serializable> rootTask);

  /*
   * Called to generate the taks tree from the parse context/operator tree
   */
  protected abstract void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException;

  /**
   * Create a clone of the parse context
   */
  public ParseContext getParseContext(ParseContext pCtx, List<Task<? extends Serializable>> rootTasks) {
    ParseContext clone = new ParseContext(queryState,
        pCtx.getOpToPartPruner(), pCtx.getOpToPartList(), pCtx.getTopOps(),
        pCtx.getJoinOps(), pCtx.getSmbMapJoinOps(),
        pCtx.getLoadTableWork(), pCtx.getLoadFileWork(),
        pCtx.getColumnStatsAutoGatherContexts(), pCtx.getContext(),
        pCtx.getIdToTableNameMap(), pCtx.getDestTableId(), pCtx.getUCtx(),
        pCtx.getListMapJoinOpsNoReducer(),
        pCtx.getPrunedPartitions(), pCtx.getTabNameToTabObject(), pCtx.getOpToSamplePruner(), pCtx.getGlobalLimitCtx(),
        pCtx.getNameToSplitSample(), pCtx.getSemanticInputs(), rootTasks,
        pCtx.getOpToPartToSkewedPruner(), pCtx.getViewAliasToInput(),
        pCtx.getReduceSinkOperatorsAddedByEnforceBucketingSorting(),
        pCtx.getAnalyzeRewrite(), pCtx.getCreateTable(),
        pCtx.getCreateViewDesc(), pCtx.getMaterializedViewUpdateDesc(),
        pCtx.getQueryProperties(), pCtx.getViewProjectToTableSchema(),
        pCtx.getAcidSinks());
    clone.setFetchTask(pCtx.getFetchTask());
    clone.setLineageInfo(pCtx.getLineageInfo());
    clone.setMapJoinOps(pCtx.getMapJoinOps());
    clone.setRsToRuntimeValuesInfoMap(pCtx.getRsToRuntimeValuesInfoMap());
    clone.setRsToSemiJoinBranchInfo(pCtx.getRsToSemiJoinBranchInfo());
    clone.setColExprToGBMap(pCtx.getColExprToGBMap());
    clone.setSemiJoinHints(pCtx.getSemiJoinHints());

    return clone;
  }

}
