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
import com.google.common.collect.Lists;

import org.apache.commons.collections.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ResultFileFormat;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.alter.rewrite.AlterMaterializedViewRewriteDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.NonBlockingOpDeDupProc;
import org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkDeDuplication;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
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
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftFormatter;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TaskCompiler is the base class for classes that compile
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

  @SuppressWarnings("nls")
  public void compile(final ParseContext pCtx,
      final List<Task<?>> rootTasks,
      final Set<ReadEntity> inputs, final Set<WriteEntity> outputs) throws SemanticException {

    Context ctx = pCtx.getContext();
    GlobalLimitCtx globalLimitCtx = pCtx.getGlobalLimitCtx();
    List<Task<MoveWork>> mvTask = new ArrayList<>();

    List<LoadTableDesc> loadTableWork = pCtx.getLoadTableWork();
    List<LoadFileDesc> loadFileWork = pCtx.getLoadFileWork();

    boolean isCStats = pCtx.getQueryProperties().isAnalyzeRewrite();
    int outerQueryLimit = pCtx.getQueryProperties().getOuterQueryLimit();

    boolean directInsert = false;
    if (pCtx.getCreateTable() != null && pCtx.getCreateTable().getStorageHandler() != null) {
      try {
        directInsert =
            HiveUtils.getStorageHandler(conf, pCtx.getCreateTable().getStorageHandler()).directInsert();
      } catch (HiveException e) {
        throw new SemanticException("Failed to load storage handler:  " + e.getMessage());
      }
    }
    if (pCtx.getCreateViewDesc() != null && pCtx.getCreateViewDesc().getStorageHandler() != null) {
      try {
        directInsert =
                HiveUtils.getStorageHandler(conf, pCtx.getCreateViewDesc().getStorageHandler()).directInsert();
      } catch (HiveException e) {
        throw new SemanticException("Failed to load storage handler:  " + e.getMessage());
      }
    }

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

    if (!pCtx.getQueryProperties().isAnalyzeCommand()) {
      LOG.debug("Skipping optimize operator plan for analyze command.");
      optimizeOperatorPlan(pCtx);
    }

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

      TableDesc resultTab = pCtx.getFetchTableDesc();
      boolean shouldSetOutputFormatter = false;
      if (resultTab == null) {
        ResultFileFormat resFileFormat = conf.getResultFileFormat();
        String fileFormat;
        Class<? extends Deserializer> serdeClass;
        if (SessionState.get().getIsUsingThriftJDBCBinarySerDe()
            && resFileFormat == ResultFileFormat.SEQUENCEFILE) {
          fileFormat = resFileFormat.toString();
          serdeClass = ThriftJDBCBinarySerDe.class;
          shouldSetOutputFormatter = true;
        } else if (resFileFormat == ResultFileFormat.SEQUENCEFILE) {
          // file format is changed so that IF file sink provides list of files to fetch from (instead
          // of whole directory) list status is done on files (which is what HiveSequenceFileInputFormat does)
          fileFormat = "HiveSequenceFile";
          serdeClass = LazySimpleSerDe.class;
        } else {
          // All other cases we use the defined file format and LazySimpleSerde
          fileFormat = resFileFormat.toString();
          serdeClass = LazySimpleSerDe.class;
        }
        resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, fileFormat, serdeClass);
      } else {
        shouldSetOutputFormatter = resultTab.getProperties().getProperty(serdeConstants.SERIALIZATION_LIB)
          .equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName());
      }

      if (shouldSetOutputFormatter) {
        // Set the fetch formatter to be a no-op for the ListSinkOperator, since we will
        // read formatted thrift objects from the output SequenceFile written by Tasks.
        conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, NoOpFetchFormatter.class.getName());
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

      // The idea here is to keep an object reference both in FileSink and in FetchTask for list of files
      // to be fetched. During Job close file sink will populate the list and fetch task later will use it
      // to fetch the results.
      Collection<Operator<?>> tableScanOps =
          Lists.<Operator<?>>newArrayList(pCtx.getTopOps().values());
      Set<FileSinkOperator> fsOps = OperatorUtils.findOperators(tableScanOps, FileSinkOperator.class);
      if(fsOps != null && fsOps.size() == 1) {
        FileSinkOperator op = fsOps.iterator().next();
        Set<FileStatus> filesToFetch =  new HashSet<>();
        op.getConf().setFilesToFetch(filesToFetch);
        fetch.setFilesToFetch(filesToFetch);
      }

      pCtx.setFetchTask((FetchTask) TaskFactory.get(fetch));

      // For the FetchTask, the limit optimization requires we fetch all the rows
      // in memory and count how many rows we get. It's not practical if the
      // limit factor is too big
      int fetchLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_LIMIT_OPT_MAX_FETCH);
      if (globalLimitCtx.isEnable() && globalLimitCtx.getGlobalLimit() > fetchLimit) {
        LOG.info("For FetchTask, LIMIT " + globalLimitCtx.getGlobalLimit() + " > " + fetchLimit
            + ". Doesn't qualify limit optimization.");
        globalLimitCtx.disableOpt();

      }
      if (outerQueryLimit == 0) {
        // Believe it or not, some tools do generate queries with limit 0 and than expect
        // query to run quickly. Let's meet their requirement.
        LOG.info("Limit 0. No query execution needed.");
        return;
      }
    } else if (!isCStats) {
      for (LoadTableDesc ltd : loadTableWork) {
        Task<MoveWork> tsk = TaskFactory
            .get(new MoveWork(pCtx.getQueryProperties().isCTAS() && pCtx.getCreateTable().isExternal(),
                    null, null, ltd, null, false));
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
        mvTask.add(TaskFactory.get(
            new MoveWork(pCtx.getQueryProperties().isCTAS() && pCtx.getCreateTable().isExternal(), null, null, null,
                lfd, false)));
      }
    }

    generateTaskTree(rootTasks, pCtx, mvTask, inputs, outputs);

    // For each task, set the key descriptor for the reducer
    for (Task<?> rootTask : rootTasks) {
      GenMapRedUtils.setKeyAndValueDescForTaskTree(rootTask);
    }

    // If a task contains an operator which instructs bucketizedhiveinputformat
    // to be used, please do so
    for (Task<?> rootTask : rootTasks) {
      setInputFormat(rootTask);
    }

    if (directInsert) {
      if (pCtx.getCreateTable() != null) {
        CreateTableDesc crtTblDesc = pCtx.getCreateTable();
        crtTblDesc.validate(conf);
        Task<?> crtTask = TaskFactory.get(new DDLWork(inputs, outputs, crtTblDesc));
        for (Task<?> rootTask : rootTasks) {
          crtTask.addDependentTask(rootTask);
          rootTasks.clear();
          rootTasks.add(crtTask);
        }
      } else if (pCtx.getCreateViewDesc() != null) {
        CreateMaterializedViewDesc createMaterializedViewDesc = pCtx.getCreateViewDesc();
        Task<?> crtTask = TaskFactory.get(new DDLWork(inputs, outputs, createMaterializedViewDesc));
        MaterializedViewUpdateDesc materializedViewUpdateDesc = new MaterializedViewUpdateDesc(
                createMaterializedViewDesc.getViewName(), createMaterializedViewDesc.isRewriteEnabled(), false, false);
        Task<?> updateTask = TaskFactory.get(new DDLWork(inputs, outputs, materializedViewUpdateDesc));
        crtTask.addDependentTask(updateTask);
        for (Task<?> rootTask : rootTasks) {
          updateTask.addDependentTask(rootTask);
          rootTasks.clear();
          rootTasks.add(crtTask);
        }
      }
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
          Task<?> root = rootTasks.iterator().next();
          StatsTask tsk = (StatsTask) genTableStats(pCtx, pCtx.getTopOps().values()
              .iterator().next(), root, outputs);
          root.addDependentTask(tsk);
          map.put(extractTableFullName(tsk), tsk);
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        genColumnStatsTask(pCtx.getAnalyzeRewrite(), loadFileWork, map, outerQueryLimit, 0);
      } else {
        Set<Task<?>> leafTasks = new LinkedHashSet<Task<?>>();
        getLeafTasks(rootTasks, leafTasks);
        List<Task<?>> nonStatsLeafTasks = new ArrayList<>();
        for (Task<?> tsk : leafTasks) {
          // map table name to the correct ColumnStatsTask
          if (tsk instanceof StatsTask) {
            map.put(extractTableFullName((StatsTask) tsk), (StatsTask) tsk);
          } else {
            nonStatsLeafTasks.add(tsk);
          }
        }
        // add cStatsTask as a dependent of all the nonStatsLeafTasks
        for (Task<?> tsk : nonStatsLeafTasks) {
          for (Task<?> cStatsTask : map.values()) {
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

    // for direct insert CTAS, we don't need this table creation DDL task, since the table will be created
    // ahead of time by the non-native table
    if (pCtx.getQueryProperties().isCTAS() && !pCtx.getCreateTable().isMaterialization() && !directInsert) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateTableDesc crtTblDesc = pCtx.getCreateTable();
      crtTblDesc.validate(conf);
      Task<?> crtTblTask = TaskFactory.get(new DDLWork(inputs, outputs, crtTblDesc));
      patchUpAfterCTASorMaterializedView(rootTasks, inputs, outputs, crtTblTask,
          CollectionUtils.isEmpty(crtTblDesc.getPartColNames()));
    } else if (pCtx.getQueryProperties().isMaterializedView() && !directInsert) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateMaterializedViewDesc viewDesc = pCtx.getCreateViewDesc();
      Task<?> crtViewTask = TaskFactory.get(new DDLWork(
          inputs, outputs, viewDesc));
      patchUpAfterCTASorMaterializedView(rootTasks, inputs, outputs, crtViewTask,
          CollectionUtils.isEmpty(viewDesc.getPartColNames()));
    } else if (pCtx.getMaterializedViewUpdateDesc() != null) {
      // If there is a materialized view update desc, we create introduce it at the end
      // of the tree.
      MaterializedViewUpdateDesc materializedViewDesc = pCtx.getMaterializedViewUpdateDesc();
      DDLWork ddlWork = new DDLWork(inputs, outputs, materializedViewDesc);
      Set<Task<?>> leafTasks = new LinkedHashSet<Task<?>>();
      getLeafTasks(rootTasks, leafTasks);
      Task<?> materializedViewTask = TaskFactory.get(ddlWork, conf);
      for (Task<?> task : leafTasks) {
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

    // Perform Final chores on generated Map works
    //   1.  Intern the table descriptors
    //   2.  Derive final explain attributes based on previous compilation.
    GenMapRedUtils.finalMapWorkChores(rootTasks, pCtx.getConf(), interner);
  }

  private String extractTableFullName(StatsTask tsk) throws SemanticException {
    return tsk.getWork().getFullTableName();
  }

  private Task<?> genTableStats(ParseContext parseContext, TableScanOperator tableScan, Task currentTask, final Set<WriteEntity> outputs)
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

    if (BasicStatsNoJobTask.canUseFooterScan(table, inputFormat)) {
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
    Long txnId = null;
    int stmtId = 0; // CTAS or CMV cannot be part of multi-txn stmt
    FileSinkDesc dataSink = null;
    String loc = null;
    if (pCtx.getQueryProperties().isCTAS()) {
      CreateTableDesc ctd = pCtx.getCreateTable();
      dataSink = ctd.getAndUnsetWriter();
      txnId = ctd.getInitialWriteId();
      loc = ctd.getLocation();
    } else {
      CreateMaterializedViewDesc cmv = pCtx.getCreateViewDesc();
      dataSink = cmv.getAndUnsetWriter();
      txnId = cmv.getInitialWriteId();
      loc = cmv.getLocation();
    }
    Path location = (loc == null) ? getDefaultCtasOrCMVLocation(pCtx) : new Path(loc);
    if (pCtx.getQueryProperties().isCTAS()) {
      CreateTableDesc ctd = pCtx.getCreateTable();
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.CREATE_TABLE_AS_EXTERNAL)) {
        ctd.getTblProps().put(hive_metastoreConstants.CTAS_LEGACY_CONFIG, "true"); // create as external table
      }
      try {
        Table table = ctd.toTable(conf);
        if (!ctd.isMaterialization()) {
          table = db.getTranslateTableDryrun(table.getTTable());
        }
        org.apache.hadoop.hive.metastore.api.Table tTable = table.getTTable();
        if (tTable.getSd() != null && tTable.getSd().getLocation() != null) {
          location = new Path(tTable.getSd().getLocation());
        }
        ctd.getTblProps().remove(hive_metastoreConstants.CTAS_LEGACY_CONFIG);
        ctd.fromTable(tTable);
      } catch (HiveException ex) {
        throw new SemanticException(ex);
      }
      pCtx.setCreateTable(ctd);
    }
    if (txnId != null) {
      dataSink.setDirName(location);
      location = new Path(location, AcidUtils.deltaSubdir(txnId, txnId, stmtId));
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

  private Path getDefaultCtasOrCMVLocation(final ParseContext pCtx) throws SemanticException {
    try {
      String protoName = null, suffix = "";
      boolean isExternal = false;
      boolean createTableOrMVUseSuffix = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
              || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED);

      if (pCtx.getQueryProperties().isCTAS()) {
        protoName = pCtx.getCreateTable().getDbTableName();
        isExternal = pCtx.getCreateTable().isExternal();
        createTableOrMVUseSuffix &= AcidUtils.isTransactionalTable(pCtx.getCreateTable());
        suffix = Utilities.getTableOrMVSuffix(pCtx.getContext(), createTableOrMVUseSuffix);
      } else if (pCtx.getQueryProperties().isMaterializedView()) {
        protoName = pCtx.getCreateViewDesc().getViewName();
        createTableOrMVUseSuffix &= AcidUtils.isTransactionalView(pCtx.getCreateViewDesc());
        suffix = Utilities.getTableOrMVSuffix(pCtx.getContext(), createTableOrMVUseSuffix);
      }
      String[] names = Utilities.getDbTableName(protoName);
      if (!db.databaseExists(names[0])) {
        throw new SemanticException("ERROR: The database " + names[0] + " does not exist.");
      }
      Warehouse wh = new Warehouse(conf);
      return wh.getDefaultTablePath(db.getDatabase(names[0]), names[1] + suffix, isExternal);
    } catch (HiveException | MetaException e) {
      throw new SemanticException(e);
    }
  }

  private void patchUpAfterCTASorMaterializedView(List<Task<?>> rootTasks,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs, Task<?> createTask,
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
    Set<Task<?>> leaves = new LinkedHashSet<>();
    getLeafTasks(rootTasks, leaves);
    assert (leaves.size() > 0);
    // Target task is supposed to be the last task
    Task<?> targetTask = createTask;
    for (Task<?> task : leaves) {
      if (task instanceof StatsTask) {
        // StatsTask require table to already exist
        for (Task<?> parentOfStatsTask : task.getParentTasks()) {
          if (parentOfStatsTask instanceof MoveTask && !createTaskAfterMoveTask) {
            // For partitioned CTAS, we need to create the table before the move task
            // as we need to create the partitions in metastore and for that we should
            // have already registered the table
            interleaveTask(parentOfStatsTask, createTask);
          } else {
            parentOfStatsTask.addDependentTask(createTask);
          }
        }
        for (Task<?> parentOfCrtTblTask : createTask.getParentTasks()) {
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
      DDLTask ddlTask = (DDLTask)createTask;
      DDLWork work = ddlTask.getWork();
      DDLDesc desc = work.getDDLDesc();
      if (desc instanceof CreateMaterializedViewDesc) {
        CreateMaterializedViewDesc createViewDesc = (CreateMaterializedViewDesc)desc;
        String tableName = createViewDesc.getViewName();
        boolean retrieveAndInclude = createViewDesc.isRewriteEnabled();
        MaterializedViewUpdateDesc materializedViewUpdateDesc =
            new MaterializedViewUpdateDesc(tableName, retrieveAndInclude, false, false);
        DDLWork ddlWork = new DDLWork(inputs, outputs, materializedViewUpdateDesc);
        targetTask.addDependentTask(TaskFactory.get(ddlWork, conf));
      } else if (desc instanceof AlterMaterializedViewRewriteDesc) {
        AlterMaterializedViewRewriteDesc alterMVRewriteDesc = (AlterMaterializedViewRewriteDesc)desc;
        String tableName = alterMVRewriteDesc.getMaterializedViewName();
        boolean retrieveAndInclude = alterMVRewriteDesc.isRewriteEnable();
        boolean disableRewrite = !alterMVRewriteDesc.isRewriteEnable();
        MaterializedViewUpdateDesc materializedViewUpdateDesc =
            new MaterializedViewUpdateDesc(tableName, retrieveAndInclude, disableRewrite, false);
        DDLWork ddlWork = new DDLWork(inputs, outputs, materializedViewUpdateDesc);
        targetTask.addDependentTask(TaskFactory.get(ddlWork, conf));
      }
    }
  }

  /**
   * Makes dependentTask dependent of task.
   */
  private void interleaveTask(Task<?> dependentTask, Task<?> task) {
    for (Task<?> parentOfStatsTask : dependentTask.getParentTasks()) {
      parentOfStatsTask.addDependentTask(task);
    }
    for (Task<?> parentOfCrtTblTask : task.getParentTasks()) {
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

    TableDesc resultTab;
    if (SessionState.get().isHiveServerQuery() && conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS)) {
      resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, ResultFileFormat.SEQUENCEFILE.toString(),
              ThriftJDBCBinarySerDe.class);
    } else {
      resultTab = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, conf.getResultFileFormat().toString(),
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
  private void getLeafTasks(List<Task<?>> rootTasks,
      Set<Task<?>> leaves) {

    for (Task<?> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<?> task,
      Set<Task<?>> leaves) {
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
  protected abstract void decideExecMode(List<Task<?>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx) throws SemanticException;

  /*
   * Called at the beginning of the compile phase to have another chance to optimize the operator plan
   */
  protected void optimizeOperatorPlan(ParseContext pCtxSet) throws SemanticException {
  }

  /*
   * Called after the tasks have been generated to run another round of optimization
   */
  protected abstract void optimizeTaskPlan(List<Task<?>> rootTasks,
      ParseContext pCtx, Context ctx) throws SemanticException;

  /*
   * Called to set the appropriate input format for tasks
   */
  protected abstract void setInputFormat(Task<?> rootTask);

  /*
   * Called to generate the tasks tree from the parse context/operator tree
   */
  protected abstract void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException;

  /*
   * Called for dynamic partitioning sort optimization.
   */
  protected void runDynPartitionSortOptimizations(ParseContext parseContext, HiveConf hConf) throws SemanticException {
    // run Sorted dynamic partition optimization

    if(HiveConf.getBoolVar(hConf, HiveConf.ConfVars.DYNAMIC_PARTITIONING) &&
        HiveConf.getVar(hConf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE).equals("nonstrict") &&
        !HiveConf.getBoolVar(hConf, HiveConf.ConfVars.HIVE_OPT_LIST_BUCKETING)) {
      new SortedDynPartitionOptimizer().transform(parseContext);

      if(HiveConf.getBoolVar(hConf, HiveConf.ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION)) {
        // Dynamic sort partition adds an extra RS therefore need to de-dup
        new ReduceSinkDeDuplication().transform(parseContext);
        // there is an issue with dedup logic wherein SELECT is created with wrong columns
        // NonBlockingOpDeDupProc fixes that
        new NonBlockingOpDeDupProc().transform(parseContext);
      }
    }
  }

  /**
   * Create a clone of the parse context
   */
  public ParseContext getParseContext(ParseContext pCtx, List<Task<?>> rootTasks) {
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
        pCtx.getQueryProperties(), pCtx.getViewProjectToTableSchema());
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
