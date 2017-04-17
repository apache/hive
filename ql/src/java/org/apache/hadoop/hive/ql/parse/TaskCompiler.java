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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnStatsTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.physical.AnnotateRunTimeStatsOptimizer;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDesc;
import org.apache.hadoop.hive.ql.plan.ColumnStatsWork;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.DefaultFetchFormatter;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftFormatter;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

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
  public void compile(final ParseContext pCtx, final List<Task<? extends Serializable>> rootTasks,
      final HashSet<ReadEntity> inputs, final HashSet<WriteEntity> outputs) throws SemanticException {

    Context ctx = pCtx.getContext();
    GlobalLimitCtx globalLimitCtx = pCtx.getGlobalLimitCtx();
    List<Task<MoveWork>> mvTask = new ArrayList<Task<MoveWork>>();

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

      pCtx.setFetchTask((FetchTask) TaskFactory.get(fetch, conf));

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
        Task<MoveWork> tsk = TaskFactory.get(new MoveWork(null, null, ltd, null, false), conf);
        mvTask.add(tsk);
        // Check to see if we are stale'ing any indexes and auto-update them if we want
        if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEINDEXAUTOUPDATE)) {
          IndexUpdater indexUpdater = new IndexUpdater(loadTableWork, inputs, conf);
          try {
            List<Task<? extends Serializable>> indexUpdateTasks = indexUpdater
                .generateUpdateTasks();
            for (Task<? extends Serializable> updateTask : indexUpdateTasks) {
              tsk.addDependentTask(updateTask);
            }
          } catch (HiveException e) {
            console
                .printInfo("WARNING: could not auto-update stale indexes, which are not in sync");
          }
        }
      }

      boolean oneLoadFile = true;
      for (LoadFileDesc lfd : loadFileWork) {
        if (pCtx.getQueryProperties().isCTAS() || pCtx.getQueryProperties().isMaterializedView()) {
          assert (oneLoadFile); // should not have more than 1 load file for
          // CTAS
          // make the movetask's destination directory the table's destination.
          Path location;
          String loc = pCtx.getQueryProperties().isCTAS() ?
                  pCtx.getCreateTable().getLocation() : pCtx.getCreateViewDesc().getLocation();
          if (loc == null) {
            // get the default location
            Path targetPath;
            try {
              String protoName = null;
              if (pCtx.getQueryProperties().isCTAS()) {
                protoName = pCtx.getCreateTable().getTableName();
              } else if (pCtx.getQueryProperties().isMaterializedView()) {
                protoName = pCtx.getCreateViewDesc().getViewName();
              }
              String[] names = Utilities.getDbTableName(protoName);
              if (!db.databaseExists(names[0])) {
                throw new SemanticException("ERROR: The database " + names[0]
                    + " does not exist.");
              }
              Warehouse wh = new Warehouse(conf);
              targetPath = wh.getDefaultTablePath(db.getDatabase(names[0]), names[1]);
            } catch (HiveException e) {
              throw new SemanticException(e);
            } catch (MetaException e) {
              throw new SemanticException(e);
            }

            location = targetPath;
          } else {
            location = new Path(loc);
          }
          lfd.setTargetDir(location);

          oneLoadFile = false;
        }
        mvTask.add(TaskFactory.get(new MoveWork(null, null, null, lfd, false), conf));
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
     */
    if (isCStats || !pCtx.getColumnStatsAutoGatherContexts().isEmpty()) {
      Set<Task<? extends Serializable>> leafTasks = new LinkedHashSet<Task<? extends Serializable>>();
      getLeafTasks(rootTasks, leafTasks);
      if (isCStats) {
        genColumnStatsTask(pCtx.getAnalyzeRewrite(), loadFileWork, leafTasks, outerQueryLimit, 0);
      } else {
        for (ColumnStatsAutoGatherContext columnStatsAutoGatherContext : pCtx
            .getColumnStatsAutoGatherContexts()) {
          if (!columnStatsAutoGatherContext.isInsertInto()) {
            genColumnStatsTask(columnStatsAutoGatherContext.getAnalyzeRewrite(),
                columnStatsAutoGatherContext.getLoadFileWork(), leafTasks, outerQueryLimit, 0);
          } else {
            int numBitVector;
            try {
              numBitVector = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
            } catch (Exception e) {
              throw new SemanticException(e.getMessage());
            }
            genColumnStatsTask(columnStatsAutoGatherContext.getAnalyzeRewrite(),
                columnStatsAutoGatherContext.getLoadFileWork(), leafTasks, outerQueryLimit, numBitVector);
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
          inputs, outputs, crtTblDesc), conf);
      patchUpAfterCTASorMaterializedView(rootTasks, outputs, crtTblTask);
    } else if (pCtx.getQueryProperties().isMaterializedView()) {
      // generate a DDL task and make it a dependent task of the leaf
      CreateViewDesc viewDesc = pCtx.getCreateViewDesc();
      Task<? extends Serializable> crtViewTask = TaskFactory.get(new DDLWork(
          inputs, outputs, viewDesc), conf);
      patchUpAfterCTASorMaterializedView(rootTasks, outputs, crtViewTask);
    }

    if (globalLimitCtx.isEnable() && pCtx.getFetchTask() != null) {
      LOG.info("set least row check for FetchTask: " + globalLimitCtx.getGlobalLimit());
      pCtx.getFetchTask().getWork().setLeastNumRows(globalLimitCtx.getGlobalLimit());
    }

    if (globalLimitCtx.isEnable() && globalLimitCtx.getLastReduceLimitDesc() != null) {
      LOG.info("set least row check for LimitDesc: " + globalLimitCtx.getGlobalLimit());
      globalLimitCtx.getLastReduceLimitDesc().setLeastRows(globalLimitCtx.getGlobalLimit());
      List<ExecDriver> mrTasks = Utilities.getMRTasks(rootTasks);
      for (ExecDriver tsk : mrTasks) {
        tsk.setRetryCmdWhenFail(true);
      }
      List<SparkTask> sparkTasks = Utilities.getSparkTasks(rootTasks);
      for (SparkTask sparkTask : sparkTasks) {
        sparkTask.setRetryCmdWhenFail(true);
      }
    }

    Interner<TableDesc> interner = Interners.newStrongInterner();
    for (Task<? extends Serializable> rootTask : rootTasks) {
      GenMapRedUtils.internTableDesc(rootTask, interner);
      GenMapRedUtils.deriveFinalExplainAttributes(rootTask, pCtx.getConf());
    }
  }

  private void patchUpAfterCTASorMaterializedView(final List<Task<? extends Serializable>>  rootTasks,
                                                  final HashSet<WriteEntity> outputs,
                                                  Task<? extends Serializable> createTask) {
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

    // find all leaf tasks and make the DDLTask as a dependent task of all of
    // them
    HashSet<Task<? extends Serializable>> leaves =
        new LinkedHashSet<Task<? extends Serializable>>();
    getLeafTasks(rootTasks, leaves);
    assert (leaves.size() > 0);
    for (Task<? extends Serializable> task : leaves) {
      if (task instanceof StatsTask) {
        // StatsTask require table to already exist
        for (Task<? extends Serializable> parentOfStatsTask : task.getParentTasks()) {
          parentOfStatsTask.addDependentTask(createTask);
        }
        for (Task<? extends Serializable> parentOfCrtTblTask : createTask.getParentTasks()) {
          parentOfCrtTblTask.removeDependentTask(task);
        }
        createTask.addDependentTask(task);
      } else {
        task.addDependentTask(createTask);
      }
    }
  }

  /**
   * A helper function to generate a column stats task on top of map-red task. The column stats
   * task fetches from the output of the map-red task, constructs the column stats object and
   * persists it to the metastore.
   *
   * This method generates a plan with a column stats task on top of map-red task and sets up the
   * appropriate metadata to be used during execution.
   *
   * @param analyzeRewrite
   * @param loadTableWork
   * @param loadFileWork
   * @param rootTasks
   * @param outerQueryLimit
   */
  @SuppressWarnings("unchecked")
  protected void genColumnStatsTask(AnalyzeRewriteContext analyzeRewrite,
      List<LoadFileDesc> loadFileWork, Set<Task<? extends Serializable>> leafTasks,
      int outerQueryLimit, int numBitVector) {
    ColumnStatsTask cStatsTask = null;
    ColumnStatsWork cStatsWork = null;
    FetchWork fetch = null;
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
        colName, colType, isTblLevel, numBitVector);
    cStatsWork = new ColumnStatsWork(fetch, cStatsDesc);
    cStatsTask = (ColumnStatsTask) TaskFactory.get(cStatsWork, conf);
    for (Task<? extends Serializable> tsk : leafTasks) {
      tsk.addDependentTask(cStatsTask);
    }
  }


  /**
   * Find all leaf tasks of the list of root tasks.
   */
  protected void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
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
        pCtx.getLoadTableWork(), pCtx.getLoadFileWork(), pCtx.getColumnStatsAutoGatherContexts(), pCtx.getContext(),
        pCtx.getIdToTableNameMap(), pCtx.getDestTableId(), pCtx.getUCtx(),
        pCtx.getListMapJoinOpsNoReducer(),
        pCtx.getPrunedPartitions(), pCtx.getTabNameToTabObject(), pCtx.getOpToSamplePruner(), pCtx.getGlobalLimitCtx(),
        pCtx.getNameToSplitSample(), pCtx.getSemanticInputs(), rootTasks,
        pCtx.getOpToPartToSkewedPruner(), pCtx.getViewAliasToInput(),
        pCtx.getReduceSinkOperatorsAddedByEnforceBucketingSorting(),
        pCtx.getAnalyzeRewrite(), pCtx.getCreateTable(), pCtx.getCreateViewDesc(),
        pCtx.getQueryProperties(), pCtx.getViewProjectToTableSchema(),
        pCtx.getAcidSinks());
    clone.setFetchTask(pCtx.getFetchTask());
    clone.setLineageInfo(pCtx.getLineageInfo());
    clone.setMapJoinOps(pCtx.getMapJoinOps());
    clone.setRsToRuntimeValuesInfoMap(pCtx.getRsToRuntimeValuesInfoMap());
    clone.setRsOpToTsOpMap(pCtx.getRsOpToTsOpMap());

    return clone;
  }

}
