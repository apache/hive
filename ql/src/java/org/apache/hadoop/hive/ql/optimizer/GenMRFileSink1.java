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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.merge.MergeWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Processor for the rule - table scan followed by reduce sink.
 */
public class GenMRFileSink1 implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(GenMRFileSink1.class.getName());

  public GenMRFileSink1() {
  }

  /**
   * File Sink Operator encountered.
   *
   * @param nd
   *          the file sink operator encountered
   * @param opProcCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    boolean chDir = false;
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    FileSinkOperator fsOp = (FileSinkOperator) nd;
    boolean isInsertTable = // is INSERT OVERWRITE TABLE
      fsOp.getConf().getTableInfo().getTableName() != null &&
      parseCtx.getQB().getParseInfo().isInsertToTable();
    HiveConf hconf = parseCtx.getConf();


    // Has the user enabled merging of files for map-only jobs or for all jobs
    if ((ctx.getMvTask() != null) && (!ctx.getMvTask().isEmpty())) {
      List<Task<MoveWork>> mvTasks = ctx.getMvTask();

      // In case of unions or map-joins, it is possible that the file has
      // already been seen.
      // So, no need to attempt to merge the files again.
      if ((ctx.getSeenFileSinkOps() == null)
          || (!ctx.getSeenFileSinkOps().contains(nd))) {

        // no need of merging if the move is to a local file system
        MoveTask mvTask = (MoveTask) findMoveTask(mvTasks, fsOp);

        if (isInsertTable &&
            hconf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
          addStatsTask(fsOp, mvTask, currTask, parseCtx.getConf());
        }

        if ((mvTask != null) && !mvTask.isLocal()) {
          // There are separate configuration parameters to control whether to
          // merge for a map-only job
          // or for a map-reduce job
          MapredWork currWork = (MapredWork) currTask.getWork();
          boolean mergeMapOnly =
            hconf.getBoolVar(HiveConf.ConfVars.HIVEMERGEMAPFILES) &&
            currWork.getReducer() == null;
          boolean mergeMapRed =
            hconf.getBoolVar(HiveConf.ConfVars.HIVEMERGEMAPREDFILES) &&
            currWork.getReducer() != null;
          if (mergeMapOnly || mergeMapRed) {
            chDir = true;
          }
        }
      }
    }

    String finalName = processFS(nd, stack, opProcCtx, chDir);

    // need to merge the files in the destination table/partitions
    if (chDir && (finalName != null)) {
      createMergeJob((FileSinkOperator) nd, ctx, finalName);
    }

    return null;
  }

  /**
   * Add the StatsTask as a dependent task of the MoveTask
   * because StatsTask will change the Table/Partition metadata. For atomicity, we
   * should not change it before the data is actually there done by MoveTask.
   * @param nd the FileSinkOperator whose results are taken care of by the MoveTask.
   * @param mvTask The MoveTask that moves the FileSinkOperator's results.
   * @param currTask The MapRedTask that the FileSinkOperator belongs to.
   * @param hconf HiveConf
   */
  private void addStatsTask(FileSinkOperator nd, MoveTask mvTask,
      Task<? extends Serializable> currTask, HiveConf hconf) {

    MoveWork mvWork = ((MoveTask)mvTask).getWork();
    StatsWork statsWork = null;
    if(mvWork.getLoadTableWork() != null){
       statsWork = new StatsWork(mvWork.getLoadTableWork());
    }else if (mvWork.getLoadFileWork() != null){
       statsWork = new StatsWork(mvWork.getLoadFileWork());
    }
    assert statsWork != null : "Error when genereting StatsTask";
    statsWork.setStatsReliable(hconf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
    MapredWork mrWork = (MapredWork) currTask.getWork();

    // AggKey in StatsWork is used for stats aggregation while StatsAggPrefix
    // in FileSinkDesc is used for stats publishing. They should be consistent.
    statsWork.setAggKey(((FileSinkOperator)nd).getConf().getStatsAggPrefix());
    Task<? extends Serializable> statsTask = TaskFactory.get(statsWork, hconf);

    // mark the MapredWork and FileSinkOperator for gathering stats
    nd.getConf().setGatherStats(true);
    mrWork.setGatheringStats(true);
    nd.getConf().setStatsReliable(hconf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
    // mrWork.addDestinationTable(nd.getConf().getTableInfo().getTableName());

    // subscribe feeds from the MoveTask so that MoveTask can forward the list
    // of dynamic partition list to the StatsTask
    mvTask.addDependentTask(statsTask);
    statsTask.subscribeFeed(mvTask);
  }

  private void createMapReduce4Merge(FileSinkOperator fsOp, GenMRProcContext ctx, String finalName)
      throws SemanticException {
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    RowSchema inputRS = fsOp.getSchema();

    // create a reduce Sink operator - key is the first column
    ArrayList<ExprNodeDesc> keyCols = new ArrayList<ExprNodeDesc>();
    keyCols.add(TypeCheckProcFactory.DefaultExprProcessor
        .getFuncExprNodeDesc("rand"));

    // value is all the columns in the FileSink operator input
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    for (ColumnInfo ci : inputRS.getSignature()) {
      valueCols.add(new ExprNodeColumnDesc(ci.getType(), ci.getInternalName(),
          ci.getTabAlias(), ci.getIsVirtualCol()));
    }

    // create a dummy tableScan operator
    Operator<? extends Serializable> tsMerge = OperatorFactory.get(
        TableScanDesc.class, inputRS);

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++) {
      outputColumns.add(SemanticAnalyzer.getColumnInternalName(i));
    }

    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(
        new ArrayList<ExprNodeDesc>(), valueCols, outputColumns, false, -1, -1,
        -1);
    OperatorFactory.getAndMakeChild(rsDesc, inputRS, tsMerge);
    ParseContext parseCtx = ctx.getParseCtx();
    FileSinkDesc fsConf = fsOp.getConf();

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = ctx.getParseCtx().getOpParseCtx().get(fsOp).getRowResolver();
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(pos.toString(), colInfo
          .getType(), info[0], colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator<ExtractDesc> extract = OperatorFactory.getAndMakeChild(new ExtractDesc(
        new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
            Utilities.ReduceField.VALUE.toString(), "", false)),
            new RowSchema(out_rwsch.getColumnInfos()));

    TableDesc ts = (TableDesc) fsConf.getTableInfo().clone();
    fsConf.getTableInfo().getProperties().remove(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);

    FileSinkDesc newFSD = new FileSinkDesc(finalName, ts, parseCtx.getConf()
        .getBoolVar(HiveConf.ConfVars.COMPRESSRESULT));
    FileSinkOperator newOutput = (FileSinkOperator) OperatorFactory.
      getAndMakeChild(newFSD, inputRS, extract);

    HiveConf conf = parseCtx.getConf();
    MapredWork cplan = createMergeTask(conf, tsMerge, fsConf);
    cplan.setReducer(extract);

    // NOTE: we should gather stats in MR1 (rather than the merge MR job)
    // since it is unknown if the merge MR will be triggered at execution time.

    MoveWork dummyMv = new MoveWork(null, null, null,
        new LoadFileDesc(fsConf.getDirName(), finalName, true, null, null), false);

    ConditionalTask cndTsk = createCondTask(conf, currTask, dummyMv, cplan,
        fsConf.getDirName());

    linkMoveTask(ctx, newOutput, cndTsk);
  }

  /**
   * Create a MapReduce job for a particular partition if Hadoop version is pre 0.20,
   * otherwise create a Map-only job using CombineHiveInputFormat for all partitions.
   * @param fsOp The FileSink operator.
   * @param ctx The MR processing context.
   * @param finalName the final destination path the merge job should output.
   * @throws SemanticException
   */
  private void createMergeJob(FileSinkOperator fsOp, GenMRProcContext ctx, String finalName)
      throws SemanticException {

    // if the hadoop version support CombineFileInputFormat (version >= 0.20),
    // create a Map-only job for merge, otherwise create a MapReduce merge job.
    ParseContext parseCtx = ctx.getParseCtx();
    HiveConf conf = parseCtx.getConf();
    if (conf.getBoolVar(HiveConf.ConfVars.HIVEMERGEMAPONLY) &&
        Utilities.supportCombineFileInputFormat()) {
      // create Map-only merge job
      createMap4Merge(fsOp, ctx, finalName);
      LOG.info("use CombineHiveInputformat for the merge job");
    } else {
      if (fsOp.getConf().getDynPartCtx() != null) {
        throw new SemanticException(ErrorMsg.DYNAMIC_PARTITION_MERGE.getMsg());
      }
      createMapReduce4Merge(fsOp, ctx, finalName);
      LOG.info("use HiveInputFormat for the merge job");
    }
  }

  /**
   * create a Map-only merge job with the following operators:
   * @param fsInput
   * @param ctx
   * @param finalName
   *  MR job J0:
   *          ...
   *              |
   *              v
   *         FileSinkOperator_1 (fsInput)
   *             |
   *             v
   *  Merge job J1:
   *             |
   *             v
   *         TableScan (using CombineHiveInputFormat) (tsMerge)
   *             |
   *             v
   *         FileSinkOperator (fsMerge)
   *
   * Here the pathToPartitionInfo & pathToAlias will remain the same, which means the paths do
   * not contain the dynamic partitions (their parent). So after the dynamic partitions are
   * created (after the first job finished before the moveTask or ConditionalTask start),
   * we need to change the pathToPartitionInfo & pathToAlias to include the dynamic partition
   * directories.
   *
   */
  private void createMap4Merge(FileSinkOperator fsInput, GenMRProcContext ctx, String finalName) throws SemanticException {

    //
    // 1. create the operator tree
    //
    ParseContext parseCtx = ctx.getParseCtx();
    FileSinkDesc fsInputDesc = fsInput.getConf();

    // Create a TableScan operator
    RowSchema inputRS = fsInput.getSchema();
    Operator<? extends Serializable> tsMerge = OperatorFactory.get(TableScanDesc.class, inputRS);

    // Create a FileSink operator
    TableDesc ts = (TableDesc) fsInputDesc.getTableInfo().clone();
    FileSinkDesc fsOutputDesc =  new FileSinkDesc(finalName, ts,
        parseCtx.getConf().getBoolVar(HiveConf.ConfVars.COMPRESSRESULT));
    FileSinkOperator fsOutput = (FileSinkOperator) OperatorFactory.getAndMakeChild(
        fsOutputDesc,  inputRS, tsMerge);

    // If the input FileSinkOperator is a dynamic partition enabled, the tsMerge input schema
    // needs to include the partition column, and the fsOutput should have
    // a DynamicPartitionCtx to indicate that it needs to dynamically partitioned.
    DynamicPartitionCtx dpCtx = fsInputDesc.getDynPartCtx();
    if (dpCtx != null && dpCtx.getNumDPCols() > 0) {
      // adding DP ColumnInfo to the RowSchema signature
      ArrayList<ColumnInfo> signature = inputRS.getSignature();
      String tblAlias = fsInputDesc.getTableInfo().getTableName();
      LinkedHashMap<String, String> colMap = new LinkedHashMap<String, String>();
      StringBuilder partCols = new StringBuilder();
      for (String dpCol: dpCtx.getDPColNames()) {
        ColumnInfo colInfo = new ColumnInfo(dpCol,
            TypeInfoFactory.stringTypeInfo, // all partition column type should be string
            tblAlias, true); // partition column is virtual column
        signature.add(colInfo);
        colMap.put(dpCol, dpCol); // input and output have the same column name
        partCols.append(dpCol).append('/');
      }
      partCols.setLength(partCols.length()-1); // remove the last '/'
      inputRS.setSignature(signature);

      // create another DynamicPartitionCtx, which has a different input-to-DP column mapping
      DynamicPartitionCtx dpCtx2 = new DynamicPartitionCtx(dpCtx);
      dpCtx2.setInputToDPCols(colMap);
      fsOutputDesc.setDynPartCtx(dpCtx2);

      // update the FileSinkOperator to include partition columns
      fsInputDesc.getTableInfo().getProperties().setProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS,
        partCols.toString()); // list of dynamic partition column names
    } else {
      // non-partitioned table
      fsInputDesc.getTableInfo().getProperties().remove(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    }

    //
    // 2. Constructing a conditional task consisting of a move task and a map reduce task
    //
    MapRedTask currTask = (MapRedTask) ctx.getCurrTask();
    MoveWork dummyMv = new MoveWork(null, null, null,
        new LoadFileDesc(fsInputDesc.getDirName(), finalName, true, null, null), false);
    MapredWork cplan;

    if(parseCtx.getConf().getBoolVar(HiveConf.ConfVars.
        HIVEMERGERCFILEBLOCKLEVEL) &&
        fsInputDesc.getTableInfo().getInputFileFormatClass().
        equals(RCFileInputFormat.class)) {

      // Check if InputFormatClass is valid
      String inputFormatClass = parseCtx.getConf().
          getVar(HiveConf.ConfVars.HIVEMERGEINPUTFORMATBLOCKLEVEL);
      try {
        Class c = (Class <? extends InputFormat>) Class.forName(inputFormatClass);

        LOG.info("RCFile format- Using block level merge");
        cplan = createRCFileMergeTask(fsInputDesc, finalName,
            dpCtx != null && dpCtx.getNumDPCols() > 0);
      } catch (ClassNotFoundException e) {
        String msg = "Illegal input format class: " + inputFormatClass;
        throw new SemanticException(msg);
      }

    } else {
      cplan = createMergeTask(ctx.getConf(), tsMerge, fsInputDesc);
      // use CombineHiveInputFormat for map-only merging
    }
    cplan.setInputformat("org.apache.hadoop.hive.ql.io.CombineHiveInputFormat");
    // NOTE: we should gather stats in MR1 rather than MR2 at merge job since we don't
    // know if merge MR2 will be triggered at execution time
    ConditionalTask cndTsk = createCondTask(ctx.getConf(), ctx.getCurrTask(), dummyMv, cplan,
        fsInputDesc.getDirName());

    // keep the dynamic partition context in conditional task resolver context
    ConditionalResolverMergeFilesCtx mrCtx =
      (ConditionalResolverMergeFilesCtx) cndTsk.getResolverCtx();
    mrCtx.setDPCtx(fsInputDesc.getDynPartCtx());

    //
    // 3. add the moveTask as the children of the conditional task
    //
    linkMoveTask(ctx, fsOutput, cndTsk);
 }

  /**
   * Make the move task in the GenMRProcContext following the FileSinkOperator a dependent of all
   * possible subtrees branching from the ConditionalTask.
   *
   * @param ctx
   * @param newOutput
   * @param cndTsk
   */
  private void linkMoveTask(GenMRProcContext ctx, FileSinkOperator newOutput,
      ConditionalTask cndTsk) {

    List<Task<MoveWork>> mvTasks = ctx.getMvTask();
    Task<MoveWork> mvTask = findMoveTask(mvTasks, newOutput);

    for (Task<? extends Serializable> tsk : cndTsk.getListTasks()) {
      linkMoveTask(ctx, mvTask, tsk);
    }
  }

  /**
   * Follows the task tree down from task and makes all leaves parents of mvTask
   *
   * @param ctx
   * @param mvTask
   * @param task
   */
  private void linkMoveTask(GenMRProcContext ctx, Task<MoveWork> mvTask,
      Task<? extends Serializable> task) {

    if (task.getDependentTasks() == null || task.getDependentTasks().isEmpty()) {
      // If it's a leaf, add the move task as a child
      addDependentMoveTasks(ctx, mvTask, task);
    } else {
      // Otherwise, for each child run this method recursively
      for (Task<? extends Serializable> childTask : task.getDependentTasks()) {
        linkMoveTask(ctx, mvTask, childTask);
      }
    }
  }

  /**
   * Adds the dependencyTaskForMultiInsert in ctx as a dependent of parentTask.  If mvTask is a
   * load table, and HIVE_MULTI_INSERT_ATOMIC_OUTPUTS is set, adds mvTask as a dependent of
   * dependencyTaskForMultiInsert in ctx, otherwise adds mvTask as a dependent of parentTask as
   * well.
   * @param ctx
   * @param mvTask
   * @param parentTask
   */
  private void addDependentMoveTasks(GenMRProcContext ctx, Task<MoveWork> mvTask,
      Task<? extends Serializable> parentTask) {

    if (mvTask != null) {
      if (ctx.getConf().getBoolVar(
          HiveConf.ConfVars.HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES)) {

        DependencyCollectionTask dependencyTask = ctx.getDependencyTaskForMultiInsert();
        parentTask.addDependentTask(dependencyTask);
        if (mvTask.getWork().getLoadTableWork() != null) {
          // Moving tables/partitions depend on the dependencyTask
          dependencyTask.addDependentTask(mvTask);
        } else {
          // Moving files depends on the parentTask (we still want the dependencyTask to depend
          // on the parentTask)
          parentTask.addDependentTask(mvTask);
        }
      } else {
        parentTask.addDependentTask(mvTask);
      }
    }
  }

  /**
   * Create a MapredWork based on input path, the top operator and the input
   * table descriptor.
   * @param conf
   * @param topOp the table scan operator that is the root of the MapReduce task.
   * @param fsDesc the file sink descriptor that serves as the input to this merge task.
   * @param parentMR the parent MapReduce work
   * @param parentFS the last FileSinkOperator in the parent MapReduce work
   * @return the MapredWork
   */
  private MapredWork createMergeTask(HiveConf conf, Operator<? extends Serializable> topOp,
      FileSinkDesc fsDesc) {

    ArrayList<String> aliases = new ArrayList<String>();
    String inputDir = fsDesc.getDirName();
    TableDesc tblDesc = fsDesc.getTableInfo();
    aliases.add(inputDir); // dummy alias: just use the input path

    // constructing the default MapredWork
    MapredWork cplan = GenMapRedUtils.getMapRedWorkFromConf(conf);
    cplan.getPathToAliases().put(inputDir, aliases);
    cplan.getPathToPartitionInfo().put(inputDir, new PartitionDesc(tblDesc, null));
    cplan.setNumReduceTasks(0);
    cplan.getAliasToWork().put(inputDir, topOp);
    cplan.setMapperCannotSpanPartns(true);

    return cplan;
  }

  /**
   * Create a block level merge task for RCFiles.
   * @param fsInputDesc
   * @param finalName
   * @return MergeWork if table is stored as RCFile,
   *         null otherwise
   */
  private MapredWork createRCFileMergeTask(FileSinkDesc fsInputDesc,
      String finalName, boolean hasDynamicPartitions) throws SemanticException {

    String inputDir = fsInputDesc.getDirName();
    TableDesc tblDesc = fsInputDesc.getTableInfo();

    if(tblDesc.getInputFileFormatClass().equals(RCFileInputFormat.class)) {
      ArrayList<String> inputDirs = new ArrayList<String>();
      if (!hasDynamicPartitions) {
        inputDirs.add(inputDir);
      }

      MergeWork work = new MergeWork(inputDirs, finalName,
          hasDynamicPartitions, fsInputDesc.getDynPartCtx());
      LinkedHashMap<String, ArrayList<String>> pathToAliases =
          new LinkedHashMap<String, ArrayList<String>>();
      pathToAliases.put(inputDir, (ArrayList<String>) inputDirs.clone());
      work.setMapperCannotSpanPartns(true);
      work.setPathToAliases(pathToAliases);
      work.setAliasToWork(
          new LinkedHashMap<String, Operator<? extends Serializable>>());
      if (hasDynamicPartitions) {
        work.getPathToPartitionInfo().put(inputDir,
            new PartitionDesc(tblDesc, null));
      }

      return work;
    }

    throw new SemanticException("createRCFileMergeTask called on non-RCFile table");
  }

  /**
   * Construct a conditional task given the current leaf task, the MoveWork and the MapredWork.
   * @param conf HiveConf
   * @param currTask current leaf task
   * @param mvWork MoveWork for the move task
   * @param mergeWork MapredWork for the merge task.
   * @param inputPath the input directory of the merge/move task
   * @return The conditional task
   */
  private ConditionalTask createCondTask(HiveConf conf,
      Task<? extends Serializable> currTask, MoveWork mvWork,
      MapredWork mergeWork, String inputPath) {

    // There are 3 options for this ConditionalTask:
    // 1) Merge the partitions
    // 2) Move the partitions (i.e. don't merge the partitions)
    // 3) Merge some partitions and move other partitions (i.e. merge some partitions and don't
    //    merge others) in this case the merge is done first followed by the move to prevent
    //    conflicts.
    Task<? extends Serializable> mergeOnlyMergeTask = TaskFactory.get(mergeWork, conf);
    Task<? extends Serializable> moveOnlyMoveTask = TaskFactory.get(mvWork, conf);
    Task<? extends Serializable> mergeAndMoveMergeTask = TaskFactory.get(mergeWork, conf);
    Task<? extends Serializable> mergeAndMoveMoveTask = TaskFactory.get(mvWork, conf);

    // NOTE! It is necessary merge task is the parent of the move task, and not
    // the other way around, for the proper execution of the execute method of
    // ConditionalTask
    mergeAndMoveMergeTask.addDependentTask(mergeAndMoveMoveTask);

    List<Serializable> listWorks = new ArrayList<Serializable>();
    listWorks.add(mvWork);
    listWorks.add(mergeWork);

    ConditionalWork cndWork = new ConditionalWork(listWorks);

    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    listTasks.add(moveOnlyMoveTask);
    listTasks.add(mergeOnlyMergeTask);
    listTasks.add(mergeAndMoveMergeTask);

    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, conf);
    cndTsk.setListTasks(listTasks);

    // create resolver
    cndTsk.setResolver(new ConditionalResolverMergeFiles());
    ConditionalResolverMergeFilesCtx mrCtx =
      new ConditionalResolverMergeFilesCtx(listTasks, inputPath);
    cndTsk.setResolverCtx(mrCtx);

    // make the conditional task as the child of the current leaf task
    currTask.addDependentTask(cndTsk);

    return cndTsk;
  }

  private Task<MoveWork> findMoveTask(
      List<Task<MoveWork>> mvTasks, FileSinkOperator fsOp) {
    // find the move task
    for (Task<MoveWork> mvTsk : mvTasks) {
      MoveWork mvWork = mvTsk.getWork();
      String srcDir = null;
      if (mvWork.getLoadFileWork() != null) {
        srcDir = mvWork.getLoadFileWork().getSourceDir();
      } else if (mvWork.getLoadTableWork() != null) {
        srcDir = mvWork.getLoadTableWork().getSourceDir();
      }

      if ((srcDir != null)
          && (srcDir.equalsIgnoreCase(fsOp.getConf().getDirName()))) {
        return mvTsk;
      }
    }
    return null;
  }

  /**
   * Process the FileSink operator to generate a MoveTask if necessary.
   * @param nd current FileSink operator
   * @param stack parent operators
   * @param opProcCtx
   * @param chDir whether the operator should be first output to a tmp dir and then merged
   *        to the final dir later
   * @return the final file name to which the FileSinkOperator should store.
   * @throws SemanticException
   */
  private String processFS(Node nd, Stack<Node> stack,
      NodeProcessorCtx opProcCtx, boolean chDir) throws SemanticException {

    // Is it the dummy file sink after the mapjoin
    FileSinkOperator fsOp = (FileSinkOperator) nd;
    if ((fsOp.getParentOperators().size() == 1)
        && (fsOp.getParentOperators().get(0) instanceof MapJoinOperator)) {
      return null;
    }

    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    List<FileSinkOperator> seenFSOps = ctx.getSeenFileSinkOps();
    if (seenFSOps == null) {
      seenFSOps = new ArrayList<FileSinkOperator>();
    }
    if (!seenFSOps.contains(fsOp)) {
      seenFSOps.add(fsOp);
    }
    ctx.setSeenFileSinkOps(seenFSOps);

    Task<? extends Serializable> currTask = ctx.getCurrTask();

    // If the directory needs to be changed, send the new directory
    String dest = null;

    if (chDir) {
      dest = fsOp.getConf().getDirName();

      // generate the temporary file
      // it must be on the same file system as the current destination
      ParseContext parseCtx = ctx.getParseCtx();
      Context baseCtx = parseCtx.getContext();
      String tmpDir = baseCtx.getExternalTmpFileURI((new Path(dest)).toUri());

      fsOp.getConf().setDirName(tmpDir);
    }

    Task<MoveWork> mvTask = null;

    if (!chDir) {
      mvTask = findMoveTask(ctx.getMvTask(), fsOp);
    }

    Operator<? extends Serializable> currTopOp = ctx.getCurrTopOp();
    String currAliasId = ctx.getCurrAliasId();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap =
      ctx.getOpTaskMap();
    List<Operator<? extends Serializable>> seenOps = ctx.getSeenOps();
    List<Task<? extends Serializable>> rootTasks = ctx.getRootTasks();

    // Set the move task to be dependent on the current task
    if (mvTask != null) {

      addDependentMoveTasks(ctx, mvTask, currTask);
    }

    // In case of multi-table insert, the path to alias mapping is needed for
    // all the sources. Since there is no
    // reducer, treat it as a plan with null reducer
    // If it is a map-only job, the task needs to be processed
    if (currTopOp != null) {
      Task<? extends Serializable> mapTask = opTaskMap.get(null);
      if (mapTask == null) {
        if (!seenOps.contains(currTopOp)) {
          seenOps.add(currTopOp);
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp,
              (MapredWork) currTask.getWork(), false, ctx);
        }
        opTaskMap.put(null, currTask);
        if (!rootTasks.contains(currTask)) {
          rootTasks.add(currTask);
        }
      } else {
        if (!seenOps.contains(currTopOp)) {
          seenOps.add(currTopOp);
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp,
              (MapredWork) mapTask.getWork(), false, ctx);
        } else {
          UnionOperator currUnionOp = ctx.getCurrUnionOp();
          if (currUnionOp != null) {
            opTaskMap.put(null, currTask);
            ctx.setCurrTopOp(null);
            GenMapRedUtils.initUnionPlan(ctx, currTask, false);
            return dest;
          }
        }
        // mapTask and currTask should be merged by and join/union operator
        // (e.g., GenMRUnion1j) which has multiple topOps.
        // assert mapTask == currTask : "mapTask.id = " + mapTask.getId()
        // + "; currTask.id = " + currTask.getId();
      }

      return dest;

    }

    UnionOperator currUnionOp = ctx.getCurrUnionOp();

    if (currUnionOp != null) {
      opTaskMap.put(null, currTask);
      GenMapRedUtils.initUnionPlan(ctx, currTask, false);
      return dest;
    }

    AbstractMapJoinOperator<? extends MapJoinDesc> currMapJoinOp = ctx.getCurrMapJoinOp();

    if (currMapJoinOp != null) {
      opTaskMap.put(null, currTask);
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(currMapJoinOp);
      MapredWork plan = (MapredWork) currTask.getWork();

      String taskTmpDir = mjCtx.getTaskTmpDir();
      TableDesc tt_desc = mjCtx.getTTDesc();
      assert plan.getPathToAliases().get(taskTmpDir) == null;
      plan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
      plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
      plan.getPathToPartitionInfo().put(taskTmpDir,
          new PartitionDesc(tt_desc, null));
      plan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
      return dest;
    }

    return dest;
  }
}
