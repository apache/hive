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

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverMergeFiles.ConditionalResolverMergeFilesCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.extractDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Processor for the rule - table scan followed by reduce sink
 */
public class GenMRFileSink1 implements NodeProcessor {

  public GenMRFileSink1() {
  }

  /**
   * File Sink Operator encountered 
   * @param nd the file sink operator encountered
   * @param opProcCtx context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx, Object... nodeOutputs) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext)opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    boolean chDir = false;
    Task<? extends Serializable> currTask = ctx.getCurrTask();

    // Has the user enabled merging of files for map-only jobs or for all jobs
    if ((ctx.getMvTask() != null) && (!ctx.getMvTask().isEmpty())) 
    {
      List<Task<? extends Serializable>> mvTasks = ctx.getMvTask();

      // In case of unions or map-joins, it is possible that the file has already been seen.
      // So, no need to attempt to merge the files again.
      if ((ctx.getSeenFileSinkOps() == null) ||
          (!ctx.getSeenFileSinkOps().contains((FileSinkOperator)nd)))  {
        
        // no need of merging if the move is to a local file system
        MoveTask mvTask = (MoveTask)findMoveTask(mvTasks, (FileSinkOperator)nd);
        if ((mvTask != null) && !mvTask.isLocal())
        {
          // There are separate configuration parameters to control whether to merge for a map-only job
          // or for a map-reduce job
          if ((parseCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVEMERGEMAPFILES) &&
              (((mapredWork)currTask.getWork()).getReducer() == null)) ||
              parseCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVEMERGEMAPREDFILES))
            chDir = true;
        }
      }
    }

    String finalName = processFS(nd, stack, opProcCtx, chDir);
    
    // If it is a map-only job, insert a new task to do the concatenation
    if (chDir && (finalName != null)) {
      createMergeJob((FileSinkOperator)nd, ctx, finalName);
    }
    
    return null;
  }
  
  private void createMergeJob(FileSinkOperator fsOp, GenMRProcContext ctx, String finalName) {
    Task<? extends Serializable> currTask = ctx.getCurrTask();
    RowSchema fsRS = fsOp.getSchema();
    
    // create a reduce Sink operator - key is the first column
    ArrayList<exprNodeDesc> keyCols = new ArrayList<exprNodeDesc>();
    keyCols.add(TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("rand"));

    ArrayList<exprNodeDesc> valueCols = new ArrayList<exprNodeDesc>();
    for (ColumnInfo ci : fsRS.getSignature()) {
      valueCols.add(new exprNodeColumnDesc(ci.getType(), ci.getInternalName()));
    }

    // create a dummy tableScan operator
    Operator<? extends Serializable> ts_op = 
      OperatorFactory.get(tableScanDesc.class, fsRS);

    ArrayList<String> outputColumns = new ArrayList<String>();
    for (int i = 0; i < valueCols.size(); i++)
      outputColumns.add(SemanticAnalyzer.getColumnInternalName(i));
    
    reduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(new ArrayList<exprNodeDesc>(), valueCols, 
                                                        outputColumns, false, -1, -1, -1); 
    ReduceSinkOperator rsOp = (ReduceSinkOperator)OperatorFactory.getAndMakeChild(rsDesc, fsRS, ts_op);
    mapredWork cplan = GenMapRedUtils.getMapRedWork();
    ParseContext parseCtx = ctx.getParseCtx();

    Task<? extends Serializable> mergeTask = TaskFactory.get(cplan, parseCtx.getConf());
    fileSinkDesc fsConf = fsOp.getConf();
    
    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = ctx.getParseCtx().getOpParseCtx().get(fsOp).getRR();
    Integer pos = Integer.valueOf(0);
    for(ColumnInfo colInfo: interim_rwsch.getColumnInfos()) {
      String [] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1],
                    new ColumnInfo(pos.toString(), colInfo.getType()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator extract = 
      OperatorFactory.getAndMakeChild(
        new extractDesc(new exprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, Utilities.ReduceField.VALUE.toString())),
        new RowSchema(out_rwsch.getColumnInfos()));
    
    tableDesc ts = (tableDesc)fsConf.getTableInfo().clone();
    fsConf.getTableInfo().getProperties().remove(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    FileSinkOperator newOutput = 
      (FileSinkOperator)OperatorFactory.getAndMakeChild(
         new fileSinkDesc(finalName, ts, 
                          parseCtx.getConf().getBoolVar(HiveConf.ConfVars.COMPRESSINTERMEDIATE)),
         fsRS, extract);

    cplan.setReducer(extract);
    ArrayList<String> aliases = new ArrayList<String>();
    aliases.add(fsConf.getDirName());
    cplan.getPathToAliases().put(fsConf.getDirName(), aliases);
    cplan.getAliasToWork().put(fsConf.getDirName(), ts_op);    
    cplan.getPathToPartitionInfo().put(fsConf.getDirName(), new partitionDesc(fsConf.getTableInfo(), null));
    cplan.setNumReduceTasks(-1);
    
    moveWork dummyMv = new moveWork(null, new loadFileDesc(fsOp.getConf().getDirName(), finalName, true, null, null), false);
    Task<? extends Serializable> dummyMergeTask = TaskFactory.get(dummyMv, ctx.getConf());
    List<Serializable> listWorks = new ArrayList<Serializable>();
    listWorks.add(dummyMv);
    listWorks.add(mergeTask.getWork());
    ConditionalWork cndWork = new ConditionalWork(listWorks);
    
    ConditionalTask cndTsk = (ConditionalTask)TaskFactory.get(cndWork, ctx.getConf());
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    listTasks.add(dummyMergeTask);
    listTasks.add(mergeTask);
    cndTsk.setListTasks(listTasks);
    
    cndTsk.setResolver(new ConditionalResolverMergeFiles());
    cndTsk.setResolverCtx(new ConditionalResolverMergeFilesCtx(listTasks, fsOp.getConf().getDirName()));
    
    currTask.addDependentTask(cndTsk);
    
    List<Task<? extends Serializable>> mvTasks = ctx.getMvTask();
    Task<? extends Serializable> mvTask = findMoveTask(mvTasks, newOutput);
    
    if (mvTask != null)
      cndTsk.addDependentTask(mvTask);
  }
 
  private Task<? extends Serializable> findMoveTask(List<Task<? extends Serializable>> mvTasks, FileSinkOperator fsOp) {
    // find the move task
    for (Task<? extends Serializable> mvTsk : mvTasks) {
      moveWork mvWork = (moveWork)mvTsk.getWork();
      String srcDir = null;
      if (mvWork.getLoadFileWork() != null) 
        srcDir = mvWork.getLoadFileWork().getSourceDir();
      else if (mvWork.getLoadTableWork() != null)
        srcDir = mvWork.getLoadTableWork().getSourceDir();
      
      if ((srcDir != null) && (srcDir.equalsIgnoreCase(fsOp.getConf().getDirName())))
        return mvTsk;
    }
     
    return null;
  }
  
  private String processFS(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx, boolean chDir) 
    throws SemanticException {
    
    // Is it the dummy file sink after the mapjoin
    FileSinkOperator fsOp = (FileSinkOperator)nd;
    if ((fsOp.getParentOperators().size() == 1) && (fsOp.getParentOperators().get(0) instanceof MapJoinOperator))
      return null;

    GenMRProcContext ctx = (GenMRProcContext)opProcCtx;
    List<FileSinkOperator> seenFSOps = ctx.getSeenFileSinkOps();
    if (seenFSOps == null) 
      seenFSOps = new ArrayList<FileSinkOperator>();
    if (!seenFSOps.contains(fsOp))
      seenFSOps.add(fsOp);
    ctx.setSeenFileSinkOps(seenFSOps);

    Task<? extends Serializable> currTask = ctx.getCurrTask();
    
    // If the directory needs to be changed, send the new directory
    String dest = null;

    if (chDir) {
      dest = fsOp.getConf().getDirName();

      // generate the temporary file
      ParseContext parseCtx = ctx.getParseCtx();
      Context baseCtx = parseCtx.getContext();
      String tmpDir = baseCtx.getMRTmpFileURI();
      
      fsOp.getConf().setDirName(tmpDir);
    }
    
    boolean ret = false;
    Task<? extends Serializable> mvTask = null;
    
    if (!chDir)
      mvTask = findMoveTask(ctx.getMvTask(), fsOp);
    
    Operator<? extends Serializable> currTopOp = ctx.getCurrTopOp();
    String currAliasId = ctx.getCurrAliasId();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = ctx.getOpTaskMap();
    List<Operator<? extends Serializable>> seenOps = ctx.getSeenOps();
    List<Task<? extends Serializable>>  rootTasks = ctx.getRootTasks();

    // Set the move task to be dependent on the current task
    if (mvTask != null) 
      ret = currTask.addDependentTask(mvTask);
    
    // In case of multi-table insert, the path to alias mapping is needed for all the sources. Since there is no
    // reducer, treat it as a plan with null reducer
    // If it is a map-only job, the task needs to be processed
    if (currTopOp != null) {
      Task<? extends Serializable> mapTask = opTaskMap.get(null);
      if (mapTask == null) {
        assert (!seenOps.contains(currTopOp));
        seenOps.add(currTopOp);
        GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, (mapredWork) currTask.getWork(), false, ctx);
        opTaskMap.put(null, currTask);
        rootTasks.add(currTask);
      }
      else {
        if (!seenOps.contains(currTopOp)) {
          seenOps.add(currTopOp);
          GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, (mapredWork) mapTask.getWork(), false, ctx);
        }
        if (ret)
          currTask.removeDependentTask(mvTask);
      }

      return dest;

    }

    UnionOperator currUnionOp = ctx.getCurrUnionOp();
    
    if  (currUnionOp != null) {
      opTaskMap.put(null, currTask);
      GenMapRedUtils.initUnionPlan(ctx, currTask, false);
      return dest;
    }
    
    MapJoinOperator currMapJoinOp = ctx.getCurrMapJoinOp();
    
    if  (currMapJoinOp != null) {
      opTaskMap.put(null, currTask);
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(currMapJoinOp);
      mapredWork plan = (mapredWork) currTask.getWork();

      String taskTmpDir = mjCtx.getTaskTmpDir();
      tableDesc tt_desc = mjCtx.getTTDesc(); 
      assert plan.getPathToAliases().get(taskTmpDir) == null;
      plan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
      plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
      plan.getPathToPartitionInfo().put(taskTmpDir, new partitionDesc(tt_desc, null));
      plan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
      return dest;
    }
    
    return dest;
  }
}
