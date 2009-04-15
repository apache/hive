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
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Stack;
import java.io.Serializable;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * General utility common functions for the Processor to convert operator into map-reduce tasks
 */
public class GenMapRedUtils {
  private static Log LOG;

  static {
    LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils");
  }

  /**
   * Initialize the current plan by adding it to root tasks
   * @param op the reduce sink operator encountered
   * @param opProcCtx processing context
   */
  public static void initPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx) throws SemanticException {
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = opProcCtx.getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask    = mapredCtx.getCurrTask();
    mapredWork plan = (mapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx.getOpTaskMap();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();

    opTaskMap.put(reducer, currTask);
    plan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc)op.getConf();
    
    plan.setNumReduceTasks(desc.getNumReducers());

    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();

    rootTasks.add(currTask);
    if (reducer.getClass() == JoinOperator.class)
      plan.setNeedsTagging(true);

    assert currTopOp != null;
    List<Operator<? extends Serializable>> seenOps = opProcCtx.getSeenOps();
    String currAliasId = opProcCtx.getCurrAliasId();
      
    seenOps.add(currTopOp);
    setTaskPlan(currAliasId, currTopOp, op, plan, false, opProcCtx);

    currTopOp = null;
    currAliasId = null;

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
  }

  /**
   * Merge the current task with the task for the current reducer
   * @param task for the old task for the current reducer
   * @param opProcCtx processing context
   */
  public static void joinPlan(ReduceSinkOperator op,
                              Task<? extends Serializable> oldTask, 
                              Task<? extends Serializable> task, 
                              GenMRProcContext opProcCtx) throws SemanticException {
    Task<? extends Serializable> currTask = task;
    mapredWork plan = (mapredWork) currTask.getWork();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();

    // terminate the old task and make current task dependent on it
    if (oldTask != null) {
      splitTasks(op, oldTask, currTask, opProcCtx);
    }

    if (currTopOp != null) {
      List<Operator<? extends Serializable>> seenOps = opProcCtx.getSeenOps();
      String                                 currAliasId = opProcCtx.getCurrAliasId();
      
      if (!seenOps.contains(currTopOp)) {
        seenOps.add(currTopOp);
        setTaskPlan(currAliasId, currTopOp, op, plan, false, opProcCtx);
      }
      currTopOp = null;
      opProcCtx.setCurrTopOp(currTopOp);
    }

    opProcCtx.setCurrTask(currTask);
  }

  /**
   * Split the current plan by creating a temporary destination
   * @param op the reduce sink operator encountered
   * @param opProcCtx processing context
   */
  public static void splitPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx) 
    throws SemanticException {
    // Generate a new task              
    mapredWork cplan = getMapRedWork();
    ParseContext parseCtx = opProcCtx.getParseCtx();
    Task<? extends Serializable> redTask = TaskFactory.get(cplan, parseCtx.getConf());
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);

    // Add the reducer
    cplan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc)op.getConf();
    
    cplan.setNumReduceTasks(new Integer(desc.getNumReducers()));

    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx.getOpTaskMap();
    opTaskMap.put(reducer, redTask);
    Task<? extends Serializable> currTask    = opProcCtx.getCurrTask();

    splitTasks(op, currTask, redTask, opProcCtx);
    opProcCtx.getRootOps().add(op);
  }

  /**
   * set the current task in the mapredWork
   * @param alias_id current alias
   * @param topOp    the top operator of the stack
   * @param plan     current plan
   * @param local    whether you need to add to map-reduce or local work
   * @param opProcCtx processing context
   */
  public static void setTaskPlan(String alias_id, Operator<? extends Serializable> topOp, Operator<? extends Serializable> currOp,
      mapredWork plan, boolean local, GenMRProcContext opProcCtx) 
    throws SemanticException {
    ParseContext parseCtx = opProcCtx.getParseCtx();

    if (!local) {
      // Generate the map work for this alias_id
      PartitionPruner pruner = parseCtx.getAliasToPruner().get(alias_id);
      Set<Partition> parts = null;
      try {
        // pass both confirmed and unknown partitions through the map-reduce framework
        PartitionPruner.PrunedPartitionList partsList = pruner.prune();

        parts = partsList.getConfirmedPartns();
        parts.addAll(partsList.getUnknownPartns());
      } catch (HiveException e) {
        // Has to use full name to make sure it does not conflict with org.apache.commons.lang.StringUtils
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
      SamplePruner samplePruner = parseCtx.getAliasToSamplePruner().get(alias_id);
      
      for (Partition part : parts) {
        // Later the properties have to come from the partition as opposed
        // to from the table in order to support versioning.
        Path paths[];
        if (samplePruner != null) {
          paths = samplePruner.prune(part);
        }
        else {
          paths = part.getPath();
        }
        
        for (Path p: paths) {
          String path = p.toString();
          LOG.debug("Adding " + path + " of table" + alias_id);
          // Add the path to alias mapping
          if (plan.getPathToAliases().get(path) == null) {
            plan.getPathToAliases().put(path, new ArrayList<String>());
          }
          plan.getPathToAliases().get(path).add(alias_id);
          plan.getPathToPartitionInfo().put(path, Utilities.getPartitionDesc(part));
          LOG.debug("Information added for path " + path);
        }
      }
      plan.getAliasToWork().put(alias_id, topOp);
      setKeyAndValueDesc(plan, currOp);
      LOG.debug("Created Map Work for " + alias_id);
    }
    else {
      FileSinkOperator fOp = (FileSinkOperator) topOp;
      fileSinkDesc fConf = (fileSinkDesc)fOp.getConf();
      // populate local work if needed
    }
  }

  /**
   * set key and value descriptor
   * @param plan     current plan
   * @param topOp    current top operator in the path
   */
  public static void setKeyAndValueDesc(mapredWork plan, Operator<? extends Serializable> topOp) {
    if (topOp == null)
      return;
    
    if (topOp instanceof ReduceSinkOperator) {
      ReduceSinkOperator rs = (ReduceSinkOperator)topOp;
      plan.setKeyDesc(rs.getConf().getKeySerializeInfo());
      int tag = Math.max(0, rs.getConf().getTag());
      List<tableDesc> tagToSchema = plan.getTagToValueDesc();
      while (tag + 1 > tagToSchema.size()) {
        tagToSchema.add(null);
      }
      tagToSchema.set(tag, rs.getConf().getValueSerializeInfo());
    } else {
      List<Operator<? extends Serializable>> children = topOp.getChildOperators(); 
      if (children != null) {
        for(Operator<? extends Serializable> op: children) {
          setKeyAndValueDesc(plan, op);
        }
      }
    }
  }

  /**
   * create a new plan and return
   * @return the new plan
   */
  public static mapredWork getMapRedWork() {
    mapredWork work = new mapredWork();
    work.setPathToAliases(new LinkedHashMap<String, ArrayList<String>>());
    work.setPathToPartitionInfo(new LinkedHashMap<String, partitionDesc>());
    work.setAliasToWork(new HashMap<String, Operator<? extends Serializable>>());
    work.setTagToValueDesc(new ArrayList<tableDesc>());
    work.setReducer(null);
    return work;
  }

  /**
   * insert in the map for the operator to row resolver
   * @param op operator created
   * @param rr row resolver
   * @param parseCtx parse context
   */
  @SuppressWarnings("nls")
  private static Operator<? extends Serializable> putOpInsertMap(Operator<? extends Serializable> op, RowResolver rr, ParseContext parseCtx) 
  {
    OpParseContext ctx = new OpParseContext(rr);
    parseCtx.getOpParseCtx().put(op, ctx);
    return op;
  }

  @SuppressWarnings("nls")
  /**
   * Merge the tasks - by creating a temporary file between them.
   * @param op reduce sink operator being processed
   * @param oldTask the parent task
   * @param task the child task
   * @param opProcCtx context
   **/
  private static void splitTasks(ReduceSinkOperator op,
                                 Task<? extends Serializable> parentTask, 
                                 Task<? extends Serializable> childTask, 
                                 GenMRProcContext opProcCtx) throws SemanticException {
    mapredWork plan = (mapredWork) childTask.getWork();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();
    
    ParseContext parseCtx = opProcCtx.getParseCtx();
    parentTask.addDependentTask(childTask);

    // Root Task cannot depend on any other task, therefore childTask cannot be a root Task
    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
    if (rootTasks.contains(childTask))
      rootTasks.remove(childTask);

    // generate the temporary file
    String scratchDir = opProcCtx.getScratchDir();
    int randomid = opProcCtx.getRandomId();
    int pathid   = opProcCtx.getPathId();
      
    String taskTmpDir = (new Path(scratchDir + File.separator + randomid + '.' + pathid)).toString();
    pathid++;
    opProcCtx.setPathId(pathid);
    
    Operator<? extends Serializable> parent = op.getParentOperators().get(0);
    tableDesc tt_desc = 
      PlanUtils.getBinaryTableDesc(PlanUtils.getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol")); 
    
    // Create a file sink operator for this file name
    Operator<? extends Serializable> fs_op =
      putOpInsertMap(OperatorFactory.get
                     (new fileSinkDesc(taskTmpDir, tt_desc,
                                       parseCtx.getConf().getBoolVar(HiveConf.ConfVars.COMPRESSINTERMEDIATE)),
                      parent.getSchema()), null, parseCtx);
    
    // replace the reduce child with this operator
    List<Operator<? extends Serializable>> childOpList = parent.getChildOperators();
    for (int pos = 0; pos < childOpList.size(); pos++) {
      if (childOpList.get(pos) == op) {
        childOpList.set(pos, fs_op);
        break;
      }
    }
    
    List<Operator<? extends Serializable>> parentOpList = new ArrayList<Operator<? extends Serializable>>();
    parentOpList.add(parent);
    fs_op.setParentOperators(parentOpList);
    
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    
    String streamDesc;
    mapredWork cplan = (mapredWork) childTask.getWork();
    
    if (reducer.getClass() == JoinOperator.class) {
      String origStreamDesc;
      streamDesc = "$INTNAME";
      origStreamDesc = streamDesc;
      int pos = 0;
      while (cplan.getAliasToWork().get(streamDesc) != null)
        streamDesc = origStreamDesc.concat(String.valueOf(++pos));
    }
    else
      streamDesc = taskTmpDir;
    
    // Add the path to alias mapping
    if (cplan.getPathToAliases().get(taskTmpDir) == null) {
      cplan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
    }
    
    cplan.getPathToAliases().get(taskTmpDir).add(streamDesc);
    cplan.getPathToPartitionInfo().put(taskTmpDir, new partitionDesc(tt_desc, null));
    cplan.getAliasToWork().put(streamDesc, op);
    setKeyAndValueDesc(cplan, op);

    // TODO: Allocate work to remove the temporary files and make that
    // dependent on the redTask
    if (reducer.getClass() == JoinOperator.class)
      cplan.setNeedsTagging(true);

    currTopOp = null;
    String currAliasId = null;
    
    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
    opProcCtx.setCurrTask(childTask);
  }    
}
