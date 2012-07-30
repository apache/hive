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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.MapredLocalTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx;

/**
 * An implementation of PhysicalPlanResolver. It iterator each MapRedTask to see whether the task
 * has a local map work if it has, it will move the local work to a new local map join task. Then it
 * will make this new generated task depends on current task's parent task and make current task
 * depends on this new generated task.
 */
public class MapJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    // create dispatcher and graph walker
    Dispatcher disp = new LocalMapJoinTaskDispatcher(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.rootTasks);

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * Iterator each tasks. If this task has a local work,create a new task for this local work, named
   * MapredLocalTask. then make this new generated task depends on current task's parent task, and
   * make current task depends on this new generated task
   */
  class LocalMapJoinTaskDispatcher implements Dispatcher {

    private PhysicalContext physicalContext;

    public LocalMapJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    private void processCurrentTask(Task<? extends Serializable> currTask,
        ConditionalTask conditionalTask) throws SemanticException {
      // get current mapred work and its local work
      MapredWork mapredWork = (MapredWork) currTask.getWork();
      MapredLocalWork localwork = mapredWork.getMapLocalWork();
      if (localwork != null) {
        // get the context info and set up the shared tmp URI
        Context ctx = physicalContext.getContext();
        String tmpFileURI = Utilities.generateTmpURI(ctx.getLocalTmpFileURI(), currTask.getId());
        localwork.setTmpFileURI(tmpFileURI);
        String hdfsTmpURI = Utilities.generateTmpURI(ctx.getMRTmpFileURI(), currTask.getId());
        mapredWork.setTmpHDFSFileURI(hdfsTmpURI);
        // create a task for this local work; right now, this local work is shared
        // by the original MapredTask and this new generated MapredLocalTask.
        MapredLocalTask localTask = (MapredLocalTask) TaskFactory.get(localwork, physicalContext
            .getParseContext().getConf());

        // set the backup task from curr task
        localTask.setBackupTask(currTask.getBackupTask());
        localTask.setBackupChildrenTasks(currTask.getBackupChildrenTasks());
        currTask.setBackupChildrenTasks(null);
        currTask.setBackupTask(null);

        if (currTask.getTaskTag() == Task.CONVERTED_MAPJOIN) {
          localTask.setTaskTag(Task.CONVERTED_LOCAL_MAPJOIN);
        } else {
          localTask.setTaskTag(Task.LOCAL_MAPJOIN);
        }
        // replace the map join operator to local_map_join operator in the operator tree
        // and return all the dummy parent
        LocalMapJoinProcCtx  localMapJoinProcCtx= adjustLocalTask(localTask);
        List<Operator<? extends Serializable>> dummyOps = localMapJoinProcCtx.getDummyParentOp();

        // create new local work and setup the dummy ops
        MapredLocalWork newLocalWork = new MapredLocalWork();
        newLocalWork.setDummyParentOp(dummyOps);
        newLocalWork.setTmpFileURI(tmpFileURI);
        newLocalWork.setInputFileChangeSensitive(localwork.getInputFileChangeSensitive());
        newLocalWork.setBucketMapjoinContext(localwork.copyPartSpecMappingOnly());
        mapredWork.setMapLocalWork(newLocalWork);
        // get all parent tasks
        List<Task<? extends Serializable>> parentTasks = currTask.getParentTasks();
        currTask.setParentTasks(null);
        if (parentTasks != null) {
          for (Task<? extends Serializable> tsk : parentTasks) {
            // make new generated task depends on all the parent tasks of current task.
            tsk.addDependentTask(localTask);
            // remove the current task from its original parent task's dependent task
            tsk.removeDependentTask(currTask);
          }
        } else {
          // in this case, current task is in the root tasks
          // so add this new task into root tasks and remove the current task from root tasks
          if (conditionalTask == null) {
            physicalContext.addToRootTask(localTask);
            physicalContext.removeFromRootTask(currTask);
          } else {
            // set list task
            List<Task<? extends Serializable>> listTask = conditionalTask.getListTasks();
            ConditionalWork conditionalWork = conditionalTask.getWork();
            int index = listTask.indexOf(currTask);
            listTask.set(index, localTask);
            // set list work
            List<Serializable> listWork = (List<Serializable>) conditionalWork.getListWorks();
            index = listWork.indexOf(mapredWork);
            listWork.set(index, (Serializable) localwork);
            conditionalWork.setListWorks(listWork);
            ConditionalResolver resolver = conditionalTask.getResolver();
            if (resolver instanceof ConditionalResolverSkewJoin) {
              // get bigKeysDirToTaskMap
              ConditionalResolverSkewJoinCtx context = (ConditionalResolverSkewJoinCtx) conditionalTask
                  .getResolverCtx();
              HashMap<String, Task<? extends Serializable>> bigKeysDirToTaskMap = context
                  .getDirToTaskMap();
              // to avoid concurrent modify the hashmap
              HashMap<String, Task<? extends Serializable>> newbigKeysDirToTaskMap = new HashMap<String, Task<? extends Serializable>>();
              // reset the resolver
              for (Map.Entry<String, Task<? extends Serializable>> entry : bigKeysDirToTaskMap
                  .entrySet()) {
                Task<? extends Serializable> task = entry.getValue();
                String key = entry.getKey();
                if (task.equals(currTask)) {
                  newbigKeysDirToTaskMap.put(key, localTask);
                } else {
                  newbigKeysDirToTaskMap.put(key, task);
                }
              }
              context.setDirToTaskMap(newbigKeysDirToTaskMap);
              conditionalTask.setResolverCtx(context);
            } else if (resolver instanceof ConditionalResolverCommonJoin) {
              // get bigKeysDirToTaskMap
              ConditionalResolverCommonJoinCtx context = (ConditionalResolverCommonJoinCtx) conditionalTask
                  .getResolverCtx();
              HashMap<String, Task<? extends Serializable>> aliasToWork = context.getAliasToTask();
              // to avoid concurrent modify the hashmap
              HashMap<String, Task<? extends Serializable>> newAliasToWork = new HashMap<String, Task<? extends Serializable>>();
              // reset the resolver
              for (Map.Entry<String, Task<? extends Serializable>> entry : aliasToWork.entrySet()) {
                Task<? extends Serializable> task = entry.getValue();
                String key = entry.getKey();

                if (task.equals(currTask)) {
                  newAliasToWork.put(key, localTask);
                } else {
                  newAliasToWork.put(key, task);
                }
              }
              context.setAliasToTask(newAliasToWork);
              conditionalTask.setResolverCtx(context);
            }
          }
        }
        // make current task depends on this new generated localMapJoinTask
        // now localTask is the parent task of the current task
        localTask.addDependentTask(currTask);
      }
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      // not map reduce task or not conditional task, just skip
      if (currTask.isMapRedTask()) {
        if (currTask instanceof ConditionalTask) {
          // get the list of task
          List<Task<? extends Serializable>> taskList = ((ConditionalTask) currTask).getListTasks();
          for (Task<? extends Serializable> tsk : taskList) {
            if (tsk.isMapRedTask()) {
              this.processCurrentTask(tsk, ((ConditionalTask) currTask));
            }
          }
        } else {
          this.processCurrentTask(currTask, null);
        }
      }
      return null;
    }

    // replace the map join operator to local_map_join operator in the operator tree
    private LocalMapJoinProcCtx adjustLocalTask(MapredLocalTask task)
        throws SemanticException {
      LocalMapJoinProcCtx localMapJoinProcCtx = new LocalMapJoinProcCtx(task, physicalContext
          .getParseContext());
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", "MAPJOIN%"), LocalMapJoinProcFactory.getJoinProc());
      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(LocalMapJoinProcFactory.getDefaultProc(),
          opRules, localMapJoinProcCtx);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      // iterator the reducer operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(task.getWork().getAliasToWork().values());
      ogw.startWalking(topNodes, null);
      return localMapJoinProcCtx;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  /**
   * A container of current task and parse context.
   */
  public static class LocalMapJoinProcCtx implements NodeProcessorCtx {
    private Task<? extends Serializable> currentTask;
    private ParseContext parseCtx;
    private List<Operator<? extends Serializable>> dummyParentOp = null;
    private boolean isFollowedByGroupBy;

    public LocalMapJoinProcCtx(Task<? extends Serializable> task, ParseContext parseCtx) {
      currentTask = task;
      this.parseCtx = parseCtx;
      dummyParentOp = new ArrayList<Operator<? extends Serializable>>();
      isFollowedByGroupBy = false;
    }

    public Task<? extends Serializable> getCurrentTask() {
      return currentTask;
    }

    public void setCurrentTask(Task<? extends Serializable> currentTask) {
      this.currentTask = currentTask;
    }

    public boolean isFollowedByGroupBy() {
      return isFollowedByGroupBy;
    }

    public void setFollowedByGroupBy(boolean isFollowedByGroupBy) {
      this.isFollowedByGroupBy = isFollowedByGroupBy;
    }
    public ParseContext getParseCtx() {
      return parseCtx;
    }

    public void setParseCtx(ParseContext parseCtx) {
      this.parseCtx = parseCtx;
    }

    public void setDummyParentOp(List<Operator<? extends Serializable>> op) {
      this.dummyParentOp = op;
    }

    public List<Operator<? extends Serializable>> getDummyParentOp() {
      return this.dummyParentOp;
    }

    public void addDummyParentOp(Operator<? extends Serializable> op) {
      this.dummyParentOp.add(op);
    }
  }
}
