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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker.TaskGraphWalkerContext;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverCommonJoin.ConditionalResolverCommonJoinCtx;


public class CommonJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    // create dispatcher and graph walker
    Dispatcher disp = new CommonJoinTaskDispatcher(pctx);
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
  class CommonJoinTaskDispatcher implements Dispatcher {

    private final PhysicalContext physicalContext;

    public CommonJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    private ConditionalTask processCurrentTask(MapRedTask currTask,
        ConditionalTask conditionalTask, Context context)
        throws SemanticException {

      // whether it contains common join op; if contains, return this common join op
      JoinOperator joinOp = getJoinOp(currTask);
      if (joinOp == null) {
        return null;
      }
      currTask.setTaskTag(Task.COMMON_JOIN);

      MapredWork currWork = currTask.getWork();
      // create conditional work list and task list
      List<Serializable> listWorks = new ArrayList<Serializable>();
      List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();

      // create alias to task mapping and alias to input file mapping for resolver
      HashMap<String, Task<? extends Serializable>> aliasToTask = new HashMap<String, Task<? extends Serializable>>();
      HashMap<String, ArrayList<String>> pathToAliases = currTask.getWork().getPathToAliases();

      // get parseCtx for this Join Operator
      ParseContext parseCtx = physicalContext.getParseContext();
      QBJoinTree joinTree = parseCtx.getJoinContext().get(joinOp);

      // start to generate multiple map join tasks
      JoinDesc joinDesc = joinOp.getConf();
      Byte[] order = joinDesc.getTagOrder();
      int numAliases = order.length;
      
      long aliasTotalKnownInputSize = 0;
      HashMap<String, Long> aliasToSize = new HashMap<String, Long>();
      try {
        // go over all the input paths, and calculate a known total size, known
        // size for each input alias.
        Utilities.getInputSummary(context, currWork, null).getLength();
        
        // set alias to size mapping, this can be used to determine if one table
        // is choosen as big table, what's the total size of left tables, which
        // are going to be small tables.
        for (Map.Entry<String, ArrayList<String>> entry : pathToAliases.entrySet()) {
          String path = entry.getKey();
          List<String> aliasList = entry.getValue();
          ContentSummary cs = context.getCS(path);
          if (cs != null) {
            long size = cs.getLength();
            for (String alias : aliasList) {
              aliasTotalKnownInputSize += size;
              Long es = aliasToSize.get(alias);
              if(es == null) {
                es = new Long(0);
              }
              es += size;
              aliasToSize.put(alias, es);
            }
          }
        }
        
        HashSet<Integer> bigTableCandidates = MapJoinProcessor.getBigTableCandidates(joinDesc.getConds());
        
        // no table could be the big table; there is no need to convert
        if (bigTableCandidates == null) {
          return null;
        }
        currWork.setOpParseCtxMap(parseCtx.getOpParseCtx());
        currWork.setJoinTree(joinTree);

        String xml = currWork.toXML();
        String bigTableAlias = null;

        long ThresholdOfSmallTblSizeSum = HiveConf.getLongVar(context.getConf(),
            HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);
        for (int i = 0; i < numAliases; i++) {
          // this table cannot be big table
          if (!bigTableCandidates.contains(i)) {
            continue;
          }
          
          // create map join task and set big table as i
          // deep copy a new mapred work from xml
          InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
          MapredWork newWork = Utilities.deserializeMapRedWork(in, physicalContext.getConf());
          // create a mapred task for this work
          MapRedTask newTask = (MapRedTask) TaskFactory.get(newWork, physicalContext
              .getParseContext().getConf());
          JoinOperator newJoinOp = getJoinOp(newTask);

          // optimize this newWork and assume big table position is i
          bigTableAlias = MapJoinProcessor.genMapJoinOpAndLocalWork(newWork, newJoinOp, i);

          Long aliasKnownSize = aliasToSize.get(bigTableAlias);
          if (aliasKnownSize != null && aliasKnownSize.longValue() > 0) {
            long smallTblTotalKnownSize = aliasTotalKnownInputSize
                - aliasKnownSize.longValue();
            if(smallTblTotalKnownSize > ThresholdOfSmallTblSizeSum) {
              //this table is not good to be a big table.
              continue;
            }
          }
          
          // add into conditional task
          listWorks.add(newWork);
          listTasks.add(newTask);
          newTask.setTaskTag(Task.CONVERTED_MAPJOIN);

          //set up backup task
          newTask.setBackupTask(currTask);
          newTask.setBackupChildrenTasks(currTask.getChildTasks());

          // put the mapping alias to task
          aliasToTask.put(bigTableAlias, newTask);
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new SemanticException("Generate Map Join Task Error: " + e.getMessage());
      }

      // insert current common join task to conditional task
      listWorks.add(currTask.getWork());
      listTasks.add(currTask);
      // clear JoinTree and OP Parse Context
      currWork.setOpParseCtxMap(null);
      currWork.setJoinTree(null);

      // create conditional task and insert conditional task into task tree
      ConditionalWork cndWork = new ConditionalWork(listWorks);
      ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork, parseCtx.getConf());
      cndTsk.setListTasks(listTasks);

      // set resolver and resolver context
      cndTsk.setResolver(new ConditionalResolverCommonJoin());
      ConditionalResolverCommonJoinCtx resolverCtx = new ConditionalResolverCommonJoinCtx();
      resolverCtx.setPathToAliases(pathToAliases);
      resolverCtx.setAliasToKnownSize(aliasToSize);
      resolverCtx.setAliasToTask(aliasToTask);
      resolverCtx.setCommonJoinTask(currTask);
      resolverCtx.setLocalTmpDir(context.getLocalScratchDir(false));
      resolverCtx.setHdfsTmpDir(context.getMRScratchDir());
      cndTsk.setResolverCtx(resolverCtx);

      //replace the current task with the new generated conditional task
      this.replaceTaskWithConditionalTask(currTask, cndTsk, physicalContext);
      return cndTsk;
    }

    private void replaceTaskWithConditionalTask(
        Task<? extends Serializable> currTask, ConditionalTask cndTsk,
        PhysicalContext physicalContext) {
      // add this task into task tree
      // set all parent tasks
      List<Task<? extends Serializable>> parentTasks = currTask.getParentTasks();
      currTask.setParentTasks(null);
      if (parentTasks != null) {
        for (Task<? extends Serializable> tsk : parentTasks) {
          // make new generated task depends on all the parent tasks of current task.
          tsk.addDependentTask(cndTsk);
          // remove the current task from its original parent task's dependent task
          tsk.removeDependentTask(currTask);
        }
      } else {
        // remove from current root task and add conditional task to root tasks
        physicalContext.removeFromRootTask(currTask);
        physicalContext.addToRootTask(cndTsk);
      }
      // set all child tasks
      List<Task<? extends Serializable>> oldChildTasks = currTask.getChildTasks();
      if (oldChildTasks != null) {
        for (Task<? extends Serializable> tsk : cndTsk.getListTasks()) {
          if (tsk.equals(currTask)) {
            continue;
          }
          for (Task<? extends Serializable> oldChild : oldChildTasks) {
            tsk.addDependentTask(oldChild);
          }
        }
      }
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      if (nodeOutputs == null || nodeOutputs.length == 0) {
        throw new SemanticException("No Dispatch Context");
      }

      TaskGraphWalkerContext walkerCtx = (TaskGraphWalkerContext) nodeOutputs[0];

      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      // not map reduce task or not conditional task, just skip
      if (currTask.isMapRedTask()) {
        if (currTask instanceof ConditionalTask) {
          // get the list of task
          List<Task<? extends Serializable>> taskList = ((ConditionalTask) currTask).getListTasks();
          for (Task<? extends Serializable> tsk : taskList) {
            if (tsk.isMapRedTask()) {
              ConditionalTask cndTask = this.processCurrentTask((MapRedTask) tsk,
                  ((ConditionalTask) currTask), physicalContext.getContext());
              walkerCtx.addToDispatchList(cndTask);
            }
          }
        } else {
          ConditionalTask cndTask = this.processCurrentTask((MapRedTask) currTask, null, physicalContext.getContext());
          walkerCtx.addToDispatchList(cndTask);
        }
      }
      return null;
    }

    private JoinOperator getJoinOp(MapRedTask task) throws SemanticException {
      if (task.getWork() == null) {
        return null;
      }
      Operator<? extends Serializable> reducerOp = task.getWork().getReducer();
      if (reducerOp instanceof JoinOperator) {
        return (JoinOperator) reducerOp;
      } else {
        return null;
      }
    }
  }
}
