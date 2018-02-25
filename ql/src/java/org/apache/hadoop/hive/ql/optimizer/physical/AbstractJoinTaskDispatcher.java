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
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker.TaskGraphWalkerContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapWork;

/**
 * Common iteration methods for converting joins and sort-merge joins.
 */
public abstract class AbstractJoinTaskDispatcher implements Dispatcher {

  protected final PhysicalContext physicalContext;

  public AbstractJoinTaskDispatcher(PhysicalContext context) {
    physicalContext = context;
  }

  public abstract Task<? extends Serializable> processCurrentTask(MapRedTask currTask,
      ConditionalTask conditionalTask, Context context)
      throws SemanticException;

  protected void replaceTaskWithConditionalTask(
      Task<? extends Serializable> currTask, ConditionalTask cndTsk) {
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

  // Replace the task with the new task. Copy the children and parents of the old
  // task to the new task.
  protected void replaceTask(
      Task<? extends Serializable> currTask, Task<? extends Serializable> newTask) {
    // add this task into task tree
    // set all parent tasks
    List<Task<? extends Serializable>> parentTasks = currTask.getParentTasks();
    currTask.setParentTasks(null);
    if (parentTasks != null) {
      for (Task<? extends Serializable> tsk : parentTasks) {
        // remove the current task from its original parent task's dependent task
        tsk.removeDependentTask(currTask);
        // make new generated task depends on all the parent tasks of current task.
        tsk.addDependentTask(newTask);
      }
    } else {
      // remove from current root task and add conditional task to root tasks
      physicalContext.removeFromRootTask(currTask);
      physicalContext.addToRootTask(newTask);
    }

    // set all child tasks
    List<Task<? extends Serializable>> oldChildTasks = currTask.getChildTasks();
    currTask.setChildTasks(null);
    if (oldChildTasks != null) {
      for (Task<? extends Serializable> tsk : oldChildTasks) {
        // remove the current task from its original parent task's dependent task
        tsk.getParentTasks().remove(currTask);
        // make new generated task depends on all the parent tasks of current task.
        newTask.addDependentTask(tsk);
      }
    }
  }

  public long getTotalKnownInputSize(Context context, MapWork currWork,
      Map<Path, ArrayList<String>> pathToAliases,
      HashMap<String, Long> aliasToSize) throws SemanticException {
    try {
      // go over all the input paths, and calculate a known total size, known
      // size for each input alias.
      Utilities.getInputSummary(context, currWork, null).getLength();

      // set alias to size mapping, this can be used to determine if one table
      // is chosen as big table, what's the total size of left tables, which
      // are going to be small tables.
      long aliasTotalKnownInputSize = 0L;
      for (Map.Entry<Path, ArrayList<String>> entry : pathToAliases.entrySet()) {
        Path path = entry.getKey();
        List<String> aliasList = entry.getValue();
        ContentSummary cs = context.getCS(path);
        if (cs != null) {
          long size = cs.getLength();
          for (String alias : aliasList) {
            aliasTotalKnownInputSize += size;
            Long es = aliasToSize.get(alias);
            if (es == null) {
              es = new Long(0);
            }
            es += size;
            aliasToSize.put(alias, es);
          }
        }
      }
      return aliasTotalKnownInputSize;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SemanticException("Generate Map Join Task Error: " + e.getMessage());
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
            Task<? extends Serializable> newTask = this.processCurrentTask((MapRedTask) tsk,
                ((ConditionalTask) currTask), physicalContext.getContext());
            walkerCtx.addToDispatchList(newTask);
          }
        }
      } else {
        Task<? extends Serializable> newTask =
            this.processCurrentTask((MapRedTask) currTask, null, physicalContext.getContext());
        walkerCtx.addToDispatchList(newTask);
      }
    }
    return null;
  }
}
