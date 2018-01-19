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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Simple renumbering of stage ids
 */
public class StageIDsRearranger implements PhysicalPlanResolver {

  private static final String PREFIX = "Stage-";

  enum ArrangeType {
    NONE, IDONLY, TRAVERSE, EXECUTION
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    int counter = 0;
    for (Task task : getExplainOrder(pctx)) {
      task.setId(PREFIX + (++counter));
    }
    return null;
  }

  private static List<Task> getExplainOrder(PhysicalContext pctx) {
    List<Task> tasks = getExplainOrder(pctx.getConf(), pctx.getRootTasks());
    if (pctx.getFetchTask() != null) {
      tasks.add(pctx.getFetchTask());
    }
    return tasks;
  }

  public static List<Task> getFetchSources(List<Task<?>> tasks) {
    final List<Task> sources = new ArrayList<Task>();
    TaskTraverse traverse = new TaskTraverse() {
      @Override
      protected void accepted(Task<?> task) {
        if (task.getNumChild() == 0 && task.isFetchSource()) {
          sources.add(task);
        }
      }
    };
    for (Task<? extends Serializable> task : tasks) {
      traverse.traverse(task);
    }
    return sources;
  }

  public static List<Task> getExplainOrder(HiveConf conf, List<Task<?>> tasks) {
    for (Task<? extends Serializable> task : tasks) {
      task.setRootTask(true);
    }
    String var = conf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE);
    ArrangeType type = ArrangeType.valueOf(var.toUpperCase());
    if (type == ArrangeType.EXECUTION) {
      return executionOrder(tasks);
    }
    return traverseOrder(type, tasks);
  }

  private static List<Task> executionOrder(List<Task<?>> tasks) {
    final Queue<Task<?>> queue = new ConcurrentLinkedQueue<Task<?>>(tasks);

    TaskTraverse traverse = new TaskTraverse() {
      @Override
      protected void accepted(Task<?> task) {
        List<Task<?>> childTasks = getChildTasks(task);
        if (childTasks != null && !childTasks.isEmpty()) {
          queue.addAll(childTasks);
        }
      }
      @Override
      protected void rejected(Task<?> child) {
        queue.add(child);
      }
      @Override
      protected List<Task<?>> next(Task<?> task) {
        return queue.isEmpty() ? null : Arrays.<Task<?>>asList(queue.remove());
      }
    };
    if (!queue.isEmpty()) {
      traverse.traverse(queue.remove());
    }
    return new ArrayList<Task>(traverse.traversed);
  }

  static List<Task> traverseOrder(final ArrangeType type, List<Task<?>> tasks) {

    TaskTraverse traverse = new TaskTraverse() {
      @Override
      protected boolean isReady(Task<?> task) {
        return type == ArrangeType.NONE || type == ArrangeType.IDONLY || super.isReady(task);
      }
    };
    for (Task<? extends Serializable> task : tasks) {
      traverse.traverse(task);
    }
    return new ArrayList<Task>(traverse.traversed);
  }


  public static abstract class TaskTraverse {

    protected final Set<Task<?>> traversed = new LinkedHashSet<Task<?>>();

    public void traverse(Task<?> task) {
      if (traversed.add(task)) {
        accepted(task);
      }
      List<Task<?>> children = next(task);
      if (children != null && !children.isEmpty()) {
        for (Task<?> child : children) {
          if (isReady(child)) {
            traverse(child);
          } else {
            rejected(child);
          }
        }
      }
    }

    protected boolean isReady(Task<?> task) {
      return task.getParentTasks() == null || traversed.containsAll(task.getParentTasks());
    }

    protected void accepted(Task<?> task) {
    }

    protected void rejected(Task<?> child) {
    }

    protected List<Task<?>> next(Task<?> task) {
      return getChildTasks(task);
    }

    protected List<Task<?>> getChildTasks(Task<?> task) {
      if (task instanceof ConditionalTask) {
        return ((ConditionalTask) task).getListTasks();
      }
      return task.getChildTasks();
    }
  }
}
