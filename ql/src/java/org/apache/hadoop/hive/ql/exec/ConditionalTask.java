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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Conditional Task implementation.
 */
public class ConditionalTask extends Task<ConditionalWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<Task<?>> listTasks;

  private boolean resolved = false;

  private List<Task<?>> resTasks;

  private ConditionalResolver resolver;
  private Object resolverCtx;

  public ConditionalTask() {
    super();
  }


  @Override
  public boolean isMapRedTask() {
    for (Task<?> task : listTasks) {
      if (task.isMapRedTask()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean canExecuteInParallel() {
    return isMapRedTask();
  }

  @Override
  public boolean hasReduce() {
    for (Task<?> task : listTasks) {
      if (task.hasReduce()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public int execute() {
    resTasks = resolver.getTasks(conf, resolverCtx);
    resolved = true;

    ReentrantLock resolveTaskLock = queryState.getResolveConditionalTaskLock();
    try {
      resolveTaskLock.lock();
      resolveTask();
    } catch (Exception e) {
      setException(e);
      return 1;
    } finally {
      resolveTaskLock.unlock();
    }
    return 0;
  }

  private void resolveTask() throws HiveException {
    for (Task<?> tsk : getListTasks()) {
      if (!resTasks.contains(tsk)) {
        taskQueue.remove(tsk);
        console.printInfo(tsk.getId() + " is filtered out by condition resolver.");
        if (tsk.isMapRedTask()) {
          taskQueue.incCurJobNo(1);
        }
        //recursively remove this task from its children's parent task
        tsk.removeFromChildrenTasks();
      } else {
        if (getParentTasks() != null) {
          // This makes it so that we can go back up the tree later
          for (Task<?> task : getParentTasks()) {
            task.addDependentTask(tsk);
          }
        }
        // resolved task
        if (taskQueue.addToRunnable(tsk)) {
          console.printInfo(tsk.getId() + " is selected by condition resolver.");
        }
      }
    }
  }


  /**
   * @return the resolver
   */
  public ConditionalResolver getResolver() {
    return resolver;
  }

  /**
   * @param resolver
   *          the resolver to set
   */
  public void setResolver(ConditionalResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * @return the resolverCtx
   */
  public Object getResolverCtx() {
    return resolverCtx;
  }

  // used to determine whether child tasks can be run.
  @Override
  public boolean done() {
    boolean ret = true;
    List<Task<?>> parentTasks = getParentTasks();
    if (parentTasks != null) {
      for (Task<?> par : parentTasks) {
        ret = ret && par.done();
      }
    }
    List<Task<?>> retTasks;
    if (resolved) {
      retTasks = resTasks;
    } else {
      retTasks = getListTasks();
    }
    if (ret && retTasks != null) {
      for (Task<?> tsk : retTasks) {
        ret = ret && tsk.done();
      }
    }
    return ret;
  }

  /**
   * @param resolverCtx
   *          the resolverCtx to set
   */
  public void setResolverCtx(Object resolverCtx) {
    this.resolverCtx = resolverCtx;
  }

  /**
   * @return the listTasks
   */
  public List<Task<?>> getListTasks() {
    return listTasks;
  }

  /**
   * @param listTasks
   *          the listTasks to set
   */
  public void setListTasks(List<Task<?>> listTasks) {
    this.listTasks = listTasks;
  }

  @Override
  public StageType getType() {
    return StageType.CONDITIONAL;
  }

  @Override
  public String getName() {
    return "CONDITION";
  }

  /**
   * Add a dependent task on the current conditional task. The task will not be a direct child of
   * conditional task. Actually it will be added as child task of associated tasks.
   *
   * @return true if the task got added false if it already existed
   */
  @Override
  public boolean addDependentTask(Task<?> dependent) {
    boolean ret = false;
    if (getListTasks() != null) {
      ret = true;
      for (Task<?> tsk : getListTasks()) {
        ret = ret & tsk.addDependentTask(dependent);
      }
    }
    return ret;
  }

  @Override
  public List<Task<?>> getDependentTasks() {
    return listTasks;
  }
}
