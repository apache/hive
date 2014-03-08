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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;

/**
 * Conditional Task implementation.
 */
public class ConditionalTask extends Task<ConditionalWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<Task<? extends Serializable>> listTasks;

  private boolean resolved = false;

  private List<Task<? extends Serializable>> resTasks;

  private ConditionalResolver resolver;
  private Object resolverCtx;

  public ConditionalTask() {
    super();
  }


  @Override
  public boolean isMapRedTask() {
    for (Task<? extends Serializable> task : listTasks) {
      if (task.isMapRedTask()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean hasReduce() {
    for (Task<? extends Serializable> task : listTasks) {
      if (task.hasReduce()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
  }

  @Override
  public int execute(DriverContext driverContext) {
    resTasks = resolver.getTasks(conf, resolverCtx);
    resolved = true;

    try {
      resolveTask(driverContext);
    } catch (Exception e) {
      setException(e);
      return 1;
    }
    return 0;
  }

  private void resolveTask(DriverContext driverContext) throws HiveException {
    for (Task<? extends Serializable> tsk : getListTasks()) {
      if (!resTasks.contains(tsk)) {
        driverContext.remove(tsk);
        console.printInfo(tsk.getId() + " is filtered out by condition resolver.");
        if (tsk.isMapRedTask()) {
          driverContext.incCurJobNo(1);
        }
        //recursively remove this task from its children's parent task
        tsk.removeFromChildrenTasks();
      } else {
        if (getParentTasks() != null) {
          // This makes it so that we can go back up the tree later
          for (Task<? extends Serializable> task : getParentTasks()) {
            task.addDependentTask(tsk);
          }
        }
        // resolved task
        if (driverContext.addToRunnable(tsk)) {
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
    List<Task<? extends Serializable>> parentTasks = getParentTasks();
    if (parentTasks != null) {
      for (Task<? extends Serializable> par : parentTasks) {
        ret = ret && par.done();
      }
    }
    List<Task<? extends Serializable>> retTasks;
    if (resolved) {
      retTasks = resTasks;
    } else {
      retTasks = getListTasks();
    }
    if (ret && retTasks != null) {
      for (Task<? extends Serializable> tsk : retTasks) {
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
  public List<Task<? extends Serializable>> getListTasks() {
    return listTasks;
  }

  /**
   * @param listTasks
   *          the listTasks to set
   */
  public void setListTasks(List<Task<? extends Serializable>> listTasks) {
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
  public boolean addDependentTask(Task<? extends Serializable> dependent) {
    boolean ret = false;
    if (getListTasks() != null) {
      for (Task<? extends Serializable> tsk : getListTasks()) {
        ret = ret & tsk.addDependentTask(dependent);
      }
    }
    return ret;
  }

  @Override
  public List<Task<? extends Serializable>> getDependentTasks() {
    return listTasks;
  }
}
