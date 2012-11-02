/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express  or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 **/
package org.apache.hadoop.hive.ql.hooks;

import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.io.Serializable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class OptrStatGroupByHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();

    List<TaskRunner> completedTasks = hookContext.getCompleteTaskList();

    boolean enableProgress = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEJOBPROGRESS);

    /** For each task visit the opeartor tree and and if the operator is GROUPBY
     *  then print the HASH_OUT Optr level stat value.
     **/
    if (completedTasks != null) {
      for (TaskRunner taskRunner : completedTasks) {
        Task<? extends Serializable> task = taskRunner.getTask();
        if (task.isMapRedTask() && !task.isMapRedLocalTask()) {
          Set<Operator<? extends OperatorDesc>> optrSet = getOptrsForTask(task);
          for (Operator<? extends OperatorDesc> optr : optrSet) {
            if (optr.getType() == OperatorType.GROUPBY) {
               printCounterValue(optr.getCounters());
            }
          }
        }
      }
    }
  }

  private void printCounterValue(HashMap<String, Long> ctrs) {
    for (String ctrName : ctrs.keySet()) {
      if (ctrName.contains("HASH_OUT")) {
        SessionState.getConsole().printError(ctrName+"="+ctrs.get(ctrName));
      }
    }
  }

  private Set<Operator<? extends OperatorDesc>> getOptrsForTask(
    Task<? extends Serializable> task) {

    Collection<Operator<? extends OperatorDesc>> topOptrs = task.getTopOperators();
    Set<Operator<? extends OperatorDesc>> allOptrs =
      new HashSet<Operator<? extends OperatorDesc>>();
    Queue<Operator<? extends OperatorDesc>> opsToVisit =
      new LinkedList<Operator<? extends OperatorDesc>>();
    if(topOptrs != null) {
      opsToVisit.addAll(topOptrs);
      addChildOptrs(opsToVisit, allOptrs);
    }

    return allOptrs;
  }

  private void addChildOptrs(
    Queue<Operator<? extends OperatorDesc>> opsToVisit,
    Set<Operator<? extends OperatorDesc>> opsVisited) {

    if(opsToVisit == null || opsVisited == null) {
      return;
    }

    while (opsToVisit.peek() != null) {
      Operator<? extends OperatorDesc> op = opsToVisit.remove();
      opsVisited.add(op);
      if (op.getChildOperators() != null) {
        for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
          if (!opsVisited.contains(childOp)) {
            opsToVisit.add(childOp);
          }
        }
      }
    }
  }
}
