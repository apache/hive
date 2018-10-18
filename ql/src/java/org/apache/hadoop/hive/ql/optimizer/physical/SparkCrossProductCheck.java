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

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

/**
 * Check each MapJoin and ShuffleJoin Operator to see if they are performing a cross product.
 * If yes, output a warning to the Session's console.
 * The Checks made are the following:
 * 1. Shuffle Join:
 * Check the parent ReduceSinkOp of the JoinOp. If its keys list is size = 0, then
 * this is a cross product.
 * 2. Map Join:
 * If the keys expr list on the mapJoin Desc is an empty list for any input,
 * this implies a cross product.
 */
public class SparkCrossProductCheck implements PhysicalPlanResolver, Dispatcher {

  @Override
  public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
    @SuppressWarnings("unchecked")
    Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
    if (currTask instanceof SparkTask) {
      SparkWork sparkWork = ((SparkTask) currTask).getWork();
      checkShuffleJoin(sparkWork);
      checkMapJoin((SparkTask) currTask);
    } else if (currTask instanceof ConditionalTask) {
      List<Task<? extends Serializable>> taskList = ((ConditionalTask) currTask).getListTasks();
      for (Task<? extends Serializable> task : taskList) {
        dispatch(task, stack, nodeOutputs);
      }
    }
    return null;
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    TaskGraphWalker ogw = new TaskGraphWalker(this);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private void warn(String msg) {
    SessionState.getConsole().printInfo("Warning: " + msg, false);
  }

  private void checkShuffleJoin(SparkWork sparkWork) throws SemanticException {
    for (ReduceWork reduceWork : sparkWork.getAllReduceWork()) {
      Operator<? extends OperatorDesc> reducer = reduceWork.getReducer();
      if (reducer instanceof JoinOperator || reducer instanceof CommonMergeJoinOperator) {
        Map<Integer, CrossProductHandler.ExtractReduceSinkInfo.Info> rsInfo = new TreeMap<Integer, CrossProductHandler.ExtractReduceSinkInfo.Info>();
        for (BaseWork parent : sparkWork.getParents(reduceWork)) {
          rsInfo.putAll(new CrossProductHandler.ExtractReduceSinkInfo(null).analyze(parent));
        }
        checkForCrossProduct(reduceWork.getName(), reducer, rsInfo);
      }
    }
  }

  private void checkMapJoin(SparkTask sparkTask) throws SemanticException {
    SparkWork sparkWork = sparkTask.getWork();
    for (BaseWork baseWork : sparkWork.getAllWork()) {
      List<String> warnings =
          new CrossProductHandler.MapJoinCheck(sparkTask.toString()).analyze(baseWork);
      for (String w : warnings) {
        warn(w);
      }
    }
  }

  private void checkForCrossProduct(String workName,
      Operator<? extends OperatorDesc> reducer,
      Map<Integer, CrossProductHandler.ExtractReduceSinkInfo.Info> rsInfo) {
    if (rsInfo.isEmpty()) {
      return;
    }
    Iterator<CrossProductHandler.ExtractReduceSinkInfo.Info> it = rsInfo.values().iterator();
    CrossProductHandler.ExtractReduceSinkInfo.Info info = it.next();
    if (info.keyCols.size() == 0) {
      List<String> iAliases = new ArrayList<String>();
      iAliases.addAll(info.inputAliases);
      while (it.hasNext()) {
        info = it.next();
        iAliases.addAll(info.inputAliases);
      }
      String warning = String.format(
          "Shuffle Join %s[tables = %s] in Work '%s' is a cross product",
          reducer.toString(),
          iAliases,
          workName);
      warn(warning);
    }
  }
}
