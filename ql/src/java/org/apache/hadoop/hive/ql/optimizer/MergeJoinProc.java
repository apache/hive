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

import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;

public class MergeJoinProc implements NodeProcessor {
  @Override
  public Object
      process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
    GenTezProcContext context = (GenTezProcContext) procCtx;
    CommonMergeJoinOperator mergeJoinOp = (CommonMergeJoinOperator) nd;
    if (stack.size() < 2 || !(stack.get(stack.size() - 2) instanceof DummyStoreOperator)) {
      context.currentMergeJoinOperator = mergeJoinOp;
      return null;
    }

    TezWork tezWork = context.currentTask.getWork();
    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parentOp =
        (Operator<? extends OperatorDesc>) ((stack.get(stack.size() - 2)));
    // Guaranteed to be just 1 because each DummyStoreOperator can be part of only one work.
    BaseWork parentWork = context.childToWorkMap.get(parentOp).get(0);


    // we need to set the merge work that has been created as part of the dummy store walk. If a
    // merge work already exists for this merge join operator, add the dummy store work to the
    // merge work. Else create a merge work, add above work to the merge work
    MergeJoinWork mergeWork = null;
    if (context.opMergeJoinWorkMap.containsKey(mergeJoinOp)) {
      // we already have the merge work corresponding to this merge join operator
      mergeWork = context.opMergeJoinWorkMap.get(mergeJoinOp);
    } else {
      mergeWork = new MergeJoinWork();
      tezWork.add(mergeWork);
      context.opMergeJoinWorkMap.put(mergeJoinOp, mergeWork);
    }

    mergeWork.addMergedWork(null, parentWork, context.leafOperatorToFollowingWork);
    mergeWork.setMergeJoinOperator(mergeJoinOp);
    tezWork.setVertexType(mergeWork, VertexType.MULTI_INPUT_UNINITIALIZED_EDGES);

    for (BaseWork grandParentWork : tezWork.getParents(parentWork)) {
      TezEdgeProperty edgeProp = tezWork.getEdgeProperty(grandParentWork, parentWork);
      tezWork.disconnect(grandParentWork, parentWork);
      tezWork.connect(grandParentWork, mergeWork, edgeProp);
    }

    for (BaseWork childWork : tezWork.getChildren(parentWork)) {
      TezEdgeProperty edgeProp = tezWork.getEdgeProperty(parentWork, childWork);
      tezWork.disconnect(parentWork, childWork);
      tezWork.connect(mergeWork, childWork, edgeProp);
    }

    tezWork.remove(parentWork);

    DummyStoreOperator dummyOp = (DummyStoreOperator) (stack.get(stack.size() - 2));

    parentWork.setTag(mergeJoinOp.getTagForOperator(dummyOp));

    mergeJoinOp.getParentOperators().remove(dummyOp);
    dummyOp.getChildOperators().clear();

    return true;
  }
}
