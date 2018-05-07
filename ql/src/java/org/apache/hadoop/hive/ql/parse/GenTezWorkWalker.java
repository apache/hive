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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Walks the operator tree in DFS fashion.
 */
public class GenTezWorkWalker extends DefaultGraphWalker {

  private final GenTezProcContext ctx;

  /**
   * constructor of the walker - the dispatcher is passed.
   *
   * @param disp the dispatcher to be called for each node visited
   * @param ctx the context where we'll set the current root operator
   *
   */
  public GenTezWorkWalker(Dispatcher disp, GenTezProcContext ctx) {
    super(disp);
    this.ctx = ctx;
  }

  private void setRoot(Node nd) {
    ctx.currentRootOperator = (Operator<? extends OperatorDesc>) nd;
    ctx.preceedingWork = null;
    ctx.parentOfRoot = null;
    ctx.currentUnionOperators = new ArrayList<>();
  }

  /**
   * starting point for walking.
   *
   * @throws SemanticException
   */
  @Override
  public void startWalking(Collection<Node> startNodes,
      HashMap<Node, Object> nodeOutput) throws SemanticException {
    toWalk.addAll(startNodes);
    while (toWalk.size() > 0) {
      Node nd = toWalk.remove(0);
      setRoot(nd);
      walk(nd);
      if (nodeOutput != null) {
        nodeOutput.put(nd, retMap.get(nd));
      }
    }
  }

  /**
   * Walk the given operator.
   *
   * @param nd operator being walked
   */
  @Override
  protected void walk(Node nd) throws SemanticException {
    List<? extends Node> children = nd.getChildren();

    // maintain the stack of operators encountered
    opStack.push(nd);
    Boolean skip = dispatchAndReturn(nd, opStack);

    // save some positional state
    Operator<? extends OperatorDesc> currentRoot = ctx.currentRootOperator;
    Operator<? extends OperatorDesc> parentOfRoot = ctx.parentOfRoot;
    List<UnionOperator> currentUnionOperators = ctx.currentUnionOperators;
    BaseWork preceedingWork = ctx.preceedingWork;

    if (skip == null || !skip) {
      // move all the children to the front of queue
      for (Node ch : children) {

        // and restore the state before walking each child
        ctx.currentRootOperator = currentRoot;
        ctx.parentOfRoot = parentOfRoot;
        ctx.preceedingWork = preceedingWork;
        ctx.currentUnionOperators = new ArrayList<>();
        ctx.currentUnionOperators.addAll(currentUnionOperators);

        walk(ch);
      }
    }

    // done with this operator
    opStack.pop();
  }
}
