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

package org.apache.hadoop.hive.ql.lib;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ForwardWalker extends DefaultGraphWalker {

  /**
   * Constructor.
   *
   * @param disp
   * dispatcher to call for each op encountered
   */
  public ForwardWalker(Dispatcher disp) {
    super(disp);
  }

  @SuppressWarnings("unchecked")
  protected boolean allParentsDispatched(Node nd) {
    Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
    if (op.getParentOperators() == null) {
      return true;
    }
    for (Node pNode : op.getParentOperators()) {
      if (!getDispatchedList().contains(pNode)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  protected void addAllParents(Node nd) {
    Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
    toWalk.removeAll(op.getParentOperators());
    toWalk.addAll(0, op.getParentOperators());
  }

  /**
   * walk the current operator and its descendants.
   *
   * @param nd
   * current operator in the graph
   * @throws SemanticException
   */
  @Override
  protected void walk(Node nd) throws SemanticException {
    if (opStack.empty() || nd != opStack.peek()) {
      opStack.push(nd);
    }
    if (allParentsDispatched(nd)) {
      // all children are done or no need to walk the children
      if (!getDispatchedList().contains(nd)) {
        toWalk.addAll(nd.getChildren());
        dispatch(nd, opStack);
      }
      opStack.pop();
      return;
    }
    // add children, self to the front of the queue in that order
    toWalk.add(0, nd);
    addAllParents(nd);
  }
}