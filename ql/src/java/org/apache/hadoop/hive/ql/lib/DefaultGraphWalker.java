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

package org.apache.hadoop.hive.ql.lib;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * base class for operator graph walker this class takes list of starting ops
 * and walks them one by one. it maintains list of walked operators
 * (dispatchedList) and a list of operators that are discovered but not yet
 * dispatched
 */
public class DefaultGraphWalker implements GraphWalker {

  protected Stack<Node> opStack;
  private final List<Node> toWalk = new ArrayList<Node>();
  private final HashMap<Node, Object> retMap = new HashMap<Node, Object>();
  private final Dispatcher dispatcher;

  /**
   * Constructor.
   * 
   * @param disp
   *          dispatcher to call for each op encountered
   */
  public DefaultGraphWalker(Dispatcher disp) {
    dispatcher = disp;
    opStack = new Stack<Node>();
  }

  /**
   * @return the toWalk
   */
  public List<Node> getToWalk() {
    return toWalk;
  }

  /**
   * @return the doneList
   */
  public Set<Node> getDispatchedList() {
    return retMap.keySet();
  }

  /**
   * Dispatch the current operator.
   * 
   * @param nd
   *          node being walked
   * @param ndStack
   *          stack of nodes encountered
   * @throws SemanticException
   */
  public void dispatch(Node nd, Stack<Node> ndStack) throws SemanticException {
    Object[] nodeOutputs = null;
    if (nd.getChildren() != null) {
      nodeOutputs = new Object[nd.getChildren().size()];
      int i = 0;
      for (Node child : nd.getChildren()) {
        nodeOutputs[i++] = retMap.get(child);
      }
    }

    Object retVal = dispatcher.dispatch(nd, ndStack, nodeOutputs);
    retMap.put(nd, retVal);
  }

  /**
   * starting point for walking.
   * 
   * @throws SemanticException
   */
  public void startWalking(Collection<Node> startNodes,
      HashMap<Node, Object> nodeOutput) throws SemanticException {
    toWalk.addAll(startNodes);
    while (toWalk.size() > 0) {
      Node nd = toWalk.remove(0);
      walk(nd);
      if (nodeOutput != null) {
        nodeOutput.put(nd, retMap.get(nd));
      }
    }
  }

  /**
   * walk the current operator and its descendants.
   * 
   * @param nd
   *          current operator in the graph
   * @throws SemanticException
   */
  public void walk(Node nd) throws SemanticException {
    if (opStack.empty() || nd != opStack.peek()) {
      opStack.push(nd);
    }

    if ((nd.getChildren() == null)
        || getDispatchedList().containsAll(nd.getChildren())) {
      // all children are done or no need to walk the children
      if (getDispatchedList().contains(nd)) {
        // sanity check
        assert false;
      }
      dispatch(nd, opStack);
      opStack.pop();
      return;
    }
    // add children, self to the front of the queue in that order
    getToWalk().add(0, nd);
    getToWalk().removeAll(nd.getChildren());
    getToWalk().addAll(0, nd.getChildren());
  }
}
