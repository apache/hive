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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * This is a level-wise walker implementation which dispatches the node in the order
 * that the node will only get dispatched after all the parents are dispatched.
 *
 * Each node will be accessed once while it could be dispatched multiple times.
 * e.g., for a lineage generator with operator tree, 2 levels of current node's
 * ancestors need to keep in the operator stack.
 *                  FIL(2) FIL(4)
 *                      |    |
 *                    RS(3) RS(5)
 *                       \  /
 *                      JOIN(7)
 * The join lineage needs to be called twice for JOIN(7) node with different operator
 * ancestors.
 */
public class LevelOrderWalker extends DefaultGraphWalker {
  // Only specified nodes of these types will be walked.
  // Empty set means all the nodes will be walked.
  private HashSet<Class<? extends Node>> nodeTypes = new HashSet<Class<? extends Node>>();

  // How many levels of ancestors to keep in the stack during dispatching
  private final int numLevels;

  /**
   * Constructor with keeping all the ancestors in the operator stack during dispatching.
   * @param disp Dispatcher to call for each op encountered
   */
  public LevelOrderWalker(Dispatcher disp) {
    super(disp);
    this.numLevels = Integer.MAX_VALUE;
  }

  /**
   * Constructor with specified number of ancestor levels to keep in the operator
   * stack during dispatching.
   * @param disp      Dispatcher to call for each op encountered
   * @param numLevels Number of ancestor levels
   */
  public LevelOrderWalker(Dispatcher disp, int numLevels) {
    super(disp);
    this.numLevels = numLevels;
  }

  @SuppressWarnings("unchecked")
  public void setNodeTypes(Class<? extends Node> ...nodeTypes) {
    this.nodeTypes.addAll(Arrays.asList(nodeTypes));
  }

  /**
   * starting point for walking.
   *
   * @throws SemanticException
   */
  @SuppressWarnings("unchecked")
  @Override
  public void startWalking(Collection<Node> startNodes,
      HashMap<Node, Object> nodeOutput) throws SemanticException {
    toWalk.addAll(startNodes);

    // Starting from the startNodes, add the children whose parents have been
    // included in the list.
    HashSet<Node> addedNodes = new HashSet<Node>();
    for (Node node : startNodes) {
      addedNodes.add(node);
    }
    int index = 0;
    while(index < toWalk.size()) {
      if (toWalk.get(index).getChildren() != null) {
        for(Node child : toWalk.get(index).getChildren()) {
          Operator<? extends OperatorDesc> childOP =
              (Operator<? extends OperatorDesc>) child;

          if (!addedNodes.contains(child) &&
              (childOP.getParentOperators() == null ||
              addedNodes.containsAll(childOP.getParentOperators()))) {
            toWalk.add(child);
            addedNodes.add(child);
          }
        }
      }
      ++index;
    }

    for(Node nd : toWalk) {
      if (!nodeTypes.isEmpty() && !nodeTypes.contains(nd.getClass())) {
        continue;
      }

      opStack.clear();
      opStack.push(nd);
      walk(nd, 0, opStack);
      if (nodeOutput != null && getDispatchedList().contains(nd)) {
        nodeOutput.put(nd, retMap.get(nd));
      }
    }
  }

  /**
   * Enumerate numLevels of ancestors by putting them in the stack and dispatch
   * the current node.
   * @param nd current operator in the ancestor tree
   * @param level how many level of ancestors included in the stack
   * @param stack operator stack
   * @throws SemanticException
   */
  @SuppressWarnings("unchecked")
  private void walk(Node nd, int level, Stack<Node> stack) throws SemanticException {
    List<Operator<? extends OperatorDesc>> parents =
        ((Operator<? extends OperatorDesc>)nd).getParentOperators();

    if (level >= numLevels || parents == null || parents.isEmpty()) {
      dispatch(stack.peek(), stack);
      return;
    }

    for(Node parent : parents) {
      stack.add(0, parent);
      walk(parent, level+1, stack);
      stack.remove(0);
    }
  }
}
