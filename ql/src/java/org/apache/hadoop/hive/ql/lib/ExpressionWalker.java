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

import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * class for traversing tree
 * This class assumes that the given node represents TREE and not GRAPH
 * i.e. there is only single path to reach a node
 */
public class ExpressionWalker extends DefaultGraphWalker{

  /**
   * Constructor.
   *
   * @param disp
   * dispatcher to call for each op encountered
   */
  public ExpressionWalker(SemanticDispatcher disp) {
    super(disp);
  }


  private static class NodeLabeled {
    final private Node nd;
    private int currChildIdx;

    NodeLabeled(Node nd) {
      this.nd = nd;
      this.currChildIdx = -1;
    }

    public void incrementChildIdx() {
      this.currChildIdx++;
    }

    public int getCurrChildIdx() {
      return  this.currChildIdx;
    }

    public Node getNd() {
      return this.nd;
    }
  }

  protected boolean shouldByPass(Node childNode, Node parentNode) {
    return false;
  }

  /**
   * walk the current operator and its descendants.
   *
   * @param nd
   *          current operator in the tree
   * @throws SemanticException
   */
  protected void walk(Node nd) throws SemanticException {
    Deque<NodeLabeled> traversalStack = new ArrayDeque<>();
    traversalStack.push(new NodeLabeled(nd));

    opStack.push(nd);

    while(!traversalStack.isEmpty()) {
      NodeLabeled currLabeledNode = traversalStack.peek();
      Node currNode = currLabeledNode.getNd();
      int currIdx = currLabeledNode.getCurrChildIdx();

      if(currNode.getChildren() != null && currNode.getChildren().size() > currIdx + 1) {
        Node nextChild = currNode.getChildren().get(currIdx+1);
        //check if this node should be skipped and not dispatched
        if(shouldByPass(nextChild, currNode)) {
          retMap.put(nextChild, null);
          currLabeledNode.incrementChildIdx();
          continue;
        }
        traversalStack.push(new NodeLabeled(nextChild));
        opStack.push(nextChild);
        currLabeledNode.incrementChildIdx();
      } else {
        // dispatch the node
        dispatch(currNode, opStack);
        opQueue.add(currNode);
        opStack.pop();
        traversalStack.pop();
      }
    }
  }
}

