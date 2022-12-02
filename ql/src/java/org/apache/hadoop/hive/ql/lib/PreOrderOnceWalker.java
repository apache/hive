/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.lib;

import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashSet;
import java.util.Set;

/**
 * Graph walker this class takes list of starting nodes and walks them in pre-order.
 * If a rule fires up against a given node, we do not try to apply the rule
 * on its children.
 */
public class PreOrderOnceWalker extends PreOrderWalker {
  private final Set<Class<? extends Node>> excludeNodes = new HashSet<>();

  public PreOrderOnceWalker(SemanticDispatcher disp) {
    super(disp);
  }

  /**
   * Walk the current operator and its descendants.
   * 
   * @param nd
   *          current operator in the graph
   * @throws SemanticException
   */
  @Override
  public void walk(Node nd) throws SemanticException {
    if (excludeNodes.contains(nd.getClass())) {
      return;
    }
    opStack.push(nd);
    dispatch(nd, opStack);

    // The rule has been applied, we bail out
    if (retMap.get(nd) != null) {
      opStack.pop();
      return;
    }

    // move all the children to the front of queue
    if (nd.getChildren() != null) {
      for (Node n : nd.getChildren()) {
        walk(n);
      }
    }

    opStack.pop();
  }

  /**
   * Excludes the nodes with the specified type from the graph traversal.
   * <p>
   * The traversal of a path will stop if it encounters a node with an excluded type.
   * </p>
   *
   * @param type the type of the nodes to exclude from the graph traversal.
   */
  public void excludeNode(Class<? extends Node> type) {
    excludeNodes.add(type);
  }
}
