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

import java.util.List;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;

/**
 * Walks the operator tree in pre order fashion.
 */
public class GenMapRedWalker extends DefaultGraphWalker {

  /**
   * constructor of the walker - the dispatcher is passed.
   * 
   * @param disp
   *          the dispatcher to be called for each node visited
   */
  public GenMapRedWalker(SemanticDispatcher disp) {
    super(disp);
  }

  /**
   * Walk the given operator.
   * 
   * @param nd
   *          operator being walked
   */
  @Override
  protected void walk(Node nd) throws SemanticException {
    List<? extends Node> children = nd.getChildren();

    // maintain the stack of operators encountered
    opStack.push(nd);
    Boolean result = dispatchAndReturn(nd, opStack);

    // kids of reduce sink operator or mapjoin operators merged into root task
    // need not be traversed again
    if (children == null || result == Boolean.FALSE) {
      opStack.pop();
      return;
    }

    // move all the children to the front of queue
    for (Node ch : children) {
      walk(ch);
    }

    // done with this operator
    opStack.pop();
  }
}
