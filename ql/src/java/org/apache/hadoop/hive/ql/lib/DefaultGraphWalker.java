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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * base class for operator graph walker
 * this class takes list of starting ops and walks them one by one. it maintains list of walked
 * operators (dispatchedList) and a list of operators that are discovered but not yet dispatched
 */
public class DefaultGraphWalker implements GraphWalker {

  protected Stack<Node> opStack;
  private List<Node> toWalk = new ArrayList<Node>();
  private Set<Node> dispatchedList = new HashSet<Node>();
  private Dispatcher dispatcher;

  /**
   * Constructor
   * @param ctx graph of operators to walk
   * @param disp dispatcher to call for each op encountered
   */
  public DefaultGraphWalker(Dispatcher disp) {
    this.dispatcher = disp;
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
    return dispatchedList;
  }

  /**
   * Dispatch the current operator
   * @param op operator being walked
   * @param opStack stack of operators encountered
   * @throws SemanticException
   */
  public void dispatch(Node nd, Stack<Node> ndStack) throws SemanticException {
    this.dispatcher.dispatch(nd, ndStack);
    this.dispatchedList.add(nd);
  }

  /**
   * starting point for walking
   * @throws SemanticException
   */
  public void startWalking(Collection<Node> startNodes) throws SemanticException {
    toWalk.addAll(startNodes);
    while(toWalk.size() > 0)
      walk(toWalk.remove(0));
  }

  /**
   * walk the current operator and its descendants
   * @param nd current operator in the graph
   * @throws SemanticException
   */
  public void walk(Node nd) throws SemanticException {
    opStack.push(nd);

    if((nd.getChildren() == null) 
        || getDispatchedList().containsAll(nd.getChildren())) {
      // all children are done or no need to walk the children
      dispatch(nd, opStack);
      opStack.pop();
      return;
    }
    // move all the children to the front of queue
    getToWalk().removeAll(nd.getChildren());
    getToWalk().addAll(0, nd.getChildren());
    // add self to the end of the queue
    getToWalk().add(nd);
    opStack.pop();
  }
}
