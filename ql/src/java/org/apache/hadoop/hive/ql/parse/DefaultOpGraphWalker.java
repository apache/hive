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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;

/**
 * base class for operator graph walker
 * this class takes list of starting ops and walks them one by one. it maintains list of walked
 * operators (dispatchedList) and a list of operators that are discovered but not yet dispatched
 */
public abstract class DefaultOpGraphWalker implements OpGraphWalker {

  List<Operator<? extends Serializable>> toWalk = new ArrayList<Operator<? extends Serializable>>();
  Set<Operator<? extends Serializable>> dispatchedList = new HashSet<Operator<? extends Serializable>>();
  Dispatcher dispatcher;

  /**
   * Constructor
   * @param ctx graph of operators to walk
   * @param disp dispatcher to call for each op encountered
   */
  public DefaultOpGraphWalker(Dispatcher disp) {
    this.dispatcher = disp;
  }

  /**
   * @return the toWalk
   */
  public List<Operator<? extends Serializable>> getToWalk() {
    return toWalk;
  }

  /**
   * @return the doneList
   */
  public Set<Operator<? extends Serializable>> getDispatchedList() {
    return dispatchedList;
  }

  /**
   * Dispatch the current operator
   * @param op operator being walked
   * @param opStack stack of operators encountered
   * @throws SemanticException
   */
  public void dispatch(Operator<? extends Serializable> op, Stack opStack) throws SemanticException {
    this.dispatcher.dispatch(op, opStack);
    this.dispatchedList.add(op);
  }

  /**
   * starting point for walking
   * @throws SemanticException
   */
  public void startWalking(Collection<Operator<? extends Serializable>> startOps) throws SemanticException {
    toWalk.addAll(startOps);
    while(toWalk.size() > 0)
      walk(toWalk.remove(0));
  }

  /**
   * walk the current operator and its descendants
   * @param op current operator in the graph
   * @throws SemanticException
   */
  public abstract void walk(Operator<? extends Serializable> op) throws SemanticException;
}
