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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * BaseWork. Base class for any "work" that's being done on the cluster. Items like stats
 * gathering that are commonly used regardless of the type of work live here.
 */
@SuppressWarnings({"serial"})
public abstract class BaseWork extends AbstractOperatorDesc {
  static final private Logger LOG = LoggerFactory.getLogger(BaseWork.class);

  // dummyOps is a reference to all the HashTableDummy operators in the
  // plan. These have to be separately initialized when we setup a task.
  // Their function is mainly as root ops to give the mapjoin the correct
  // schema info.
  List<HashTableDummyOperator> dummyOps;
  int tag = 0;
  private final List<String> sortColNames = new ArrayList<String>();

  private MapredLocalWork mrLocalWork;

  public BaseWork() {}

  public BaseWork(String name) {
    setName(name);
  }

  private boolean gatheringStats;

  private String name;

  // Vectorization.

  protected VectorizedRowBatchCtx vectorizedRowBatchCtx;

  protected boolean llapMode = false;
  protected boolean uberMode = false;

  public void setGatheringStats(boolean gatherStats) {
    this.gatheringStats = gatherStats;
  }

  public boolean isGatheringStats() {
    return this.gatheringStats;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<HashTableDummyOperator> getDummyOps() {
    return dummyOps;
  }

  public void setDummyOps(List<HashTableDummyOperator> dummyOps) {
    if (this.dummyOps != null && !this.dummyOps.isEmpty()
        && (dummyOps == null || dummyOps.isEmpty())) {
      LOG.info("Removing dummy operators from " + name + " " + this.getClass().getSimpleName());
    }
    this.dummyOps = dummyOps;
  }

  public void addDummyOp(HashTableDummyOperator dummyOp) {
    if (dummyOps == null) {
      dummyOps = new LinkedList<HashTableDummyOperator>();
    }
    dummyOps.add(dummyOp);
  }

  public abstract void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap);

  public abstract Set<Operator<? extends OperatorDesc>> getAllRootOperators();
  public abstract Operator<? extends OperatorDesc> getAnyRootOperator();

  public Set<Operator<?>> getAllOperators() {

    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Set<Operator<?>> opSet = getAllRootOperators();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();

    // add all children
    opStack.addAll(opSet);

    while(!opStack.empty()) {
      Operator<?> op = opStack.pop();
      returnSet.add(op);
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }

    return returnSet;
  }

  /**
   * Returns a set containing all leaf operators from the operator tree in this work.
   * @return a set containing all leaf operators in this operator tree.
   */
  public Set<Operator<? extends OperatorDesc>> getAllLeafOperators() {
    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Set<Operator<?>> opSet = getAllRootOperators();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();

    // add all children
    opStack.addAll(opSet);

    while (!opStack.empty()) {
      Operator<?> op = opStack.pop();
      if (op.getNumChild() == 0) {
        returnSet.add(op);
      }
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }

    return returnSet;
  }

  // -----------------------------------------------------------------------------------------------

  public VectorizedRowBatchCtx getVectorizedRowBatchCtx() {
    return vectorizedRowBatchCtx;
  }

  public void setVectorizedRowBatchCtx(VectorizedRowBatchCtx vectorizedRowBatchCtx) {
    this.vectorizedRowBatchCtx = vectorizedRowBatchCtx;
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * @return the mapredLocalWork
   */
  @Explain(displayName = "Local Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public MapredLocalWork getMapRedLocalWork() {
    return mrLocalWork;
  }

  /**
   * @param mapLocalWork
   *          the mapredLocalWork to set
   */
  public void setMapRedLocalWork(final MapredLocalWork mapLocalWork) {
    this.mrLocalWork = mapLocalWork;
  }

  public void setUberMode(boolean uberMode) {
    this.uberMode = uberMode;
  }

  public boolean getUberMode() {
    return uberMode;
  }

  public void setLlapMode(boolean llapMode) {
    this.llapMode = llapMode;
  }

  public boolean getLlapMode() {
    return llapMode;
  }

  public abstract void configureJobConf(JobConf job);

  public void setTag(int tag) {
    this.tag = tag;
  }

  public int getTag() {
    return tag;
  }

  public void addSortCols(List<String> sortCols) {
    this.sortColNames.addAll(sortCols);
  }

  public List<String> getSortCols() {
    return sortColNames;
  }
}
