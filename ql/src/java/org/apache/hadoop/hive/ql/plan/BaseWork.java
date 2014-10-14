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

import java.util.LinkedList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.mapred.JobConf;

/**
 * BaseWork. Base class for any "work" that's being done on the cluster. Items like stats
 * gathering that are commonly used regardless of the type of work live here.
 */
@SuppressWarnings({"serial", "deprecation"})
public abstract class BaseWork extends AbstractOperatorDesc {

  // dummyOps is a reference to all the HashTableDummy operators in the
  // plan. These have to be separately initialized when we setup a task.
  // Their function is mainly as root ops to give the mapjoin the correct
  // schema info.
  List<HashTableDummyOperator> dummyOps;
  int tag;

  public BaseWork() {}

  public BaseWork(String name) {
    setName(name);
  }

  private boolean gatheringStats;

  private String name;

  // Vectorization.
  protected Map<String, Map<Integer, String>> scratchColumnVectorTypes = null;
  protected Map<String, Map<String, Integer>> scratchColumnMap = null;
  protected boolean vectorMode = false;

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
    this.dummyOps = dummyOps;
  }

  public void addDummyOp(HashTableDummyOperator dummyOp) {
    if (dummyOps == null) {
      dummyOps = new LinkedList<HashTableDummyOperator>();
    }
    dummyOps.add(dummyOp);
  }

  public abstract void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap);

  public abstract Set<Operator<?>> getAllRootOperators();

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

  public Map<String, Map<Integer, String>> getScratchColumnVectorTypes() {
    return scratchColumnVectorTypes;
  }

  public void setScratchColumnVectorTypes(
      Map<String, Map<Integer, String>> scratchColumnVectorTypes) {
    this.scratchColumnVectorTypes = scratchColumnVectorTypes;
  }

  public Map<String, Map<String, Integer>> getScratchColumnMap() {
    return scratchColumnMap;
  }

  public void setScratchColumnMap(Map<String, Map<String, Integer>> scratchColumnMap) {
    this.scratchColumnMap = scratchColumnMap;
  }

  @Override
  public void setVectorMode(boolean vectorMode) {
    this.vectorMode = vectorMode;
  }

  public boolean getVectorMode() {
    return vectorMode;
  }

  public abstract void configureJobConf(JobConf job);

  public void setTag(int tag) {
    this.tag = tag;
  }

  public int getTag() {
    return tag;
  }
}
