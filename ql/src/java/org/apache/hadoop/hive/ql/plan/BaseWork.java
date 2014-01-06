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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;

/**
 * BaseWork. Base class for any "work" that's being done on the cluster. Items like stats
 * gathering that are commonly used regarless of the type of work live here.
 */
@SuppressWarnings({"serial", "deprecation"})
public abstract class BaseWork extends AbstractOperatorDesc {

  // dummyOps is a reference to all the HashTableDummy operators in the
  // plan. These have to be separately initialized when we setup a task.
  // Their funtion is mainly as root ops to give the mapjoin the correct
  // schema info.
  List<HashTableDummyOperator> dummyOps;

  public BaseWork() {}

  public BaseWork(String name) {
    setName(name);
  }

  private boolean gatheringStats;

  private String name;

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

  protected abstract List<Operator<?>> getAllRootOperators();

  public List<Operator<?>> getAllOperators() {

    List<Operator<?>> returnList = new ArrayList<Operator<?>>();
    List<Operator<?>> opList = getAllRootOperators();

    //recursively add all children
    while (!opList.isEmpty()) {
      Operator<?> op = opList.remove(0);
      if (op.getChildOperators() != null) {
        opList.addAll(op.getChildOperators());
      }
      returnList.add(op);
    }

    return returnList;
  }
}
