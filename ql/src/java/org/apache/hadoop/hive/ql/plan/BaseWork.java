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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;

/**
 * BaseWork. Base class for any "work" that's being done on the cluster. Items like stats
 * gathering that are commonly used regarless of the type of work live here.
 */
@SuppressWarnings({"serial", "deprecation"})
public abstract class BaseWork extends AbstractOperatorDesc {

  private boolean gatheringStats;

  public void setGatheringStats(boolean gatherStats) {
    this.gatheringStats = gatherStats;
  }

  public boolean isGatheringStats() {
    return this.gatheringStats;
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
