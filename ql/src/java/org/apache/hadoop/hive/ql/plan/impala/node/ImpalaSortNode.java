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

package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.SortNode;
import org.apache.impala.thrift.TSortType;

public class ImpalaSortNode extends SortNode {

  private ImpalaNodeInfo nodeInfo;

  public ImpalaSortNode(PlanNodeId id, PlanNode input, SortInfo sortInfo, long offset,
      ImpalaNodeInfo nodeInfo) {
    super(id, input, sortInfo, offset, TSortType.TOTAL /* TODO: support TopN and Partial */);
    this.nodeInfo = nodeInfo;
  }

  @Override
  public void assignConjuncts(Analyzer analyzer) {
    // Do nothing since Sort does not evaluate conjuncts; just make sure
    // we were supplied an empty list of conjuncts
    Preconditions.checkState(nodeInfo.getAssignedConjuncts().size() == 0);
  }

}
