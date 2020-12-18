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

package org.apache.hadoop.hive.impala.node;

import org.apache.calcite.util.Pair;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.UnionNode;

import java.util.List;

public class ImpalaUnionNode extends UnionNode {

  public ImpalaUnionNode(PlanNodeId id, TupleId tupleId,
      List<Expr> resultExprs,
      List<Pair<PlanNode, List<Expr>>> planNodeAndExprsList) {
    // CDPD-9406: Need to determine when we should set subPlanId to true.
    super(id, tupleId, resultExprs, false /* subPlanId */);
    for (Pair<PlanNode, List<Expr>> planNodeAndExprs : planNodeAndExprsList) {
      addChild(planNodeAndExprs.left, planNodeAndExprs.right);
    }
  }

  @Override
  public void assignConjuncts(Analyzer analyzer) {
  }
}
