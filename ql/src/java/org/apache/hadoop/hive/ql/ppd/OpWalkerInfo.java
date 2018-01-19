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
package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Context class for operator walker of predicate pushdown.
 */
public class OpWalkerInfo implements NodeProcessorCtx {
  /**
   * Operator to Pushdown Predicates Map. This keeps track of the final pushdown
   * predicates for each operator as you walk the Op Graph from child to parent
   */
  private final HashMap<Operator<? extends OperatorDesc>, ExprWalkerInfo>
    opToPushdownPredMap;
  private final ParseContext pGraphContext;
  private final List<FilterOperator> candidateFilterOps;

  public OpWalkerInfo(ParseContext pGraphContext) {
    this.pGraphContext = pGraphContext;
    opToPushdownPredMap = new HashMap<Operator<? extends OperatorDesc>, ExprWalkerInfo>();
    candidateFilterOps = new ArrayList<FilterOperator>();
  }

  public ExprWalkerInfo getPrunedPreds(Operator<? extends OperatorDesc> op) {
    return opToPushdownPredMap.get(op);
  }

  public ExprWalkerInfo putPrunedPreds(Operator<? extends OperatorDesc> op,
      ExprWalkerInfo value) {
    return opToPushdownPredMap.put(op, value);
  }

  public ParseContext getParseContext() {
    return pGraphContext;
  }

  public List<FilterOperator> getCandidateFilterOps() {
    return candidateFilterOps;
  }

  public void addCandidateFilterOp(FilterOperator fop) {
    candidateFilterOps.add(fop);
  }

}
