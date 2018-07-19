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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.List;

import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;

import com.google.common.collect.ImmutableList;

/**
 * JoinTypeCheckCtx is used by Calcite planner(CBO) to generate Join Conditions from Join Condition AST.
 * Reasons for sub class:
 * 1. Join Conditions can not handle:
 *    a. Stateful Functions
 *    b. Distinct
 *    c. '*' expr
 *    d. '.*' expr
 *    e. Windowing expr
 *    f. Complex type member access
 *    g. Array Index Access
 *    h. Sub query
 *    i. GB expr elimination
 * 2. Join Condn expr has two input RR as opposed to one.
 */

/**
 * TODO:<br>
 * 1. Could we use combined RR instead of list of RR ?<br>
 * 2. Why not use GB expr ?
 */
public class JoinTypeCheckCtx extends TypeCheckCtx {
  private final ImmutableList<RowResolver> inputRRLst;
  private final boolean outerJoin;

  public JoinTypeCheckCtx(RowResolver leftRR, RowResolver rightRR, JoinType hiveJoinType)
      throws SemanticException {
    super(RowResolver.getCombinedRR(leftRR, rightRR), true, false, false, false, false, false, false, false,
        true, false);
    this.inputRRLst = ImmutableList.of(leftRR, rightRR);
    this.outerJoin = (hiveJoinType == JoinType.LEFTOUTER) || (hiveJoinType == JoinType.RIGHTOUTER)
        || (hiveJoinType == JoinType.FULLOUTER);
  }

  /**
   * @return the inputRR List
   */
  public List<RowResolver> getInputRRList() {
    return inputRRLst;
  }

  public boolean isOuterJoin() {
    return outerJoin;
  }
}
