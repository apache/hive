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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import java.util.EnumSet;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Cost model interface.
 */
public abstract class HiveCostModel {

  private static final Log LOG = LogFactory.getLog(HiveCostModel.class);

  // NOTE: COMMON_JOIN & SMB_JOIN are Sort Merge Join (in case of COMMON_JOIN
  // each parallel computation handles multiple splits where as in case of SMB
  // each parallel computation handles one bucket). MAP_JOIN and BUCKET_JOIN is
  // hash joins where MAP_JOIN keeps the whole data set of non streaming tables
  // in memory where as BUCKET_JOIN keeps only the b
  public enum JoinAlgorithm {
    NONE, COMMON_JOIN, MAP_JOIN, BUCKET_JOIN, SMB_JOIN
  }

  public abstract RelOptCost getDefaultCost();

  public abstract RelOptCost getAggregateCost(HiveAggregate aggregate);

  public RelOptCost getJoinCost(HiveJoin join) {
    // Retrieve algorithms
    EnumSet<JoinAlgorithm> possibleAlgorithms = getExecutableJoinAlgorithms(join);

    // Select algorithm with min cost
    JoinAlgorithm joinAlgorithm = null;
    RelOptCost minJoinCost = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Join algorithm selection for:\n" + RelOptUtil.toString(join));
    }
    for (JoinAlgorithm possibleAlgorithm : possibleAlgorithms) {
      RelOptCost joinCost = getJoinCost(join, possibleAlgorithm);
      if (LOG.isDebugEnabled()) {
        LOG.debug(possibleAlgorithm + " cost: " + joinCost);
      }
      if (minJoinCost == null || joinCost.isLt(minJoinCost) ) {
        joinAlgorithm = possibleAlgorithm;
        minJoinCost = joinCost;
      }
    }
    join.setJoinAlgorithm(joinAlgorithm);
    join.setJoinCost(minJoinCost);
    if (LOG.isDebugEnabled()) {
      LOG.debug(joinAlgorithm + " selected");
    }

    return minJoinCost;
  }

  /**
   * Returns the possible algorithms for a given join operator.
   *
   * @param join the join operator
   * @return a set containing all the possible join algorithms that can be
   * executed for this join operator
   */
  abstract EnumSet<JoinAlgorithm> getExecutableJoinAlgorithms(HiveJoin join);

  /**
   * Returns the cost for a given algorithm and execution engine.
   *
   * @param join the join operator
   * @param algorithm the join algorithm
   * @return the cost for the given algorithm, or null if the algorithm is not
   * defined for this execution engine
   */
  abstract RelOptCost getJoinCost(HiveJoin join, JoinAlgorithm algorithm);
}
