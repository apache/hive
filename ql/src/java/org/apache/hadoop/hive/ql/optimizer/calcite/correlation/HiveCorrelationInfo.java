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
package org.apache.hadoop.hive.ql.optimizer.calcite.correlation;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import java.util.Set;

/**
 * HiveCorrelationInfo contains information gathered by visiting the RexSubQuery node
 */
public class HiveCorrelationInfo {
  // The RexSubQuery relevant to the correlation info.
  public final RexSubQuery rexSubQuery;

  // List of all correlationIds within the SubQuery.  This does not contain correlationIds
  // of a RexSubQuery nested within a RexSubQuery
  public final Set<CorrelationId> correlationIds;

  // the Aggreate RelNode if it exists within the RexNode
  public final HiveAggregate aggregateRel;

  // True if there is a not flag before the RexSubQuery (e.g. NOT EXISTS (select ...)
  public final boolean notFlag;

  public HiveCorrelationInfo(Set<CorrelationId> correlationIds, RexSubQuery rexSubQuery,
      HiveAggregate aggregateRel, boolean notFlag) {
    this.correlationIds = ImmutableSet.copyOf(correlationIds);
    this.rexSubQuery = rexSubQuery;
    this.aggregateRel = aggregateRel;
    this.notFlag = notFlag;
  }

  
  /**
   * isCorrScalarQuery returns true for special cases as specified in the
   * HiveSubQueryRemoveRule rewrite methods for steps that need to be taken
   * before writing the join operator. It will add an sq_count_check to ensure
   * that there is a row.
   *
   * This logic was copied from QBSubQuery.java for HIVE-26736. The following
   * comment was copied from there:
   *
   * Restriction.13.m :: In the case of an implied Group By on a
   * correlated SubQuery, the SubQuery always returns 1 row.
   *   Following is special cases for different type of subqueries which have aggregate and implicit group by
   *   and are correlatd
   *     * SCALAR - This should return true since later in subquery remove
   *                rule we need to know about this case.
   *     * IN - always allowed, BUT returns true for cases with aggregate other than COUNT since later in subquery remove
   *            rule we need to know about this case.
   *     * NOT IN - always allow, but always return true because later subq remove rule will generate diff plan for this case
   */
  public boolean isCorrScalarQuery() {
    if (aggregateRel == null) {
      return false;
    }

    if (hasExplicitGroupBy()) {
      return false;
    }

    switch (rexSubQuery.getKind()) {
      case SCALAR_QUERY:
        return hasCorrelation();
      case IN:
        return notFlag ? hasCorrelation() : hasCorrelation() && hasCount();
      default:
        return false;
    }
  }

  private boolean hasExplicitGroupBy() {
    return aggregateRel.getGroupCount() > 0;
  }

  private boolean hasCorrelation() {
    return !correlationIds.isEmpty();
  }

  private boolean hasCount() {
    for (AggregateCall aggCall : aggregateRel.getAggCallList()) {
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
        return true;
      }
    }
    return false;
  }
}
