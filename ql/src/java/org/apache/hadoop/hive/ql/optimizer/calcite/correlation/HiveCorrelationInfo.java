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
   * isCorrScalarQuery returns true when we think the subquery will return 1 row.
   */
  public boolean isCorrScalarQuery() {
    // Not aggregating. Can't determine number of rows
    if (aggregateRel == null) {
      return false;
    }

    // Has a gruop by.  Can't determine number of rows
    if (hasExplicitGroupBy()) {
      return false;
    }

    switch (rexSubQuery.getKind()) {
      case SCALAR_QUERY:
        // is a correlated scalar query, return true.
        return hasCorrelation();
      case IN:
        // XXX: need to understand this better from code in parse/QBSubQuery.java
        return notFlag ? hasCorrelation() : hasCorrelation() && hasCount();
      default:
        return false;
    }
  }

  private boolean hasExplicitGroupBy() {
    return aggregateRel.getGroupCount() > 0;
  }

  private boolean hasCorrelation() {
    return correlationIds.size() > 0;
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
