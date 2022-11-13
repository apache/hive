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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCorrelationInfo {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCorrelationInfo.class);
  public final Set<CorrelationId> correlationIds;
  public final HiveAggregate aggregateRel;
  public final RexSubQuery rexSubQuery;
  public final Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap;
  public final boolean notFlag;
  public final boolean hasWindowingFn;

  public HiveCorrelationInfo() {
    correlationIds = new HashSet<>();
    aggregateRel = null;
    rexSubQuery = null;
    correlationInfoMap = new HashMap<>();
    notFlag = false;
    hasWindowingFn = false;
  }

  public HiveCorrelationInfo(Set<CorrelationId> correlationIds, RexSubQuery rexSubQuery,
      HiveAggregate aggregateRel,
      Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap,
      boolean notFlag, boolean hasWindowingFn) {
    ImmutableSet.Builder builder = ImmutableSet.builder();
    builder.addAll(correlationIds);
    for (HiveCorrelationInfo h : correlationInfoMap.values()) {
      // XXX: should  do recursively
      builder.addAll(h.correlationIds);
    }
    this.correlationIds = builder.build();
    this.rexSubQuery = rexSubQuery;
    this.aggregateRel = aggregateRel;
    //XXX: make this immutable
    this.correlationInfoMap = correlationInfoMap;
    this.notFlag = notFlag;
    this.hasWindowingFn = hasWindowingFn;
  }

  public boolean isCorrScalarQuery() {
    if (aggregateRel == null) {
      LOG.info("SJC: AGGREGATE IS NULL");
      return false;
    }
    LOG.info("SJC: HAS EXPLICIT GROUPS IS " + hasExplicitGroupBy());
    if (hasExplicitGroupBy()) {
      return false;
    }

    LOG.info("SJC: HAS CORRELATION IS " + hasCorrelation());
    LOG.info("SJC: HAS COUNT IS " + hasCount());
    LOG.info("SJC: NOT FLAG IS " + notFlag);
    
    LOG.info("SJC: REXSUBQUERY IS " + rexSubQuery);
    LOG.info("SJC: REXSUBQUERY KIND IS " + rexSubQuery.getKind());
    switch (rexSubQuery.getKind()) {
      case SCALAR_QUERY:
        return hasCorrelation() || hasWindowingFn;
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
