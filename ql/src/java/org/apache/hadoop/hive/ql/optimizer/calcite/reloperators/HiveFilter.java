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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.CorrelationIdSearcher;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.HiveCorrelationInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.calcite.rel.core.CorrelationId;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveFilter extends Filter implements HiveRelNode {
  private static final Logger LOG = LoggerFactory.getLogger(HiveFilter.class);

  // lazy fetch, only populate if getVariablesSet is called
  private CorrelationInfoMap correlationInfoMap = new CorrelationInfoMap();

  public static class StatEnhancedHiveFilter extends HiveFilter {

    private long rowCount;

    // FIXME: use a generic proxy wrapper to create runtimestat enhanced nodes
    public StatEnhancedHiveFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition,
        long rowCount) {
      super(cluster, traits, child, condition);
      this.rowCount = rowCount;
    }

    public long getRowCount() {
      return rowCount;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
      return rowCount;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
      assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
      return new StatEnhancedHiveFilter(getCluster(), traitSet, input, condition, rowCount);
    }

  }

  public HiveFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    return new HiveFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public void implement(Implementor implementor) {
  }

  public Set<CorrelationId> getVariablesSet(RexSubQuery subquery) {
    return correlationInfoMap.get().get(subquery).correlationIds;
  }

  public HiveCorrelationInfo getCorrelationInfo() {
    //XXX: needs fixing, this is ugly
    for (RexSubQuery rsq : correlationInfoMap.get().keySet()) {
      return correlationInfoMap.get().get(rsq);
    }
    return new HiveCorrelationInfo();
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    if (shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

  @Override
  public Set<CorrelationId> getVariablesSet() {
    Set<CorrelationId> correlationIds = new LinkedHashSet<CorrelationId>();
    for (Map.Entry<RexSubQuery, HiveCorrelationInfo> entry : correlationInfoMap.get().entrySet()) {
      correlationIds.addAll(entry.getValue().correlationIds);
    }
    return correlationIds;
  }

  public Set<HiveCorrelationInfo> getCorrelationInfos() {
    return ImmutableSet.copyOf(correlationInfoMap.get().values());
  }

  private class CorrelationInfoMap {
    Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap; 

    public Map<RexSubQuery, HiveCorrelationInfo> get() {
      if (correlationInfoMap == null) {
        CorrelationIdSearcher searcher = new CorrelationIdSearcher(getCondition()); 
        correlationInfoMap = searcher.getCorrelationInfoMap();
      }
      return correlationInfoMap;
    }
  }
}
