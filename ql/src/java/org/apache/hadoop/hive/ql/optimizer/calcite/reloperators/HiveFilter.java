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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.CorrelationInfoVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.HiveCorrelationInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.calcite.rel.core.CorrelationId;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;

public class HiveFilter extends Filter implements HiveRelNode {

  // Information about correlations within a subquery.
  private final CorrelationInfoSupplier correlationInfos;

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
    this.correlationInfos = new CorrelationInfoSupplier(getCondition());
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    return new HiveFilter(getCluster(), traitSet, input, condition);
  }

  @Override
  public Set<CorrelationId> getVariablesSet() {
    Set<CorrelationId> correlationIds = new LinkedHashSet<>();
    for (HiveCorrelationInfo h : correlationInfos.get()) {
      correlationIds.addAll(h.correlationIds);
    }
    return correlationIds;
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    if (shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

  public List<HiveCorrelationInfo> getCorrelationInfos() {
    return correlationInfos.get();
  }

  /**
   * CorrelationInfoSupplier allows for a lazy fetch so that the HiveCorrelationInfo
   * only gets retrieved on demand.
   */
  private static class CorrelationInfoSupplier {
    private final RexNode condition;
    public CorrelationInfoSupplier(RexNode condition) {
      this.condition = condition;
    }

    List<HiveCorrelationInfo> correlationInfos;

    public List<HiveCorrelationInfo> get() {
      if (correlationInfos == null) {
        correlationInfos = CorrelationInfoVisitor.getCorrelationInfos(condition);
      }
      return correlationInfos;
    }
  }
}
