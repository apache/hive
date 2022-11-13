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


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.calcite.rel.core.CorrelationId;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.LinkedHashMap;

public class CorrelationIdSearcher {
  private Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap = new LinkedHashMap<>();
  private Set<CorrelationId> correlationIds = new LinkedHashSet<>();
  private boolean hasWindowingFn;
  private HiveAggregate aggregateRel;

  private static class CorrelationIdVisitor extends RexShuttle {
    public Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap = new LinkedHashMap<>();
    public Set<CorrelationId> correlationIds = new LinkedHashSet<>();
    public HiveAggregate aggregateRel;
    public boolean hasWindowingFn;
    public boolean notFlag;

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getOperator().getKind() == SqlKind.NOT) {
        notFlag = true;
      }
      RexNode returnNode = super.visitCall(call);
      notFlag = false;
      return returnNode;
    }

    @Override
    public RexNode visitOver(RexOver over) {
      hasWindowingFn = true;
      return super.visitOver(over);
    }

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      correlationIds.add(variable.id);
      return super.visitCorrelVariable(variable);
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      SubQueryRelNodeShuttle relShuttle = new SubQueryRelNodeShuttle(subQuery, notFlag);
      subQuery.rel.accept(relShuttle);
      correlationInfoMap.put(subQuery, relShuttle.getHiveCorrelationInfo());

      return subQuery;
    }
  }

  private static class SubQueryRelNodeShuttle extends HiveRelShuttleImpl {
    public final RexSubQuery rexSubQuery;
    public HiveAggregate aggregateRel;
    public Map<RexSubQuery, HiveCorrelationInfo> correlationInfoMap = new LinkedHashMap<>();
    public Set<CorrelationId> correlationIds = new LinkedHashSet<>();
    public boolean hasWindowingFn;
    public boolean notFlag;

    public SubQueryRelNodeShuttle(RexSubQuery rexSubQuery, boolean notFlag) {
      this.rexSubQuery = rexSubQuery;
      this.notFlag = notFlag;
    }

    @Override
    public RelNode visit(HiveFilter filter) {
      CorrelationIdSearcher searcher = new CorrelationIdSearcher(filter.getCondition());
      this.correlationInfoMap.putAll(searcher.correlationInfoMap);
      this.correlationIds.addAll(searcher.correlationIds);
      this.hasWindowingFn = searcher.hasWindowingFn;
      return super.visit(filter);
    }

    @Override
    public RelNode visit(HiveProject project) {
      for (RexNode r : project.getProjects()) {
        CorrelationIdSearcher searcher = new CorrelationIdSearcher(r);
        this.correlationInfoMap.putAll(searcher.correlationInfoMap);
        this.correlationIds.addAll(searcher.correlationIds);
        this.hasWindowingFn = searcher.hasWindowingFn;
      }
      return super.visit(project);
    }

    @Override
    public RelNode visit(HiveJoin join) {
      CorrelationIdSearcher searcher = new CorrelationIdSearcher(join.getCondition());
      this.correlationInfoMap.putAll(searcher.correlationInfoMap);
      this.correlationIds.addAll(searcher.correlationIds);
      this.hasWindowingFn = searcher.hasWindowingFn;
      return super.visit(join);
    }

    @Override
    public RelNode visit(HiveAggregate aggregate) {
      this.aggregateRel = aggregate;
      return super.visit(aggregate);
    }

    public HiveCorrelationInfo getHiveCorrelationInfo() {
      return new HiveCorrelationInfo(correlationIds, rexSubQuery, aggregateRel,
          correlationInfoMap, notFlag, hasWindowingFn);
    }
  }

  public CorrelationIdSearcher(RexNode rexNode) {
    CorrelationIdVisitor visitor = new CorrelationIdVisitor();
    rexNode.accept(visitor);
    //XXX: make this immutable
    correlationInfoMap = visitor.correlationInfoMap;
    correlationIds = visitor.correlationIds;
    hasWindowingFn = visitor.hasWindowingFn;
    aggregateRel = visitor.aggregateRel;
  }

  public Map<RexSubQuery, HiveCorrelationInfo> getCorrelationInfoMap() {
    return correlationInfoMap;
  }
}
