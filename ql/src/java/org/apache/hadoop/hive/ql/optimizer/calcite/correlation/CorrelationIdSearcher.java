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
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.calcite.rel.core.CorrelationId;
import java.util.Set;
import java.util.HashSet;

public class CorrelationIdSearcher {
  private final HiveCorrelationInfo correlationInfo;

  private static class CorrelationIdVisitor extends RexShuttle {
    public Set<CorrelationId> correlationIds = new HashSet<>();
    public boolean hasGroupByAgg;
    public RexSubQuery rexSubQuery;

    private CorrelationIdVisitor getOuter() {
      return this;
    }
  
    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      correlationIds.add(variable.id);
      return super.visitCorrelVariable(variable);
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      if (rexSubQuery == null) {
        rexSubQuery = subQuery;
      }
      HiveRelShuttle relShuttle = new HiveRelShuttleImpl() {
        @Override
        public RelNode visit(HiveFilter filter) {
          filter.getCondition().accept(getOuter());
          return super.visit(filter);
        }

        @Override
        public RelNode visit(HiveProject project) {
          for (RexNode r : project.getProjects()) {
            r.accept(getOuter());
          }
          return super.visit(project);
        }

        @Override
        public RelNode visit(HiveAggregate aggregate) {
          getOuter().hasGroupByAgg = true;
          return super.visit(aggregate);
        }
      };
      subQuery.rel.accept(relShuttle);
      return subQuery;
    }
  }

  public CorrelationIdSearcher(RexNode rexNode) {
    CorrelationIdVisitor visitor = new CorrelationIdVisitor();
    rexNode.accept(visitor);
    correlationInfo = new HiveCorrelationInfo(visitor.correlationIds, visitor.rexSubQuery,
        visitor.hasGroupByAgg);
  }

  public HiveCorrelationInfo getCorrelationInfo() {
    return correlationInfo;
  }
}
