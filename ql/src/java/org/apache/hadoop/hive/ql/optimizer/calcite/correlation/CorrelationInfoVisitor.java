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


import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;

/**
 * CorrelationIdVisitor digs out information from a given RexNode about all the RexSubQuery
 * RexNodes in the top layer. It does not go into RexSubQuery nodes within a RexSubQuery node.
 */
public class CorrelationInfoVisitor extends RexShuttle {
  // List of CorrelationInfo for all the found RexSubQuery, one HiveCorrelationInfo for each
  // RexSubQuery
  private List<HiveCorrelationInfo> correlationInfos = new ArrayList<>();

  // True if there is a "NOT" outside of the subquery
  private boolean notFlag;

  // Prevent direct instantiation.  Information should be retrieved through the static
  // getCorrelationInfos method at the bottom of the file.
  private CorrelationInfoVisitor() {
  }

  // Private method called by static methoc to retrieve information after RexNode was visited
  private List<HiveCorrelationInfo> getCorrelationInfos() {
    return correlationInfos;
  }

  @Override
  public RexNode visitCall(RexCall call) {
    // Record if we see not flag before seeing the subquery.
    // Can handle NOT(NOT(...)) if  it exists
    if (call.getOperator().getKind() == SqlKind.NOT) {
      notFlag = !notFlag;
    }
    RexNode returnNode = super.visitCall(call);
    // reset flag after visit.
    if (call.getOperator().getKind() == SqlKind.NOT) {
      notFlag = !notFlag;
    }
    return returnNode;
  }

  @Override
  public RexNode visitSubQuery(RexSubQuery subQuery) {
    // Call the RelNode Shuttle to gather all the information within the RelNodes of the
    // RexSubQUery
    SubQueryRelNodeShuttle relShuttle = new SubQueryRelNodeShuttle(subQuery, notFlag);
    subQuery.rel.accept(relShuttle);
    correlationInfos.add(relShuttle.getCorrelationInfo());
    return subQuery;
  }

  /**
   * InnerRexSubQueryVisitor is a shuttle that only gets called during the RexSubQuery RexNode
   * visit.  The RexSubQuery has RelNodes inside. These RelNodes contain the correlationIds used
   * for the subquery within their RexNodes.
   */
  private static class InsideRexSubQueryVisitor extends RexShuttle {
    private Set<CorrelationId> correlationIds = new LinkedHashSet<>();

    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
      correlationIds.add(variable.id);
      return super.visitCorrelVariable(variable);
    }

    public Set<CorrelationId> getCorrelationIds() {
      return correlationIds;
    }
  }

  /**
   * The SubQueryRelNodeShuttle walks through the RelNodes within the RexSubQuery.
   * The CorrelationIds can be found within the RexNodes inside the RelNodes, so
   * we call the InsideRexSubQueryVisitor to fetch them. This visitor will hold the
   * array of correlationIds and keep the composite list of Ids across RelNodes
   */
  private static class SubQueryRelNodeShuttle extends HiveRelShuttleImpl {
    public final RexSubQuery rexSubQuery;
    public final boolean notFlag;
    private HiveAggregate aggregateRel;
    private InsideRexSubQueryVisitor visitor = new InsideRexSubQueryVisitor();

    public SubQueryRelNodeShuttle(RexSubQuery rexSubQuery, boolean notFlag) {
      this.rexSubQuery = rexSubQuery;
      this.notFlag = notFlag;
    }

    @Override
    public RelNode visit(HiveFilter filter) {
      // call the shuttle to fetch the CorrelationIds
      filter.getCondition().accept(visitor);
      return super.visit(filter);
    }

    @Override
    public RelNode visit(HiveProject project) {
      for (RexNode r : project.getProjects()) {
        // call the shuttle for each project to fetch the CorrelationIds
        r.accept(visitor);
      }
      return super.visit(project);
    }

    @Override
    public RelNode visit(HiveJoin join) {
      // call the shuttle to fetch the CorrelationIds
      join.getCondition().accept(visitor);
      return super.visit(join);
    }

    @Override
    public RelNode visit(HiveAggregate aggregate) {
      // capture the aggregate RelNode to grab information off of it.
      if (this.aggregateRel == null) {
        this.aggregateRel = aggregate;
      }
      return super.visit(aggregate);
    }

    public HiveCorrelationInfo getCorrelationInfo() {
      return new HiveCorrelationInfo(visitor.getCorrelationIds(), rexSubQuery,
          aggregateRel, notFlag);
    }
  }

  public static List<HiveCorrelationInfo> getCorrelationInfos(RexNode rexNode) {
    CorrelationInfoVisitor visitor = new CorrelationInfoVisitor();
    rexNode.accept(visitor);
    return ImmutableList.copyOf(visitor.getCorrelationInfos());
  }
}
