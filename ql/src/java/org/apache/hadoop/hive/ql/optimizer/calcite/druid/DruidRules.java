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
package org.apache.hadoop.hive.ql.optimizer.calcite.druid;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveDateGranularity;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectSortTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveSortProjectTransposeRule;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Rules and relational operators for {@link DruidQuery}.
 *
 * TODO: to be removed when Calcite is upgraded to 1.9
 */
public class DruidRules {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidRules.class);

  // Avoid instantiation
  private DruidRules() {
  }

  public static final DruidFilterRule FILTER = new DruidFilterRule();
  public static final DruidProjectRule PROJECT = new DruidProjectRule();
  public static final DruidAggregateRule AGGREGATE = new DruidAggregateRule();
  public static final DruidProjectAggregateRule PROJECT_AGGREGATE = new DruidProjectAggregateRule();
  public static final DruidSortRule SORT = new DruidSortRule();
  public static final DruidProjectSortRule PROJECT_SORT = new DruidProjectSortRule();
  public static final DruidSortProjectRule SORT_PROJECT = new DruidSortProjectRule();

  /** Predicate that returns whether Druid can not handle an aggregate. */
  private static final Predicate<AggregateCall> BAD_AGG = new Predicate<AggregateCall>() {
    public boolean apply(AggregateCall aggregateCall) {
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
        case SUM:
        case SUM0:
        case MIN:
        case MAX:
          return false;
        default:
          return true;
      }
    }
  };

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Filter} into a {@link DruidQuery}.
   */
  private static class DruidFilterRule extends RelOptRule {
    private DruidFilterRule() {
      super(operand(Filter.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'f')
              || !query.isValidFilter(filter.getCondition())) {
        return;
      }
      // Timestamp
      int timestampFieldIdx = -1;
      for (int i = 0; i < query.getRowType().getFieldCount(); i++) {
        if (DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(
                query.getRowType().getFieldList().get(i).getName())) {
          timestampFieldIdx = i;
          break;
        }
      }
      final Pair<List<RexNode>, List<RexNode>> pair = splitFilters(
              filter.getCluster().getRexBuilder(), query, filter.getCondition(), timestampFieldIdx);
      if (pair == null) {
        // We can't push anything useful to Druid.
        return;
      }
      List<Interval> intervals = null;
      if (!pair.left.isEmpty()) {
        intervals = DruidIntervalUtils.createInterval(
                query.getRowType().getFieldList().get(timestampFieldIdx).getType(),
                pair.left);
        if (intervals == null) {
          // We can't push anything useful to Druid.
          return;
        }
      }
      DruidQuery newDruidQuery = query;
      if (!pair.right.isEmpty()) {
        if (!validConditions(pair.right)) {
          return;
        }
        final RelNode newFilter = filter.copy(filter.getTraitSet(), Util.last(query.rels),
                RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), pair.right, false));
        newDruidQuery = DruidQuery.extendQuery(query, newFilter);
      }
      if (intervals != null) {
        newDruidQuery = DruidQuery.extendQuery(newDruidQuery, intervals);
      }
      call.transformTo(newDruidQuery);
    }

    /* Splits the filter condition in two groups: those that filter on the timestamp column
     * and those that filter on other fields */
    private static Pair<List<RexNode>, List<RexNode>> splitFilters(final RexBuilder rexBuilder,
            final DruidQuery input, RexNode cond, final int timestampFieldIdx) {
      final List<RexNode> timeRangeNodes = new ArrayList<>();
      final List<RexNode> otherNodes = new ArrayList<>();
      List<RexNode> conjs = RelOptUtil.conjunctions(cond);
      if (conjs.isEmpty()) {
        // We do not transform
        return null;
      }
      // Number of columns with the dimensions and timestamp
      int max = input.getRowType().getFieldCount() - input.druidTable.metricFieldNames.size();
      for (RexNode conj : conjs) {
        final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
        conj.accept(visitor);
        if (visitor.inputPosReferenced.contains(timestampFieldIdx)) {
          if (visitor.inputPosReferenced.size() != 1) {
            // Complex predicate, transformation currently not supported
            return null;
          }
          timeRangeNodes.add(conj);
        } else if (!visitor.inputPosReferenced.tailSet(max).isEmpty()) {
          // Filter on metrics, not supported in Druid
          return null;
        } else {
          otherNodes.add(conj);
        }
      }
      return Pair.of(timeRangeNodes, otherNodes);
    }

    /* Checks that all conditions are on ref + literal*/
    private static boolean validConditions(List<RexNode> nodes) {
      for (RexNode node: nodes) {
        try {
          node.accept(
              new RexVisitorImpl<Void>(true) {
                @SuppressWarnings("incomplete-switch")
                @Override public Void visitCall(RexCall call) {
                  switch (call.getKind()) {
                    case CAST:
                      // Only if on top of ref or literal
                      if (call.getOperands().get(0) instanceof RexInputRef ||
                              call.getOperands().get(0) instanceof RexLiteral) {
                        break;
                      }
                      // Not supported
                      throw Util.FoundOne.NULL;
                    case EQUALS:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                      // Check cast
                      RexNode left = call.getOperands().get(0);
                      if (left.getKind() == SqlKind.CAST) {
                        left = ((RexCall)left).getOperands().get(0);
                      }
                      RexNode right = call.getOperands().get(1);
                      if (right.getKind() == SqlKind.CAST) {
                        right = ((RexCall)right).getOperands().get(0);
                      }
                      if (left instanceof RexInputRef && right instanceof RexLiteral) {
                        break;
                      }
                      if (right instanceof RexInputRef && left instanceof RexLiteral) {
                        break;
                      }
                      // Not supported if it is not ref + literal
                      throw Util.FoundOne.NULL;
                    case BETWEEN:
                    case IN:
                      // Not supported here yet
                      throw Util.FoundOne.NULL;
                  }
                  return super.visitCall(call);
                }
              });
        } catch (Util.FoundOne e) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Rule to push a {@link org.apache.calcite.rel.core.Project} into a {@link DruidQuery}.
   */
  private static class DruidProjectRule extends RelOptRule {
    private DruidProjectRule() {
      super(operand(Project.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'p')) {
        return;
      }

      if (canProjectAll(project.getProjects())) {
        // All expressions can be pushed to Druid in their entirety.
        final RelNode newProject = project.copy(project.getTraitSet(),
                ImmutableList.of(Util.last(query.rels)));
        RelNode newNode = DruidQuery.extendQuery(query, newProject);
        call.transformTo(newNode);
        return;
      }
      final Pair<List<RexNode>, List<RexNode>> pair = splitProjects(
              project.getCluster().getRexBuilder(), query, project.getProjects());
      if (pair == null) {
        // We can't push anything useful to Druid.
        return;
      }
      final List<RexNode> above = pair.left;
      final List<RexNode> below = pair.right;
      final RelDataTypeFactory.FieldInfoBuilder builder = project.getCluster().getTypeFactory()
              .builder();
      final RelNode input = Util.last(query.rels);
      for (RexNode e : below) {
        final String name;
        if (e instanceof RexInputRef) {
          name = input.getRowType().getFieldNames().get(((RexInputRef) e).getIndex());
        } else {
          name = null;
        }
        builder.add(name, e.getType());
      }
      final RelNode newProject = project.copy(project.getTraitSet(), input, below, builder.build());
      final DruidQuery newQuery = DruidQuery.extendQuery(query, newProject);
      final RelNode newProject2 = project.copy(project.getTraitSet(), newQuery, above,
              project.getRowType());
      call.transformTo(newProject2);
    }

    private static boolean canProjectAll(List<RexNode> nodes) {
      for (RexNode e : nodes) {
        if (!(e instanceof RexInputRef)) {
          return false;
        }
      }
      return true;
    }

    private static Pair<List<RexNode>, List<RexNode>> splitProjects(final RexBuilder rexBuilder,
            final RelNode input, List<RexNode> nodes) {
      final RelOptUtil.InputReferencedVisitor visitor = new RelOptUtil.InputReferencedVisitor();
      for (RexNode node : nodes) {
        node.accept(visitor);
      }
      if (visitor.inputPosReferenced.size() == input.getRowType().getFieldCount()) {
        // All inputs are referenced
        return null;
      }
      final List<RexNode> belowNodes = new ArrayList<>();
      final List<RelDataType> belowTypes = new ArrayList<>();
      final List<Integer> positions = Lists.newArrayList(visitor.inputPosReferenced);
      for (int i : positions) {
        final RexNode node = rexBuilder.makeInputRef(input, i);
        belowNodes.add(node);
        belowTypes.add(node.getType());
      }
      final List<RexNode> aboveNodes = new ArrayList<>();
      for (RexNode node : nodes) {
        aboveNodes.add(node.accept(new RexShuttle() {
          @Override
          public RexNode visitInputRef(RexInputRef ref) {
            final int index = positions.indexOf(ref.getIndex());
            return rexBuilder.makeInputRef(belowTypes.get(index), index);
          }
        }));
      }
      return Pair.of(aboveNodes, belowNodes);
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link DruidQuery}.
   */
  private static class DruidAggregateRule extends RelOptRule {
    private DruidAggregateRule() {
      super(operand(Aggregate.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'a')) {
        return;
      }
      if (aggregate.indicator
              || aggregate.getGroupSets().size() != 1
              || Iterables.any(aggregate.getAggCallList(), BAD_AGG)
              || !validAggregate(aggregate, query)) {
        return;
      }
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      call.transformTo(DruidQuery.extendQuery(query, newAggregate));
    }

    /* Check whether agg functions reference timestamp */
    private static boolean validAggregate(Aggregate aggregate, DruidQuery query) {
      ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        builder.addAll(aggCall.getArgList());
      }
      return !checkTimestampRefOnQuery(builder.build(), query.getTopNode());
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} and
   * {@link org.apache.calcite.rel.core.Project} into a {@link DruidQuery}.
   */
  private static class DruidProjectAggregateRule extends RelOptRule {
    private DruidProjectAggregateRule() {
      super(operand(Aggregate.class,
              operand(Project.class,
                      operand(DruidQuery.class, none()))));
    }

    public void onMatch(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final DruidQuery query = call.rel(2);
      if (!DruidQuery.isValidSignature(query.signature() + 'p' + 'a')) {
        return;
      }
      int timestampIdx;
      if ((timestampIdx = validProject(project, query)) == -1) {
        return;
      }
      if (aggregate.indicator
              || aggregate.getGroupSets().size() != 1
              || Iterables.any(aggregate.getAggCallList(), BAD_AGG)
              || !validAggregate(aggregate, timestampIdx)) {
        return;
      }

      final RelNode newProject = project.copy(project.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      final DruidQuery projectDruidQuery = DruidQuery.extendQuery(query, newProject);
      final RelNode newAggregate = aggregate.copy(aggregate.getTraitSet(),
              ImmutableList.of(Util.last(projectDruidQuery.rels)));
      call.transformTo(DruidQuery.extendQuery(projectDruidQuery, newAggregate));
    }

    /* To be a valid Project, we allow it to contain references, and a single call
     * to an EXTRACT function on the timestamp column. Returns the reference to
     * the timestamp, if any. */
    private static int validProject(Project project, DruidQuery query) {
      List<RexNode> nodes = project.getProjects();
      int idxTimestamp = -1;
      for (int i = 0; i < nodes.size(); i++) {
        final RexNode e = nodes.get(i);
        if (e instanceof RexCall) {
          // It is a call, check that it is EXTRACT and follow-up conditions
          final RexCall call = (RexCall) e;
          if (!HiveDateGranularity.ALL_FUNCTIONS.contains(call.getOperator())) {
            return -1;
          }
          if (idxTimestamp != -1) {
            // Already one usage of timestamp column
            return -1;
          }
          if (!(call.getOperands().get(0) instanceof RexInputRef)) {
            return -1;
          }
          final RexInputRef ref = (RexInputRef) call.getOperands().get(0);
          if (!(checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()), query.getTopNode()))) {
            return -1;
          }
          idxTimestamp = i;
          continue;
        }
        if (!(e instanceof RexInputRef)) {
          // It needs to be a reference
          return -1;
        }
        final RexInputRef ref = (RexInputRef) e;
        if (checkTimestampRefOnQuery(ImmutableBitSet.of(ref.getIndex()), query.getTopNode())) {
          if (idxTimestamp != -1) {
            // Already one usage of timestamp column
            return -1;
          }
          idxTimestamp = i;
        }
      }
      return idxTimestamp;
    }

    private static boolean validAggregate(Aggregate aggregate, int idx) {
      if (!aggregate.getGroupSet().get(idx)) {
        return false;
      }
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        if (aggCall.getArgList().contains(idx)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Sort} through a
   * {@link org.apache.calcite.rel.core.Project}. Useful to transform
   * to complex Druid queries.
   */
  private static class DruidProjectSortRule extends HiveSortProjectTransposeRule {
    private DruidProjectSortRule() {
      super(operand(Sort.class,
              operand(Project.class,
                      operand(DruidQuery.class, none()))));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      return true;
    }

  }

  /**
   * Rule to push back {@link org.apache.calcite.rel.core.Project} through a
   * {@link org.apache.calcite.rel.core.Sort}. Useful if after pushing Sort,
   * we could not push it inside DruidQuery.
   */
  private static class DruidSortProjectRule extends HiveProjectSortTransposeRule {
    private DruidSortProjectRule() {
      super(operand(Project.class,
              operand(Sort.class,
                      operand(DruidQuery.class, none()))));
    }
  }

  /**
   * Rule to push an {@link org.apache.calcite.rel.core.Aggregate} into a {@link DruidQuery}.
   */
  private static class DruidSortRule extends RelOptRule {
    private DruidSortRule() {
      super(operand(Sort.class,
              operand(DruidQuery.class, none())));
    }

    public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final DruidQuery query = call.rel(1);
      if (!DruidQuery.isValidSignature(query.signature() + 'l')) {
        return;
      }
      // Either it is:
      // - a sort without limit on the time column on top of
      //     Agg operator (transformable to timeseries query), or
      // - it is a sort w/o limit on columns that do not include
      //     the time column on top of Agg operator, or
      // - a simple limit on top of other operator than Agg
      if (!validSortLimit(sort, query)) {
        return;
      }
      final RelNode newSort = sort.copy(sort.getTraitSet(),
              ImmutableList.of(Util.last(query.rels)));
      call.transformTo(DruidQuery.extendQuery(query, newSort));
    }

    /* Check sort valid */
    private static boolean validSortLimit(Sort sort, DruidQuery query) {
      if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
        // offset not supported by Druid
        return false;
      }
      if (query.getTopNode() instanceof Aggregate) {
        final Aggregate topAgg = (Aggregate) query.getTopNode();
        final ImmutableBitSet.Builder positionsReferenced = ImmutableBitSet.builder();
        int metricsRefs = 0;
        for (RelFieldCollation col : sort.collation.getFieldCollations()) {
          int idx = col.getFieldIndex();
          if (idx >= topAgg.getGroupCount()) {
            metricsRefs++;
            continue;
          }
          positionsReferenced.set(topAgg.getGroupSet().nth(idx));
        }
        boolean refsTimestamp =
                checkTimestampRefOnQuery(positionsReferenced.build(), topAgg.getInput());
        if (refsTimestamp && metricsRefs != 0) {
          return false;
        }
        return true;
      }
      // If it is going to be a Druid select operator, we push the limit iff
      // 1) it does not contain a sort specification (required by Druid) and
      // 2) limit is smaller than select threshold, as otherwise it might be
      //   better to obtain some parallelization and let global limit
      //   optimizer kick in
      HiveDruidConf conf = sort.getCluster().getPlanner()
              .getContext().unwrap(HiveDruidConf.class);
      return HiveCalciteUtil.pureLimitRelNode(sort) &&
              RexLiteral.intValue(sort.fetch) <= conf.getSelectThreshold();
    }
  }

  /* Check if any of the references leads to the timestamp column */
  private static boolean checkTimestampRefOnQuery(ImmutableBitSet set, RelNode top) {
    if (top instanceof Project) {
      ImmutableBitSet.Builder newSet = ImmutableBitSet.builder();
      final Project project = (Project) top;
      for (int index : set) {
        RexNode node = project.getProjects().get(index);
        if (node instanceof RexInputRef) {
          newSet.set(((RexInputRef)node).getIndex());
        } else if (node instanceof RexCall) {
          RexCall call = (RexCall) node;
          assert HiveDateGranularity.ALL_FUNCTIONS.contains(call.getOperator());
          newSet.set(((RexInputRef)call.getOperands().get(0)).getIndex());
        }
      }
      top = project.getInput();
      set = newSet.build();
    }

    // Check if any references the timestamp column
    for (int index : set) {
      if (DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(top.getRowType().getFieldNames().get(index))) {
        return true;
      }
    }

    return false;
  }

}

// End DruidRules.java