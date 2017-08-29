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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Stacks;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * NOTE: this whole logic is replicated from Calcite's RelDecorrelator
 *  and is exteneded to make it suitable for HIVE
 *  TODO:
 *    We should get rid of this and replace it with Calcite's RelDecorrelator
 *    once that works with Join, Project etc instead of LogicalJoin, LogicalProject.
 *    Also we need to have CALCITE-1511 fixed
 *
 * RelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 * <p>TODO:</p>
 * <ul>
 *   <li>replace {@code CorelMap} constructor parameter with a RelNode
 *   <li>make {@link #currentRel} immutable (would require a fresh
 *      RelDecorrelator for each node being decorrelated)</li>
 *   <li>make fields of {@code CorelMap} immutable</li>
 *   <li>make sub-class rules static, and have them create their own
 *   de-correlator</li>
 * </ul>
 */
public class HiveRelDecorrelator implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  protected static final Logger LOG = LoggerFactory.getLogger(
          HiveRelDecorrelator.class);

  //~ Instance fields --------------------------------------------------------

  private final RelBuilder relBuilder;

  // map built during translation
  private CorelMap cm;

  private final ReflectUtil.MethodDispatcher<Frame> dispatcher =
          ReflectUtil.createMethodDispatcher(Frame.class, this, "decorrelateRel",
                  RelNode.class);

  private final RexBuilder rexBuilder;

  // The rel which is being visited
  private RelNode currentRel;

  private final Context context;

  /** Built during decorrelation, of rel to all the newly created correlated
   * variables in its output, and to map old input positions to new input
   * positions. This is from the view point of the parent rel of a new rel. */
  private final Map<RelNode, Frame> map = new HashMap<>();

  private final HashSet<LogicalCorrelate> generatedCorRels = Sets.newHashSet();

  private Stack valueGen = new Stack();

  //~ Constructors -----------------------------------------------------------

  private HiveRelDecorrelator (
          RelOptCluster cluster,
          CorelMap cm,
          Context context) {
    this.cm = cm;
    this.rexBuilder = cluster.getRexBuilder();
    this.context = context;
    relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);

  }

  //~ Methods ----------------------------------------------------------------

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code RelDecorrelator}.
   *
   * @param rootRel Root node of the query
   *
   * @return Equivalent query with all
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel) {
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final HiveRelDecorrelator decorrelator =
            new HiveRelDecorrelator(cluster, corelMap,
                    cluster.getPlanner().getContext());

    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (!decorrelator.cm.mapCorToCorRel.isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    return newRootRel;
  }

  private void setCurrent(RelNode root, LogicalCorrelate corRel) {
    currentRel = corRel;
    if (corRel != null) {
      cm = new CorelMapBuilder().build(Util.first(root, corRel));
    }
  }

  private RelNode decorrelate(RelNode root) {
    // first adjust count() expression if any
    HepProgram program = HepProgram.builder()
            .addRuleInstance(new AdjustProjectForCountAggregateRule(false))
            .addRuleInstance(new AdjustProjectForCountAggregateRule(true))
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
            // FilterCorrelateRule rule mistakenly pushes a FILTER, consiting of correlated vars,
            // on top of LogicalCorrelate to within  left input for scalar corr queries
            // which causes exception during decorrelation. This has been disabled for now.
            //.addRuleInstance(FilterCorrelateRule.INSTANCE)
            .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    root = planner.findBestExp();

    // Perform decorrelation.
    map.clear();

    final Frame frame = getInvoke(root, null);
    if (frame != null) {
      // has been rewritten; apply rules post-decorrelation
      final HepProgram program2 = HepProgram.builder()
              .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
              .addRuleInstance(FilterJoinRule.JOIN)
              .build();

      final HepPlanner planner2 = createPlanner(program2);
      final RelNode newRoot = frame.r;
      planner2.setRoot(newRoot);
      return planner2.findBestExp();
    }

    assert(valueGen.isEmpty());

    return root;
  }

  private Function2<RelNode, RelNode, Void> createCopyHook() {
    return new Function2<RelNode, RelNode, Void>() {
      public Void apply(RelNode oldNode, RelNode newNode) {
        if (cm.mapRefRelToCorRef.containsKey(oldNode)) {
          final CorelMap corelMap = new CorelMapBuilder().build(newNode);
          // since after various rules original relnode could have different
          // corref (or might not have at all) we need to traverse the new node
          // to figure out new cor refs and put that into map
          if(!corelMap.mapRefRelToCorRef.isEmpty()){
            cm.mapRefRelToCorRef.putAll(newNode,
                    corelMap.mapRefRelToCorRef.get(newNode));
          }
        }
        if (oldNode instanceof LogicalCorrelate
                && newNode instanceof LogicalCorrelate) {
          LogicalCorrelate oldCor = (LogicalCorrelate) oldNode;
          CorrelationId c = oldCor.getCorrelationId();
          if (cm.mapCorToCorRel.get(c) == oldNode) {
            cm.mapCorToCorRel.put(c, newNode);
          }

          if (generatedCorRels.contains(oldNode)) {
            generatedCorRels.add((LogicalCorrelate) newNode);
          }
        }
        return null;
      }
    };
  }

  private HepPlanner createPlanner(HepProgram program) {
    // Create a planner with a hook to update the mapping tables when a
    // node is copied when it is registered.
    return new HepPlanner(
            program,
            context,
            true,
            createCopyHook(),
            RelOptCostImpl.FACTORY);
  }

  public RelNode removeCorrelationViaRule(RelNode root) {
    HepProgram program = HepProgram.builder()
            .addRuleInstance(new RemoveSingleAggregateRule())
            .addRuleInstance(new RemoveCorrelationForScalarProjectRule())
            .addRuleInstance(new RemoveCorrelationForScalarAggregateRule())
            .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    return planner.findBestExp();
  }

  protected RexNode decorrelateExpr(RexNode exp, boolean valueGenerator) {
    DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
    shuttle.setValueGenerator(valueGenerator);
    return exp.accept(shuttle);
  }
  protected RexNode decorrelateExpr(RexNode exp) {
    DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
    shuttle.setValueGenerator(false);
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(rexBuilder,
                    projectPulledAboveLeftCorrelator, null, ImmutableSet.<Integer>of());
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator,
          RexInputRef nullIndicator) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(rexBuilder,
                    projectPulledAboveLeftCorrelator, nullIndicator,
                    ImmutableSet.<Integer>of());
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
          RexNode exp,
          boolean projectPulledAboveLeftCorrelator,
          Set<Integer> isCount) {
    RemoveCorrelationRexShuttle shuttle =
            new RemoveCorrelationRexShuttle(rexBuilder,
                    projectPulledAboveLeftCorrelator, null, isCount);
    return exp.accept(shuttle);
  }

  /** Fallback if none of the other {@code decorrelateRel} methods match. */
  public Frame decorrelateRel(RelNode rel) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

    if (rel.getInputs().size() > 0) {
      List<RelNode> oldInputs = rel.getInputs();
      List<RelNode> newInputs = Lists.newArrayList();
      for (int i = 0; i < oldInputs.size(); ++i) {
        final Frame frame = getInvoke(oldInputs.get(i), rel);
        if (frame == null || !frame.corDefOutputs.isEmpty()) {
          // if input is not rewritten, or if it produces correlated
          // variables, terminate rewrite
          return null;
        }
        newInputs.add(frame.r);
        newRel.replaceInput(i, frame.r);
      }

      if (!Util.equalShallow(oldInputs, newInputs)) {
        newRel = rel.copy(rel.getTraitSet(), newInputs);
      }
    }

    // the output position should not change since there are no corVars
    // coming from below.
    return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()),
            ImmutableSortedMap.<CorDef, Integer>of());
  }

  /**
   * Rewrite Sort.
   *
   * @param rel Sort to be rewritten
   */
  public Frame decorrelateRel(HiveSortLimit rel) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // Sort itself should not reference cor vars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    // Sort only references field positions in collations field.
    // The collations field in the newRel now need to refer to the
    // new output positions in its input.
    // Its output does not change the input ordering, so there's no
    // need to call propagateExpr.

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final RelNode newInput = frame.r;

    Mappings.TargetMapping mapping =
            Mappings.target(
                    frame.oldToNewOutputs,
                    oldInput.getRowType().getFieldCount(),
                    newInput.getRowType().getFieldCount());

    RelCollation oldCollation = rel.getCollation();
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

    final RelNode newSort = HiveSortLimit.create(newInput, newCollation, rel.offset, rel.fetch);

    // Sort does not change input ordering
    return register(rel, newSort, frame.oldToNewOutputs,
            frame.corDefOutputs);
  }
  /**
   * Rewrite Sort.
   *
   * @param rel Sort to be rewritten
   */
  public Frame decorrelateRel(Sort rel) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // Sort itself should not reference cor vars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    // Sort only references field positions in collations field.
    // The collations field in the newRel now need to refer to the
    // new output positions in its input.
    // Its output does not change the input ordering, so there's no
    // need to call propagateExpr.

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final RelNode newInput = frame.r;

    Mappings.TargetMapping mapping =
            Mappings.target(
                    frame.oldToNewOutputs,
                    oldInput.getRowType().getFieldCount(),
                    newInput.getRowType().getFieldCount());

    RelCollation oldCollation = rel.getCollation();
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

    final RelNode newSort = HiveSortLimit.create(newInput, newCollation, rel.offset, rel.fetch);

    // Sort does not change input ordering
    return register(rel, newSort, frame.oldToNewOutputs,
            frame.corDefOutputs);
  }

  /**
   * Rewrites a {@link Values}.
   *
   * @param rel Values to be rewritten
   */
  public Frame decorrelateRel(Values rel) {
    // There are no inputs, so rel does not need to be changed.
    return null;
  }

  /**
   * Rewrites a {@link LogicalAggregate}.
   *
   * @param rel Aggregate to rewrite
   */
  public Frame decorrelateRel(LogicalAggregate rel) throws SemanticException{
    if (rel.getGroupType() != Aggregate.Group.SIMPLE) {
      throw new AssertionError(Bug.CALCITE_461_FIXED);
    }
    //
    // Rewrite logic:
    //
    // 1. Permute the group by keys to the front.
    // 2. If the input of an aggregate produces correlated variables,
    //    add them to the group list.
    // 3. Change aggCalls to reference the new project.
    //

    // Aggregate itself should not reference cor vars.
    assert !cm.mapRefRelToCorRef.containsKey(rel);

    final RelNode oldInput = rel.getInput();
    final Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    final RelNode newInput = frame.r;

    // map from newInput
    Map<Integer, Integer> mapNewInputToProjOutputs = new HashMap<>();
    final int oldGroupKeyCount = rel.getGroupSet().cardinality();

    // Project projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    List<RelDataTypeField> newInputOutput =
            newInput.getRowType().getFieldList();

    int newPos = 0;

    // oldInput has the original group by keys in the front.
    final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
    for (int i = 0; i < oldGroupKeyCount; i++) {
      final RexLiteral constant = projectedLiteral(newInput, i);
      if (constant != null) {
        // Exclude constants. Aggregate({true}) occurs because Aggregate({})
        // would generate 1 row even when applied to an empty table.
        omittedConstants.put(i, constant);
        continue;
      }
      int newInputPos = frame.oldToNewOutputs.get(i);
      projects.add(RexInputRef.of2(newInputPos, newInputOutput));
      mapNewInputToProjOutputs.put(newInputPos, newPos);
      newPos++;
    }

    final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    if (!frame.corDefOutputs.isEmpty()) {
      // If input produces correlated variables, move them to the front,
      // right after any existing GROUP BY fields.

      // Now add the corVars from the input, starting from
      // position oldGroupKeyCount.
      for (Map.Entry<CorDef, Integer> entry
              : frame.corDefOutputs.entrySet()) {
        projects.add(RexInputRef.of2(entry.getValue(), newInputOutput));

        corDefOutputs.put(entry.getKey(), newPos);
        mapNewInputToProjOutputs.put(entry.getValue(), newPos);
        newPos++;
      }
    }

    // add the remaining fields
    final int newGroupKeyCount = newPos;
    for (int i = 0; i < newInputOutput.size(); i++) {
      if (!mapNewInputToProjOutputs.containsKey(i)) {
        projects.add(RexInputRef.of2(i, newInputOutput));
        mapNewInputToProjOutputs.put(i, newPos);
        newPos++;
      }
    }

    assert newPos == newInputOutput.size();

    // This Project will be what the old input maps to,
    // replacing any previous mapping from old input).

    RelNode newProject = HiveProject.create(newInput, Pair.left(projects), Pair.right(projects));

    // update mappings:
    // oldInput ----> newInput
    //
    //                newProject
    //                   |
    // oldInput ----> newInput
    //
    // is transformed to
    //
    // oldInput ----> newProject
    //                   |
    //                newInput
    Map<Integer, Integer> combinedMap = Maps.newHashMap();

    for (Integer oldInputPos : frame.oldToNewOutputs.keySet()) {
      combinedMap.put(oldInputPos,
              mapNewInputToProjOutputs.get(
                      frame.oldToNewOutputs.get(oldInputPos)));
    }

    register(oldInput, newProject, combinedMap, corDefOutputs);

    // now it's time to rewrite the Aggregate
    final ImmutableBitSet newGroupSet = ImmutableBitSet.range(newGroupKeyCount);
    List<AggregateCall> newAggCalls = Lists.newArrayList();
    List<AggregateCall> oldAggCalls = rel.getAggCallList();

    int oldInputOutputFieldCount = rel.getGroupSet().cardinality();
    int newInputOutputFieldCount = newGroupSet.cardinality();

    int i = -1;
    for (AggregateCall oldAggCall : oldAggCalls) {
      ++i;
      List<Integer> oldAggArgs = oldAggCall.getArgList();

      List<Integer> aggArgs = Lists.newArrayList();

      // Adjust the aggregator argument positions.
      // Note aggregator does not change input ordering, so the input
      // output position mapping can be used to derive the new positions
      // for the argument.
      for (int oldPos : oldAggArgs) {
        aggArgs.add(combinedMap.get(oldPos));
      }
      final int filterArg = oldAggCall.filterArg < 0 ? oldAggCall.filterArg
              : combinedMap.get(oldAggCall.filterArg);

      newAggCalls.add(
              oldAggCall.adaptTo(newProject, aggArgs, filterArg,
                      oldGroupKeyCount, newGroupKeyCount));

      // The old to new output position mapping will be the same as that
      // of newProject, plus any aggregates that the oldAgg produces.
      combinedMap.put(
              oldInputOutputFieldCount + i,
              newInputOutputFieldCount + i);
    }

    relBuilder.push(
            LogicalAggregate.create(newProject,
                    false,
                    newGroupSet,
                    null,
                    newAggCalls));

    if (!omittedConstants.isEmpty()) {
      final List<RexNode> postProjects = new ArrayList<>(relBuilder.fields());
      for (Map.Entry<Integer, RexLiteral> entry
              : omittedConstants.descendingMap().entrySet()) {
        postProjects.add(entry.getKey() + frame.corDefOutputs.size(),
                entry.getValue());
      }
      relBuilder.project(postProjects);
    }

    // Aggregate does not change input ordering so corVars will be
    // located at the same position as the input newProject.
    return register(rel, relBuilder.build(), combinedMap, corDefOutputs);
  }

  public Frame getInvoke(RelNode r, RelNode parent) {
    final Frame frame = dispatcher.invoke(r);
    if (frame != null) {
      map.put(r, frame);
    }
    currentRel = parent;
    return frame;
  }

  /** Returns a literal output field, or null if it is not literal. */
  private static RexLiteral projectedLiteral(RelNode rel, int i) {
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final RexNode node = project.getProjects().get(i);
      if (node instanceof RexLiteral) {
        return (RexLiteral) node;
      }
    }
    return null;
  }

  public Frame decorrelateRel(HiveAggregate rel) throws SemanticException{
    {
      if (rel.getGroupType() != Aggregate.Group.SIMPLE) {
        throw new AssertionError(Bug.CALCITE_461_FIXED);
      }
      //
      // Rewrite logic:
      //
      // 1. Permute the group by keys to the front.
      // 2. If the input of an aggregate produces correlated variables,
      //    add them to the group list.
      // 3. Change aggCalls to reference the new project.
      //

      // Aggregate itself should not reference cor vars.
      assert !cm.mapRefRelToCorRef.containsKey(rel);

      final RelNode oldInput = rel.getInput();
      final Frame frame = getInvoke(oldInput, rel);
      if (frame == null) {
        // If input has not been rewritten, do not rewrite this rel.
        return null;
      }
      //assert !frame.corVarOutputPos.isEmpty();
      final RelNode newInput = frame.r;

      // map from newInput
      Map<Integer, Integer> mapNewInputToProjOutputs =  new HashMap<>();
      final int oldGroupKeyCount = rel.getGroupSet().cardinality();

      // Project projects the original expressions,
      // plus any correlated variables the input wants to pass along.
      final List<Pair<RexNode, String>> projects = Lists.newArrayList();

      List<RelDataTypeField> newInputOutput =
              newInput.getRowType().getFieldList();

      int newPos = 0;

      // oldInput has the original group by keys in the front.
      final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
      for (int i = 0; i < oldGroupKeyCount; i++) {
        final RexLiteral constant = projectedLiteral(newInput, i);
        if (constant != null) {
          // Exclude constants. Aggregate({true}) occurs because Aggregate({})
          // would generate 1 row even when applied to an empty table.
          omittedConstants.put(i, constant);
          continue;
        }
        int newInputPos = frame.oldToNewOutputs.get(i);
        projects.add(RexInputRef.of2(newInputPos, newInputOutput));
        mapNewInputToProjOutputs.put(newInputPos, newPos);
        newPos++;
      }

      final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
      if (!frame.corDefOutputs.isEmpty()) {
        // If input produces correlated variables, move them to the front,
        // right after any existing GROUP BY fields.

        // Now add the corVars from the input, starting from
        // position oldGroupKeyCount.
        for (Map.Entry<CorDef, Integer> entry
                : frame.corDefOutputs.entrySet()) {
          projects.add(RexInputRef.of2(entry.getValue(), newInputOutput));

          corDefOutputs.put(entry.getKey(), newPos);
          mapNewInputToProjOutputs.put(entry.getValue(), newPos);
          newPos++;
        }
      }

      // add the remaining fields
      final int newGroupKeyCount = newPos;
      for (int i = 0; i < newInputOutput.size(); i++) {
        if (!mapNewInputToProjOutputs.containsKey(i)) {
          projects.add(RexInputRef.of2(i, newInputOutput));
          mapNewInputToProjOutputs.put(i, newPos);
          newPos++;
        }
      }

      assert newPos == newInputOutput.size();

      // This Project will be what the old input maps to,
      // replacing any previous mapping from old input).
      RelNode newProject = HiveProject.create(newInput, Pair.left(projects), Pair.right(projects));

      // update mappings:
      // oldInput ----> newInput
      //
      //                newProject
      //                   |
      // oldInput ----> newInput
      //
      // is transformed to
      //
      // oldInput ----> newProject
      //                   |
      //                newInput
      Map<Integer, Integer> combinedMap = Maps.newHashMap();

      for (Integer oldInputPos : frame.oldToNewOutputs.keySet()) {
        combinedMap.put(oldInputPos,
                mapNewInputToProjOutputs.get(
                        frame.oldToNewOutputs.get(oldInputPos)));
      }

      register(oldInput, newProject, combinedMap, corDefOutputs);

      // now it's time to rewrite the Aggregate
      final ImmutableBitSet newGroupSet = ImmutableBitSet.range(newGroupKeyCount);
      List<AggregateCall> newAggCalls = Lists.newArrayList();
      List<AggregateCall> oldAggCalls = rel.getAggCallList();

      int oldInputOutputFieldCount = rel.getGroupSet().cardinality();
      int newInputOutputFieldCount = newGroupSet.cardinality();

      int i = -1;
      for (AggregateCall oldAggCall : oldAggCalls) {
        ++i;
        List<Integer> oldAggArgs = oldAggCall.getArgList();

        List<Integer> aggArgs = Lists.newArrayList();

        // Adjust the aggregator argument positions.
        // Note aggregator does not change input ordering, so the input
        // output position mapping can be used to derive the new positions
        // for the argument.
        for (int oldPos : oldAggArgs) {
          aggArgs.add(combinedMap.get(oldPos));
        }
        final int filterArg = oldAggCall.filterArg < 0 ? oldAggCall.filterArg
                : combinedMap.get(oldAggCall.filterArg);

        newAggCalls.add(
                oldAggCall.adaptTo(newProject, aggArgs, filterArg,
                        oldGroupKeyCount, newGroupKeyCount));

        // The old to new output position mapping will be the same as that
        // of newProject, plus any aggregates that the oldAgg produces.
        combinedMap.put(
                oldInputOutputFieldCount + i,
                newInputOutputFieldCount + i);
      }

      relBuilder.push(
              new HiveAggregate(rel.getCluster(), rel.getTraitSet(), newProject, false, newGroupSet, null, newAggCalls) );

      if (!omittedConstants.isEmpty()) {
        final List<RexNode> postProjects = new ArrayList<>(relBuilder.fields());
        for (Map.Entry<Integer, RexLiteral> entry
                : omittedConstants.descendingMap().entrySet()) {
          postProjects.add(entry.getKey() + frame.corDefOutputs.size(),
                  entry.getValue());
        }
        relBuilder.project(postProjects);
      }

      // Aggregate does not change input ordering so corVars will be
      // located at the same position as the input newProject.
      return register(rel, relBuilder.build(), combinedMap, corDefOutputs);
    }
  }

  public Frame decorrelateRel(HiveProject rel) throws SemanticException{
    {
      //
      // Rewrite logic:
      //
      // 1. Pass along any correlated variables coming from the input.
      //

      final RelNode oldInput = rel.getInput();
      Frame frame = getInvoke(oldInput, rel);
      if (frame == null) {
        // If input has not been rewritten, do not rewrite this rel.
        return null;
      }
      final List<RexNode> oldProjects = rel.getProjects();
      final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

      // LogicalProject projects the original expressions,
      // plus any correlated variables the input wants to pass along.
      final List<Pair<RexNode, String>> projects = Lists.newArrayList();

      // If this LogicalProject has correlated reference, create value generator
      // and produce the correlated variables in the new output.
      if (cm.mapRefRelToCorRef.containsKey(rel)) {
        frame = decorrelateInputWithValueGenerator(rel);
      }

      // LogicalProject projects the original expressions
      final Map<Integer, Integer> mapOldToNewOutputs =  new HashMap<>();
      int newPos;
      for (newPos = 0; newPos < oldProjects.size(); newPos++) {
        projects.add(
                newPos,
                Pair.of(
                        decorrelateExpr(oldProjects.get(newPos)),
                        relOutput.get(newPos).getName()));
        mapOldToNewOutputs.put(newPos, newPos);
      }


      // Project any correlated variables the input wants to pass along.
      final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
      for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
        projects.add(
                RexInputRef.of2(entry.getValue(),
                        frame.r.getRowType().getFieldList()));
        corDefOutputs.put(entry.getKey(), newPos);
        newPos++;
      }

      RelNode newProject = HiveProject.create(frame.r, Pair.left(projects), SqlValidatorUtil.uniquify(Pair.right(projects)));

      return register(rel, newProject, mapOldToNewOutputs,
              corDefOutputs);
    }
  }
  /**
   * Rewrite LogicalProject.
   *
   * @param rel the project rel to rewrite
   */
  public Frame decorrelateRel(LogicalProject rel) throws SemanticException{
    //
    // Rewrite logic:
    //
    // 1. Pass along any correlated variables coming from the input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }
    final List<RexNode> oldProjects = rel.getProjects();
    final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

    // LogicalProject projects the original expressions,
    // plus any correlated variables the input wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    // If this LogicalProject has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorRef.containsKey(rel)) {
      frame = decorrelateInputWithValueGenerator(rel);
    }

    // LogicalProject projects the original expressions
    final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
    int newPos;
    for (newPos = 0; newPos < oldProjects.size(); newPos++) {
      projects.add(
              newPos,
              Pair.of(
                      decorrelateExpr(oldProjects.get(newPos)),
                      relOutput.get(newPos).getName()));
      mapOldToNewOutputs.put(newPos, newPos);
    }

    // Project any correlated variables the input wants to pass along.
    final SortedMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
      projects.add(
              RexInputRef.of2(entry.getValue(),
                      frame.r.getRowType().getFieldList()));
      corDefOutputs.put(entry.getKey(), newPos);
      newPos++;
    }

    RelNode newProject = HiveProject.create(frame.r, Pair.left(projects), Pair.right(projects));

    return register(rel, newProject, mapOldToNewOutputs,
            corDefOutputs);
  }

  /**
   * Create RelNode tree that produces a list of correlated variables.
   *
   * @param correlations         correlated variables to generate
   * @param valueGenFieldOffset  offset in the output that generated columns
   *                             will start
   * @param mapCorVarToOutputPos output positions for the correlated variables
   *                             generated
   * @return RelNode the root of the resultant RelNode tree
   */
  private RelNode createValueGenerator(
          Iterable<CorRef> correlations,
          int valueGenFieldOffset,
          SortedMap<CorDef, Integer> corDefOutputs) {
    final Map<RelNode, List<Integer>> mapNewInputToOutputs =
            new HashMap<>();

    final Map<RelNode, Integer> mapNewInputToNewOffset = new HashMap<>();

    // Input provides the definition of a correlated variable.
    // Add to map all the referenced positions (relative to each input rel).
    for (CorRef corVar : correlations) {
      final int oldCorVarOffset = corVar.field;

      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final Frame frame = map.get(oldInput);
      assert frame != null;
      final RelNode newInput = frame.r;

      final List<Integer> newLocalOutputs;
      if (!mapNewInputToOutputs.containsKey(newInput)) {
        newLocalOutputs =  new ArrayList<>();
      } else {
        newLocalOutputs =
                mapNewInputToOutputs.get(newInput);
      }

      final int newCorVarOffset = frame.oldToNewOutputs.get(oldCorVarOffset);

      // Add all unique positions referenced.
      if (!newLocalOutputs.contains(newCorVarOffset)) {
        newLocalOutputs.add(newCorVarOffset);
      }
      mapNewInputToOutputs.put(newInput, newLocalOutputs);
    }

    int offset = 0;

    // Project only the correlated fields out of each inputRel
    // and join the projectRel together.
    // To make sure the plan does not change in terms of join order,
    // join these rels based on their occurrence in cor var list which
    // is sorted.
    final Set<RelNode> joinedInputs =  new HashSet<>();

    RelNode r = null;
    for (CorRef corVar : correlations) {
      final RelNode oldInput = getCorRel(corVar);
      assert oldInput != null;
      final RelNode newInput = map.get(oldInput).r;
      assert newInput != null;

      if (!joinedInputs.contains(newInput)) {
        RelNode project =
                RelOptUtil.createProject(
                        newInput,
                        mapNewInputToOutputs.get(newInput));
        RelNode distinct = RelOptUtil.createDistinctRel(project);
        RelOptCluster cluster = distinct.getCluster();

        joinedInputs.add(newInput);
        mapNewInputToNewOffset.put(newInput, offset);
        offset += distinct.getRowType().getFieldCount();

        if (r == null) {
          r = distinct;
        } else {
          r =
                  LogicalJoin.create(r, distinct,
                          cluster.getRexBuilder().makeLiteral(true),
                          ImmutableSet.<CorrelationId>of(), JoinRelType.INNER);
        }
      }
    }

    // Translate the positions of correlated variables to be relative to
    // the join output, leaving room for valueGenFieldOffset because
    // valueGenerators are joined with the original left input of the rel
    // referencing correlated variables.
    for (CorRef corRef : correlations) {
      // The first input of a Correlator is always the rel defining
      // the correlated variables.
      final RelNode oldInput = getCorRel(corRef);
      assert oldInput != null;
      final Frame frame = map.get(oldInput);
      final RelNode newInput = frame.r;
      assert newInput != null;

      final List<Integer> newLocalOutputs =
              mapNewInputToOutputs.get(newInput);

      final int newLocalOutput = frame.oldToNewOutputs.get(corRef.field);

      // newOutput is the index of the cor var in the referenced
      // position list plus the offset of referenced position list of
      // each newInput.
      final int newOutput =
              newLocalOutputs.indexOf(newLocalOutput)
                      + mapNewInputToNewOffset.get(newInput)
                      + valueGenFieldOffset;

      corDefOutputs.put(corRef.def(), newOutput);
    }

    return r;
  }


  //this returns the source of corVar i.e. Rel which produces cor var
  // value. Therefore it is always LogicalCorrelate's left input which is outer query
  private RelNode getCorRel(CorRef corVar) {
    final RelNode r = cm.mapCorToCorRel.get(corVar.corr);
    RelNode ret = r.getInput(0);
    return ret;
  }

  private Frame decorrelateInputWithValueGenerator(RelNode rel) {
    // currently only handles one input input
    assert rel.getInputs().size() == 1;
    RelNode oldInput = rel.getInput(0);
    final Frame frame = map.get(oldInput);

    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(frame.corDefOutputs);

    final Collection<CorRef> corVarList = cm.mapRefRelToCorRef.get(rel);

    // Try to populate correlation variables using local fields.
    // This means that we do not need a value generator.
    if (rel instanceof Filter) {
      SortedMap<CorDef, Integer> map = new TreeMap<>();
      for (CorRef correlation : corVarList) {
        final CorDef def = correlation.def();
        if (corDefOutputs.containsKey(def) || map.containsKey(def)) {
          continue;
        }
        try {
          findCorrelationEquivalent(correlation, ((Filter) rel).getCondition());
        } catch (Util.FoundOne e) {
          // we need to keep predicate kind e.g. EQUAL or NOT EQUAL
          // so that later while decorrelating LogicalCorrelate appropriate join predicate
          // is generated
          def.setPredicateKind((SqlKind)((Pair)e.getNode()).getValue());
          map.put(def, (Integer)((Pair) e.getNode()).getKey());
        }
      }
      // If all correlation variables are now satisfied, skip creating a value
      // generator.
      if (map.size() == corVarList.size()) {
        map.putAll(frame.corDefOutputs);
        return register(oldInput, frame.r,
                frame.oldToNewOutputs, map);
      }
    }


    int leftInputOutputCount = frame.r.getRowType().getFieldCount();

    // can directly add positions into corDefOutputs since join
    // does not change the output ordering from the inputs.
    RelNode valueGen =
            createValueGenerator(
                    corVarList,
                    leftInputOutputCount,
                    corDefOutputs);

    RelNode join =
            LogicalJoin.create(frame.r, valueGen, rexBuilder.makeLiteral(true),
                    ImmutableSet.<CorrelationId>of(), JoinRelType.INNER);

    // LogicalJoin or LogicalFilter does not change the old input ordering. All
    // input fields from newLeftInput(i.e. the original input to the old
    // LogicalFilter) are in the output and in the same position.
    return register(oldInput, join, frame.oldToNewOutputs, corDefOutputs);
  }


  /** Finds a {@link RexInputRef} that is equivalent to a {@link CorRef},
   * and if found, throws a {@link Util.FoundOne}. */
  private void findCorrelationEquivalent(CorRef correlation, RexNode e)
          throws Util.FoundOne {
    switch (e.getKind()) {
    // for now only EQUAL and NOT EQUAL corr predicates are optimized
    case NOT_EQUALS:
      if((boolean)valueGen.peek()) {
        // we will need value generator
        break;
      }
    case EQUALS:
        final RexCall call = (RexCall) e;
        final List<RexNode> operands = call.getOperands();
        if (references(operands.get(0), correlation)
                && operands.get(1) instanceof RexInputRef) {
          throw new Util.FoundOne(Pair.of(((RexInputRef) operands.get(1)).getIndex(), e.getKind()));
        }
        if (references(operands.get(1), correlation)
                && operands.get(0) instanceof RexInputRef) {
          throw new Util.FoundOne(Pair.of(((RexInputRef) operands.get(0)).getIndex(), e.getKind()));
        }
        break;
      case AND:
        for (RexNode operand : ((RexCall) e).getOperands()) {
          findCorrelationEquivalent(correlation, operand);
        }
    }
  }

  private boolean references(RexNode e, CorRef correlation) {
    switch (e.getKind()) {
      case CAST:
        final RexNode operand = ((RexCall) e).getOperands().get(0);
        if (isWidening(e.getType(), operand.getType())) {
          return references(operand, correlation);
        }
        return false;
      case FIELD_ACCESS:
        final RexFieldAccess f = (RexFieldAccess) e;
        if (f.getField().getIndex() == correlation.field
                && f.getReferenceExpr() instanceof RexCorrelVariable) {
          if (((RexCorrelVariable) f.getReferenceExpr()).id == correlation.corr) {
            return true;
          }
        }
        // fall through
      default:
        return false;
    }
  }

  /** Returns whether one type is just a widening of another.
   *
   * <p>For example:<ul>
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(5)}.
   * <li>{@code VARCHAR(10)} is a widening of {@code VARCHAR(10) NOT NULL}.
   * </ul>
   */
  private boolean isWidening(RelDataType type, RelDataType type1) {
    return type.getSqlTypeName() == type1.getSqlTypeName()
            && type.getPrecision() >= type1.getPrecision();
  }

  public Frame decorrelateRel(HiveFilter rel) throws SemanticException {
    {
      //
      // Rewrite logic:
      //
      // 1. If a LogicalFilter references a correlated field in its filter
      // condition, rewrite the LogicalFilter to be
      //   LogicalFilter
      //     LogicalJoin(cross product)
      //       OriginalFilterInput
      //       ValueGenerator(produces distinct sets of correlated variables)
      // and rewrite the correlated fieldAccess in the filter condition to
      // reference the LogicalJoin output.
      //
      // 2. If LogicalFilter does not reference correlated variables, simply
      // rewrite the filter condition using new input.
      //

      final RelNode oldInput = rel.getInput();
      Frame frame = getInvoke(oldInput, rel);
      if (frame == null) {
        // If input has not been rewritten, do not rewrite this rel.
        return null;
      }

      Frame oldInputFrame = frame;
      // If this LogicalFilter has correlated reference, create value generator
      // and produce the correlated variables in the new output.
      if (cm.mapRefRelToCorRef.containsKey(rel)) {
        frame = decorrelateInputWithValueGenerator(rel);
      }

      boolean valueGenerator = true;
      if(frame.r == oldInputFrame.r) {
        // this means correated value generator wasn't generated
        valueGenerator = false;
      }
        // Replace the filter expression to reference output of the join
        // Map filter to the new filter over join
        relBuilder.push(frame.r).filter(
            (decorrelateExpr(rel.getCondition(), valueGenerator)));
      // Filter does not change the input ordering.
      // Filter rel does not permute the input.
      // All corvars produced by filter will have the same output positions in the
      // input rel.
      return register(rel, relBuilder.build(), frame.oldToNewOutputs,
              frame.corDefOutputs);
    }
  }

    /**
     * Rewrite LogicalFilter.
     *
     * @param rel the filter rel to rewrite
     */
  public Frame decorrelateRel(LogicalFilter rel) {
    //
    // Rewrite logic:
    //
    // 1. If a LogicalFilter references a correlated field in its filter
    // condition, rewrite the LogicalFilter to be
    //   LogicalFilter
    //     LogicalJoin(cross product)
    //       OriginalFilterInput
    //       ValueGenerator(produces distinct sets of correlated variables)
    // and rewrite the correlated fieldAccess in the filter condition to
    // reference the LogicalJoin output.
    //
    // 2. If LogicalFilter does not reference correlated variables, simply
    // rewrite the filter condition using new input.
    //

    final RelNode oldInput = rel.getInput();
    Frame frame = getInvoke(oldInput, rel);
    if (frame == null) {
      // If input has not been rewritten, do not rewrite this rel.
      return null;
    }

    // If this LogicalFilter has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorRef.containsKey(rel)) {
      frame = decorrelateInputWithValueGenerator(rel);

    }

    boolean valueGenerator = true;
    if(frame.r == oldInput) {
      // this means correated value generator wasn't generated
      valueGenerator = false;
    }

    // Replace the filter expression to reference output of the join
    // Map filter to the new filter over join
    relBuilder.push(frame.r).filter(decorrelateExpr(rel.getCondition(), valueGenerator));


    // Filter does not change the input ordering.
    // Filter rel does not permute the input.
    // All corvars produced by filter will have the same output positions in the
    // input rel.
    return register(rel, relBuilder.build(), frame.oldToNewOutputs,
            frame.corDefOutputs);
  }

  /**
   * Rewrite Correlator into a left outer join.
   *
   * @param rel Correlator
   */
  public Frame decorrelateRel(LogicalCorrelate rel) {
    //
    // Rewrite logic:
    //
    // The original left input will be joined with the new right input that
    // has generated correlated variables propagated up. For any generated
    // cor vars that are not used in the join key, pass them along to be
    // joined later with the CorrelatorRels that produce them.
    //

    // the right input to Correlator should produce correlated variables
    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    boolean mightRequireValueGen = new findIfValueGenRequired().traverse(oldRight);
    valueGen.push(mightRequireValueGen);

    final Frame leftFrame = getInvoke(oldLeft, rel);
    final Frame rightFrame = getInvoke(oldRight, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    if (rightFrame.corDefOutputs.isEmpty()) {
      return null;
    }

    assert rel.getRequiredColumns().cardinality()
            <= rightFrame.corDefOutputs.keySet().size();

    // Change correlator rel into a join.
    // Join all the correlated variables produced by this correlator rel
    // with the values generated and propagated from the right input
    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(rightFrame.corDefOutputs);
    final List<RexNode> conditions = new ArrayList<>();
    final List<RelDataTypeField> newLeftOutput =
            leftFrame.r.getRowType().getFieldList();
    int newLeftFieldCount = newLeftOutput.size();

    final List<RelDataTypeField> newRightOutput =
            rightFrame.r.getRowType().getFieldList();

    for (Map.Entry<CorDef, Integer> rightOutput
            : new ArrayList<>(corDefOutputs.entrySet())) {
      final CorDef corDef = rightOutput.getKey();
      if (!corDef.corr.equals(rel.getCorrelationId())) {
        continue;
      }
      final int newLeftPos = leftFrame.oldToNewOutputs.get(corDef.field);
      final int newRightPos = rightOutput.getValue();
      if(corDef.getPredicateKind() == SqlKind.NOT_EQUALS) {
        conditions.add(
            rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS,
                RexInputRef.of(newLeftPos, newLeftOutput),
                new RexInputRef(newLeftFieldCount + newRightPos,
                    newRightOutput.get(newRightPos).getType())));

      }
      else {
        assert(corDef.getPredicateKind() == null
          || corDef.getPredicateKind() == SqlKind.EQUALS);
        conditions.add(
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                RexInputRef.of(newLeftPos, newLeftOutput),
                new RexInputRef(newLeftFieldCount + newRightPos,
                    newRightOutput.get(newRightPos).getType())));

      }

      // remove this cor var from output position mapping
      corDefOutputs.remove(corDef);
    }

    // Update the output position for the cor vars: only pass on the cor
    // vars that are not used in the join key.
    for (CorDef corDef : corDefOutputs.keySet()) {
      int newPos = corDefOutputs.get(corDef) + newLeftFieldCount;
      corDefOutputs.put(corDef, newPos);
    }

    // then add any cor var from the left input. Do not need to change
    // output positions.
    corDefOutputs.putAll(leftFrame.corDefOutputs);

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    final Map<Integer, Integer> mapOldToNewOutputs =  new HashMap<>();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
            == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(
              i + oldLeftFieldCount,
              rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final RexNode condition =
            RexUtil.composeConjunction(rexBuilder, conditions, false);
    RelNode newJoin =
            LogicalJoin.create(leftFrame.r, rightFrame.r, condition,
                    ImmutableSet.<CorrelationId>of(), rel.getJoinType().toJoinType());

    valueGen.pop();

    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  public Frame decorrelateRel(HiveJoin rel) throws SemanticException{
    //
    // Rewrite logic:
    //
    // 1. rewrite join condition.
    // 2. map output positions and produce cor vars if any.
    //

    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, rel);
    final Frame rightFrame = getInvoke(oldRight, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    final RelNode newJoin = HiveJoin.getJoin(rel.getCluster(), leftFrame.r, rightFrame.r, decorrelateExpr(rel.getCondition()), rel.getJoinType() );

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputs = Maps.newHashMap();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
    int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
            == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
              rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(leftFrame.corDefOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (Map.Entry<CorDef, Integer> entry
            : rightFrame.corDefOutputs.entrySet()) {
      corDefOutputs.put(entry.getKey(),
              entry.getValue() + newLeftFieldCount);
    }
    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }
  /**
   * Rewrite LogicalJoin.
   *
   * @param rel LogicalJoin
   */
  public Frame decorrelateRel(LogicalJoin rel) {
    //
    // Rewrite logic:
    //
    // 1. rewrite join condition.
    // 2. map output positions and produce cor vars if any.
    //

    final RelNode oldLeft = rel.getInput(0);
    final RelNode oldRight = rel.getInput(1);

    final Frame leftFrame = getInvoke(oldLeft, rel);
    final Frame rightFrame = getInvoke(oldRight, rel);

    if (leftFrame == null || rightFrame == null) {
      // If any input has not been rewritten, do not rewrite this rel.
      return null;
    }

    final RelNode newJoin = HiveJoin.getJoin(rel.getCluster(), leftFrame.r,
            rightFrame.r, decorrelateExpr(rel.getCondition()), rel.getJoinType() );

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputs = Maps.newHashMap();

    int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
    int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();

    int oldRightFieldCount = oldRight.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
            == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputs.put(i + oldLeftFieldCount,
              rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
    }

    final SortedMap<CorDef, Integer> corDefOutputs =
            new TreeMap<>(leftFrame.corDefOutputs);

    // Right input positions are shifted by newLeftFieldCount.
    for (Map.Entry<CorDef, Integer> entry
            : rightFrame.corDefOutputs.entrySet()) {
      corDefOutputs.put(entry.getKey(),
              entry.getValue() + newLeftFieldCount);
    }
    return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs);
  }

  private RexInputRef getNewForOldInputRef(RexInputRef oldInputRef) {
    assert currentRel != null;

    int oldOrdinal = oldInputRef.getIndex();
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    RelNode oldInput = null;

    for (RelNode oldInput0 : currentRel.getInputs()) {
      RelDataType oldInputType = oldInput0.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInput = oldInput0;
        break;
      }
      RelNode newInput = map.get(oldInput0).r;
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }

    assert oldInput != null;

    final Frame frame = map.get(oldInput);
    assert frame != null;

    // now oldOrdinal is relative to oldInput
    int oldLocalOrdinal = oldOrdinal;

    // figure out the newLocalOrdinal, relative to the newInput.
    int newLocalOrdinal = oldLocalOrdinal;

    if (!frame.oldToNewOutputs.isEmpty()) {
      newLocalOrdinal = frame.oldToNewOutputs.get(oldLocalOrdinal);
    }

    newOrdinal += newLocalOrdinal;

    return new RexInputRef(newOrdinal,
            frame.r.getRowType().getFieldList().get(newLocalOrdinal).getType());
  }

  /**
   * Pulls project above the join from its RHS input. Enforces nullability
   * for join output.
   *
   * @param join          Join
   * @param project       Original project as the right-hand input of the join
   * @param nullIndicatorPos Position of null indicator
   * @return the subtree with the new LogicalProject at the root
   */
  private RelNode projectJoinOutputWithNullability(
          LogicalJoin join,
          LogicalProject project,
          int nullIndicatorPos) {
    final RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
    final RelNode left = join.getLeft();
    final JoinRelType joinType = join.getJoinType();

    RexInputRef nullIndicator =
            new RexInputRef(
                    nullIndicatorPos,
                    typeFactory.createTypeWithNullability(
                            join.getRowType().getFieldList().get(nullIndicatorPos)
                                    .getType(),
                            true));

    // now create the new project
    List<Pair<RexNode, String>> newProjExprs = Lists.newArrayList();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
            left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjExprs.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
              removeCorrelationExpr(
                      pair.left,
                      projectPulledAboveLeftCorrelator,
                      nullIndicator);

      newProjExprs.add(Pair.of(newProjExpr, pair.right));
    }

    return RelOptUtil.createProject(join, newProjExprs, false);
  }

  /**
   * Pulls a {@link Project} above a {@link Correlate} from its RHS input.
   * Enforces nullability for join output.
   *
   * @param correlate  Correlate
   * @param project the original project as the RHS input of the join
   * @param isCount Positions which are calls to the <code>COUNT</code>
   *                aggregation function
   * @return the subtree with the new LogicalProject at the root
   */
  private RelNode aggregateCorrelatorOutput(
          Correlate correlate,
          LogicalProject project,
          Set<Integer> isCount) {
    final RelNode left = correlate.getLeft();
    final JoinRelType joinType = correlate.getJoinType().toJoinType();

    // now create the new project
    final List<Pair<RexNode, String>> newProjects = Lists.newArrayList();

    // Project everything from the LHS and then those from the original
    // project
    final List<RelDataTypeField> leftInputFields =
            left.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjects.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
            joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      RexNode newProjExpr =
              removeCorrelationExpr(
                      pair.left,
                      projectPulledAboveLeftCorrelator,
                      isCount);
      newProjects.add(Pair.of(newProjExpr, pair.right));
    }

    return RelOptUtil.createProject(correlate, newProjects, false);
  }

  /**
   * Checks whether the correlations in projRel and filter are related to
   * the correlated variables provided by corRel.
   *
   * @param correlate    Correlate
   * @param project   The original Project as the RHS input of the join
   * @param filter    Filter
   * @param correlatedJoinKeys Correlated join keys
   * @return true if filter and proj only references corVar provided by corRel
   */
  private boolean checkCorVars(
          LogicalCorrelate correlate,
          LogicalProject project,
          LogicalFilter filter,
          List<RexFieldAccess> correlatedJoinKeys) {
    if (filter != null) {
      assert correlatedJoinKeys != null;

      // check that all correlated refs in the filter condition are
      // used in the join(as field access).
      Set<CorRef> corVarInFilter =
              Sets.newHashSet(cm.mapRefRelToCorRef.get(filter));

      for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
        corVarInFilter.remove(cm.mapFieldAccessToCorRef.get(correlatedJoinKey));
      }

      if (!corVarInFilter.isEmpty()) {
        return false;
      }

      // Check that the correlated variables referenced in these
      // comparisons do come from the correlatorRel.
      corVarInFilter.addAll(cm.mapRefRelToCorRef.get(filter));

      for (CorRef corVar : corVarInFilter) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    // if project has any correlated reference, make sure they are also
    // provided by the current correlate. They will be projected out of the LHS
    // of the correlate.
    if ((project != null) && cm.mapRefRelToCorRef.containsKey(project)) {
      for (CorRef corVar : cm.mapRefRelToCorRef.get(project)) {
        if (cm.mapCorToCorRel.get(corVar.corr) != correlate) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Remove correlated variables from the tree at root corRel
   *
   * @param correlate Correlator
   */
  private void removeCorVarFromTree(LogicalCorrelate correlate) {
    if (cm.mapCorToCorRel.get(correlate.getCorrelationId()) == correlate) {
      cm.mapCorToCorRel.remove(correlate.getCorrelationId());
    }
  }

  /**
   * Projects all {@code input} output fields plus the additional expressions.
   *
   * @param input        Input relational expression
   * @param additionalExprs Additional expressions and names
   * @return the new LogicalProject
   */
  private RelNode createProjectWithAdditionalExprs(
          RelNode input,
          List<Pair<RexNode, String>> additionalExprs) {
    final List<RelDataTypeField> fieldList =
            input.getRowType().getFieldList();
    List<Pair<RexNode, String>> projects = Lists.newArrayList();
    for (Ord<RelDataTypeField> field : Ord.zip(fieldList)) {
      projects.add(
              Pair.of(
                      (RexNode) rexBuilder.makeInputRef(
                              field.e.getType(), field.i),
                      field.e.getName()));
    }
    projects.addAll(additionalExprs);
    return RelOptUtil.createProject(input, projects, false);
  }

  /* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
  static Map<Integer, Integer> identityMap(int count) {
    ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
    for (int i = 0; i < count; i++) {
      builder.put(i, i);
    }
    return builder.build();
  }

  /** Registers a relational expression and the relational expression it became
   * after decorrelation. */
  Frame register(RelNode rel, RelNode newRel,
                 Map<Integer, Integer> oldToNewOutputs,
                 SortedMap<CorDef, Integer> corDefOutputs) {
    final Frame frame = new Frame(rel, newRel, corDefOutputs, oldToNewOutputs);
    map.put(rel, frame);
    return frame;
  }

  static boolean allLessThan(Collection<Integer> integers, int limit,
                             Litmus ret) {
    for (int value : integers) {
      if (value >= limit) {
        return ret.fail("out of range; value: " + value + ", limit: " + limit);
      }
    }
    return ret.succeed();
  }

  private static RelNode stripHep(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      HepRelVertex hepRelVertex = (HepRelVertex) rel;
      rel = hepRelVertex.getCurrentRel();
    }
    return rel;
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Shuttle that decorrelates. */
  private class DecorrelateRexShuttle extends RexShuttle {
    private boolean valueGenerator;
    public void setValueGenerator(boolean valueGenerator) {
      this.valueGenerator = valueGenerator;
    }

    // DecorrelateRexShuttle ends up decorrelating expressions cor.col1 <> $4
    // to $4=$4 if value generator is not generated, $4<>$4 is further simplified
    // to false. This is wrong and messes up the whole tree. To prevent this visitCall
    // is overridden to rewrite/simply such predicates to is not null.
    // we also need to take care that we do this only for correlated predicates and
    // not user specified explicit predicates
    // TODO:  This code should be removed once CALCITE-1851 is fixed and
    // there is support of not equal
    @Override  public RexNode visitCall(final RexCall call) {
      if(!valueGenerator) {
        switch (call.getKind()) {
        case EQUALS:
        case NOT_EQUALS:
          final List<RexNode> operands = new ArrayList<>(call.operands);
          RexNode o0 = operands.get(0);
          RexNode o1 = operands.get(1);
          boolean isCorrelated = false;
          if (o0 instanceof RexFieldAccess && (cm.mapFieldAccessToCorRef.get(o0) != null)) {
            o0 = decorrFieldAccess((RexFieldAccess) o0);
            isCorrelated = true;

          }
          if (o1 instanceof RexFieldAccess && (cm.mapFieldAccessToCorRef.get(o1) != null)) {
            o1 = decorrFieldAccess((RexFieldAccess) o1);
            isCorrelated = true;
          }
          if (isCorrelated && RexUtil.eq(o0, o1)) {
            return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, o0);
          }

          final List<RexNode> newOperands = new ArrayList<>();
          newOperands.add(o0);
          newOperands.add(o1);
          boolean[] update = { false };
          List<RexNode> clonedOperands = visitList(newOperands, update);

          return relBuilder.call(call.getOperator(), clonedOperands);
        }
      }
      return super.visitCall(call);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      return decorrFieldAccess(fieldAccess);
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      final RexInputRef ref = getNewForOldInputRef(inputRef);
      if (ref.getIndex() == inputRef.getIndex()
              && ref.getType() == inputRef.getType()) {
        return inputRef; // re-use old object, to prevent needless expr cloning
      }
      return ref;
    }
    private RexNode decorrFieldAccess(RexFieldAccess fieldAccess) {
      int newInputOutputOffset = 0;
      for (RelNode input : currentRel.getInputs()) {
        final Frame frame = map.get(input);

        if (frame != null) {
          // try to find in this input rel the position of cor var
          final CorRef corRef = cm.mapFieldAccessToCorRef.get(fieldAccess);

          if (corRef != null) {
            Integer newInputPos = frame.corDefOutputs.get(corRef.def());
            if (newInputPos != null) {
              // This input rel does produce the cor var referenced.
              // Assume fieldAccess has the correct type info.
              return new RexInputRef(newInputPos + newInputOutputOffset,
                  frame.r.getRowType().getFieldList().get(newInputPos)
                      .getType());
            }
          }

          // this input rel does not produce the cor var needed
          newInputOutputOffset += frame.r.getRowType().getFieldCount();
        } else {
          // this input rel is not rewritten
          newInputOutputOffset += input.getRowType().getFieldCount();
        }
      }
      return fieldAccess;
    }
  }

  /** Shuttle that removes correlations. */
  private class RemoveCorrelationRexShuttle extends RexShuttle {
    final RexBuilder rexBuilder;
    final RelDataTypeFactory typeFactory;
    final boolean projectPulledAboveLeftCorrelator;
    final RexInputRef nullIndicator;
    final ImmutableSet<Integer> isCount;

    public RemoveCorrelationRexShuttle(
            RexBuilder rexBuilder,
            boolean projectPulledAboveLeftCorrelator,
            RexInputRef nullIndicator,
            Set<Integer> isCount) {
      this.projectPulledAboveLeftCorrelator =
              projectPulledAboveLeftCorrelator;
      this.nullIndicator = nullIndicator; // may be null
      this.isCount = ImmutableSet.copyOf(isCount);
      this.rexBuilder = rexBuilder;
      this.typeFactory = rexBuilder.getTypeFactory();
    }

    private RexNode createCaseExpression(
            RexInputRef nullInputRef,
            RexLiteral lit,
            RexNode rexNode) {
      RexNode[] caseOperands = new RexNode[3];

      // Construct a CASE expression to handle the null indicator.
      //
      // This also covers the case where a left correlated subquery
      // projects fields from outer relation. Since LOJ cannot produce
      // nulls on the LHS, the projection now need to make a nullable LHS
      // reference using a nullability indicator. If this this indicator
      // is null, it means the subquery does not produce any value. As a
      // result, any RHS ref by this usbquery needs to produce null value.

      // WHEN indicator IS NULL
      caseOperands[0] =
              rexBuilder.makeCall(
                      SqlStdOperatorTable.IS_NULL,
                      new RexInputRef(
                              nullInputRef.getIndex(),
                              typeFactory.createTypeWithNullability(
                                      nullInputRef.getType(),
                                      true)));

      // THEN CAST(NULL AS newInputTypeNullable)
      caseOperands[1] =
              rexBuilder.makeCast(
                      typeFactory.createTypeWithNullability(
                              rexNode.getType(),
                              true),
                      lit);

      // ELSE cast (newInput AS newInputTypeNullable) END
      caseOperands[2] =
              rexBuilder.makeCast(
                      typeFactory.createTypeWithNullability(
                              rexNode.getType(),
                              true),
                      rexNode);

      return rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              caseOperands);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (cm.mapFieldAccessToCorRef.containsKey(fieldAccess)) {
        // if it is a corVar, change it to be input ref.
        CorRef corVar = cm.mapFieldAccessToCorRef.get(fieldAccess);

        // corVar offset should point to the leftInput of currentRel,
        // which is the Correlator.
        RexNode newRexNode =
                new RexInputRef(corVar.field, fieldAccess.getType());

        if (projectPulledAboveLeftCorrelator
                && (nullIndicator != null)) {
          // need to enforce nullability by applying an additional
          // cast operator over the transformed expression.
          newRexNode =
                  createCaseExpression(
                          nullIndicator,
                          rexBuilder.constantNull(),
                          newRexNode);
        }
        return newRexNode;
      }
      return fieldAccess;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (currentRel instanceof LogicalCorrelate) {
        // if this rel references corVar
        // and now it needs to be rewritten
        // it must have been pulled above the Correlator
        // replace the input ref to account for the LHS of the
        // Correlator
        final int leftInputFieldCount =
                ((LogicalCorrelate) currentRel).getLeft().getRowType()
                        .getFieldCount();
        RelDataType newType = inputRef.getType();

        if (projectPulledAboveLeftCorrelator) {
          newType =
                  typeFactory.createTypeWithNullability(newType, true);
        }

        int pos = inputRef.getIndex();
        RexInputRef newInputRef =
                new RexInputRef(leftInputFieldCount + pos, newType);

        if ((isCount != null) && isCount.contains(pos)) {
          return createCaseExpression(
                  newInputRef,
                  rexBuilder.makeExactLiteral(BigDecimal.ZERO),
                  newInputRef);
        } else {
          return newInputRef;
        }
      }
      return inputRef;
    }

    @Override public RexNode visitLiteral(RexLiteral literal) {
      // Use nullIndicator to decide whether to project null.
      // Do nothing if the literal is null.
      if (!RexUtil.isNull(literal)
              && projectPulledAboveLeftCorrelator
              && (nullIndicator != null)) {
        return createCaseExpression(
                nullIndicator,
                rexBuilder.constantNull(),
                literal);
      }
      return literal;
    }

    @Override public RexNode visitCall(final RexCall call) {
      RexNode newCall;

      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      if (update[0]) {
        SqlOperator operator = call.getOperator();

        boolean isSpecialCast = false;
        if (operator instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) operator;
          if (function.getKind() == SqlKind.CAST) {
            if (call.operands.size() < 2) {
              isSpecialCast = true;
            }
          }
        }

        final RelDataType newType;
        if (!isSpecialCast) {
          // TODO: ideally this only needs to be called if the result
          // type will also change. However, since that requires
          // support from type inference rules to tell whether a rule
          // decides return type based on input types, for now all
          // operators will be recreated with new type if any operand
          // changed, unless the operator has "built-in" type.
          newType = rexBuilder.deriveReturnType(operator, clonedOperands);
        } else {
          // Use the current return type when creating a new call, for
          // operators with return type built into the operator
          // definition, and with no type inference rules, such as
          // cast function with less than 2 operands.

          // TODO: Comments in RexShuttle.visitCall() mention other
          // types in this category. Need to resolve those together
          // and preferably in the base class RexShuttle.
          newType = call.getType();
        }
        newCall =
                rexBuilder.makeCall(
                        newType,
                        operator,
                        clonedOperands);
      } else {
        newCall = call;
      }

      if (projectPulledAboveLeftCorrelator && (nullIndicator != null)) {
        return createCaseExpression(
                nullIndicator,
                rexBuilder.constantNull(),
                newCall);
      }
      return newCall;
    }
  }

  /**
   * Rule to remove single_value rel. For cases like
   *
   * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key
   * AggRel single group</blockquote>
   */
  private final class RemoveSingleAggregateRule extends RelOptRule {
    public RemoveSingleAggregateRule() {
      super(
              operand(
                      LogicalAggregate.class,
                      operand(
                              LogicalProject.class,
                              operand(LogicalAggregate.class, any()))));
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalAggregate singleAggregate = call.rel(0);
      LogicalProject project = call.rel(1);
      LogicalAggregate aggregate = call.rel(2);

      // check singleAggRel is single_value agg
      if ((!singleAggregate.getGroupSet().isEmpty())
              || (singleAggregate.getAggCallList().size() != 1)
              || !(singleAggregate.getAggCallList().get(0).getAggregation()
              instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check projRel only projects one expression
      // check this project only projects one expression, i.e. scalar
      // subqueries.
      List<RexNode> projExprs = project.getProjects();
      if (projExprs.size() != 1) {
        return;
      }

      // check the input to projRel is an aggregate on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      // singleAggRel produces a nullable type, so create the new
      // projection that casts proj expr to a nullable type.
      final RelOptCluster cluster = project.getCluster();
      RelNode newProject =
              RelOptUtil.createProject(aggregate,
                      ImmutableList.of(
                              rexBuilder.makeCast(
                                      cluster.getTypeFactory().createTypeWithNullability(
                                              projExprs.get(0).getType(),
                                              true),
                                      projExprs.get(0))),
                      null);
      call.transformTo(newProject);
    }
  }

  /** Planner rule that removes correlations for scalar projects. */
  private final class RemoveCorrelationForScalarProjectRule extends RelOptRule {
    public RemoveCorrelationForScalarProjectRule() {
      super(
              operand(LogicalCorrelate.class,
                      operand(RelNode.class, any()),
                      operand(LogicalAggregate.class,
                              operand(LogicalProject.class,
                                      operand(RelNode.class, any())))));
    }

    public void onMatch(RelOptRuleCall call) {
      final LogicalCorrelate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final LogicalAggregate aggregate = call.rel(2);
      final LogicalProject project = call.rel(3);
      RelNode right = call.rel(4);
      final RelOptCluster cluster = correlate.getCluster();

      setCurrent(call.getPlanner().getRoot(), correlate);

      // Check for this pattern.
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation.
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalAggregate (groupby (0) single_value())
      //     LogicalProject-A (may reference coVar)
      //       RightInputRel
      final JoinRelType joinType = correlate.getJoinType().toJoinType();

      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is of the following type:
      // doing a single_value() on the entire input
      if ((!aggregate.getGroupSet().isEmpty())
              || (aggregate.getAggCallList().size() != 1)
              || !(aggregate.getAggCallList().get(0).getAggregation()
              instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check this project only projects one expression, i.e. scalar
      // subqueries.
      if (project.getProjects().size() != 1) {
        return;
      }

      int nullIndicatorPos;

      if ((right instanceof LogicalFilter)
              && cm.mapRefRelToCorRef.containsKey(right)) {
        // rightInputRel has this shape:
        //
        //       LogicalFilter (references corvar)
        //         FilterInputRel

        // If rightInputRel is a filter and contains correlated
        // reference, make sure the correlated keys in the filter
        // condition forms a unique key of the RHS.

        LogicalFilter filter = (LogicalFilter) right;
        right = filter.getInput();

        assert right instanceof HepRelVertex;
        right = ((HepRelVertex) right).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // extract the correlation out of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys(from the original
        // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
        // can be expressions, while rightJoinKeys need to be input
        // refs. These comparisons are AND'ed together.
        List<RexNode> tmpRightJoinKeys = Lists.newArrayList();
        List<RexNode> correlatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
                filter,
                tmpRightJoinKeys,
                correlatedJoinKeys,
                false);

        // check that the columns referenced in these comparisons form
        // an unique key of the filterInputRel
        final List<RexInputRef> rightJoinKeys = new ArrayList<>();
        for (RexNode key : tmpRightJoinKeys) {
          assert key instanceof RexInputRef;
          rightJoinKeys.add((RexInputRef) key);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInputRel
        if (rightJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, right,
                rightJoinKeys)) {
          //SQL2REL_LOGGER.fine(rightJoinKeys.toString()
           //       + "are not unique keys for "
            //      + right.toString());
          return;
        }

        RexUtil.FieldAccessFinder visitor =
                new RexUtil.FieldAccessFinder();
        RexUtil.apply(visitor, correlatedJoinKeys, null);
        List<RexFieldAccess> correlatedKeyList =
                visitor.getFieldAccessList();

        if (!checkCorVars(correlate, project, filter, correlatedKeyList)) {
          return;
        }

        // Change the plan to this structure.
        // Note that the aggregateRel is removed.
        //
        // LogicalProject-A' (replace corvar to input ref from the LogicalJoin)
        //   LogicalJoin (replace corvar to input ref from LeftInputRel)
        //     LeftInputRel
        //     RightInputRel(oreviously FilterInputRel)

        // Change the filter condition into a join condition
        joinCond =
                removeCorrelationExpr(filter.getCondition(), false);

        nullIndicatorPos =
                left.getRowType().getFieldCount()
                        + rightJoinKeys.get(0).getIndex();
      } else if (cm.mapRefRelToCorRef.containsKey(project)) {
        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        if (!checkCorVars(correlate, project, null, null)) {
          return;
        }

        // Change the plan to this structure.
        //
        // LogicalProject-A' (replace corvar to input ref from LogicalJoin)
        //   LogicalJoin (left, condition = true)
        //     LeftInputRel
        //     LogicalAggregate(groupby(0), single_value(0), s_v(1)....)
        //       LogicalProject-B (everything from input plus literal true)
        //         ProjInputRel

        // make the new projRel to provide a null indicator
        right =
                createProjectWithAdditionalExprs(right,
                        ImmutableList.of(
                                Pair.<RexNode, String>of(
                                        rexBuilder.makeLiteral(true), "nullIndicator")));

        // make the new aggRel
        right =
                RelOptUtil.createSingleValueAggRel(cluster, right);

        // The last field:
        //     single_value(true)
        // is the nullIndicator
        nullIndicatorPos =
                left.getRowType().getFieldCount()
                        + right.getRowType().getFieldCount() - 1;
      } else {
        return;
      }

      // make the new join rel
      LogicalJoin join =
              LogicalJoin.create(left, right, joinCond,
                      ImmutableSet.<CorrelationId>of(), joinType);

      RelNode newProject =
              projectJoinOutputWithNullability(join, project, nullIndicatorPos);

      call.transformTo(newProject);

      removeCorVarFromTree(correlate);
    }
  }

  /** Planner rule that removes correlations for scalar aggregates. */
  private final class RemoveCorrelationForScalarAggregateRule
          extends RelOptRule {
    public RemoveCorrelationForScalarAggregateRule() {
      super(
              operand(LogicalCorrelate.class,
                      operand(RelNode.class, any()),
                      operand(LogicalProject.class,
                              operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE,
                                      operand(LogicalProject.class,
                                              operand(RelNode.class, any()))))));
    }

    public void onMatch(RelOptRuleCall call) {
      final LogicalCorrelate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final LogicalProject aggOutputProject = call.rel(2);
      final LogicalAggregate aggregate = call.rel(3);
      final LogicalProject aggInputProject = call.rel(4);
      RelNode right = call.rel(5);
      final RelOptCluster cluster = correlate.getCluster();

      setCurrent(call.getPlanner().getRoot(), correlate);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalProject-A (a RexNode)
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)
      //       LogicalProject-B (references coVar)
      //         rightInputRel

      // check aggOutputProject projects only one expression
      final List<RexNode> aggOutputProjects = aggOutputProject.getProjects();
      if (aggOutputProjects.size() != 1) {
        return;
      }

      final JoinRelType joinType = correlate.getJoinType().toJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      final List<RexNode> aggInputProjects = aggInputProject.getProjects();

      final List<AggregateCall> aggCalls = aggregate.getAggCallList();
      final Set<Integer> isCountStar = Sets.newHashSet();

      // mark if agg produces count(*) which needs to reference the
      // nullIndicator after the transformation.
      int k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
                && (aggCall.getArgList().size() == 0)) {
          isCountStar.add(k);
        }
      }

      if ((right instanceof LogicalFilter)
              && cm.mapRefRelToCorRef.containsKey(right)) {
        // rightInputRel has this shape:
        //
        //       LogicalFilter (references corvar)
        //         FilterInputRel
        LogicalFilter filter = (LogicalFilter) right;
        right = filter.getInput();

        assert right instanceof HepRelVertex;
        right = ((HepRelVertex) right).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // check filter condition type First extract the correlation out
        // of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys(from the original
        // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
        // can only be RexFieldAccess, while rightJoinKeys can be
        // expressions. These comparisons are AND'ed together.
        List<RexNode> rightJoinKeys = Lists.newArrayList();
        List<RexNode> tmpCorrelatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
                filter,
                rightJoinKeys,
                tmpCorrelatedJoinKeys,
                true);

        // make sure the correlated reference forms a unique key check
        // that the columns referenced in these comparisons form an
        // unique key of the leftInputRel
        List<RexFieldAccess> correlatedJoinKeys = Lists.newArrayList();
        List<RexInputRef> correlatedInputRefJoinKeys = Lists.newArrayList();
        for (RexNode joinKey : tmpCorrelatedJoinKeys) {
          assert joinKey instanceof RexFieldAccess;
          correlatedJoinKeys.add((RexFieldAccess) joinKey);
          RexNode correlatedInputRef =
                  removeCorrelationExpr(joinKey, false);
          assert correlatedInputRef instanceof RexInputRef;
          correlatedInputRefJoinKeys.add(
                  (RexInputRef) correlatedInputRef);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInputRel
        if (correlatedInputRefJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, left,
                correlatedInputRefJoinKeys)) {
          //SQL2REL_LOGGER.fine(correlatedJoinKeys.toString()
           //       + "are not unique keys for "
            //      + left.toString());
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(correlate,
                aggInputProject,
                filter,
                correlatedJoinKeys)) {
          return;
        }

        // Rewrite the above plan:
        //
        // CorrelateRel(left correlation, condition = true)
        //   LeftInputRel
        //   LogicalProject-A (a RexNode)
        //     LogicalAggregate (groupby(0), agg0(),agg1()...)
        //       LogicalProject-B (may reference coVar)
        //         LogicalFilter (references corVar)
        //           RightInputRel (no correlated reference)
        //

        // to this plan:
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     LogicalProject-B' (rewriten original projected exprs)
        //       LogicalJoin(replace corvar w/ input ref from LeftInputRel)
        //         LeftInputRel
        //         RightInputRel
        //

        // In the case where agg is count(*) or count($corVar), it is
        // changed to count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     LogicalProject-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       LogicalJoin(replace corvar to input ref from LeftInputRel)
        //         LeftInputRel
        //         LogicalProject (everything from RightInputRel plus
        //                     the nullIndicator "true")
        //           RightInputRel
        //

        // first change the filter condition into a join condition
        joinCond =
                removeCorrelationExpr(filter.getCondition(), false);
      } else if (cm.mapRefRelToCorRef.containsKey(aggInputProject)) {
        // check rightInputRel contains no correlation
        if (RelOptUtil.getVariablesUsed(right).size() > 0) {
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(correlate, aggInputProject, null, null)) {
          return;
        }

        int nFields = left.getRowType().getFieldCount();
        ImmutableBitSet allCols = ImmutableBitSet.range(nFields);

        // leftInputRel contains unique keys
        // i.e. each row is distinct and can group by on all the left
        // fields
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!RelMdUtil.areColumnsDefinitelyUnique(mq, left, allCols)) {
          //SQL2REL_LOGGER.fine("There are no unique keys for " + left);
          return;
        }
        //
        // Rewrite the above plan:
        //
        // CorrelateRel(left correlation, condition = true)
        //   LeftInputRel
        //   LogicalProject-A (a RexNode)
        //     LogicalAggregate (groupby(0), agg0(), agg1()...)
        //       LogicalProject-B (references coVar)
        //         RightInputRel (no correlated reference)
        //

        // to this plan:
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     LogicalProject-B' (rewriten original projected exprs)
        //       LogicalJoin (LOJ cond = true)
        //         LeftInputRel
        //         RightInputRel
        //

        // In the case where agg is count($corVar), it is changed to
        // count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     LogicalProject-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       LogicalJoin(replace corvar to input ref from LeftInputRel)
        //         LeftInputRel
        //         LogicalProject (everything from RightInputRel plus
        //                     the nullIndicator "true")
        //           RightInputRel
      } else {
        return;
      }

      RelDataType leftInputFieldType = left.getRowType();
      int leftInputFieldCount = leftInputFieldType.getFieldCount();
      int joinOutputProjExprCount =
              leftInputFieldCount + aggInputProjects.size() + 1;

      right =
              createProjectWithAdditionalExprs(right,
                      ImmutableList.of(
                              Pair.<RexNode, String>of(rexBuilder.makeLiteral(true),
                                      "nullIndicator")));

      LogicalJoin join =
              LogicalJoin.create(left, right, joinCond,
                      ImmutableSet.<CorrelationId>of(), joinType);

      // To the consumer of joinOutputProjRel, nullIndicator is located
      // at the end
      int nullIndicatorPos = join.getRowType().getFieldCount() - 1;

      RexInputRef nullIndicator =
              new RexInputRef(
                      nullIndicatorPos,
                      cluster.getTypeFactory().createTypeWithNullability(
                              join.getRowType().getFieldList()
                                      .get(nullIndicatorPos).getType(),
                              true));

      // first project all group-by keys plus the transformed agg input
      List<RexNode> joinOutputProjects = Lists.newArrayList();

      // LOJ Join preserves LHS types
      for (int i = 0; i < leftInputFieldCount; i++) {
        joinOutputProjects.add(
                rexBuilder.makeInputRef(
                        leftInputFieldType.getFieldList().get(i).getType(), i));
      }

      for (RexNode aggInputProjExpr : aggInputProjects) {
        joinOutputProjects.add(
                removeCorrelationExpr(aggInputProjExpr,
                        joinType.generatesNullsOnRight(),
                        nullIndicator));
      }

      joinOutputProjects.add(
              rexBuilder.makeInputRef(join, nullIndicatorPos));

      RelNode joinOutputProject =
              RelOptUtil.createProject(
                      join,
                      joinOutputProjects,
                      null);

      // nullIndicator is now at a different location in the output of
      // the join
      nullIndicatorPos = joinOutputProjExprCount - 1;

      final int groupCount = leftInputFieldCount;

      List<AggregateCall> newAggCalls = Lists.newArrayList();
      k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        final List<Integer> argList;

        if (isCountStar.contains(k)) {
          // this is a count(*), transform it to count(nullIndicator)
          // the null indicator is located at the end
          argList = Collections.singletonList(nullIndicatorPos);
        } else {
          argList = Lists.newArrayList();

          for (int aggArg : aggCall.getArgList()) {
            argList.add(aggArg + groupCount);
          }
        }

        int filterArg = aggCall.filterArg < 0 ? aggCall.filterArg
                : aggCall.filterArg + groupCount;
        newAggCalls.add(
                aggCall.adaptTo(joinOutputProject, argList, filterArg,
                        aggregate.getGroupCount(), groupCount));
      }

      ImmutableBitSet groupSet =
              ImmutableBitSet.range(groupCount);
      LogicalAggregate newAggregate =
              LogicalAggregate.create(joinOutputProject,
                      false,
                      groupSet,
                      null,
                      newAggCalls);

      List<RexNode> newAggOutputProjectList = Lists.newArrayList();
      for (int i : groupSet) {
        newAggOutputProjectList.add(
                rexBuilder.makeInputRef(newAggregate, i));
      }

      RexNode newAggOutputProjects =
              removeCorrelationExpr(aggOutputProjects.get(0), false);
      newAggOutputProjectList.add(
              rexBuilder.makeCast(
                      cluster.getTypeFactory().createTypeWithNullability(
                              newAggOutputProjects.getType(),
                              true),
                      newAggOutputProjects));

      RelNode newAggOutputProject =
              RelOptUtil.createProject(
                      newAggregate,
                      newAggOutputProjectList,
                      null);

      call.transformTo(newAggOutputProject);

      removeCorVarFromTree(correlate);
    }
  }

  // REVIEW jhyde 29-Oct-2007: This rule is non-static, depends on the state
  // of members in RelDecorrelator, and has side-effects in the decorrelator.
  // This breaks the contract of a planner rule, and the rule will not be
  // reusable in other planners.

  // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
  // the flavor attribute into the description?

  /** Planner rule that adjusts projects when counts are added. */
  private final class AdjustProjectForCountAggregateRule extends RelOptRule {
    final boolean flavor;

    public AdjustProjectForCountAggregateRule(boolean flavor) {
      super(
              flavor
                      ? operand(LogicalCorrelate.class,
                      operand(RelNode.class, any()),
                      operand(LogicalProject.class,
                              operand(LogicalAggregate.class, any())))
                      : operand(LogicalCorrelate.class,
                      operand(RelNode.class, any()),
                      operand(LogicalAggregate.class, any())));
      this.flavor = flavor;
    }

    public void onMatch(RelOptRuleCall call) {
      final LogicalCorrelate correlate = call.rel(0);
      final RelNode left = call.rel(1);
      final LogicalProject aggOutputProject;
      final LogicalAggregate aggregate;
      if (flavor) {
        aggOutputProject = call.rel(2);
        aggregate = call.rel(3);
      } else {
        aggregate = call.rel(2);

        // Create identity projection
        final List<Pair<RexNode, String>> projects = Lists.newArrayList();
        final List<RelDataTypeField> fields =
                aggregate.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
          projects.add(RexInputRef.of2(projects.size(), fields));
        }
        aggOutputProject =
                (LogicalProject) RelOptUtil.createProject(
                        aggregate,
                        projects,
                        false);
      }
      onMatch2(call, correlate, left, aggOutputProject, aggregate);
    }

    private void onMatch2(
            RelOptRuleCall call,
            LogicalCorrelate correlate,
            RelNode leftInput,
            LogicalProject aggOutputProject,
            LogicalAggregate aggregate) {
      if (generatedCorRels.contains(correlate)) {
        // This correlator was generated by a previous invocation of
        // this rule. No further work to do.
        return;
      }

      setCurrent(call.getPlanner().getRoot(), correlate);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalProject-A (a RexNode)
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)

      // check aggOutputProj projects only one expression
      List<RexNode> aggOutputProjExprs = aggOutputProject.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = correlate.getJoinType().toJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
              || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggregate.getGroupSet().isEmpty()) {
        return;
      }

      List<AggregateCall> aggCalls = aggregate.getAggCallList();
      Set<Integer> isCount = Sets.newHashSet();

      // remember the count() positions
      int i = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++i;
        if (aggCall.getAggregation() instanceof SqlCountAggFunction) {
          isCount.add(i);
        }
      }

      // now rewrite the plan to
      //
      // Project-A' (all LHS plus transformed original projections,
      //             replacing references to count() with case statement)
      //   Correlator(left correlation, condition = true)
      //     LeftInputRel
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)
      //
      LogicalCorrelate newCorrelate =
              LogicalCorrelate.create(leftInput, aggregate,
                      correlate.getCorrelationId(), correlate.getRequiredColumns(),
                      correlate.getJoinType());

      // remember this rel so we don't fire rule on it again
      // REVIEW jhyde 29-Oct-2007: rules should not save state; rule
      // should recognize patterns where it does or does not need to do
      // work
      generatedCorRels.add(newCorrelate);

      // need to update the mapCorVarToCorRel Update the output position
      // for the cor vars: only pass on the cor vars that are not used in
      // the join key.
      if (cm.mapCorToCorRel.get(correlate.getCorrelationId()) == correlate) {
        cm.mapCorToCorRel.put(correlate.getCorrelationId(), newCorrelate);
      }

      RelNode newOutput =
              aggregateCorrelatorOutput(newCorrelate, aggOutputProject, isCount);

      call.transformTo(newOutput);
    }
  }

  /**
   * A unique reference to a correlation field.
   *
   * <p>For instance, if a RelNode references emp.name multiple times, it would
   * result in multiple {@code CorRef} objects that differ just in
   * {@link CorRef#uniqueKey}.
   */
  static class CorRef implements Comparable<CorRef> {
    public final int uniqueKey;
    public final CorrelationId corr;
    public final int field;

    CorRef(CorrelationId corr, int field, int uniqueKey) {
      this.corr = corr;
      this.field = field;
      this.uniqueKey = uniqueKey;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(uniqueKey, corr, field);
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof CorRef
          && uniqueKey == ((CorRef) o).uniqueKey
          && corr == ((CorRef) o).corr
          && field == ((CorRef) o).field;
    }

    public int compareTo(@Nonnull CorRef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      c = Integer.compare(field, o.field);
      if (c != 0) {
        return c;
      }
      return Integer.compare(uniqueKey, o.uniqueKey);
    }

    public CorDef def() {
      return new CorDef(corr, field);
    }
  }

  /** A correlation and a field. */
  static class CorDef implements Comparable<CorDef> {
    public final CorrelationId corr;
    public final int field;
    private SqlKind predicateKind;

    CorDef(CorrelationId corr, int field) {
      this.corr = corr;
      this.field = field;
      this.predicateKind = null;
    }

    @Override public String toString() {
      return corr.getName() + '.' + field;
    }

    @Override public int hashCode() {
      return Objects.hash(corr, field);
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof CorDef
          && corr == ((CorDef) o).corr
          && field == ((CorDef) o).field;
    }

    public int compareTo(@Nonnull CorDef o) {
      int c = corr.compareTo(o.corr);
      if (c != 0) {
        return c;
      }
      return Integer.compare(field, o.field);
    }
    public SqlKind getPredicateKind() {
      return predicateKind;
    }
    public void setPredicateKind(SqlKind predKind) {
      this.predicateKind = predKind;

    }
  }

  /** A map of the locations of
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate}
   * in a tree of {@link RelNode}s.
   *
   * <p>It is used to drive the decorrelation process.
   * Treat it as immutable; rebuild if you modify the tree.
   *
   * <p>There are three maps:<ol>
   *
   * <li>{@link #mapRefRelToCorRef} maps a {@link RelNode} to the correlated
   * variables it references;
   *
   * <li>{@link #mapCorToCorRel} maps a correlated variable to the
   * {@link Correlate} providing it;
   *
   * <li>{@link #mapFieldAccessToCorRef} maps a rex field access to
   * the corVar it represents. Because typeFlattener does not clone or
   * modify a correlated field access this map does not need to be
   * updated.
   *
   * </ol> */
  private static class CorelMap {
    private final Multimap<RelNode, CorRef> mapRefRelToCorRef;
    private final SortedMap<CorrelationId, RelNode> mapCorToCorRel;
    private final Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef;

    // TODO: create immutable copies of all maps
    private CorelMap(Multimap<RelNode, CorRef> mapRefRelToCorRef,
        SortedMap<CorrelationId, RelNode> mapCorToCorRel,
        Map<RexFieldAccess, CorRef> mapFieldAccessToCorRef) {
      this.mapRefRelToCorRef = mapRefRelToCorRef;
      this.mapCorToCorRel = mapCorToCorRel;
      this.mapFieldAccessToCorRef = ImmutableMap.copyOf(mapFieldAccessToCorRef);
    }

    @Override public String toString() {
      return "mapRefRelToCorRef=" + mapRefRelToCorRef
          + "\nmapCorToCorRel=" + mapCorToCorRel
          + "\nmapFieldAccessToCorRef=" + mapFieldAccessToCorRef
          + "\n";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof CorelMap
          && mapRefRelToCorRef.equals(((CorelMap) obj).mapRefRelToCorRef)
          && mapCorToCorRel.equals(((CorelMap) obj).mapCorToCorRel)
          && mapFieldAccessToCorRef.equals(
              ((CorelMap) obj).mapFieldAccessToCorRef);
    }

    @Override public int hashCode() {
      return Objects.hash(mapRefRelToCorRef, mapCorToCorRel,
          mapFieldAccessToCorRef);
    }

    /** Creates a CorelMap with given contents. */
    public static CorelMap of(
        SortedSetMultimap<RelNode, CorRef> mapRefRelToCorVar,
        SortedMap<CorrelationId, RelNode> mapCorToCorRel,
        Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar) {
      return new CorelMap(mapRefRelToCorVar, mapCorToCorRel,
          mapFieldAccessToCorVar);
    }

    /**
     * Returns whether there are any correlating variables in this statement.
     *
     * @return whether there are any correlating variables
     */
    public boolean hasCorrelation() {
      return !mapCorToCorRel.isEmpty();
    }
  }

  private static class findIfValueGenRequired extends HiveRelShuttleImpl {
    private boolean mightRequireValueGen ;
    findIfValueGenRequired() { this.mightRequireValueGen = true; }

    private boolean hasRexOver(List<RexNode> projects) {
      for(RexNode expr : projects) {
        if(expr instanceof  RexOver) {
          return true;
        }
      }
      return false;
    }
    @Override public RelNode visit(HiveJoin rel) {
      mightRequireValueGen = true;
      return rel;
    }
    public RelNode visit(HiveSortLimit rel) {
      mightRequireValueGen = true;
      return rel;
    }
    public RelNode visit(HiveUnion rel) {
      mightRequireValueGen = true;
      return rel;
    }
    public RelNode visit(LogicalUnion rel) {
      mightRequireValueGen = true;
      return rel;
    }
    public RelNode visit(LogicalIntersect rel) {
      mightRequireValueGen = true;
      return rel;
    }

    public RelNode visit(HiveIntersect rel) {
      mightRequireValueGen = true;
      return rel;
    }

    @Override public RelNode visit(LogicalJoin rel) {
      mightRequireValueGen = true;
      return rel;
    }
    @Override public RelNode visit(HiveProject rel) {
      if(!(hasRexOver(((HiveProject)rel).getProjects()))) {
        mightRequireValueGen = false;
        return super.visit(rel);
      }
      else {
        mightRequireValueGen = true;
        return rel;
      }
    }
    @Override public RelNode visit(LogicalProject rel) {
      if(!(hasRexOver(((LogicalProject)rel).getProjects()))) {
        mightRequireValueGen = false;
        return super.visit(rel);
      }
      else {
        mightRequireValueGen = true;
        return rel;
      }
    }
    @Override public RelNode visit(HiveAggregate rel) {
      // if there are aggregate functions or grouping sets we will need
      // value generator
      if((((HiveAggregate)rel).getAggCallList().isEmpty() == true
          && ((HiveAggregate)rel).indicator == false)) {
        this.mightRequireValueGen = false;
        return super.visit(rel);
      }
      else {
        // need to reset to true in case previous aggregate/project
        // has set it to false
        this.mightRequireValueGen = true;
        return rel;
      }
    }
    @Override public RelNode visit(LogicalAggregate rel) {
      if((((LogicalAggregate)rel).getAggCallList().isEmpty() == true
          && ((LogicalAggregate)rel).indicator == false)) {
        this.mightRequireValueGen = false;
        return super.visit(rel);
      }
      else {
        // need to reset to true in case previous aggregate/project
        // has set it to false
        this.mightRequireValueGen = true;
        return rel;
      }
    }
    @Override public RelNode visit(LogicalCorrelate rel) {
      // this means we are hitting nested subquery so don't
      // need to go further
      return rel;
    }

    public boolean traverse(RelNode root) {
      root.accept(this);
      return mightRequireValueGen;
    }
  }
  /** Builds a {@link org.apache.calcite.sql2rel.RelDecorrelator.CorelMap}. */
  private static class CorelMapBuilder extends HiveRelShuttleImpl {
    final SortedMap<CorrelationId, RelNode> mapCorToCorRel =
        new TreeMap<>();

    final SortedSetMultimap<RelNode, CorRef> mapRefRelToCorRef =
        Multimaps.newSortedSetMultimap(
            new HashMap<RelNode, Collection<CorRef>>(),
            new Supplier<TreeSet<CorRef>>() {
              public TreeSet<CorRef> get() {
                Bug.upgrade("use MultimapBuilder when we're on Guava-16");
                return Sets.newTreeSet();
              }
            });

    final Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar = new HashMap<>();

    final Holder<Integer> offset = Holder.of(0);
    int corrIdGenerator = 0;

    final List<RelNode> stack = new ArrayList<>();

    /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
    CorelMap build(RelNode rel) {
      stripHep(rel).accept(this);
      return new CorelMap(mapRefRelToCorRef, mapCorToCorRel,
              mapFieldAccessToCorVar);
    }

    @Override public RelNode visit(LogicalJoin join) {
      try {
        Stacks.push(stack, join);
        join.getCondition().accept(rexVisitor(join));
      } finally {
        Stacks.pop(stack, join);
      }
      return visitJoin(join);
    }

    public RelNode visit(HiveJoin join) {
      try {
        Stacks.push(stack, join);
        join.getCondition().accept(rexVisitor(join));
      } finally {
        Stacks.pop(stack, join);
      }
      return visitJoin(join);
    }

    @Override protected RelNode visitChild(RelNode parent, int i,
                                           RelNode input) {
      return super.visitChild(parent, i, stripHep(input));
    }

    @Override public RelNode visit(LogicalCorrelate correlate) {
      mapCorToCorRel.put(correlate.getCorrelationId(), correlate);
      return visitJoin(correlate);
    }

    private RelNode visitJoin(BiRel join) {
      final int x = offset.get();
      visitChild(join, 0, join.getLeft());
      offset.set(x + join.getLeft().getRowType().getFieldCount());
      visitChild(join, 1, join.getRight());
      offset.set(x);
      return join;
    }

    public RelNode visit(final HiveProject project) {
      try {
        Stacks.push(stack, project);
        for (RexNode node : project.getProjects()) {
          node.accept(rexVisitor(project));
        }
      } finally {
        Stacks.pop(stack, project);
      }
      return super.visit(project);
    }
    public RelNode visit(final HiveFilter filter) {
      try {
        Stacks.push(stack, filter);
        filter.getCondition().accept(rexVisitor(filter));
      } finally {
        Stacks.pop(stack, filter);
      }
      return super.visit(filter);
    }
    @Override public RelNode visit(final LogicalFilter filter) {
      try {
        Stacks.push(stack, filter);
        filter.getCondition().accept(rexVisitor(filter));
      } finally {
        Stacks.pop(stack, filter);
      }
      return super.visit(filter);
    }

    @Override public RelNode visit(LogicalProject project) {
      try {
        Stacks.push(stack, project);
        for (RexNode node : project.getProjects()) {
          node.accept(rexVisitor(project));
        }
      } finally {
        Stacks.pop(stack, project);
      }
      return super.visit(project);
    }

    private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
      return new RexVisitorImpl<Void>(true) {
        @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
          final RexNode ref = fieldAccess.getReferenceExpr();
          if (ref instanceof RexCorrelVariable) {
            final RexCorrelVariable var = (RexCorrelVariable) ref;
            if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
              //for cases where different Rel nodes are referring to
              // same correlation var (e.g. in case of NOT IN)
              // avoid generating another correlation var
              // and record the 'rel' is using the same correlation
              mapRefRelToCorRef.put(rel,
                  mapFieldAccessToCorVar.get(fieldAccess));
            } else {
              final CorRef correlation =
                  new CorRef(var.id, fieldAccess.getField().getIndex(),
                      corrIdGenerator++);
              mapFieldAccessToCorVar.put(fieldAccess, correlation);
              mapRefRelToCorRef.put(rel, correlation);
            }
          }
          return super.visitFieldAccess(fieldAccess);
        }

        @Override public Void visitSubQuery(RexSubQuery subQuery) {
          subQuery.rel.accept(CorelMapBuilder.this);
          return super.visitSubQuery(subQuery);
        }
      };
    }
  }

  /** Frame describing the relational expression after decorrelation
   * and where to find the output fields and correlation variables
   * among its output fields. */
  static class Frame {
    final RelNode r;
    final ImmutableSortedMap<CorDef, Integer> corDefOutputs;
    final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

    Frame(RelNode oldRel, RelNode r, SortedMap<CorDef, Integer> corDefOutputs,
          Map<Integer, Integer> oldToNewOutputs) {
      this.r = Preconditions.checkNotNull(r);
      this.corDefOutputs = ImmutableSortedMap.copyOf(corDefOutputs);
      this.oldToNewOutputs = ImmutableSortedMap.copyOf(oldToNewOutputs);
      assert allLessThan(corDefOutputs.values(),
              r.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(oldToNewOutputs.keySet(),
              oldRel.getRowType().getFieldCount(), Litmus.THROW);
      assert allLessThan(oldToNewOutputs.values(),
              r.getRowType().getFieldCount(), Litmus.THROW);
    }
  }
}

// End RelDecorrelator.java
