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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


//TODO: Move this to calcite
public class HiveRelMdPredicates implements MetadataHandler<BuiltInMetadata.Predicates> {

  public static final RelMetadataProvider SOURCE =
          ChainedRelMetadataProvider.of(
                  ImmutableList.of(
                          ReflectiveRelMetadataProvider.reflectiveSource(
                                  BuiltInMethod.PREDICATES.method, new HiveRelMdPredicates()),
                          RelMdPredicates.SOURCE));

  private static final List<RexNode> EMPTY_LIST = ImmutableList.of();

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdPredicates() {}

  //~ Methods ----------------------------------------------------------------

  @Override
  public MetadataDef<BuiltInMetadata.Predicates> getDef() {
    return BuiltInMetadata.Predicates.DEF;
  }

  /**
   * Infers predicates for a project.
   *
   * <ol>
   * <li>create a mapping from input to projection. Map only positions that
   * directly reference an input column.
   * <li>Expressions that only contain above columns are retained in the
   * Project's pullExpressions list.
   * <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e'
   * is not in the projection list.
   *
   * <pre>
   * childPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
   * projectionExprs:       {a, b, c, e / 2}
   * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
   * </pre>
   *
   * </ol>
   */
  public RelOptPredicateList getPredicates(Project project, RelMetadataQuery mq) {

    RelNode child = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelOptPredicateList childInfo = mq.getPulledUpPredicates(child);

    List<RexNode> projectPullUpPredicates = new ArrayList<RexNode>();
    HashMultimap<Integer, Integer> inpIndxToOutIndxMap = HashMultimap.create();

    ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION, child.getRowType().getFieldCount(),
        project.getRowType().getFieldCount());

    for (Ord<RexNode> o : Ord.zip(project.getProjects())) {
      if (o.e instanceof RexInputRef) {
        int sIdx = ((RexInputRef) o.e).getIndex();
        m.set(sIdx, o.i);
        inpIndxToOutIndxMap.put(sIdx, o.i);
        columnsMappedBuilder.set(sIdx);
      }
    }

    // Go over childPullUpPredicates. If a predicate only contains columns in
    // 'columnsMapped' construct a new predicate based on mapping.
    final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
    for (RexNode r : childInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (columnsMapped.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, child));
        projectPullUpPredicates.add(r);
      }
    }

    // Project can also generate constants. We need to include them.
    for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
      if (RexLiteral.isNullLiteral(expr.e)) {
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(project, expr.i)));
      } else if (expr.e instanceof RexLiteral) {
        final RexLiteral literal = (RexLiteral) expr.e;
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(project, expr.i), literal));
      } else if (expr.e instanceof RexCall && HiveCalciteUtil.isDeterministicFuncOnLiterals(expr.e)) {
      //TODO: Move this to calcite
        projectPullUpPredicates.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(project, expr.i), expr.e));
      }
    }
    return RelOptPredicateList.of(projectPullUpPredicates);
  }

  /** Infers predicates for a {@link org.apache.calcite.rel.core.Join}. */
  public RelOptPredicateList getPredicates(Join join, RelMetadataQuery mq) {
    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode left = join.getInput(0);
    RelNode right = join.getInput(1);

    final RelOptPredicateList leftInfo = mq.getPulledUpPredicates(left);
    final RelOptPredicateList rightInfo = mq.getPulledUpPredicates(right);

    JoinConditionBasedPredicateInference jI =
        new JoinConditionBasedPredicateInference(join,
            RexUtil.composeConjunction(rB, leftInfo.pulledUpPredicates, false),
            RexUtil.composeConjunction(rB, rightInfo.pulledUpPredicates,
                false));

    return jI.inferPredicates(false);
  }

  /**
   * Infers predicates for an Aggregate.
   *
   * <p>Pulls up predicates that only contains references to columns in the
   * GroupSet. For e.g.
   *
   * <pre>
   * inputPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
   * groupSet         : { a, b}
   * pulledUpExprs    : { a &gt; 7}
   * </pre>
   */
  public RelOptPredicateList getPredicates(Aggregate agg, RelMetadataQuery mq) {
    final RelNode input = agg.getInput();
    final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);
    final List<RexNode> aggPullUpPredicates = new ArrayList<>();

    ImmutableBitSet groupKeys = agg.getGroupSet();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
        input.getRowType().getFieldCount(), agg.getRowType().getFieldCount());

    int i = 0;
    for (int j : groupKeys) {
      m.set(j, i++);
    }

    for (RexNode r : inputInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (!rCols.isEmpty() && groupKeys.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, input));
        aggPullUpPredicates.add(r);
      }
    }
    return RelOptPredicateList.of(aggPullUpPredicates);
  }

  /**
   * Infers predicates for a Union.
   */
  public RelOptPredicateList getPredicates(Union union, RelMetadataQuery mq) {
    RexBuilder rB = union.getCluster().getRexBuilder();

    Map<String, RexNode> finalPreds = new HashMap<>();
    List<RexNode> finalResidualPreds = new ArrayList<>();
    for (int i = 0; i < union.getInputs().size(); i++) {
      RelNode input = union.getInputs().get(i);
      RelOptPredicateList info = mq.getPulledUpPredicates(input);
      if (info.pulledUpPredicates.isEmpty()) {
        return RelOptPredicateList.EMPTY;
      }
      Map<String, RexNode> preds = new HashMap<>();
      List<RexNode> residualPreds = new ArrayList<>();
      for (RexNode pred : info.pulledUpPredicates) {
        final String predString = pred.toString();
        if (i == 0) {
          preds.put(predString, pred);
          continue;
        }
        if (finalPreds.containsKey(predString)) {
          preds.put(predString, pred);
        } else {
          residualPreds.add(pred);
        }
      }
      // Add new residual preds
      finalResidualPreds.add(RexUtil.composeConjunction(rB, residualPreds, false));
      // Add those that are not part of the final set to residual
      for (Entry<String, RexNode> e : finalPreds.entrySet()) {
        if (!preds.containsKey(e.getKey())) {
          // This node was in previous union inputs, but it is not in this one
          for (int j = 0; j < i; j++) {
            finalResidualPreds.set(j, RexUtil.composeConjunction(rB, Lists.newArrayList(
                    finalResidualPreds.get(j), e.getValue()), false));
          }
        }
      }
      // Final preds
      finalPreds = preds;
    }

    List<RexNode> preds = new ArrayList<>(finalPreds.values());
    RexNode disjPred = RexUtil.composeDisjunction(rB, finalResidualPreds, false);
    if (!disjPred.isAlwaysTrue()) {
      preds.add(disjPred);
    }
    return RelOptPredicateList.of(preds);
  }

  /**
   * Utility to infer predicates from one side of the join that apply on the
   * other side.
   *
   * <p>Contract is:<ul>
   *
   * <li>initialize with a {@link org.apache.calcite.rel.core.Join} and
   * optional predicates applicable on its left and right subtrees.
   *
   * <li>you can
   * then ask it for equivalentPredicate(s) given a predicate.
   *
   * </ul>
   *
   * <p>So for:
   * <ol>
   * <li>'<code>R1(x) join R2(y) on x = y</code>' a call for
   * equivalentPredicates on '<code>x &gt; 7</code>' will return '
   * <code>[y &gt; 7]</code>'
   * <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for
   * equivalentPredicates on the second join '<code>x &gt; 7</code>' will return
   * </ol>
   */
  static class JoinConditionBasedPredicateInference {
    final Join joinRel;
    final boolean isSemiJoin;
    final int nSysFields;
    final int nFieldsLeft;
    final int nFieldsRight;
    final ImmutableBitSet leftFieldsBitSet;
    final ImmutableBitSet rightFieldsBitSet;
    final ImmutableBitSet allFieldsBitSet;
    SortedMap<Integer, BitSet> equivalence;
    final Map<String, ImmutableBitSet> exprFields;
    final Set<String> allExprsDigests;
    final Set<String> equalityPredicates;
    final RexNode leftChildPredicates;
    final RexNode rightChildPredicates;

    public JoinConditionBasedPredicateInference(Join joinRel,
            RexNode lPreds, RexNode rPreds) {
      this(joinRel, joinRel instanceof SemiJoin, lPreds, rPreds);
    }

    private JoinConditionBasedPredicateInference(Join joinRel, boolean isSemiJoin,
        RexNode lPreds, RexNode rPreds) {
      super();
      this.joinRel = joinRel;
      this.isSemiJoin = isSemiJoin;
      nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
      nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
      nSysFields = joinRel.getSystemFieldList().size();
      leftFieldsBitSet = ImmutableBitSet.range(nSysFields,
          nSysFields + nFieldsLeft);
      rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
          nSysFields + nFieldsLeft + nFieldsRight);
      allFieldsBitSet = ImmutableBitSet.range(0,
          nSysFields + nFieldsLeft + nFieldsRight);

      exprFields = Maps.newHashMap();
      allExprsDigests = new HashSet<>();

      if (lPreds == null) {
        leftChildPredicates = null;
      } else {
        Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft);
        leftChildPredicates = lPreds.accept(
            new RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0)));

        for (RexNode r : RelOptUtil.conjunctions(leftChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprsDigests.add(r.toString());
        }
      }
      if (rPreds == null) {
        rightChildPredicates = null;
      } else {
        Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft, 0, nFieldsRight);
        rightChildPredicates = rPreds.accept(
            new RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1)));

        for (RexNode r : RelOptUtil.conjunctions(rightChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprsDigests.add(r.toString());
        }
      }

      equivalence = Maps.newTreeMap();
      equalityPredicates = new HashSet<>();
      for (int i = 0; i < nSysFields + nFieldsLeft + nFieldsRight; i++) {
        equivalence.put(i, BitSets.of(i));
      }

      // Only process equivalences found in the join conditions. Processing
      // Equivalences from the left or right side infer predicates that are
      // already present in the Tree below the join.
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      List<RexNode> exprs =
          RelOptUtil.conjunctions(
              compose(rexBuilder, ImmutableList.of(joinRel.getCondition())));

      final EquivalenceFinder eF = new EquivalenceFinder();
      new ArrayList<>(
          Lists.transform(exprs,
              new Function<RexNode, Void>() {
                public Void apply(RexNode input) {
                  return input.accept(eF);
                }
              }));

      equivalence = BitSets.closure(equivalence);
    }

    /**
     * The PullUp Strategy is sound but not complete.
     * <ol>
     * <li>We only pullUp inferred predicates for now. Pulling up existing
     * predicates causes an explosion of duplicates. The existing predicates are
     * pushed back down as new predicates. Once we have rules to eliminate
     * duplicate Filter conditions, we should pullUp all predicates.
     * <li>For Left Outer: we infer new predicates from the left and set them as
     * applicable on the Right side. No predicates are pulledUp.
     * <li>Right Outer Joins are handled in an analogous manner.
     * <li>For Full Outer Joins no predicates are pulledUp or inferred.
     * </ol>
     */
    public RelOptPredicateList inferPredicates(
        boolean includeEqualityInference) {
      final List<RexNode> inferredPredicates = new ArrayList<>();
      final List<RexNode> nonFieldsPredicates = new ArrayList<>();
      final Set<String> allExprsDigests = new HashSet<>(this.allExprsDigests);
      final JoinRelType joinType = joinRel.getJoinType();
      final List<RexNode> leftPreds = ImmutableList.copyOf(RelOptUtil.conjunctions(leftChildPredicates));
      final List<RexNode> rightPreds = ImmutableList.copyOf(RelOptUtil.conjunctions(rightChildPredicates));
      switch (joinType) {
      case INNER:
      case LEFT:
        infer(leftPreds, allExprsDigests, inferredPredicates,
            nonFieldsPredicates, includeEqualityInference,
            joinType == JoinRelType.LEFT ? rightFieldsBitSet
                : allFieldsBitSet);
        break;
      }
      switch (joinType) {
      case INNER:
      case RIGHT:
        infer(rightPreds, allExprsDigests, inferredPredicates,
            nonFieldsPredicates, includeEqualityInference,
            joinType == JoinRelType.RIGHT ? leftFieldsBitSet
                : allFieldsBitSet);
        break;
      }

      Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft + nFieldsRight,
          0, nSysFields + nFieldsLeft, nFieldsRight);
      final RexPermuteInputsShuttle rightPermute =
          new RexPermuteInputsShuttle(rightMapping, joinRel);
      Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft);
      final RexPermuteInputsShuttle leftPermute =
          new RexPermuteInputsShuttle(leftMapping, joinRel);
      final List<RexNode> leftInferredPredicates = new ArrayList<>();
      final List<RexNode> rightInferredPredicates = new ArrayList<>();

      for (RexNode iP : inferredPredicates) {
        ImmutableBitSet iPBitSet = RelOptUtil.InputFinder.bits(iP);
        if (leftFieldsBitSet.contains(iPBitSet)) {
          leftInferredPredicates.add(iP.accept(leftPermute));
        } else if (rightFieldsBitSet.contains(iPBitSet)) {
          rightInferredPredicates.add(iP.accept(rightPermute));
        }
      }

      if (joinType == JoinRelType.INNER && !nonFieldsPredicates.isEmpty()) {
        // Predicates without field references can be pushed to both inputs
        final Set<String> leftPredsSet = new HashSet<String>(
                Lists.transform(leftPreds, HiveCalciteUtil.REX_STR_FN));
        final Set<String> rightPredsSet = new HashSet<String>(
                Lists.transform(rightPreds, HiveCalciteUtil.REX_STR_FN));
        for (RexNode iP : nonFieldsPredicates) {
          if (!leftPredsSet.contains(iP.toString())) {
            leftInferredPredicates.add(iP);
          }
          if (!rightPredsSet.contains(iP.toString())) {
            rightInferredPredicates.add(iP);
          }
        }
      }

      switch (joinType) {
      case INNER:
        Iterable<RexNode> pulledUpPredicates;
        if (isSemiJoin) {
          pulledUpPredicates = Iterables.concat(leftPreds, leftInferredPredicates);
        } else {
          pulledUpPredicates = Iterables.concat(leftPreds, rightPreds,
                RelOptUtil.conjunctions(joinRel.getCondition()), inferredPredicates);
        }
        return RelOptPredicateList.of(
          pulledUpPredicates, leftInferredPredicates, rightInferredPredicates);
      case LEFT:    
        return RelOptPredicateList.of(    
          leftPreds, EMPTY_LIST, rightInferredPredicates);
      case RIGHT:   
        return RelOptPredicateList.of(    
          rightPreds, leftInferredPredicates, EMPTY_LIST);
      default:
        assert inferredPredicates.size() == 0;
        return RelOptPredicateList.EMPTY;
      }
    }

    public RexNode left() {
      return leftChildPredicates;
    }

    public RexNode right() {
      return rightChildPredicates;
    }

    private void infer(List<RexNode> predicates, Set<String> allExprsDigests,
        List<RexNode> inferedPredicates, List<RexNode> nonFieldsPredicates,
        boolean includeEqualityInference, ImmutableBitSet inferringFields) {
      for (RexNode r : predicates) {
        if (!includeEqualityInference
            && equalityPredicates.contains(r.toString())) {
          continue;
        }
        Iterable<Mapping> ms = mappings(r);
        if (ms.iterator().hasNext()) {
          for (Mapping m : ms) {
            RexNode tr = r.accept(
                new RexPermuteInputsShuttle(m, joinRel.getInput(0),
                    joinRel.getInput(1)));
            if (inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
                && !allExprsDigests.contains(tr.toString())
                && !isAlwaysTrue(tr)) {
              inferedPredicates.add(tr);
              allExprsDigests.add(tr.toString());
            }
          }
        } else {
          if (!isAlwaysTrue(r)) {
            nonFieldsPredicates.add(r);
          }
        }
      }
    }

    Iterable<Mapping> mappings(final RexNode predicate) {
      return new Iterable<Mapping>() {
        public Iterator<Mapping> iterator() {
          ImmutableBitSet fields = exprFields.get(predicate.toString());
          if (fields.cardinality() == 0) {
            return Iterators.emptyIterator();
          }
          return new ExprsItr(fields);
        }
      };
    }

    private void equivalent(int p1, int p2) {
      BitSet b = equivalence.get(p1);
      b.set(p2);

      b = equivalence.get(p2);
      b.set(p1);
    }

    RexNode compose(RexBuilder rexBuilder, Iterable<RexNode> exprs) {
      exprs = Linq4j.asEnumerable(exprs).where(new Predicate1<RexNode>() {
        public boolean apply(RexNode expr) {
          return expr != null;
        }
      });
      return RexUtil.composeConjunction(rexBuilder, exprs, false);
    }

    /**
     * Find expressions of the form 'col_x = col_y'.
     */
    class EquivalenceFinder extends RexVisitorImpl<Void> {
      protected EquivalenceFinder() {
        super(true);
      }

      @Override public Void visitCall(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(call.getOperands().get(0));
          int rPos = pos(call.getOperands().get(1));
          if (lPos != -1 && rPos != -1) {
            JoinConditionBasedPredicateInference.this.equivalent(lPos, rPos);
            JoinConditionBasedPredicateInference.this.equalityPredicates
                .add(call.toString());
          }
        }
        return null;
      }
    }

    /**
     * Given an expression returns all the possible substitutions.
     *
     * <p>For example, for an expression 'a + b + c' and the following
     * equivalences: <pre>
     * a : {a, b}
     * b : {a, b}
     * c : {c, e}
     * </pre>
     *
     * <p>The following Mappings will be returned:
     * <pre>
     * {a &rarr; a, b &rarr; a, c &rarr; c}
     * {a &rarr; a, b &rarr; a, c &rarr; e}
     * {a &rarr; a, b &rarr; b, c &rarr; c}
     * {a &rarr; a, b &rarr; b, c &rarr; e}
     * {a &rarr; b, b &rarr; a, c &rarr; c}
     * {a &rarr; b, b &rarr; a, c &rarr; e}
     * {a &rarr; b, b &rarr; b, c &rarr; c}
     * {a &rarr; b, b &rarr; b, c &rarr; e}
     * </pre>
     *
     * <p>which imply the following inferences:
     * <pre>
     * a + a + c
     * a + a + e
     * a + b + c
     * a + b + e
     * b + a + c
     * b + a + e
     * b + b + c
     * b + b + e
     * </pre>
     */
    class ExprsItr implements Iterator<Mapping> {
      final int[] columns;
      final BitSet[] columnSets;
      final int[] iterationIdx;
      Mapping nextMapping;
      boolean firstCall;

      ExprsItr(ImmutableBitSet fields) {
        nextMapping = null;
        columns = new int[fields.cardinality()];
        columnSets = new BitSet[fields.cardinality()];
        iterationIdx = new int[fields.cardinality()];
        for (int j = 0, i = fields.nextSetBit(0); i >= 0; i = fields
            .nextSetBit(i + 1), j++) {
          columns[j] = i;
          columnSets[j] = equivalence.get(i);
          iterationIdx[j] = 0;
        }
        firstCall = true;
      }

      public boolean hasNext() {
        if (firstCall) {
          initializeMapping();
          firstCall = false;
        } else {
          computeNextMapping(iterationIdx.length - 1);
        }
        return nextMapping != null;
      }

      public Mapping next() {
        return nextMapping;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      private void computeNextMapping(int level) {
        int t = columnSets[level].nextSetBit(iterationIdx[level]);
        if (t < 0) {
          if (level == 0) {
            nextMapping = null;
          } else {
            iterationIdx[level] = 0;
            computeNextMapping(level - 1);
          }
        } else {
          nextMapping.set(columns[level], t);
          iterationIdx[level] = t + 1;
        }
      }

      private void initializeMapping() {
        nextMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft + nFieldsRight);
        for (int i = 0; i < columnSets.length; i++) {
          BitSet c = columnSets[i];
          int t = c.nextSetBit(iterationIdx[i]);
          if (t < 0) {
            nextMapping = null;
            return;
          }
          nextMapping.set(columns[i], t);
          iterationIdx[i] = t + 1;
        }
      }
    }

    private int pos(RexNode expr) {
      if (expr instanceof RexInputRef) {
        return ((RexInputRef) expr).getIndex();
      }
      return -1;
    }

    private boolean isAlwaysTrue(RexNode predicate) {
      if (predicate instanceof RexCall) {
        RexCall c = (RexCall) predicate;
        if (c.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(c.getOperands().get(0));
          int rPos = pos(c.getOperands().get(1));
          return lPos != -1 && lPos == rPos;
        }
      }
      return predicate.isAlwaysTrue();
    }
  }

}
