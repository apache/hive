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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import static org.apache.calcite.rex.RexUtil.andNot;
import static org.apache.calcite.rex.RexUtil.removeAll;
import static org.apache.calcite.rex.RexUtil.simplify;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 * <p>The call {@code new SubstitutionVisitor(target, query).go(replacement))}
 * will return {@code query} with every occurrence of {@code target} replaced
 * by {@code replacement}.</p>
 *
 * <p>The following example shows how {@code SubstitutionVisitor} can be used
 * for materialized view recognition.</p>
 *
 * <ul>
 * <li>query = SELECT a, c FROM t WHERE x = 5 AND b = 4</li>
 * <li>target = SELECT a, b, c FROM t WHERE x = 5</li>
 * <li>replacement = SELECT * FROM mv</li>
 * <li>result = SELECT a, c FROM mv WHERE b = 4</li>
 * </ul>
 *
 * <p>Note that {@code result} uses the materialized view table {@code mv} and a
 * simplified condition {@code b = 4}.</p>
 *
 * <p>Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.</p>
 *
 * <p>The inputs must only include the core relational operators:
 * {@link org.apache.calcite.rel.logical.LogicalTableScan},
 * {@link org.apache.calcite.rel.logical.LogicalFilter},
 * {@link org.apache.calcite.rel.logical.LogicalProject},
 * {@link org.apache.calcite.rel.logical.LogicalJoin},
 * {@link org.apache.calcite.rel.logical.LogicalUnion},
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.</p>
 *
 * TODO: Remove when we upgrade to Calcite version using builders.
 */
public class SubstitutionVisitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubstitutionVisitor.class);

  /** Equivalence that compares objects by their {@link Object#toString()}
   * method. */
  private static final Equivalence<Object> STRING_EQUIVALENCE =
      new Equivalence<Object>() {
        @Override protected boolean doEquivalent(Object o, Object o2) {
          return o.toString().equals(o2.toString());
        }

        @Override protected int doHash(Object o) {
          return o.toString().hashCode();
        }
      };

  /** Equivalence that compares {@link Lists}s by the
   * {@link Object#toString()} of their elements. */
  @SuppressWarnings("unchecked")
  private static final Equivalence<List<?>> PAIRWISE_STRING_EQUIVALENCE =
      (Equivalence) STRING_EQUIVALENCE.pairwise();

  protected static final ImmutableList<UnifyRule> DEFAULT_RULES =
      ImmutableList.<UnifyRule>of(
          TrivialRule.INSTANCE,
          ScanToProjectUnifyRule.INSTANCE,
          ProjectToProjectUnifyRule.INSTANCE,
          FilterToProjectUnifyRule.INSTANCE,
//          ProjectToFilterUnifyRule.INSTANCE,
//          FilterToFilterUnifyRule.INSTANCE,
          AggregateToAggregateUnifyRule.INSTANCE,
          AggregateOnProjectToAggregateUnifyRule.INSTANCE);

  /**
   * Factory for a builder for relational expressions.
   * <p>The actual builder is available via {@link RelOptRuleCall#builder()}.
   */
  protected final RelBuilder relBuilder;

  private final ImmutableList<UnifyRule> rules;
  private final Map<Pair<Class, Class>, List<UnifyRule>> ruleMap =
      new HashMap<>();
  private final RelOptCluster cluster;
  private final Holder query;
  private final MutableRel target;

  /**
   * Nodes in {@link #target} that have no children.
   */
  final List<MutableRel> targetLeaves;

  /**
   * Nodes in {@link #query} that have no children.
   */
  final List<MutableRel> queryLeaves;

  final Map<MutableRel, MutableRel> replacementMap = new HashMap<>();

  final Multimap<MutableRel, MutableRel> equivalents =
      LinkedHashMultimap.create();

  /** Workspace while rule is being matched.
   * Careful, re-entrant!
   * Assumes no rule needs more than 2 slots. */
  protected final MutableRel[] slots = new MutableRel[2];

  /** Creates a SubstitutionVisitor with the default rule set. */
  public SubstitutionVisitor(RelNode target_, RelNode query_) {
    this(target_, query_, DEFAULT_RULES);
  }

  public SubstitutionVisitor(RelNode target_, RelNode query_,
          ImmutableList<UnifyRule> rules) {
    this(target_, query_, rules, RelFactories.LOGICAL_BUILDER);
  }

  /** Creates a SubstitutionVisitor. */
  public SubstitutionVisitor(RelNode target_, RelNode query_,
      ImmutableList<UnifyRule> rules, RelBuilderFactory relBuilderFactory) {
    this.cluster = target_.getCluster();
    this.rules = rules;
    this.query = Holder.of(toMutable(query_));
    this.target = toMutable(target_);
    this.relBuilder = relBuilderFactory.create(cluster, null);
    final Set<MutableRel> parents = Sets.newIdentityHashSet();
    final List<MutableRel> allNodes = new ArrayList<>();
    final MutableRelVisitor visitor =
        new MutableRelVisitor() {
          public void visit(MutableRel node) {
            parents.add(node.parent);
            allNodes.add(node);
            super.visit(node);
          }
        };
    visitor.go(target);

    // Populate the list of leaves in the tree under "target".
    // Leaves are all nodes that are not parents.
    // For determinism, it is important that the list is in scan order.
    allNodes.removeAll(parents);
    targetLeaves = ImmutableList.copyOf(allNodes);

    allNodes.clear();
    parents.clear();
    visitor.go(query);
    allNodes.removeAll(parents);
    queryLeaves = ImmutableList.copyOf(allNodes);
  }

  private static MutableRel toMutable(RelNode rel) {
    if (rel instanceof TableScan) {
      return MutableScan.of((TableScan) rel);
    }
    if (rel instanceof Values) {
      return MutableValues.of((Values) rel);
    }
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final MutableRel input = toMutable(project.getInput());
      return MutableProject.of(input, project.getProjects(),
          project.getRowType().getFieldNames());
    }
    if (rel instanceof Filter) {
      final Filter filter = (Filter) rel;
      final MutableRel input = toMutable(filter.getInput());
      return MutableFilter.of(input, filter.getCondition());
    }
    if (rel instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) rel;
      final MutableRel input = toMutable(aggregate.getInput());
      return MutableAggregate.of(input, aggregate.indicator,
          aggregate.getGroupSet(), aggregate.getGroupSets(),
          aggregate.getAggCallList());
    }
    if (rel instanceof Join) {
      final Join join = (Join) rel;
      final MutableRel left = toMutable(join.getLeft());
      final MutableRel right = toMutable(join.getRight());
      return MutableJoin.of(join.getCluster(), left, right,
          join.getCondition(), join.getJoinType(), join.getVariablesSet());
    }
    if (rel instanceof Sort) {
      final Sort sort = (Sort) rel;
      final MutableRel input = toMutable(sort.getInput());
      return MutableSort.of(input, sort.getCollation(), sort.offset, sort.fetch);
    }
    throw new RuntimeException("cannot translate " + rel + " to MutableRel");
  }

  void register(MutableRel result, MutableRel query) {
  }

  /**
   * Maps a condition onto a target.
   *
   * <p>If condition is stronger than target, returns the residue.
   * If it is equal to target, returns the expression that evaluates to
   * the constant {@code true}. If it is weaker than target, returns
   * {@code null}.</p>
   *
   * <p>The terms satisfy the relation</p>
   *
   * <pre>
   *     {@code condition = target AND residue}
   * </pre>
   *
   * <p>and {@code residue} must be as weak as possible.</p>
   *
   * <p>Example #1: condition stronger than target</p>
   * <ul>
   * <li>condition: x = 1 AND y = 2</li>
   * <li>target: x = 1</li>
   * <li>residue: y = 2</li>
   * </ul>
   *
   * <p>Note that residue {@code x &gt; 0 AND y = 2} would also satisfy the
   * relation {@code condition = target AND residue} but is stronger than
   * necessary, so we prefer {@code y = 2}.</p>
   *
   * <p>Example #2: target weaker than condition (valid, but not currently
   * implemented)</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 OR z = 3</li>
   * <li>residue: NOT (z = 3)</li>
   * </ul>
   *
   * <p>Example #3: condition and target are equivalent</p>
   * <ul>
   * <li>condition: x = 1 AND y = 2</li>
   * <li>target: y = 2 AND x = 1</li>
   * <li>residue: TRUE</li>
   * </ul>
   *
   * <p>Example #4: condition weaker than target</p>
   * <ul>
   * <li>condition: x = 1</li>
   * <li>target: x = 1 AND y = 2</li>
   * <li>residue: null (i.e. no match)</li>
   * </ul>
   *
   * <p>There are many other possible examples. It amounts to solving
   * whether {@code condition AND NOT target} can ever evaluate to
   * true, and therefore is a form of the NP-complete
   * <a href="http://en.wikipedia.org/wiki/Satisfiability">Satisfiability</a>
   * problem.</p>
   */
  @VisibleForTesting
  public static RexNode splitFilter(
      final RexBuilder rexBuilder, RexNode condition, RexNode target) {
    // First, try splitting into ORs.
    // Given target    c1 OR c2 OR c3 OR c4
    // and condition   c2 OR c4
    // residue is      NOT c1 AND NOT c3
    // Also deals with case target [x] condition [x] yields residue [true].
    RexNode z = splitOr(rexBuilder, condition, target);
    if (z != null) {
      return z;
    }

    RexNode x = andNot(rexBuilder, target, condition);
    if (mayBeSatisfiable(x)) {
      RexNode x2 = andNot(rexBuilder, condition, target);
      return simplify(rexBuilder, x2);
    }
    return null;
  }

  private static RexNode splitOr(
      final RexBuilder rexBuilder, RexNode condition, RexNode target) {
    List<RexNode> targets = RelOptUtil.disjunctions(target);
    for (RexNode e : RelOptUtil.disjunctions(condition)) {
      boolean found = removeAll(targets, e);
      if (!found) {
        return null;
      }
    }
    return RexUtil.composeConjunction(rexBuilder,
        Lists.transform(targets, RexUtil.notFn(rexBuilder)), false);
  }

  /**
   * Returns whether a boolean expression ever returns true.
   *
   * <p>This method may give false positives. For instance, it will say
   * that {@code x = 5 AND x > 10} is satisfiable, because at present it
   * cannot prove that it is not.</p>
   */
  public static boolean mayBeSatisfiable(RexNode e) {
    // Example:
    //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
    //  disjunctions: {x = 1, y = 2, z = 3}
    //  notDisjunctions: {x = 1 AND y = 2}
    final List<RexNode> disjunctions = new ArrayList<>();
    final List<RexNode> notDisjunctions = new ArrayList<>();
    RelOptUtil.decomposeConjunction(e, disjunctions, notDisjunctions);

    // If there is a single FALSE or NOT TRUE, the whole expression is
    // always false.
    for (RexNode disjunction : disjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (!RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    for (RexNode disjunction : notDisjunctions) {
      switch (disjunction.getKind()) {
      case LITERAL:
        if (RexLiteral.booleanValue(disjunction)) {
          return false;
        }
      }
    }
    // If one of the not-disjunctions is a disjunction that is wholly
    // contained in the disjunctions list, the expression is not
    // satisfiable.
    //
    // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
    // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
    // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
    for (RexNode notDisjunction : notDisjunctions) {
      final List<RexNode> disjunctions2 =
          RelOptUtil.conjunctions(notDisjunction);
      if (disjunctions.containsAll(disjunctions2)) {
        return false;
      }
    }
    return true;
  }

  public RelNode go0(RelNode replacement_) {
    assert false; // not called
    MutableRel replacement = toMutable(replacement_);
    assert MutableRels.equalType(
        "target", target, "replacement", replacement, Litmus.THROW);
    replacementMap.put(target, replacement);
    final UnifyResult unifyResult = matchRecurse(target);
    if (unifyResult == null) {
      return null;
    }
    final MutableRel node0 = unifyResult.result;
    MutableRel node = node0; // replaceAncestors(node0);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Convert: query:\n"
          + query.deep()
          + "\nunify.query:\n"
          + unifyResult.call.query.deep()
          + "\nunify.result:\n"
          + unifyResult.result.deep()
          + "\nunify.target:\n"
          + unifyResult.call.target.deep()
          + "\nnode0:\n"
          + node0.deep()
          + "\nnode:\n"
          + node.deep());
    }
    return fromMutable(node);
  }

  /**
   * Returns a list of all possible rels that result from substituting the
   * matched RelNode with the replacement RelNode within the query.
   *
   * <p>For example, the substitution result of A join B, while A and B
   * are both a qualified match for replacement R, is R join B, R join R,
   * A join R.
   */
  public List<RelNode> go(RelNode replacement_) {
    List<List<Replacement>> matches = go(toMutable(replacement_));
    if (matches.isEmpty()) {
      return ImmutableList.of();
    }
    List<RelNode> sub = Lists.newArrayList();
    sub.add(fromMutable(query.input));
    reverseSubstitute(query, matches, sub, 0, matches.size());
    return sub;
  }

  /**
   * Substitutes the query with replacement whenever possible but meanwhile
   * keeps track of all the substitutions and their original rel before
   * replacement, so that in later processing stage, the replacement can be
   * recovered individually to produce a list of all possible rels with
   * substitution in different places.
   */
  private List<List<Replacement>> go(MutableRel replacement) {
    assert MutableRels.equalType(
        "target", target, "replacement", replacement, Litmus.THROW);
    final List<MutableRel> queryDescendants = MutableRels.descendants(query);
    final List<MutableRel> targetDescendants = MutableRels.descendants(target);

    // Populate "equivalents" with (q, t) for each query descendant q and
    // target descendant t that are equal.
    final Map<MutableRel, MutableRel> map = Maps.newHashMap();
    for (MutableRel queryDescendant : queryDescendants) {
      map.put(queryDescendant, queryDescendant);
    }
    for (MutableRel targetDescendant : targetDescendants) {
      MutableRel queryDescendant = map.get(targetDescendant);
      if (queryDescendant != null) {
        assert queryDescendant.rowType.equals(targetDescendant.rowType);
        equivalents.put(queryDescendant, targetDescendant);
      }
    }
    map.clear();

    final List<Replacement> attempted = Lists.newArrayList();
    List<List<Replacement>> substitutions = Lists.newArrayList();

    for (;;) {
      int count = 0;
      MutableRel queryDescendant = query;
    outer:
      while (queryDescendant != null) {
        for (Replacement r : attempted) {
          if (queryDescendant == r.after) {
            // This node has been replaced by previous iterations in the
            // hope to match its ancestors, so the node itself should not
            // be matched again.
            queryDescendant = MutableRels.preOrderTraverseNext(queryDescendant);
            continue outer;
          }
        }
        final MutableRel next = MutableRels.preOrderTraverseNext(queryDescendant);
        final MutableRel childOrNext =
            queryDescendant.getInputs().isEmpty()
                ? next : queryDescendant.getInputs().get(0);
        for (MutableRel targetDescendant : targetDescendants) {
          for (UnifyRule rule
              : applicableRules(queryDescendant, targetDescendant)) {
            UnifyRuleCall call =
                rule.match(this, queryDescendant, targetDescendant);
            if (call != null) {
              final UnifyResult result = rule.apply(call);
              if (result != null) {
                ++count;
                attempted.add(new Replacement(result.call.query, result.result));
                MutableRel parent = result.call.query.replaceInParent(result.result);

                // Replace previous equivalents with new equivalents, higher up
                // the tree.
                for (int i = 0; i < rule.slotCount; i++) {
                  Collection<MutableRel> equi = equivalents.get(slots[i]);
                  if (!equi.isEmpty()) {
                    equivalents.remove(slots[i], equi.iterator().next());
                  }
                }
                assert result.result.rowType.equals(result.call.query.rowType)
                    : Pair.of(result.result, result.call.query);
                equivalents.put(result.result, result.call.query);
                if (targetDescendant == target) {
                  // A real substitution happens. We purge the attempted
                  // replacement list and add them into substitution list.
                  // Meanwhile we stop matching the descendants and jump
                  // to the next subtree in pre-order traversal.
                  if (!target.equals(replacement)) {
                    Replacement r = MutableRels.replace(
                        query.input, target, copyMutable(replacement));
                    assert r != null
                        : rule + "should have returned a result containing the target.";
                    attempted.add(r);
                  }
                  substitutions.add(ImmutableList.copyOf(attempted));
                  attempted.clear();
                  queryDescendant = next;
                  continue outer;
                }
                // We will try walking the query tree all over again to see
                // if there can be any substitutions after the replacement
                // attempt.
                break outer;
              }
            }
          }
        }
        queryDescendant = childOrNext;
      }
      // Quit the entire loop if:
      // 1) we have walked the entire query tree with one or more successful
      //    substitutions, thus count != 0 && attempted.isEmpty();
      // 2) we have walked the entire query tree but have made no replacement
      //    attempt, thus count == 0 && attempted.isEmpty();
      // 3) we had done some replacement attempt in a previous walk, but in
      //    this one we have not found any potential matches or substitutions,
      //    thus count == 0 && !attempted.isEmpty().
      if (count == 0 || attempted.isEmpty()) {
        break;
      }
    }
    if (!attempted.isEmpty()) {
      // We had done some replacement attempt in the previous walk, but that
      // did not lead to any substitutions in this walk, so we need to recover
      // the replacement.
      undoReplacement(attempted);
    }
    return substitutions;
  }

  /**
   * Represents a replacement action: before &rarr; after.
   */
  private static class Replacement {
    final MutableRel before;
    final MutableRel after;

    Replacement(MutableRel before, MutableRel after) {
      this.before = before;
      this.after = after;
    }
  }

  private static void undoReplacement(List<Replacement> replacement) {
    for (int i = replacement.size() - 1; i >= 0; i--) {
      Replacement r = replacement.get(i);
      r.after.replaceInParent(r.before);
    }
  }

  private static void redoReplacement(List<Replacement> replacement) {
    for (Replacement r : replacement) {
      r.before.replaceInParent(r.after);
    }
  }

  private void reverseSubstitute(Holder query,
      List<List<Replacement>> matches, List<RelNode> sub,
      int replaceCount, int maxCount) {
    if (matches.isEmpty()) {
      return;
    }
    final List<List<Replacement>> rem = matches.subList(1, matches.size());
    reverseSubstitute(query, rem, sub, replaceCount, maxCount);
    undoReplacement(matches.get(0));
    if (++replaceCount < maxCount) {
      sub.add(fromMutable(query.input));
    }
    reverseSubstitute(query, rem, sub, replaceCount, maxCount);
    redoReplacement(matches.get(0));
  }

  private List<RelNode> fromMutables(List<MutableRel> nodes) {
    return Lists.transform(nodes,
        new Function<MutableRel, RelNode>() {
          public RelNode apply(MutableRel mutableRel) {
            return fromMutable(mutableRel);
          }
        });
  }

  private RelNode fromMutable(MutableRel node) {
    switch (node.type) {
    case SCAN:
    case VALUES:
      return ((MutableLeafRel) node).rel;
    case PROJECT:
      final MutableProject project = (MutableProject) node;
      relBuilder.push(fromMutable(project.input));
      relBuilder.project(project.projects);
      return relBuilder.build();
    case FILTER:
      final MutableFilter filter = (MutableFilter) node;
      relBuilder.push(fromMutable(filter.input));
      relBuilder.filter(filter.condition);
      return relBuilder.build();
    case AGGREGATE:
      final MutableAggregate aggregate = (MutableAggregate) node;
      return LogicalAggregate.create(fromMutable(aggregate.input),
          aggregate.indicator, aggregate.groupSet, aggregate.groupSets,
          aggregate.aggCalls);
    case SORT:
      final MutableSort sort = (MutableSort) node;
      return LogicalSort.create(fromMutable(sort.input), sort.collation,
          sort.offset, sort.fetch);
    case UNION:
      final MutableUnion union = (MutableUnion) node;
      return LogicalUnion.create(fromMutables(union.inputs), union.all);
    case JOIN:
      final MutableJoin join = (MutableJoin) node;
      return LogicalJoin.create(fromMutable(join.getLeft()), fromMutable(join.getRight()),
          join.getCondition(), join.getVariablesSet(), join.getJoinType());
    default:
      throw new AssertionError(node.deep());
    }
  }

  private static List<MutableRel> copyMutables(List<MutableRel> nodes) {
    return Lists.transform(nodes,
        new Function<MutableRel, MutableRel>() {
          public MutableRel apply(MutableRel mutableRel) {
            return copyMutable(mutableRel);
          }
        });
  }

  private static MutableRel copyMutable(MutableRel node) {
    switch (node.type) {
    case SCAN:
      return MutableScan.of((TableScan) ((MutableScan) node).rel);
    case VALUES:
      return MutableValues.of((Values) ((MutableValues) node).rel);
    case PROJECT:
      final MutableProject project = (MutableProject) node;
      return MutableProject.of(project.rowType,
          copyMutable(project.input), project.projects);
    case FILTER:
      final MutableFilter filter = (MutableFilter) node;
      return MutableFilter.of(copyMutable(filter.input), filter.condition);
    case AGGREGATE:
      final MutableAggregate aggregate = (MutableAggregate) node;
      return MutableAggregate.of(copyMutable(aggregate.input),
          aggregate.indicator, aggregate.groupSet, aggregate.groupSets,
          aggregate.aggCalls);
    case SORT:
      final MutableSort sort = (MutableSort) node;
      return MutableSort.of(copyMutable(sort.input), sort.collation,
          sort.offset, sort.fetch);
    case UNION:
      final MutableUnion union = (MutableUnion) node;
      return MutableUnion.of(copyMutables(union.inputs), union.all);
    case JOIN:
      final MutableJoin join = (MutableJoin) node;
      return MutableJoin.of(join.cluster, copyMutable(join.getLeft()),
          copyMutable(join.getRight()), join.getCondition(), join.getJoinType(),
          join.getVariablesSet());
    default:
      throw new AssertionError(node.deep());
    }
  }

  private UnifyResult matchRecurse(MutableRel target) {
    assert false; // not called
    final List<MutableRel> targetInputs = target.getInputs();
    MutableRel queryParent = null;

    for (MutableRel targetInput : targetInputs) {
      UnifyResult unifyResult = matchRecurse(targetInput);
      if (unifyResult == null) {
        return null;
      }
      queryParent = unifyResult.call.query.replaceInParent(unifyResult.result);
    }

    if (targetInputs.isEmpty()) {
      for (MutableRel queryLeaf : queryLeaves) {
        for (UnifyRule rule : applicableRules(queryLeaf, target)) {
          final UnifyResult x = apply(rule, queryLeaf, target);
          if (x != null) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Rule: " + rule
                  + "\nQuery:\n"
                  + queryParent
                  + (x.call.query != queryParent
                     ? "\nQuery (original):\n"
                     + queryParent
                     : "")
                  + "\nTarget:\n"
                  + target.deep()
                  + "\nResult:\n"
                  + x.result.deep()
                  + "\n");
            }
            return x;
          }
        }
      }
    } else {
      assert queryParent != null;
      for (UnifyRule rule : applicableRules(queryParent, target)) {
        final UnifyResult x = apply(rule, queryParent, target);
        if (x != null) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Rule: " + rule
                + "\nQuery:\n"
                + queryParent.deep()
                + (x.call.query != queryParent
                   ? "\nQuery (original):\n"
                   + queryParent.toString()
                   : "")
                + "\nTarget:\n"
                + target.deep()
                + "\nResult:\n"
                + x.result.deep()
                + "\n");
          }
          return x;
        }
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Unify failed:"
          + "\nQuery:\n"
          + queryParent.toString()
          + "\nTarget:\n"
          + target.toString()
          + "\n");
    }
    return null;
  }

  private UnifyResult apply(UnifyRule rule, MutableRel query,
      MutableRel target) {
    final UnifyRuleCall call = new UnifyRuleCall(rule, query, target, null);
    return rule.apply(call);
  }

  private List<UnifyRule> applicableRules(MutableRel query,
      MutableRel target) {
    final Class queryClass = query.getClass();
    final Class targetClass = target.getClass();
    final Pair<Class, Class> key = Pair.of(queryClass, targetClass);
    List<UnifyRule> list = ruleMap.get(key);
    if (list == null) {
      final ImmutableList.Builder<UnifyRule> builder =
          ImmutableList.builder();
      for (UnifyRule rule : rules) {
        //noinspection unchecked
        if (mightMatch(rule, queryClass, targetClass)) {
          builder.add(rule);
        }
      }
      list = builder.build();
      ruleMap.put(key, list);
    }
    return list;
  }

  private static boolean mightMatch(UnifyRule rule,
      Class queryClass, Class targetClass) {
    return rule.queryOperand.clazz.isAssignableFrom(queryClass)
        && rule.targetOperand.clazz.isAssignableFrom(targetClass);
  }

  /** Exception thrown to exit a matcher. Not really an error. */
  protected static class MatchFailed extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final MatchFailed INSTANCE = new MatchFailed();
  }

  /** Rule that attempts to match a query relational expression
   * against a target relational expression.
   *
   * <p>The rule declares the query and target types; this allows the
   * engine to fire only a few rules in a given context.</p>
   */
  protected abstract static class UnifyRule {
    protected final int slotCount;
    protected final Operand queryOperand;
    protected final Operand targetOperand;

    protected UnifyRule(int slotCount, Operand queryOperand,
        Operand targetOperand) {
      this.slotCount = slotCount;
      this.queryOperand = queryOperand;
      this.targetOperand = targetOperand;
    }

    /**
     * <p>Applies this rule to a particular node in a query. The goal is
     * to convert {@code query} into {@code target}. Before the rule is
     * invoked, Calcite has made sure that query's children are equivalent
     * to target's children.
     *
     * <p>There are 3 possible outcomes:</p>
     *
     * <ul>
     *
     * <li>{@code query} already exactly matches {@code target}; returns
     * {@code target}</li>
     *
     * <li>{@code query} is sufficiently close to a match for
     * {@code target}; returns {@code target}</li>
     *
     * <li>{@code query} cannot be made to match {@code target}; returns
     * null</li>
     *
     * </ul>
     *
     * <p>REVIEW: Is possible that we match query PLUS one or more of its
     * ancestors?</p>
     *
     * @param call Input parameters
     */
    protected abstract UnifyResult apply(UnifyRuleCall call);

    protected UnifyRuleCall match(SubstitutionVisitor visitor, MutableRel query,
        MutableRel target) {
      if (queryOperand.matches(visitor, query)) {
        if (targetOperand.matches(visitor, target)) {
          return visitor.new UnifyRuleCall(this, query, target,
              copy(visitor.slots, slotCount));
        }
      }
      return null;
    }

    protected <E> ImmutableList<E> copy(E[] slots, int slotCount) {
      // Optimize if there are 0 or 1 slots.
      switch (slotCount) {
      case 0:
        return ImmutableList.of();
      case 1:
        return ImmutableList.of(slots[0]);
      default:
        return ImmutableList.copyOf(slots).subList(0, slotCount);
      }
    }
  }

  /**
   * Arguments to an application of a {@link UnifyRule}.
   */
  protected class UnifyRuleCall {
    protected final UnifyRule rule;
    public final MutableRel query;
    public final MutableRel target;
    protected final ImmutableList<MutableRel> slots;

    public UnifyRuleCall(UnifyRule rule, MutableRel query, MutableRel target,
        ImmutableList<MutableRel> slots) {
      this.rule = Preconditions.checkNotNull(rule);
      this.query = Preconditions.checkNotNull(query);
      this.target = Preconditions.checkNotNull(target);
      this.slots = Preconditions.checkNotNull(slots);
    }

    public UnifyResult result(MutableRel result) {
      assert MutableRels.contains(result, target);
      assert MutableRels.equalType("result", result, "query", query,
          Litmus.THROW);
      MutableRel replace = replacementMap.get(target);
      if (replace != null) {
        assert false; // replacementMap is always empty
        // result =
        MutableRels.replace(result, target, replace);
      }
      register(result, query);
      return new UnifyResult(this, result);
    }

    /**
     * Creates a {@link UnifyRuleCall} based on the parent of {@code query}.
     */
    public UnifyRuleCall create(MutableRel query) {
      return new UnifyRuleCall(rule, query, target, slots);
    }

    public RelOptCluster getCluster() {
      return cluster;
    }
  }

  /**
   * Result of an application of a {@link UnifyRule} indicating that the
   * rule successfully matched {@code query} against {@code target} and
   * generated a {@code result} that is equivalent to {@code query} and
   * contains {@code target}.
   */
  protected static class UnifyResult {
    private final UnifyRuleCall call;
    // equivalent to "query", contains "result"
    private final MutableRel result;

    UnifyResult(UnifyRuleCall call, MutableRel result) {
      this.call = call;
      assert MutableRels.equalType("query", call.query, "result", result,
          Litmus.THROW);
      this.result = result;
    }
  }

  /** Abstract base class for implementing {@link UnifyRule}. */
  protected abstract static class AbstractUnifyRule extends UnifyRule {
    public AbstractUnifyRule(Operand queryOperand, Operand targetOperand,
        int slotCount) {
      super(slotCount, queryOperand, targetOperand);
      //noinspection AssertWithSideEffects
      assert isValid();
    }

    protected boolean isValid() {
      final SlotCounter slotCounter = new SlotCounter();
      slotCounter.visit(queryOperand);
      assert slotCounter.queryCount == slotCount;
      assert slotCounter.targetCount == 0;
      slotCounter.queryCount = 0;
      slotCounter.visit(targetOperand);
      assert slotCounter.queryCount == 0;
      assert slotCounter.targetCount == slotCount;
      return true;
    }

    /** Creates an operand with given inputs. */
    protected static Operand operand(Class<? extends MutableRel> clazz,
        Operand... inputOperands) {
      return new InternalOperand(clazz, ImmutableList.copyOf(inputOperands));
    }

    /** Creates an operand that doesn't check inputs. */
    protected static Operand any(Class<? extends MutableRel> clazz) {
      return new AnyOperand(clazz);
    }

    /** Creates an operand that matches a relational expression in the query. */
    protected static Operand query(int ordinal) {
      return new QueryOperand(ordinal);
    }

    /** Creates an operand that matches a relational expression in the
     * target. */
    protected static Operand target(int ordinal) {
      return new TargetOperand(ordinal);
    }
  }

  /** Implementation of {@link UnifyRule} that matches if the query is already
   * equal to the target.
   *
   * <p>Matches scans to the same table, because these will be
   * {@link MutableScan}s with the same
   * {@link org.apache.calcite.rel.logical.LogicalTableScan} instance.</p>
   */
  private static class TrivialRule extends AbstractUnifyRule {
    private static final TrivialRule INSTANCE = new TrivialRule();

    private TrivialRule() {
      super(any(MutableRel.class), any(MutableRel.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      if (call.query.equals(call.target)) {
        return call.result(call.query);
      }
      return null;
    }
  }

  /** Implementation of {@link UnifyRule} that matches
   * {@link org.apache.calcite.rel.logical.LogicalTableScan}. */
  private static class ScanToProjectUnifyRule extends AbstractUnifyRule {
    public static final ScanToProjectUnifyRule INSTANCE =
        new ScanToProjectUnifyRule();

    private ScanToProjectUnifyRule() {
      super(any(MutableScan.class),
          any(MutableProject.class), 0);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableProject target = (MutableProject) call.target;
      final MutableScan query = (MutableScan) call.query;
      // We do not need to check query's parent type to avoid duplication
      // of ProjectToProjectUnifyRule or FilterToProjectUnifyRule, since
      // SubstitutionVisitor performs a top-down match.
      if (!query.equals(target.getInput())) {
        return null;
      }
      final RexShuttle shuttle = getRexShuttle(target);
      final RexBuilder rexBuilder = target.cluster.getRexBuilder();
      final List<RexNode> newProjects;
      try {
        newProjects = (List<RexNode>)
            shuttle.apply(rexBuilder.identityProjects(query.getRowType()));
      } catch (MatchFailed e) {
        return null;
      }
      final MutableProject newProject =
          MutableProject.of(
              query.getRowType(), target, newProjects);
      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }
  }

  /** Implementation of {@link UnifyRule} that matches
   * {@link org.apache.calcite.rel.logical.LogicalProject}. */
  private static class ProjectToProjectUnifyRule extends AbstractUnifyRule {
    public static final ProjectToProjectUnifyRule INSTANCE =
        new ProjectToProjectUnifyRule();

    private ProjectToProjectUnifyRule() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableProject target = (MutableProject) call.target;
      final MutableProject query = (MutableProject) call.query;
      final RexShuttle shuttle = getRexShuttle(target);
      final List<RexNode> newProjects;
      try {
        newProjects = shuttle.apply(query.getProjects());
      } catch (MatchFailed e) {
        return null;
      }
      final MutableProject newProject =
          MutableProject.of(
              query.getRowType(), target, newProjects);
      final MutableRel newProject2 = MutableRels.strip(newProject);
      return call.result(newProject2);
    }
  }


  /** Implementation of {@link UnifyRule} that matches a {@link MutableFilter}
   * to a {@link MutableProject}. */
  private static class FilterToProjectUnifyRule extends AbstractUnifyRule {
    public static final FilterToProjectUnifyRule INSTANCE =
        new FilterToProjectUnifyRule();

    private FilterToProjectUnifyRule() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableProject.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      // Child of projectTarget is equivalent to child of filterQuery.
      try {
        // TODO: make sure that constants are ok
        final MutableProject target = (MutableProject) call.target;
        final RexShuttle shuttle = getRexShuttle(target);
        final RexNode newCondition;
        final MutableFilter query = (MutableFilter) call.query;
        try {
          newCondition = query.getCondition().accept(shuttle);
        } catch (MatchFailed e) {
          return null;
        }
        final MutableFilter newFilter = MutableFilter.of(target, newCondition);
        if (query.parent instanceof MutableProject) {
          final MutableRel inverse =
              invert(((MutableProject) query.parent).getNamedProjects(),
                  newFilter, shuttle);
          return call.create(query.parent).result(inverse);
        } else {
          final MutableRel inverse = invert(query, newFilter, target);
          return call.result(inverse);
        }
      } catch (MatchFailed e) {
        return null;
      }
    }

    protected MutableRel invert(List<Pair<RexNode, String>> namedProjects,
        MutableRel input,
        RexShuttle shuttle) {
      LOGGER.trace("SubstitutionVisitor: invert:\nprojects: {}\ninput: {}\nproject: {}\n",
          namedProjects, input, shuttle);
      final List<RexNode> exprList = new ArrayList<>();
      final RexBuilder rexBuilder = input.cluster.getRexBuilder();
      final List<RexNode> projects = Pair.left(namedProjects);
      for (RexNode expr : projects) {
        exprList.add(rexBuilder.makeZeroLiteral(expr.getType()));
      }
      for (Ord<RexNode> expr : Ord.zip(projects)) {
        final RexNode node = expr.e.accept(shuttle);
        if (node == null) {
          throw MatchFailed.INSTANCE;
        }
        exprList.set(expr.i, node);
      }
      return MutableProject.of(input, exprList, Pair.right(namedProjects));
    }

    protected MutableRel invert(MutableRel model, MutableRel input,
        MutableProject project) {
      LOGGER.trace("SubstitutionVisitor: invert:\nmodel: {}\ninput: {}\nproject: {}\n",
          model, input, project);
      if (project.getProjects().size() < model.getRowType().getFieldCount()) {
        throw MatchFailed.INSTANCE;
      }
      final List<RexNode> exprList = new ArrayList<>();
      final RexBuilder rexBuilder = model.cluster.getRexBuilder();
      for (RelDataTypeField field : model.getRowType().getFieldList()) {
        exprList.add(rexBuilder.makeZeroLiteral(field.getType()));
      }
      for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
        if (expr.e instanceof RexInputRef) {
          final int target = ((RexInputRef) expr.e).getIndex();
          exprList.set(target,
              rexBuilder.ensureType(expr.e.getType(),
                  RexInputRef.of(expr.i, input.rowType),
                  false));
        } else {
          throw MatchFailed.INSTANCE;
        }
      }
      return MutableProject.of(model.rowType, input, exprList);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a
   * {@link MutableFilter}. */
  private static class FilterToFilterUnifyRule extends AbstractUnifyRule {
    public static final FilterToFilterUnifyRule INSTANCE =
        new FilterToFilterUnifyRule();

    private FilterToFilterUnifyRule() {
      super(operand(MutableFilter.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      // in.query can be rewritten in terms of in.target if its condition
      // is weaker. For example:
      //   query: SELECT * FROM t WHERE x = 1 AND y = 2
      //   target: SELECT * FROM t WHERE x = 1
      // transforms to
      //   result: SELECT * FROM (target) WHERE y = 2
      final MutableFilter query = (MutableFilter) call.query;
      final MutableFilter target = (MutableFilter) call.target;
      final MutableFilter newFilter =
          createFilter(query, target);
      if (newFilter == null) {
        return null;
      }
      return call.result(newFilter);
    }

    MutableFilter createFilter(MutableFilter query, MutableFilter target) {
      final RexNode newCondition =
          splitFilter(query.cluster.getRexBuilder(), query.getCondition(),
              target.getCondition());
      if (newCondition == null) {
        // Could not map query onto target.
        return null;
      }
      if (newCondition.isAlwaysTrue()) {
        return target;
      }
      return MutableFilter.of(target, newCondition);
    }
  }

  /** Implementation of {@link UnifyRule} that matches a {@link MutableProject}
   * to a {@link MutableFilter}. */
  private static class ProjectToFilterUnifyRule extends AbstractUnifyRule {
    public static final ProjectToFilterUnifyRule INSTANCE =
        new ProjectToFilterUnifyRule();

    private ProjectToFilterUnifyRule() {
      super(operand(MutableProject.class, query(0)),
          operand(MutableFilter.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      if (call.query.parent instanceof MutableFilter) {
        final UnifyRuleCall in2 = call.create(call.query.parent);
        final MutableFilter query = (MutableFilter) in2.query;
        final MutableFilter target = (MutableFilter) in2.target;
        final MutableFilter newFilter =
            FilterToFilterUnifyRule.INSTANCE.createFilter(
                query, target);
        if (newFilter == null) {
          return null;
        }
        return in2.result(query.replaceInParent(newFilter));
      }
      return null;
    }
  }

  /** Implementation of {@link UnifyRule} that matches a
   * {@link org.apache.calcite.rel.logical.LogicalAggregate} to a
   * {@link org.apache.calcite.rel.logical.LogicalAggregate}, provided
   * that they have the same child. */
  private static class AggregateToAggregateUnifyRule extends AbstractUnifyRule {
    public static final AggregateToAggregateUnifyRule INSTANCE =
        new AggregateToAggregateUnifyRule();

    private AggregateToAggregateUnifyRule() {
      super(operand(MutableAggregate.class, query(0)),
          operand(MutableAggregate.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableAggregate query = (MutableAggregate) call.query;
      final MutableAggregate target = (MutableAggregate) call.target;
      assert query != target;
      // in.query can be rewritten in terms of in.target if its groupSet is
      // a subset, and its aggCalls are a superset. For example:
      //   query: SELECT x, COUNT(b) FROM t GROUP BY x
      //   target: SELECT x, y, SUM(a) AS s, COUNT(b) AS cb FROM t GROUP BY x, y
      // transforms to
      //   result: SELECT x, SUM(cb) FROM (target) GROUP BY x
      if (!target.getGroupSet().contains(query.getGroupSet())) {
        return null;
      }
      MutableRel result = unifyAggregates(query, target);
      if (result == null) {
        return null;
      }
      return call.result(result);
    }
  }

  public static MutableAggregate permute(MutableAggregate aggregate,
      MutableRel input, Mapping mapping) {
    ImmutableBitSet groupSet = Mappings.apply(mapping, aggregate.getGroupSet());
    ImmutableList<ImmutableBitSet> groupSets =
        Mappings.apply2(mapping, aggregate.getGroupSets());
    List<AggregateCall> aggregateCalls =
        apply(mapping, aggregate.getAggCallList());
    return MutableAggregate.of(input, aggregate.indicator, groupSet, groupSets,
        aggregateCalls);
  }

  private static List<AggregateCall> apply(final Mapping mapping,
      List<AggregateCall> aggCallList) {
    return Lists.transform(aggCallList,
        new Function<AggregateCall, AggregateCall>() {
          public AggregateCall apply(AggregateCall call) {
            return call.copy(Mappings.apply2(mapping, call.getArgList()),
                Mappings.apply(mapping, call.filterArg));
          }
        });
  }

  public static MutableRel unifyAggregates(MutableAggregate query,
      MutableAggregate target) {
    if (query.getGroupType() != Aggregate.Group.SIMPLE
        || target.getGroupType() != Aggregate.Group.SIMPLE) {
      throw new AssertionError(Bug.CALCITE_461_FIXED);
    }
    MutableRel result;
    if (query.getGroupSet().equals(target.getGroupSet())) {
      // Same level of aggregation. Generate a project.
      final List<Integer> projects = Lists.newArrayList();
      final int groupCount = query.getGroupSet().cardinality();
      for (int i = 0; i < groupCount; i++) {
        projects.add(i);
      }
      for (AggregateCall aggregateCall : query.getAggCallList()) {
        int i = target.getAggCallList().indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        projects.add(groupCount + i);
      }
      result = MutableRels.createProject(target, projects);
    } else {
      // Target is coarser level of aggregation. Generate an aggregate.
      final ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
      final List<Integer> targetGroupList = target.getGroupSet().asList();
      for (int c : query.getGroupSet()) {
        int c2 = targetGroupList.indexOf(c);
        if (c2 < 0) {
          return null;
        }
        groupSet.set(c2);
      }
      final List<AggregateCall> aggregateCalls = Lists.newArrayList();
      for (AggregateCall aggregateCall : query.getAggCallList()) {
        if (aggregateCall.isDistinct()) {
          return null;
        }
        int i = target.getAggCallList().indexOf(aggregateCall);
        if (i < 0) {
          return null;
        }
        aggregateCalls.add(
            AggregateCall.create(getRollup(aggregateCall.getAggregation()),
                aggregateCall.isDistinct(),
                ImmutableList.of(target.groupSet.cardinality() + i), -1,
                aggregateCall.type, aggregateCall.name));
      }
      result = MutableAggregate.of(target, false, groupSet.build(), null,
          aggregateCalls);
    }
    return MutableRels.createCastRel(result, query.getRowType(), true);
  }

  /** Implementation of {@link UnifyRule} that matches a
   * {@link MutableAggregate} on
   * a {@link MutableProject} query to an {@link MutableAggregate} target.
   *
   * <p>The rule is necessary when we unify query=Aggregate(x) with
   * target=Aggregate(x, y). Query will tend to have an extra Project(x) on its
   * input, which this rule knows is safe to ignore.</p> */
  private static class AggregateOnProjectToAggregateUnifyRule
      extends AbstractUnifyRule {
    public static final AggregateOnProjectToAggregateUnifyRule INSTANCE =
        new AggregateOnProjectToAggregateUnifyRule();

    private AggregateOnProjectToAggregateUnifyRule() {
      super(
          operand(MutableAggregate.class,
              operand(MutableProject.class, query(0))),
          operand(MutableAggregate.class, target(0)), 1);
    }

    public UnifyResult apply(UnifyRuleCall call) {
      final MutableAggregate query = (MutableAggregate) call.query;
      final MutableAggregate target = (MutableAggregate) call.target;
      if (!(query.getInput() instanceof MutableProject)) {
        return null;
      }
      final MutableProject project = (MutableProject) query.getInput();
      if (project.getInput() != target.getInput()) {
        return null;
      }
      final Mappings.TargetMapping mapping = project.getMapping();
      if (mapping == null) {
        return null;
      }
      final MutableAggregate aggregate2 =
          permute(query, project.getInput(), mapping.inverse());
      final MutableRel result = unifyAggregates(aggregate2, target);
      return result == null ? null : call.result(result);
    }
  }

  public static SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation == SqlStdOperatorTable.SUM
        || aggregation == SqlStdOperatorTable.MIN
        || aggregation == SqlStdOperatorTable.MAX
        || aggregation == SqlStdOperatorTable.SUM0) {
      return aggregation;
    } else if (aggregation == SqlStdOperatorTable.COUNT) {
      return SqlStdOperatorTable.SUM0;
    } else {
      return null;
    }
  }

  /** Builds a shuttle that stores a list of expressions, and can map incoming
   * expressions to references to them. */
  protected static RexShuttle getRexShuttle(MutableProject target) {
    final Map<String, Integer> map = new HashMap<>();
    for (RexNode e : target.getProjects()) {
      map.put(e.toString(), map.size());
    }
    return new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        final Integer integer = map.get(ref.getName());
        if (integer != null) {
          return new RexInputRef(integer, ref.getType());
        }
        throw MatchFailed.INSTANCE;
      }

      @Override public RexNode visitCall(RexCall call) {
        final Integer integer = map.get(call.toString());
        if (integer != null) {
          return new RexInputRef(integer, call.getType());
        }
        return super.visitCall(call);
      }
    };
  }

  /** Type of {@code MutableRel}. */
  private enum MutableRelType {
    SCAN,
    PROJECT,
    FILTER,
    AGGREGATE,
    SORT,
    UNION,
    JOIN,
    HOLDER,
    VALUES
  }

  /** Visitor over {@link MutableRel}. */
  private static class MutableRelVisitor {
    private MutableRel root;

    public void visit(MutableRel node) {
      node.childrenAccept(this);
    }

    public MutableRel go(MutableRel p) {
      this.root = p;
      visit(p);
      return root;
    }
  }

  /** Mutable equivalent of {@link RelNode}.
   *
   * <p>Each node has mutable state, and keeps track of its parent and position
   * within parent.
   * It doesn't make sense to canonize {@code MutableRels},
   * otherwise one node could end up with multiple parents.
   * It follows that {@code #hashCode} and {@code #equals} are less efficient
   * than their {@code RelNode} counterparts.
   * But, you don't need to copy a {@code MutableRel} in order to change it.
   * For this reason, you should use {@code MutableRel} for short-lived
   * operations, and transcribe back to {@code RelNode} when you are done.</p>
   */
  protected abstract static class MutableRel {
    MutableRel parent;
    int ordinalInParent;
    public final RelOptCluster cluster;
    final RelDataType rowType;
    final MutableRelType type;

    private MutableRel(RelOptCluster cluster, RelDataType rowType,
        MutableRelType type) {
      this.cluster = cluster;
      this.rowType = rowType;
      this.type = type;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public abstract void setInput(int ordinalInParent, MutableRel input);

    public abstract List<MutableRel> getInputs();

    public abstract void childrenAccept(MutableRelVisitor visitor);

    /** Replaces this {@code MutableRel} in its parent with another node at the
     * same position.
     *
     * <p>Before the method, {@code child} must be an orphan (have null parent)
     * and after this method, this {@code MutableRel} is an orphan.
     *
     * @return The parent
     */
    public MutableRel replaceInParent(MutableRel child) {
      final MutableRel parent = this.parent;
      if (this != child) {
/*
        if (child.parent != null) {
          child.parent.setInput(child.ordinalInParent, null);
          child.parent = null;
        }
*/
        if (parent != null) {
          parent.setInput(ordinalInParent, child);
          this.parent = null;
          this.ordinalInParent = 0;
        }
      }
      return parent;
    }

    public abstract StringBuilder digest(StringBuilder buf);

    public final String deep() {
      return new MutableRelDumper().apply(this);
    }

    @Override public final String toString() {
      return deep();
    }

    public MutableRel getParent() { return parent; }
  }

  /** Implementation of {@link MutableRel} whose only purpose is to have a
   * child. Used as the root of a tree. */
  private static class Holder extends MutableSingleRel {
    private Holder(MutableRelType type, RelDataType rowType, MutableRel input) {
      super(type, rowType, input);
    }

    static Holder of(MutableRel input) {
      return new Holder(MutableRelType.HOLDER, input.rowType, input);
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Holder");
    }
  }

   /** Abstract base class for implementations of {@link MutableRel} that have
   * no inputs. */
  protected abstract static class MutableLeafRel extends MutableRel {
    protected final RelNode rel;

    MutableLeafRel(MutableRelType type, RelNode rel) {
      super(rel.getCluster(), rel.getRowType(), type);
      this.rel = rel;
    }

    public void setInput(int ordinalInParent, MutableRel input) {
      throw new IllegalArgumentException();
    }

    public List<MutableRel> getInputs() {
      return ImmutableList.of();
    }

    public void childrenAccept(MutableRelVisitor visitor) {
      // no children - nothing to do
    }
  }

  /** Mutable equivalent of {@link SingleRel}. */
  protected abstract static class MutableSingleRel extends MutableRel {
    protected MutableRel input;

    MutableSingleRel(MutableRelType type, RelDataType rowType,
        MutableRel input) {
      super(input.cluster, rowType, type);
      this.input = input;
      input.parent = this;
      input.ordinalInParent = 0;
    }

    public void setInput(int ordinalInParent, MutableRel input) {
      if (ordinalInParent >= 1) {
        throw new IllegalArgumentException();
      }
      this.input = input;
      if (input != null) {
        input.parent = this;
        input.ordinalInParent = 0;
      }
    }

    public List<MutableRel> getInputs() {
      return ImmutableList.of(input);
    }

    public void childrenAccept(MutableRelVisitor visitor) {
      visitor.visit(input);
    }

    public MutableRel getInput() {
      return input;
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalTableScan}. */
  protected static class MutableScan extends MutableLeafRel {
    private MutableScan(TableScan rel) {
      super(MutableRelType.SCAN, rel);
    }

    static MutableScan of(TableScan rel) {
      return new MutableScan(rel);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableScan
          && rel.getTable().equals(((MutableScan) obj).rel.getTable());
    }

    @Override public int hashCode() {
      return rel.getTable().hashCode();
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Scan(table: ")
          .append(rel.getTable().getQualifiedName()).append(")");
    }
  }

  /** Mutable equivalent of {@link org.apache.calcite.rel.core.Values}. */
  protected static class MutableValues extends MutableLeafRel {
    private MutableValues(Values rel) {
      super(MutableRelType.VALUES, rel);
    }

    static MutableValues of(Values rel) {
      return new MutableValues(rel);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableValues
          && rel == ((MutableValues) obj).rel;
    }

    @Override public int hashCode() {
      return rel.hashCode();
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Values(tuples: ")
          .append(((Values) rel).getTuples()).append(")");
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalProject}. */
  protected static class MutableProject extends MutableSingleRel {
    private final List<RexNode> projects;

    private MutableProject(RelDataType rowType, MutableRel input,
        List<RexNode> projects) {
      super(MutableRelType.PROJECT, rowType, input);
      this.projects = projects;
      assert RexUtil.compatibleTypes(projects, rowType, Litmus.THROW);
    }

    public static MutableProject of(RelDataType rowType, MutableRel input,
        List<RexNode> projects) {
      return new MutableProject(rowType, input, projects);
    }

    /** Equivalent to
     * {@link RelOptUtil#createProject(org.apache.calcite.rel.RelNode, java.util.List, java.util.List)}
     * for {@link MutableRel}. */
    public static MutableRel of(MutableRel child, List<RexNode> exprList,
        List<String> fieldNameList) {
      final RelDataType rowType =
          RexUtil.createStructType(child.cluster.getTypeFactory(), exprList,
              fieldNameList, SqlValidatorUtil.F_SUGGESTER);
      return of(rowType, child, exprList);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableProject
          && PAIRWISE_STRING_EQUIVALENCE.equivalent(
              projects, ((MutableProject) obj).projects)
          && input.equals(((MutableProject) obj).input);
    }

    @Override public int hashCode() {
      return Objects.hash(input,
          PAIRWISE_STRING_EQUIVALENCE.hash(projects));
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Project(projects: ").append(projects).append(")");
    }

    public List<RexNode> getProjects() {
      return projects;
    }

    /** Returns a list of (expression, name) pairs. */
    public final List<Pair<RexNode, String>> getNamedProjects() {
      return Pair.zip(getProjects(), getRowType().getFieldNames());
    }

    public Mappings.TargetMapping getMapping() {
      return Project.getMapping(
          input.getRowType().getFieldCount(), projects);
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalFilter}. */
  protected static class MutableFilter extends MutableSingleRel {
    private final RexNode condition;

    private MutableFilter(MutableRel input, RexNode condition) {
      super(MutableRelType.FILTER, input.rowType, input);
      this.condition = condition;
    }

    public static MutableFilter of(MutableRel input, RexNode condition) {
      return new MutableFilter(input, condition);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableFilter
          && condition.toString().equals(
              ((MutableFilter) obj).condition.toString())
          && input.equals(((MutableFilter) obj).input);
    }

    @Override public int hashCode() {
      return Objects.hash(input, condition.toString());
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Filter(condition: ").append(condition).append(")");
    }

    public RexNode getCondition() {
      return condition;
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalAggregate}. */
  protected static class MutableAggregate extends MutableSingleRel {
    public final boolean indicator;
    private final ImmutableBitSet groupSet;
    private final ImmutableList<ImmutableBitSet> groupSets;
    private final List<AggregateCall> aggCalls;

    private MutableAggregate(MutableRel input, RelDataType rowType,
        boolean indicator, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      super(MutableRelType.AGGREGATE, rowType, input);
      this.indicator = indicator;
      this.groupSet = groupSet;
      this.groupSets = groupSets == null
          ? ImmutableList.of(groupSet)
          : ImmutableList.copyOf(groupSets);
      this.aggCalls = aggCalls;
    }

    static MutableAggregate of(MutableRel input, boolean indicator,
        ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls) {
      RelDataType rowType =
          Aggregate.deriveRowType(input.cluster.getTypeFactory(),
              input.getRowType(), indicator, groupSet, groupSets, aggCalls);
      return new MutableAggregate(input, rowType, indicator, groupSet,
          groupSets, aggCalls);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableAggregate
          && groupSet.equals(((MutableAggregate) obj).groupSet)
          && aggCalls.equals(((MutableAggregate) obj).aggCalls)
          && input.equals(((MutableAggregate) obj).input);
    }

    @Override public int hashCode() {
      return Objects.hash(input, groupSet, aggCalls);
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Aggregate(groupSet: ").append(groupSet)
          .append(", groupSets: ").append(groupSets)
          .append(", calls: ").append(aggCalls).append(")");
    }

    public ImmutableBitSet getGroupSet() {
      return groupSet;
    }

    public ImmutableList<ImmutableBitSet> getGroupSets() {
      return groupSets;
    }

    public List<AggregateCall> getAggCallList() {
      return aggCalls;
    }

    public Aggregate.Group getGroupType() {
      return Aggregate.Group.induce(groupSet, groupSets);
    }
  }

  /** Mutable equivalent of {@link org.apache.calcite.rel.core.Sort}. */
  protected static class MutableSort extends MutableSingleRel {
    private final RelCollation collation;
    private final RexNode offset;
    private final RexNode fetch;

    private MutableSort(MutableRel input, RelCollation collation,
        RexNode offset, RexNode fetch) {
      super(MutableRelType.SORT, input.rowType, input);
      this.collation = collation;
      this.offset = offset;
      this.fetch = fetch;
    }

    static MutableSort of(MutableRel input, RelCollation collation,
        RexNode offset, RexNode fetch) {
      return new MutableSort(input, collation, offset, fetch);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableSort
          && collation.equals(((MutableSort) obj).collation)
          && Objects.equals(offset, ((MutableSort) obj).offset)
          && Objects.equals(fetch, ((MutableSort) obj).fetch)
          && input.equals(((MutableSort) obj).input);
    }

    @Override public int hashCode() {
      return Objects.hash(input, collation, offset, fetch);
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      buf.append("Sort(collation: ").append(collation);
      if (offset != null) {
        buf.append(", offset: ").append(offset);
      }
      if (fetch != null) {
        buf.append(", fetch: ").append(fetch);
      }
      return buf.append(")");
    }
  }

  /** Base class for set-operations. */
  protected abstract static class MutableSetOp extends MutableRel {
    protected final List<MutableRel> inputs;

    private MutableSetOp(RelOptCluster cluster, RelDataType rowType,
        MutableRelType type, List<MutableRel> inputs) {
      super(cluster, rowType, type);
      this.inputs = inputs;
    }

    @Override public void setInput(int ordinalInParent, MutableRel input) {
      inputs.set(ordinalInParent, input);
      if (input != null) {
        input.parent = this;
        input.ordinalInParent = ordinalInParent;
      }
    }

    @Override public List<MutableRel> getInputs() {
      return inputs;
    }

    @Override public void childrenAccept(MutableRelVisitor visitor) {
      for (MutableRel input : inputs) {
        visitor.visit(input);
      }
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalUnion}. */
  protected static class MutableUnion extends MutableSetOp {
    public boolean all;

    private MutableUnion(RelOptCluster cluster, RelDataType rowType,
        List<MutableRel> inputs, boolean all) {
      super(cluster, rowType, MutableRelType.UNION, inputs);
      this.all = all;
    }

    static MutableUnion of(List<MutableRel> inputs, boolean all) {
      assert inputs.size() >= 2;
      final MutableRel input0 = inputs.get(0);
      return new MutableUnion(input0.cluster, input0.rowType, inputs, all);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableUnion
          && inputs.equals(((MutableUnion) obj).getInputs());
    }

    @Override public int hashCode() {
      return Objects.hash(type, inputs);
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Union");
    }
  }

  /** Base Class for relations with two inputs */
  private abstract static class MutableBiRel extends MutableRel {
    protected MutableRel left;
    protected MutableRel right;

    MutableBiRel(MutableRelType type, RelOptCluster cluster, RelDataType rowType,
                        MutableRel left, MutableRel right) {
      super(cluster, rowType, type);
      this.left = left;
      left.parent = this;
      left.ordinalInParent = 0;

      this.right = right;
      right.parent = this;
      right.ordinalInParent = 1;
    }

    public void setInput(int ordinalInParent, MutableRel input) {
      if (ordinalInParent > 1) {
        throw new IllegalArgumentException();
      }
      if (ordinalInParent == 0) {
        this.left = input;
      } else {
        this.right = input;
      }
      if (input != null) {
        input.parent = this;
        input.ordinalInParent = ordinalInParent;
      }
    }

    public List<MutableRel> getInputs() {
      return ImmutableList.of(left, right);
    }

    public MutableRel getLeft() {
      return left;
    }

    public MutableRel getRight() {
      return right;
    }

    public void childrenAccept(MutableRelVisitor visitor) {

      visitor.visit(left);
      visitor.visit(right);
    }
  }

  /** Mutable equivalent of
   * {@link org.apache.calcite.rel.logical.LogicalJoin}. */
  private static class MutableJoin extends MutableBiRel {
    //~ Instance fields --------------------------------------------------------

    protected final RexNode condition;
    protected final ImmutableSet<CorrelationId> variablesSet;

    /**
     * Values must be of enumeration {@link JoinRelType}, except that
     * {@link JoinRelType#RIGHT} is disallowed.
     */
    protected JoinRelType joinType;

    private MutableJoin(
        RelDataType rowType,
        MutableRel left,
        MutableRel right,
        RexNode condition,
        JoinRelType joinType,
        Set<CorrelationId> variablesSet) {
      super(MutableRelType.JOIN, left.cluster, rowType, left, right);
      this.condition = Preconditions.checkNotNull(condition);
      this.variablesSet = ImmutableSet.copyOf(variablesSet);
      this.joinType = Preconditions.checkNotNull(joinType);
    }

    public RexNode getCondition() {
      return condition;
    }

    public JoinRelType getJoinType() {
      return joinType;
    }

    public ImmutableSet<CorrelationId> getVariablesSet() {
      return variablesSet;
    }

    static MutableJoin of(RelOptCluster cluster, MutableRel left,
        MutableRel right, RexNode condition, JoinRelType joinType,
        Set<CorrelationId> variablesStopped) {
      List<RelDataTypeField> fieldList = Collections.emptyList();
      RelDataType rowType =
          SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
              right.getRowType(), joinType, cluster.getTypeFactory(), null,
              fieldList);
      return new MutableJoin(rowType, left, right, condition, joinType,
          variablesStopped);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof MutableJoin
          && joinType == ((MutableJoin) obj).joinType
          && condition.toString().equals(
              ((MutableJoin) obj).condition.toString())
          && left.equals(((MutableJoin) obj).left)
          && right.equals(((MutableJoin) obj).right);
    }

    @Override public int hashCode() {
      return Objects.hash(left, right, condition.toString(), joinType);
    }

    @Override public StringBuilder digest(StringBuilder buf) {
      return buf.append("Join(left: ").append(left)
          .append(", right:").append(right)
          .append(")");
    }
  }

  /** Utilities for dealing with {@link MutableRel}s. */
  protected static class MutableRels {
    public static boolean contains(MutableRel ancestor,
        final MutableRel target) {
      if (ancestor.equals(target)) {
        // Short-cut common case.
        return true;
      }
      try {
        new MutableRelVisitor() {
          @Override public void visit(MutableRel node) {
            if (node.equals(target)) {
              throw Util.FoundOne.NULL;
            }
            super.visit(node);
          }
          // CHECKSTYLE: IGNORE 1
        }.go(ancestor);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }

    public static MutableRel preOrderTraverseNext(MutableRel node) {
      MutableRel parent = node.getParent();
      int ordinal = node.ordinalInParent + 1;
      while (parent != null) {
        if (parent.getInputs().size() > ordinal) {
          return parent.getInputs().get(ordinal);
        }
        node = parent;
        parent = node.getParent();
        ordinal = node.ordinalInParent + 1;
      }
      return null;
    }

    private static List<MutableRel> descendants(MutableRel query) {
      final List<MutableRel> list = new ArrayList<>();
      descendantsRecurse(list, query);
      return list;
    }

    private static void descendantsRecurse(List<MutableRel> list,
        MutableRel rel) {
      list.add(rel);
      for (MutableRel input : rel.getInputs()) {
        descendantsRecurse(list, input);
      }
    }

    /** Returns whether two relational expressions have the same row-type. */
    public static boolean equalType(String desc0, MutableRel rel0, String desc1,
        MutableRel rel1, Litmus litmus) {
      return RelOptUtil.equal(desc0, rel0.getRowType(),
          desc1, rel1.getRowType(), litmus);
    }

    /** Within a relational expression {@code query}, replaces occurrences of
     * {@code find} with {@code replace}.
     *
     * <p>Assumes relational expressions (and their descendants) are not null.
     * Does not handle cycles. */
    public static Replacement replace(MutableRel query, MutableRel find,
        MutableRel replace) {
      if (find.equals(replace)) {
        // Short-cut common case.
        return null;
      }
      assert equalType("find", find, "replace", replace, Litmus.THROW);
      return replaceRecurse(query, find, replace);
    }

    /** Helper for {@link #replace}. */
    private static Replacement replaceRecurse(MutableRel query,
        MutableRel find, MutableRel replace) {
      if (find.equals(query)) {
        query.replaceInParent(replace);
        return new Replacement(query, replace);
      }
      for (MutableRel input : query.getInputs()) {
        Replacement r = replaceRecurse(input, find, replace);
        if (r != null) {
          return r;
        }
      }
      return null;
    }

    /** Based on
     * {@link org.apache.calcite.rel.rules.ProjectRemoveRule#strip}. */
    public static MutableRel strip(MutableProject project) {
      return isTrivial(project) ? project.getInput() : project;
    }

    /** Based on
     * {@link org.apache.calcite.rel.rules.ProjectRemoveRule#isTrivial(org.apache.calcite.rel.core.Project)}. */
    public static boolean isTrivial(MutableProject project) {
      MutableRel child = project.getInput();
      final RelDataType childRowType = child.getRowType();
      return RexUtil.isIdentity(project.getProjects(), childRowType);
    }

    /** Equivalent to
     * {@link RelOptUtil#createProject(org.apache.calcite.rel.RelNode, java.util.List)}
     * for {@link MutableRel}. */
    public static MutableRel createProject(final MutableRel child,
        final List<Integer> posList) {
      final RelDataType rowType = child.getRowType();
      if (Mappings.isIdentity(posList, rowType.getFieldCount())) {
        return child;
      }
      return MutableProject.of(
          RelOptUtil.permute(child.cluster.getTypeFactory(), rowType,
              Mappings.bijection(posList)),
          child,
          new AbstractList<RexNode>() {
            public int size() {
              return posList.size();
            }

            public RexNode get(int index) {
              final int pos = posList.get(index);
              return RexInputRef.of(pos, rowType);
            }
          });
    }

    /** Equivalence to {@link org.apache.calcite.plan.RelOptUtil#createCastRel}
     * for {@link MutableRel}. */
    public static MutableRel createCastRel(MutableRel rel,
        RelDataType castRowType, boolean rename) {
      RelDataType rowType = rel.getRowType();
      if (RelOptUtil.areRowTypesEqual(rowType, castRowType, rename)) {
        // nothing to do
        return rel;
      }
      List<RexNode> castExps =
          RexUtil.generateCastExpressions(rel.cluster.getRexBuilder(),
              castRowType, rowType);
      final List<String> fieldNames =
          rename ? castRowType.getFieldNames() : rowType.getFieldNames();
      return MutableProject.of(rel, castExps, fieldNames);
    }
  }

  /** Visitor that prints an indented tree of {@link MutableRel}s. */
  protected static class MutableRelDumper extends MutableRelVisitor {
    private final StringBuilder buf = new StringBuilder();
    private int level;

    @Override public void visit(MutableRel node) {
      Spaces.append(buf, level * 2);
      if (node == null) {
        buf.append("null");
      } else {
        node.digest(buf);
        buf.append("\n");
        ++level;
        super.visit(node);
        --level;
      }
    }

    public String apply(MutableRel rel) {
      go(rel);
      return buf.toString();
    }
  }

  /** Returns if one rel is weaker than another. */
  protected boolean isWeaker(MutableRel rel0, MutableRel rel) {
    if (rel0 == rel || equivalents.get(rel0).contains(rel)) {
      return false;
    }

    if (!(rel0 instanceof MutableFilter)
        || !(rel instanceof MutableFilter)) {
      return false;
    }

    if (!rel.getRowType().equals(rel0.getRowType())) {
      return false;
    }

    final MutableRel rel0input = ((MutableFilter) rel0).getInput();
    final MutableRel relinput = ((MutableFilter) rel).getInput();
    if (rel0input != relinput
        && !equivalents.get(rel0input).contains(relinput)) {
      return false;
    }

    RexExecutorImpl rexImpl =
        (RexExecutorImpl) (rel.cluster.getPlanner().getExecutor());
    RexImplicationChecker rexImplicationChecker = new RexImplicationChecker(
        rel.cluster.getRexBuilder(),
        rexImpl, rel.getRowType());

    return rexImplicationChecker.implies(((MutableFilter) rel0).getCondition(),
        ((MutableFilter) rel).getCondition());
  }

  /** Operand to a {@link UnifyRule}. */
  protected abstract static class Operand {
    protected final Class<? extends MutableRel> clazz;

    protected Operand(Class<? extends MutableRel> clazz) {
      this.clazz = clazz;
    }

    public abstract boolean matches(SubstitutionVisitor visitor, MutableRel rel);

    public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      return false;
    }
  }

  /** Operand to a {@link UnifyRule} that matches a relational expression of a
   * given type. It has zero or more child operands. */
  private static class InternalOperand extends Operand {
    private final List<Operand> inputs;

    InternalOperand(Class<? extends MutableRel> clazz, List<Operand> inputs) {
      super(clazz);
      this.inputs = inputs;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel)
          && allMatch(visitor, inputs, rel.getInputs());
    }

    @Override public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel)
          && allWeaker(visitor, inputs, rel.getInputs());
    }
    private static boolean allMatch(SubstitutionVisitor visitor,
        List<Operand> operands, List<MutableRel> rels) {
      if (operands.size() != rels.size()) {
        return false;
      }
      for (Pair<Operand, MutableRel> pair : Pair.zip(operands, rels)) {
        if (!pair.left.matches(visitor, pair.right)) {
          return false;
        }
      }
      return true;
    }

    private static boolean allWeaker(
        SubstitutionVisitor visitor,
        List<Operand> operands, List<MutableRel> rels) {
      if (operands.size() != rels.size()) {
        return false;
      }
      for (Pair<Operand, MutableRel> pair : Pair.zip(operands, rels)) {
        if (!pair.left.isWeaker(visitor, pair.right)) {
          return false;
        }
      }
      return true;
    }
  }

  /** Operand to a {@link UnifyRule} that matches a relational expression of a
   * given type. */
  private static class AnyOperand extends Operand {
    AnyOperand(Class<? extends MutableRel> clazz) {
      super(clazz);
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      return clazz.isInstance(rel);
    }
  }

  /** Operand that assigns a particular relational expression to a variable.
   *
   * <p>It is applied to a descendant of the query, writes the operand into the
   * slots array, and always matches.
   * There is a corresponding operand of type {@link TargetOperand} that checks
   * whether its relational expression, a descendant of the target, is
   * equivalent to this {@code QueryOperand}'s relational expression.
   */
  private static class QueryOperand extends Operand {
    private final int ordinal;

    protected QueryOperand(int ordinal) {
      super(MutableRel.class);
      this.ordinal = ordinal;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      visitor.slots[ordinal] = rel;
      return true;
    }
  }

  /** Operand that checks that a relational expression matches the corresponding
   * relational expression that was passed to a {@link QueryOperand}. */
  private static class TargetOperand extends Operand {
    private final int ordinal;

    protected TargetOperand(int ordinal) {
      super(MutableRel.class);
      this.ordinal = ordinal;
    }

    @Override public boolean matches(SubstitutionVisitor visitor, MutableRel rel) {
      final MutableRel rel0 = visitor.slots[ordinal];
      assert rel0 != null : "QueryOperand should have been called first";
      return rel0 == rel || visitor.equivalents.get(rel0).contains(rel);
    }

    @Override public boolean isWeaker(SubstitutionVisitor visitor, MutableRel rel) {
      final MutableRel rel0 = visitor.slots[ordinal];
      assert rel0 != null : "QueryOperand should have been called first";
      return visitor.isWeaker(rel0, rel);
    }
  }

  /** Visitor that counts how many {@link QueryOperand} and
   * {@link TargetOperand} in an operand tree. */
  private static class SlotCounter {
    int queryCount;
    int targetCount;

    void visit(Operand operand) {
      if (operand instanceof QueryOperand) {
        ++queryCount;
      } else if (operand instanceof TargetOperand) {
        ++targetCount;
      } else if (operand instanceof AnyOperand) {
        // nothing
      } else {
        for (Operand input : ((InternalOperand) operand).inputs) {
          visit(input);
        }
      }
    }
  }

  /**
   * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalFilter}
   * on top of a {@link org.apache.calcite.rel.logical.LogicalProject} into a
   * trivial filter (on a boolean column).
   */
  public static class FilterOnProjectRule extends RelOptRule {
    private static final Predicate<Filter> PREDICATE =
        new Predicate<Filter>() {
          public boolean apply(Filter input) {
            return input.getCondition() instanceof RexInputRef;
          }
        };

    public static final FilterOnProjectRule INSTANCE =
        new FilterOnProjectRule();

    private FilterOnProjectRule() {
      super(
          operand(Filter.class, null, PREDICATE,
              some(operand(Project.class, any()))));
    }

    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final Project project = call.rel(1);

      final List<RexNode> newProjects = new ArrayList<>(project.getProjects());
      newProjects.add(filter.getCondition());

      final RelOptCluster cluster = filter.getCluster();
      RelDataType newRowType =
          cluster.getTypeFactory().builder()
              .addAll(project.getRowType().getFieldList())
              .add("condition", Util.last(newProjects).getType())
              .build();
      final RelNode newProject =
          project.copy(project.getTraitSet(),
              project.getInput(),
              newProjects,
              newRowType);

      final RexInputRef newCondition =
          cluster.getRexBuilder().makeInputRef(newProject,
              newProjects.size() - 1);

      call.transformTo(filter.copy(filter.getTraitSet(), newProject, newCondition));
    }
  }
}

// End SubstitutionVisitor.java
