/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql2rel.CorrelationReferenceFinder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class comes from Calcite almost as-is. The only change concerns
 * the dispatcher and the builder, so the trimmer is thread-safe and can
 * be reused across different queries. Definition follows.
 *
 * <p>Transformer that walks over a tree of relational expressions, replacing each
 * {@link RelNode} with a 'slimmed down' relational expression that projects
 * only the columns required by its consumer.
 *
 * <p>Uses multi-methods to fire the right rule for each type of relational
 * expression. This allows the transformer to be extended without having to
 * add a new method to RelNode, and without requiring a collection of rule
 * classes scattered to the four winds.
 */
public class RelFieldTrimmer implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOG = LoggerFactory.getLogger(RelFieldTrimmer.class);

  protected static final ThreadLocal<RelBuilder> REL_BUILDER =
      new ThreadLocal<>();

  //~ Instance fields --------------------------------------------------------

  protected final HiveReflectUtil.ClassMethodDispatcher<TrimResult, RelNode> trimFieldsDispatcher;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelFieldTrimmer.
   * @param useLMFBasedDispatcher True if we want to create a dispatcher based on
   *                              {@link java.lang.invoke.LambdaMetafactory} that is
   *                              thread-safe, or false if we want to create a
   *                              Calcite dispatcher based on reflection that is not
   *                              thread-safe. False should only be used for
   *                              testing/benchmarking purposes
   */
  protected RelFieldTrimmer(boolean useLMFBasedDispatcher) {
    if (useLMFBasedDispatcher) {
      this.trimFieldsDispatcher =
          HiveReflectUtil.createMethodDispatcher(
              TrimResult.class,
              this,
              "trimFields",
              RelNode.class,
              ImmutableBitSet.class,
              Set.class);
    } else {
      this.trimFieldsDispatcher =
          HiveReflectUtil.createCalciteMethodDispatcherWrapper(
              ReflectUtil.createMethodDispatcher(
                TrimResult.class,
                this,
                "trimFields",
                RelNode.class,
                ImmutableBitSet.class,
                Set.class));
    }
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Trims unused fields from a relational expression.
   *
   * <p>We presume that all fields of the relational expression are wanted by
   * its consumer, so only trim fields that are not used within the tree.
   *
   * @param relBuilder Rel builder
   * @param root Root node of relational expression
   * @return Trimmed relational expression
   */
  public RelNode trim(RelBuilder relBuilder, RelNode root) {
    try {
      REL_BUILDER.set(relBuilder);
      final int fieldCount = root.getRowType().getFieldCount();
      final ImmutableBitSet fieldsUsed = ImmutableBitSet.range(fieldCount);
      final Set<RelDataTypeField> extraFields = Collections.emptySet();
      final TrimResult trimResult =
          dispatchTrimFields(root, fieldsUsed, extraFields);
      if (!trimResult.right.isIdentity()) {
        throw new IllegalArgumentException();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            RelOptUtil.dumpPlan("Plan after trimming unused fields",
                trimResult.left, SqlExplainFormat.TEXT,
                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
      }
      return trimResult.left;
    } finally {
      REL_BUILDER.remove();
    }
  }

  /**
   * Trims the fields of an input relational expression.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChild(
      RelNode rel,
      RelNode input,
      final ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final ImmutableBitSet.Builder fieldsUsedBuilder = fieldsUsed.rebuild();

    // Fields that define the collation cannot be discarded.
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final ImmutableList<RelCollation> collations = mq.collations(input);
    if (collations != null) {
      for (RelCollation collation : collations) {
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
          fieldsUsedBuilder.set(fieldCollation.getFieldIndex());
        }
      }
    }
    // Correlating variables are a means for other relational expressions to use
    // fields.
    for (final CorrelationId correlation : rel.getVariablesSet()) {
      rel.accept(
          new CorrelationReferenceFinder() {
            protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)) {
                fieldsUsedBuilder.set(fieldAccess.getField().getIndex());
              }
              return fieldAccess;
            }
          });
    }

    return dispatchTrimFields(input, fieldsUsedBuilder.build(), extraFields);
  }

  /**
   * Trims a child relational expression, then adds back a dummy project to
   * restore the fields that were removed.
   *
   * <p>Sounds pointless? It causes unused fields to be removed
   * further down the tree (towards the leaves), but it ensure that the
   * consuming relational expression continues to see the same fields.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChildRestore(
      RelNode rel,
      RelNode input,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    TrimResult trimResult = trimChild(rel, input, fieldsUsed, extraFields);
    if (trimResult.right.isIdentity()) {
      return trimResult;
    }
    final RelDataType rowType = input.getRowType();
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    final List<RexNode> exprList = new ArrayList<>();
    final List<String> nameList = rowType.getFieldNames();
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    assert trimResult.right.getSourceCount() == fieldList.size();
    for (int i = 0; i < fieldList.size(); i++) {
      int source = trimResult.right.getTargetOpt(i);
      RelDataTypeField field = fieldList.get(i);
      exprList.add(
          source < 0
              ? rexBuilder.makeZeroLiteral(field.getType())
              : rexBuilder.makeInputRef(field.getType(), source));
    }
    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(trimResult.left)
        .project(exprList, nameList);
    return result(relBuilder.build(),
        Mappings.createIdentity(fieldList.size()));
  }

  /**
   * Invokes {@link #trimFields}, or the appropriate method for the type
   * of the rel parameter, using multi-method dispatch.
   *
   * @param rel        Relational expression
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected final TrimResult dispatchTrimFields(
      RelNode rel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final TrimResult trimResult =
        trimFieldsDispatcher.invoke(rel, fieldsUsed, extraFields);
    final RelNode newRel = trimResult.left;
    final Mapping mapping = trimResult.right;
    final int fieldCount = rel.getRowType().getFieldCount();
    assert mapping.getSourceCount() == fieldCount
        : "source: " + mapping.getSourceCount() + " != " + fieldCount;
    final int newFieldCount = newRel.getRowType().getFieldCount();
    assert mapping.getTargetCount() + extraFields.size() == newFieldCount
        || Bug.TODO_FIXED
        : "target: " + mapping.getTargetCount()
        + " + " + extraFields.size()
        + " != " + newFieldCount;
    if (Bug.TODO_FIXED) {
      assert newFieldCount > 0 : "rel has no fields after trim: " + rel;
    }
    if (newRel.equals(rel)) {
      return result(rel, mapping);
    }
    return trimResult;
  }

  protected TrimResult result(RelNode r, final Mapping mapping) {
    final RelBuilder relBuilder = REL_BUILDER.get();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    for (final CorrelationId correlation : r.getVariablesSet()) {
      r = r.accept(
          new CorrelationReferenceFinder() {
            protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)
                  && v.getType().getFieldCount() == mapping.getSourceCount()) {
                final int old = fieldAccess.getField().getIndex();
                final int new_ = mapping.getTarget(old);
                final RelDataTypeFactory.Builder typeBuilder =
                    relBuilder.getTypeFactory().builder();
                for (int target : Util.range(mapping.getTargetCount())) {
                  typeBuilder.add(
                      v.getType().getFieldList().get(mapping.getSource(target)));
                }
                final RexNode newV =
                    rexBuilder.makeCorrel(typeBuilder.build(), v.id);
                if (old != new_) {
                  return rexBuilder.makeFieldAccess(newV, new_);
                }
              }
              return fieldAccess;
            }
          });
    }
    return new TrimResult(r, mapping);
  }

  /**
   * Visit method, per {@link org.apache.calcite.util.ReflectiveVisitor}.
   *
   * <p>This method is invoked reflectively, so there may not be any apparent
   * calls to it. The class (or derived classes) may contain overloads of
   * this method with more specific types for the {@code rel} parameter.
   *
   * <p>Returns a pair: the relational expression created, and the mapping
   * between the original fields and the fields of the newly created
   * relational expression.
   *
   * @param rel        Relational expression
   * @param fieldsUsed Fields needed by the consumer
   * @return relational expression and mapping
   */
  public TrimResult trimFields(
      RelNode rel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // We don't know how to trim this kind of relational expression, so give
    // it back intact.
    Util.discard(fieldsUsed);
    return result(rel,
        Mappings.createIdentity(rel.getRowType().getFieldCount()));
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public TrimResult trimFields(
      Project project,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = project.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = project.getInput();

    // Which fields are required from the input?
    final Set<RelDataTypeField> inputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields);
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }
    ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(project, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(project, Mappings.createIdentity(fieldCount));
    }

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      return dummyProject(fieldCount, newInput);
    }

    // Build new project expressions, and populate the mapping.
    final List<RexNode> newProjects = new ArrayList<>();
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(
            inputMapping, newInput);
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality());
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        mapping.set(ord.i, newProjects.size());
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjects.add(newProjectExpr);
      }
    }

    final RelDataType newRowType =
        RelOptUtil.permute(project.getCluster().getTypeFactory(), rowType,
            mapping);

    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput);
    relBuilder.project(newProjects, newRowType.getFieldNames());
    return result(relBuilder.build(), mapping);
  }

  /** Creates a project with a dummy column, to protect the parts of the system
   * that cannot handle a relational expression with no columns.
   *
   * @param fieldCount Number of fields in the original relational expression
   * @param input Trimmed input
   * @return Dummy project, or null if no dummy is required
   */
  protected TrimResult dummyProject(int fieldCount, RelNode input) {
    final RelOptCluster cluster = input.getCluster();
    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, 1);
    if (input.getRowType().getFieldCount() == 1) {
      // Input already has one field (and may in fact be a dummy project we
      // created for the child). We can't do better.
      return result(input, mapping);
    }
    final RexLiteral expr =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO);
    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(input);
    relBuilder.project(ImmutableList.<RexNode>of(expr), ImmutableList.of("DUMMY"));
    return result(relBuilder.build(), mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalFilter}.
   */
  public TrimResult trimFields(
      Filter filter,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = filter.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RexNode conditionExpr = filter.getCondition();
    final RelNode input = filter.getInput();

    // We use the fields used by the consumer, plus any fields used in the
    // filter.
    final Set<RelDataTypeField> inputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields, fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(filter, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(filter, Mappings.createIdentity(fieldCount));
    }

    // Build new project expressions, and populate the mapping.
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(inputMapping, newInput);
    RexNode newConditionExpr =
        conditionExpr.accept(shuttle);

    // Build new filter with trimmed input and condition.
    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput)
        .filter(filter.getVariablesSet(), newConditionExpr);

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(relBuilder.build(), inputMapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.Sort}.
   */
  public TrimResult trimFields(
      Sort sort,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sort.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = sort.getCollation();
    final RelNode input = sort.getInput();

    // We use the fields used by the consumer, plus any fields used as sort
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(sort, input, inputFieldsUsed.build(), inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return result(sort, Mappings.createIdentity(fieldCount));
    }

    // leave the Sort unchanged in case we have dynamic limits
    if (sort.offset instanceof RexDynamicParam
        || sort.fetch instanceof RexDynamicParam) {
      return result(sort, inputMapping);
    }

    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput);
    final int offset =
        sort.offset == null ? 0 : RexLiteral.intValue(sort.offset);
    final int fetch =
        sort.fetch == null ? -1 : RexLiteral.intValue(sort.fetch);
    final ImmutableList<RexNode> fields =
        relBuilder.fields(RexUtil.apply(inputMapping, collation));
    relBuilder.sortLimit(offset, fetch, fields);

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(relBuilder.build(), inputMapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  public TrimResult trimFields(
      Join join,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = join.getSystemFieldList().size()
        + join.getLeft().getRowType().getFieldCount()
        + join.getRight().getRowType().getFieldCount();
    final RexNode conditionExpr = join.getCondition();
    final int systemFieldCount = join.getSystemFieldList().size();

    // Add in fields used in the condition.
    final Set<RelDataTypeField> combinedInputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(combinedInputExtraFields, fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet fieldsUsedPlus = inputFinder.build();

    // If no system fields are used, we can remove them.
    int systemFieldUsedCount = 0;
    for (int i = 0; i < systemFieldCount; ++i) {
      if (fieldsUsed.get(i)) {
        ++systemFieldUsedCount;
      }
    }
    final int newSystemFieldCount;
    if (systemFieldUsedCount == 0) {
      newSystemFieldCount = 0;
    } else {
      newSystemFieldCount = systemFieldCount;
    }

    int offset = systemFieldCount;
    int changeCount = 0;
    int newFieldCount = newSystemFieldCount;
    final List<RelNode> newInputs = new ArrayList<>(2);
    final List<Mapping> inputMappings = new ArrayList<>();
    final List<Integer> inputExtraFieldCounts = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // Compute required mapping.
      ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
      for (int bit : fieldsUsedPlus) {
        if (bit >= offset && bit < offset + inputFieldCount) {
          inputFieldsUsed.set(bit - offset);
        }
      }

      // If there are system fields, we automatically use the
      // corresponding field in each input.
      inputFieldsUsed.set(0, newSystemFieldCount);

      // FIXME: We ought to collect extra fields for each input
      // individually. For now, we assume that just one input has
      // on-demand fields.
      Set<RelDataTypeField> inputExtraFields =
          RelDataTypeImpl.extra(inputRowType) == null
              ? Collections.emptySet()
              : combinedInputExtraFields;
      inputExtraFieldCounts.add(inputExtraFields.size());
      TrimResult trimResult =
          trimChild(join, input, inputFieldsUsed.build(), inputExtraFields);
      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // Move offset to point to start of next input.
      offset += inputFieldCount;
      newFieldCount +=
          inputMapping.getTargetCount() + inputExtraFields.size();
    }

    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            newFieldCount);
    for (int i = 0; i < newSystemFieldCount; ++i) {
      mapping.set(i, i);
    }
    offset = systemFieldCount;
    int newOffset = newSystemFieldCount;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount()
          + inputExtraFieldCounts.get(i);
    }

    if (changeCount == 0
        && mapping.isIdentity()) {
      return result(join, Mappings.createIdentity(fieldCount));
    }

    // Build new join.
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(
            mapping, newInputs.get(0), newInputs.get(1));
    RexNode newConditionExpr =
        conditionExpr.accept(shuttle);

    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInputs.get(0));
    relBuilder.push(newInputs.get(1));

    switch (join.getJoinType()) {
      case SEMI:
      case ANTI:
        // For SemiJoins and AntiJoins only map fields from the left-side
        if (join.getJoinType() == JoinRelType.SEMI) {
          relBuilder.semiJoin(newConditionExpr);
        } else {
          relBuilder.antiJoin(newConditionExpr);
        }
        Mapping inputMapping = inputMappings.get(0);
        mapping = Mappings.create(MappingType.INVERSE_SURJECTION,
            join.getRowType().getFieldCount(),
            newSystemFieldCount + inputMapping.getTargetCount());
        for (int i = 0; i < newSystemFieldCount; ++i) {
          mapping.set(i, i);
        }
        offset = systemFieldCount;
        newOffset = newSystemFieldCount;
        for (IntPair pair : inputMapping) {
          mapping.set(pair.source + offset, pair.target + newOffset);
        }
        break;
      default:
        relBuilder.join(join.getJoinType(), newConditionExpr);
    }

    return result(relBuilder.build(), mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.SetOp} (including UNION and UNION ALL).
   */
  public TrimResult trimFields(
      SetOp setOp,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = setOp.getRowType();
    final int fieldCount = rowType.getFieldCount();
    int changeCount = 0;

    // Fennel abhors an empty row type, so pretend that the parent rel
    // wants the last field. (The last field is the least likely to be a
    // system field.)
    if (fieldsUsed.isEmpty()) {
      fieldsUsed = ImmutableBitSet.of(rowType.getFieldCount() - 1);
    }

    // Compute the desired field mapping. Give the consumer the fields they
    // want, in the order that they appear in the bitset.
    final Mapping mapping = createMapping(fieldsUsed, fieldCount);

    // Create input with trimmed columns.
    for (RelNode input : setOp.getInputs()) {
      TrimResult trimResult =
          trimChild(setOp, input, fieldsUsed, extraFields);

      // We want "mapping", the input gave us "inputMapping", compute
      // "remaining" mapping.
      //    |                   |                |
      //    |---------------- mapping ---------->|
      //    |-- inputMapping -->|                |
      //    |                   |-- remaining -->|
      //
      // For instance, suppose we have columns [a, b, c, d],
      // the consumer asked for mapping = [b, d],
      // and the transformed input has columns inputMapping = [d, a, b].
      // remaining will permute [b, d] to [d, a, b].
      Mapping remaining = Mappings.divide(mapping, trimResult.right);

      // Create a projection; does nothing if remaining is identity.
      final RelBuilder relBuilder = REL_BUILDER.get();
      relBuilder.push(trimResult.left);
      relBuilder.permute(remaining);

      if (input != relBuilder.peek()) {
        ++changeCount;
      }
    }

    final RelBuilder relBuilder = REL_BUILDER.get();
    // If the input is unchanged, and we need to project all columns,
    // there's to do.
    if (changeCount == 0
        && mapping.isIdentity()) {
      for (RelNode input : setOp.getInputs()) {
        relBuilder.build();
      }
      return result(setOp, mapping);
    }

    switch (setOp.kind) {
      case UNION:
        relBuilder.union(setOp.all, setOp.getInputs().size());
        break;
      case INTERSECT:
        relBuilder.intersect(setOp.all, setOp.getInputs().size());
        break;
      case EXCEPT:
        assert setOp.getInputs().size() == 2;
        relBuilder.minus(setOp.all);
        break;
      default:
        throw new AssertionError("unknown setOp " + setOp);
    }
    return result(relBuilder.build(), mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
   */
  public TrimResult trimFields(
      Aggregate aggregate,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // Fields:
    //
    // | sys fields | group fields | indicator fields | agg functions |
    //
    // Two kinds of trimming:
    //
    // 1. If agg rel has system fields but none of these are used, create an
    // agg rel with no system fields.
    //
    // 2. If aggregate functions are not used, remove them.
    //
    // But group and indicator fields stay, even if they are not used.

    final RelDataType rowType = aggregate.getRowType();

    // Compute which input fields are used.
    // 1. group fields are always used
    final ImmutableBitSet.Builder inputFieldsUsed =
        aggregate.getGroupSet().rebuild();
    // 2. agg functions
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      inputFieldsUsed.addAll(aggCall.getArgList());
      if (aggCall.filterArg >= 0) {
        inputFieldsUsed.set(aggCall.filterArg);
      }
      inputFieldsUsed.addAll(RelCollations.ordinals(aggCall.collation));
    }

    // Create input with trimmed columns.
    final RelNode input = aggregate.getInput();
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    final TrimResult trimResult =
        trimChild(aggregate, input, inputFieldsUsed.build(), inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // We have to return group keys and (if present) indicators.
    // So, pretend that the consumer asked for them.
    final int groupCount = aggregate.getGroupSet().cardinality();
    fieldsUsed =
        fieldsUsed.union(ImmutableBitSet.range(groupCount));

    // If the input is unchanged, and we need to project all columns,
    // there's nothing to do.
    if (input == newInput
        && fieldsUsed.equals(ImmutableBitSet.range(rowType.getFieldCount()))) {
      return result(aggregate,
          Mappings.createIdentity(rowType.getFieldCount()));
    }

    // Which agg calls are used by our consumer?
    int j = groupCount;
    int usedAggCallCount = 0;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      if (fieldsUsed.get(j++)) {
        ++usedAggCallCount;
      }
    }

    // Offset due to the number of system fields having changed.
    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            groupCount + usedAggCallCount);

    final ImmutableBitSet newGroupSet =
        Mappings.apply(inputMapping, aggregate.getGroupSet());

    final ImmutableList<ImmutableBitSet> newGroupSets =
        ImmutableList.copyOf(
            Iterables.transform(aggregate.getGroupSets(),
                input1 -> Mappings.apply(inputMapping, input1)));

    // Populate mapping of where to find the fields. System, group key and
    // indicator fields first.
    for (j = 0; j < groupCount; j++) {
      mapping.set(j, j);
    }

    // Now create new agg calls, and populate mapping for them.
    final RelBuilder relBuilder = REL_BUILDER.get();
    relBuilder.push(newInput);
    final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
    j = groupCount;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (fieldsUsed.get(j)) {
        final ImmutableList<RexNode> args =
            relBuilder.fields(
                Mappings.apply2(inputMapping, aggCall.getArgList()));
        final RexNode filterArg = aggCall.filterArg < 0 ? null
            : relBuilder.field(Mappings.apply(inputMapping, aggCall.filterArg));
        RelBuilder.AggCall newAggCall =
            relBuilder.aggregateCall(aggCall.getAggregation(), args)
                .distinct(aggCall.isDistinct())
                .filter(filterArg)
                .approximate(aggCall.isApproximate())
                .sort(relBuilder.fields(aggCall.collation))
                .as(aggCall.name);
        mapping.set(j, groupCount + newAggCallList.size());
        newAggCallList.add(newAggCall);
      }
      ++j;
    }

    final RelBuilder.GroupKey groupKey =
        relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);

    return result(relBuilder.build(), mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableModify}.
   */
  public TrimResult trimFields(
      LogicalTableModify modifier,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // Ignore what consumer wants. We always project all columns.
    Util.discard(fieldsUsed);

    final RelDataType rowType = modifier.getRowType();
    final int fieldCount = rowType.getFieldCount();
    RelNode input = modifier.getInput();

    // We want all fields from the child.
    final int inputFieldCount = input.getRowType().getFieldCount();
    final ImmutableBitSet inputFieldsUsed =
        ImmutableBitSet.range(inputFieldCount);

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(modifier, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;
    if (!inputMapping.isIdentity()) {
      // We asked for all fields. Can't believe that the child decided
      // to permute them!
      throw new AssertionError(
          "Expected identity mapping, got " + inputMapping);
    }

    LogicalTableModify newModifier = modifier;
    if (newInput != input) {
      newModifier =
          modifier.copy(
              modifier.getTraitSet(),
              Collections.singletonList(newInput));
    }
    assert newModifier.getClass() == modifier.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return result(newModifier, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan}.
   */
  public TrimResult trimFields(
      LogicalTableFunctionScan tabFun,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = tabFun.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final List<RelNode> newInputs = new ArrayList<>();

    for (RelNode input : tabFun.getInputs()) {
      final int inputFieldCount = input.getRowType().getFieldCount();
      ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range(inputFieldCount);

      // Create input with trimmed columns.
      final Set<RelDataTypeField> inputExtraFields =
          Collections.emptySet();
      TrimResult trimResult =
          trimChildRestore(
              tabFun, input, inputFieldsUsed, inputExtraFields);
      assert trimResult.right.isIdentity();
      newInputs.add(trimResult.left);
    }

    LogicalTableFunctionScan newTabFun = tabFun;
    if (!tabFun.getInputs().equals(newInputs)) {
      newTabFun = tabFun.copy(tabFun.getTraitSet(), newInputs,
          tabFun.getCall(), tabFun.getElementType(), tabFun.getRowType(),
          tabFun.getColumnMappings());
    }
    assert newTabFun.getClass() == tabFun.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return result(newTabFun, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalValues}.
   */
  public TrimResult trimFields(
      LogicalValues values,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = values.getRowType();
    final int fieldCount = rowType.getFieldCount();

    // If they are asking for no fields, we can't give them what they want,
    // because zero-column records are illegal. Give them the last field,
    // which is unlikely to be a system field.
    if (fieldsUsed.isEmpty()) {
      fieldsUsed = ImmutableBitSet.range(fieldCount - 1, fieldCount);
    }

    // If all fields are used, return unchanged.
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))) {
      Mapping mapping = Mappings.createIdentity(fieldCount);
      return result(values, mapping);
    }

    final ImmutableList.Builder<ImmutableList<RexLiteral>> newTuples =
        ImmutableList.builder();
    for (ImmutableList<RexLiteral> tuple : values.getTuples()) {
      ImmutableList.Builder<RexLiteral> newTuple = ImmutableList.builder();
      for (int field : fieldsUsed) {
        newTuple.add(tuple.get(field));
      }
      newTuples.add(newTuple.build());
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    final RelDataType newRowType =
        RelOptUtil.permute(values.getCluster().getTypeFactory(), rowType,
            mapping);
    final LogicalValues newValues =
        LogicalValues.create(values.getCluster(), newRowType,
            newTuples.build());
    return result(newValues, mapping);
  }

  protected Mapping createMapping(ImmutableBitSet fieldsUsed, int fieldCount) {
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality());
    int i = 0;
    for (int field : fieldsUsed) {
      mapping.set(field, i++);
    }
    return mapping;
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableScan}.
   */
  public TrimResult trimFields(
      final TableScan tableAccessRel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = tableAccessRel.getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
        && extraFields.isEmpty()) {
      // if there is nothing to project or if we are projecting everything
      // then no need to introduce another RelNode
      return trimFields(
          (RelNode) tableAccessRel, fieldsUsed, extraFields);
    }
    final RelNode newTableAccessRel =
        tableAccessRel.project(fieldsUsed, extraFields, REL_BUILDER.get());

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      RelNode input = newTableAccessRel;
      if (input instanceof Project) {
        // The table has implemented the project in the obvious way - by
        // creating project with 0 fields. Strip it away, and create our own
        // project with one field.
        Project project = (Project) input;
        if (project.getRowType().getFieldCount() == 0) {
          input = project.getInput();
        }
      }
      return dummyProject(fieldCount, input);
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    return result(newTableAccessRel, mapping);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Result of an attempt to trim columns from a relational expression.
   *
   * <p>The mapping describes where to find the columns wanted by the parent
   * of the current relational expression.
   *
   * <p>The mapping is a
   * {@link org.apache.calcite.util.mapping.Mappings.SourceMapping}, which means
   * that no column can be used more than once, and some columns are not used.
   * {@code columnsUsed.getSource(i)} returns the source of the i-th output
   * field.
   *
   * <p>For example, consider the mapping for a relational expression that
   * has 4 output columns but only two are being used. The mapping
   * {2 &rarr; 1, 3 &rarr; 0} would give the following behavior:
   *
   * <ul>
   * <li>columnsUsed.getSourceCount() returns 4
   * <li>columnsUsed.getTargetCount() returns 2
   * <li>columnsUsed.getSource(0) returns 3
   * <li>columnsUsed.getSource(1) returns 2
   * <li>columnsUsed.getSource(2) throws IndexOutOfBounds
   * <li>columnsUsed.getTargetOpt(3) returns 0
   * <li>columnsUsed.getTargetOpt(0) returns -1
   * </ul>
   */
  protected static class TrimResult extends Pair<RelNode, Mapping> {
    /**
     * Creates a TrimResult.
     *
     * @param left  New relational expression
     * @param right Mapping of fields onto original fields
     */
    public TrimResult(RelNode left, Mapping right) {
      super(left, right);
      assert right.getTargetCount() == left.getRowType().getFieldCount()
          : "rowType: " + left.getRowType() + ", mapping: " + right;
    }
  }
}

// End RelFieldTrimmer.java
