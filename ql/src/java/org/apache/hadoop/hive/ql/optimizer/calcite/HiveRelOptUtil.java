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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;


import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveRelOptUtil extends RelOptUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HiveRelOptUtil.class);


  /**
   * Splits out the equi-join (and optionally, a single non-equi) components
   * of a join condition, and returns what's left. Projection might be
   * required by the caller to provide join keys that are not direct field
   * references.
   *
   * @param sysFieldList  list of system fields
   * @param inputs        join inputs
   * @param condition     join condition
   * @param joinKeys      The join keys from the inputs which are equi-join
   *                      keys
   * @param filterNulls   The join key positions for which null values will not
   *                      match. null values only match for the "is not distinct
   *                      from" condition.
   * @param rangeOp       if null, only locate equi-joins; otherwise, locate a
   *                      single non-equi join predicate and return its operator
   *                      in this list; join keys associated with the non-equi
   *                      join predicate are at the end of the key lists
   *                      returned
   * @return What's left, never null
   * @throws CalciteSemanticException
   */
  public static RexNode splitHiveJoinCondition(
      List<RelDataTypeField> sysFieldList,
      List<RelNode> inputs,
      RexNode condition,
      List<List<RexNode>> joinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp) throws CalciteSemanticException {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitJoinCondition(
        sysFieldList,
        inputs,
        condition,
        joinKeys,
        filterNulls,
        rangeOp,
        nonEquiList);

    // Convert the remainders into a list that are AND'ed together.
    return RexUtil.composeConjunction(
        inputs.get(0).getCluster().getRexBuilder(), nonEquiList, false);
  }

  private static void splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      List<RelNode> inputs,
      RexNode condition,
      List<List<RexNode>> joinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp,
      List<RexNode> nonEquiList) throws CalciteSemanticException {
    final int sysFieldCount = sysFieldList.size();
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator() == SqlStdOperatorTable.AND) {
        for (RexNode operand : call.getOperands()) {
          splitJoinCondition(
              sysFieldList,
              inputs,
              operand,
              joinKeys,
              filterNulls,
              rangeOp,
              nonEquiList);
        }
        return;
      }

      RexNode leftKey = null;
      RexNode rightKey = null;
      int leftInput = 0;
      int rightInput = 0;
      List<RelDataTypeField> leftFields = null;
      List<RelDataTypeField> rightFields = null;
      boolean reverse = false;

      SqlKind kind = call.getKind();

      // Only consider range operators if we haven't already seen one
      if ((kind == SqlKind.EQUALS)
          || (filterNulls != null
          && kind == SqlKind.IS_NOT_DISTINCT_FROM)
          || (rangeOp != null
          && rangeOp.isEmpty()
          && (kind == SqlKind.GREATER_THAN
          || kind == SqlKind.GREATER_THAN_OR_EQUAL
          || kind == SqlKind.LESS_THAN
          || kind == SqlKind.LESS_THAN_OR_EQUAL))) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        final ImmutableBitSet projRefs0 = InputFinder.bits(op0);
        final ImmutableBitSet projRefs1 = InputFinder.bits(op1);

        final ImmutableBitSet[] inputsRange = new ImmutableBitSet[inputs.size()];
        int totalFieldCount = 0;
        for (int i = 0; i < inputs.size(); i++) {
          final int firstField = totalFieldCount + sysFieldCount;
          totalFieldCount = firstField + inputs.get(i).getRowType().getFieldCount();
          inputsRange[i] = ImmutableBitSet.range(firstField, totalFieldCount);
        }

        boolean foundBothInputs = false;
        for (int i = 0; i < inputs.size() && !foundBothInputs; i++) {
          if (projRefs0.intersects(inputsRange[i])
                  && projRefs0.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op0;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op0;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              reverse = true;
              foundBothInputs = true;
            }
          } else if (projRefs1.intersects(inputsRange[i])
                  && projRefs1.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op1;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op1;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              foundBothInputs = true;
            }
          }
        }

        if ((leftKey != null) && (rightKey != null)) {
          // adjustment array
          int[] adjustments = new int[totalFieldCount];
          for (int i = 0; i < inputs.size(); i++) {
            final int adjustment = inputsRange[i].nextSetBit(0);
            for (int j = adjustment; j < inputsRange[i].length(); j++) {
              adjustments[j] = -adjustment;
            }
          }

          // replace right Key input ref
          rightKey =
              rightKey.accept(
                  new RelOptUtil.RexInputConverter(
                      rexBuilder,
                      rightFields,
                      rightFields,
                      adjustments));

          // left key only needs to be adjusted if there are system
          // fields, but do it for uniformity
          leftKey =
              leftKey.accept(
                  new RelOptUtil.RexInputConverter(
                      rexBuilder,
                      leftFields,
                      leftFields,
                      adjustments));

          RelDataType leftKeyType = leftKey.getType();
          RelDataType rightKeyType = rightKey.getType();

          if (leftKeyType != rightKeyType) {
            // perform casting using Hive rules
            TypeInfo rType = TypeConverter.convert(rightKeyType);
            TypeInfo lType = TypeConverter.convert(leftKeyType);
            TypeInfo tgtType = FunctionRegistry.getCommonClassForComparison(lType, rType);

            if (tgtType == null) {
              throw new CalciteSemanticException(
                  "Cannot find common type for join keys "
                      + leftKey + " (type " + leftKeyType + ") and "
                      + rightKey + " (type " + rightKeyType + ")");
            }
            RelDataType targetKeyType = TypeConverter.convert(tgtType, rexBuilder.getTypeFactory());

            if (leftKeyType != targetKeyType && TypeInfoUtils.isConversionRequiredForComparison(tgtType, lType)) {
              leftKey =
                  rexBuilder.makeCast(targetKeyType, leftKey);
            }

            if (rightKeyType != targetKeyType && TypeInfoUtils.isConversionRequiredForComparison(tgtType, rType)) {
              rightKey =
                  rexBuilder.makeCast(targetKeyType, rightKey);
            }
          }
        }
      }

      if ((leftKey != null) && (rightKey != null)) {
        // found suitable join keys
        // add them to key list, ensuring that if there is a
        // non-equi join predicate, it appears at the end of the
        // key list; also mark the null filtering property
        addJoinKey(
            joinKeys.get(leftInput),
            leftKey,
            (rangeOp != null) && !rangeOp.isEmpty());
        addJoinKey(
            joinKeys.get(rightInput),
            rightKey,
            (rangeOp != null) && !rangeOp.isEmpty());
        if (filterNulls != null
            && kind == SqlKind.EQUALS) {
          // nulls are considered not matching for equality comparison
          // add the position of the most recently inserted key
          filterNulls.add(joinKeys.get(leftInput).size() - 1);
        }
        if (rangeOp != null
            && kind != SqlKind.EQUALS
            && kind != SqlKind.IS_DISTINCT_FROM) {
          if (reverse) {
            kind = reverse(kind);
          }
          rangeOp.add(op(kind, call.getOperator()));
        }
        return;
      } // else fall through and add this condition as nonEqui condition
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  private static SqlKind reverse(SqlKind kind) {
    switch (kind) {
    case GREATER_THAN:
      return SqlKind.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlKind.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlKind.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlKind.GREATER_THAN_OR_EQUAL;
    default:
      return kind;
    }
  }

  private static void addJoinKey(
      List<RexNode> joinKeyList,
      RexNode key,
      boolean preserveLastElementInList) {
    if (!joinKeyList.isEmpty() && preserveLastElementInList) {
      joinKeyList.add(joinKeyList.size() - 1, key);
    } else {
      joinKeyList.add(key);
    }
  }

  /**
   * Creates a relational expression that projects the given fields of the
   * input.
   *
   * <p>Optimizes if the fields are the identity projection.
   *
   * @param relBuilder RelBuilder
   * @param child Input relational expression
   * @param posList Source of each projected field
   * @return Relational expression that projects given fields
   */
  public static RelNode createProject(final RelBuilder relBuilder,
      final RelNode child, final List<Integer> posList) {
    RelDataType rowType = child.getRowType();
    final List<String> fieldNames = rowType.getFieldNames();
    final RexBuilder rexBuilder = child.getCluster().getRexBuilder();
    return createProject(child,
        new AbstractList<RexNode>() {
          public int size() {
            return posList.size();
          }

          public RexNode get(int index) {
            final int pos = posList.get(index);
            return rexBuilder.makeInputRef(child, pos);
          }
        },
        new AbstractList<String>() {
          public int size() {
            return posList.size();
          }

          public String get(int index) {
            final int pos = posList.get(index);
            return fieldNames.get(pos);
          }
        }, true, relBuilder);
  }

  public static RexNode splitCorrelatedFilterCondition(
      Filter filter,
      List<RexNode> joinKeys,
      List<RexNode> correlatedJoinKeys,
      boolean extractCorrelatedFieldAccess) {
    final List<RexNode> nonEquiList = new ArrayList<>();

    splitCorrelatedFilterCondition(
        filter,
        filter.getCondition(),
        joinKeys,
        correlatedJoinKeys,
        nonEquiList,
        extractCorrelatedFieldAccess);

    // Convert the remainders into a list that are AND'ed together.
    return RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), nonEquiList, true);
  }

  private static void splitCorrelatedFilterCondition(
      Filter filter,
      RexNode condition,
      List<RexNode> joinKeys,
      List<RexNode> correlatedJoinKeys,
      List<RexNode> nonEquiList,
      boolean extractCorrelatedFieldAccess) {
    if (condition instanceof RexCall) {
      RexCall call = (RexCall) condition;
      if (call.getOperator().getKind() == SqlKind.AND) {
        for (RexNode operand : call.getOperands()) {
          splitCorrelatedFilterCondition(
              filter,
              operand,
              joinKeys,
              correlatedJoinKeys,
              nonEquiList,
              extractCorrelatedFieldAccess);
        }
        return;
      }

      if (call.getOperator().getKind() == SqlKind.EQUALS) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        if (extractCorrelatedFieldAccess) {
          if (!RexUtil.containsFieldAccess(op0)
              && (op1 instanceof RexFieldAccess)) {
            joinKeys.add(op0);
            correlatedJoinKeys.add(op1);
            return;
          } else if (
              (op0 instanceof RexFieldAccess)
                  && !RexUtil.containsFieldAccess(op1)) {
            correlatedJoinKeys.add(op0);
            joinKeys.add(op1);
            return;
          }
        } else {
          if (!(RexUtil.containsInputRef(op0))
              && (op1 instanceof RexInputRef)) {
            correlatedJoinKeys.add(op0);
            joinKeys.add(op1);
            return;
          } else if (
              (op0 instanceof RexInputRef)
                  && !(RexUtil.containsInputRef(op1))) {
            joinKeys.add(op0);
            correlatedJoinKeys.add(op1);
            return;
          }
        }
      }
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  /**
   * Creates a LogicalAggregate that removes all duplicates from the result of
   * an underlying relational expression.
   *
   * @param rel underlying rel
   * @return rel implementing SingleValueAgg
   */
  public static RelNode createSingleValueAggRel(
      RelOptCluster cluster,
      RelNode rel,
      RelFactories.AggregateFactory aggregateFactory) {
    // assert (rel.getRowType().getFieldCount() == 1);
    final int aggCallCnt = rel.getRowType().getFieldCount();
    final List<AggregateCall> aggCalls = new ArrayList<>();

    for (int i = 0; i < aggCallCnt; i++) {
      aggCalls.add(
          AggregateCall.create(
              SqlStdOperatorTable.SINGLE_VALUE, false, false,
              ImmutableList.of(i), -1, 0, rel, null, null));
    }

    return aggregateFactory.createAggregate(rel, Collections.emptyList(), ImmutableBitSet.of(), null, aggCalls);
  }

  /**
   * Given a RelNode, it checks whether there is any filtering condition
   * below. Basically we check whether the operators
   * below altered the PK cardinality in any way
   */
  public static boolean isRowFilteringPlan(final RelMetadataQuery mq, RelNode operator) {
    final Multimap<Class<? extends RelNode>, RelNode> nodesBelowNonFkInput =
        mq.getNodeTypes(operator);
    for (Entry<Class<? extends RelNode>, Collection<RelNode>> e :
        nodesBelowNonFkInput.asMap().entrySet()) {
      if (e.getKey() == Project.class) {
        // It does not alter cardinality, continue
        continue;
      }

      if (e.getKey() == TableScan.class) {
        if (e.getValue().size() > 1) {
          // Bail out as we may not have more than one TS on non-FK side
          return true;
        }
      } else if (e.getKey() == Aggregate.class) {
        // We check there is are not grouping sets
        for (RelNode node : e.getValue()) {
          Aggregate a = (Aggregate) node;
          if (a.getGroupType() != Group.SIMPLE) {
            // Bail out as it may change cardinality
            return true;
          }
        }
      } else if (e.getKey() == Sort.class) {
        // We check whether there is a limit clause
        for (RelNode node : e.getValue()) {
          Sort s = (Sort) node;
          if (s.fetch != null || s.offset != null) {
            // Bail out as it may change cardinality
            return true;
          }
        }
      } else {
        // Bail out, we cannot rewrite the expression if non-fk side cardinality
        // is being altered
        return true;
      }
    }
    // It passed all the tests
    return false;
  }

  /**
   * Returns a triple where first value represents whether we could extract a FK-PK join
   * or not, the second value is a pair with the column from left and right input that
   * are used for the FK-PK join, and the third value are the predicates that are not
   * part of the FK-PK condition. Currently we can only extract one FK-PK join.
   */
  public static PKFKJoinInfo extractPKFKJoin(
      Join join, List<RexNode> joinFilters, boolean leftInputPotentialFK, RelMetadataQuery mq) {
    final List<RexNode> residualPreds = new ArrayList<>();
    final JoinRelType joinType = join.getJoinType();
    final RelNode fkInput = leftInputPotentialFK ? join.getLeft() : join.getRight();
    final PKFKJoinInfo cannotExtract =
        PKFKJoinInfo.of(false, null, null);

    if (joinType != JoinRelType.INNER) {
      // If it is not an inner, we transform it as the metadata
      // providers for expressions do not pull information through
      // outer join (as it would not be correct)
      join = join.copy(join.getTraitSet(), join.getCluster().getRexBuilder().makeLiteral(true),
          join.getLeft(), join.getRight(), JoinRelType.INNER, false);
    }

    // 1) Gather all tables from the FK side and the table from the
    // non-FK side
    final Set<RelTableRef> leftTables = mq.getTableReferences(join.getLeft());
    if (leftTables == null) {
      // Could not infer, bail out
      return cannotExtract;
    }
    final Set<RelTableRef> joinTables = mq.getTableReferences(join);
    if (joinTables == null) {
      // Could not infer, bail out
      return cannotExtract;
    }
    final Set<RelTableRef> rightTables = Sets.difference(joinTables, leftTables);
    final Set<RelTableRef> fkTables = join.getLeft() == fkInput ? leftTables : rightTables;
    final Set<RelTableRef> nonFkTables = join.getLeft() == fkInput ? rightTables : leftTables;

    // 2) Check whether there is a FK relationship
    Set<RexCall> candidatePredicates = new HashSet<>();
    EquivalenceClasses ec = new EquivalenceClasses();
    for (RexNode conj : joinFilters) {
      if (!conj.isA(SqlKind.EQUALS)) {
        // Not an equality, continue
        residualPreds.add(conj);
        continue;
      }
      RexCall equiCond = (RexCall) conj;
      RexNode eqOp1 = equiCond.getOperands().get(0);
      if (!RexUtil.isReferenceOrAccess(eqOp1, true)) {
        // Ignore
        residualPreds.add(conj);
        continue;
      }
      Set<RexNode> eqOp1ExprsLineage = mq.getExpressionLineage(join, eqOp1);
      if (eqOp1ExprsLineage == null) {
        // Cannot be mapped, continue
        residualPreds.add(conj);
        continue;
      }
      RexNode eqOp2 = equiCond.getOperands().get(1);
      if (!RexUtil.isReferenceOrAccess(eqOp2, true)) {
        // Ignore
        residualPreds.add(conj);
        continue;
      }
      Set<RexNode> eqOp2ExprsLineage = mq.getExpressionLineage(join, eqOp2);
      if (eqOp2ExprsLineage == null) {
        // Cannot be mapped, continue
        residualPreds.add(conj);
        continue;
      }
      List<RexTableInputRef> eqOp2ExprsFiltered = null;
      for (RexNode eqOpExprLineage1 : eqOp1ExprsLineage) {
        RexTableInputRef inputRef1 = extractTableInputRef(eqOpExprLineage1);
        if (inputRef1 == null) {
          // This condition could not be map into an input reference
          continue;
        }
        if (eqOp2ExprsFiltered == null) {
          // First iteration
          eqOp2ExprsFiltered = new ArrayList<>();
          for (RexNode eqOpExprLineage2 : eqOp2ExprsLineage) {
            RexTableInputRef inputRef2 = extractTableInputRef(eqOpExprLineage2);
            if (inputRef2 == null) {
              // Bail out as this condition could not be map into an input reference
              continue;
            }
            // Add to list of expressions for follow-up iterations
            eqOp2ExprsFiltered.add(inputRef2);
            // Add to equivalence classes and backwards mapping
            ec.addEquivalence(inputRef1, inputRef2, equiCond);
            candidatePredicates.add(equiCond);
          }
        } else {
          // Rest of iterations, only adding, no checking
          for (RexTableInputRef inputRef2 : eqOp2ExprsFiltered) {
            ec.addEquivalence(inputRef1, inputRef2, equiCond);
          }
        }
      }
      if (!candidatePredicates.contains(conj)) {
        // We add it to residual already
        residualPreds.add(conj);
      }
    }
    if (ec.getEquivalenceClassesMap().isEmpty()) {
      // This may be a cartesian product, we bail out
      return cannotExtract;
    }

    // 4) For each table, check whether there is a matching on the non-FK side.
    // If there is and it is the only condition, we are ready to transform
    for (final RelTableRef nonFkTable : nonFkTables) {
      final List<String> nonFkTableQName = nonFkTable.getQualifiedName();
      for (RelTableRef tRef : fkTables) {
        List<RelReferentialConstraint> constraints = tRef.getTable().getReferentialConstraints();
        for (RelReferentialConstraint constraint : constraints) {
          if (constraint.getTargetQualifiedName().equals(nonFkTableQName)) {
            EquivalenceClasses ecT = EquivalenceClasses.copy(ec);
            Set<RexNode> removedOriginalPredicates = new HashSet<>();
            ImmutableBitSet.Builder lBitSet = ImmutableBitSet.builder();
            ImmutableBitSet.Builder rBitSet = ImmutableBitSet.builder();
            boolean allContained = true;
            for (int pos = 0; pos < constraint.getNumColumns(); pos++) {
              int foreignKeyPos = constraint.getColumnPairs().get(pos).source;
              RelDataType foreignKeyColumnType =
                  tRef.getTable().getRowType().getFieldList().get(foreignKeyPos).getType();
              RexTableInputRef foreignKeyColumnRef =
                  RexTableInputRef.of(tRef, foreignKeyPos, foreignKeyColumnType);
              int uniqueKeyPos = constraint.getColumnPairs().get(pos).target;
              RexTableInputRef uniqueKeyColumnRef = RexTableInputRef.of(nonFkTable, uniqueKeyPos,
                  nonFkTable.getTable().getRowType().getFieldList().get(uniqueKeyPos).getType());
              if (ecT.getEquivalenceClassesMap().containsKey(uniqueKeyColumnRef) &&
                  ecT.getEquivalenceClassesMap().get(uniqueKeyColumnRef).contains(foreignKeyColumnRef)) {
                // Remove this condition from eq classes as we have checked that it is present
                // in the join condition. In turn, populate the columns that are referenced
                // from the join inputs
                for (RexCall originalPred : ecT.removeEquivalence(uniqueKeyColumnRef, foreignKeyColumnRef)) {
                  ImmutableBitSet leftCols = RelOptUtil.InputFinder.bits(originalPred.getOperands().get(0));
                  ImmutableBitSet rightCols = RelOptUtil.InputFinder.bits(originalPred.getOperands().get(1));
                  // Get length and flip column references if join condition specified in
                  // reverse order to join sources
                  int nFieldsLeft = join.getLeft().getRowType().getFieldList().size();
                  int nFieldsRight = join.getRight().getRowType().getFieldList().size();
                  int nSysFields = join.getSystemFieldList().size();
                  ImmutableBitSet rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
                      nSysFields + nFieldsLeft + nFieldsRight);
                  if (rightFieldsBitSet.contains(leftCols)) {
                    ImmutableBitSet t = leftCols;
                    leftCols = rightCols;
                    rightCols = t;
                  }
                  lBitSet.set(leftCols.nextSetBit(0) - nSysFields);
                  rBitSet.set(rightCols.nextSetBit(0) - (nSysFields + nFieldsLeft));
                  removedOriginalPredicates.add(originalPred);
                }
              } else {
                // No relationship, we cannot do anything
                allContained = false;
                break;
              }
            }
            if (allContained) {
              // This is a PK-FK, reassign equivalence classes and remove conditions
              // TODO: Support inference of multiple PK-FK relationships

              // 4.1) Add to residual whatever is remaining
              candidatePredicates.removeAll(removedOriginalPredicates);
              residualPreds.addAll(candidatePredicates);
              // 4.2) Return result
              return PKFKJoinInfo.of(true, Pair.of(lBitSet.build(), rBitSet.build()), residualPreds);
            }
          }
        }
      }
    }

    return cannotExtract;
  }

  public static class PKFKJoinInfo {
    public final boolean isPkFkJoin;
    public final Pair<ImmutableBitSet, ImmutableBitSet> pkFkJoinColumns;
    public final List<RexNode> additionalPredicates;

    private PKFKJoinInfo(boolean isPkFkJoin, Pair<ImmutableBitSet, ImmutableBitSet> pkFkJoinColumns,
          List<RexNode> additionalPredicates) {
      this.isPkFkJoin = isPkFkJoin;
      this.pkFkJoinColumns = pkFkJoinColumns;
      this.additionalPredicates = additionalPredicates == null ? null :
          ImmutableList.copyOf(additionalPredicates);
    }

    public static PKFKJoinInfo of(boolean isPkFkJoin, Pair<ImmutableBitSet, ImmutableBitSet> pkFkJoinColumns,
         List<RexNode> additionalPredicates) {
      return new PKFKJoinInfo(isPkFkJoin, pkFkJoinColumns, additionalPredicates);
    }
  }

  public static RewritablePKFKJoinInfo isRewritablePKFKJoin(Join join,
        final RelNode fkInput, final RelNode nonFkInput,
        RelMetadataQuery mq) {
    final JoinRelType joinType = join.getJoinType();
    final RexNode cond = join.getCondition();

    Preconditions.checkArgument(fkInput == join.getLeft()
        || fkInput == join.getRight(), "Invalid input: " + fkInput);
    Preconditions.checkArgument(nonFkInput == join.getLeft()
        || nonFkInput == join.getRight(), "Invalid input: " + nonFkInput);

    final RewritablePKFKJoinInfo nonRewritable = RewritablePKFKJoinInfo.of(false, null);

    // TODO : Need to handle Anti join.
    // https://issues.apache.org/jira/browse/HIVE-23906
    if (joinType == JoinRelType.ANTI) {
      return nonRewritable;
    }

    if (joinType != JoinRelType.INNER && !join.isSemiJoin()) {
      // If it is not an inner, we transform it as the metadata
      // providers for expressions do not pull information through
      // outer join (as it would not be correct)
      join = join.copy(join.getTraitSet(), cond,
          join.getLeft(), join.getRight(), JoinRelType.INNER,
          false);
    }

    // 1) Check whether there is any filtering condition on the
    // non-FK side. Basically we check whether the operators
    // below altered the PK cardinality in any way
    if (HiveRelOptUtil.isRowFilteringPlan(mq, nonFkInput)) {
      return nonRewritable;
    }

    // 2) Check whether there is an FK relationship
    final Map<RexTableInputRef, RexNode> refToRex = new HashMap<>();
    final EquivalenceClasses ec = new EquivalenceClasses();
    for (RexNode conj : RelOptUtil.conjunctions(cond)) {
      if (!conj.isA(SqlKind.EQUALS)) {
        // Not an equality, we bail out
        return nonRewritable;
      }
      RexCall equiCond = (RexCall) conj;
      RexNode eqOp1 = equiCond.getOperands().get(0);
      Set<RexNode> eqOp1ExprsLineage = mq.getExpressionLineage(join, eqOp1);
      if (eqOp1ExprsLineage == null) {
        // Cannot be mapped, bail out
        return nonRewritable;
      }
      RexNode eqOp2 = equiCond.getOperands().get(1);
      Set<RexNode> eqOp2ExprsLineage = mq.getExpressionLineage(join, eqOp2);
      if (eqOp2ExprsLineage == null) {
        // Cannot be mapped, bail out
        return nonRewritable;
      }
      List<RexTableInputRef> eqOp2ExprsFiltered = null;
      for (RexNode eqOpExprLineage1 : eqOp1ExprsLineage) {
        RexTableInputRef inputRef1 = extractTableInputRef(eqOpExprLineage1);
        if (inputRef1 == null) {
          // Bail out as this condition could not be map into an input reference
          return nonRewritable;
        }
        refToRex.put(inputRef1, eqOp1);
        if (eqOp2ExprsFiltered == null) {
          // First iteration
          eqOp2ExprsFiltered = new ArrayList<>();
          for (RexNode eqOpExprLineage2 : eqOp2ExprsLineage) {
            RexTableInputRef inputRef2 = extractTableInputRef(eqOpExprLineage2);
            if (inputRef2 == null) {
              // Bail out as this condition could not be map into an input reference
              return nonRewritable;
            }
            // Add to list of expressions for follow-up iterations
            eqOp2ExprsFiltered.add(inputRef2);
            // Add to equivalence classes and backwards mapping
            ec.addEquivalence(inputRef1, inputRef2);
            refToRex.put(inputRef2, eqOp2);
          }
        } else {
          // Rest of iterations, only adding, no checking
          for (RexTableInputRef inputRef2 : eqOp2ExprsFiltered) {
            ec.addEquivalence(inputRef1, inputRef2);
          }
        }
      }
    }
    if (ec.getEquivalenceClassesMap().isEmpty()) {
      // This may be a cartesian product, we bail out
      return nonRewritable;
    }

    // 3) Gather all tables from the FK side and the table from the
    // non-FK side
    final Set<RelTableRef> leftTables = mq.getTableReferences(join.getLeft());
    final Set<RelTableRef> rightTables =
        Sets.difference(mq.getTableReferences(join), mq.getTableReferences(join.getLeft()));
    final Set<RelTableRef> fkTables = join.getLeft() == fkInput ? leftTables : rightTables;
    final Set<RelTableRef> nonFkTables = join.getLeft() == fkInput ? rightTables : leftTables;
    assert nonFkTables.size() == 1;
    final RelTableRef nonFkTable = nonFkTables.iterator().next();
    final List<String> nonFkTableQName = nonFkTable.getQualifiedName();

    // 4) For each table, check whether there is a matching on the non-FK side.
    // If there is and it is the only condition, we are ready to transform
    boolean canBeRewritten = false;
    List<RexNode> nullableNodes = null;
    for (RelTableRef tRef : fkTables) {
      List<RelReferentialConstraint> constraints = tRef.getTable().getReferentialConstraints();
      for (RelReferentialConstraint constraint : constraints) {
        if (constraint.getTargetQualifiedName().equals(nonFkTableQName)) {
          nullableNodes = new ArrayList<>();
          EquivalenceClasses ecT = EquivalenceClasses.copy(ec);
          boolean allContained = true;
          for (int pos = 0; pos < constraint.getNumColumns(); pos++) {
            int foreignKeyPos = constraint.getColumnPairs().get(pos).source;
            RelDataType foreignKeyColumnType =
                tRef.getTable().getRowType().getFieldList().get(foreignKeyPos).getType();
            RexTableInputRef foreignKeyColumnRef =
                RexTableInputRef.of(tRef, foreignKeyPos, foreignKeyColumnType);
            int uniqueKeyPos = constraint.getColumnPairs().get(pos).target;
            RexTableInputRef uniqueKeyColumnRef = RexTableInputRef.of(nonFkTable, uniqueKeyPos,
                nonFkTable.getTable().getRowType().getFieldList().get(uniqueKeyPos).getType());
            if (ecT.getEquivalenceClassesMap().containsKey(uniqueKeyColumnRef) &&
                ecT.getEquivalenceClassesMap().get(uniqueKeyColumnRef).contains(foreignKeyColumnRef)) {
              if (foreignKeyColumnType.isNullable()) {
                //TODO : Handle Anti Join. https://issues.apache.org/jira/browse/HIVE-23906
                if (joinType == JoinRelType.INNER || join.isSemiJoin()) {
                  // If it is nullable and it is an INNER, we just need a IS NOT NULL filter
                  RexNode originalCondOp = refToRex.get(foreignKeyColumnRef);
                  assert originalCondOp != null;
                  nullableNodes.add(originalCondOp);
                } else {
                  // If it is nullable and this is not an INNER, we cannot execute any transformation
                  allContained = false;
                  break;
                }
              }
              // Remove this condition from eq classes as we have checked that it is present
              // in the join condition
              ecT.removeEquivalence(uniqueKeyColumnRef, foreignKeyColumnRef);
            } else {
              // No relationship, we cannot do anything
              allContained = false;
              break;
            }
          }
          if (allContained && ecT.getEquivalenceClassesMap().isEmpty()) {
            // We made it
            canBeRewritten = true;
            break;
          }
        }
      }
    }

    return RewritablePKFKJoinInfo.of(canBeRewritten, nullableNodes);
  }

  public static class RewritablePKFKJoinInfo {
    public final boolean rewritable;
    public final List<RexNode> nullableNodes;

    private RewritablePKFKJoinInfo(boolean rewritable, List<RexNode> nullableNodes) {
      this.rewritable = rewritable;
      this.nullableNodes = nullableNodes == null ? null :
          ImmutableList.copyOf(nullableNodes);
    }

    public static RewritablePKFKJoinInfo of(boolean rewritable, List<RexNode> nullableNodes) {
      return new RewritablePKFKJoinInfo(rewritable, nullableNodes);
    }
  }

  private static RexTableInputRef extractTableInputRef(RexNode node) {
    RexTableInputRef ref = null;
    if (node instanceof RexTableInputRef) {
      ref = (RexTableInputRef) node;
    } else if (RexUtil.isLosslessCast(node) &&
        ((RexCall) node).getOperands().get(0) instanceof RexTableInputRef) {
      ref = (RexTableInputRef) ((RexCall) node).getOperands().get(0);
    }
    return ref;
  }

  /**
   * Class representing an equivalence class, i.e., a set of equivalent columns
   *
   * TODO: This is a subset of a private class in materialized view rewriting
   * in Calcite. It should be moved to its own class in Calcite so it can be
   * accessible here.
   */
  private static class EquivalenceClasses {

    // Contains the node to equivalence class nodes
    private final Map<RexTableInputRef, Set<RexTableInputRef>> nodeToEquivalenceClass;
    // Contains the pair of equivalences to original expression that they originate from
    private final Multimap<Pair<RexTableInputRef, RexTableInputRef>, RexCall> equivalenceToOriginalNode;

    protected EquivalenceClasses() {
      nodeToEquivalenceClass = new HashMap<>();
      equivalenceToOriginalNode = HashMultimap.create();
    }

    protected void addEquivalence(RexTableInputRef p1, RexTableInputRef p2, RexCall originalCond) {
      addEquivalence(p1, p2);
      equivalenceToOriginalNode.put(Pair.of(p1, p2), originalCond);
      equivalenceToOriginalNode.put(Pair.of(p2, p1), originalCond);
    }

    protected void addEquivalence(RexTableInputRef p1, RexTableInputRef p2) {
      Set<RexTableInputRef> c1 = nodeToEquivalenceClass.get(p1);
      Set<RexTableInputRef> c2 = nodeToEquivalenceClass.get(p2);
      if (c1 != null && c2 != null) {
        // Both present, we need to merge
        if (c1.size() < c2.size()) {
          // We swap them to merge
          Set<RexTableInputRef> c2Temp = c2;
          c2 = c1;
          c1 = c2Temp;
        }
        for (RexTableInputRef newRef : c2) {
          c1.add(newRef);
          nodeToEquivalenceClass.put(newRef, c1);
        }
      } else if (c1 != null) {
        // p1 present, we need to merge into it
        c1.add(p2);
        nodeToEquivalenceClass.put(p2, c1);
      } else if (c2 != null) {
        // p2 present, we need to merge into it
        c2.add(p1);
        nodeToEquivalenceClass.put(p1, c2);
      } else {
        // None are present, add to same equivalence class
        Set<RexTableInputRef> equivalenceClass = new LinkedHashSet<>();
        equivalenceClass.add(p1);
        equivalenceClass.add(p2);
        nodeToEquivalenceClass.put(p1, equivalenceClass);
        nodeToEquivalenceClass.put(p2, equivalenceClass);
      }
    }

    protected Map<RexTableInputRef, Set<RexTableInputRef>> getEquivalenceClassesMap() {
      return nodeToEquivalenceClass;
    }

    // Returns the original nodes that the equivalences were generated from
    protected Set<RexCall> removeEquivalence(RexTableInputRef p1, RexTableInputRef p2) {
      nodeToEquivalenceClass.get(p1).remove(p2);
      if (nodeToEquivalenceClass.get(p1).size() == 1) { // self
        nodeToEquivalenceClass.remove(p1);
      }
      nodeToEquivalenceClass.get(p2).remove(p1);
      if (nodeToEquivalenceClass.get(p2).size() == 1) { // self
        nodeToEquivalenceClass.remove(p2);
      }
      Set<RexCall> originalNodes = new HashSet<>();
      originalNodes.addAll(equivalenceToOriginalNode.removeAll(Pair.of(p1, p2)));
      originalNodes.addAll(equivalenceToOriginalNode.removeAll(Pair.of(p2, p1)));
      return originalNodes;
    }

    protected static EquivalenceClasses copy(EquivalenceClasses ec) {
      final EquivalenceClasses newEc = new EquivalenceClasses();
      for (Entry<RexTableInputRef, Set<RexTableInputRef>> e : ec.nodeToEquivalenceClass.entrySet()) {
        newEc.nodeToEquivalenceClass.put(e.getKey(), Sets.newLinkedHashSet(e.getValue()));
      }
      for (Entry<Pair<RexTableInputRef, RexTableInputRef>, Collection<RexCall>> e : ec.equivalenceToOriginalNode.asMap().entrySet()) {
        newEc.equivalenceToOriginalNode.putAll(e.getKey(), e.getValue());
      }
      return newEc;
    }
  }

  public static Pair<RelOptTable, List<Integer>> getColumnOriginSet(RelNode rel, ImmutableBitSet colSet) {
    RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    Map<RelTableRef, List<Integer>> tabToOriginColumns = new HashMap<>();
    for(int col:colSet) {
      final RexInputRef tempColRef =  rexBuilder.makeInputRef(rel, col);
      Set<RexNode> columnOrigins = mq.getExpressionLineage(rel, tempColRef);
      if (null == columnOrigins || columnOrigins.isEmpty()) {
        // if even on
        return null;
      }
      // we have either one or multiple origins of the column, we need to make sure that all of the column
      for (RexNode orgCol : columnOrigins) {
        RexTableInputRef inputRef = extractTableInputRef(orgCol);
        if(inputRef == null) {
          return null;
        }
        List<Integer> cols = tabToOriginColumns.get(inputRef.getTableRef());
        if (cols == null) {
          cols = new ArrayList<>();
        }
        cols.add(inputRef.getIndex());
        tabToOriginColumns.put(inputRef.getTableRef(), cols);
      }
    }

    // return the first table which has same number of backtracked columns as colSet
    // ideally we should return all, in case one doesn't work we can fall back to another
    for(Entry<RelTableRef, List<Integer>> mapEntries: tabToOriginColumns.entrySet()) {
      RelTableRef tblRef = mapEntries.getKey();
      List<Integer> mapColList = mapEntries.getValue();
      if(mapColList.size() == colSet.cardinality()) {
        RelOptTable tbl = tblRef.getTable();
        return Pair.of(tbl, mapColList);
      }
    }
    return null;
  }

  /**
   * Converts a relational expression to a string, showing information that will aid
   * to parse the string back.
   */
  public static String toJsonString(final RelNode rel) {
    if (rel == null) {
      return null;
    }

    final HiveRelJsonImpl planWriter = new HiveRelJsonImpl();
    rel.explain(planWriter);
    return planWriter.asString();
  }

  /**
   * Map Sort and SortExchange keys to the specified Project columns.
   * @param project the Project
   * @param sortCollation current collation in Sort
   * @param cluster RelOptCluster
   * @return new collation should be used in the Sort
   */
  public static List<RelFieldCollation> getNewRelFieldCollations(
          HiveProject project, RelCollation sortCollation, RelOptCluster cluster) {
    // Determine mapping between project input and output fields.
    // In Hive, Sort is always based on RexInputRef
    // HiveSort*PullUpConstantsRule should remove constants (RexLiteral)
    // We only need to check if project can contain all the positions that sortCollation needs.
    final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                    project.getProjects(), project.getInput().getRowType()).inverse();
    Set<Integer> needed = new HashSet<>();
    for (RelFieldCollation fc : sortCollation.getFieldCollations()) {
      needed.add(fc.getFieldIndex());
      final RexNode node = project.getProjects().get(map.getTarget(fc.getFieldIndex()));
      if (node.isA(SqlKind.CAST)) {
        // Check whether it is a monotonic preserving cast, otherwise we cannot push
        final RexCall cast = (RexCall) node;
        final RexCallBinding binding =
                RexCallBinding.create(cluster.getTypeFactory(), cast,
                        ImmutableList.of(RexUtil.apply(map, sortCollation)));
        if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
          return null;
        }
      }
    }
    Map<Integer, Integer> m = new HashMap<>();
    for (int projPos = 0; projPos < project.getProjects().size(); projPos++) {
      RexNode expr = project.getProjects().get(projPos);
      if (expr instanceof RexInputRef) {
        Set<Integer> positions = HiveCalciteUtil.getInputRefs(expr);
        if (positions.size() <= 1) {
          int parentPos = positions.iterator().next();
          if(needed.contains(parentPos)){
            m.put(parentPos, projPos);
            needed.remove(parentPos);
          }
        }
      }
    }
    if(!needed.isEmpty()){
      return null;
    }

    List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (RelFieldCollation fc : sortCollation.getFieldCollations()) {
      fieldCollations.add(new RelFieldCollation(m.get(fc.getFieldIndex()), fc.direction, fc.nullDirection));
    }
    return fieldCollations;
  }

  /**
   * Map Exchange distribution keys to the specified Project columns.
   * @param project the Project
   * @param distribution current distribution in Exchange
   * @return new distribution should be used in the Exchange
   */
  public static List<Integer> getNewRelDistributionKeys(
          HiveProject project, RelDistribution distribution) {
    Set<Integer> needed = new HashSet<>(distribution.getKeys());
    Map<Integer, Integer> m = new HashMap<>();
    for (int projPos = 0; projPos < project.getProjects().size(); projPos++) {
      RexNode expr = project.getProjects().get(projPos);
      if (expr instanceof RexInputRef) {
        Set<Integer> positions = HiveCalciteUtil.getInputRefs(expr);
        if (positions.size() <= 1) {
          int parentPos = positions.iterator().next();
          if(needed.contains(parentPos)){
            m.put(parentPos, projPos);
            needed.remove(parentPos);
          }
        }
      }
    }
    if(!needed.isEmpty()){
      return null;
    }

    List<Integer> distributionKeys = new ArrayList<>();
    for (Integer keyIndex : distribution.getKeys()) {
      distributionKeys.add(m.get(keyIndex));
    }
    return distributionKeys;
  }
}
