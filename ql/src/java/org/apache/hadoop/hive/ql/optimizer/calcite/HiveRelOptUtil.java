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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;


public class HiveRelOptUtil extends RelOptUtil {

  /**
   * Splits out the equi-join (and optionally, a single non-equi) components
   * of a join condition, and returns what's left. Projection might be
   * required by the caller to provide join keys that are not direct field
   * references.
   *
   * @param sysFieldList  list of system fields
   * @param leftRel       left join input
   * @param rightRel      right join input
   * @param condition     join condition
   * @param leftJoinKeys  The join keys from the left input which are equi-join
   *                      keys
   * @param rightJoinKeys The join keys from the right input which are
   *                      equi-join keys
   * @param filterNulls   The join key positions for which null values will not
   *                      match. null values only match for the "is not distinct
   *                      from" condition.
   * @param rangeOp       if null, only locate equi-joins; otherwise, locate a
   *                      single non-equi join predicate and return its operator
   *                      in this list; join keys associated with the non-equi
   *                      join predicate are at the end of the key lists
   *                      returned
   * @return What's left, never null
   */
  public static RexNode splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      RelNode leftRel,
      RelNode rightRel,
      RexNode condition,
      List<RexNode> leftJoinKeys,
      List<RexNode> rightJoinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp) {
    return splitJoinCondition(
        sysFieldList,
        ImmutableList.of(leftRel, rightRel),
        condition,
        ImmutableList.of(leftJoinKeys, rightJoinKeys),
        filterNulls,
        rangeOp);
  }

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
   */
  public static RexNode splitJoinCondition(
      List<RelDataTypeField> sysFieldList,
      List<RelNode> inputs,
      RexNode condition,
      List<List<RexNode>> joinKeys,
      List<Integer> filterNulls,
      List<SqlOperator> rangeOp) {
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
      List<RexNode> nonEquiList) {
    final int sysFieldCount = sysFieldList.size();
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    int[] firstFieldInputs = new int[inputs.size()];
    int totalFieldCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      firstFieldInputs[i] = totalFieldCount + sysFieldCount;
      totalFieldCount += sysFieldCount
              + inputs.get(i).getRowType().getFieldCount();
    }

    // adjustment array
    int[] adjustments = new int[totalFieldCount];
    for (int i = 0; i < inputs.size(); i++) {
      int limit = i == inputs.size() - 1
              ? totalFieldCount : firstFieldInputs[i + 1];
      for (int j = firstFieldInputs[i]; j < limit; j++) {
        adjustments[j] = -firstFieldInputs[i];
      }
    }

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

        boolean foundBothInputs = false;
        for (int i = 0; i < inputs.size() && !foundBothInputs; i++) {
          final int lowerLimit = firstFieldInputs[i];
          final int upperLimit = i == inputs.size() - 1
                  ? totalFieldCount : firstFieldInputs[i + 1];
          if (projRefs0.nextSetBit(lowerLimit) != -1
                  && projRefs0.nextSetBit(upperLimit) == -1
                  && projRefs0.nextSetBit(0) == projRefs0.nextSetBit(lowerLimit)
                  && projRefs0.nextSetBit(lowerLimit) < upperLimit) {
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
          } else if (projRefs1.nextSetBit(lowerLimit) != -1
                  && projRefs1.nextSetBit(upperLimit) == -1
                  && projRefs1.nextSetBit(0) == projRefs1.nextSetBit(lowerLimit)
                  && projRefs1.nextSetBit(lowerLimit) < upperLimit) {
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
            // perform casting
            RelDataType targetKeyType =
                typeFactory.leastRestrictive(
                    ImmutableList.of(leftKeyType, rightKeyType));

            if (targetKeyType == null) {
              throw Util.newInternal(
                  "Cannot find common type for join keys "
                  + leftKey + " (type " + leftKeyType + ") and "
                  + rightKey + " (type " + rightKeyType + ")");
            }

            if (leftKeyType != targetKeyType) {
              leftKey =
                  rexBuilder.makeCast(targetKeyType, leftKey);
            }

            if (rightKeyType != targetKeyType) {
              rightKey =
                  rexBuilder.makeCast(targetKeyType, rightKey);
            }
          }
        }
      }

      if ((rangeOp == null)
          && ((leftKey == null) || (rightKey == null))) {
        // no equality join keys found yet:
        // try transforming the condition to
        // equality "join" conditions, e.g.
        //     f(LHS) > 0 ===> ( f(LHS) > 0 ) = TRUE,
        // and make the RHS produce TRUE, but only if we're strictly
        // looking for equi-joins
        final ImmutableBitSet projRefs = InputFinder.bits(condition);
        leftKey = null;
        rightKey = null;

        boolean foundInput = false;
        for (int i = 0; i < inputs.size() && !foundInput; i++) {
          final int lowerLimit = firstFieldInputs[i];
          final int upperLimit = i == inputs.size() - 1
                  ? totalFieldCount : firstFieldInputs[i + 1];
          if (projRefs.nextSetBit(lowerLimit) < upperLimit) {
            leftInput = i;
            leftFields = inputs.get(leftInput).getRowType().getFieldList();

            leftKey = condition.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    leftFields,
                    leftFields,
                    adjustments));

            rightKey = rexBuilder.makeLiteral(true);

            // effectively performing an equality comparison
            kind = SqlKind.EQUALS;

            foundInput = true;
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

  private static SqlOperator op(SqlKind kind, SqlOperator operator) {
    switch (kind) {
    case EQUALS:
      return SqlStdOperatorTable.EQUALS;
    case NOT_EQUALS:
      return SqlStdOperatorTable.NOT_EQUALS;
    case GREATER_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case IS_DISTINCT_FROM:
      return SqlStdOperatorTable.IS_DISTINCT_FROM;
    case IS_NOT_DISTINCT_FROM:
      return SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
    default:
      return operator;
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

}
