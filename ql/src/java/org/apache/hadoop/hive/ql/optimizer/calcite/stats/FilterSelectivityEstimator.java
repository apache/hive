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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputReferencedVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfPlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.SearchTransformer;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterSelectivityEstimator extends RexVisitorImpl<Double> {

  protected static final Logger LOG = LoggerFactory.getLogger(FilterSelectivityEstimator.class);

  private final RelNode childRel;
  private final double  childCardinality;
  private final RelMetadataQuery mq;
  private final RexBuilder rexBuilder;

  public FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    super(true);
    this.mq = mq;
    this.childRel = childRel;
    this.childCardinality = mq.getRowCount(childRel);
    this.rexBuilder = childRel.getCluster().getRexBuilder();
  }

  public Double estimateSelectivity(RexNode predicate) {
    return predicate.accept(this);
  }

  @Override
  public Double visitInputRef(RexInputRef inputRef) {
    if (inputRef.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      // If it is a boolean and we assume uniform distribution,
      // it will filter half the rows
      return 0.5;
    }
    return null;
  }

  @Override
  public Double visitCall(RexCall call) {
    if (!deep) {
      return 1.0;
    }

    /*
     * Ignore any predicates on partition columns because we have already
     * accounted for these in the Table row count.
     */
    if (isPartitionPredicate(call, this.childRel)) {
      return 1.0;
    }

    Double selectivity;

    switch (call.getKind()) {
    case AND: {
      selectivity = computeConjunctionSelectivity(call);
      break;
    }
    case SEARCH:
      return new SearchTransformer<>(rexBuilder, call, RexUnknownAs.FALSE).transform().accept(this);
    case OR: {
      selectivity = computeDisjunctionSelectivity(call);
      break;
    }

    case NOT: {
      Double opSelectivity = call.getOperands().get(0).accept(this);
      assert (opSelectivity >= 0 && opSelectivity <= 1);
      return 1.0 - opSelectivity;
    }
    case NOT_EQUALS: {
      selectivity = computeNotEqualitySelectivity(call);
      break;
    }

    case IS_NOT_NULL: {
      if (childRel instanceof HiveTableScan) {
        double noOfNulls = getMaxNulls(call, (HiveTableScan) childRel);
        double totalNoOfTuples = mq.getRowCount(childRel);
        if (totalNoOfTuples >= noOfNulls) {
          selectivity = (totalNoOfTuples - noOfNulls) / Math.max(totalNoOfTuples, 1);
        } else {
          // If we are running explain, we will print the warning in the console
          // and the log files. Otherwise, we just print it in the log files.
          HiveConfPlannerContext ctx = childRel.getCluster().getPlanner().getContext().unwrap(HiveConfPlannerContext.class);
          String msg = "Invalid statistics: Number of null values > number of tuples. " +
              "Consider recomputing statistics for table: " +
              ((RelOptHiveTable) childRel.getTable()).getHiveTableMD().getFullyQualifiedName();
          if (ctx.isExplainPlan()) {
            SessionState.getConsole().printError("WARNING: " + msg);
          }
          LOG.warn(msg);
          selectivity = ((double) 1 / (double) 3);
        }
      } else {
        selectivity = computeNotEqualitySelectivity(call);
      }
      break;
    }

    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case GREATER_THAN: {
      selectivity = computeRangePredicateSelectivity(call, call.getKind());
      break;
    }

    case BETWEEN:
      selectivity = computeBetweenPredicateSelectivity(call);
      break;

    default:
      if (HiveIn.INSTANCE.equals(call.op)) {
        // TODO: 1) check for duplicates 2) We assume in clause values to be
        // present in NDV which may not be correct (Range check can find it) 3) We
        // assume values in NDV set is uniformly distributed over col values
        // (account for skewness - histogram).
        selectivity = computeFunctionSelectivity(call);
        if (selectivity != null) {
          selectivity = selectivity * (call.operands.size() - 1);
          if (selectivity <= 0.0) {
            selectivity = 0.10;
          } else if (selectivity >= 1.0) {
            selectivity = 1.0;
          }
        }
        break;
      }
      selectivity = computeFunctionSelectivity(call);
    }

    return selectivity;
  }

  /**
   * Return whether the expression is a removable cast based on stats and type bounds.
   *
   * <p>
   * In Hive, if a value cannot be represented by the cast, the result of the cast is NULL,
   * and therefore cannot fulfill the predicate. So the possible range of the values
   * is limited by the range of possible values of the type.
   * </p>
   *
   * @param exp       the expression to check
   * @param tableScan the table that provides the statistics
   * @return true if the expression is a removable cast, false otherwise
   */
  private boolean isRemovableCast(RexNode exp, HiveTableScan tableScan) {
    if(SqlKind.CAST != exp.getKind()) {
      return false;
    }
    RexCall cast = (RexCall) exp;
    RexNode op0 = cast.getOperands().getFirst();
    if (!(op0 instanceof RexInputRef)) {
      return false;
    }
    int index = ((RexInputRef) op0).getIndex();
    final List<ColStatistics> colStats = tableScan.getColStat(Collections.singletonList(index));
    if (colStats.isEmpty()) {
      return false;
    }

    SqlTypeName targetType = cast.getType().getSqlTypeName();
    SqlTypeName sourceType = op0.getType().getSqlTypeName();

    switch (sourceType) {
    case TINYINT, SMALLINT, INTEGER, BIGINT:
      // additional checks are needed
      break;
    case FLOAT, DOUBLE, DECIMAL, TIMESTAMP, DATE:
      return true;
    default:
      // unknown type, do not remove the cast
      return false;
    }

    // If the source type is completely within the target type, the cast is lossless
    Range<Float> targetRange = getRangeOfType(cast.getType(), BoundType.CLOSED, BoundType.CLOSED);
    Range<Float> sourceRange = getRangeOfType(op0.getType(), BoundType.CLOSED, BoundType.CLOSED);
    if (sourceRange.equals(targetRange.intersection(sourceRange))) {
      return true;
    }

    // Check that the possible values of the input column are all within the type range of the cast
    // otherwise the CAST introduces some modulo-like behavior
    ColStatistics colStat = colStats.getFirst();
    ColStatistics.Range colRange = colStat.getRange();
    if (colRange == null || colRange.minValue == null || colRange.maxValue == null) {
      return false;
    }


    // are all values of the input column accepted by the cast?
    double min = ((Number) targetType.getLimit(false, SqlTypeName.Limit.OVERFLOW, false, -1, -1)).doubleValue();
    double max = ((Number) targetType.getLimit(true, SqlTypeName.Limit.OVERFLOW, false, -1, -1)).doubleValue();
    return min < colRange.minValue.doubleValue() && colRange.maxValue.doubleValue() < max;
  }

  /**
   * Get the range of values that are rounded to valid values of a type.
   *
   * @param type the type
   * @param lowerBound the lower bound type of the result
   * @param upperBound the upper bound type of the result
   * @return the range of the type
   */
  private static Range<Float> getRangeOfType(RelDataType type, BoundType lowerBound, BoundType upperBound) {
    switch (type.getSqlTypeName()) {
    // in case of integer types,
    case TINYINT:
      return Range.closed(-128.99998f, 127.99999f);
    case SMALLINT:
      return Range.closed(-32768.996f, 32767.998f);
    case INTEGER:
      return Range.closed(-2.1474836E9f, 2.1474836E9f);
    case BIGINT:
      return Range.closed(-9.223372E18f, 9.223372E18f);
    case DECIMAL:
      return getRangeOfDecimalType(type, lowerBound, upperBound);
    case DATE, TIMESTAMP:
      return Range.closed((float) Long.MIN_VALUE, (float) Long.MAX_VALUE);
    default:
      throw new IllegalStateException("Unsupported type: " + type);
    }
  }

  private static Range<Float> getRangeOfDecimalType(RelDataType type, BoundType lowerBound, BoundType upperBound) {
    // values outside the representable range are cast to NULL, so adapt the boundaries
    int digits = type.getPrecision() - type.getScale();
    // the cast does some rounding, i.e., CAST(99.9499 AS DECIMAL(3,1)) = 99.9
    // but CAST(99.95 AS DECIMAL(3,1)) = NULL
    float adjust = (float) (5 * Math.pow(10, -(type.getScale() + 1)));
    // the range of values supported by the type is interval [-typeRangeExtent, typeRangeExtent] (both inclusive)
    // e.g., the typeRangeExt is 99.94999 for DECIMAL(3,1)
    float typeRangeExtent = Math.nextDown((float) (Math.pow(10, digits) - adjust));

    // the resulting value of +- adjust would be rounded up, so in some cases we need to use Math.nextDown
    boolean lowerInclusive = BoundType.CLOSED.equals(lowerBound);
    boolean upperInclusive = BoundType.CLOSED.equals(upperBound);
    float lowerUniverse = lowerInclusive ? -typeRangeExtent : Math.nextDown(-typeRangeExtent);
    float upperUniverse = upperInclusive ? typeRangeExtent : Math.nextUp(typeRangeExtent);
    return makeRange(lowerUniverse, lowerBound, upperUniverse, upperBound);
  }

  /**
   * Adjust the type boundaries if necessary.
   *
   * <p>
   * Special care is taken to support the cast to DECIMAL(precision, scale):
   * The cast to DECIMAL rounds the value the same way as {@link RoundingMode#HALF_UP}.
   * The boundaries are adjusted accordingly.
   * </p>
   *
   * @param predicateRange boundaries of the range predicate
   * @param type the DECIMAL type
   * @param typeRange the boundaries of the type range
   * @return the adjusted boundary
   */
  private static Range<Float> adjustRangeToType(Range<Float> predicateRange, RelDataType type,
      Range<Float> typeRange) {
    boolean lowerInclusive = BoundType.CLOSED.equals(predicateRange.lowerBoundType());
    boolean upperInclusive = BoundType.CLOSED.equals(predicateRange.upperBoundType());
    switch (type.getSqlTypeName()) {
    case TINYINT, SMALLINT, INTEGER, BIGINT: {
      // when casting a floating point, its values are rounded towards 0
      // i.e, 10.99 is rounded to 10, and -10.99 is rounded to -10
      // to take this into account, the predicate range is transformed in the following ways
      // [10.0, 15.0] -> [10, 15.99999]
      // (10.0, 15.0) -> [11, 14.99999]
      // [10.2, 15.2] -> [11, 15.99999]
      // (10.2, 15.2) -> [11, 15.99999]

      // [-15.0, -10.0] -> [-15.9999, -10]
      // (-15.0, -10.0) -> [-14.9999, -11]
      // [-15.2, -10.2] -> [-15.9999, -11]
      // (-15.2, -10.2) -> [-15.9999, -11]

      // normalize the range to make the formulas easier
      Range<Float> range = convertRangeToClosedOpen(predicateRange);
      Range<Float> typeClosedOpen = convertRangeToClosedOpen(typeRange);
      float rangeLower = (range.lowerEndpoint() >= 0 ? (float) Math.ceil(range.lowerEndpoint())
          : Math.nextUp(-(float) Math.ceil(Math.nextUp(-range.lowerEndpoint()))));
      float rangeUpper = range.upperEndpoint() >= 0 ? Math.nextDown((float) Math.ceil(range.upperEndpoint()))
          : Math.nextUp((float) -Math.ceil(-range.upperEndpoint()));
      float lower = Math.max(typeClosedOpen.lowerEndpoint(), rangeLower);
      float upper = Math.min(typeClosedOpen.upperEndpoint(), rangeUpper);
      return makeRange(lower, BoundType.CLOSED, upper, BoundType.OPEN);
    }
    case DECIMAL: {
      float adjust = (float) (5 * Math.pow(10, -(type.getScale() + 1)));
      // the resulting value of +- adjust would be rounded up, so in some cases we need to use Math.nextDown
      float adjusted1 = lowerInclusive ? predicateRange.lowerEndpoint() - adjust
          : Math.nextDown(predicateRange.lowerEndpoint() + adjust);
      float adjusted2 = upperInclusive ? Math.nextDown(predicateRange.upperEndpoint() + adjust)
          : predicateRange.upperEndpoint() - adjust;
      float lower = Math.max(adjusted1, typeRange.lowerEndpoint());
      float upper = Math.min(adjusted2, typeRange.upperEndpoint());
      // the boundaries might result in an invalid range (e.g., left > right)
      // in that case the predicate does not select anything, and we return an empty range
      return makeRange(lower, predicateRange.lowerBoundType(), upper, predicateRange.upperBoundType());
    }
    case TIMESTAMP, DATE:
      return predicateRange;
    default:
      return typeRange.intersection(predicateRange);
    }
  }

  /**
   * If the arguments lead to a valid range, it is returned, otherwise an empty array is returned.
   */
  private static Range<Float> makeRange(float lower, BoundType lowerType, float upper, BoundType upperType) {
    if (lower > upper) {
      return Range.closedOpen(0f, 0f);
    }
    if (lower == upper && lowerType == BoundType.OPEN && upperType == BoundType.OPEN) {
      return Range.closedOpen(0f, 0f);
    }

    return Range.range(lower, lowerType, upper, upperType);
  }

  private double computeRangePredicateSelectivity(RexCall call, SqlKind op) {
    double defaultSelectivity = ((double) 1 / (double) 3);
    if (!(childRel instanceof HiveTableScan)) {
      return defaultSelectivity;
    }

    // search for the literal
    List<RexNode> operands = call.getOperands();
    final Optional<Float> leftLiteral = extractLiteral(operands.get(0));
    final Optional<Float> rightLiteral = extractLiteral(operands.get(1));
    // ensure that there's exactly one literal
    if ((leftLiteral.isPresent()) == (rightLiteral.isPresent())) {
      return defaultSelectivity;
    }
    int literalOpIdx = leftLiteral.isPresent() ? 0 : 1;

    // analyze the predicate
    float value = leftLiteral.orElseGet(rightLiteral::get);
    int boundaryIdx;
    boolean openBound = op == SqlKind.LESS_THAN || op == SqlKind.GREATER_THAN;
    switch (op) {
    case LESS_THAN, LESS_THAN_OR_EQUAL:
      boundaryIdx = literalOpIdx;
      break;
    case GREATER_THAN, GREATER_THAN_OR_EQUAL:
      boundaryIdx = 1 - literalOpIdx;
      break;
    default:
      return defaultSelectivity;
    }
    float[] boundaryValues = new float[] { Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY };
    BoundType[] inclusive = new BoundType[] { BoundType.CLOSED, BoundType.CLOSED };
    boundaryValues[boundaryIdx] = value;
    inclusive[boundaryIdx] = openBound ? BoundType.OPEN : BoundType.CLOSED;
    Range<Float> boundaries = Range.range(boundaryValues[0], inclusive[0], boundaryValues[1], inclusive[1]);

    // extract the column index from the other operator
    final HiveTableScan scan = (HiveTableScan) childRel;
    int inputRefOpIndex = 1 - literalOpIdx;
    RexNode node = operands.get(inputRefOpIndex);
    if (isRemovableCast(node, scan)) {
      Range<Float> rangeOfDecimalType =
          getRangeOfType(node.getType(), boundaries.lowerBoundType(), boundaries.upperBoundType());
      boundaries = adjustRangeToType(boundaries, node.getType(), rangeOfDecimalType);
      node = RexUtil.removeCast(node);
    }

    int inputRefIndex = -1;
    if (node.getKind().equals(SqlKind.INPUT_REF)) {
      inputRefIndex = ((RexInputRef) node).getIndex();
    }

    if (inputRefIndex < 0) {
      return defaultSelectivity;
    }

    final List<ColStatistics> colStats = scan.getColStat(Collections.singletonList(inputRefIndex));
    if (colStats.isEmpty() || !isHistogramAvailable(colStats.get(0))) {
      return defaultSelectivity;
    }

    final KllFloatsSketch kll = KllFloatsSketch.heapify(Memory.wrap(colStats.get(0).getHistogram()));
    double rawSelectivity = rangedSelectivity(kll, boundaries);
    return scaleSelectivityToNullableValues(kll, rawSelectivity, scan);
  }

  /**
   * Adjust the selectivity estimate to take NULL values into account.
   * <p>
   * The rawSelectivity does not account for null values. We multiply with the number of non-null values (getN)
   * and we divide by the total number (non-null + null values) to get the overall selectivity.
   * <p>
   * Example: consider a filter "col < 3", and the following table rows:
   * <pre>
   *  _____
   * | col |
   * |_____|
   * |1    |
   * |null |
   * |null |
   * |3    |
   * |4    |
   * -------
   * </pre>
   * kll.getN() would be 3, rawSelectivity 1/3, scan.getTable().getRowCount() 5
   * so the final result would be 3 * 1/3 / 5 = 1/5, as expected.
   */
  private static double scaleSelectivityToNullableValues(KllFloatsSketch kll, double rawSelectivity,
      HiveTableScan scan) {
    if (scan.getTable() == null) {
      return rawSelectivity;
    }
    return kll.getN() * rawSelectivity / scan.getTable().getRowCount();
  }

  private Double computeBetweenPredicateSelectivity(RexCall call) {
    if (!(childRel instanceof HiveTableScan)) {
      return computeFunctionSelectivity(call);
    }

    List<RexNode> operands = call.getOperands();
    final boolean hasLiteralBool = operands.get(0).getKind().equals(SqlKind.LITERAL);
    Optional<Float> leftLiteral = extractLiteral(operands.get(2));
    Optional<Float> rightLiteral = extractLiteral(operands.get(3));

    if (hasLiteralBool && leftLiteral.isPresent() && rightLiteral.isPresent()) {
      final HiveTableScan scan = (HiveTableScan) childRel;
      float leftValue = leftLiteral.get();
      float rightValue = rightLiteral.get();

      final Object inverseBoolValueObject = ((RexLiteral) operands.getFirst()).getValue();
      boolean inverseBool = Boolean.parseBoolean(inverseBoolValueObject.toString());
      // when they are equal it's an equality predicate, we cannot handle it as "BETWEEN"
      if (Objects.equals(leftValue, rightValue)) {
        return inverseBool ? computeNotEqualitySelectivity(call) : computeFunctionSelectivity(call);
      }

      Range<Float> rangeBoundaries = makeRange(leftValue, BoundType.CLOSED, rightValue, BoundType.CLOSED);
      Range<Float> typeBoundaries = inverseBool ? Range.closed(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY) : null;

      RexNode expr = operands.get(1); // expr to be checked by the BETWEEN
      if (isRemovableCast(expr, scan)) {
        typeBoundaries =
            getRangeOfType(expr.getType(), rangeBoundaries.lowerBoundType(), rangeBoundaries.upperBoundType());
        rangeBoundaries = adjustRangeToType(rangeBoundaries, expr.getType(), typeBoundaries);
        expr = RexUtil.removeCast(expr);
      }

      int inputRefIndex = -1;
      if (expr.getKind().equals(SqlKind.INPUT_REF)) {
        inputRefIndex = ((RexInputRef) expr).getIndex();
      }

      if (inputRefIndex < 0) {
        return computeFunctionSelectivity(call);
      }

      final List<ColStatistics> colStats = scan.getColStat(Collections.singletonList(inputRefIndex));
      if (!colStats.isEmpty() && isHistogramAvailable(colStats.get(0))) {
        final KllFloatsSketch kll = KllFloatsSketch.heapify(Memory.wrap(colStats.get(0).getHistogram()));
        double rawSelectivity = rangedSelectivity(kll, rangeBoundaries);
        if (inverseBool) {
          // when inverseBool == true, this is a NOT_BETWEEN and selectivity must be inverted
          // if there's a cast, the inversion is with respect to its codomain (range of the values of the cast)
          double typeRangeSelectivity = rangedSelectivity(kll, typeBoundaries);
          rawSelectivity = typeRangeSelectivity - rawSelectivity;
        }
        return scaleSelectivityToNullableValues(kll, rawSelectivity, scan);
      }
    }
    return computeFunctionSelectivity(call);
  }

  private Optional<Float> extractLiteral(RexNode node) {
    if (node.getKind() != SqlKind.LITERAL) {
      return Optional.empty();
    }
    RexLiteral literal = (RexLiteral) node;
    if (literal.getValue() == null) {
      return Optional.empty();
    }
    return extractLiteral(literal.getTypeName(), literal.getValue());
  }

  private Optional<Float> extractLiteral(SqlTypeName typeName, Object boundValueObject) {
    final String boundValueString = boundValueObject.toString();

    float value;
    switch (typeName) {
    case TINYINT:
      value = Byte.parseByte(boundValueString);
      break;
    case SMALLINT:
      value = Short.parseShort(boundValueString);
      break;
    case INTEGER:
      value = Integer.parseInt(boundValueString);
      break;
    case BIGINT:
      value = Long.parseLong(boundValueString);
      break;
    case FLOAT:
      value = Float.parseFloat(boundValueString);
      break;
    case DOUBLE:
      value = (float) Double.parseDouble(boundValueString);
      break;
    case DECIMAL:
      value = new BigDecimal(boundValueString).floatValue();
      break;
    case DATE:
    case TIMESTAMP:
      value = ((GregorianCalendar) boundValueObject).toInstant().getEpochSecond();
      break;
    default:
      return Optional.empty();
    }
    return Optional.of(value);
  }

  /**
   * NDV of "f1(x, y, z) != f2(p, q, r)" ->
   * "(maxNDV(x,y,z,p,q,r) - 1)/maxNDV(x,y,z,p,q,r)".
   * <p>
   *
   * @param call
   * @return
   */
  private Double computeNotEqualitySelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) {
      // Could not be computed
      return null;
    }

    if (tmpNDV > 1) {
      return (tmpNDV - 1) / tmpNDV;
    } else {
      return 1.0;
    }
  }

  /**
   * Selectivity of f(X,y,z) -> 1/maxNDV(x,y,z).
   * <p>
   * Note that = is considered a generic function and uses this method to find its selectivity.
   * @param call
   * @return
   */
  private Double computeFunctionSelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) {
      // Could not be computed
      return null;
    }
    return 1 / tmpNDV;
  }

  /**
   * Disjunction Selectivity -> (1 D(1-m1/n)(1-m2/n)) where n is the total
   * number of tuples from child and m1 and m2 is the expected number of tuples
   * from each part of the disjunction predicate.
   * <p>
   * Note we compute m1. m2.. by applying selectivity of the disjunctive element
   * on the cardinality from child.
   *
   * @param call
   * @return
   */
  private Double computeDisjunctionSelectivity(RexCall call) {
    Double tmpCardinality;
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode dje : call.getOperands()) {
      tmpSelectivity = dje.accept(this);
      if (tmpSelectivity == null) {
        tmpSelectivity = 0.99;
      }
      tmpCardinality = childCardinality * tmpSelectivity;

      if (tmpCardinality > 1 && tmpCardinality < childCardinality) {
        tmpSelectivity = (1 - tmpCardinality / childCardinality);
      } else {
        tmpSelectivity = 1.0;
      }

      selectivity *= tmpSelectivity;
    }

    if (selectivity < 0.0) {
      selectivity = 0.0;
    }

    return (1 - selectivity);
  }

  /**
   * Selectivity of conjunctive predicate -> (selectivity of conjunctive
   * element1) * (selectivity of conjunctive element2)...
   *
   * @param call
   * @return
   */
  private Double computeConjunctionSelectivity(RexCall call) {
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode cje : call.getOperands()) {
      tmpSelectivity = cje.accept(this);
      if (tmpSelectivity != null) {
        selectivity *= tmpSelectivity;
      }
    }

    return selectivity;
  }

  /**
   * Given a RexCall & TableScan find max no of nulls. Currently it picks the
   * col with max no of nulls.
   *
   * TODO: improve this
   *
   * @param call
   * @param t
   * @return
   */
  private long getMaxNulls(RexCall call, HiveTableScan t) {
    long tmpNoNulls = 0;
    long maxNoNulls = 0;

    Set<Integer> iRefSet = HiveCalciteUtil.getInputRefs(call);
    List<ColStatistics> colStats = t.getColStat(new ArrayList<Integer>(iRefSet));

    for (ColStatistics cs : colStats) {
      tmpNoNulls = cs.getNumNulls();
      if (tmpNoNulls > maxNoNulls) {
        maxNoNulls = tmpNoNulls;
      }
    }

    return maxNoNulls;
  }

  private Double getMaxNDV(RexCall call) {
    Double tmpNDV;
    double maxNDV = 1.0;
    InputReferencedVisitor irv;
    for (RexNode op : call.getOperands()) {
      if (op instanceof RexInputRef) {
        tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(this.childRel, mq,
            ((RexInputRef) op).getIndex());
        if (tmpNDV == null) {
          return null;
        }
        if (tmpNDV > maxNDV) {
          maxNDV = tmpNDV;
        }
      } else {
        irv = new InputReferencedVisitor();
        irv.apply(op);
        for (Integer childProjIndx : irv.inputPosReferenced) {
          tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(this.childRel,
              mq, childProjIndx);
          if (tmpNDV == null) {
            return null;
          }
          if (tmpNDV > maxNDV) {
            maxNDV = tmpNDV;
          }
        }
      }
    }

    return maxNDV;
  }

  private boolean isPartitionPredicate(RexNode expr, RelNode r) {
    if (r instanceof Project) {
      expr = RelOptUtil.pushFilterPastProject(expr, (Project) r);
      return isPartitionPredicate(expr, ((Project) r).getInput());
    } else if (r instanceof Filter) {
      return isPartitionPredicate(expr, ((Filter) r).getInput());
    } else if (r instanceof HiveTableScan) {
      RelOptHiveTable table = (RelOptHiveTable) r.getTable();
      ImmutableBitSet cols = RelOptUtil.InputFinder.bits(expr);
      return table.containsPartitionColumnsOnly(cols);
    }
    return false;
  }

  @Override
  public Double visitLiteral(RexLiteral literal) {
    if (literal.isAlwaysFalse() || RexUtil.isNull(literal)) {
      return 0.0;
    } else if (literal.isAlwaysTrue()) {
      return 1.0;
    } else {
      assert false;
    }
    return null;
  }

  /**
   * Returns the selectivity of a predicate "val1 &lt;= column &lt; val2".
   * @param kll the sketch
   * @param boundaries the boundaries
   * @return the selectivity of "val1 &lt;= column &lt; val2"
   */
  private static double rangedSelectivity(KllFloatsSketch kll, Range<Float> boundaries) {
    // convert the condition to a range val1 <= x < val2
    Range<Float> closedOpen = convertRangeToClosedOpen(boundaries);
    return rangedSelectivity(kll, closedOpen.lowerEndpoint(), closedOpen.upperEndpoint());
  }

  /**
   * Normalizes the range to the form "val1 &lt;= column &lt; val2".
   */
  private static Range<Float> convertRangeToClosedOpen(Range<Float> boundaries) {
    boolean leftClosed = BoundType.CLOSED.equals(boundaries.lowerBoundType());
    boolean rightOpen = BoundType.OPEN.equals(boundaries.upperBoundType());
    if (leftClosed && rightOpen) {
      return boundaries;
    }
    float newLower = leftClosed ? boundaries.lowerEndpoint() : Math.nextUp(boundaries.lowerEndpoint());
    float newUpper = rightOpen ? boundaries.upperEndpoint() : Math.nextUp(boundaries.upperEndpoint());
    return Range.closedOpen(newLower, newUpper);
  }

  /**
   * Returns the selectivity of a predicate "val1 &lt;= column &lt; val2".
   * @param kll the sketch
   * @param val1 lower bound (inclusive)
   * @param val2 upper bound (exclusive)
   * @return the selectivity of "val1 &lt;= column &lt; val2"
   */
  static double rangedSelectivity(KllFloatsSketch kll, float val1, float val2) {
    if (val1 >= val2) {
      return 0;
    }
    float[] splitPoints = new float[] { val1, val2 };
    double[] boundaries = kll.getCDF(splitPoints, QuantileSearchCriteria.EXCLUSIVE);
    return boundaries[1] - boundaries[0];
  }

  /**
   * Returns the selectivity of a predicate "column &gt; value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &gt; value" in the range [0, 1]
   */
  public static double greaterThanSelectivity(KllFloatsSketch kll, float value) {
    float max = kll.getMaxItem();
    if (value > max) {
      return 0;
    }
    float nextValue = Math.nextUp(value);
    if (Double.compare(value, max) == 0 || Double.compare(nextValue, max) == 0) {
      return 0;
    }
    return rangedSelectivity(kll, nextValue, Math.nextUp(max));
  }

  /**
   * Returns the selectivity of a predicate "column &gt;= value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &gt;= value" in the range [0, 1]
   */
  public static double greaterThanOrEqualSelectivity(KllFloatsSketch kll, float value) {
    if (value > kll.getMaxItem()) {
      return 0;
    }
    return rangedSelectivity(kll, value, Math.nextUp(kll.getMaxItem()));
  }

  /**
   * Returns the selectivity of a predicate "column &lt;= value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &lt;= value" in the range [0, 1]
   */
  public static double lessThanOrEqualSelectivity(KllFloatsSketch kll, float value) {
    if (value < kll.getMinItem()) {
      return 0;
    }
    return kll.getCDF(new float[] { Math.nextUp(value) }, QuantileSearchCriteria.EXCLUSIVE)[0];
  }

  /**
   * Returns the selectivity of a predicate "column &lt; value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &lt; value" in the range [0, 1]
   */
  public static double lessThanSelectivity(KllFloatsSketch kll, float value) {
    float min = kll.getMinItem();
    if (value < min) {
      return 0;
    }
    if (Double.compare(value, min) == 0 || Double.compare(Math.nextUp(value), min) == 0) {
      return 0;
    }
    return kll.getCDF(new float[] { value }, QuantileSearchCriteria.EXCLUSIVE)[0];
  }

  /**
   * Returns the selectivity of a predicate "column BETWEEN leftValue AND rightValue" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param leftValue the LHS literal in the BETWEEN statement
   * @param rightValue the RHS literal in the BETWEEN statement
   * @return the selectivity of a predicate "column BETWEEN leftValue AND rightValue" in the range [0, 1]
   * @throws IllegalArgumentException if leftValue is equal to rightValue
   */
  public static double betweenSelectivity(KllFloatsSketch kll, float leftValue, float rightValue) {
    // column >= leftValue AND column <= rightValue
    if (rightValue < leftValue) {
      return 0;
    }
    if (Double.compare(leftValue, rightValue) == 0) {
      throw new IllegalArgumentException(
          "Selectivity for BETWEEN leftValue AND rightValue when the two values coincide is not supported, found: "
          + "leftValue = " + leftValue + " and rightValue = " + rightValue);
    }
    return rangedSelectivity(kll, leftValue, Math.nextUp(rightValue));
  }

  /**
   * Returns true if histogram statistics are available in the column statistics information, false otherwise.
   * @param colStats the input column statistics information
   * @return true if histogram statistics are available in the column statistics information, false otherwise.
   */
  public static boolean isHistogramAvailable(ColStatistics colStats) {
    return colStats != null && colStats.getHistogram() != null && colStats.getHistogram().length > 0;
  }
}
