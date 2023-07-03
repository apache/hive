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
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputReferencedVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfPlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_BETWEEN;

public class FilterSelectivityEstimator extends RexVisitorImpl<Double> {

  protected static final Logger LOG = LoggerFactory.getLogger(FilterSelectivityEstimator.class);

  private final RelNode childRel;
  private final double  childCardinality;
  private final RelMetadataQuery mq;

  public FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    super(true);
    this.mq = mq;
    this.childRel = childRel;
    this.childCardinality = mq.getRowCount(childRel);
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
    SqlKind op = getOp(call);

    switch (op) {
    case AND: {
      selectivity = computeConjunctionSelectivity(call);
      break;
    }

    case OR: {
      selectivity = computeDisjunctionSelectivity(call);
      break;
    }

    case NOT:
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
      selectivity = computeRangePredicateSelectivity(call, op);
      break;
    }

    case BETWEEN:
      selectivity = computeBetweenPredicateSelectivity(call);
      break;

    case IN: {
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

    default:
      selectivity = computeFunctionSelectivity(call);
    }

    return selectivity;
  }

  private double computeRangePredicateSelectivity(RexCall call, SqlKind op) {
    final boolean isLiteralLeft = call.getOperands().get(0).getKind().equals(SqlKind.LITERAL);
    final boolean isLiteralRight = call.getOperands().get(1).getKind().equals(SqlKind.LITERAL);
    final boolean isInputRefLeft = call.getOperands().get(0).getKind().equals(SqlKind.INPUT_REF);
    final boolean isInputRefRight = call.getOperands().get(1).getKind().equals(SqlKind.INPUT_REF);

    if (childRel instanceof HiveTableScan && isLiteralLeft != isLiteralRight && isInputRefLeft != isInputRefRight) {
      final HiveTableScan t = (HiveTableScan) childRel;
      final int inputRefIndex = ((RexInputRef) call.getOperands().get(isInputRefLeft ? 0 : 1)).getIndex();
      final List<ColStatistics> colStats = t.getColStat(Collections.singletonList(inputRefIndex));

      if (!colStats.isEmpty() && isHistogramAvailable(colStats.get(0))) {
        final KllFloatsSketch kll = KllFloatsSketch.heapify(Memory.wrap(colStats.get(0).getHistogram()));
        final Object boundValueObject = ((RexLiteral) call.getOperands().get(isLiteralLeft ? 0 : 1)).getValue();
        final SqlTypeName typeName = call.getOperands().get(isInputRefLeft ? 0 : 1).getType().getSqlTypeName();
        float value = extractLiteral(typeName, boundValueObject);
        boolean closedBound = op.equals(SqlKind.LESS_THAN_OR_EQUAL) || op.equals(SqlKind.GREATER_THAN_OR_EQUAL);

        double selectivity;
        if (op.equals(SqlKind.LESS_THAN_OR_EQUAL) || op.equals(SqlKind.LESS_THAN)) {
          selectivity = closedBound ? lessThanOrEqualSelectivity(kll, value) : lessThanSelectivity(kll, value);
        } else {
          selectivity = closedBound ? greaterThanOrEqualSelectivity(kll, value) : greaterThanSelectivity(kll, value);
        }

        // selectivity does not account for null values, we multiply for the number of non-null values (getN)
        // and we divide by the total (non-null + null values) to get the overall selectivity.
        //
        // Example: consider a filter "col < 3", and the following table rows:
        //  _____
        // | col |
        // |_____|
        // |1    |
        // |null |
        // |null |
        // |3    |
        // |4    |
        // -------
        // kll.getN() would be 3, selectivity 1/3, t.getTable().getRowCount() 5
        // so the final result would be 3 * 1/3 / 5 = 1/5, as expected.
        return kll.getN() * selectivity / t.getTable().getRowCount();
      }
    }
    return ((double) 1 / (double) 3);
  }

  private Double computeBetweenPredicateSelectivity(RexCall call) {
    final boolean hasLiteralBool = call.getOperands().get(0).getKind().equals(SqlKind.LITERAL);
    final boolean hasInputRef = call.getOperands().get(1).getKind().equals(SqlKind.INPUT_REF);
    final boolean hasLiteralLeft = call.getOperands().get(2).getKind().equals(SqlKind.LITERAL);
    final boolean hasLiteralRight = call.getOperands().get(3).getKind().equals(SqlKind.LITERAL);

    if (childRel instanceof HiveTableScan && hasLiteralBool && hasInputRef && hasLiteralLeft && hasLiteralRight) {
      final HiveTableScan t = (HiveTableScan) childRel;
      final int inputRefIndex = ((RexInputRef) call.getOperands().get(1)).getIndex();
      final List<ColStatistics> colStats = t.getColStat(Collections.singletonList(inputRefIndex));

      if (!colStats.isEmpty() && isHistogramAvailable(colStats.get(0))) {
        final KllFloatsSketch kll = KllFloatsSketch.heapify(Memory.wrap(colStats.get(0).getHistogram()));
        final SqlTypeName typeName = call.getOperands().get(1).getType().getSqlTypeName();
        final Object inverseBoolValueObject = ((RexLiteral) call.getOperands().get(0)).getValue();
        boolean inverseBool = Boolean.parseBoolean(inverseBoolValueObject.toString());
        final Object leftBoundValueObject = ((RexLiteral) call.getOperands().get(2)).getValue();
        float leftValue = extractLiteral(typeName, leftBoundValueObject);
        final Object rightBoundValueObject = ((RexLiteral) call.getOperands().get(3)).getValue();
        float rightValue = extractLiteral(typeName, rightBoundValueObject);
        // when inverseBool == true, this is a NOT_BETWEEN and selectivity must be inverted
        if (inverseBool) {
          if (rightValue == leftValue) {
            return computeNotEqualitySelectivity(call);
          } else if (rightValue < leftValue) {
            return 1.0;
          }
          return 1.0 - (kll.getN() * betweenSelectivity(kll, leftValue, rightValue) / t.getTable().getRowCount());
        }
        // when they are equal it's an equality predicate, we cannot handle it as "between"
        if (Double.compare(leftValue, rightValue) != 0) {
          return kll.getN() * betweenSelectivity(kll, leftValue, rightValue) / t.getTable().getRowCount();
        }
      }
    }
    return computeFunctionSelectivity(call);
  }

  private float extractLiteral(SqlTypeName typeName, Object boundValueObject) {
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
      throw new IllegalStateException(
          "Unsupported type for comparator selectivity evaluation using histogram: " + typeName);
    }
    return value;
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
      RelOptHiveTable table = (RelOptHiveTable) ((HiveTableScan) r).getTable();
      ImmutableBitSet cols = RelOptUtil.InputFinder.bits(expr);
      return table.containsPartitionColumnsOnly(cols);
    }
    return false;
  }

  private SqlKind getOp(RexCall call) {
    SqlKind op = call.getKind();

    if (call.getKind().equals(SqlKind.OTHER_FUNCTION)
        && SqlTypeUtil.inBooleanFamily(call.getType())) {
      SqlOperator sqlOp = call.getOperator();
      String opName = (sqlOp != null) ? sqlOp.getName() : "";
      if (opName.equalsIgnoreCase("in")) {
        op = SqlKind.IN;
      }
    }

    return op;
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

  private static double rangedSelectivity(KllFloatsSketch kll, float val1, float val2) {
    float[] splitPoints = new float[] { val1, val2 };
    double[] boundaries = kll.getCDF(splitPoints);
    return boundaries[1] - boundaries[0];
  }

  /**
   * Returns the selectivity of a predicate "column &gt; value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &gt; value" in the range [0, 1]
   */
  public static double greaterThanSelectivity(KllFloatsSketch kll, float value) {
    float max = kll.getMaxValue();
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
    if (value > kll.getMaxValue()) {
      return 0;
    }
    return rangedSelectivity(kll, value, Math.nextUp(kll.getMaxValue()));
  }

  /**
   * Returns the selectivity of a predicate "column &lt;= value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &lt;= value" in the range [0, 1]
   */
  public static double lessThanOrEqualSelectivity(KllFloatsSketch kll, float value) {
    if (value < kll.getMinValue()) {
      return 0;
    }
    return kll.getCDF(new float[] { Math.nextUp(value) })[0];
  }

  /**
   * Returns the selectivity of a predicate "column &lt; value" in the range [0, 1]
   * @param kll the KLL sketch for the involved column
   * @param value the literal used in the comparison
   * @return the selectivity of a predicate "column &lt; value" in the range [0, 1]
   */
  public static double lessThanSelectivity(KllFloatsSketch kll, float value) {
    float min = kll.getMinValue();
    if (value < min) {
      return 0;
    }
    if (Double.compare(value, min) == 0 || Double.compare(Math.nextUp(value), min) == 0) {
      return 0;
    }
    return kll.getCDF(new float[] { value })[0];
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
    return rangedSelectivity(kll, Math.nextDown(leftValue), Math.nextUp(rightValue));
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
