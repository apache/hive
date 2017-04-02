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
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

public class FilterSelectivityEstimator extends RexVisitorImpl<Double> {
  private final RelNode childRel;
  private final double  childCardinality;

  protected FilterSelectivityEstimator(RelNode childRel) {
    super(true);
    this.childRel = childRel;
    this.childCardinality = RelMetadataQuery.instance().getRowCount(childRel);
  }

  public Double estimateSelectivity(RexNode predicate) {
    return predicate.accept(this);
  }

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

    Double selectivity = null;
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
        double totalNoOfTuples = childRel.getRows();
        if (totalNoOfTuples >= noOfNulls) {
          selectivity = (totalNoOfTuples - noOfNulls) / Math.max(totalNoOfTuples, 1);
        } else {
          throw new RuntimeException("Invalid Stats number of null > no of tuples");
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
      selectivity = ((double) 1 / (double) 3);
      break;
    }

    case IN: {
      // TODO: 1) check for duplicates 2) We assume in clause values to be
      // present in NDV which may not be correct (Range check can find it) 3) We
      // assume values in NDV set is uniformly distributed over col values
      // (account for skewness - histogram).
      selectivity = computeFunctionSelectivity(call) * (call.operands.size() - 1);
      if (selectivity <= 0.0) {
        selectivity = 0.10;
      } else if (selectivity >= 1.0) {
        selectivity = 1.0;
      }
      break;
    }

    default:
      selectivity = computeFunctionSelectivity(call);
    }

    return selectivity;
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
    double tmpNDV = getMaxNDV(call);

    if (tmpNDV > 1)
      return (tmpNDV - (double) 1) / tmpNDV;
    else
      return 1.0;
  }

  /**
   * Selectivity of f(X,y,z) -> 1/maxNDV(x,y,z).
   * <p>
   * Note that >, >=, <, <=, = ... are considered generic functions and uses
   * this method to find their selectivity.
   * 
   * @param call
   * @return
   */
  private Double computeFunctionSelectivity(RexCall call) {
    return 1 / getMaxNDV(call);
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

    if (selectivity < 0.0)
      selectivity = 0.0;

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
    double tmpNDV;
    double maxNDV = 1.0;
    InputReferencedVisitor irv;
    RelMetadataQuery mq = RelMetadataQuery.instance();
    for (RexNode op : call.getOperands()) {
      if (op instanceof RexInputRef) {
        tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(this.childRel, mq,
            ((RexInputRef) op).getIndex());
        if (tmpNDV > maxNDV)
          maxNDV = tmpNDV;
      } else {
        irv = new InputReferencedVisitor();
        irv.apply(op);
        for (Integer childProjIndx : irv.inputPosReferenced) {
          tmpNDV = HiveRelMdDistinctRowCount.getDistinctRowCount(this.childRel,
              mq, childProjIndx);
          if (tmpNDV > maxNDV)
            maxNDV = tmpNDV;
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

  public Double visitLiteral(RexLiteral literal) {
    if (literal.isAlwaysFalse()) {
      return 0.0;
    } else if (literal.isAlwaysTrue()) {
      return 1.0;
    } else {
      assert false;
    }
    return null;
  }
}
