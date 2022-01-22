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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

public class PartitionPrune {

  /**
   * Breaks the predicate into 2 pieces. The first piece is the expressions that
   * only contain partition columns and can be used for Partition Pruning; the
   * second piece is the predicates that are left.
   * 
   * @param cluster
   * @param hiveTable
   * @param predicate
   * @return a Pair of expressions, each of which maybe null. The 1st predicate
   *         is expressions that only contain partition columns; the 2nd
   *         predicate contains the remaining predicates.
   */
  public static Pair<RexNode, RexNode> extractPartitionPredicates(
      RelOptCluster cluster, RelOptHiveTable hiveTable, RexNode predicate) {
    RexNode partitionPruningPred = predicate
        .accept(new ExtractPartPruningPredicate(cluster, hiveTable));
    RexNode remainingPred = predicate.accept(new ExtractRemainingPredicate(
        cluster, partitionPruningPred));
    return new Pair<RexNode, RexNode>(partitionPruningPred, remainingPred);
  }

  public static class ExtractPartPruningPredicate extends
      RexVisitorImpl<RexNode> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtractPartPruningPredicate.class);
    final RelOptHiveTable hiveTable;
    final RelDataType rType;
    final Set<String> partCols;
    final RelOptCluster cluster;

    public ExtractPartPruningPredicate(RelOptCluster cluster,
        RelOptHiveTable hiveTable) {
      super(true);
      this.hiveTable = hiveTable;
      rType = hiveTable.getRowType();
      List<FieldSchema> pfs = hiveTable.getHiveTableMD().getPartCols();
      partCols = new HashSet<String>();
      for (FieldSchema pf : pfs) {
        partCols.add(pf.getName());
      }
      this.cluster = cluster;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      RelDataTypeField f = rType.getFieldList().get(inputRef.getIndex());
      if (partCols.contains(f.getName())) {
        return inputRef;
      } else {
        return null;
      }
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      GenericUDF hiveUDF = null;
      try {
        hiveUDF = SqlFunctionConverter.getHiveUDF(call.getOperator(),
            call.getType(), call.operands.size());
        if (hiveUDF != null &&
            !FunctionRegistry.isConsistentWithinQuery(hiveUDF)) {
          return null;
        }
      } finally {
        if (hiveUDF != null) {
          try {
            hiveUDF.close();
          } catch (IOException  e) {
            LOG.debug("Exception in closing {}", hiveUDF, e);
          }
        }
      }

      List<RexNode> args = new LinkedList<RexNode>();
      boolean argsPruned = false;
      
      for (RexNode operand : call.operands) {
        RexNode n = operand.accept(this);
        if (n != null) {
          args.add(n);
        } else {
          argsPruned = true;
        }
      }

      if (call.getOperator() != SqlStdOperatorTable.AND) {
        return argsPruned ? null : call;
      } else {
        if (args.size() == 0) {
          return null;
        } else if (args.size() == 1) {
          return args.get(0);
        } else {
          return cluster.getRexBuilder().makeCall(call.getOperator(), args);
        }
      }
    }

  }

  public static class ExtractRemainingPredicate extends RexVisitorImpl<RexNode> {

    List<RexNode> pruningPredicates;
    final RelOptCluster cluster;

    public ExtractRemainingPredicate(RelOptCluster cluster,
        RexNode partPruningExpr) {
      super(true);
      this.cluster = cluster;
      pruningPredicates = new ArrayList<RexNode>();
      flattenPredicates(partPruningExpr);
    }

    private void flattenPredicates(RexNode r) {
      if (r instanceof RexCall
          && ((RexCall) r).getOperator() == SqlStdOperatorTable.AND) {
        for (RexNode c : ((RexCall) r).getOperands()) {
          flattenPredicates(c);
        }
      } else {
        pruningPredicates.add(r);
      }
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      return literal;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return inputRef;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      if (call.getOperator() != SqlStdOperatorTable.AND) {
        if (pruningPredicates.contains(call)) {
          return null;
        } else {
          return call;
        }
      }

      List<RexNode> args = new LinkedList<RexNode>();

      for (RexNode operand : call.operands) {
        RexNode n = operand.accept(this);
        if (n != null) {
          args.add(n);
        }
      }

      if (args.size() == 0) {
        return null;
      } else if (args.size() == 1) {
        return args.get(0);
      } else {
        return cluster.getRexBuilder().makeCall(call.getOperator(), args);
      }
    }
  }
}
