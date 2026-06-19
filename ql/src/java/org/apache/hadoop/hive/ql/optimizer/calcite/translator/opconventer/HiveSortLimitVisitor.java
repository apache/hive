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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;

class HiveSortLimitVisitor extends HiveRelNodeVisitor<HiveSortLimit> {
  HiveSortLimitVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveSortLimit sortRel) throws SemanticException {
    OpAttr inputOpAf = hiveOpConverter.dispatch(sortRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
          + " with row type: [" + sortRel.getRowType() + "]");
      if (sortRel.getCollation() == RelCollations.EMPTY) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of limit");
      } else if (sortRel.fetch == null) {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of sort");
      } else {
        LOG.debug("Operator rel#" + sortRel.getId() + ":" + sortRel.getRelTypeName()
            + " consists of sort+limit");
      }
    }

    Operator<?> inputOp = inputOpAf.inputs.get(0);
    Operator<?> resultOp = inputOpAf.inputs.get(0);

    // 1. If we need to sort tuples based on the value of some
    // of their columns
    if (sortRel.getCollation() != RelCollations.EMPTY) {

      // In strict mode, in the presence of order by, limit must be specified.
      if (sortRel.fetch == null) {
        String error = StrictChecks.checkNoLimit(hiveOpConverter.getHiveConf());
        if (error != null) throw new SemanticException(error);
      }

      // 1.a. Extract order for each column from collation
      // Generate sortCols and order
      ImmutableBitSet.Builder sortColsPosBuilder = ImmutableBitSet.builder();
      ImmutableBitSet.Builder sortOutputColsPosBuilder = ImmutableBitSet.builder();
      Map<Integer, RexNode> obRefToCallMap = sortRel.getInputRefToCallMap();
      List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      StringBuilder nullOrder = new StringBuilder();
      for (RelFieldCollation sortInfo : sortRel.getCollation().getFieldCollations()) {
        int sortColumnPos = sortInfo.getFieldIndex();
        ColumnInfo columnInfo = new ColumnInfo(inputOp.getSchema().getSignature()
            .get(sortColumnPos));
        ExprNodeColumnDesc sortColumn = new ExprNodeColumnDesc(columnInfo.getType(),
            columnInfo.getInternalName(), columnInfo.getTabAlias(), columnInfo.getIsVirtualCol());
        sortCols.add(sortColumn);
        if (sortInfo.getDirection() == RelFieldCollation.Direction.DESCENDING) {
          order.append("-");
        } else {
          order.append("+");
        }
        if (sortInfo.nullDirection == RelFieldCollation.NullDirection.FIRST) {
          nullOrder.append("a");
        } else if (sortInfo.nullDirection == RelFieldCollation.NullDirection.LAST) {
          nullOrder.append("z");
        } else {
          // Default
          nullOrder.append(sortInfo.getDirection() == RelFieldCollation.Direction.DESCENDING ? "z" : "a");
        }

        if (obRefToCallMap != null) {
          RexNode obExpr = obRefToCallMap.get(sortColumnPos);
          sortColsPosBuilder.set(sortColumnPos);
          if (obExpr == null) {
            sortOutputColsPosBuilder.set(sortColumnPos);
          }
        }
      }
      // Use only 1 reducer for order by
      int numReducers = 1;

      // We keep the columns only the columns that are part of the final output
      List<String> keepColumns = new ArrayList<String>();
      final ImmutableBitSet sortColsPos = sortColsPosBuilder.build();
      final ImmutableBitSet sortOutputColsPos = sortOutputColsPosBuilder.build();
      final List<ColumnInfo> inputSchema = inputOp.getSchema().getSignature();
      for (int pos=0; pos<inputSchema.size(); pos++) {
        if ((sortColsPos.get(pos) && sortOutputColsPos.get(pos)) ||
                (!sortColsPos.get(pos) && !sortOutputColsPos.get(pos))) {
          keepColumns.add(inputSchema.get(pos).getInternalName());
        }
      }

      // 1.b. Generate reduce sink and project operator
      resultOp = HiveOpConverterUtils.genReduceSinkAndBacktrackSelect(resultOp, sortCols.toArray(
          new ExprNodeDesc[sortCols.size()]), 0, new ArrayList<ExprNodeDesc>(), order.toString(), nullOrder.toString(),
          numReducers, Operation.NOT_ACID, hiveOpConverter.getHiveConf(), keepColumns);
    }

    // 2. If we need to generate limit
    if (sortRel.fetch != null) {
      int limit = RexLiteral.intValue(sortRel.fetch);
      int offset = sortRel.offset == null ? 0 : RexLiteral.intValue(sortRel.offset);
      LimitDesc limitDesc = new LimitDesc(offset,limit);
      ArrayList<ColumnInfo> cinfoLst = HiveOpConverterUtils.createColInfos(resultOp);
      resultOp = OperatorFactory.getAndMakeChild(limitDesc, new RowSchema(cinfoLst), resultOp);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + resultOp + " with row schema: [" + resultOp.getSchema() + "]");
      }
    }

    // 3. Return result
    return inputOpAf.clone(resultOp);
  }
}
