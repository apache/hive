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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

import static java.util.Arrays.asList;

class HiveSortExchangeVisitor extends HiveRelNodeVisitor<HiveSortExchange> {
  HiveSortExchangeVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveSortExchange exchangeRel) throws SemanticException {
    OpAttr inputOpAf = hiveOpConverter.dispatch(exchangeRel.getInput());
    String tabAlias = inputOpAf.tabAlias;
    if (tabAlias == null || tabAlias.length() == 0) {
      tabAlias = hiveOpConverter.getHiveDerivedTableAlias();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + exchangeRel.getId() + ":"
          + exchangeRel.getRelTypeName() + " with row type: [" + exchangeRel.getRowType() + "]");
    }

    RelDistribution distribution = exchangeRel.getDistribution();
    ArrayList<ExprNodeDesc> partitionKeyList;
    if (distribution.getType() == Type.HASH_DISTRIBUTED) {
      partitionKeyList = new ArrayList<>(exchangeRel.getDistribution().getKeys().size());
      for (int index = 0; index < exchangeRel.getDistribution().getKeys().size(); index++) {
        partitionKeyList.add(HiveOpConverterUtils.convertToExprNode(
                exchangeRel.getCluster().getRexBuilder().makeInputRef(exchangeRel.getInput(), index),
                exchangeRel.getInput(), inputOpAf.tabAlias, inputOpAf.vcolsInCalcite));
      }
//      for (int i = 0; i < distribution.getKeys().size(); i++) {
//        int key = distribution.getKeys().get(i);
//        ColumnInfo colInfo = inputOpAf.inputs.get(0).getSchema().getSignature().get(key);
//        ExprNodeDesc column = new ExprNodeColumnDesc(colInfo);
//        distributeKeys[i] = column;
//      }
    } else if (distribution.getType() != Type.ANY) {
      throw new SemanticException("Only hash distribution supported for HiveSortExchange");
    } else {
      partitionKeyList = new ArrayList<>(0);
    }
    ExprNodeDesc[] expressions = new ExprNodeDesc[exchangeRel.getKeys().size()];
    for (int index = 0; index < exchangeRel.getKeys().size(); index++) {
      expressions[index] = HiveOpConverterUtils.convertToExprNode(exchangeRel.getKeys().get(index),
          exchangeRel.getInput(), inputOpAf.tabAlias, inputOpAf.vcolsInCalcite);
    }
    exchangeRel.setKeyExpressions(expressions);

    Operator<?> inputOp = inputOpAf.inputs.get(0);
    List<String> keepColumns = new ArrayList<>();
//    final ImmutableBitSet sortColsPos = sortColsPosBuilder.build();
//    final ImmutableBitSet sortOutputColsPos = sortOutputColsPosBuilder.build();
    final List<ColumnInfo> inputSchema = inputOp.getSchema().getSignature();
    for (int pos=0; pos<inputSchema.size(); pos++) {
//      if ((sortColsPos.get(pos) && sortOutputColsPos.get(pos)) ||
//              (!sortColsPos.get(pos) && !sortOutputColsPos.get(pos))) {
        keepColumns.add(inputSchema.get(pos).getInternalName());
//      }
    }

    Operator<?> resultOp = HiveOpConverterUtils.genReduceSinkAndBacktrackSelect(
            inputOpAf.inputs.get(0), expressions, -1, partitionKeyList, "", "",
            -1, Operation.NOT_ACID, hiveOpConverter.getHiveConf(), keepColumns);

    return new OpAttr(tabAlias, inputOpAf.vcolsInCalcite, resultOp);
  }
}
