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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;

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
    } else if (distribution.getType() != Type.ANY) {
      throw new SemanticException("Only hash distribution supported for HiveSortExchange");
    } else {
      partitionKeyList = new ArrayList<>(0);
    }
    ExprNodeDesc[] expressions = new ExprNodeDesc[exchangeRel.getKeys().size()];
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    for (int index = 0; index < exchangeRel.getCollation().getFieldCollations().size(); index++) {
      RelFieldCollation fieldCollation = exchangeRel.getCollation().getFieldCollations().get(index);
      expressions[index] = HiveOpConverterUtils.convertToExprNode(exchangeRel.getKeys().get(index),
          exchangeRel.getInput(), inputOpAf.tabAlias, inputOpAf.vcolsInCalcite);

      order.append(DirectionUtils.codeToSign(DirectionUtils.directionToCode(fieldCollation.getDirection())));
      nullOrder.append(NullOrdering.fromDirection(fieldCollation.nullDirection).getSign());
    }
    exchangeRel.setKeyExpressions(expressions);

    Operator<?> inputOp = inputOpAf.inputs.get(0);
    List<String> keepColumns = new ArrayList<>();
    final List<ColumnInfo> inputSchema = inputOp.getSchema().getSignature();
    for (ColumnInfo columnInfo : inputSchema) {
      keepColumns.add(columnInfo.getInternalName());
    }

    ReduceSinkOperator resultOp = HiveOpConverterUtils.genReduceSink(inputOpAf.inputs.get(0), tabAlias, expressions,
            -1, partitionKeyList, order.toString(), nullOrder.toString(), -1, Operation.NOT_ACID,
            hiveOpConverter.getHiveConf());

    return new OpAttr(tabAlias, inputOpAf.vcolsInCalcite, resultOp);
  }
}
