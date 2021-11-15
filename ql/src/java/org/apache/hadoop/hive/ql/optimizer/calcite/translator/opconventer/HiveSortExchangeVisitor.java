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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

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
    if (distribution.getType() != Type.HASH_DISTRIBUTED) {
      throw new SemanticException("Only hash distribution supported for LogicalExchange");
    }
    ExprNodeDesc[] expressions = new ExprNodeDesc[exchangeRel.getKeys().size()];
    for (int index = 0; index < exchangeRel.getKeys().size(); index++) {
      expressions[index] = HiveOpConverterUtils.convertToExprNode(exchangeRel.getKeys().get(index),
          exchangeRel.getInput(), inputOpAf.tabAlias, inputOpAf.vcolsInCalcite);
    }
    exchangeRel.setKeyExpressions(expressions);

    ReduceSinkOperator rsOp = genReduceSink(inputOpAf.inputs.get(0), tabAlias, expressions,
        -1, -1, Operation.NOT_ACID, hiveOpConverter.getHiveConf());

    return new OpAttr(tabAlias, inputOpAf.vcolsInCalcite, rsOp);
  }

  private static ReduceSinkOperator genReduceSink(Operator<?> input, String tableAlias, ExprNodeDesc[] keys, int tag,
      int numReducers, Operation acidOperation, HiveConf hiveConf) throws SemanticException {
    return HiveOpConverterUtils.genReduceSink(input, tableAlias, keys, tag, new ArrayList<ExprNodeDesc>(), "", "",
        numReducers, acidOperation, hiveConf);
  }
}
