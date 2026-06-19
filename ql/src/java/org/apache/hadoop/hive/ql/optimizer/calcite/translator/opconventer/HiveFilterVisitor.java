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

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;

class HiveFilterVisitor extends HiveRelNodeVisitor<HiveFilter> {
  HiveFilterVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  /**
   * TODO: 1) isSamplingPred 2) sampleDesc 3) isSortedFilter.
   */
  @Override
  OpAttr visit(HiveFilter filterRel) throws SemanticException {
    OpAttr inputOpAf = hiveOpConverter.dispatch(filterRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + filterRel.getId() + ":" + filterRel.getRelTypeName()
          + " with row type: [" + filterRel.getRowType() + "]");
    }

    ExprNodeDesc filCondExpr = filterRel.getCondition().accept(
        new ExprNodeConverter(inputOpAf.tabAlias, filterRel.getInput().getRowType(), inputOpAf.vcolsInCalcite,
            filterRel.getCluster().getTypeFactory(), true));
    FilterDesc filDesc = new FilterDesc(filCondExpr, false);
    ArrayList<ColumnInfo> cinfoLst = HiveOpConverterUtils.createColInfos(inputOpAf.inputs.get(0));
    FilterOperator filOp = (FilterOperator) OperatorFactory.getAndMakeChild(filDesc,
        new RowSchema(cinfoLst), inputOpAf.inputs.get(0));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + filOp + " with row schema: [" + filOp.getSchema() + "]");
    }

    return inputOpAf.clone(filOp);
  }
}
