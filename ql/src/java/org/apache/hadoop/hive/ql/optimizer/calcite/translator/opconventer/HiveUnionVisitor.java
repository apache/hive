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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

class HiveUnionVisitor extends HiveRelNodeVisitor<HiveUnion> {
  HiveUnionVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveUnion unionRel) throws SemanticException {
    // 1. Convert inputs
    List<RelNode> inputsList = extractRelNodeFromUnion(unionRel);
    OpAttr[] inputs = new OpAttr[inputsList.size()];
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = hiveOpConverter.dispatch(inputsList.get(i));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + unionRel.getId() + ":" + unionRel.getRelTypeName()
          + " with row type: [" + unionRel.getRowType() + "]");
    }

    // 2. Create a new union operator
    UnionDesc unionDesc = new UnionDesc();
    unionDesc.setNumInputs(inputs.length);
    String tableAlias = hiveOpConverter.getHiveDerivedTableAlias();
    ArrayList<ColumnInfo> cinfoLst = createColInfos(inputs[0].inputs.get(0), tableAlias);
    Operator<?>[] children = new Operator<?>[inputs.length];
    for (int i = 0; i < children.length; i++) {
      if (i == 0) {
        children[i] = inputs[i].inputs.get(0);
      } else {
        Operator<?> op = inputs[i].inputs.get(0);
        // We need to check if the other input branches for union is following the first branch
        // We may need to cast the data types for specific columns.
        children[i] = genInputSelectForUnion(op, cinfoLst);
      }
    }
    Operator<? extends OperatorDesc> unionOp = OperatorFactory.getAndMakeChild(
        hiveOpConverter.getSemanticAnalyzer().getOpContext(), unionDesc, new RowSchema(cinfoLst), children);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + unionOp + " with row schema: [" + unionOp.getSchema() + "]");
    }

    //TODO: Can columns retain virtualness out of union
    // 3. Return result
    return new OpAttr(tableAlias, inputs[0].vcolsInCalcite, unionOp);
  }

  // use this function to make the union "flat" for both execution and explain purpose
  private List<RelNode> extractRelNodeFromUnion(HiveUnion unionRel) {
    List<RelNode> ret = new ArrayList<RelNode>();
    for (RelNode input : unionRel.getInputs()) {
      if (input instanceof HiveUnion) {
        ret.addAll(extractRelNodeFromUnion((HiveUnion) input));
      } else {
        ret.add(input);
      }
    }
    return ret;
  }

  //create column info with new tableAlias
  private ArrayList<ColumnInfo> createColInfos(Operator<?> input, String tableAlias) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      ColumnInfo copyOfColumnInfo = new ColumnInfo(ci);
      copyOfColumnInfo.setTabAlias(tableAlias);
      cInfoLst.add(copyOfColumnInfo);
    }
    return cInfoLst;
  }

  private Operator<? extends OperatorDesc> genInputSelectForUnion(Operator<? extends OperatorDesc> origInputOp,
      ArrayList<ColumnInfo> uColumnInfo) throws SemanticException {
    Iterator<ColumnInfo> oIter = origInputOp.getSchema().getSignature().iterator();
    Iterator<ColumnInfo> uIter = uColumnInfo.iterator();
    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    List<String> colName = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    boolean needSelectOp = false;
    while (oIter.hasNext()) {
      ColumnInfo oInfo = oIter.next();
      ColumnInfo uInfo = uIter.next();
      if (!oInfo.isSameColumnForRR(uInfo)) {
        needSelectOp = true;
      }
      ExprNodeDesc column = new ExprNodeColumnDesc(oInfo.getType(), oInfo.getInternalName(),
          oInfo.getTabAlias(), oInfo.getIsVirtualCol(), oInfo.isSkewedCol());
      if (!oInfo.getType().equals(uInfo.getType())) {
        column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(column, (PrimitiveTypeInfo) uInfo.getType());
      }
      columns.add(column);
      colName.add(uInfo.getInternalName());
      columnExprMap.put(uInfo.getInternalName(), column);
    }
    if (needSelectOp) {
      return OperatorFactory.getAndMakeChild(new SelectDesc(
          columns, colName), new RowSchema(uColumnInfo), columnExprMap, origInputOp);
    } else {
      return origInputOp;
    }
  }
}
