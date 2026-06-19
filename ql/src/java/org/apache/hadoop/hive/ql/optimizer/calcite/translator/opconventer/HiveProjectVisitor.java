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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingComponentizer;
import org.apache.hadoop.hive.ql.parse.WindowingSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

class HiveProjectVisitor extends HiveRelNodeVisitor<HiveProject> {
  HiveProjectVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveProject projectRel) throws SemanticException {
    OpAttr inputOpAf = hiveOpConverter.dispatch(projectRel.getInput());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + projectRel.getId() + ":"
          + projectRel.getRelTypeName() + " with row type: [" + projectRel.getRowType() + "]");
    }

    WindowingSpec windowingSpec = new WindowingSpec();
    List<String> exprNames = new ArrayList<String>(projectRel.getRowType().getFieldNames());
    List<ExprNodeDesc> exprCols = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int pos = 0; pos < projectRel.getProjects().size(); pos++) {
      ExprNodeConverter converter = new ExprNodeConverter(inputOpAf.tabAlias, projectRel
          .getRowType().getFieldNames().get(pos), projectRel.getInput().getRowType(),
          projectRel.getRowType(), inputOpAf.vcolsInCalcite, projectRel.getCluster().getTypeFactory(),
          true);
      ExprNodeDesc exprCol = projectRel.getProjects().get(pos).accept(converter);
      colExprMap.put(exprNames.get(pos), exprCol);
      exprCols.add(exprCol);
      //TODO: Cols that come through PTF should it retain (VirtualColumness)?
      if (converter.getWindowFunctionSpec() != null) {
        for (WindowFunctionSpec wfs : converter.getWindowFunctionSpec()) {
          windowingSpec.addWindowFunction(wfs);
        }
      }
    }
    if (windowingSpec.getWindowExpressions() != null
        && !windowingSpec.getWindowExpressions().isEmpty()) {
      inputOpAf = genPTF(inputOpAf, windowingSpec);
    }
    // TODO: is this a safe assumption (name collision, external names...)
    SelectDesc sd = new SelectDesc(exprCols, exprNames);
    Pair<ArrayList<ColumnInfo>, Set<Integer>> colInfoVColPair = createColInfos(projectRel.getProjects(), exprCols,
        exprNames, inputOpAf);
    SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(sd, new RowSchema(
        colInfoVColPair.getKey()), inputOpAf.inputs.get(0));
    selOp.setColumnExprMap(colExprMap);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selOp + " with row schema: [" + selOp.getSchema() + "]");
    }

    return new OpAttr(inputOpAf.tabAlias, colInfoVColPair.getValue(), selOp);
  }

  private OpAttr genPTF(OpAttr inputOpAf, WindowingSpec wSpec) throws SemanticException {
    Operator<?> input = inputOpAf.inputs.get(0);

    wSpec.validateAndMakeEffective();
    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = new RowResolver();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      rr.put(inputOpAf.tabAlias, ci.getInternalName(), ci);
    }

    while (groups.hasNext()) {
      wSpec = groups.next(hiveOpConverter.getHiveConf(), hiveOpConverter.getSemanticAnalyzer(),
          hiveOpConverter.getUnparseTranslator(), rr);

      // 1. Create RS and backtrack Select operator on top
      ArrayList<ExprNodeDesc> keyCols = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
      StringBuilder order = new StringBuilder();
      StringBuilder nullOrder = new StringBuilder();

      for (PartitionExpression partCol : wSpec.getQueryPartitionSpec().getExpressions()) {
        ExprNodeDesc partExpr = hiveOpConverter.getSemanticAnalyzer().genExprNodeDesc(partCol.getExpression(), rr);
        if (ExprNodeDescUtils.indexOf(partExpr, partCols) < 0) {
          keyCols.add(partExpr);
          partCols.add(partExpr);
          order.append('+');
          nullOrder.append('a');
        }
      }

      if (wSpec.getQueryOrderSpec() != null) {
        for (OrderExpression orderCol : wSpec.getQueryOrderSpec().getExpressions()) {
          ExprNodeDesc orderExpr = hiveOpConverter.getSemanticAnalyzer().genExprNodeDesc(orderCol.getExpression(), rr);
          char orderChar = orderCol.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
          char nullOrderChar = orderCol.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST ? 'a' : 'z';
          int index = ExprNodeDescUtils.indexOf(orderExpr, keyCols);
          if (index >= 0) {
            order.setCharAt(index, orderChar);
            nullOrder.setCharAt(index, nullOrderChar);
            continue;
          }
          keyCols.add(orderExpr);
          order.append(orderChar);
          nullOrder.append(nullOrderChar);
        }
      }

      SelectOperator selectOp = genReduceSinkAndBacktrackSelect(input,
          keyCols.toArray(new ExprNodeDesc[keyCols.size()]), 0, partCols,
          order.toString(), nullOrder.toString(), -1, Operation.NOT_ACID, hiveOpConverter.getHiveConf());

      // 2. Finally create PTF
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, hiveOpConverter.getSemanticAnalyzer(),
          hiveOpConverter.getHiveConf(), rr, hiveOpConverter.getUnparseTranslator());
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();

      Operator<?> ptfOp = OperatorFactory.getAndMakeChild(
          ptfDesc, new RowSchema(ptfOpRR.getColumnInfos()), selectOp);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated " + ptfOp + " with row schema: [" + ptfOp.getSchema() + "]");
      }

      // 3. Prepare for next iteration (if any)
      rr = ptfOpRR;
      input = ptfOp;
    }

    return inputOpAf.clone(input);
  }

  private static SelectOperator genReduceSinkAndBacktrackSelect(Operator<?> input,
          ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols, String order,
          String nullOrder, int numReducers, Operation acidOperation, HiveConf hiveConf)
              throws SemanticException {
    return HiveOpConverterUtils.genReduceSinkAndBacktrackSelect(input, keys, tag, partitionCols, order, nullOrder,
        numReducers, acidOperation, hiveConf, input.getSchema().getColumnNames());
  }

  private Pair<ArrayList<ColumnInfo>, Set<Integer>> createColInfos(List<RexNode> calciteExprs,
      List<ExprNodeDesc> hiveExprs, List<String> projNames, OpAttr inpOpAf) {
    if (hiveExprs.size() != projNames.size()) {
      throw new RuntimeException("Column expressions list doesn't match Column Names list");
    }

    RexNode rexN;
    ExprNodeDesc pe;
    ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
    boolean vc;
    Set<Integer> newVColSet = new HashSet<Integer>();
    for (int i = 0; i < hiveExprs.size(); i++) {
      pe = hiveExprs.get(i);
      rexN = calciteExprs.get(i);
      vc = false;
      if (rexN instanceof RexInputRef) {
        if (inpOpAf.vcolsInCalcite.contains(((RexInputRef) rexN).getIndex())) {
          newVColSet.add(i);
          vc = true;
        }
      }
      colInfos.add(new ColumnInfo(projNames.get(i), pe.getTypeInfo(), inpOpAf.tabAlias, vc));
    }

    return new Pair<ArrayList<ColumnInfo>, Set<Integer>>(colInfos, newVColSet);
  }
}
