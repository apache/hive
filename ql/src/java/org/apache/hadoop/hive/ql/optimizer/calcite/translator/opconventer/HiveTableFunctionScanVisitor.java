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
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

class HiveTableFunctionScanVisitor extends HiveRelNodeVisitor<HiveTableFunctionScan> {
  HiveTableFunctionScanVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveTableFunctionScan scanRel) throws SemanticException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Translating operator rel#" + scanRel.getId() + ":"
          + scanRel.getRelTypeName() + " with row type: [" + scanRel.getRowType() + "]");
    }

    RexCall call = (RexCall)scanRel.getCall();

    RowResolver rowResolver = new RowResolver();
    List<String> fieldNames = new ArrayList<>(scanRel.getRowType().getFieldNames());
    List<String> functionFieldNames = new ArrayList<>();
    List<ExprNodeDesc> exprCols = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    for (int pos = 0; pos < call.getOperands().size(); pos++) {
      ExprNodeConverter converter = new ExprNodeConverter(SemanticAnalyzer.DUMMY_TABLE, fieldNames.get(pos),
          scanRel.getRowType(), scanRel.getRowType(), ((HiveTableScan)scanRel.getInput(0)).getPartOrVirtualCols(),
          scanRel.getCluster().getTypeFactory(), true);
      ExprNodeDesc exprCol = call.getOperands().get(pos).accept(converter);
      colExprMap.put(HiveConf.getColumnInternalName(pos), exprCol);
      exprCols.add(exprCol);

      ColumnInfo columnInfo = new ColumnInfo(HiveConf.getColumnInternalName(pos),
          exprCol.getWritableObjectInspector(), SemanticAnalyzer.DUMMY_TABLE, false);
      rowResolver.put(columnInfo.getTabAlias(), columnInfo.getAlias(), columnInfo);

      functionFieldNames.add(HiveConf.getColumnInternalName(pos));
    }

    OpAttr inputOpAf = hiveOpConverter.dispatch(scanRel.getInputs().get(0));
    TableScanOperator op = (TableScanOperator)inputOpAf.inputs.get(0);
    op.getConf().setRowLimit(1);

    Operator<?> output = OperatorFactory.getAndMakeChild(new SelectDesc(exprCols, functionFieldNames, false),
        new RowSchema(rowResolver.getRowSchema()), op);
    output.setColumnExprMap(colExprMap);

    Operator<?> funcOp = genUDTFPlan(call, functionFieldNames, output, rowResolver);

    return new OpAttr(null, new HashSet<Integer>(), funcOp);
  }

  private Operator<?> genUDTFPlan(RexCall call, List<String> colAliases, Operator<?> input, RowResolver rowResolver)
      throws SemanticException {
    LOG.debug("genUDTFPlan, Col aliases: {}", colAliases);

    GenericUDTF genericUDTF = createGenericUDTF(call);
    StructObjectInspector rowOI = createStructObjectInspector(rowResolver, colAliases);
    StructObjectInspector outputOI = genericUDTF.initialize(rowOI);
    List<ColumnInfo> columnInfos = createColumnInfos(outputOI);

    // Add the UDTFOperator to the operator DAG
    return OperatorFactory.getAndMakeChild(new UDTFDesc(genericUDTF, false), new RowSchema(columnInfos), input);
  }

  private GenericUDTF createGenericUDTF(RexCall call) throws SemanticException {
    String functionName = call.getOperator().getName();
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(functionName);
    return fi.getGenericUDTF();
  }

  private StructObjectInspector createStructObjectInspector(RowResolver rowResolver, List<String> colAliases)
      throws SemanticException {
    // Create the object inspector for the input columns and initialize the UDTF
    List<String> colNames = rowResolver.getColumnInfos().stream().map(ci -> ci.getInternalName())
        .collect(Collectors.toList());
    List<ObjectInspector> colOIs = rowResolver.getColumnInfos().stream().map(ci -> ci.getObjectInspector())
        .collect(Collectors.toList());

    return ObjectInspectorFactory.getStandardStructObjectInspector(colNames, colOIs);
  }

  private List<ColumnInfo> createColumnInfos(StructObjectInspector outputOI) {
    // Generate the output column info's / row resolver using internal names.
    List<ColumnInfo> columnInfos = new ArrayList<>();
    for (StructField sf : outputOI.getAllStructFieldRefs()) {

      // Since the UDTF operator feeds into a LVJ operator that will rename all the internal names, we can just use
      // field name from the UDTF's OI as the internal name
      ColumnInfo col = new ColumnInfo(sf.getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()), null, false);
      col.setAlias(sf.getFieldName());
      columnInfos.add(col);
    }
    return columnInfos;
  }
}
