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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class GenMaterializedViewOrgPlan {

  private Operator<? extends OperatorDesc> newOperator;

  private Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap = new HashMap<>();

  public GenMaterializedViewOrgPlan(Table destinationTable, String sortColsStr, String distributeColsStr,
      RowResolver inputRR, Operator input) throws SemanticException {
    this(getSortColInfos(destinationTable, sortColsStr, inputRR),
        getDistributeColInfos(destinationTable, distributeColsStr, inputRR), inputRR, input);
  }

  public GenMaterializedViewOrgPlan(List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos,
      RowResolver inputRR, Operator input) {
    // In this case, we will introduce a RS and immediately after a SEL that restores
    // the row schema to what follow-up operations are expecting
    Set<String> keys = sortColInfos.stream()
        .map(ColumnInfo::getInternalName)
        .collect(Collectors.toSet());
    Set<String> distributeKeys = distributeColInfos.stream()
        .map(ColumnInfo::getInternalName)
        .collect(Collectors.toSet());
    List<ExprNodeDesc> keyCols = new ArrayList<>();
    List<String> keyColNames = new ArrayList<>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    List<ExprNodeDesc> valCols = new ArrayList<>();
    List<String> valColNames = new ArrayList<>();
    List<ExprNodeDesc> partCols = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    Map<String, String> nameMapping = new HashMap<>();
    // map _col0 to KEY._col0, etc
    for (ColumnInfo ci : inputRR.getRowSchema().getSignature()) {
      ExprNodeColumnDesc e = new ExprNodeColumnDesc(ci);
      String columnName = ci.getInternalName();
      if (keys.contains(columnName)) {
        // key (sort column)
        keyColNames.add(columnName);
        keyCols.add(e);
        colExprMap.put(Utilities.ReduceField.KEY + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.KEY + "." + columnName);
        order.append("+");
        nullOrder.append("a");
      } else {
        // value
        valColNames.add(columnName);
        valCols.add(e);
        colExprMap.put(Utilities.ReduceField.VALUE + "." + columnName, e);
        nameMapping.put(columnName, Utilities.ReduceField.VALUE + "." + columnName);
      }
      if (distributeKeys.contains(columnName)) {
        // distribute column
        partCols.add(e.clone());
      }
    }
    // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
    // the reduce operator will initialize Extract operator with information
    // from Key and Value TableDesc
    List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols,
        keyColNames, 0, "");
    TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, order.toString(), nullOrder.toString());
    List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols,
        valColNames, 0, "");
    TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
    List<List<Integer>> distinctColumnIndices = new ArrayList<>();
    // Number of reducers is set to default (-1)
    ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols,
        keyColNames, distinctColumnIndices, valColNames, -1, partCols, -1, keyTable,
        valueTable, Operation.NOT_ACID);
    RowResolver rsRR = new RowResolver();
    List<ColumnInfo> rsSignature = new ArrayList<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      colInfo.setInternalName(nameMapping.get(colInfo.getInternalName()));
      rsSignature.add(colInfo);
      rsRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
    }

    newOperator = OperatorUtils.createOperator(rsConf, new RowSchema(rsSignature), input);
    operatorMap.put(newOperator, new OpParseContext(rsRR));
    newOperator.setColumnExprMap(colExprMap);

    // Create SEL operator
    RowResolver selRR = new RowResolver();
    List<ColumnInfo> selSignature = new ArrayList<>();
    List<ExprNodeDesc> columnExprs = new ArrayList<>();
    List<String> colNames = new ArrayList<>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<>();
    for (int index = 0; index < input.getSchema().getSignature().size(); index++) {
      ColumnInfo colInfo = new ColumnInfo(input.getSchema().getSignature().get(index));
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      selSignature.add(colInfo);
      selRR.put(nm[0], nm[1], colInfo);
      if (nm2 != null) {
        selRR.addMappingOnly(nm2[0], nm2[1], colInfo);
      }
      String colName = colInfo.getInternalName();
      ExprNodeDesc exprNodeDesc;
      if (keys.contains(colName)) {
        exprNodeDesc = new ExprNodeColumnDesc(colInfo.getType(), ReduceField.KEY.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      } else {
        exprNodeDesc = new ExprNodeColumnDesc(colInfo.getType(), ReduceField.VALUE.toString() + "." + colName, null, false);
        columnExprs.add(exprNodeDesc);
      }
      colNames.add(colName);
      selColExprMap.put(colName, exprNodeDesc);
    }
    SelectDesc selConf = new SelectDesc(columnExprs, colNames);
    newOperator = OperatorUtils.createOperator(selConf, new RowSchema(selSignature), newOperator);
    operatorMap.put(newOperator, new OpParseContext(selRR));
    newOperator.setColumnExprMap(selColExprMap);
  }

  private static List<ColumnInfo> getSortColInfos(Table destinationTable, String sortColsStr,
      RowResolver inputRR) throws SemanticException {
    Map<String, Integer> colNameToIdx = new HashMap<>();
    for (int i = 0; i < destinationTable.getCols().size(); i++) {
      colNameToIdx.put(destinationTable.getCols().get(i).getName(), i);
    }
    List<ColumnInfo> colInfos = inputRR.getColumnInfos();
    List<ColumnInfo> sortColInfos = new ArrayList<>();
    if (sortColsStr != null) {
      Utilities.decodeColumnNames(sortColsStr)
          .forEach(s -> sortColInfos.add(colInfos.get(colNameToIdx.get(s))));
    }
    return sortColInfos;
  }

  private static List<ColumnInfo> getDistributeColInfos(Table destinationTable, String distributeColsStr,
      RowResolver inputRR) throws SemanticException {
    Map<String, Integer> colNameToIdx = new HashMap<>();
    for (int i = 0; i < destinationTable.getCols().size(); i++) {
      colNameToIdx.put(destinationTable.getCols().get(i).getName(), i);
    }
    List<ColumnInfo> colInfos = inputRR.getColumnInfos();
    List<ColumnInfo> distributeColInfos = new ArrayList<>();
    if (distributeColsStr != null) {
      Utilities.decodeColumnNames(distributeColsStr)
          .forEach(s -> distributeColInfos.add(colInfos.get(colNameToIdx.get(s))));
    }
    return distributeColInfos;
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return newOperator;
  }

  public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
    return operatorMap;
  }
}
