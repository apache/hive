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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HiveOpConverterUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveOpConverterUtils.class);

  private HiveOpConverterUtils() {
    throw new UnsupportedOperationException("HiveOpConverterUtils should not be instantiated!");
  }

  static SelectOperator genReduceSinkAndBacktrackSelect(Operator<?> input,
      ExprNodeDesc[] keys, int tag, ArrayList<ExprNodeDesc> partitionCols, String order, String nullOrder,
      int numReducers, Operation acidOperation, HiveConf hiveConf,
      List<String> keepColNames) throws SemanticException {
    // 1. Generate RS operator
    // 1.1 Prune the tableNames, only count the tableNames that are not empty strings
  // as empty string in table aliases is only allowed for virtual columns.
    String tableAlias = null;
    Set<String> tableNames = input.getSchema().getTableNames();
    for (String tableName : tableNames) {
      if (tableName != null) {
        if (tableName.length() == 0) {
          if (tableAlias == null) {
            tableAlias = tableName;
          }
        } else {
          if (tableAlias == null || tableAlias.length() == 0) {
            tableAlias = tableName;
          } else {
            if (!tableName.equals(tableAlias)) {
              throw new SemanticException("In CBO return path, genReduceSinkAndBacktrackSelect is expecting only " +
                  "one tableAlias but there is more than one");
            }
          }
        }
      }
    }
    if (tableAlias == null) {
      throw new SemanticException(
          "In CBO return path, genReduceSinkAndBacktrackSelect is expecting only one tableAlias but there is none");
    }
    // 1.2 Now generate RS operator
    ReduceSinkOperator rsOp = genReduceSink(input, tableAlias, keys, tag, partitionCols, order,
            nullOrder, numReducers, acidOperation, hiveConf);

    // 2. Generate backtrack Select operator
    Map<String, ExprNodeDesc> descriptors = buildBacktrackFromReduceSink(keepColNames,
        rsOp.getConf().getOutputKeyColumnNames(), rsOp.getConf().getOutputValueColumnNames(),
        rsOp.getValueIndex(), input);
    SelectDesc selectDesc = new SelectDesc(new ArrayList<ExprNodeDesc>(descriptors.values()),
        new ArrayList<String>(descriptors.keySet()));
    ArrayList<ColumnInfo> cinfoLst = createColInfosSubset(input, keepColNames);
    SelectOperator selectOp = (SelectOperator) OperatorFactory.getAndMakeChild(
        selectDesc, new RowSchema(cinfoLst), rsOp);
    selectOp.setColumnExprMap(descriptors);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + selectOp + " with row schema: [" + selectOp.getSchema() + "]");
    }

    return selectOp;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  static ReduceSinkOperator genReduceSink(Operator<?> input, String tableAlias, ExprNodeDesc[] keys, int tag,
      ArrayList<ExprNodeDesc> partitionCols, String order, String nullOrder, int numReducers,
      Operation acidOperation, HiveConf hiveConf) throws SemanticException {
    Operator dummy = Operator.createDummy(); // dummy for backtracking
    dummy.setParentOperators(Arrays.asList(input));

    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeysBack = new ArrayList<ExprNodeDesc>();

    // Compute join keys and store in reduceKeys
    for (ExprNodeDesc key : keys) {
      reduceKeys.add(key);
      reduceKeysBack.add(ExprNodeDescUtils.backtrack(key, dummy, input));
    }

    // Walk over the input schema and copy in the output
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceValuesBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    List<ColumnInfo> inputColumns = input.getSchema().getSignature();
    ArrayList<ColumnInfo> outputColumns = new ArrayList<ColumnInfo>();
    List<String> outputColumnNames = new ArrayList<String>();
    int[] index = new int[inputColumns.size()];
    for (int i = 0; i < inputColumns.size(); i++) {
      ColumnInfo colInfo = inputColumns.get(i);
      String outputColName = colInfo.getInternalName();
      ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc exprBack = ExprNodeDescUtils.backtrack(expr, dummy, input);
      int kindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceKeysBack);
      if (kindex >= 0) {
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setAlias(outputColName);
        newColInfo.setTabAlias(tableAlias);
        outputColumns.add(newColInfo);
        index[i] = kindex;
        continue;
      }
      int vindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceValuesBack);
      if (vindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -reduceValues.size() - 1;

      reduceValues.add(expr);
      reduceValuesBack.add(exprBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setAlias(outputColName);
      newColInfo.setTabAlias(tableAlias);

      outputColumns.add(newColInfo);
      outputColumnNames.add(outputColName);
    }
    dummy.setParentOperators(null);

    // Use only 1 reducer if no reduce keys
    if (reduceKeys.size() == 0) {
      numReducers = 1;

      // Cartesian product is not supported in strict mode
      String error = StrictChecks.checkCartesian(hiveConf);
      if (error != null) {
        throw new SemanticException(error);
      }
    }

    ReduceSinkDesc rsDesc;
    if (order.isEmpty()) {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          reduceKeys.size(), numReducers, acidOperation, NullOrdering.defaultNullOrder(hiveConf));
    } else {
      rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, reduceValues, outputColumnNames, false, tag,
          partitionCols, order, nullOrder, NullOrdering.defaultNullOrder(hiveConf), numReducers, acidOperation, false);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
        rsDesc, new RowSchema(outputColumns), input);

    List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
    for (int i = 0; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), reduceKeys.get(i));
    }
    List<String> valColNames = rsDesc.getOutputValueColumnNames();
    for (int i = 0; i < valColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valColNames.get(i), reduceValues.get(i));
    }

    rsOp.setValueIndex(index);
    rsOp.setColumnExprMap(colExprMap);
    rsOp.setInputAliases(input.getSchema().getTableNames()
        .toArray(new String[input.getSchema().getTableNames().size()]));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + rsOp + " with row schema: [" + rsOp.getSchema() + "]");
    }

    return rsOp;
  }

  private static Map<String, ExprNodeDesc> buildBacktrackFromReduceSink(List<String> keepColNames,
      List<String> keyColNames, List<String> valueColNames, int[] index, Operator<?> inputOp) {
    Map<String, ExprNodeDesc> columnDescriptors = new LinkedHashMap<String, ExprNodeDesc>();
    int pos = 0;
    for (int i = 0; i < index.length; i++) {
      ColumnInfo info = inputOp.getSchema().getSignature().get(i);
      if (pos < keepColNames.size() &&
              info.getInternalName().equals(keepColNames.get(pos))) {
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
        }
        ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(), field, info.getTabAlias(),
            info.getIsVirtualCol());
        columnDescriptors.put(keepColNames.get(pos), desc);
        pos++;
      }
    }
    return columnDescriptors;
  }

  static ExprNodeDesc convertToExprNode(RexNode rn, RelNode inputRel, String tabAlias, Set<Integer> vcolsInCalcite) {
    return rn.accept(new ExprNodeConverter(tabAlias, inputRel.getRowType(), vcolsInCalcite,
        inputRel.getCluster().getTypeFactory(), true));
  }

  static ArrayList<ColumnInfo> createColInfos(Operator<?> input) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      cInfoLst.add(new ColumnInfo(ci));
    }
    return cInfoLst;
  }

  private static ArrayList<ColumnInfo> createColInfosSubset(Operator<?> input,
          List<String> keepColNames) {
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    int pos = 0;
    for (ColumnInfo ci : input.getSchema().getSignature()) {
      if (pos < keepColNames.size() &&
              ci.getInternalName().equals(keepColNames.get(pos))) {
        cInfoLst.add(new ColumnInfo(ci));
        pos++;
      }
    }
    return cInfoLst;
  }
}
