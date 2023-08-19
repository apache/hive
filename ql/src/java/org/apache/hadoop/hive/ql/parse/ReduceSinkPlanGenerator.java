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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

public class ReduceSinkPlanGenerator {

  public static Result genPlan(String dest, QB qb, Operator<?> input,
      int numReducers, boolean hasOrderBy,
      HiveConf conf,
      HiveTxnManager txnMgr,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      UnparseTranslator unparseTranslator
      ) throws SemanticException {
    RowResolver inputRR = operatorMap.get(input).getRowResolver();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for
    // partition function
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        partCols.add(SemanticAnalyzer.genExprNodeDesc(cl, inputRR, true, false, unparseTranslator, conf));
      }
    }
    ASTNode sortExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getSortByForClause(dest);
    }

    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getOrderByForClause(dest);
      if (sortExprs != null) {
        assert numReducers == 1;
        // in strict mode, in the presence of order by, limit must be specified
        if (qb.getParseInfo().getDestLimit(dest) == null) {
          String error = StrictChecks.checkNoLimit(conf);
          if (error != null) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(sortExprs, error));
          }
        }
      }
    }
    List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          // SortBy ASC
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          // SortBy DESC
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else {
          // ClusterBy
          order.append("+");
          nullOrder.append("a");
        }
        ExprNodeDesc exprNode = SemanticAnalyzer.genExprNodeDesc(cl, inputRR, true, false, unparseTranslator, conf);
        sortCols.add(exprNode);
      }
    }

    Table dest_tab = qb.getMetaData().getDestTableForAlias(dest);
    AcidUtils.Operation acidOp = Operation.NOT_ACID;
    if (AcidUtils.isTransactionalTable(dest_tab)) {
      acidOp = SemanticAnalyzer.getAcidType(Utilities.getTableDesc(dest_tab).getOutputFileFormatClass(), dest,
          AcidUtils.isInsertOnlyTable(dest_tab), txnMgr);
    }
    boolean isCompaction = false;
    if (dest_tab != null && dest_tab.getParameters() != null) {
      isCompaction = AcidUtils.isCompactionTable(dest_tab.getParameters());
    }
    Result finalResult = genPlan(
        input, partCols, sortCols, order.toString(), nullOrder.toString(),
        numReducers, acidOp, true, isCompaction, sa, operatorMap);
    Operator<? extends OperatorDesc> reduceOperator = finalResult.getOperator();
    
    if (reduceOperator.getParentOperators().size() == 1 &&
        reduceOperator.getParentOperators().get(0) instanceof ReduceSinkOperator) {
      ((ReduceSinkOperator) reduceOperator.getParentOperators().get(0))
          .getConf().setHasOrderBy(hasOrderBy);
    }
    return new Result(reduceOperator, finalResult.getOperatorMap());
  }

  public static Result genPlan(Operator<?> input,
      List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
      String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp, boolean isCompaction,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException {
    return genPlan(input, partitionCols, sortCols, sortOrder, nullOrder, numReducers,
        acidOp, false, isCompaction, sa, operatorMap);
  }

  @SuppressWarnings("nls")
  public static Result genPlan(Operator<?> input,
      List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
      String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp,
      boolean pullConstants, boolean isCompaction, ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
                                     ) throws SemanticException {
    Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap = new HashMap<>();
    RowResolver inputRR = operatorMap.get(input).getRowResolver();

    Operator dummy = Operator.createDummy();
    dummy.setParentOperators(Arrays.asList(input));

    List<ExprNodeDesc> newSortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder newSortOrder = new StringBuilder();
    StringBuilder newNullOrder = new StringBuilder();
    List<ExprNodeDesc> sortColsBack = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < sortCols.size(); i++) {
      ExprNodeDesc sortCol = sortCols.get(i);
      // If we are not pulling constants, OR
      // we are pulling constants but this is not a constant
      if (!pullConstants || !(sortCol instanceof ExprNodeConstantDesc)) {
        newSortCols.add(sortCol);
        newSortOrder.append(sortOrder.charAt(i));
        newNullOrder.append(nullOrder.charAt(i));
        sortColsBack.add(ExprNodeDescUtils.backtrack(sortCol, dummy, input));
      }
    }

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    RowResolver rsRR = new RowResolver();
    List<String> outputColumns = new ArrayList<String>();
    List<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> valueColsBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> constantCols = new ArrayList<ExprNodeDesc>();

    List<ColumnInfo> columnInfos = inputRR.getColumnInfos();

    int[] index = new int[columnInfos.size()];
    for (int i = 0; i < index.length; i++) {
      ColumnInfo colInfo = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeColumnDesc value = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc valueBack = ExprNodeDescUtils.backtrack(value, dummy, input);
      if (pullConstants && valueBack instanceof ExprNodeConstantDesc) {
        // ignore, it will be generated by SEL op
        index[i] = Integer.MAX_VALUE;
        constantCols.add(valueBack);
        continue;
      }
      int kindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, sortColsBack);
      if (kindex >= 0) {
        index[i] = kindex;
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setTabAlias(nm[0]);
        rsRR.put(nm[0], nm[1], newColInfo);
        if (nm2 != null) {
          rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
        }
        continue;
      }
      int vindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, valueColsBack);
      if (vindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -valueCols.size() - 1;
      String outputColName = SemanticAnalyzer.getColumnInternalName(valueCols.size());

      valueCols.add(value);
      valueColsBack.add(valueBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setTabAlias(nm[0]);

      rsRR.put(nm[0], nm[1], newColInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
      }
      outputColumns.add(outputColName);
    }

    dummy.setParentOperators(null);

    ReduceSinkDesc rsdesc = PlanUtils.getReduceSinkDesc(newSortCols, valueCols, outputColumns,
        false, -1, partitionCols, newSortOrder.toString(), newNullOrder.toString(), sa.getDefaultNullOrdering(),
        numReducers, acidOp, isCompaction);
    Operator interim = OperatorUtils.createOperator(rsdesc,
        new RowSchema(rsRR.getColumnInfos()), input);

    finalOperatorMap.put(interim, new OpParseContext(rsRR));

    List<String> keyColNames = rsdesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), newSortCols.get(i));
    }
    List<String> valueColNames = rsdesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valueColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valueColNames.get(i), valueCols.get(i));
    }
    interim.setColumnExprMap(colExprMap);

    RowResolver selectRR = new RowResolver();
    List<ExprNodeDesc> selCols = new ArrayList<ExprNodeDesc>();
    List<String> selOutputCols = new ArrayList<String>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();

    Iterator<ExprNodeDesc> constants = constantCols.iterator();
    for (int i = 0; i < index.length; i++) {
      ColumnInfo prev = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(prev.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(prev.getInternalName());
      ColumnInfo info = new ColumnInfo(prev);

      ExprNodeDesc desc;
      if (index[i] == Integer.MAX_VALUE) {
        desc = constants.next();
      } else {
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
        }
        desc = new ExprNodeColumnDesc(info.getType(),
            field, info.getTabAlias(), info.getIsVirtualCol());
      }
      selCols.add(desc);

      String internalName = SemanticAnalyzer.getColumnInternalName(i);
      info.setInternalName(internalName);
      selectRR.put(nm[0], nm[1], info);
      if (nm2 != null) {
        selectRR.addMappingOnly(nm2[0], nm2[1], info);
      }
      selOutputCols.add(internalName);
      selColExprMap.put(internalName, desc);
    }
    SelectDesc select = new SelectDesc(selCols, selOutputCols);

    Operator<? extends OperatorDesc> reduceOperator = OperatorUtils.createOperator(select,
        new RowSchema(selectRR.getColumnInfos()), interim);
    finalOperatorMap.put(reduceOperator, new OpParseContext(selectRR));
    reduceOperator.setColumnExprMap(selColExprMap);
    return new Result(reduceOperator, finalOperatorMap);
  }

  public static class Result {
    private final Operator<? extends OperatorDesc> reduceOperator;
    private final Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap;

    public Result(Operator<? extends OperatorDesc> reduceOperator,
        Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap) {
      this.reduceOperator = reduceOperator;
      this.finalOperatorMap = finalOperatorMap;
    }

    public Operator getOperator() {
      return reduceOperator;
    }

    public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
      return finalOperatorMap;
    }
  }
}
