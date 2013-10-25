/**
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 * TableAccessAnalyzer walks the operator graph from joins and group bys
 * to the table scan operator backing it. It checks whether the operators
 * in the path are pass-through of the base table (no aggregations/joins),
 * and if the keys are mapped by expressions that do not modify the bucket
 * for the key. If all the keys for a join/group by are clean pass-through
 * of the base table columns, we can consider this operator as a candidate
 * for improvement through bucketing.
 */
public class TableAccessAnalyzer {
  private static final Log LOG = LogFactory.getLog(TableAccessAnalyzer.class.getName());
  private final ParseContext pGraphContext;

  public TableAccessAnalyzer() {
    pGraphContext = null;
  }

  public TableAccessAnalyzer(ParseContext pactx) {
    pGraphContext = pactx;
  }

  public TableAccessInfo analyzeTableAccess() throws SemanticException {

    // Set up the rules for the graph walker for group by and join operators
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%"),
        new GroupByProcessor(pGraphContext));
    opRules.put(new RuleRegExp("R2", JoinOperator.getOperatorName() + "%"),
        new JoinProcessor(pGraphContext));
    opRules.put(new RuleRegExp("R3", MapJoinOperator.getOperatorName() + "%"),
        new JoinProcessor(pGraphContext));

    TableAccessCtx tableAccessCtx = new TableAccessCtx();
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, tableAccessCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes and walk!
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return tableAccessCtx.getTableAccessInfo();
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  /**
   * Processor for GroupBy operator
   */
  public class GroupByProcessor implements NodeProcessor {
    protected ParseContext pGraphContext;

    public GroupByProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) {
      GroupByOperator op = (GroupByOperator)nd;
      TableAccessCtx tableAccessCtx = (TableAccessCtx)procCtx;

      // Get the key column names, and check if the keys are all constants
      // or columns (not expressions). If yes, proceed.
      List<String> keyColNames =
          TableAccessAnalyzer.getKeyColNames(op.getConf().getKeys());

      if (keyColNames == null) {
        // we are done, since there are no keys to check for
        return null;
      }

      // Walk the operator tree to the TableScan and build the mapping
      // along the way for the columns that the group by uses as keys
      TableScanOperator tso = TableAccessAnalyzer.genRootTableScan(
          op.getParentOperators().get(0), keyColNames);

      if (tso == null) {
        // Could not find an allowed path to a table scan operator,
        // hence we are done
        return null;
      }

      Map<String, List<String>> tableToKeysMap = new HashMap<String, List<String>>();
      Table tbl = pGraphContext.getTopToTable().get(tso);
      tableToKeysMap.put(tbl.getCompleteName(), keyColNames);
      tableAccessCtx.addOperatorTableAccess(op, tableToKeysMap);

      return null;
    }
  }

  /**
   * Processor for Join operator.
   */
  public class JoinProcessor implements NodeProcessor {
    protected ParseContext pGraphContext;

    public JoinProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) {
      JoinOperator op = (JoinOperator)nd;
      TableAccessCtx tableAccessCtx = (TableAccessCtx)procCtx;
      Map<String, List<String>> tableToKeysMap = new HashMap<String, List<String>>();

      List<Operator<? extends OperatorDesc>> parentOps = op.getParentOperators();

      // Get the key column names for each side of the join,
      // and check if the keys are all constants
      // or columns (not expressions). If yes, proceed.
      QBJoinTree joinTree = pGraphContext.getJoinContext().get(op);
      assert(parentOps.size() == joinTree.getBaseSrc().length);
      int pos = 0;
      for (String src : joinTree.getBaseSrc()) {
        if (src != null) {
          assert(parentOps.get(pos) instanceof ReduceSinkOperator);
          ReduceSinkOperator reduceSinkOp = (ReduceSinkOperator) parentOps.get(pos);

          // Get the key column names, and check if the keys are all constants
          // or columns (not expressions). If yes, proceed.
          List<String> keyColNames =
              TableAccessAnalyzer.getKeyColNames(reduceSinkOp.getConf().getKeyCols());

          if (keyColNames == null) {
            // we are done, since there are no keys to check for
            return null;
          }

          // Walk the operator tree to the TableScan and build the mapping
          // along the way for the columns that the group by uses as keys
          TableScanOperator tso = TableAccessAnalyzer.genRootTableScan(
              reduceSinkOp.getParentOperators().get(0), keyColNames);

          if (tso == null) {
            // Could not find an allowed path to a table scan operator,
            // hence we are done
            return null;
          }

          Table tbl = pGraphContext.getTopToTable().get(tso);
          tableToKeysMap.put(tbl.getCompleteName(), keyColNames);
        } else {
          return null;
        }
        pos++;
      }

      // We only get here if we could map all join keys to source table columns
      tableAccessCtx.addOperatorTableAccess(op, tableToKeysMap);
      return null;
    }
  }

  /**
   * This method traces up from the given operator to the root
   * of the operator graph until a TableScanOperator is reached.
   * Along the way, if any operators are present that do not
   * provide a direct mapping from columns of the base table to
   * the keys on the input operator, the trace-back is stopped at that
   * point. If the trace back can be done successfully, the method
   * returns the root TableScanOperator as well as the list of column
   * names on that table that map to the keys used for the input
   * operator (which is currently only a join or group by).
   */
  public static TableScanOperator genRootTableScan(
      Operator<? extends OperatorDesc> op, List<String> keyNames) {

    Operator<? extends OperatorDesc> currOp = op;
    List<String> currColNames = keyNames;
    List<Operator<? extends OperatorDesc>> parentOps = null;

    // Track as you walk up the tree if there is an operator
    // along the way that changes the rows from the table through
    // joins or aggregations. Only allowed operators are selects
    // and filters.
    while (true) {
      parentOps = currOp.getParentOperators();
      if ((parentOps == null) || (parentOps.isEmpty())) {
        return (TableScanOperator) currOp;
      }

      if (parentOps.size() > 1 ||
          !(currOp.columnNamesRowResolvedCanBeObtained())) {
        return null;
      } else {
        // Generate the map of the input->output column name for the keys
        // we are about
        if (!TableAccessAnalyzer.genColNameMap(currOp, currColNames)) {
          return null;
        }
        currOp = parentOps.get(0);
      }
    }
  }

  /*
   * This method takes in an input operator and a subset of its output
   * column names, and generates the input column names for the operator
   * corresponding to those outputs. If the mapping from the input column
   * name to the output column name is not simple, the method returns
   * false, else it returns true. The list of output column names is
   * modified by this method to be the list of corresponding input column
   * names.
   */
  private static boolean genColNameMap(
      Operator<? extends OperatorDesc> op, List<String> currColNames) {

    List<ExprNodeDesc> colList = null;
    List<String> outputColNames = null;

    assert(op.columnNamesRowResolvedCanBeObtained());
    // Only select operators among the allowed operators can cause changes in the
    // column names
    if (op instanceof SelectOperator) {
      SelectDesc selectDesc = ((SelectOperator)op).getConf();
      if (!selectDesc.isSelStarNoCompute()) {
        colList = selectDesc.getColList();
        outputColNames = selectDesc.getOutputColumnNames();

        // Only columns and constants can be selected
        for (int pos = 0; pos < colList.size(); pos++) {
          ExprNodeDesc colExpr = colList.get(pos);
          String outputColName = outputColNames.get(pos);

          // If it is not a column we need for the keys, move on
          if (!currColNames.contains(outputColName)) {
            continue;
          }

          if ((colExpr instanceof ExprNodeConstantDesc) ||
            (colExpr instanceof ExprNodeNullDesc)) {
            currColNames.remove(outputColName);
            continue;
          } else if (colExpr instanceof ExprNodeColumnDesc) {
            String inputColName = ((ExprNodeColumnDesc) colExpr).getColumn();
            if (!outputColName.equals(inputColName)) {
              currColNames.set(currColNames.indexOf(outputColName), inputColName);
            }
          } else {
            // the column map can not be generated
            return false;
          }
        }
      }
    }

    return true;
  }

  private static List<String> getKeyColNames(List<ExprNodeDesc> keys) {
    List<String> colList = new ArrayList<String>();
    for (ExprNodeDesc expr: keys) {
      if (expr instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc colExpr = (ExprNodeColumnDesc)expr;
        colList.add(colExpr.getColumn());
      } else if (expr instanceof ExprNodeConstantDesc || expr instanceof ExprNodeNullDesc) {
        continue;
      } else {
        return null;
      }
    }
    return colList;
  }
}
