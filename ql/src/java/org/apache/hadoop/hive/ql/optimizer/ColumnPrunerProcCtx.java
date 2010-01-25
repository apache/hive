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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 * This class implements the processor context for Column Pruner.
 */
public class ColumnPrunerProcCtx implements NodeProcessorCtx {

  private final Map<Operator<? extends Serializable>, List<String>> prunedColLists;

  private final HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;

  private final Map<CommonJoinOperator, Map<Byte, List<String>>> joinPrunedColLists;

  public ColumnPrunerProcCtx(
      HashMap<Operator<? extends Serializable>, OpParseContext> opToParseContextMap) {
    prunedColLists = new HashMap<Operator<? extends Serializable>, List<String>>();
    opToParseCtxMap = opToParseContextMap;
    joinPrunedColLists = new HashMap<CommonJoinOperator, Map<Byte, List<String>>>();
  }

  public Map<CommonJoinOperator, Map<Byte, List<String>>> getJoinPrunedColLists() {
    return joinPrunedColLists;
  }

  /**
   * @return the prunedColLists
   */
  public List<String> getPrunedColList(Operator<? extends Serializable> op) {
    return prunedColLists.get(op);
  }

  public HashMap<Operator<? extends Serializable>, OpParseContext> getOpToParseCtxMap() {
    return opToParseCtxMap;
  }

  public Map<Operator<? extends Serializable>, List<String>> getPrunedColLists() {
    return prunedColLists;
  }

  /**
   * Creates the list of internal column names(these names are used in the
   * RowResolver and are different from the external column names) that are
   * needed in the subtree. These columns eventually have to be selected from
   * the table scan.
   * 
   * @param curOp
   *          The root of the operator subtree.
   * @return List<String> of the internal column names.
   * @throws SemanticException
   */
  public List<String> genColLists(Operator<? extends Serializable> curOp)
      throws SemanticException {
    List<String> colList = new ArrayList<String>();
    if (curOp.getChildOperators() != null) {
      for (Operator<? extends Serializable> child : curOp.getChildOperators()) {
        if (child instanceof CommonJoinOperator) {
          int tag = child.getParentOperators().indexOf(curOp);
          List<String> prunList = joinPrunedColLists.get(child).get((byte) tag);
          colList = Utilities.mergeUniqElems(colList, prunList);
        } else {
          colList = Utilities
              .mergeUniqElems(colList, prunedColLists.get(child));
        }
      }
    }
    return colList;
  }

  /**
   * Creates the list of internal column names from select expressions in a
   * select operator. This function is used for the select operator instead of
   * the genColLists function (which is used by the rest of the operators).
   * 
   * @param op
   *          The select operator.
   * @return List<String> of the internal column names.
   */
  public List<String> getColsFromSelectExpr(SelectOperator op) {
    List<String> cols = new ArrayList<String>();
    SelectDesc conf = op.getConf();
    ArrayList<ExprNodeDesc> exprList = conf.getColList();
    for (ExprNodeDesc expr : exprList) {
      cols = Utilities.mergeUniqElems(cols, expr.getCols());
    }
    return cols;
  }

  /**
   * Creates the list of internal column names for select * expressions.
   * 
   * @param op
   *          The select operator.
   * @param colList
   *          The list of internal column names returned by the children of the
   *          select operator.
   * @return List<String> of the internal column names.
   */
  public List<String> getSelectColsFromChildren(SelectOperator op,
      List<String> colList) {
    List<String> cols = new ArrayList<String>();
    SelectDesc conf = op.getConf();

    if (conf.isSelStarNoCompute()) {
      cols.addAll(colList);
      return cols;
    }

    ArrayList<ExprNodeDesc> selectExprs = conf.getColList();

    // The colList is the output columns used by child operators, they are
    // different
    // from input columns of the current operator. we need to find out which
    // input columns are used.
    ArrayList<String> outputColumnNames = conf.getOutputColumnNames();
    for (int i = 0; i < outputColumnNames.size(); i++) {
      if (colList.contains(outputColumnNames.get(i))) {
        ExprNodeDesc expr = selectExprs.get(i);
        cols = Utilities.mergeUniqElems(cols, expr.getCols());
      }
    }

    return cols;
  }
}
