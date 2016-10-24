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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * This class implements the processor context for Column Pruner.
 */
public class ColumnPrunerProcCtx implements NodeProcessorCtx {

  private final ParseContext pctx;

  private final Map<Operator<? extends OperatorDesc>, List<String>> prunedColLists;

  /**
   * This map stores the pruned nested column path for each operator
   */
  private final Map<Operator<? extends OperatorDesc>, List<String>> prunedNestedColLists;

  private final Map<CommonJoinOperator, Map<Byte, List<String>>> joinPrunedColLists;

  private final Map<UnionOperator, List<Integer>> unionPrunedColLists;

  public ColumnPrunerProcCtx(ParseContext pctx) {
    this.pctx = pctx;
    prunedColLists = new HashMap<Operator<? extends OperatorDesc>, List<String>>();
    prunedNestedColLists = new HashMap<Operator<? extends OperatorDesc>, List<String>>();
    joinPrunedColLists = new HashMap<CommonJoinOperator, Map<Byte, List<String>>>();
    unionPrunedColLists = new HashMap<>();
  }

  public ParseContext getParseContext() {
    return pctx;
  }

  public Map<CommonJoinOperator, Map<Byte, List<String>>> getJoinPrunedColLists() {
    return joinPrunedColLists;
  }

  public Map<UnionOperator, List<Integer>> getUnionPrunedColLists() {
    return unionPrunedColLists;
  }

  /**
   * @return the prunedColLists
   */
  public List<String> getPrunedColList(Operator<? extends OperatorDesc> op) {
    return prunedColLists.get(op);
  }

  public Map<Operator<? extends OperatorDesc>, List<String>> getPrunedColLists() {
    return prunedColLists;
  }

  public Map<Operator<? extends OperatorDesc>, List<String>> getPrunedNestedColLists() {
    return prunedNestedColLists;
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
  public List<String> genColLists(Operator<? extends OperatorDesc> curOp)
      throws SemanticException {
    if (curOp.getChildOperators() == null) {
      return null;
    }
    List<String> colList = null;
    for (Operator<? extends OperatorDesc> child : curOp.getChildOperators()) {
      List<String> prunList = null;
      if (child instanceof CommonJoinOperator) {
        int tag = child.getParentOperators().indexOf(curOp);
        prunList = joinPrunedColLists.get(child).get((byte) tag);
      } else if (child instanceof UnionOperator) {
        List<Integer> positions = unionPrunedColLists.get(child);
        if (positions != null) {
          prunList = new ArrayList<>();
          RowSchema oldRS = curOp.getSchema();
          for (Integer pos : positions) {
            ColumnInfo colInfo = oldRS.getSignature().get(pos);
            prunList.add(colInfo.getInternalName());
          }
        }
      } else if (child instanceof FileSinkOperator) {
        prunList = new ArrayList<>();
        RowSchema oldRS = curOp.getSchema();
        for (ColumnInfo colInfo : oldRS.getSignature()) {
          prunList.add(colInfo.getInternalName());
        }
      } else {
        prunList = prunedColLists.get(child);
      }
      if (prunList == null) {
        continue;
      }
      if (colList == null) {
        colList = new ArrayList<String>(prunList);
      } else {
        colList = Utilities.mergeUniqElems(colList, prunList);
      }
    }
    return colList;
  }

  /**
   * Get the path to the root column for the nested column attribute
   *
   * @param curOp current operator
   * @return the nested column paths for current operator and its child operator
   */
  public List<String> genNestedColPaths(Operator<? extends OperatorDesc> curOp) {
    if (curOp.getChildOperators() == null) {
      return null;
    }
    Set<String> groupPathsList = new HashSet<>();

    for (Operator<? extends OperatorDesc> child : curOp.getChildOperators()) {
      if (prunedNestedColLists.containsKey(child)) {
        groupPathsList.addAll(prunedNestedColLists.get(child));
      }
    }

    return new ArrayList<>(groupPathsList);
  }

  /**
   * Creates the list of internal column names(these names are used in the
   * RowResolver and are different from the external column names) that are
   * needed in the subtree. These columns eventually have to be selected from
   * the table scan.
   *
   * @param curOp
   *          The root of the operator subtree.
   * @param child
   *          The consumer.
   * @return List<String> of the internal column names.
   * @throws SemanticException
   */
  public List<String> genColLists(Operator<? extends OperatorDesc> curOp,
          Operator<? extends OperatorDesc> child)
      throws SemanticException {
    if (curOp.getChildOperators() == null) {
      return null;
    }
    if (child instanceof CommonJoinOperator) {
      int tag = child.getParentOperators().indexOf(curOp);
      return joinPrunedColLists.get(child).get((byte) tag);
    } else if (child instanceof UnionOperator) {
      List<Integer> positions = unionPrunedColLists.get(child);
      List<String> prunList = new ArrayList<>();
      if (positions != null && positions.size() > 0) {
        RowSchema oldRS = curOp.getSchema();
        for (Integer pos : positions) {
          ColumnInfo colInfo = oldRS.getSignature().get(pos);
          prunList.add(colInfo.getInternalName());
        }
      }
      return prunList;
    } else {
      return prunedColLists.get(child);
    }
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
    if(conf.isSelStarNoCompute()) {
      for (ColumnInfo colInfo : op.getSchema().getSignature()) {
        cols.add(colInfo.getInternalName());
      }
    }
    else {
      List<ExprNodeDesc> exprList = conf.getColList();
        for (ExprNodeDesc expr : exprList) {
          cols = Utilities.mergeUniqElems(cols, expr.getCols());
        }
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

    if (colList != null  && conf.isSelStarNoCompute()) {
      cols.addAll(colList);
      return cols;
    }

    List<ExprNodeDesc> selectExprs = conf.getColList();

    // The colList is the output columns used by child operators, they are
    // different
    // from input columns of the current operator. we need to find out which
    // input columns are used.
    List<String> outputColumnNames = conf.getOutputColumnNames();
    for (int i = 0; i < outputColumnNames.size(); i++) {
      if (colList == null || colList.contains(outputColumnNames.get(i))) {
        ExprNodeDesc expr = selectExprs.get(i);
        cols = Utilities.mergeUniqElems(cols, expr.getCols());
      }
    }

    return cols;
  }

  /**
   * Creates the list of internal group paths for select * expressions.
   *
   * @param op        The select operator.
   * @param paths The list of nested column paths returned by the children of the
   *                  select operator.
   * @return List<String> of the nested column path from leaf to the root.
   */
  public List<String> getSelectNestedColPathsFromChildren(
    SelectOperator op,
    List<String> paths) {
    List<String> groups = new ArrayList<>();
    SelectDesc conf = op.getConf();

    if (paths != null && conf.isSelStarNoCompute()) {
      groups.addAll(paths);
      return groups;
    }

    List<ExprNodeDesc> selectDescs = conf.getColList();

    List<String> outputColumnNames = conf.getOutputColumnNames();
    for (int i = 0; i < outputColumnNames.size(); i++) {
      if (paths == null || paths.contains(outputColumnNames.get(i))) {
        ExprNodeDesc desc = selectDescs.get(i);
        List<String> gp = getNestedColPathByDesc(desc);
        groups.addAll(gp);
      }
    }

    return groups;
  }

  // Entry method
  private List<String> getNestedColPathByDesc(ExprNodeDesc desc) {
    List<String> res = new ArrayList<>();
    getNestedColsFromExprNodeDesc(desc, "", res);
    return res;
  }

  private void getNestedColsFromExprNodeDesc(
    ExprNodeDesc desc,
    String pathToRoot,
    List<String> paths) {
    if (desc instanceof ExprNodeColumnDesc) {
      String f = ((ExprNodeColumnDesc) desc).getColumn();
      String p = pathToRoot.isEmpty() ? f : f + "." + pathToRoot;
      paths.add(p);
    } else if (desc instanceof ExprNodeFieldDesc) {
      String f = ((ExprNodeFieldDesc) desc).getFieldName();
      String p = pathToRoot.isEmpty() ? f : f + "." + pathToRoot;
      getNestedColsFromExprNodeDesc(((ExprNodeFieldDesc) desc).getDesc(), p, paths);
    } else {
      List<ExprNodeDesc> children = desc.getChildren();
      if (children == null || children.isEmpty()) {
        return;
      }
      for (ExprNodeDesc c : children) {
        getNestedColsFromExprNodeDesc(c, pathToRoot, paths);
      }
    }
  }

  /**
   * Create the list of internal columns for select tag of LV
   */
  public List<String> getSelectColsFromLVJoin(RowSchema rs,
      List<String> colList) throws SemanticException {
    List<String> columns = new ArrayList<String>();
    for (String col : colList) {
      if (rs.getColumnInfo(col) != null) {
        columns.add(col);
      }
    }
    return columns;
  }

  /**
   * If the input filter operator has direct child(ren) which are union operator,
   * and the filter's column is not the same as union's
   * create select operator between them. The select operator has same number of columns as
   * pruned child operator.
   *
   * @param curOp
   *          The filter operator which need to handle children.
   * @throws SemanticException
   */
  public void handleFilterUnionChildren(Operator<? extends OperatorDesc> curOp)
      throws SemanticException {
    if (curOp.getChildOperators() == null || !(curOp instanceof FilterOperator)) {
      return;
    }
    List<String> parentPrunList = prunedColLists.get(curOp);
    if(parentPrunList == null || parentPrunList.size() == 0) {
      return;
    }
    FilterOperator filOp = (FilterOperator)curOp;
    List<String> prunList = null;
    List<Integer>[] childToParentIndex = null;

    for (Operator<? extends OperatorDesc> child : curOp.getChildOperators()) {
      if (child instanceof UnionOperator) {
        prunList = genColLists(child);
        if (prunList == null || prunList.size() == 0 || parentPrunList.size() == prunList.size()) {
          continue;
        }

        ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputColNames = new ArrayList<String>();
        Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
        ArrayList<ColumnInfo> outputRS = new ArrayList<ColumnInfo>();
        for (ColumnInfo colInfo : child.getSchema().getSignature()) {
          if (!prunList.contains(colInfo.getInternalName())) {
            continue;
          }
          ExprNodeDesc colDesc = new ExprNodeColumnDesc(colInfo.getType(),
              colInfo.getInternalName(), colInfo.getTabAlias(), colInfo.getIsVirtualCol());
          exprs.add(colDesc);
          outputColNames.add(colInfo.getInternalName());
          ColumnInfo newCol = new ColumnInfo(colInfo.getInternalName(), colInfo.getType(),
                  colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
          newCol.setAlias(colInfo.getAlias());
          outputRS.add(newCol);
          colExprMap.put(colInfo.getInternalName(), colDesc);
        }
        SelectDesc select = new SelectDesc(exprs, outputColNames, false);
        curOp.removeChild(child);
        SelectOperator sel = (SelectOperator) OperatorFactory.getAndMakeChild(
            select, new RowSchema(outputRS), curOp);
        OperatorFactory.makeChild(sel, child);
        sel.setColumnExprMap(colExprMap);

      }

    }
  }

}
