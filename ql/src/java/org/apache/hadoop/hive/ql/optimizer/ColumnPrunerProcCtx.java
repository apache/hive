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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static org.apache.hadoop.hive.ql.optimizer.FieldNode.mergeFieldNodes;

/**
 * This class implements the processor context for Column Pruner.
 */
public class ColumnPrunerProcCtx implements NodeProcessorCtx {
  private final ParseContext pctx;

  /**
   * A mapping from operators to nested column paths being used in them.
   * Note: paths are of format "s.a.b" which represents field "b" of
   *   struct "a" is being used, while "a" itself is a field of struct "s".
   */
  private final Map<Operator<? extends OperatorDesc>, List<FieldNode>> prunedColLists;
  private final Map<CommonJoinOperator, Map<Byte, List<FieldNode>>> joinPrunedColLists;

  public ColumnPrunerProcCtx(ParseContext pctx) {
    this.pctx = pctx;
    prunedColLists = new HashMap<>();
    joinPrunedColLists = new HashMap<>();
  }

  public ParseContext getParseContext() {
    return pctx;
  }

  public Map<CommonJoinOperator, Map<Byte, List<FieldNode>>> getJoinPrunedColLists() {
    return joinPrunedColLists;
  }

  public List<FieldNode> getPrunedColList(Operator<? extends OperatorDesc> op) {
    return prunedColLists.get(op);
  }

  public Map<Operator<? extends OperatorDesc>, List<FieldNode>> getPrunedColLists() {
    return prunedColLists;
  }

  /**
   * Creates the list of internal column names(represented by field nodes,
   * these names are used in the RowResolver and are different from the
   * external column names) that are needed in the subtree. These columns
   * eventually have to be selected from the table scan.
   *
   * @param curOp The root of the operator subtree.
   * @return a list of field nodes representing the internal column names.
   */
  public List<FieldNode> genColLists(Operator<? extends OperatorDesc> curOp)
      throws SemanticException {
    if (curOp.getChildOperators() == null) {
      return null;
    }
    List<FieldNode> colList = null;
    for (Operator<? extends OperatorDesc> child : curOp.getChildOperators()) {
      List<FieldNode> prunList = null;
      if (child instanceof CommonJoinOperator) {
        int tag = child.getParentOperators().indexOf(curOp);
        prunList = joinPrunedColLists.get(child).get((byte) tag);
      } else if (child instanceof FileSinkOperator) {
        prunList = new ArrayList<>();
        RowSchema oldRS = curOp.getSchema();
        for (ColumnInfo colInfo : oldRS.getSignature()) {
          prunList.add(new FieldNode(colInfo.getInternalName()));
        }
      } else {
        prunList = prunedColLists.get(child);
      }
      if (prunList == null) {
        continue;
      }
      if (colList == null) {
        colList = new ArrayList<>(prunList);
      } else {
        colList = mergeFieldNodes(colList, prunList);
      }
    }
    return colList;
  }

  /**
   * Creates the list of internal column names (represented by field nodes,
   * these names are used in the RowResolver and are different from the
   * external column names) that are needed in the subtree. These columns
   * eventually have to be selected from the table scan.
   *
   * @param curOp The root of the operator subtree.
   * @param child The consumer.
   * @return a list of field nodes representing the internal column names.
   */
  public List<FieldNode> genColLists(Operator<? extends OperatorDesc> curOp,
          Operator<? extends OperatorDesc> child)
      throws SemanticException {
    if (curOp.getChildOperators() == null) {
      return null;
    }
    if (child instanceof CommonJoinOperator) {
      int tag = child.getParentOperators().indexOf(curOp);
      return joinPrunedColLists.get(child).get((byte) tag);
    } else {
      return prunedColLists.get(child);
    }
  }

  /**
   * Creates the list of internal column names (represented by field nodes)
   * from select expressions in a select operator. This function is used for the
   * select operator instead of the genColLists function (which is used by
   * the rest of the operators).
   *
   * @param op The select operator.
   * @return a list of field nodes representing the internal column names.
   */
  public List<FieldNode> getColsFromSelectExpr(SelectOperator op) {
    List<FieldNode> cols = new ArrayList<>();
    SelectDesc conf = op.getConf();
    if(conf.isSelStarNoCompute()) {
      for (ColumnInfo colInfo : op.getSchema().getSignature()) {
        cols.add(new FieldNode(colInfo.getInternalName()));
      }
    } else {
      List<ExprNodeDesc> exprList = conf.getColList();
        for (ExprNodeDesc expr : exprList) {
          cols = mergeFieldNodesWithDesc(cols, expr);
        }
    }
    return cols;
  }

  /**
   * Creates the list of internal column names for select * expressions.
   *
   * @param op The select operator.
   * @param colList The list of internal column names (represented by field nodes)
   *                returned by the children of the select operator.
   * @return a list of field nodes representing the internal column names.
   */
  public List<FieldNode> getSelectColsFromChildren(SelectOperator op,
      List<FieldNode> colList) {
    List<FieldNode> cols = new ArrayList<>();
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
      if (colList == null) {
        cols = mergeFieldNodesWithDesc(cols, selectExprs.get(i));
      } else {
        FieldNode childFn = lookupColumn(colList, outputColumnNames.get(i));
        if (childFn != null) {
          // In SemanticAnalyzer we inject SEL op before aggregation. The columns
          // in this SEL are derived from the table schema, and do not reflect the
          // actual columns being selected in the current query.
          // In this case, we skip the merge and just use the path from the child ops.
          ExprNodeDesc desc = selectExprs.get(i);
          if (desc instanceof ExprNodeColumnDesc && ((ExprNodeColumnDesc) desc).getIsGenerated()) {
            FieldNode fn = new FieldNode(((ExprNodeColumnDesc) desc).getColumn());
            fn.setNodes(childFn.getNodes());
            cols = mergeFieldNodes(cols, fn);
          } else {
            cols = mergeFieldNodesWithDesc(cols, selectExprs.get(i));
          }
        }
      }
    }

    return cols;
  }

  /**
   * Given the 'desc', construct a list of field nodes representing the
   * nested columns paths referenced by this 'desc'.
   * @param desc the node descriptor
   * @return a list of nested column paths referenced in the 'desc'
   */
  private static List<FieldNode> getNestedColPathByDesc(ExprNodeDesc desc) {
    List<FieldNode> res = new ArrayList<>();
    getNestedColsFromExprNodeDesc(desc, null, res);
    return mergeFieldNodes(new ArrayList<FieldNode>(), res);
  }

  private static void getNestedColsFromExprNodeDesc(
    ExprNodeDesc desc,
    FieldNode pathToRoot,
    List<FieldNode> paths) {
    if (desc instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) desc;
      FieldNode p = new FieldNode(columnDesc.getColumn());
      checkListAndMap(columnDesc, pathToRoot, p);
      paths.add(p);
    } else if (desc instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc fieldDesc = (ExprNodeFieldDesc) desc;
      ExprNodeDesc childDesc = fieldDesc.getDesc();
      FieldNode p = new FieldNode(fieldDesc.getFieldName());
      checkListAndMap(fieldDesc, pathToRoot, p);
      getNestedColsFromExprNodeDesc(childDesc, p, paths);
    } else {
      List<ExprNodeDesc> children = desc.getChildren();
      if (children != null) {
        for (ExprNodeDesc c : children) {
          getNestedColsFromExprNodeDesc(c, pathToRoot, paths);
        }
      }
    }
  }

  private static void checkListAndMap(ExprNodeDesc desc, FieldNode pathToRoot, FieldNode fn) {
    TypeInfo ti = desc.getTypeInfo();

    // Check cases for arr[i].f and map[key].v
    // For these we should not generate paths like arr.f or map.v
    // Otherwise we would have a mismatch between type info and path
    if (ti.getCategory() != ObjectInspector.Category.LIST
        && ti.getCategory() != ObjectInspector.Category.MAP) {
      fn.addFieldNodes(pathToRoot);
    }
  }

  /**
   * Create the list of internal columns for select tag of LV
   */
  public List<FieldNode> getSelectColsFromLVJoin(RowSchema rs,
      List<FieldNode> colList) throws SemanticException {
    List<FieldNode> columns = new ArrayList<>();
    for (FieldNode col : colList) {
      if (rs.getColumnInfo(col.getFieldName()) != null) {
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
    List<FieldNode> parentPrunList = prunedColLists.get(curOp);
    if(parentPrunList == null || parentPrunList.size() == 0) {
      return;
    }
    List<FieldNode> prunList = null;

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
          if (lookupColumn(prunList, colInfo.getInternalName()) == null) {
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

  static ArrayList<String> toColumnNames(List<FieldNode> columns) {
    ArrayList<String> names = new ArrayList<>();
    for (FieldNode fn : columns) {
      names.add(fn.getFieldName());
    }
    return names;
  }

  static List<FieldNode> fromColumnNames(List<String> columnNames) {
    List<FieldNode> fieldNodes = new ArrayList<>();
    for (String cn : columnNames) {
      fieldNodes.add(new FieldNode(cn));
    }
    return fieldNodes;
  }

  static FieldNode lookupColumn(Collection<FieldNode> columns, String colName) {
    for (FieldNode fn : columns) {
      if (fn.getFieldName() != null && fn.getFieldName().equals(colName)) {
        return fn;
      }
    }
    return null;
  }

  static List<FieldNode> mergeFieldNodesWithDesc(List<FieldNode> left, ExprNodeDesc desc) {
    return FieldNode.mergeFieldNodes(left, getNestedColPathByDesc(desc));
  }
}
