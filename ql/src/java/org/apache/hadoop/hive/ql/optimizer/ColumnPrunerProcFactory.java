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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 * Factory for generating the different node processors used by ColumnPruner.
 */
public final class ColumnPrunerProcFactory {
  protected static final Log LOG = LogFactory.getLog(ColumnPrunerProcFactory.class.getName());
  private ColumnPrunerProcFactory() {
    // prevent instantiation
  }

  /**
   * Node Processor for Column Pruning on Filter Operators.
   */
  public static class ColumnPrunerFilterProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator op = (FilterOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      ExprNodeDesc condn = op.getConf().getPredicate();
      // get list of columns used in the filter
      List<String> cl = condn.getCols();
      // merge it with the downstream col list
      cppCtx.getPrunedColLists().put(op,
          Utilities.mergeUniqElems(cppCtx.genColLists(op), cl));

      pruneOperator(cppCtx, op, cppCtx.getPrunedColLists().get(op));

      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerFilterProc class.
   *
   * @return ColumnPrunerFilterProc
   */
  public static ColumnPrunerFilterProc getFilterProc() {
    return new ColumnPrunerFilterProc();
  }

  /**
   * Node Processor for Column Pruning on Group By Operators.
   */
  public static class ColumnPrunerGroupByProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator op = (GroupByOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> colLists = new ArrayList<String>();
      GroupByDesc conf = op.getConf();
      ArrayList<ExprNodeDesc> keys = conf.getKeys();
      for (ExprNodeDesc key : keys) {
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());
      }

      ArrayList<AggregationDesc> aggrs = conf.getAggregators();
      for (AggregationDesc aggr : aggrs) {
        ArrayList<ExprNodeDesc> params = aggr.getParameters();
        for (ExprNodeDesc param : params) {
          colLists = Utilities.mergeUniqElems(colLists, param.getCols());
        }
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerGroupByProc class.
   *
   * @return ColumnPrunerGroupByProc
   */
  public static ColumnPrunerGroupByProc getGroupByProc() {
    return new ColumnPrunerGroupByProc();
  }

  /**
   * The Default Node Processor for Column Pruning.
   */
  public static class ColumnPrunerDefaultProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      cppCtx.getPrunedColLists().put((Operator<? extends Serializable>) nd,
          cppCtx.genColLists((Operator<? extends Serializable>) nd));

      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerDefaultProc class.
   *
   * @return ColumnPrunerDefaultProc
   */
  public static ColumnPrunerDefaultProc getDefaultProc() {
    return new ColumnPrunerDefaultProc();
  }

  /**
   * The Node Processor for Column Pruning on Table Scan Operators. It will
   * store needed columns in tableScanDesc.
   */
  public static class ColumnPrunerTableScanProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOp = (TableScanOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = cppCtx
          .genColLists((Operator<? extends Serializable>) nd);
      cppCtx.getPrunedColLists().put((Operator<? extends Serializable>) nd,
          cols);
      ArrayList<Integer> needed_columns = new ArrayList<Integer>();
      RowResolver inputRR = cppCtx.getOpToParseCtxMap().get(scanOp).getRR();
      TableScanDesc desc = scanOp.getConf();
      List<VirtualColumn> virtualCols = desc.getVirtualCols();
      List<VirtualColumn> newVirtualCols = new ArrayList<VirtualColumn>();
      for (int i = 0; i < cols.size(); i++) {
        String[] tabCol = inputRR.reverseLookup(cols.get(i));
        if(tabCol == null) {
          continue;
        }
        ColumnInfo colInfo = inputRR.get(tabCol[0], tabCol[1]);
        if (colInfo.getIsVirtualCol()) {
          // part is also a virtual column, but part col should not in this
          // list.
          for (int j = 0; j < virtualCols.size(); j++) {
            VirtualColumn vc = virtualCols.get(j);
            if (vc.getName().equals(colInfo.getInternalName())) {
              newVirtualCols.add(vc);
            }
          }
          //no need to pass virtual columns to reader.
          continue;
        }
        int position = inputRR.getPosition(cols.get(i));
        if (position >=0) {
          needed_columns.add(position);
        }
      }

      desc.setVirtualCols(newVirtualCols);
      scanOp.setNeededColumnIDs(needed_columns);
      return null;
    }
  }

  /**
   * Factory method to get the ColumnPrunerDefaultProc class.
   *
   * @return ColumnPrunerTableScanProc
   */
  public static ColumnPrunerTableScanProc getTableScanProc() {
    return new ColumnPrunerTableScanProc();
  }

  /**
   * The Node Processor for Column Pruning on Reduce Sink Operators.
   */
  public static class ColumnPrunerReduceSinkProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap = cppCtx
          .getOpToParseCtxMap();
      RowResolver redSinkRR = opToParseCtxMap.get(op).getRR();
      ReduceSinkDesc conf = op.getConf();
      List<Operator<? extends Serializable>> childOperators = op
          .getChildOperators();
      List<Operator<? extends Serializable>> parentOperators = op
          .getParentOperators();

      List<String> colLists = new ArrayList<String>();
      ArrayList<ExprNodeDesc> keys = conf.getKeyCols();
      for (ExprNodeDesc key : keys) {
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());
      }

      if ((childOperators.size() == 1)
          && (childOperators.get(0) instanceof JoinOperator)) {
        assert parentOperators.size() == 1;
        Operator<? extends Serializable> par = parentOperators.get(0);
        JoinOperator childJoin = (JoinOperator) childOperators.get(0);
        RowResolver parRR = opToParseCtxMap.get(par).getRR();
        List<String> childJoinCols = cppCtx.getJoinPrunedColLists().get(
            childJoin).get((byte) conf.getTag());
        boolean[] flags = new boolean[conf.getValueCols().size()];
        for (int i = 0; i < flags.length; i++) {
          flags[i] = false;
        }
        if (childJoinCols != null && childJoinCols.size() > 0) {
          Map<String, ExprNodeDesc> exprMap = op.getColumnExprMap();
          for (String childCol : childJoinCols) {
            ExprNodeDesc desc = exprMap.get(childCol);
            int index = conf.getValueCols().indexOf(desc);
            flags[index] = true;
            String[] nm = redSinkRR.reverseLookup(childCol);
            if (nm != null) {
              ColumnInfo cInfo = parRR.get(nm[0], nm[1]);
              if (!colLists.contains(cInfo.getInternalName())) {
                colLists.add(cInfo.getInternalName());
              }
            }
          }
        }
        Collections.sort(colLists);
        pruneReduceSinkOperator(flags, op, cppCtx);
      } else {
        // Reduce Sink contains the columns needed - no need to aggregate from
        // children
        ArrayList<ExprNodeDesc> vals = conf.getValueCols();
        for (ExprNodeDesc val : vals) {
          colLists = Utilities.mergeUniqElems(colLists, val.getCols());
        }
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  /**
   * The Factory method to get ColumnPrunerReduceSinkProc class.
   *
   * @return ColumnPrunerReduceSinkProc
   */
  public static ColumnPrunerReduceSinkProc getReduceSinkProc() {
    return new ColumnPrunerReduceSinkProc();
  }

  /**
   * The Node Processor for Column Pruning on Lateral View Join Operators.
   */
  public static class ColumnPrunerLateralViewJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LateralViewJoinOperator op = (LateralViewJoinOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = new ArrayList<String>();

      cols = cppCtx.genColLists(op);
      Map<String, ExprNodeDesc> colExprMap = op.getColumnExprMap();

      // As columns go down the DAG, the LVJ will transform internal column
      // names from something like 'key' to '_col0'. Because of this, we need
      // to undo this transformation using the column expression map as the
      // column names propagate up the DAG.
      List<String> colsAfterReplacement = new ArrayList<String>();
      for (String col : cols) {
        if (colExprMap.containsKey(col)) {
          ExprNodeDesc expr = colExprMap.get(col);
          colsAfterReplacement.addAll(expr.getCols());
        } else {
          colsAfterReplacement.add(col);
        }
      }

      cppCtx.getPrunedColLists().put(op,
          colsAfterReplacement);
      return null;
    }
  }

  /**
   * The Node Processor for Column Pruning on Select Operators.
   */
  public static class ColumnPrunerSelectProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator op = (SelectOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = new ArrayList<String>();

      if (op.getChildOperators() != null) {
        for (Operator<? extends Serializable> child : op.getChildOperators()) {
          // If one of my children is a FileSink or Script, return all columns.
          // Without this break, a bug in ReduceSink to Extract edge column
          // pruning will manifest
          // which should be fixed before remove this
          if ((child instanceof FileSinkOperator)
              || (child instanceof ScriptOperator)
              || (child instanceof UDTFOperator)
              || (child instanceof LimitOperator)
              || (child instanceof UnionOperator)) {
            cppCtx.getPrunedColLists()
                .put(op, cppCtx.getColsFromSelectExpr(op));
            return null;
          }
        }
      }
      cols = cppCtx.genColLists(op);

      SelectDesc conf = op.getConf();
      // The input to the select does not matter. Go over the expressions
      // and return the ones which have a marked column
      cppCtx.getPrunedColLists().put(op,
          cppCtx.getSelectColsFromChildren(op, cols));

      if (conf.isSelStarNoCompute()) {
        return null;
      }

      // do we need to prune the select operator?
      List<ExprNodeDesc> originalColList = op.getConf().getColList();
      List<String> columns = new ArrayList<String>();
      for (ExprNodeDesc expr : originalColList) {
        Utilities.mergeUniqElems(columns, expr.getCols());
      }
      // by now, 'prunedCols' are columns used by child operators, and 'columns'
      // are columns used by this select operator.
      ArrayList<String> originalOutputColumnNames = conf.getOutputColumnNames();
      if (cols.size() < originalOutputColumnNames.size()) {
        ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
        ArrayList<String> newOutputColumnNames = new ArrayList<String>();
        ArrayList<ColumnInfo> rs_oldsignature = op.getSchema().getSignature();
        ArrayList<ColumnInfo> rs_newsignature = new ArrayList<ColumnInfo>();
        RowResolver old_rr = cppCtx.getOpToParseCtxMap().get(op).getRR();
        RowResolver new_rr = new RowResolver();
        for (String col : cols) {
          int index = originalOutputColumnNames.indexOf(col);
          newOutputColumnNames.add(col);
          newColList.add(originalColList.get(index));
          rs_newsignature.add(rs_oldsignature.get(index));
          String[] tabcol = old_rr.reverseLookup(col);
          ColumnInfo columnInfo = old_rr.get(tabcol[0], tabcol[1]);
          new_rr.put(tabcol[0], tabcol[1], columnInfo);
        }
        cppCtx.getOpToParseCtxMap().get(op).setRR(new_rr);
        op.getSchema().setSignature(rs_newsignature);
        conf.setColList(newColList);
        conf.setOutputColumnNames(newOutputColumnNames);
        handleChildren(op, cols, cppCtx);
      }
      return null;
    }

    /**
     * since we pruned the select operator, we should let its children operator
     * know that. ReduceSinkOperator may send out every output columns of its
     * parent select. When the select operator is pruned, its child reduce
     * sink(direct child) operator should also be pruned.
     *
     * @param op
     * @param retainedSelOutputCols
     * @throws SemanticException
     */
    private void handleChildren(SelectOperator op,
        List<String> retainedSelOutputCols, ColumnPrunerProcCtx cppCtx) throws SemanticException {
      for (Operator<? extends Serializable> child : op.getChildOperators()) {
        if (child instanceof ReduceSinkOperator) {
          boolean[] flags = getPruneReduceSinkOpRetainFlags(
              retainedSelOutputCols, (ReduceSinkOperator) child);
          pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
        } else if (child instanceof FilterOperator) {
          // filter operator has the same output columns as its parent
          for (Operator<? extends Serializable> filterChild : child
              .getChildOperators()) {
            if (filterChild instanceof ReduceSinkOperator) {
              boolean[] flags = getPruneReduceSinkOpRetainFlags(
                  retainedSelOutputCols, (ReduceSinkOperator) filterChild);
              pruneReduceSinkOperator(flags, (ReduceSinkOperator) filterChild,
                  cppCtx);
            }
          }
        }
      }
    }
  }

  private static boolean[] getPruneReduceSinkOpRetainFlags(
      List<String> retainedParentOpOutputCols, ReduceSinkOperator reduce) {
    ReduceSinkDesc reduceConf = reduce.getConf();
    java.util.ArrayList<ExprNodeDesc> originalValueEval = reduceConf
        .getValueCols();
    boolean[] flags = new boolean[originalValueEval.size()];
    for (int i = 0; i < originalValueEval.size(); i++) {
      flags[i] = false;
      List<String> current = originalValueEval.get(i).getCols();
      if (current == null || current.size() == 0) {
        flags[i] = true;
      } else {
        for (int j = 0; j < current.size(); j++) {
          if (retainedParentOpOutputCols.contains(current.get(j))) {
            flags[i] = true;
            break;
          }
        }
      }
    }
    return flags;
  }

  private static void pruneReduceSinkOperator(boolean[] retainFlags,
      ReduceSinkOperator reduce, ColumnPrunerProcCtx cppCtx) throws SemanticException {
    ReduceSinkDesc reduceConf = reduce.getConf();
    Map<String, ExprNodeDesc> oldMap = reduce.getColumnExprMap();
    Map<String, ExprNodeDesc> newMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ColumnInfo> sig = new ArrayList<ColumnInfo>();
    RowResolver oldRR = cppCtx.getOpToParseCtxMap().get(reduce).getRR();
    RowResolver newRR = new RowResolver();
    ArrayList<String> originalValueOutputColNames = reduceConf
        .getOutputValueColumnNames();
    java.util.ArrayList<ExprNodeDesc> originalValueEval = reduceConf
        .getValueCols();
    ArrayList<String> newOutputColNames = new ArrayList<String>();
    java.util.ArrayList<ExprNodeDesc> newValueEval = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < retainFlags.length; i++) {
      if (retainFlags[i]) {
        newValueEval.add(originalValueEval.get(i));
        String outputCol = originalValueOutputColNames.get(i);
        newOutputColNames.add(outputCol);
        String[] nm = oldRR.reverseLookup(outputCol);
        if (nm == null) {
          outputCol = Utilities.ReduceField.VALUE.toString() + "." + outputCol;
          nm = oldRR.reverseLookup(outputCol);
        }
        newMap.put(outputCol, oldMap.get(outputCol));
        ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
        newRR.put(nm[0], nm[1], colInfo);
        sig.add(colInfo);
      }
    }

    ArrayList<ExprNodeDesc> keyCols = reduceConf.getKeyCols();
    List<String> keys = new ArrayList<String>();
    RowResolver parResover = cppCtx.getOpToParseCtxMap().get(
        reduce.getParentOperators().get(0)).getRR();
    for (int i = 0; i < keyCols.size(); i++) {
      keys = Utilities.mergeUniqElems(keys, keyCols.get(i).getCols());
    }
    for (int i = 0; i < keys.size(); i++) {
      String outputCol = keys.get(i);
      String[] nm = parResover.reverseLookup(outputCol);
      ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
      if (colInfo != null) {
        newRR.put(nm[0], nm[1], colInfo);
      }
    }

    cppCtx.getOpToParseCtxMap().get(reduce).setRR(newRR);
    reduce.setColumnExprMap(newMap);
    reduce.getSchema().setSignature(sig);
    reduceConf.setOutputValueColumnNames(newOutputColNames);
    reduceConf.setValueCols(newValueEval);
    TableDesc newValueTable = PlanUtils.getReduceValueTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(reduceConf.getValueCols(),
        newOutputColNames, 0, ""));
    reduceConf.setValueSerializeInfo(newValueTable);
  }

  /**
   * The Factory method to get the ColumnPrunerSelectProc class.
   *
   * @return ColumnPrunerSelectProc
   */
  public static ColumnPrunerSelectProc getSelectProc() {
    return new ColumnPrunerSelectProc();
  }

  public static ColumnPrunerLateralViewJoinProc getLateralViewJoinProc() {
    return new ColumnPrunerLateralViewJoinProc();
  }

  /**
   * The Node Processor for Column Pruning on Join Operators.
   */
  public static class ColumnPrunerJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator op = (JoinOperator) nd;
      pruneJoinOperator(ctx, op, op.getConf(), op.getColumnExprMap(), null,
          false);
      return null;
    }
  }

  /**
   * The Factory method to get ColumnJoinProc class.
   *
   * @return ColumnPrunerJoinProc
   */
  public static ColumnPrunerJoinProc getJoinProc() {
    return new ColumnPrunerJoinProc();
  }

  /**
   * The Node Processor for Column Pruning on Map Join Operators.
   */
  public static class ColumnPrunerMapJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator op = (MapJoinOperator) nd;
      pruneJoinOperator(ctx, op, op.getConf(), op.getColumnExprMap(), op
          .getConf().getRetainList(), true);
      return null;
    }
  }

  private static void pruneOperator(NodeProcessorCtx ctx,
      Operator<? extends Serializable> op,
      List<String> cols)
      throws SemanticException {
    // the pruning needs to preserve the order of columns in the input schema
    RowSchema inputSchema = op.getSchema();
    if (inputSchema != null) {
      ArrayList<ColumnInfo> rs = new ArrayList<ColumnInfo>();
      ArrayList<ColumnInfo> inputCols = inputSchema.getSignature();
    	for (ColumnInfo i: inputCols) {
        if (cols.contains(i.getInternalName())) {
          rs.add(i);
        }
    	}
      op.getSchema().setSignature(rs);
    }
  }

  private static void pruneJoinOperator(NodeProcessorCtx ctx,
      CommonJoinOperator op, JoinDesc conf,
      Map<String, ExprNodeDesc> columnExprMap,
      Map<Byte, List<Integer>> retainMap, boolean mapJoin) throws SemanticException {
    ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
    Map<Byte, List<String>> prunedColLists = new HashMap<Byte, List<String>>();
    List<Operator<? extends Serializable>> childOperators = op
        .getChildOperators();

    for (Operator<? extends Serializable> child : childOperators) {
      if (child instanceof FileSinkOperator) {
        return;
      }
    }

    List<String> childColLists = cppCtx.genColLists(op);

    //add the columns in join filters
    Set<Map.Entry<Byte, List<ExprNodeDesc>>> filters =
      conf.getFilters().entrySet();
    Iterator<Map.Entry<Byte, List<ExprNodeDesc>>> iter = filters.iterator();
    while (iter.hasNext()) {
      Map.Entry<Byte, List<ExprNodeDesc>> entry = iter.next();
      Byte tag = entry.getKey();
      for (ExprNodeDesc desc : entry.getValue()) {
        List<String> cols = prunedColLists.get(tag);
        cols = Utilities.mergeUniqElems(cols, desc.getCols());
        prunedColLists.put(tag, cols);
     }
    }

    RowResolver joinRR = cppCtx.getOpToParseCtxMap().get(op).getRR();
    RowResolver newJoinRR = new RowResolver();
    ArrayList<String> outputCols = new ArrayList<String>();
    ArrayList<ColumnInfo> rs = new ArrayList<ColumnInfo>();
    Map<String, ExprNodeDesc> newColExprMap = new HashMap<String, ExprNodeDesc>();

    for (int i = 0; i < conf.getOutputColumnNames().size(); i++) {
      String internalName = conf.getOutputColumnNames().get(i);
      ExprNodeDesc desc = columnExprMap.get(internalName);
      Byte tag = conf.getReversedExprs().get(internalName);
      if (!childColLists.contains(internalName)) {
        int index = conf.getExprs().get(tag).indexOf(desc);
        if (index < 0) {
          continue;
        }
        conf.getExprs().get(tag).remove(desc);
        if (retainMap != null) {
          retainMap.get(tag).remove(index);
        }
      } else {
        List<String> prunedRSList = prunedColLists.get(tag);
        if (prunedRSList == null) {
          prunedRSList = new ArrayList<String>();
          prunedColLists.put(tag, prunedRSList);
        }
        prunedRSList = Utilities.mergeUniqElems(prunedRSList, desc.getCols());
        outputCols.add(internalName);
        newColExprMap.put(internalName, desc);
      }
    }

    if (mapJoin) {
      // regenerate the valueTableDesc
      List<TableDesc> valueTableDescs = new ArrayList<TableDesc>();
      for (int pos = 0; pos < op.getParentOperators().size(); pos++) {
        List<ExprNodeDesc> valueCols = conf.getExprs()
            .get(new Byte((byte) pos));
        StringBuilder keyOrder = new StringBuilder();
        for (int i = 0; i < valueCols.size(); i++) {
          keyOrder.append("+");
        }

        TableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(PlanUtils
            .getFieldSchemasFromColumnList(valueCols, "mapjoinvalue"));

        valueTableDescs.add(valueTableDesc);
      }
      ((MapJoinDesc) conf).setValueTblDescs(valueTableDescs);

      Set<Map.Entry<Byte, List<ExprNodeDesc>>> exprs = ((MapJoinDesc) conf)
          .getKeys().entrySet();
      Iterator<Map.Entry<Byte, List<ExprNodeDesc>>> iters = exprs.iterator();
      while (iters.hasNext()) {
        Map.Entry<Byte, List<ExprNodeDesc>> entry = iters.next();
        List<ExprNodeDesc> lists = entry.getValue();
        for (int j = 0; j < lists.size(); j++) {
          ExprNodeDesc desc = lists.get(j);
          Byte tag = entry.getKey();
          List<String> cols = prunedColLists.get(tag);
          cols = Utilities.mergeUniqElems(cols, desc.getCols());
          prunedColLists.put(tag, cols);
        }
      }

    }

    for (Operator<? extends Serializable> child : childOperators) {
      if (child instanceof ReduceSinkOperator) {
        boolean[] flags = getPruneReduceSinkOpRetainFlags(childColLists,
            (ReduceSinkOperator) child);
        pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
      }
    }

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      String[] nm = joinRR.reverseLookup(internalName);
      ColumnInfo col = joinRR.get(nm[0], nm[1]);
      newJoinRR.put(nm[0], nm[1], col);
      rs.add(col);
    }

    op.setColumnExprMap(newColExprMap);
    conf.setOutputColumnNames(outputCols);
    op.getSchema().setSignature(rs);
    cppCtx.getOpToParseCtxMap().get(op).setRR(newJoinRR);
    cppCtx.getJoinPrunedColLists().put(op, prunedColLists);
  }

  /**
   * The Factory method to get ColumnMapJoinProc class.
   *
   * @return ColumnPrunerMapJoinProc
   */
  public static ColumnPrunerMapJoinProc getMapJoinProc() {
    return new ColumnPrunerMapJoinProc();
  }

}
