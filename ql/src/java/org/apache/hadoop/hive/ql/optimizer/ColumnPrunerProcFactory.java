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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.ShapeDetails;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Factory for generating the different node processors used by ColumnPruner.
 */
public final class ColumnPrunerProcFactory {
  protected static final Logger LOG = LoggerFactory.getLogger(ColumnPrunerProcFactory.class.getName());
  private ColumnPrunerProcFactory() {
    // prevent instantiation
  }

  /**
   * Node Processor for Column Pruning on Filter Operators.
   */
  public static class ColumnPrunerFilterProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator op = (FilterOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      ExprNodeDesc condn = op.getConf().getPredicate();
      // get list of columns used in the filter
      List<String> cl = condn.getCols();
      // merge it with the downstream col list
      List<String> filterOpPrunedColLists = Utilities.mergeUniqElems(cppCtx.genColLists(op), cl);
      List<String> filterOpPrunedColListsOrderPreserved = preserveColumnOrder(op,
          filterOpPrunedColLists);
      cppCtx.getPrunedColLists().put(op,
          filterOpPrunedColListsOrderPreserved);

      pruneOperator(cppCtx, op, cppCtx.getPrunedColLists().get(op));
      cppCtx.handleFilterUnionChildren(op);
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
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbOp = (GroupByOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> colLists = new ArrayList<String>();
      GroupByDesc conf = gbOp.getConf();
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
      int groupingSetPosition = conf.getGroupingSetPosition();
      if (groupingSetPosition >= 0) {
        List<String> neededCols = cppCtx.genColLists(gbOp);
        String groupingColumn = conf.getOutputColumnNames().get(groupingSetPosition);
        if (!neededCols.contains(groupingColumn)) {
          conf.getOutputColumnNames().remove(groupingSetPosition);
          if (gbOp.getSchema() != null) {
            gbOp.getSchema().getSignature().remove(groupingSetPosition);
          }
        }
      }

      // If the child has a different schema, we create a Project operator between them both,
      // as we cannot prune the columns in the GroupBy operator
      for (Operator<?> child : gbOp.getChildOperators()) {
        if (child instanceof SelectOperator || child instanceof ReduceSinkOperator) {
          continue;
        }
        List<String> colList = cppCtx.genColLists(gbOp, child);
        Set<String> neededCols = new HashSet<String>();
        if (colList != null) {
          neededCols.addAll(colList);
        } else {
          // colList will be null for FS operators.
          continue;
        }
        if (neededCols.size() < gbOp.getSchema().getSignature().size()) {
          ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
          ArrayList<String> outputColNames = new ArrayList<String>();
          Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
          ArrayList<ColumnInfo> outputRS = new ArrayList<ColumnInfo>();
          for (ColumnInfo colInfo : gbOp.getSchema().getSignature()) {
            if (!neededCols.contains(colInfo.getInternalName())) {
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
          gbOp.removeChild(child);
          SelectOperator sel = (SelectOperator) OperatorFactory.getAndMakeChild(
              select, new RowSchema(outputRS), gbOp);
          OperatorFactory.makeChild(sel, child);
          sel.setColumnExprMap(colExprMap);
        }
      }

      cppCtx.getPrunedColLists().put(gbOp, colLists);
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

  public static class ColumnPrunerScriptProc implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {

      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RowSchema inputRS = op.getSchema();

      List<String> prunedCols = cppCtx.getPrunedColList(op.getChildOperators()
          .get(0));
      Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
      RowSchema parentRS = parent.getSchema();
      List<ColumnInfo> sig = parentRS.getSignature();
      List<String> colList = new ArrayList<String>();
      for (ColumnInfo cI : sig) {
        colList.add(cI.getInternalName());
      }

      if (prunedCols.size() != inputRS.getSignature().size()
          && !(op.getChildOperators().get(0) instanceof SelectOperator)) {
        ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputs = new ArrayList<String>();
        Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
        ArrayList<ColumnInfo> outputRS = new ArrayList<ColumnInfo>();
        for (String internalName : prunedCols) {
          ColumnInfo valueInfo = inputRS.getColumnInfo(internalName);
          ExprNodeDesc colDesc = new ExprNodeColumnDesc(valueInfo.getType(),
              valueInfo.getInternalName(), valueInfo.getTabAlias(), valueInfo.getIsVirtualCol());
          exprs.add(colDesc);
          outputs.add(internalName);
          ColumnInfo newCol = new ColumnInfo(internalName, valueInfo.getType(), valueInfo.getTabAlias(),
                  valueInfo.getIsVirtualCol(), valueInfo.isHiddenVirtualCol());
          newCol.setAlias(valueInfo.getAlias());
          outputRS.add(newCol);
          colExprMap.put(internalName, colDesc);
        }
        SelectDesc select = new SelectDesc(exprs, outputs, false);

        Operator<? extends OperatorDesc> child = op.getChildOperators().get(0);
        op.removeChild(child);
        SelectOperator sel = (SelectOperator) OperatorFactory.getAndMakeChild(
            select, new RowSchema(outputRS), op);
        OperatorFactory.makeChild(sel, child);

        sel.setColumnExprMap(colExprMap);
      }

      cppCtx.getPrunedColLists().put(op, colList);
      return null;
    }
  }

  public static class ColumnPrunerLimitProc extends ColumnPrunerDefaultProc {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, ctx, nodeOutputs);
      List<String> cols = ((ColumnPrunerProcCtx)ctx).getPrunedColLists().get(nd);
      if (null != cols) {
        pruneOperator(ctx, (LimitOperator) nd, cols);
      }
      return null;
    }
  }

  public static ColumnPrunerLimitProc getLimitProc() {
    return new ColumnPrunerLimitProc();
  }

  public static ColumnPrunerScriptProc getScriptProc() {
    return new ColumnPrunerScriptProc();
  }

  /**
   * - Pruning can only be done for Windowing. PTFs are black boxes,
   *   we assume all columns are needed.
   * - add column names referenced in WindowFn args and in WindowFn expressions
   *   to the pruned list of the child Select Op.
   * - finally we set the prunedColList on the ColumnPrunerContx;
   *   and update the RR & signature on the PTFOp.
   */
  public static class ColumnPrunerPTFProc extends ColumnPrunerScriptProc {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {

      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      PTFOperator op = (PTFOperator) nd;
      PTFDesc conf = op.getConf();
      //Since we cannot know what columns will be needed by a PTF chain,
      //we do not prune columns on PTFOperator for PTF chains.
      PartitionedTableFunctionDef funcDef = conf.getFuncDef();
      List<String> referencedColumns = funcDef.getReferencedColumns();
      if (!conf.forWindowing() && !conf.forNoop() && referencedColumns == null) {
        return super.process(nd, stack, cppCtx, nodeOutputs);
      }

      List<String> prunedCols = cppCtx.getPrunedColList(op.getChildOperators().get(0));
      if (conf.forWindowing()) {
        WindowTableFunctionDef def = (WindowTableFunctionDef) funcDef;
        prunedCols = Utilities.mergeUniqElems(getWindowFunctionColumns(def), prunedCols);
      } else if (conf.forNoop()) {
        prunedCols = new ArrayList(cppCtx.getPrunedColList(op.getChildOperators().get(0)));
      } else {
        prunedCols = referencedColumns;
      }

      List<ColumnInfo> newRS = prunedColumnsList(prunedCols, op.getSchema(), funcDef);

      op.getSchema().setSignature(new ArrayList<ColumnInfo>(newRS));

      ShapeDetails outputShape = funcDef.getStartOfChain().getInput().getOutputShape();
      cppCtx.getPrunedColLists().put(op, outputShape.getColumnNames());
      return null;
    }

    private List<ColumnInfo> buildPrunedRS(List<String> prunedCols, RowSchema oldRS)
        throws SemanticException {
      ArrayList<ColumnInfo> sig = new ArrayList<ColumnInfo>();
      HashSet<String> prunedColsSet = new HashSet<String>(prunedCols);
      for (ColumnInfo cInfo : oldRS.getSignature()) {
        if (prunedColsSet.contains(cInfo.getInternalName())) {
          sig.add(cInfo);
        }
      }
      return sig;
    }

    // always should be in this order (see PTFDeserializer#initializeWindowing)
    private List<String> getWindowFunctionColumns(WindowTableFunctionDef tDef) {
      List<String> columns = new ArrayList<String>();
      if (tDef.getWindowFunctions() != null) {
        for (WindowFunctionDef wDef : tDef.getWindowFunctions()) {
          columns.add(wDef.getAlias());
        }
      }
      return columns;
    }

    private RowResolver buildPrunedRR(List<String> prunedCols, RowSchema oldRS)
        throws SemanticException {
      RowResolver resolver = new RowResolver();
      HashSet<String> prunedColsSet = new HashSet<String>(prunedCols);
      for (ColumnInfo cInfo : oldRS.getSignature()) {
        if (prunedColsSet.contains(cInfo.getInternalName())) {
          resolver.put(cInfo.getTabAlias(), cInfo.getAlias(), cInfo);
        }
      }
      return resolver;
    }

    /*
     * add any input columns referenced in WindowFn args or expressions.
     */
    private List<ColumnInfo> prunedColumnsList(List<String> prunedCols, RowSchema oldRS,
        PartitionedTableFunctionDef pDef) throws SemanticException {
      pDef.getOutputShape().setRr(null);
      pDef.getOutputShape().setColumnNames(null);
      if (pDef instanceof WindowTableFunctionDef) {
        WindowTableFunctionDef tDef = (WindowTableFunctionDef) pDef;
        if (tDef.getWindowFunctions() != null) {
          for (WindowFunctionDef wDef : tDef.getWindowFunctions()) {
            if (wDef.getArgs() == null) {
              continue;
            }
            for (PTFExpressionDef arg : wDef.getArgs()) {
              ExprNodeDesc exprNode = arg.getExprNode();
              Utilities.mergeUniqElems(prunedCols, exprNode.getCols());
            }
          }
        }
        if (tDef.getPartition() != null) {
          for (PTFExpressionDef col : tDef.getPartition().getExpressions()) {
            ExprNodeDesc exprNode = col.getExprNode();
            Utilities.mergeUniqElems(prunedCols, exprNode.getCols());
          }
        }
        if (tDef.getOrder() != null) {
          for (PTFExpressionDef col : tDef.getOrder().getExpressions()) {
            ExprNodeDesc exprNode = col.getExprNode();
            Utilities.mergeUniqElems(prunedCols, exprNode.getCols());
          }
        }
      } else {
        pDef.getOutputShape().setRr(buildPrunedRR(prunedCols, oldRS));
      }

      PTFInputDef input = pDef.getInput();
      if (input instanceof PartitionedTableFunctionDef) {
        return prunedColumnsList(prunedCols, oldRS, (PartitionedTableFunctionDef)input);
      }

      ArrayList<String> inputColumns = prunedInputList(prunedCols, input);
      input.getOutputShape().setRr(buildPrunedRR(inputColumns, oldRS));
      input.getOutputShape().setColumnNames(inputColumns);

      return buildPrunedRS(prunedCols, oldRS);
    }

    /*
     * from the prunedCols list filter out columns that refer to WindowFns or WindowExprs
     * the returned list is set as the prunedList needed by the PTFOp.
     */
    private ArrayList<String> prunedInputList(List<String> prunedCols, PTFInputDef tDef) {
      ArrayList<String> prunedInputCols = new ArrayList<String>();

      StructObjectInspector OI = tDef.getOutputShape().getOI();
      for(StructField f : OI.getAllStructFieldRefs()) {
        String fName = f.getFieldName();
        if ( prunedCols.contains(fName)) {
          prunedInputCols.add(fName);
        }
      }

      return prunedInputCols;
    }
  }

  /**
   * Factory method to get the ColumnPrunerGroupByProc class.
   *
   * @return ColumnPrunerGroupByProc
   */
  public static ColumnPrunerPTFProc getPTFProc() {
    return new ColumnPrunerPTFProc();
  }

  /**
   * The Default Node Processor for Column Pruning.
   */
  public static class ColumnPrunerDefaultProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      cppCtx.getPrunedColLists().put((Operator<? extends OperatorDesc>) nd,
          cppCtx.genColLists((Operator<? extends OperatorDesc>) nd));

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
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOp = (TableScanOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = cppCtx
          .genColLists((Operator<? extends OperatorDesc>) nd);
      if (cols == null && !scanOp.getConf().isGatherStats() ) {
        scanOp.setNeededColumnIDs(null);
        return null;
      }

      cols = cols == null ? new ArrayList<String>() : cols;
      List nestedCols = cppCtx.genNestedColPaths((Operator<? extends OperatorDesc>) nd);

      cppCtx.getPrunedColLists().put((Operator<? extends OperatorDesc>) nd, cols);
      cppCtx.getPrunedNestedColLists().put((Operator<? extends OperatorDesc>) nd, nestedCols);
      RowSchema inputRS = scanOp.getSchema();
      setupNeededColumns(scanOp, inputRS, cols);

      scanOp.setNeededNestedColumnPaths(nestedCols);

      return null;
    }
  }

  /** Sets up needed columns for TSOP. Mainly, transfers column names from input
   * RowSchema as well as the needed virtual columns, into TableScanDesc.
   */
  public static void setupNeededColumns(TableScanOperator scanOp, RowSchema inputRS,
      List<String> cols) throws SemanticException {
    List<Integer> neededColumnIds = new ArrayList<Integer>();
    List<String> neededColumnNames = new ArrayList<String>();
    List<String> referencedColumnNames = new ArrayList<String>();
    TableScanDesc desc = scanOp.getConf();
    List<VirtualColumn> virtualCols = desc.getVirtualCols();
    List<VirtualColumn> newVirtualCols = new ArrayList<VirtualColumn>();

    // add virtual columns for ANALYZE TABLE
    if(scanOp.getConf().isGatherStats()) {
      cols.add(VirtualColumn.RAWDATASIZE.getName());
    }

    for (String column : cols) {
      ColumnInfo colInfo = inputRS.getColumnInfo(column);
      if (colInfo == null) {
        continue;
      }
      referencedColumnNames.add(column);
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
      int position = inputRS.getPosition(column);
      if (position >= 0) {
        // get the needed columns by id and name
        neededColumnIds.add(position);
        neededColumnNames.add(column);
      }
    }

    desc.setVirtualCols(newVirtualCols);
    scanOp.setNeededColumnIDs(neededColumnIds);
    scanOp.setNeededColumns(neededColumnNames);
    scanOp.setReferencedColumns(referencedColumnNames);
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
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      ReduceSinkDesc conf = op.getConf();

      List<String> colLists = new ArrayList<String>();
      ArrayList<ExprNodeDesc> keys = conf.getKeyCols();
      LOG.debug("Reduce Sink Operator " + op.getIdentifier() + " key:" + keys);
      for (ExprNodeDesc key : keys) {
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());
      }
      for (ExprNodeDesc key : conf.getPartitionCols()) {
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());
      }

      assert op.getNumChild() == 1;

      Operator<? extends OperatorDesc> child = op.getChildOperators().get(0);

      List<String> childCols = null;
      if (child instanceof CommonJoinOperator) {
        childCols = cppCtx.getJoinPrunedColLists().get(child) == null
                ? null : cppCtx.getJoinPrunedColLists().get(child)
                        .get((byte) conf.getTag());
      } else {
        childCols = cppCtx.getPrunedColList(child);
      }

      List<ExprNodeDesc> valCols = conf.getValueCols();
      List<String> valColNames = conf.getOutputValueColumnNames();

      if (childCols != null) {
        boolean[] flags = new boolean[valCols.size()];

        for (String childCol : childCols) {
          int index = valColNames.indexOf(Utilities.removeValueTag(childCol));
          if (index < 0) {
            continue;
          }
          flags[index] = true;
          colLists = Utilities.mergeUniqElems(colLists, valCols.get(index).getCols());
        }

        Collections.sort(colLists);
        pruneReduceSinkOperator(flags, op, cppCtx);
        cppCtx.getPrunedColLists().put(op, colLists);
        return null;
      }

      // Reduce Sink contains the columns needed - no need to aggregate from
      // children
      for (ExprNodeDesc val : valCols) {
        colLists = Utilities.mergeUniqElems(colLists, val.getCols());
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
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LateralViewJoinOperator op = (LateralViewJoinOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = cppCtx.genColLists(op);
      if (cols == null) {
        return null;
      }

      Map<String, ExprNodeDesc> colExprMap = op.getColumnExprMap();
      // As columns go down the DAG, the LVJ will transform internal column
      // names from something like 'key' to '_col0'. Because of this, we need
      // to undo this transformation using the column expression map as the
      // column names propagate up the DAG.

      // this is SEL(*) cols + UDTF cols
      List<String> outputCols = op.getConf().getOutputInternalColNames();

      // cause we cannot prune columns from UDTF branch currently, extract
      // columns from SEL(*) branch only and append all columns from UDTF branch to it
      int numSelColumns = op.getConf().getNumSelColumns();

      List<String> colsAfterReplacement = new ArrayList<String>();
      ArrayList<String> newColNames = new ArrayList<String>();
      for (String col : cols) {
        int index = outputCols.indexOf(col);
        // colExprMap.size() == size of cols from SEL(*) branch
        if (index >= 0 && index < numSelColumns) {
          ExprNodeDesc transformed = colExprMap.get(col);
          Utilities.mergeUniqElems(colsAfterReplacement, transformed.getCols());
          newColNames.add(col);
        }
      }
      // update number of columns from sel(*)
      op.getConf().setNumSelColumns(newColNames.size());

      // add all UDTF columns
      // following SEL will do CP for columns from UDTF, not adding SEL in here
      newColNames.addAll(outputCols.subList(numSelColumns, outputCols.size()));
      op.getConf().setOutputInternalColNames(newColNames);
      pruneOperator(ctx, op, newColNames);
      cppCtx.getPrunedColLists().put(op, colsAfterReplacement);
      return null;
    }
  }

  /**
   * The Node Processor for Column Pruning on Lateral View Forward Operators.
   */
  public static class ColumnPrunerLateralViewForwardProc extends ColumnPrunerDefaultProc {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, ctx, nodeOutputs);
      LateralViewForwardOperator op = (LateralViewForwardOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;

      // get the SEL(*) branch
      Operator<?> select = op.getChildOperators().get(LateralViewJoinOperator.SELECT_TAG);

      // Update the info of SEL operator based on the pruned reordered columns
      // these are from ColumnPrunerSelectProc
      List<String> cols = cppCtx.getPrunedColList(select);
      RowSchema rs = op.getSchema();
      ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
      ArrayList<String> outputColNames = new ArrayList<String>();
      for (String col : cols) {
        // revert output cols of SEL(*) to ExprNodeColumnDesc
        ColumnInfo colInfo = rs.getColumnInfo(col);
        ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(colInfo);
        colList.add(colExpr);
        outputColNames.add(col);
      }
      // replace SEL(*) to SEL(exprs)
      ((SelectDesc)select.getConf()).setSelStarNoCompute(false);
      ((SelectDesc)select.getConf()).setColList(colList);
      ((SelectDesc)select.getConf()).setOutputColumnNames(outputColNames);
      pruneOperator(ctx, select, outputColNames);

      Operator<?> udtfPath = op.getChildOperators().get(LateralViewJoinOperator.UDTF_TAG);
      List<String> lvFCols = new ArrayList<String>(cppCtx.getPrunedColLists().get(udtfPath));
      lvFCols = Utilities.mergeUniqElems(lvFCols, outputColNames);
      pruneOperator(ctx, op, lvFCols);

      return null;
    }
  }

  /**
   * The Node Processor for Column Pruning on Select Operators.
   */
  public static class ColumnPrunerSelectProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator op = (SelectOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;


      if (op.getChildOperators() != null) {
        for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
          // UDTF is not handled yet, so the parent SelectOp of UDTF should just assume
          // all columns.
          if ((child instanceof UDTFOperator)) {
            cppCtx.getPrunedColLists()
                .put(op, cppCtx.getColsFromSelectExpr(op));
            return null;
          }
        }
      }

      LateralViewJoinOperator lvJoin = null;
      if (op.getConf().isSelStarNoCompute()) {
        assert op.getNumChild() == 1;
        Operator<? extends OperatorDesc> child = op.getChildOperators().get(0);
        if (child instanceof LateralViewJoinOperator) { // this SEL is SEL(*)
                                                        // for LV
          lvJoin = (LateralViewJoinOperator) child;
        }
      }

      List<String> cols = cppCtx.genColLists(op);

      SelectDesc conf = op.getConf();

      if (lvJoin != null) {
        // get columns for SEL(*) from LVJ
        if (cols != null) {
          RowSchema rs = op.getSchema();
          cppCtx.getPrunedColLists().put(op,
              cppCtx.getSelectColsFromLVJoin(rs, cols));
        }
        return null;
      }
      // The input to the select does not matter. Go over the expressions
      // and return the ones which have a marked column
      cppCtx.getPrunedColLists().put(op,
          cppCtx.getSelectColsFromChildren(op, cols));
      cppCtx.getPrunedNestedColLists().put(op, cppCtx.getSelectNestedColPathsFromChildren(op, cols));
      if (cols == null || conf.isSelStarNoCompute()) {
        return null;
      }

      // do we need to prune the select operator?
      List<ExprNodeDesc> originalColList = op.getConf().getColList();
      // by now, 'prunedCols' are columns used by child operators, and 'columns'
      // are columns used by this select operator.
      List<String> originalOutputColumnNames = conf.getOutputColumnNames();
      // get view column authorization.
      if (cppCtx.getParseContext().getColumnAccessInfo() != null
          && cppCtx.getParseContext().getViewProjectToTableSchema() != null
          && cppCtx.getParseContext().getViewProjectToTableSchema().containsKey(op)) {
        for (String col : cols) {
          int index = originalOutputColumnNames.indexOf(col);
          Table tab = cppCtx.getParseContext().getViewProjectToTableSchema().get(op);
          cppCtx.getParseContext().getColumnAccessInfo()
              .add(tab.getCompleteName(), tab.getCols().get(index).getName());
        }
      }
      if (cols.size() < originalOutputColumnNames.size()) {
        ArrayList<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
        ArrayList<String> newOutputColumnNames = new ArrayList<String>();
        ArrayList<ColumnInfo> rs_oldsignature = op.getSchema().getSignature();
        ArrayList<ColumnInfo> rs_newsignature = new ArrayList<ColumnInfo>();
        for (String col : cols) {
          int index = originalOutputColumnNames.indexOf(col);
          newOutputColumnNames.add(col);
          newColList.add(originalColList.get(index));
          rs_newsignature.add(rs_oldsignature.get(index));
        }
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
      for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
        if (child instanceof ReduceSinkOperator) {
          boolean[] flags = getPruneReduceSinkOpRetainFlags(
              retainedSelOutputCols, (ReduceSinkOperator) child);
          pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
        } else if (child instanceof FilterOperator) {
          // filter operator has the same output columns as its parent
          for (Operator<? extends OperatorDesc> filterChild : child
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
    LOG.info("RS " + reduce.getIdentifier() + " oldColExprMap: " + oldMap);
    RowSchema oldRS = reduce.getSchema();
    ArrayList<ColumnInfo> old_signature = oldRS.getSignature();
    ArrayList<ColumnInfo> signature = new ArrayList<ColumnInfo>(old_signature);

    List<String> valueColNames = reduceConf.getOutputValueColumnNames();
    ArrayList<String> newValueColNames = new ArrayList<String>();

    List<ExprNodeDesc> keyExprs = reduceConf.getKeyCols();
    List<ExprNodeDesc> valueExprs = reduceConf.getValueCols();
    ArrayList<ExprNodeDesc> newValueExprs = new ArrayList<ExprNodeDesc>();

    for (int i = 0; i < retainFlags.length; i++) {
      String outputCol = valueColNames.get(i);
      ExprNodeDesc outputColExpr = valueExprs.get(i);
      if (!retainFlags[i]) {
        ColumnInfo colInfo = oldRS.getColumnInfo(outputCol);
        if (colInfo == null) {
          outputCol = Utilities.ReduceField.VALUE.toString() + "." + outputCol;
          colInfo = oldRS.getColumnInfo(outputCol);
        }

        // In case there are multiple columns referenced to the same column name, we won't
        // do row resolve once more because the ColumnInfo in row resolver is already removed
        if (colInfo == null) {
          continue;
        }

        // Only remove information of a column if it is not a key,
        // i.e. this column is not appearing in keyExprs of the RS
        if (ExprNodeDescUtils.indexOf(outputColExpr, keyExprs) == -1) {
          oldMap.remove(outputCol);
          signature.remove(colInfo);
        }

      } else {
        newValueColNames.add(outputCol);
        newValueExprs.add(outputColExpr);
      }
    }

    oldRS.setSignature(signature);
    reduce.getSchema().setSignature(signature);
    reduceConf.setOutputValueColumnNames(newValueColNames);
    reduceConf.setValueCols(newValueExprs);
    TableDesc newValueTable = PlanUtils.getReduceValueTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(reduceConf.getValueCols(),
        newValueColNames, 0, ""));
    reduceConf.setValueSerializeInfo(newValueTable);
    LOG.info("RS " + reduce.getIdentifier() + " newColExprMap: " + oldMap);
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

  public static ColumnPrunerLateralViewForwardProc getLateralViewForwardProc() {
    return new ColumnPrunerLateralViewForwardProc();
  }

  /**
   * The Node Processor for Column Pruning on Join Operators.
   */
  public static class ColumnPrunerJoinProc implements NodeProcessor {
    @Override
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
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      AbstractMapJoinOperator<MapJoinDesc> op = (AbstractMapJoinOperator<MapJoinDesc>) nd;
      pruneJoinOperator(ctx, op, op.getConf(), op.getColumnExprMap(), op
          .getConf().getRetainList(), true);
      return null;
    }
  }

  /**
   * The Factory method to get UnionProc class.
   *
   * @return UnionProc
   */
  public static ColumnPrunerUnionProc getUnionProc() {
    return new ColumnPrunerUnionProc();
  }

  /**
   * The Node Processor for Column Pruning on Union Operators.
   */
  public static class ColumnPrunerUnionProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      UnionOperator op = (UnionOperator) nd;
      List<String> childColLists = cppCtx.genColLists(op);
      if (childColLists == null) {
        return null;
      }
      RowSchema inputSchema = op.getSchema();
      if (inputSchema != null) {
        List<Integer> positions = new ArrayList<>();
        RowSchema oldRS = op.getSchema();
        for (int index = 0; index < oldRS.getSignature().size(); index++) {
          ColumnInfo colInfo = oldRS.getSignature().get(index);
          if (childColLists.contains(colInfo.getInternalName())) {
            positions.add(index);
          }
        }
        cppCtx.getUnionPrunedColLists().put(op, positions);
      }
      return null;
    }
  }

  private static void pruneOperator(NodeProcessorCtx ctx,
      Operator<? extends OperatorDesc> op,
      List<String> cols)
      throws SemanticException {
    // the pruning needs to preserve the order of columns in the input schema
    RowSchema inputSchema = op.getSchema();
    if (inputSchema != null) {
      ArrayList<ColumnInfo> rs = new ArrayList<ColumnInfo>();
      RowSchema oldRS = op.getSchema();
      for(ColumnInfo i : oldRS.getSignature()) {
        if ( cols.contains(i.getInternalName())) {
          rs.add(i);
        }
      }
      op.getSchema().setSignature(rs);
    }
  }

  /**
   * The pruning needs to preserve the order of columns in the input schema
   * @param op
   * @param cols
   * @return
   * @throws SemanticException
   */
  private static List<String> preserveColumnOrder(Operator<? extends OperatorDesc> op,
      List<String> cols)
      throws SemanticException {
    RowSchema inputSchema = op.getSchema();
    if (inputSchema != null) {
      ArrayList<String> rs = new ArrayList<String>();
      ArrayList<ColumnInfo> inputCols = inputSchema.getSignature();
      for (ColumnInfo i: inputCols) {
        if (cols.contains(i.getInternalName())) {
          rs.add(i.getInternalName());
        }
      }
      return rs;
    } else {
      return cols;
    }
  }


  private static void pruneJoinOperator(NodeProcessorCtx ctx,
      CommonJoinOperator op, JoinDesc conf,
      Map<String, ExprNodeDesc> columnExprMap,
      Map<Byte, List<Integer>> retainMap, boolean mapJoin) throws SemanticException {
    ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
    List<Operator<? extends OperatorDesc>> childOperators = op
        .getChildOperators();

    LOG.info("JOIN " + op.getIdentifier() + " oldExprs: " + conf.getExprs());

    List<String> childColLists = cppCtx.genColLists(op);
    if (childColLists == null) {
      return;
    }

    Map<Byte, List<String>> prunedColLists = new HashMap<Byte, List<String>>();
    for (byte tag : conf.getTagOrder()) {
      prunedColLists.put(tag, new ArrayList<String>());
    }

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

    RowSchema joinRS = op.getSchema();
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
            .get(Byte.valueOf((byte) pos));
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

    for (Operator<? extends OperatorDesc> child : childOperators) {
      if (child instanceof ReduceSinkOperator) {
        boolean[] flags = getPruneReduceSinkOpRetainFlags(childColLists,
            (ReduceSinkOperator) child);
        pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
      }
    }

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      ColumnInfo col = joinRS.getColumnInfo(internalName);
      rs.add(col);
    }

    LOG.info("JOIN " + op.getIdentifier() + " newExprs: " + conf.getExprs());

    op.setColumnExprMap(newColExprMap);
    conf.setOutputColumnNames(outputCols);
    op.getSchema().setSignature(rs);
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
