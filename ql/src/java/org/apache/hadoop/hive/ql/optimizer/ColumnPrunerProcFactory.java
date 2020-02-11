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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
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
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
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

import static org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcCtx.fromColumnNames;
import static org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcCtx.lookupColumn;
import static org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcCtx.mergeFieldNodesWithDesc;
import static org.apache.hadoop.hive.ql.optimizer.ColumnPrunerProcCtx.toColumnNames;
import static org.apache.hadoop.hive.ql.optimizer.FieldNode.mergeFieldNodes;

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
  public static class ColumnPrunerFilterProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator op = (FilterOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      ExprNodeDesc condn = op.getConf().getPredicate();
      List<FieldNode> filterOpPrunedColLists = mergeFieldNodesWithDesc(cppCtx.genColLists(op), condn);
      List<FieldNode> filterOpPrunedColListsOrderPreserved = preserveColumnOrder(op,
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
  public static class ColumnPrunerGroupByProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbOp = (GroupByOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<FieldNode> colLists = new ArrayList<>();
      GroupByDesc conf = gbOp.getConf();

      List<ExprNodeDesc> keys = conf.getKeys();
      for (ExprNodeDesc key : keys) {
        colLists = mergeFieldNodesWithDesc(colLists, key);
      }

      List<AggregationDesc> aggrs = conf.getAggregators();
      for (AggregationDesc aggr : aggrs) {
        List<ExprNodeDesc> params = aggr.getParameters();
        for (ExprNodeDesc param : params) {
          colLists = mergeFieldNodesWithDesc(colLists, param);
        }
      }

      int groupingSetPosition = conf.getGroupingSetPosition();
      if (groupingSetPosition >= 0) {
        List<FieldNode> neededCols = cppCtx.genColLists(gbOp);
        String groupingColumn = conf.getOutputColumnNames().get(groupingSetPosition);
        if (lookupColumn(neededCols, groupingColumn) == null) {
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
        List<FieldNode> colList = cppCtx.genColLists(gbOp, child);
        Set<FieldNode> neededCols = new HashSet<>();
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
            if (lookupColumn(neededCols, colInfo.getInternalName()) == null) {
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

  public static class ColumnPrunerScriptProc implements SemanticNodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {

      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RowSchema inputRS = op.getSchema();

      List<FieldNode> prunedCols = cppCtx.getPrunedColList(op.getChildOperators()
          .get(0));
      Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
      RowSchema parentRS = parent.getSchema();
      List<ColumnInfo> sig = parentRS.getSignature();
      List<FieldNode> colList = new ArrayList<>();
      for (ColumnInfo cI : sig) {
        colList.add(new FieldNode(cI.getInternalName()));
      }

      if (prunedCols.size() != inputRS.getSignature().size()
          && !(op.getChildOperators().get(0) instanceof SelectOperator)) {
        ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputs = new ArrayList<String>();
        Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
        ArrayList<ColumnInfo> outputRS = new ArrayList<ColumnInfo>();
        for (FieldNode internalCol: prunedCols) {
          String internalName = internalCol.getFieldName();
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
      List<FieldNode> cols = ((ColumnPrunerProcCtx) ctx).getPrunedColLists().get(nd);
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
   *   and update the RR &amp; signature on the PTFOp.
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

      List<FieldNode> prunedCols = cppCtx.getPrunedColList(op.getChildOperators().get(0));
      if (conf.forWindowing()) {
        WindowTableFunctionDef def = (WindowTableFunctionDef) funcDef;
        prunedCols = mergeFieldNodes(prunedCols, getWindowFunctionColumns(def));
      } else if (conf.forNoop()) {
        prunedCols = new ArrayList(cppCtx.getPrunedColList(op.getChildOperators().get(0)));
      } else {
        prunedCols = fromColumnNames(referencedColumns);
      }

      List<ColumnInfo> newRS = prunedColumnsList(prunedCols, op.getSchema(), funcDef);

      op.getSchema().setSignature(new ArrayList<ColumnInfo>(newRS));

      ShapeDetails outputShape = funcDef.getStartOfChain().getInput().getOutputShape();
      cppCtx.getPrunedColLists().put(op, fromColumnNames(outputShape.getColumnNames()));
      return null;
    }

    private List<ColumnInfo> buildPrunedRS(List<FieldNode> prunedCols, RowSchema oldRS)
        throws SemanticException {
      ArrayList<ColumnInfo> sig = new ArrayList<ColumnInfo>();
      HashSet<FieldNode> prunedColsSet = new HashSet<>(prunedCols);
      for (ColumnInfo cInfo : oldRS.getSignature()) {
        if (lookupColumn(prunedColsSet, cInfo.getInternalName()) != null) {
          sig.add(cInfo);
        }
      }
      return sig;
    }

    // always should be in this order (see PTFDeserializer#initializeWindowing)
    private List<FieldNode> getWindowFunctionColumns(WindowTableFunctionDef tDef) {
      List<FieldNode> columns = new ArrayList<>();
      if (tDef.getWindowFunctions() != null) {
        for (WindowFunctionDef wDef : tDef.getWindowFunctions()) {
          columns.add(new FieldNode(wDef.getAlias()));
        }
      }
      return columns;
    }

    private RowResolver buildPrunedRR(List<FieldNode> prunedCols, RowSchema oldRS) throws SemanticException {
      RowResolver resolver = new RowResolver();
      HashSet<FieldNode> prunedColsSet = new HashSet<>(prunedCols);
      for (ColumnInfo cInfo : oldRS.getSignature()) {
        if (lookupColumn(prunedColsSet, cInfo.getInternalName()) != null) {
          resolver.put(cInfo.getTabAlias(), cInfo.getAlias(), cInfo);
        }
      }
      return resolver;
    }

    /*
     * add any input columns referenced in WindowFn args or expressions.
     */
    private List<ColumnInfo> prunedColumnsList(List<FieldNode> prunedCols, RowSchema oldRS,
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
              prunedCols = mergeFieldNodesWithDesc(prunedCols, exprNode);
            }
          }
        }
        if (tDef.getPartition() != null) {
          for (PTFExpressionDef col : tDef.getPartition().getExpressions()) {
            ExprNodeDesc exprNode = col.getExprNode();
            prunedCols = mergeFieldNodesWithDesc(prunedCols, exprNode);
          }
        }
        if (tDef.getOrder() != null) {
          for (PTFExpressionDef col : tDef.getOrder().getExpressions()) {
            ExprNodeDesc exprNode = col.getExprNode();
            prunedCols = mergeFieldNodesWithDesc(prunedCols, exprNode);
          }
        }
      } else {
        pDef.getOutputShape().setRr(buildPrunedRR(prunedCols, oldRS));
      }

      PTFInputDef input = pDef.getInput();
      if (input instanceof PartitionedTableFunctionDef) {
        return prunedColumnsList(prunedCols, oldRS, (PartitionedTableFunctionDef)input);
      }

      ArrayList<FieldNode> inputColumns = prunedInputList(prunedCols, input);
      input.getOutputShape().setRr(buildPrunedRR(inputColumns, oldRS));
      input.getOutputShape().setColumnNames(toColumnNames(inputColumns));

      return buildPrunedRS(prunedCols, oldRS);
    }

    /*
     * from the prunedCols list filter out columns that refer to WindowFns or WindowExprs
     * the returned list is set as the prunedList needed by the PTFOp.
     */
    private ArrayList<FieldNode> prunedInputList(List<FieldNode> prunedCols, PTFInputDef tDef) {
      ArrayList<FieldNode> prunedInputCols = new ArrayList<>();

      StructObjectInspector OI = tDef.getOutputShape().getOI();
      for(StructField f : OI.getAllStructFieldRefs()) {
        String fName = f.getFieldName();
        FieldNode fn = lookupColumn(prunedCols, fName);
        if (fn != null) {
          prunedInputCols.add(fn);
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
  public static class ColumnPrunerDefaultProc implements SemanticNodeProcessor {
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
  public static class ColumnPrunerTableScanProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOp = (TableScanOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<FieldNode> cols = cppCtx
          .genColLists((Operator<? extends OperatorDesc>) nd);
      if (cols == null && !scanOp.getConf().isGatherStats() ) {
        scanOp.setNeededColumnIDs(null);
        return null;
      }

      cols = cols == null ? new ArrayList<FieldNode>() : cols;

      cppCtx.getPrunedColLists().put((Operator<? extends OperatorDesc>) nd, cols);
      RowSchema inputRS = scanOp.getSchema();
      setupNeededColumns(scanOp, inputRS, cols);

      return null;
    }
  }

  /** Sets up needed columns for TSOP. Mainly, transfers column names from input
   * RowSchema as well as the needed virtual columns, into TableScanDesc.
   */
  public static void setupNeededColumns(TableScanOperator scanOp, RowSchema inputRS,
      List<FieldNode> cols) throws SemanticException {
    List<Integer> neededColumnIds = new ArrayList<Integer>();
    List<String> neededColumnNames = new ArrayList<String>();
    List<String> neededNestedColumnPaths = new ArrayList<>();
    List<String> referencedColumnNames = new ArrayList<String>();
    TableScanDesc desc = scanOp.getConf();
    List<VirtualColumn> virtualCols = desc.getVirtualCols();
    List<VirtualColumn> newVirtualCols = new ArrayList<VirtualColumn>();

    // add virtual columns for ANALYZE TABLE
    if(scanOp.getConf().isGatherStats()) {
      cols.add(new FieldNode(VirtualColumn.RAWDATASIZE.getName()));
    }

    for (FieldNode fn : cols) {
      String column = fn.getFieldName();
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
        neededNestedColumnPaths.addAll(fn.toPaths());
      }
    }

    desc.setVirtualCols(newVirtualCols);
    scanOp.setNeededColumnIDs(neededColumnIds);
    scanOp.setNeededColumns(neededColumnNames);
    scanOp.setNeededNestedColumnPaths(neededNestedColumnPaths);
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
  public static class ColumnPrunerReduceSinkProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      ReduceSinkDesc conf = op.getConf();

      List<FieldNode> colLists = new ArrayList<>();
      List<ExprNodeDesc> keys = conf.getKeyCols();
      LOG.debug("Reduce Sink Operator " + op.getIdentifier() + " key:" + keys);
      for (ExprNodeDesc key : keys) {
        colLists = mergeFieldNodesWithDesc(colLists, key);
      }
      for (ExprNodeDesc key : conf.getPartitionCols()) {
        colLists = mergeFieldNodesWithDesc(colLists, key);
      }

      assert op.getNumChild() == 1;

      Operator<? extends OperatorDesc> child = op.getChildOperators().get(0);

      List<FieldNode> childCols = null;
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

        for (FieldNode childCol : childCols) {
          int index = valColNames.indexOf(Utilities.removeValueTag(childCol.getFieldName()));
          if (index < 0) {
            continue;
          }
          flags[index] = true;
          colLists = mergeFieldNodesWithDesc(colLists, valCols.get(index));
        }

        Collections.sort(colLists, new Comparator<FieldNode>() {
          @Override
          public int compare(FieldNode o1, FieldNode o2) {
            return o1.getFieldName().compareTo(o2.getFieldName());
          }
        });
        pruneReduceSinkOperator(flags, op, cppCtx);
        cppCtx.getPrunedColLists().put(op, colLists);
        return null;
      }

      // Reduce Sink contains the columns needed - no need to aggregate from
      // children
      for (ExprNodeDesc val : valCols) {
        colLists = mergeFieldNodesWithDesc(colLists, val);
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
  public static class ColumnPrunerLateralViewJoinProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LateralViewJoinOperator op = (LateralViewJoinOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<FieldNode> cols = cppCtx.genColLists(op);
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

      List<FieldNode> colsAfterReplacement = new ArrayList<>();
      List<FieldNode> newCols = new ArrayList<>();
      for (int index = 0; index < numSelColumns; index++) {
        String colName = outputCols.get(index);
        FieldNode col = lookupColumn(cols, colName);
        // colExprMap.size() == size of cols from SEL(*) branch
        if (col != null) {
          ExprNodeDesc transformed = colExprMap.get(col.getFieldName());
          colsAfterReplacement = mergeFieldNodesWithDesc(colsAfterReplacement, transformed);
          newCols.add(col);
        }
      }
      // update number of columns from sel(*)
      op.getConf().setNumSelColumns(newCols.size());

      // add all UDTF columns
      // following SEL will do CP for columns from UDTF, not adding SEL in here
      newCols.addAll(fromColumnNames(outputCols.subList(numSelColumns, outputCols.size())));
      op.getConf().setOutputInternalColNames(toColumnNames(newCols));
      pruneOperator(ctx, op, newCols);
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
      List<FieldNode> cols = cppCtx.getPrunedColList(select);
      RowSchema rs = op.getSchema();
      ArrayList<ExprNodeDesc> colList = new ArrayList<>();
      List<FieldNode> outputCols = new ArrayList<>();
      for (ColumnInfo colInfo : rs.getSignature()) {
        FieldNode col = lookupColumn(cols, colInfo.getInternalName());
        if (col != null) {
          // revert output cols of SEL(*) to ExprNodeColumnDesc
          ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(colInfo);
          colList.add(colExpr);
          outputCols.add(col);
        }
      }
      // replace SEL(*) to SEL(exprs)
      ((SelectDesc)select.getConf()).setSelStarNoCompute(false);
      ((SelectDesc)select.getConf()).setColList(colList);
      ((SelectDesc)select.getConf()).setOutputColumnNames(toColumnNames(outputCols));
      pruneOperator(ctx, select, outputCols);

      Operator<?> udtfPath = op.getChildOperators().get(LateralViewJoinOperator.UDTF_TAG);
      List<FieldNode> lvFCols = new ArrayList<>(cppCtx.getPrunedColLists().get(udtfPath));
      lvFCols = mergeFieldNodes(lvFCols, outputCols);
      pruneOperator(ctx, op, lvFCols);

      return null;
    }
  }

  /**
   * The Node Processor for Column Pruning on Select Operators.
   */
  public static class ColumnPrunerSelectProc implements SemanticNodeProcessor {
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

      List<FieldNode> cols = cppCtx.genColLists(op);

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
        for (FieldNode col : cols) {
          int index = originalOutputColumnNames.indexOf(col.getFieldName());
          Table tab = cppCtx.getParseContext().getViewProjectToTableSchema().get(op);
          List<FieldSchema> fullFieldList = new ArrayList<FieldSchema>(tab.getCols());
          fullFieldList.addAll(tab.getPartCols());
          cppCtx.getParseContext().getColumnAccessInfo()
              .add(tab.getCompleteName(), fullFieldList.get(index).getName());
        }
      }
      if (cols.size() < originalOutputColumnNames.size()) {
        List<ExprNodeDesc> newColList = new ArrayList<ExprNodeDesc>();
        List<String> newOutputColumnNames = new ArrayList<String>();
        List<ColumnInfo> rs_oldsignature = op.getSchema().getSignature();
        List<ColumnInfo> rs_newsignature = new ArrayList<ColumnInfo>();
        // The pruning needs to preserve the order of columns in the input schema
        Set<String> colNames = new HashSet<String>();
        for (FieldNode col : cols) {
          colNames.add(col.getFieldName());
        }
        for (int i = 0; i < originalOutputColumnNames.size(); i++) {
          String colName = originalOutputColumnNames.get(i);
          if (colNames.contains(colName)) {
            newOutputColumnNames.add(colName);
            newColList.add(originalColList.get(i));
            rs_newsignature.add(rs_oldsignature.get(i));
          }
        }
        op.getSchema().setSignature(rs_newsignature);
        conf.setColList(newColList);
        conf.setOutputColumnNames(newOutputColumnNames);
        handleChildren(op, toColumnNames(cols), cppCtx);
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
    List<ExprNodeDesc> originalValueEval = reduceConf.getValueCols();
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
    List<ColumnInfo> old_signature = oldRS.getSignature();
    List<ColumnInfo> signature = new ArrayList<ColumnInfo>(old_signature);

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
  public static class ColumnPrunerJoinProc implements SemanticNodeProcessor {
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
  public static class ColumnPrunerMapJoinProc implements SemanticNodeProcessor {
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
  public static class ColumnPrunerUnionProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      UnionOperator op = (UnionOperator) nd;
      List<FieldNode> childColLists = cppCtx.genColLists(op);
      if (childColLists == null) {
        return null;
      }
      RowSchema inputSchema = op.getSchema();
      if (inputSchema != null) {
        List<FieldNode> prunedCols = new ArrayList<>();
        for (int index = 0; index < inputSchema.getSignature().size(); index++) {
          ColumnInfo colInfo = inputSchema.getSignature().get(index);
          FieldNode fn = lookupColumn(childColLists, colInfo.getInternalName());
          if (fn != null) {
            prunedCols.add(fn);
          }
        }
        cppCtx.getPrunedColLists().put(op, prunedCols);
      }
      return null;
    }
  }

  private static void pruneOperator(NodeProcessorCtx ctx,
                                    Operator<? extends OperatorDesc> op,
                                    List<FieldNode> cols)
      throws SemanticException {
    // the pruning needs to preserve the order of columns in the input schema
    RowSchema inputSchema = op.getSchema();
    if (inputSchema != null) {
      ArrayList<ColumnInfo> rs = new ArrayList<ColumnInfo>();
      RowSchema oldRS = op.getSchema();
      for(ColumnInfo i : oldRS.getSignature()) {
        if (lookupColumn(cols, i.getInternalName()) != null) {
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
  private static List<FieldNode> preserveColumnOrder(Operator<? extends OperatorDesc> op,
      List<FieldNode> cols)
      throws SemanticException {
    RowSchema inputSchema = op.getSchema();
    if (inputSchema != null) {
      List<FieldNode> rs = new ArrayList<>();
      List<ColumnInfo> inputCols = inputSchema.getSignature();
      for (ColumnInfo i: inputCols) {
        FieldNode fn = lookupColumn(cols, i.getInternalName());
        if (fn != null) {
          rs.add(fn);
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

    if (cppCtx.genColLists(op) == null) {
      return;
    }

    List<FieldNode> neededColList = new ArrayList<>(cppCtx.genColLists(op));

    Map<Byte, List<FieldNode>> prunedColLists = new HashMap<>();
    for (byte tag : conf.getTagOrder()) {
      prunedColLists.put(tag, new ArrayList<FieldNode>());
    }

    //add the columns in join filters
    Set<Map.Entry<Byte, List<ExprNodeDesc>>> filters =
      conf.getFilters().entrySet();
    Iterator<Map.Entry<Byte, List<ExprNodeDesc>>> iter = filters.iterator();
    while (iter.hasNext()) {
      Map.Entry<Byte, List<ExprNodeDesc>> entry = iter.next();
      Byte tag = entry.getKey();
      for (ExprNodeDesc desc : entry.getValue()) {
        List<FieldNode> cols = prunedColLists.get(tag);
        cols = mergeFieldNodesWithDesc(cols, desc);
        prunedColLists.put(tag, cols);
     }
    }

    //add the columns in residual filters
    if (conf.getResidualFilterExprs() != null) {
      for (ExprNodeDesc desc : conf.getResidualFilterExprs()) {
        neededColList = mergeFieldNodesWithDesc(neededColList, desc);
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
      if (lookupColumn(neededColList, internalName) == null) {
        int index = conf.getExprs().get(tag).indexOf(desc);
        if (index < 0) {
          continue;
        }
        conf.getExprs().get(tag).remove(desc);
        if (retainMap != null) {
          retainMap.get(tag).remove(index);
        }
      } else {
        List<FieldNode> prunedRSList = prunedColLists.get(tag);
        if (prunedRSList == null) {
          prunedRSList = new ArrayList<>();
          prunedColLists.put(tag, prunedRSList);
        }
        prunedColLists.put(tag, mergeFieldNodesWithDesc(prunedRSList, desc));
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
          List<FieldNode> cols = prunedColLists.get(tag);
          cols = mergeFieldNodesWithDesc(cols, desc);
          prunedColLists.put(tag, cols);
        }
      }

    }

    for (Operator<? extends OperatorDesc> child : childOperators) {
      if (child instanceof ReduceSinkOperator) {
        boolean[] flags = getPruneReduceSinkOpRetainFlags(toColumnNames(neededColList),
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
