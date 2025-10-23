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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyType;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Predicate;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.TableAliasInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Utils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.ptf.Noop;

/**
 * Operator factory for the rule processors for lineage.
 */
public class OpProcFactory {

  /**
   * Returns the parent operator in the walk path to the current operator.
   *
   * @param stack The stack encoding the path.
   *
   * @return Operator The parent operator in the current path.
   */
  @SuppressWarnings("unchecked")
  protected static Operator<? extends OperatorDesc> getParent(Stack<Node> stack) {
    return (Operator<? extends OperatorDesc>)Utils.getNthAncestor(stack, 1);
  }

  /**
   * Processor for Script and UDTF Operators.
   */
  public static class TransformLineage extends DefaultLineage implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // LineageCTx
      LineageCtx lCtx = (LineageCtx) procCtx;

      // The operators
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, op);

      // Create a single dependency list by concatenating the dependencies of all
      // the cols
      Dependency dep = new Dependency();
      DependencyType newType = LineageInfo.DependencyType.SCRIPT;
      dep.setType(LineageInfo.DependencyType.SCRIPT);
      // TODO: Fix this to a non null value.
      dep.setExpr(null);

      LinkedHashSet<BaseColumnInfo> colSet = new LinkedHashSet<BaseColumnInfo>();
      for(ColumnInfo ci : inpOp.getSchema().getSignature()) {
        Dependency d = lCtx.getIndex().getDependency(inpOp, ci);
        if (d != null) {
          newType = LineageCtx.getNewDependencyType(d.getType(), newType);
          if (!ci.isHiddenVirtualCol()) {
            colSet.addAll(d.getBaseCols());
          }
        }
      }

      dep.setType(newType);
      dep.setBaseCols(colSet);

      boolean isScript = op instanceof ScriptOperator;

      // This dependency is then set for all the colinfos of the script operator
      for(ColumnInfo ci : op.getSchema().getSignature()) {
        Dependency d = dep;
        if (!isScript) {
          Dependency depCi = lCtx.getIndex().getDependency(inpOp, ci);
          if (depCi != null) {
            d = depCi;
          }
        }
        lCtx.getIndex().putDependency(op, ci, d);
      }

      return null;
    }

  }

  /**
   * Processor for TableScan Operator. This actually creates the base column mappings.
   */
  public static class TableScanLineage extends DefaultLineage implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      ParseContext pctx = lCtx.getParseCtx();

      // Table scan operator.
      TableScanOperator top = (TableScanOperator)nd;
      org.apache.hadoop.hive.ql.metadata.Table t = top.getConf().getTableMetadata();
      Table tab = t.getTTable();

      // Generate the mappings
      RowSchema rs = top.getSchema();
      List<FieldSchema> cols = t.getAllCols();
      Map<String, FieldSchema> fieldSchemaMap = new HashMap<String, FieldSchema>();
      for(FieldSchema col : cols) {
        fieldSchemaMap.put(col.getName(), col);
      }

      Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry().iterator();
      while (vcs.hasNext()) {
        VirtualColumn vc = vcs.next();
        fieldSchemaMap.put(vc.getName(), new FieldSchema(vc.getName(),
            vc.getTypeInfo().getTypeName(), ""));
      }

      TableAliasInfo tai = new TableAliasInfo();
      tai.setAlias(top.getConf().getAlias());
      tai.setTable(tab);
      for(ColumnInfo ci : rs.getSignature()) {
        // Create a dependency
        Dependency dep = new Dependency();
        BaseColumnInfo bci = new BaseColumnInfo();
        bci.setTabAlias(tai);
        bci.setColumn(fieldSchemaMap.get(ci.getInternalName()));

        // Populate the dependency
        dep.setType(LineageInfo.DependencyType.SIMPLE);
        dep.setBaseCols(new LinkedHashSet<BaseColumnInfo>());
        dep.getBaseCols().add(bci);

        // Put the dependency in the map
        lCtx.getIndex().putDependency(top, ci, dep);
      }

      return null;
    }

  }

  /**
   * Processor for Join Operator.
   */
  public static class JoinLineage extends DefaultLineage implements SemanticNodeProcessor {

    private final HashMap<Node, Object> outputMap = new HashMap<Node, Object>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      JoinOperator op = (JoinOperator)nd;
      JoinDesc jd = op.getConf();

      // The input operator to the join is always a reduce sink operator
      ReduceSinkOperator inpOp = (ReduceSinkOperator)getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, op);
      Predicate cond = getPredicate(op, lCtx);
      if (cond != null) {
        lCtx.getIndex().addPredicate(op, cond);
      }

      ReduceSinkDesc rd = inpOp.getConf();
      int tag = rd.getTag();

      // Iterate over the outputs of the join operator and merge the
      // dependencies of the columns that corresponding to the tag.
      int cnt = 0;
      List<ExprNodeDesc> exprs = jd.getExprs().get((byte)tag);
      for(ColumnInfo ci : op.getSchema().getSignature()) {
        if (jd.getReversedExprs().get(ci.getInternalName()) != tag) {
          continue;
        }

        // Otherwise look up the expression corresponding to this ci
        ExprNodeDesc expr = exprs.get(cnt++);
        Dependency dependency = ExprProcFactory.getExprDependency(lCtx, inpOp, expr, outputMap);
        lCtx.getIndex().mergeDependency(op, ci, dependency);
      }

      return null;
    }

    private Predicate getPredicate(JoinOperator jop, LineageCtx lctx) {
      List<Operator<? extends OperatorDesc>> parentOperators = jop.getParentOperators();
      JoinDesc jd = jop.getConf();
      ExprNodeDesc [][] joinKeys = jd.getJoinKeys();
      if (joinKeys == null || parentOperators == null || parentOperators.size() < 2) {
        return null;
      }
      LineageCtx.Index index = lctx.getIndex();
      for (Operator<? extends OperatorDesc> op: parentOperators) {
        if (index.getDependencies(op) == null) {
          return null;
        }
      }
      Predicate cond = new Predicate();
      JoinCondDesc[] conds = jd.getConds();
      int parents = parentOperators.size();
      StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i < conds.length; i++) {
        if (i != 0) {
          sb.append(" AND ");
        }
        int left = conds[i].getLeft();
        int right = conds[i].getRight();
        if (joinKeys.length <= left
            || joinKeys[left].length == 0
            || joinKeys.length <= right
            || joinKeys[right].length == 0
            || parents < left
            || parents < right) {
          return null;
        }
        ExprNodeDesc expr = joinKeys[left][0];
        Operator<? extends OperatorDesc> op = parentOperators.get(left);
        List<Operator<? extends OperatorDesc>> p = op.getParentOperators();
        if (p == null || p.isEmpty()) {
          return null;
        }
        sb.append(ExprProcFactory.getExprString(op.getSchema(),
          expr, lctx, p.get(0), cond));
        sb.append(" = ");
        expr = joinKeys[right][0];
        op = parentOperators.get(right);
        p = op.getParentOperators();
        if (p == null || p.isEmpty()) {
          return null;
        }
        sb.append(ExprProcFactory.getExprString(op.getSchema(),
          expr, lctx, p.get(0), cond));
      }
      sb.append(")");
      cond.setExpr(sb.toString());
      return cond;
    }
  }

  /**
   * Processor for Join Operator.
   */
  public static class LateralViewJoinLineage extends DefaultLineage implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      LateralViewJoinOperator op = (LateralViewJoinOperator)nd;
      boolean isUdtfPath = true;
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      List<ColumnInfo> cols = inpOp.getSchema().getSignature();
      lCtx.getIndex().copyPredicates(inpOp, op);

      if (inpOp instanceof SelectOperator) {
        isUdtfPath = false;
      }

      // Dirty hack!!
      // For the select path the columns are the ones at the beginning of the
      // current operators schema and for the udtf path the columns are
      // at the end of the operator schema.
      List<ColumnInfo> outCols = op.getSchema().getSignature();
      int outColsSize = outCols.size();
      int colsSize = cols.size();
      int outColOffset = isUdtfPath ? outColsSize - colsSize : 0;
      for (int cnt = 0; cnt < colsSize; cnt++) {
        ColumnInfo outCol = outCols.get(outColOffset + cnt);
        if (!outCol.isHiddenVirtualCol()) {
          ColumnInfo col = cols.get(cnt);
          lCtx.getIndex().mergeDependency(op, outCol,
            lCtx.getIndex().getDependency(inpOp, col));
        }
      }
      return null;
    }

  }

  /**
   * Processor for Select operator.
   */
  public static class SelectLineage extends DefaultLineage implements SemanticNodeProcessor {

    private final HashMap<Node, Object> outputMap = new HashMap<Node, Object>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      LineageCtx lctx = (LineageCtx)procCtx;
      SelectOperator sop = (SelectOperator)nd;

      // if this is a selStarNoCompute then this select operator
      // is treated like a default operator, so just call the super classes
      // process method.
      if (sop.getConf().isSelStarNoCompute()) {
        return super.process(nd, stack, procCtx, nodeOutputs);
      }

      // Otherwise we treat this as a normal select operator and look at
      // the expressions.

      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lctx.getIndex().copyPredicates(inpOp, sop);

      RowSchema rs = sop.getSchema();
      List<ColumnInfo> colInfos = rs.getSignature();
      int cnt = 0;
      for(ExprNodeDesc expr : sop.getConf().getColList()) {
        Dependency dep = ExprProcFactory.getExprDependency(lctx, inpOp, expr, outputMap);
        if (dep != null && dep.getExpr() == null && (dep.getBaseCols().isEmpty()
            || dep.getType() != LineageInfo.DependencyType.SIMPLE)) {
          dep.setExpr(ExprProcFactory.getExprString(rs, expr, lctx, inpOp, null));
        }
        lctx.getIndex().putDependency(sop, colInfos.get(cnt++), dep);
      }

      Operator<? extends OperatorDesc> op = null;
      if (!sop.getChildOperators().isEmpty()) {
        op = sop.getChildOperators().get(0);
        if (!op.getChildOperators().isEmpty() && op instanceof LimitOperator) {
          op = op.getChildOperators().get(0);
        }
      }
      if (op == null || (op.getChildOperators().isEmpty()
          && op instanceof FileSinkOperator)) {
        lctx.getIndex().addFinalSelectOp(sop, op);
      }

      return null;
    }

  }

  /**
   * Processor for GroupBy operator.
   */
  public static class GroupByLineage extends DefaultLineage implements SemanticNodeProcessor {

    private final HashMap<Node, Object> outputMap = new HashMap<Node, Object>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      LineageCtx lctx = (LineageCtx)procCtx;
      GroupByOperator gop = (GroupByOperator)nd;
      List<ColumnInfo> colInfos = gop.getSchema().getSignature();
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lctx.getIndex().copyPredicates(inpOp, gop);
      int cnt = 0;

      for(ExprNodeDesc expr : gop.getConf().getKeys()) {
        lctx.getIndex().putDependency(gop, colInfos.get(cnt++),
            ExprProcFactory.getExprDependency(lctx, inpOp, expr, outputMap));
      }

      // If this is a reduce side GroupBy operator, check if there is
      // a corresponding map side one. If so, some expression could have
      // already been resolved in the map side.
      boolean reduceSideGop = (inpOp instanceof ReduceSinkOperator)
        && (Utils.getNthAncestor(stack, 2) instanceof GroupByOperator);

      RowSchema rs = gop.getSchema();
      for(AggregationDesc agg : gop.getConf().getAggregators()) {
        // Concatenate the dependencies of all the parameters to
        // create the new dependency
        Dependency dep = new Dependency();
        DependencyType newType = LineageInfo.DependencyType.EXPRESSION;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        LinkedHashSet<BaseColumnInfo> bciSet = new LinkedHashSet<BaseColumnInfo>();
        for(ExprNodeDesc expr : agg.getParameters()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          Dependency exprDep = ExprProcFactory.getExprDependency(lctx, inpOp, expr, outputMap);
          if (exprDep != null && !exprDep.getBaseCols().isEmpty()) {
            newType = LineageCtx.getNewDependencyType(exprDep.getType(), newType);
            bciSet.addAll(exprDep.getBaseCols());
            if (exprDep.getType() == LineageInfo.DependencyType.SIMPLE) {
              BaseColumnInfo col = exprDep.getBaseCols().iterator().next();
              Table t = col.getTabAlias().getTable();
              if (t != null) {
                sb.append(Warehouse.getQualifiedName(t)).append(".");
              }
              sb.append(col.getColumn().getName());
            }
          }
          if (exprDep == null || exprDep.getBaseCols().isEmpty()
              || exprDep.getType() != LineageInfo.DependencyType.SIMPLE) {
            sb.append(exprDep != null && exprDep.getExpr() != null ? exprDep.getExpr() :
              ExprProcFactory.getExprString(rs, expr, lctx, inpOp, null));
          }
        }
        String expr = sb.toString();
        String udafName = agg.getGenericUDAFName();
        if (!(reduceSideGop && expr.startsWith(udafName))) {
          sb.setLength(0); // reset the buffer
          sb.append(udafName);
          sb.append("(");
          if (agg.getDistinct()) {
            sb.append("DISTINCT ");
          }
          sb.append(expr);
          if (first) {
            // No parameter, count(*)
            sb.append("*");
          }
          sb.append(")");
          expr = sb.toString();
        }
        dep.setExpr(expr);

        // If the bciSet is empty, this means that the inputs to this
        // aggregate function were all constants (e.g. count(1)). In this case
        // the aggregate function is just dependent on all the tables that are in
        // the dependency list of the input operator.
        if (bciSet.isEmpty()) {
          Set<TableAliasInfo> taiSet = new LinkedHashSet<TableAliasInfo>();
          if (inpOp.getSchema() != null && inpOp.getSchema().getSignature() != null ) {
            for(ColumnInfo ci : inpOp.getSchema().getSignature()) {
              Dependency inpDep = lctx.getIndex().getDependency(inpOp, ci);
              // The dependency can be null as some of the input cis may not have
              // been set in case of joins.
              if (inpDep != null) {
                for(BaseColumnInfo bci : inpDep.getBaseCols()) {
                  newType = LineageCtx.getNewDependencyType(inpDep.getType(), newType);
                  taiSet.add(bci.getTabAlias());
                }
              }
            }
          }

          // Create the BaseColumnInfos and set them in the bciSet
          for(TableAliasInfo tai : taiSet) {
            BaseColumnInfo bci = new BaseColumnInfo();
            bci.setTabAlias(tai);
            // This is set to null to reflect that the dependency is not on any
            // particular column of the table.
            bci.setColumn(null);
            bciSet.add(bci);
          }
        }

        dep.setBaseCols(bciSet);
        dep.setType(newType);
        lctx.getIndex().putDependency(gop, colInfos.get(cnt++), dep);
      }

      return null;
    }

  }

  /**
   * Union processor.
   * In this case we call mergeDependency as opposed to putDependency
   * in order to account for visits from different parents.
   */
  public static class UnionLineage extends DefaultLineage implements SemanticNodeProcessor {

    @SuppressWarnings("unchecked")
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;

      // Get the row schema of the input operator.
      // The row schema of the parent operator
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, op);
      RowSchema rs = op.getSchema();
      List<ColumnInfo> inpCols = inpOp.getSchema().getSignature();

      // check only for input cols
      for(ColumnInfo input : inpCols) {
        Dependency inpDep = lCtx.getIndex().getDependency(inpOp, input);
        if (inpDep != null) {
          //merge it with rs colInfo
          ColumnInfo ci = rs.getColumnInfo(input.getInternalName());
          lCtx.getIndex().mergeDependency(op, ci, inpDep);
        }
      }
      return null;
    }
  }

  /**
   * ReduceSink processor.
   */
  public static class ReduceSinkLineage implements SemanticNodeProcessor {

    private final HashMap<Node, Object> outputMap = new HashMap<Node, Object>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      ReduceSinkOperator rop = (ReduceSinkOperator)nd;

      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, rop);
      int cnt = 0;

      // The keys are included only in case the reduce sink feeds into
      // a group by operator through a chain of forward operators
      Operator<? extends OperatorDesc> op = rop.getChildOperators().get(0);
      while (op instanceof ForwardOperator) {
        op = op.getChildOperators().get(0);
      }

      if (op instanceof GroupByOperator) {
        List<ColumnInfo> colInfos = rop.getSchema().getSignature();
        for(ExprNodeDesc expr : rop.getConf().getKeyCols()) {
          lCtx.getIndex().putDependency(rop, colInfos.get(cnt++),
              ExprProcFactory.getExprDependency(lCtx, inpOp, expr, outputMap));
        }
        for(ExprNodeDesc expr : rop.getConf().getValueCols()) {
          lCtx.getIndex().putDependency(rop, colInfos.get(cnt++),
              ExprProcFactory.getExprDependency(lCtx, inpOp, expr, outputMap));
        }
      } else {
        RowSchema schema = rop.getSchema();
        ReduceSinkDesc desc = rop.getConf();
        List<ExprNodeDesc> keyCols = desc.getKeyCols();
        List<String> keyColNames = desc.getOutputKeyColumnNames();
        for (int i = 0; i < keyCols.size(); i++) {
          // order-bys, joins
          ColumnInfo column = schema.getColumnInfo(Utilities.ReduceField.KEY + "." + keyColNames.get(i));
          if (column == null) {
            continue;   // key in values
          }
          lCtx.getIndex().putDependency(rop, column,
              ExprProcFactory.getExprDependency(lCtx, inpOp, keyCols.get(i), outputMap));
        }
        List<ExprNodeDesc> valCols = desc.getValueCols();
        List<String> valColNames = desc.getOutputValueColumnNames();
        for (int i = 0; i < valCols.size(); i++) {
          // todo: currently, bucketing,etc. makes RS differently with those for order-bys or joins
          ColumnInfo column = schema.getColumnInfo(valColNames.get(i));
          if (column == null) {
            // order-bys, joins
            column = schema.getColumnInfo(Utilities.ReduceField.VALUE + "." + valColNames.get(i));
          }
          lCtx.getIndex().putDependency(rop, column,
              ExprProcFactory.getExprDependency(lCtx, inpOp, valCols.get(i), outputMap));
        }
      }

      return null;
    }
  }

  /**
   * Filter processor.
   */
  public static class FilterLineage implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      FilterOperator fop = (FilterOperator)nd;

      // Get the row schema of the input operator.
      // The row schema of the parent operator
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, fop);
      FilterDesc filterDesc = fop.getConf();
      RowSchema rs = fop.getSchema();
      if (!filterDesc.isGenerated()) {
        Predicate cond = new Predicate();
        cond.setExpr(ExprProcFactory.getExprString(
          rs, filterDesc.getPredicate(), lCtx, inpOp, cond));
        lCtx.getIndex().addPredicate(fop, cond);
      }

      List<ColumnInfo> inpCols = inpOp.getSchema().getSignature();
      int cnt = 0;
      for(ColumnInfo ci : rs.getSignature()) {
        lCtx.getIndex().putDependency(fop, ci,
            lCtx.getIndex().getDependency(inpOp, inpCols.get(cnt++)));
      }

      return null;
    }
  }

  /**
   * PTF processor
   */
  public static class PTFLineage implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
      // LineageCTx
      LineageCtx lCtx = (LineageCtx) procCtx;

      // The operators
      @SuppressWarnings("unchecked")
      PTFOperator op = (PTFOperator)nd;
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, op);

      Dependency dep = new Dependency();
      DependencyType newType = DependencyType.EXPRESSION;
      dep.setType(newType);

      Set<String> columns = new HashSet<>();
      PartitionedTableFunctionDef funcDef = op.getConf().getFuncDef();
      StringBuilder sb = new StringBuilder();
      WindowFrameDef windowFrameDef = null;

      if (!(funcDef.getTFunction() instanceof Noop)) {

        if (funcDef instanceof WindowTableFunctionDef) {
          // function name
          WindowFunctionDef windowFunctionDef = ((WindowTableFunctionDef) funcDef).getWindowFunctions().getFirst();
          sb.append(windowFunctionDef.getName()).append("(");

          addArgs(sb, columns, lCtx, inpOp, op.getSchema(), windowFunctionDef.getArgs());

          windowFrameDef = windowFunctionDef.getWindowFrame();

          if (sb.charAt(sb.length() - 2) == ',') {
            sb.delete(sb.length() - 2, sb.length());
          }
          sb.append(")");
          sb.append(" over (");
        } else /* PartitionedTableFunctionDef */ {
          // function name
          sb.append(funcDef.getName()).append("(");
          addArgs(sb, columns, lCtx, inpOp, funcDef.getRawInputShape().getRr().getRowSchema(), funcDef.getArgs());

          // matchpath has argument pattern like matchpath(<input expression>, <argument methods: arg1(), arg2()...>)
          if (funcDef.getInput() != null) {
            sb.append("on ").append(funcDef.getInput().getAlias()).append(" ");

            int counter = 1;
            for (PTFExpressionDef arg : funcDef.getArgs()) {
              ExprNodeDesc exprNode = arg.getExprNode();

              addIfNotNull(columns, exprNode.getCols());

              sb.append("arg").append(counter++).append("(");
              sb.append(ExprProcFactory.getExprString(funcDef.getRawInputShape().getRr().getRowSchema(), arg.getExprNode(), lCtx, inpOp, null));
              sb.append("), ");
            }

            sb.delete(sb.length() - 2, sb.length());
          }
        }
      }

      /*
        Collect partition by and distribute by information.
        Please note, at the expression node level, there is no difference between those.
        That means distribute by gets a string partition by in the expression string.
       */
      if (funcDef.getPartition() != null ) {
        List<PTFExpressionDef> partitionExpressions = funcDef.getPartition().getExpressions();

        boolean isPartitionByAdded = false;
        for (PTFExpressionDef partitionExpr : partitionExpressions) {
          ExprNodeDesc partitionExprNode = partitionExpr.getExprNode();

          if (partitionExprNode.getCols() != null && !partitionExprNode.getCols().isEmpty()) {
            if (!isPartitionByAdded) {
              sb.append("partition by ");
              isPartitionByAdded = true;
            }

            addIfNotNull(columns, partitionExprNode.getCols());

            if (partitionExprNode instanceof ExprNodeColumnDesc) {
              sb.append(ExprProcFactory.getExprString(funcDef.getRawInputShape().getRr().getRowSchema(), partitionExprNode, lCtx, inpOp, null));
              sb.append(", ");
            }

            sb.delete(sb.length() - 2, sb.length());
          }
        }

      }

      /*
        Collects the order by and sort by information.
        Please note, at the expression node level, there is no difference between those.
        That means sort by gets a string partition by in the expression string.
       */
      if (funcDef.getOrder() != null) {
        /*
        Order by is sometimes added by the compiler to make the PTF call deterministic.
        At this point of the code execution, we don't know if it is added by the compiler or
        it was originally part of the query string.
        */
        List<OrderExpressionDef> orderExpressions = funcDef.getOrder().getExpressions();

        if (!sb.isEmpty() && sb.charAt(sb.length() - 1) != '(') {
          sb.append(" ");
        }
        sb.append("order by ");

        for (OrderExpressionDef orderExpr : orderExpressions) {
          ExprNodeDesc orderExprNode = orderExpr.getExprNode();
          addIfNotNull(columns, orderExprNode.getCols());

          sb.append(ExprProcFactory.getExprString(funcDef.getRawInputShape().getRr().getRowSchema(), orderExprNode, lCtx, inpOp, null));
          if (PTFInvocationSpec.Order.DESC.equals(orderExpr.getOrder())) {
            sb.append(" desc");
          }
          sb.append(", ");
        }

        sb.delete(sb.length() - 2, sb.length());
      }

      /*
      Window frame is sometimes added by the compiler to make the PTF call deterministic.
      At this point of the code execution, we don't know if it is added by the compiler or
      it was originally part of the query string.
      */
      if (windowFrameDef != null) {
        sb.append(" ").append(windowFrameDef.getWindowType()).append(" between ");

        appendBoundary(windowFrameDef.getStart(), sb, " preceding");

        sb.append(" and ");

        appendBoundary(windowFrameDef.getEnd(), sb, " following");
      }

      sb.append(")");
      dep.setExpr(sb.toString());

      LinkedHashSet<BaseColumnInfo> colSet = new LinkedHashSet<>();
      for(ColumnInfo ci : inpOp.getSchema().getSignature()) {
        Dependency d = lCtx.getIndex().getDependency(inpOp, ci);
        if (d != null) {
          newType = LineageCtx.getNewDependencyType(d.getType(), newType);
          if (!ci.isHiddenVirtualCol() && columns.contains(ci.getInternalName())) {
            colSet.addAll(d.getBaseCols());
          }
        }
      }

      dep.setType(newType);
      dep.setBaseCols(colSet);

      // This dependency is then set for all the colinfos of the script operator
      for(ColumnInfo ci : op.getSchema().getSignature()) {
        Dependency d = dep;
          Dependency depCi = lCtx.getIndex().getDependency(inpOp, ci);
          if (depCi != null) {
            d = depCi;
          }
        lCtx.getIndex().putDependency(op, ci, d);
      }

      return null;
    }

    private static void appendBoundary(BoundaryDef boundary, StringBuilder sb, String boundaryText) {
      if (boundary.isCurrentRow()) {
        sb.append("current_row");
      } else {
        sb.append(boundary.isUnbounded() ? "unbounded" : boundary.getAmt() + boundaryText);
      }
    }

    /*
      Adds the PTF arguments for the lineage column list and also the expression string.
    */
    private void addArgs(
            StringBuilder sb,
            Set<String> columns,
            LineageCtx lCtx,
            Operator<? extends OperatorDesc> inpOp,
            RowSchema rowSchema,
            List<PTFExpressionDef> args)
    {
      if (args == null || args.isEmpty()) {
        return;
      }

      for (PTFExpressionDef arg : args) {
        ExprNodeDesc argNode = arg.getExprNode();

        if (argNode.getCols() != null && !argNode.getCols().isEmpty()) {
          addIfNotNull(columns, argNode.getCols());
        }

        if (argNode instanceof ExprNodeConstantDesc) {
          boolean isString = "string".equals(argNode.getTypeInfo().getTypeName());

          if (isString) {
            sb.append("'");
          }
          sb.append(((ExprNodeConstantDesc) argNode).getValue());
          if (isString) {
            sb.append("'");
          }
          sb.append(", ");
        } else if (argNode instanceof ExprNodeColumnDesc || argNode instanceof ExprNodeGenericFuncDesc) {
          ExprNodeDesc exprNode = arg.getExprNode();

          addIfNotNull(columns, exprNode.getCols());
          sb.append(ExprProcFactory.getExprString(rowSchema, exprNode, lCtx, inpOp, null));
          sb.append(", ");
        }
      }
    }

    private void addIfNotNull(Set<String> set, List<String> items) {
      if (items == null || items.isEmpty()) {
        return;
      }

      for (String item : items) {
        if (item != null) {
          set.add(item);
        }
      }
    }
  }

  /**
   * Default processor. This basically passes the input dependencies as such
   * to the output dependencies.
   */
  public static class DefaultLineage implements SemanticNodeProcessor {

    @SuppressWarnings("unchecked")
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is at least one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;

      // Get the row schema of the input operator.
      // The row schema of the parent operator
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lCtx.getIndex().copyPredicates(inpOp, op);
      RowSchema rs = op.getSchema();
      List<ColumnInfo> inpCols = inpOp.getSchema().getSignature();
      int cnt = 0;
      for(ColumnInfo ci : rs.getSignature()) {
        lCtx.getIndex().putDependency(op, ci,
            lCtx.getIndex().getDependency(inpOp, inpCols.get(cnt++)));
      }
      return null;
    }
  }

  public static SemanticNodeProcessor getJoinProc() {
    return new JoinLineage();
  }

  public static SemanticNodeProcessor getLateralViewJoinProc() {
    return new LateralViewJoinLineage();
  }

  public static SemanticNodeProcessor getTSProc() {
    return new TableScanLineage();
  }

  public static SemanticNodeProcessor getTransformProc() {
    return new TransformLineage();
  }

  public static SemanticNodeProcessor getSelProc() {
    return new SelectLineage();
  }

  public static SemanticNodeProcessor getGroupByProc() {
    return new GroupByLineage();
  }

  public static SemanticNodeProcessor getUnionProc() {
    return new UnionLineage();
  }

  public static SemanticNodeProcessor getReduceSinkProc() {
    return new ReduceSinkLineage();
  }

  public static SemanticNodeProcessor getDefaultProc() {
    return new DefaultLineage();
  }

  public static SemanticNodeProcessor getFilterProc() {
    return new FilterLineage();
  }

  public static SemanticNodeProcessor getPTFProc() { return new PTFLineage(); }
}
