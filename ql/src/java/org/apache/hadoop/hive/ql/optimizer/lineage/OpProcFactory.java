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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

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
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Utils;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

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
  public static class TransformLineage extends DefaultLineage implements NodeProcessor {

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
      DependencyType new_type = LineageInfo.DependencyType.SCRIPT;
      dep.setType(LineageInfo.DependencyType.SCRIPT);
      // TODO: Fix this to a non null value.
      dep.setExpr(null);

      LinkedHashSet<BaseColumnInfo> col_set = new LinkedHashSet<BaseColumnInfo>();
      for(ColumnInfo ci : inpOp.getSchema().getSignature()) {
        Dependency d = lCtx.getIndex().getDependency(inpOp, ci);
        if (d != null) {
          new_type = LineageCtx.getNewDependencyType(d.getType(), new_type);
          if (!ci.isHiddenVirtualCol()) {
            col_set.addAll(d.getBaseCols());
          }
        }
      }

      dep.setType(new_type);
      dep.setBaseCols(col_set);

      boolean isScript = op instanceof ScriptOperator;

      // This dependency is then set for all the colinfos of the script operator
      for(ColumnInfo ci : op.getSchema().getSignature()) {
        Dependency d = dep;
        if (!isScript) {
          Dependency dep_ci = lCtx.getIndex().getDependency(inpOp, ci);
          if (dep_ci != null) {
            d = dep_ci;
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
  public static class TableScanLineage extends DefaultLineage implements NodeProcessor {

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

      Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry(pctx.getConf()).iterator();
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
  public static class JoinLineage extends DefaultLineage implements NodeProcessor {
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
        Dependency dependency = ExprProcFactory.getExprDependency(lCtx, inpOp, expr);
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
        if (joinKeys.length < left
            || joinKeys[left].length == 0
            || joinKeys.length < right
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
  public static class LateralViewJoinLineage extends DefaultLineage implements NodeProcessor {
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
      ArrayList<ColumnInfo> cols = inpOp.getSchema().getSignature();
      lCtx.getIndex().copyPredicates(inpOp, op);

      if (inpOp instanceof SelectOperator) {
        isUdtfPath = false;
      }

      // Dirty hack!!
      // For the select path the columns are the ones at the beginning of the
      // current operators schema and for the udtf path the columns are
      // at the end of the operator schema.
      ArrayList<ColumnInfo> out_cols = op.getSchema().getSignature();
      int out_cols_size = out_cols.size();
      int cols_size = cols.size();
      int outColOffset = isUdtfPath ? out_cols_size - cols_size : 0;
      for (int cnt = 0; cnt < cols_size; cnt++) {
        ColumnInfo outCol = out_cols.get(outColOffset + cnt);
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
  public static class SelectLineage extends DefaultLineage implements NodeProcessor {
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
      ArrayList<ColumnInfo> col_infos = rs.getSignature();
      int cnt = 0;
      for(ExprNodeDesc expr : sop.getConf().getColList()) {
        Dependency dep = ExprProcFactory.getExprDependency(lctx, inpOp, expr);
        if (dep != null && dep.getExpr() == null && (dep.getBaseCols().isEmpty()
            || dep.getType() != LineageInfo.DependencyType.SIMPLE)) {
          dep.setExpr(ExprProcFactory.getExprString(rs, expr, lctx, inpOp, null));
        }
        lctx.getIndex().putDependency(sop, col_infos.get(cnt++), dep);
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
  public static class GroupByLineage extends DefaultLineage implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      LineageCtx lctx = (LineageCtx)procCtx;
      GroupByOperator gop = (GroupByOperator)nd;
      ArrayList<ColumnInfo> col_infos = gop.getSchema().getSignature();
      Operator<? extends OperatorDesc> inpOp = getParent(stack);
      lctx.getIndex().copyPredicates(inpOp, gop);
      int cnt = 0;

      for(ExprNodeDesc expr : gop.getConf().getKeys()) {
        lctx.getIndex().putDependency(gop, col_infos.get(cnt++),
            ExprProcFactory.getExprDependency(lctx, inpOp, expr));
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
        DependencyType new_type = LineageInfo.DependencyType.EXPRESSION;
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        LinkedHashSet<BaseColumnInfo> bci_set = new LinkedHashSet<BaseColumnInfo>();
        for(ExprNodeDesc expr : agg.getParameters()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          Dependency expr_dep = ExprProcFactory.getExprDependency(lctx, inpOp, expr);
          if (expr_dep != null && !expr_dep.getBaseCols().isEmpty()) {
            new_type = LineageCtx.getNewDependencyType(expr_dep.getType(), new_type);
            bci_set.addAll(expr_dep.getBaseCols());
            if (expr_dep.getType() == LineageInfo.DependencyType.SIMPLE) {
              BaseColumnInfo col = expr_dep.getBaseCols().iterator().next();
              Table t = col.getTabAlias().getTable();
              if (t != null) {
                sb.append(t.getDbName()).append(".").append(t.getTableName()).append(".");
              }
              sb.append(col.getColumn().getName());
            }
          }
          if (expr_dep == null || expr_dep.getBaseCols().isEmpty()
              || expr_dep.getType() != LineageInfo.DependencyType.SIMPLE) {
            sb.append(expr_dep != null && expr_dep.getExpr() != null ? expr_dep.getExpr() :
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

        // If the bci_set is empty, this means that the inputs to this
        // aggregate function were all constants (e.g. count(1)). In this case
        // the aggregate function is just dependent on all the tables that are in
        // the dependency list of the input operator.
        if (bci_set.isEmpty()) {
          Set<TableAliasInfo> tai_set = new LinkedHashSet<TableAliasInfo>();
          if (inpOp.getSchema() != null && inpOp.getSchema().getSignature() != null ) {
            for(ColumnInfo ci : inpOp.getSchema().getSignature()) {
              Dependency inp_dep = lctx.getIndex().getDependency(inpOp, ci);
              // The dependency can be null as some of the input cis may not have
              // been set in case of joins.
              if (inp_dep != null) {
                for(BaseColumnInfo bci : inp_dep.getBaseCols()) {
                  new_type = LineageCtx.getNewDependencyType(inp_dep.getType(), new_type);
                  tai_set.add(bci.getTabAlias());
                }
              }
            }
          }

          // Create the BaseColumnInfos and set them in the bci_set
          for(TableAliasInfo tai : tai_set) {
            BaseColumnInfo bci = new BaseColumnInfo();
            bci.setTabAlias(tai);
            // This is set to null to reflect that the dependency is not on any
            // particular column of the table.
            bci.setColumn(null);
            bci_set.add(bci);
          }
        }

        dep.setBaseCols(bci_set);
        dep.setType(new_type);
        lctx.getIndex().putDependency(gop, col_infos.get(cnt++), dep);
      }

      return null;
    }

  }

  /**
   * Union processor.
   * In this case we call mergeDependency as opposed to putDependency
   * in order to account for visits from different parents.
   */
  public static class UnionLineage extends DefaultLineage implements NodeProcessor {

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
      ArrayList<ColumnInfo> inp_cols = inpOp.getSchema().getSignature();
      int cnt = 0;
      for(ColumnInfo ci : rs.getSignature()) {
        Dependency inp_dep = lCtx.getIndex().getDependency(inpOp, inp_cols.get(cnt++));
        if (inp_dep != null) {
          lCtx.getIndex().mergeDependency(op, ci, inp_dep);
        }
      }
      return null;
    }
  }

  /**
   * ReduceSink processor.
   */
  public static class ReduceSinkLineage implements NodeProcessor {

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
        ArrayList<ColumnInfo> col_infos = rop.getSchema().getSignature();
        for(ExprNodeDesc expr : rop.getConf().getKeyCols()) {
          lCtx.getIndex().putDependency(rop, col_infos.get(cnt++),
              ExprProcFactory.getExprDependency(lCtx, inpOp, expr));
        }
        for(ExprNodeDesc expr : rop.getConf().getValueCols()) {
          lCtx.getIndex().putDependency(rop, col_infos.get(cnt++),
              ExprProcFactory.getExprDependency(lCtx, inpOp, expr));
        }
      } else {
        RowSchema schema = rop.getSchema();
        ReduceSinkDesc desc = rop.getConf();
        List<ExprNodeDesc> keyCols = desc.getKeyCols();
        ArrayList<String> keyColNames = desc.getOutputKeyColumnNames();
        for (int i = 0; i < keyCols.size(); i++) {
          // order-bys, joins
          ColumnInfo column = schema.getColumnInfo(Utilities.ReduceField.KEY + "." + keyColNames.get(i));
          if (column == null) {
            continue;   // key in values
          }
          lCtx.getIndex().putDependency(rop, column,
              ExprProcFactory.getExprDependency(lCtx, inpOp, keyCols.get(i)));
        }
        List<ExprNodeDesc> valCols = desc.getValueCols();
        ArrayList<String> valColNames = desc.getOutputValueColumnNames();
        for (int i = 0; i < valCols.size(); i++) {
          // todo: currently, bucketing,etc. makes RS differently with those for order-bys or joins
          ColumnInfo column = schema.getColumnInfo(valColNames.get(i));
          if (column == null) {
            // order-bys, joins
            column = schema.getColumnInfo(Utilities.ReduceField.VALUE + "." + valColNames.get(i));
          }
          lCtx.getIndex().putDependency(rop, column,
              ExprProcFactory.getExprDependency(lCtx, inpOp, valCols.get(i)));
        }
      }

      return null;
    }
  }

  /**
   * Filter processor.
   */
  public static class FilterLineage implements NodeProcessor {

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

      ArrayList<ColumnInfo> inp_cols = inpOp.getSchema().getSignature();
      int cnt = 0;
      for(ColumnInfo ci : rs.getSignature()) {
        lCtx.getIndex().putDependency(fop, ci,
            lCtx.getIndex().getDependency(inpOp, inp_cols.get(cnt++)));
      }

      return null;
    }
  }

  /**
   * Default processor. This basically passes the input dependencies as such
   * to the output dependencies.
   */
  public static class DefaultLineage implements NodeProcessor {

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
      ArrayList<ColumnInfo> inp_cols = inpOp.getSchema().getSignature();
      int cnt = 0;
      for(ColumnInfo ci : rs.getSignature()) {
        lCtx.getIndex().putDependency(op, ci,
            lCtx.getIndex().getDependency(inpOp, inp_cols.get(cnt++)));
      }
      return null;
    }
  }

  public static NodeProcessor getJoinProc() {
    return new JoinLineage();
  }

  public static NodeProcessor getLateralViewJoinProc() {
    return new LateralViewJoinLineage();
  }

  public static NodeProcessor getTSProc() {
    return new TableScanLineage();
  }

  public static NodeProcessor getTransformProc() {
    return new TransformLineage();
  }

  public static NodeProcessor getSelProc() {
    return new SelectLineage();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByLineage();
  }

  public static NodeProcessor getUnionProc() {
    return new UnionLineage();
  }

  public static NodeProcessor getReduceSinkProc() {
    return new ReduceSinkLineage();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultLineage();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterLineage();
  }
}
