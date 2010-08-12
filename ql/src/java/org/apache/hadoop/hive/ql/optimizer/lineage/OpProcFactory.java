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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DependencyType;
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
import org.apache.hadoop.hive.ql.plan.JoinDesc;
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
  protected static Operator<? extends Serializable> getParent(Stack<Node> stack) {
    return (Operator<? extends Serializable>)Utils.getNthAncestor(stack, 1);
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
      Operator<? extends Serializable> op = (Operator<? extends Serializable>)nd;
      Operator<? extends Serializable> inpOp = getParent(stack);

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
          col_set.addAll(d.getBaseCols());
        }
      }

      dep.setType(new_type);
      dep.setBaseCols(new ArrayList<BaseColumnInfo>(col_set));

      // This dependency is then set for all the colinfos of the script operator
      for(ColumnInfo ci : op.getSchema().getSignature()) {
        lCtx.getIndex().putDependency(op, ci, dep);
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
      org.apache.hadoop.hive.ql.metadata.Table t = pctx.getTopToTable().get(top);
      Table tab = t.getTTable();

      // Generate the mappings
      RowSchema rs = top.getSchema();
      List<FieldSchema> cols = t.getAllCols();
      Map<String, FieldSchema> fieldSchemaMap = new HashMap<String, FieldSchema>();
      for(FieldSchema col : cols) {
        fieldSchemaMap.put(col.getName(), col);
      }
      
      Iterator<VirtualColumn> vcs = VirtualColumn.registry.values().iterator();
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
        // TODO: Find out how to get the expression here.
        dep.setExpr(null);
        dep.setBaseCols(new ArrayList<BaseColumnInfo>());
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

      // Assert that there is atleast one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      JoinOperator op = (JoinOperator)nd;
      JoinDesc jd = op.getConf();

      // The input operator to the join is always a reduce sink operator
      ReduceSinkOperator inpOp = (ReduceSinkOperator)getParent(stack);
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
        lCtx.getIndex().mergeDependency(op, ci,
            ExprProcFactory.getExprDependency(lCtx, inpOp, expr));
      }

      return null;
    }

  }

  /**
   * Processor for Join Operator.
   */
  public static class LateralViewJoinLineage extends DefaultLineage implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // Assert that there is atleast one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      LateralViewJoinOperator op = (LateralViewJoinOperator)nd;
      boolean isUdtfPath = true;
      Operator<? extends Serializable> inpOp = getParent(stack);
      ArrayList<ColumnInfo> cols = inpOp.getSchema().getSignature();

      if (inpOp instanceof SelectOperator) {
        isUdtfPath = false;
      }

      // Dirty hack!!
      // For the select path the columns are the ones at the end of the
      // current operators schema and for the udtf path the columns are
      // at the beginning of the operator schema.
      ArrayList<ColumnInfo> out_cols = op.getSchema().getSignature();
      int out_cols_size = out_cols.size();
      int cols_size = cols.size();
      if (isUdtfPath) {
        int cnt = 0;
        while (cnt < cols_size) {
          lCtx.getIndex().mergeDependency(op, out_cols.get(cnt),
              lCtx.getIndex().getDependency(inpOp, cols.get(cnt)));
          cnt++;
        }
      }
      else {
        int cnt = cols_size - 1;
        while (cnt >= 0) {
          lCtx.getIndex().mergeDependency(op, out_cols.get(out_cols_size - cols_size + cnt),
              lCtx.getIndex().getDependency(inpOp, cols.get(cnt)));
          cnt--;
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

      ArrayList<ColumnInfo> col_infos = sop.getSchema().getSignature();
      int cnt = 0;
      for(ExprNodeDesc expr : sop.getConf().getColList()) {
        lctx.getIndex().putDependency(sop, col_infos.get(cnt++),
            ExprProcFactory.getExprDependency(lctx, getParent(stack), expr));
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
      Operator<? extends Serializable> inpOp = getParent(stack);
      int cnt = 0;

      for(ExprNodeDesc expr : gop.getConf().getKeys()) {
        lctx.getIndex().putDependency(gop, col_infos.get(cnt++),
            ExprProcFactory.getExprDependency(lctx, inpOp, expr));
      }

      for(AggregationDesc agg : gop.getConf().getAggregators()) {
        // Concatenate the dependencies of all the parameters to
        // create the new dependency
        Dependency dep = new Dependency();
        DependencyType new_type = LineageInfo.DependencyType.EXPRESSION;
        // TODO: Get the actual string here.
        dep.setExpr(null);
        LinkedHashSet<BaseColumnInfo> bci_set = new LinkedHashSet<BaseColumnInfo>();
        for(ExprNodeDesc expr : agg.getParameters()) {
          Dependency expr_dep = ExprProcFactory.getExprDependency(lctx, inpOp, expr);
          if (expr_dep != null) {
            new_type = LineageCtx.getNewDependencyType(expr_dep.getType(), new_type);
            bci_set.addAll(expr_dep.getBaseCols());
          }
        }

        // If the bci_set is empty, this means that the inputs to this
        // aggregate function were all constants (e.g. count(1)). In this case
        // the aggregate function is just dependent on all the tables that are in
        // the dependency list of the input operator.
        if (bci_set.isEmpty()) {
          Set<TableAliasInfo> tai_set = new LinkedHashSet<TableAliasInfo>();
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

        dep.setBaseCols(new ArrayList<BaseColumnInfo>(bci_set));
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

    protected static final Log LOG = LogFactory.getLog(OpProcFactory.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is atleast one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      Operator<? extends Serializable> op = (Operator<? extends Serializable>)nd;

      // Get the row schema of the input operator.
      // The row schema of the parent operator
      Operator<? extends Serializable> inpOp = getParent(stack);
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

    protected static final Log LOG = LogFactory.getLog(OpProcFactory.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is atleast one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      ReduceSinkOperator rop = (ReduceSinkOperator)nd;

      ArrayList<ColumnInfo> col_infos = rop.getSchema().getSignature();
      Operator<? extends Serializable> inpOp = getParent(stack);
      int cnt = 0;

      // The keys are included only in case the reduce sink feeds into
      // a group by operator through a chain of forward operators
      Operator<? extends Serializable> op = rop.getChildOperators().get(0);
      while (op instanceof ForwardOperator) {
        op = op.getChildOperators().get(0);
      }

      if (op instanceof GroupByOperator) {
        for(ExprNodeDesc expr : rop.getConf().getKeyCols()) {
          lCtx.getIndex().putDependency(rop, col_infos.get(cnt++),
              ExprProcFactory.getExprDependency(lCtx, inpOp, expr));
        }
      }

      for(ExprNodeDesc expr : rop.getConf().getValueCols()) {
        lCtx.getIndex().putDependency(rop, col_infos.get(cnt++),
            ExprProcFactory.getExprDependency(lCtx, inpOp, expr));
      }

      return null;
    }
  }

  /**
   * Default processor. This basically passes the input dependencies as such
   * to the output dependencies.
   */
  public static class DefaultLineage implements NodeProcessor {

    protected static final Log LOG = LogFactory.getLog(OpProcFactory.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Assert that there is atleast one item in the stack. This should never
      // be called for leafs.
      assert(!stack.isEmpty());

      // LineageCtx
      LineageCtx lCtx = (LineageCtx) procCtx;
      Operator<? extends Serializable> op = (Operator<? extends Serializable>)nd;

      // Get the row schema of the input operator.
      // The row schema of the parent operator
      Operator<? extends Serializable> inpOp = getParent(stack);
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

}
