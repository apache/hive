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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.BaseColumnInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.Dependency;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;

/**
 * Expression processor factory for lineage. Each processor is responsible to
 * create the leaf level column info objects that the expression depends upon
 * and also generates a string representation of the expression.
 */
public class ExprProcFactory {

  /**
   * Processor for column expressions.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeColumnDesc cd = (ExprNodeColumnDesc) nd;
      ExprProcCtx epc = (ExprProcCtx) procCtx;

      // assert that the input operator is not null as there are no
      // exprs associated with table scans.
      assert (epc.getInputOperator() != null);

      ColumnInfo inp_ci = null;
      for (ColumnInfo tmp_ci : epc.getInputOperator().getSchema()
          .getSignature()) {
        if (tmp_ci.getInternalName().equals(cd.getColumn())) {
          inp_ci = tmp_ci;
          break;
        }
      }

      // Insert the dependencies of inp_ci to that of the current operator, ci
      LineageCtx lc = epc.getLineageCtx();
      Dependency dep = lc.getIndex().getDependency(epc.getInputOperator(), inp_ci);

      return dep;
    }

  }

  /**
   * Processor for any function or field expression.
   */
  public static class GenericExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      assert (nd instanceof ExprNodeGenericFuncDesc || nd instanceof ExprNodeFieldDesc);

      // Concatenate the dependencies of all the children to compute the new
      // dependency.
      Dependency dep = new Dependency();

      LinkedHashSet<BaseColumnInfo> bci_set = new LinkedHashSet<BaseColumnInfo>();
      LineageInfo.DependencyType new_type = LineageInfo.DependencyType.EXPRESSION;

      for (Object child : nodeOutputs) {
        if (child == null) {
          continue;
        }

        Dependency child_dep = (Dependency) child;
        new_type = LineageCtx.getNewDependencyType(child_dep.getType(), new_type);
        bci_set.addAll(child_dep.getBaseCols());
      }

      dep.setBaseCols(new ArrayList<BaseColumnInfo>(bci_set));
      dep.setType(new_type);

      return dep;
    }

  }

  /**
   * Processor for constants and null expressions. For such expressions the
   * processor simply returns a null dependency vector.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      assert (nd instanceof ExprNodeConstantDesc || nd instanceof ExprNodeNullDesc);

      // Create a dependency that has no basecols
      Dependency dep = new Dependency();
      dep.setType(LineageInfo.DependencyType.SIMPLE);
      dep.setBaseCols(new ArrayList<BaseColumnInfo>());
      return dep;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericExprProcessor();
  }

  public static NodeProcessor getFieldProcessor() {
    return new GenericExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  /**
   * Gets the expression dependencies for the expression.
   *
   * @param lctx
   *          The lineage context containing the input operators dependencies.
   * @param inpOp
   *          The input operator to the current operator.
   * @param expr
   *          The expression that is being processed.
   * @throws SemanticException
   */
  public static Dependency getExprDependency(LineageCtx lctx,
      Operator<? extends Serializable> inpOp, ExprNodeDesc expr)
      throws SemanticException {

    // Create the walker, the rules dispatcher and the context.
    ExprProcCtx exprCtx = new ExprProcCtx(lctx, inpOp);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(
        new RuleRegExp("R1", ExprNodeColumnDesc.class.getName() + "%"),
        getColumnProcessor());
    exprRules.put(
        new RuleRegExp("R2", ExprNodeFieldDesc.class.getName() + "%"),
        getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", ExprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, exprCtx);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(expr);

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    return (Dependency)outputMap.get(expr);
  }
}
