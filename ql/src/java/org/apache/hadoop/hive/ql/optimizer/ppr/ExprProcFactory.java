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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
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
 * Expression processor factory for partition pruning. Each processor tries to
 * convert the expression subtree into a partition pruning expression. This
 * expression is then used to figure out whether a particular partition should
 * be scanned or not.
 */
public class ExprProcFactory {

  /**
   * Processor for column expressions.
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeDesc newcd = null;
      ExprNodeColumnDesc cd = (ExprNodeColumnDesc) nd;
      ExprProcCtx epc = (ExprProcCtx) procCtx;
      if (cd.getTabAlias().equalsIgnoreCase(epc.getTabAlias())
          && cd.getIsParititonCol()) {
        newcd = cd.clone();
      } else {
        newcd = new ExprNodeConstantDesc(cd.getTypeInfo(), null);
        epc.setHasNonPartCols(true);
      }

      return newcd;
    }

  }

  /**
   * If all children are candidates and refer only to one table alias then this
   * expr is a candidate else it is not a candidate but its children could be
   * final candidates
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeDesc newfd = null;
      ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nd;

      boolean unknown = false;

      if (FunctionRegistry.isOpAndOrNot(fd)) {
        // do nothing because "And" and "Or" and "Not" supports null value
        // evaluation
        // NOTE: In the future all UDFs that treats null value as UNKNOWN (both
        // in parameters and return
        // values) should derive from a common base class UDFNullAsUnknown, so
        // instead of listing the classes
        // here we would test whether a class is derived from that base class.
      } else if (!FunctionRegistry.isDeterministic(fd.getGenericUDF())) {
        // If it's a non-deterministic UDF, set unknown to true
        unknown = true;
      } else {
        // If any child is null, set unknown to true
        for (Object child : nodeOutputs) {
          ExprNodeDesc child_nd = (ExprNodeDesc) child;
          if (child_nd instanceof ExprNodeConstantDesc
              && ((ExprNodeConstantDesc) child_nd).getValue() == null) {
            unknown = true;
          }
        }
      }

      if (unknown) {
        newfd = new ExprNodeConstantDesc(fd.getTypeInfo(), null);
      } else {
        // Create the list of children
        ArrayList<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
        for (Object child : nodeOutputs) {
          children.add((ExprNodeDesc) child);
        }
        // Create a copy of the function descriptor
        newfd = new ExprNodeGenericFuncDesc(fd.getTypeInfo(), fd
            .getGenericUDF(), children);
      }

      return newfd;
    }

  }

  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ExprNodeFieldDesc fnd = (ExprNodeFieldDesc) nd;
      boolean unknown = false;
      int idx = 0;
      ExprNodeDesc left_nd = null;
      for (Object child : nodeOutputs) {
        ExprNodeDesc child_nd = (ExprNodeDesc) child;
        if (child_nd instanceof ExprNodeConstantDesc
            && ((ExprNodeConstantDesc) child_nd).getValue() == null) {
          unknown = true;
        }
        left_nd = child_nd;
      }

      assert (idx == 0);

      ExprNodeDesc newnd = null;
      if (unknown) {
        newnd = new ExprNodeConstantDesc(fnd.getTypeInfo(), null);
      } else {
        newnd = new ExprNodeFieldDesc(fnd.getTypeInfo(), left_nd, fnd
            .getFieldName(), fnd.getIsList());
      }
      return newnd;
    }

  }

  /**
   * Processor for constants and null expressions. For such expressions the
   * processor simply clones the exprNodeDesc and returns it.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      if (nd instanceof ExprNodeConstantDesc) {
        return ((ExprNodeConstantDesc) nd).clone();
      } else if (nd instanceof ExprNodeNullDesc) {
        return ((ExprNodeNullDesc) nd).clone();
      }

      assert (false);
      return null;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  /**
   * Generates the partition pruner for the expression tree
   * 
   * @param tabAlias
   *          The table alias of the partition table that is being considered
   *          for pruning
   * @param pred
   *          The predicate from which the partition pruner needs to be
   *          generated
   * @return hasNonPartCols returns true/false depending upon whether this pred
   *         has a non partition column
   * @throws SemanticException
   */
  public static ExprNodeDesc genPruner(String tabAlias, ExprNodeDesc pred,
      boolean hasNonPartCols) throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprProcCtx pprCtx = new ExprProcCtx(tabAlias);

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
    exprRules.put(new RuleRegExp("R5", ExprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, pprCtx);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    hasNonPartCols = pprCtx.getHasNonPartCols();

    // Get the exprNodeDesc corresponding to the first start node;
    return (ExprNodeDesc) outputMap.get(pred);
  }

}
