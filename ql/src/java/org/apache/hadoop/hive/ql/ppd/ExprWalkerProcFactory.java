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
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;

/**
 * Expression factory for predicate pushdown processing. Each processor
 * determines whether the expression is a possible candidate for predicate
 * pushdown optimization for the given operator
 */
public class ExprWalkerProcFactory {

  public static class ColumnExprProcessor implements NodeProcessor {

    /**
     * Converts the reference from child row resolver to current row resolver
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      exprNodeColumnDesc colref = (exprNodeColumnDesc) nd;
      RowResolver toRR = ctx.getToRR();
      Operator<? extends Serializable> op = ctx.getOp();
      String[] colAlias = toRR.reverseLookup(colref.getColumn());

      if (op.getColumnExprMap() != null) {
        // replace the output expression with the input expression so that
        // parent op can understand this expression
        exprNodeDesc exp = op.getColumnExprMap().get(colref.getColumn());
        if (exp == null) {
          // means that expression can't be pushed either because it is value in
          // group by
          ctx.setIsCandidate(colref, false);
          return false;
        }
        ctx.addConvertedNode(colref, exp);
        ctx.setIsCandidate(exp, true);
        ctx.addAlias(exp, colAlias[0]);
      } else {
        if (colAlias == null) {
          assert false;
        }
        ctx.addAlias(colref, colAlias[0]);
      }
      ctx.setIsCandidate(colref, true);
      return true;
    }

  }

  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      exprNodeFieldDesc expr = (exprNodeFieldDesc) nd;

      boolean isCandidate = true;
      assert (nd.getChildren().size() == 1);
      exprNodeDesc ch = (exprNodeDesc) nd.getChildren().get(0);
      exprNodeDesc newCh = ctx.getConvertedNode(ch);
      if (newCh != null) {
        expr.setDesc(newCh);
        ch = newCh;
      }
      String chAlias = ctx.getAlias(ch);

      isCandidate = isCandidate && ctx.isCandidate(ch);
      // need to iterate through all children even if one is found to be not a
      // candidate
      // in case if the other children could be individually pushed up
      if (isCandidate && chAlias != null) {
        if (alias == null) {
          alias = chAlias;
        } else if (!chAlias.equalsIgnoreCase(alias)) {
          isCandidate = false;
        }
      }

      ctx.addAlias(expr, alias);
      ctx.setIsCandidate(expr, isCandidate);
      return isCandidate;
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
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      exprNodeGenericFuncDesc expr = (exprNodeGenericFuncDesc) nd;

      if (!FunctionRegistry.isDeterministic(expr.getGenericUDF())) {
        // this GenericUDF can't be pushed down
        ctx.setIsCandidate(expr, false);
        ctx.setDeterministic(false);
        return false;
      }

      boolean isCandidate = true;
      for (int i = 0; i < nd.getChildren().size(); i++) {
        exprNodeDesc ch = (exprNodeDesc) nd.getChildren().get(i);
        exprNodeDesc newCh = ctx.getConvertedNode(ch);
        if (newCh != null) {
          expr.getChildExprs().set(i, newCh);
          ch = newCh;
        }
        String chAlias = ctx.getAlias(ch);

        isCandidate = isCandidate && ctx.isCandidate(ch);
        // need to iterate through all children even if one is found to be not a
        // candidate
        // in case if the other children could be individually pushed up
        if (isCandidate && chAlias != null) {
          if (alias == null) {
            alias = chAlias;
          } else if (!chAlias.equalsIgnoreCase(alias)) {
            isCandidate = false;
          }
        }

        if (!isCandidate) {
          break;
        }
      }
      ctx.addAlias(expr, alias);
      ctx.setIsCandidate(expr, isCandidate);
      return isCandidate;
    }

  }

  /**
   * For constants and null expressions
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ctx.setIsCandidate((exprNodeDesc) nd, true);
      return true;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  private static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
      Operator<? extends Serializable> op, exprNodeDesc pred)
      throws SemanticException {
    List<exprNodeDesc> preds = new ArrayList<exprNodeDesc>();
    preds.add(pred);
    return extractPushdownPreds(opContext, op, preds);
  }

  /**
   * Extracts pushdown predicates from the given list of predicate expression
   * 
   * @param opContext
   *          operator context used for resolving column references
   * @param op
   *          operator of the predicates being processed
   * @param preds
   * @return The expression walker information
   * @throws SemanticException
   */
  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
      Operator<? extends Serializable> op, List<exprNodeDesc> preds)
      throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprWalkerInfo exprContext = new ExprWalkerInfo(op, opContext
        .getRowResolver(op));

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(
        new RuleRegExp("R1", exprNodeColumnDesc.class.getName() + "%"),
        getColumnProcessor());
    exprRules.put(
        new RuleRegExp("R2", exprNodeFieldDesc.class.getName() + "%"),
        getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", exprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, exprContext);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    List<exprNodeDesc> clonedPreds = new ArrayList<exprNodeDesc>();
    for (exprNodeDesc node : preds) {
      clonedPreds.add(node.clone());
    }
    startNodes.addAll(clonedPreds);

    egw.startWalking(startNodes, null);

    // check the root expression for final candidates
    for (exprNodeDesc pred : clonedPreds) {
      extractFinalCandidates(pred, exprContext);
    }
    return exprContext;
  }

  /**
   * Walks through the top AND nodes and determine which of them are final
   * candidates
   */
  private static void extractFinalCandidates(exprNodeDesc expr,
      ExprWalkerInfo ctx) {
    if (ctx.isCandidate(expr)) {
      ctx.addFinalCandidate(expr);
      return;
    }

    if (FunctionRegistry.isOpAnd(expr)) {
      // If the operator is AND, we need to determine if any of the children are
      // final candidates.
      for (Node ch : expr.getChildren()) {
        extractFinalCandidates((exprNodeDesc) ch, ctx);
      }
    }

  }
}
