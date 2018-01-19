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
package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.ppd.ExprWalkerInfo.ExprInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expression factory for predicate pushdown processing. Each processor
 * determines whether the expression is a possible candidate for predicate
 * pushdown optimization for the given operator
 */
public final class ExprWalkerProcFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ExprWalkerProcFactory.class.getName());

  /**
   * ColumnExprProcessor.
   *
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    /**
     * Converts the reference from child row resolver to current row resolver.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ExprNodeColumnDesc colref = (ExprNodeColumnDesc) nd;
      RowSchema toRS = ctx.getOp().getSchema();
      Operator<? extends OperatorDesc> op = ctx.getOp();
      ColumnInfo ci = toRS.getColumnInfo(colref.getColumn());
      String tabAlias = null;
      if (ci != null) {
        tabAlias = ci.getTabAlias();
      }

      ExprInfo colExprInfo = null;
      boolean isCandidate = true;
      if (op.getColumnExprMap() != null) {
        // replace the output expression with the input expression so that
        // parent op can understand this expression
        ExprNodeDesc exp = op.getColumnExprMap().get(colref.getColumn());
        // if the operator is a groupby and we are referencing the grouping
        // id column, we cannot push the predicate
        if (op instanceof GroupByOperator) {
          GroupByOperator groupBy = (GroupByOperator) op;
          if (groupBy.getConf().isGroupingSetsPresent()) {
            int groupingSetPlaceholderPos = groupBy.getConf().getKeys().size() - 1;
            if (colref.getColumn().equals(groupBy.getSchema().getColumnNames().get(groupingSetPlaceholderPos))) {
              exp = null;
            }
          }
        }
        if (exp == null) {
          // means that expression can't be pushed either because it is value in
          // group by
          colExprInfo = ctx.addOrGetExprInfo(colref);
          colExprInfo.isCandidate = false;
          return false;
        } else {
          if (exp instanceof ExprNodeGenericFuncDesc) {
            isCandidate = false;
          }
          if (exp instanceof ExprNodeColumnDesc && ci == null) {
            ExprNodeColumnDesc column = (ExprNodeColumnDesc)exp;
            tabAlias = column.getTabAlias();
          }
        }
        colExprInfo = ctx.addOrGetExprInfo(colref);
        colExprInfo.convertedExpr = exp;
        ExprInfo expInfo = ctx.addExprInfo(exp);
        expInfo.isCandidate = isCandidate;
        if (tabAlias != null) {
          expInfo.alias = tabAlias;
        } else {
          expInfo.alias = colExprInfo.alias;
        }
      } else {
        if (ci == null) {
          return false;
        }
        colExprInfo = ctx.addOrGetExprInfo(colref);
        if (tabAlias != null) {
          colExprInfo.alias = tabAlias;
        }
      }
      colExprInfo.isCandidate = isCandidate;
      return isCandidate;
    }

  }

  /**
   * FieldExprProcessor.
   *
   */
  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      ExprNodeFieldDesc expr = (ExprNodeFieldDesc) nd;

      assert (nd.getChildren().size() == 1);
      ExprNodeDesc ch = (ExprNodeDesc) nd.getChildren().get(0);
      ExprInfo chExprInfo = ctx.getExprInfo(ch);
      ExprNodeDesc newCh = chExprInfo != null ? chExprInfo.convertedExpr : null;
      if (newCh != null) {
        expr.setDesc(newCh);
        ch = newCh;
        chExprInfo = ctx.getExprInfo(ch);
      }

      boolean isCandidate;
      String chAlias;
      if (chExprInfo != null) {
        chAlias = chExprInfo.alias;
        isCandidate = chExprInfo.isCandidate;
      } else {
        chAlias = null;
        isCandidate = false;
      }
      // need to iterate through all children even if one is found to be not a
      // candidate
      // in case if the other children could be individually pushed up
      if (isCandidate && chAlias != null) {
        alias = chAlias;
      }

      ExprInfo exprInfo = ctx.addOrGetExprInfo(expr);
      if (alias != null) {
        exprInfo.alias = alias;
      }
      exprInfo.isCandidate = isCandidate;
      return isCandidate;
    }

  }

  /**
   * If all children are candidates and refer only to one table alias then this
   * expr is a candidate else it is not a candidate but its children could be
   * final candidates.
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) nd;

      if (!FunctionRegistry.isConsistentWithinQuery(expr.getGenericUDF())) {
        // this GenericUDF can't be pushed down
        ExprInfo exprInfo = ctx.addOrGetExprInfo(expr);
        exprInfo.isCandidate = false;
        ctx.setDeterministic(false);
        return false;
      }

      boolean isCandidate = true;
      for (int i = 0; i < nd.getChildren().size(); i++) {
        ExprNodeDesc ch = (ExprNodeDesc) nd.getChildren().get(i);
        ExprInfo chExprInfo = ctx.getExprInfo(ch);
        ExprNodeDesc newCh = chExprInfo != null ? chExprInfo.convertedExpr : null;
        if (newCh != null) {
          expr.getChildren().set(i, newCh);
          ch = newCh;
          chExprInfo = ctx.getExprInfo(ch);
        }

        String chAlias;
        if (chExprInfo != null) {
          chAlias = chExprInfo.alias;
          isCandidate = isCandidate && chExprInfo.isCandidate;
        } else {
          chAlias = null;
          isCandidate = false;
        }
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
      ExprInfo exprInfo = ctx.addOrGetExprInfo(expr);
      if (alias != null) {
        exprInfo.alias = alias;
      }
      exprInfo.isCandidate = isCandidate;
      return isCandidate;
    }

  }

  /**
   * For constants and null expressions.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ExprInfo exprInfo = ctx.addOrGetExprInfo((ExprNodeDesc) nd);
      exprInfo.isCandidate = true;
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
    Operator<? extends OperatorDesc> op, ExprNodeDesc pred)
    throws SemanticException {
    List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
    preds.add(pred);
    return extractPushdownPreds(opContext, op, preds);
  }

  /**
   * Extracts pushdown predicates from the given list of predicate expression.
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
    Operator<? extends OperatorDesc> op, List<ExprNodeDesc> preds)
    throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprWalkerInfo exprContext = new ExprWalkerInfo(op);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(new TypeRule(ExprNodeColumnDesc.class), getColumnProcessor());
    exprRules.put(new TypeRule(ExprNodeFieldDesc.class), getFieldProcessor());
    exprRules.put(new TypeRule(ExprNodeGenericFuncDesc.class), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, exprContext);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    List<ExprNodeDesc> clonedPreds = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc node : preds) {
      ExprNodeDesc clone = node.clone();
      clonedPreds.add(clone);
      exprContext.getNewToOldExprMap().put(clone, node);
    }
    startNodes.addAll(clonedPreds);

    egw.startWalking(startNodes, null);

    HiveConf conf = opContext.getParseContext().getConf();
    // check the root expression for final candidates
    for (ExprNodeDesc pred : clonedPreds) {
      extractFinalCandidates(pred, exprContext, conf);
    }
    return exprContext;
  }

  /**
   * Walks through the top AND nodes and determine which of them are final
   * candidates.
   */
  private static void extractFinalCandidates(ExprNodeDesc expr,
      ExprWalkerInfo ctx, HiveConf conf) {
    // We decompose an AND expression into its parts before checking if the
    // entire expression is a candidate because each part may be a candidate
    // for replicating transitively over an equijoin condition.
    if (FunctionRegistry.isOpAnd(expr)) {
      // If the operator is AND, we need to determine if any of the children are
      // final candidates.

      // For the children, we populate the NewToOldExprMap to keep track of
      // the original condition before rewriting it for this operator
      assert ctx.getNewToOldExprMap().containsKey(expr);
      for (int i = 0; i < expr.getChildren().size(); i++) {
        ctx.getNewToOldExprMap().put(
            expr.getChildren().get(i),
            ctx.getNewToOldExprMap().get(expr).getChildren().get(i));
        extractFinalCandidates(expr.getChildren().get(i),
            ctx, conf);
      }
      return;
    }

    ExprInfo exprInfo = ctx.getExprInfo(expr);
    if (exprInfo != null && exprInfo.isCandidate) {
      String alias = exprInfo.alias;
      if ((alias == null) && (exprInfo.convertedExpr != null)) {
    	ExprInfo convertedExprInfo = ctx.getExprInfo(exprInfo.convertedExpr);
    	if (convertedExprInfo != null) {
    		alias = convertedExprInfo.alias;
    	}
      }
      ctx.addFinalCandidate(alias, exprInfo.convertedExpr != null ?
              exprInfo.convertedExpr : expr);
      return;
    } else if (!FunctionRegistry.isOpAnd(expr) &&
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEPPDREMOVEDUPLICATEFILTERS)) {
      ctx.addNonFinalCandidate(exprInfo != null ? exprInfo.alias : null, expr);
    }
  }

  private ExprWalkerProcFactory() {
    // prevent instantiation
  }
}
