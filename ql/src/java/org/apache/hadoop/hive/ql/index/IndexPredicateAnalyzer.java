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
package org.apache.hadoop.hive.ql.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;

/**
 * IndexPredicateAnalyzer decomposes predicates, separating the parts
 * which can be satisfied by an index from the parts which cannot.
 * Currently, it only supports pure conjunctions over binary expressions
 * comparing a column reference with a constant value.  It is assumed
 * that all column aliases encountered refer to the same table.
 */
public class IndexPredicateAnalyzer
{
  private Set<String> udfNames;

  private Set<String> allowedColumnNames;
  
  public IndexPredicateAnalyzer() {
    udfNames = new HashSet<String>();
  }

  /**
   * Registers a comparison operator as one which can be satisfied
   * by an index search.  Unless this is called, analyzePredicate
   * will never find any indexable conditions.
   *
   * @param udfName name of comparison operator as returned
   * by either {@link GenericUDFBridge#getUdfName} (for simple UDF's)
   * or udf.getClass().getName() (for generic UDF's).
   */
  public void addComparisonOp(String udfName) {
    udfNames.add(udfName);
  }

  /**
   * Clears the set of column names allowed in comparisons.  (Initially, all
   * column names are allowed.)
   */
  public void clearAllowedColumnNames() {
    allowedColumnNames = new HashSet<String>();
  }

  /**
   * Adds a column name to the set of column names allowed.
   *
   * @param columnName name of column to be allowed
   */
  public void allowColumnName(String columnName) {
    if (allowedColumnNames == null) {
      clearAllowedColumnNames();
    }
    allowedColumnNames.add(columnName);
  }

  /**
   * Analyzes a predicate.
   *
   * @param predicate predicate to be analyzed
   *
   * @param searchConditions receives conditions produced by analysis
   *
   * @return residual predicate which could not be translated to
   * searchConditions
   */
  public ExprNodeDesc analyzePredicate(
    ExprNodeDesc predicate,
    final List<IndexSearchCondition> searchConditions) {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    NodeProcessor nodeProcessor = new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
        NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {

        // We can only push down stuff which appears as part of
        // a pure conjunction:  reject OR, CASE, etc.
        for (Node ancestor : stack) {
          if (nd == ancestor) {
            break;
          }
          if (!FunctionRegistry.isOpAnd((ExprNodeDesc) ancestor)) {
            return nd;
          }
        }

        return analyzeExpr((ExprNodeDesc) nd, searchConditions, nodeOutputs);
      }
    };

    Dispatcher disp = new DefaultRuleDispatcher(
      nodeProcessor, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(predicate);
    HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
    try {
      ogw.startWalking(topNodes, nodeOutput);
    } catch (SemanticException ex) {
      throw new RuntimeException(ex);
    }
    ExprNodeDesc residualPredicate = (ExprNodeDesc) nodeOutput.get(predicate);
    return residualPredicate;
  }

  private ExprNodeDesc analyzeExpr(
    ExprNodeDesc expr,
    List<IndexSearchCondition> searchConditions,
    Object... nodeOutputs) {

    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return expr;
    }
    if (FunctionRegistry.isOpAnd(expr)) {
      assert(nodeOutputs.length == 2);
      ExprNodeDesc residual1 = (ExprNodeDesc) nodeOutputs[0];
      ExprNodeDesc residual2 = (ExprNodeDesc) nodeOutputs[1];
      if (residual1 == null) {
        return residual2;
      }
      if (residual2 == null) {
        return residual1;
      }
      List<ExprNodeDesc> residuals = new ArrayList<ExprNodeDesc>();
      residuals.add(residual1);
      residuals.add(residual2);
      return new ExprNodeGenericFuncDesc(
        TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(),
        residuals);
    }

    String udfName;
    ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) expr;
    if (funcDesc.getGenericUDF() instanceof GenericUDFBridge) {
      GenericUDFBridge func = (GenericUDFBridge) funcDesc.getGenericUDF();
      udfName = func.getUdfName();
    } else {
      udfName = funcDesc.getGenericUDF().getClass().getName();
    }
    if (!udfNames.contains(udfName)) {
      return expr;
    }

    ExprNodeDesc child1 = (ExprNodeDesc) nodeOutputs[0];
    ExprNodeDesc child2 = (ExprNodeDesc) nodeOutputs[1];
    ExprNodeColumnDesc columnDesc = null;
    ExprNodeConstantDesc constantDesc = null;
    if ((child1 instanceof ExprNodeColumnDesc)
      && (child2 instanceof ExprNodeConstantDesc)) {
      // COL <op> CONSTANT
      columnDesc = (ExprNodeColumnDesc) child1;
      constantDesc = (ExprNodeConstantDesc) child2;
    } else if ((child2 instanceof ExprNodeColumnDesc)
      && (child1 instanceof ExprNodeConstantDesc)) {
      // CONSTANT <op> COL
      columnDesc = (ExprNodeColumnDesc) child2;
      constantDesc = (ExprNodeConstantDesc) child1;
    }
    if (columnDesc == null) {
      return expr;
    }
    if (allowedColumnNames != null) {
      if (!allowedColumnNames.contains(columnDesc.getColumn())) {
        return expr;
      }
    }
    searchConditions.add(
      new IndexSearchCondition(
        columnDesc,
        udfName,
        constantDesc,
        expr));

    // we converted the expression to a search condition, so
    // remove it from the residual predicate
    return null;
  }

  /**
   * Translates search conditions back to ExprNodeDesc form (as
   * a left-deep conjunction).
   *
   * @param searchConditions (typically produced by analyzePredicate)
   *
   * @return ExprNodeDesc form of search conditions
   */
  public ExprNodeDesc translateSearchConditions(
    List<IndexSearchCondition> searchConditions) {

    ExprNodeDesc expr = null;
    for (IndexSearchCondition searchCondition : searchConditions) {
      if (expr == null) {
        expr = searchCondition.getComparisonExpr();
        continue;
      }
      List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
      children.add(expr);
      children.add(searchCondition.getComparisonExpr());
      expr = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.booleanTypeInfo,
        FunctionRegistry.getGenericUDFForAnd(),
        children);
    }
    return expr;
  }
}
