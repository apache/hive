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

package org.apache.hadoop.hive.ql.optimizer.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * RewriteQueryUsingAggregateIndexCtx class stores the
 * context for the {@link RewriteQueryUsingAggregateIndex}
 * used to rewrite operator plan with index table instead of base table.
 */

public final class RewriteQueryUsingAggregateIndexCtx  implements NodeProcessorCtx {

  private RewriteQueryUsingAggregateIndexCtx(ParseContext parseContext, Hive hiveDb,
      String indexTableName, String baseTableName, String aggregateFunction){
    this.parseContext = parseContext;
    this.hiveDb = hiveDb;
    this.indexTableName = indexTableName;
    this.baseTableName = baseTableName;
    this.aggregateFunction = aggregateFunction;
    this.opc = parseContext.getOpParseCtx();
  }

  public static RewriteQueryUsingAggregateIndexCtx getInstance(ParseContext parseContext,
      Hive hiveDb, String indexTableName, String baseTableName, String aggregateFunction){
    return new RewriteQueryUsingAggregateIndexCtx(
        parseContext, hiveDb, indexTableName, baseTableName, aggregateFunction);
  }


  private Map<Operator<? extends Serializable>, OpParseContext> opc =
    new LinkedHashMap<Operator<? extends Serializable>, OpParseContext>();
  private final Hive hiveDb;
  private final ParseContext parseContext;
  //We need the GenericUDAFEvaluator for GenericUDAF function "sum"
  private GenericUDAFEvaluator eval = null;
  private final String indexTableName;
  private final String baseTableName;
  private final String aggregateFunction;
  private ExprNodeColumnDesc aggrExprNode = null;

  public Map<Operator<? extends Serializable>, OpParseContext> getOpc() {
    return opc;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }

  public Hive getHiveDb() {
    return hiveDb;
  }

  public String getIndexName() {
     return indexTableName;
  }

  public GenericUDAFEvaluator getEval() {
    return eval;
  }

  public void setEval(GenericUDAFEvaluator eval) {
    this.eval = eval;
  }

  public void setAggrExprNode(ExprNodeColumnDesc aggrExprNode) {
    this.aggrExprNode = aggrExprNode;
  }

  public ExprNodeColumnDesc getAggrExprNode() {
    return aggrExprNode;
  }

 /**
  * Walk the original operator tree using the {@link DefaultGraphWalker} using the rules.
  * Each of the rules invoke respective methods from the {@link RewriteQueryUsingAggregateIndex}
  * to rewrite the original query using aggregate index.
  *
  * @param topOp
  * @throws SemanticException
  */
  public void invokeRewriteQueryProc(
      Operator<? extends Serializable> topOp) throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // replace scan operator containing original table with index table
    opRules.put(new RuleRegExp("R1", "TS%"),
        RewriteQueryUsingAggregateIndex.getReplaceTableScanProc());
    //rule that replaces index key selection with
    //sum(`_count_of_indexed_column`) function in original query
    opRules.put(new RuleRegExp("R2", "SEL%"),
        RewriteQueryUsingAggregateIndex.getNewQuerySelectSchemaProc());
    //Manipulates the ExprNodeDesc from GroupByOperator aggregation list
    opRules.put(new RuleRegExp("R3", "GBY%"),
        RewriteQueryUsingAggregateIndex.getNewQueryGroupbySchemaProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);
    ogw.startWalking(topNodes, null);
  }

 /**
  * Default procedure for {@link DefaultRuleDispatcher}.
  * @return
  */
  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  public String getBaseTableName() {
    return baseTableName;
  }

  public String getAggregateFunction() {
    return aggregateFunction;
  }
}
