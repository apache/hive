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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * RewriteCanApplyCtx class stores the context for the {@link RewriteCanApplyProcFactory}
 * to determine if any index can be used and if the input query
 * meets all the criteria for rewrite optimization.
 */
public final class RewriteCanApplyCtx implements NodeProcessorCtx {

  private static final Log LOG = LogFactory.getLog(RewriteCanApplyCtx.class.getName());

  private RewriteCanApplyCtx(ParseContext parseContext) {
    this.parseContext = parseContext;
  }

  public static RewriteCanApplyCtx getInstance(ParseContext parseContext){
    return new RewriteCanApplyCtx(parseContext);
  }

  // Rewrite Variables
  private boolean selClauseColsFetchException = false;
  private boolean queryHasGroupBy = false;
  private boolean aggFuncIsNotCount = false;
  private boolean aggParameterException = false;

  //The most important, indexKey
  private String indexKey;

  private final ParseContext parseContext;
  private String alias;
  private String baseTableName;
  private String indexTableName;
  private String aggFunction;
  
  private TableScanOperator tableScanOperator;
  private List<SelectOperator> selectOperators;
  private List<GroupByOperator> groupByOperators;

  void resetCanApplyCtx(){
    setQueryHasGroupBy(false);
    setAggFuncIsNotCount(false);
    setSelClauseColsFetchException(false);
    setBaseTableName("");
    setAggFunction("");
    setIndexKey("");
  }

  public boolean isQueryHasGroupBy() {
    return queryHasGroupBy;
  }

  public void setQueryHasGroupBy(boolean queryHasGroupBy) {
    this.queryHasGroupBy = queryHasGroupBy;
  }

  public boolean isAggFuncIsNotCount() {
    return aggFuncIsNotCount;
  }

  public void setAggFuncIsNotCount(boolean aggFuncIsNotCount) {
    this.aggFuncIsNotCount = aggFuncIsNotCount;
  }

  public Map<String, String> getBaseToIdxTableMap() {
    return baseToIdxTableMap;
  }

  public void setAggFunction(String aggFunction) {
    this.aggFunction = aggFunction;
  }

  public String getAggFunction() {
    return aggFunction;
  }

  public void setSelClauseColsFetchException(boolean selClauseColsFetchException) {
    this.selClauseColsFetchException = selClauseColsFetchException;
  }

  public boolean isSelClauseColsFetchException() {
    return selClauseColsFetchException;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getBaseTableName() {
    return baseTableName;
  }

  public void setBaseTableName(String baseTableName) {
    this.baseTableName = baseTableName;
  }

  public String getIndexTableName() {
    return indexTableName;
  }

  public void setIndexTableName(String indexTableName) {
    this.indexTableName = indexTableName;
  }

  public  ParseContext getParseContext() {
    return parseContext;
  }

  /**
   * This method walks all the nodes starting from topOp TableScanOperator node
   * and invokes methods from {@link RewriteCanApplyProcFactory} for each of the rules
   * added to the opRules map. We use the {@link PreOrderWalker} for a pre-order
   * traversal of the operator tree.
   *
   * The methods from {@link RewriteCanApplyProcFactory} set appropriate values in
   * {@link RewriteVars} enum.
   *
   * @param topOp
   * @throws SemanticException
   */
  void populateRewriteVars(TableScanOperator topOp)
    throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    //^TS%[(SEL%)|(FIL%)]*GRY%[(FIL%)]*RS%[(FIL%)]*GRY%
    opRules.put(
        new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%[("
            + SelectOperator.getOperatorName() + "%)|(" + FilterOperator.getOperatorName() + "%)]*"
            + GroupByOperator.getOperatorName() + "%[" + FilterOperator.getOperatorName() + "%]*"
            + ReduceSinkOperator.getOperatorName() + "%[" + FilterOperator.getOperatorName()
            + "%]*" + GroupByOperator.getOperatorName() + "%"),
        RewriteCanApplyProcFactory.canApplyOnTableScanOperator(topOp));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, this);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.add(topOp);

    try {
      ogw.startWalking(topNodes, null);
    } catch (SemanticException e) {
      LOG.error("Exception in walking operator tree. Rewrite variables not populated");
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
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


  //Map for base table to index table mapping
  //TableScan operator for base table will be modified to read from index table
  private final Map<String, String> baseToIdxTableMap = new HashMap<String, String>();;

  public void addTable(String baseTableName, String indexTableName) {
    baseToIdxTableMap.put(baseTableName, indexTableName);
  }

  public String findBaseTable(String baseTableName) {
    return baseToIdxTableMap.get(baseTableName);
  }

  public String getIndexKey() {
    return indexKey;
  }

  public void setIndexKey(String indexKey) {
    this.indexKey = indexKey;
  }

  public TableScanOperator getTableScanOperator() {
    return tableScanOperator;
  }

  public void setTableScanOperator(TableScanOperator tableScanOperator) {
    this.tableScanOperator = tableScanOperator;
  }

  public List<SelectOperator> getSelectOperators() {
    return selectOperators;
  }

  public void setSelectOperators(List<SelectOperator> selectOperators) {
    this.selectOperators = selectOperators;
  }

  public List<GroupByOperator> getGroupByOperators() {
    return groupByOperators;
  }

  public void setGroupByOperators(List<GroupByOperator> groupByOperators) {
    this.groupByOperators = groupByOperators;
  }

  public void setAggParameterException(boolean aggParameterException) {
    this.aggParameterException = aggParameterException;
  }

  public boolean isAggParameterException() {
    return aggParameterException;
  }
}
