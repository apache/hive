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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

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
  private int aggFuncCnt = 0;
  private boolean queryHasGroupBy = false;
  private boolean aggFuncIsNotCount = false;
  private boolean aggFuncColsFetchException = false;
  private boolean whrClauseColsFetchException = false;
  private boolean selClauseColsFetchException = false;
  private boolean gbyKeysFetchException = false;
  private boolean countOnAllCols = false;
  private boolean countOfOne = false;
  private boolean queryHasMultipleTables = false;

  //Data structures that are populated in the RewriteCanApplyProcFactory
  //methods to check if the index key meets all criteria
  private Set<String> selectColumnsList = new LinkedHashSet<String>();
  private Set<String> predicateColumnsList = new LinkedHashSet<String>();
  private Set<String> gbKeyNameList = new LinkedHashSet<String>();
  private Set<String> aggFuncColList = new LinkedHashSet<String>();

  private final ParseContext parseContext;
  private String baseTableName;
  private String aggFunction;

  void resetCanApplyCtx(){
    setAggFuncCnt(0);
    setQueryHasGroupBy(false);
    setAggFuncIsNotCount(false);
    setAggFuncColsFetchException(false);
    setWhrClauseColsFetchException(false);
    setSelClauseColsFetchException(false);
    setGbyKeysFetchException(false);
    setCountOnAllCols(false);
    setCountOfOne(false);
    setQueryHasMultipleTables(false);
    selectColumnsList.clear();
    predicateColumnsList.clear();
    gbKeyNameList.clear();
    aggFuncColList.clear();
    setBaseTableName("");
    setAggFunction("");
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

  public void setAggFuncColsFetchException(boolean aggFuncColsFetchException) {
    this.aggFuncColsFetchException = aggFuncColsFetchException;
  }

  public boolean isAggFuncColsFetchException() {
    return aggFuncColsFetchException;
  }

  public void setWhrClauseColsFetchException(boolean whrClauseColsFetchException) {
    this.whrClauseColsFetchException = whrClauseColsFetchException;
  }

  public boolean isWhrClauseColsFetchException() {
    return whrClauseColsFetchException;
  }

  public void setSelClauseColsFetchException(boolean selClauseColsFetchException) {
    this.selClauseColsFetchException = selClauseColsFetchException;
  }

  public boolean isSelClauseColsFetchException() {
    return selClauseColsFetchException;
  }

  public void setGbyKeysFetchException(boolean gbyKeysFetchException) {
    this.gbyKeysFetchException = gbyKeysFetchException;
  }

  public boolean isGbyKeysFetchException() {
    return gbyKeysFetchException;
  }

  public void setCountOnAllCols(boolean countOnAllCols) {
    this.countOnAllCols = countOnAllCols;
  }

  public boolean isCountOnAllCols() {
    return countOnAllCols;
  }

  public void setCountOfOne(boolean countOfOne) {
    this.countOfOne = countOfOne;
  }

  public boolean isCountOfOne() {
    return countOfOne;
  }

  public void setQueryHasMultipleTables(boolean queryHasMultipleTables) {
    this.queryHasMultipleTables = queryHasMultipleTables;
  }

  public boolean isQueryHasMultipleTables() {
    return queryHasMultipleTables;
  }

  public Set<String> getSelectColumnsList() {
    return selectColumnsList;
  }

  public void setSelectColumnsList(Set<String> selectColumnsList) {
    this.selectColumnsList = selectColumnsList;
  }

  public Set<String> getPredicateColumnsList() {
    return predicateColumnsList;
  }

  public void setPredicateColumnsList(Set<String> predicateColumnsList) {
    this.predicateColumnsList = predicateColumnsList;
  }

  public Set<String> getGbKeyNameList() {
    return gbKeyNameList;
  }

  public void setGbKeyNameList(Set<String> gbKeyNameList) {
    this.gbKeyNameList = gbKeyNameList;
  }

  public Set<String> getAggFuncColList() {
    return aggFuncColList;
  }

  public void setAggFuncColList(Set<String> aggFuncColList) {
    this.aggFuncColList = aggFuncColList;
  }

   public int getAggFuncCnt() {
    return aggFuncCnt;
  }

  public void setAggFuncCnt(int aggFuncCnt) {
    this.aggFuncCnt = aggFuncCnt;
  }

  public String getBaseTableName() {
    return baseTableName;
  }

  public void setBaseTableName(String baseTableName) {
    this.baseTableName = baseTableName;
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
  void populateRewriteVars(Operator<? extends OperatorDesc> topOp)
    throws SemanticException{
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        RewriteCanApplyProcFactory.canApplyOnFilterOperator());
    opRules.put(new RuleRegExp("R2", GroupByOperator.getOperatorName() + "%"),
        RewriteCanApplyProcFactory.canApplyOnGroupByOperator());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        RewriteCanApplyProcFactory.canApplyOnSelectOperator());

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
  private final Map<String, String> baseToIdxTableMap =
    new HashMap<String, String>();;


  public void addTable(String baseTableName, String indexTableName) {
     baseToIdxTableMap.put(baseTableName, indexTableName);
   }

   public String findBaseTable(String baseTableName)  {
     return baseToIdxTableMap.get(baseTableName);
   }


  boolean isIndexUsableForQueryBranchRewrite(Index index, Set<String> indexKeyNames){

    //--------------------------------------------
    //Check if all columns in select list are part of index key columns
    if (!indexKeyNames.containsAll(selectColumnsList)) {
      LOG.info("Select list has non index key column : " +
          " Cannot use index " + index.getIndexName());
      return false;
    }

    //--------------------------------------------
    // Check if all columns in where predicate are part of index key columns
    if (!indexKeyNames.containsAll(predicateColumnsList)) {
      LOG.info("Predicate column ref list has non index key column : " +
          " Cannot use index  " + index.getIndexName());
      return false;
    }

      //--------------------------------------------
      // For group by, we need to check if all keys are from index columns
      // itself. Here GB key order can be different than index columns but that does
      // not really matter for final result.
      if (!indexKeyNames.containsAll(gbKeyNameList)) {
        LOG.info("Group by key has some non-indexed columns, " +
            " Cannot use index  " + index.getIndexName());
        return false;
      }

      // If we have agg function (currently only COUNT is supported), check if its inputs are
      // from index. we currently support only that.
      if (aggFuncColList.size() > 0)  {
        if (!indexKeyNames.containsAll(aggFuncColList)){
          LOG.info("Agg Func input is not present in index key columns. Currently " +
              "only agg func on index columns are supported by rewrite optimization");
          return false;
        }
      }

    //Now that we are good to do this optimization, set parameters in context
    //which would be used by transformation procedure as inputs.
    if(queryHasGroupBy
        && aggFuncCnt == 1
        && !aggFuncIsNotCount){
      addTable(baseTableName, index.getIndexTableName());
    }else{
      LOG.info("No valid criteria met to apply rewrite.");
      return false;
    }
    return true;
  }

}
