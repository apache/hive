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

package org.apache.hadoop.hive.ql.parse;

import java.util.*;

import org.antlr.runtime.tree.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of the parse information related to a query block
 *
 **/

public class QBParseInfo {

  private boolean isSubQ;
  private String alias;
  private ASTNode joinExpr;
  private ASTNode hints;
  private HashMap<String, ASTNode> aliasToSrc;
  private HashMap<String, ASTNode> nameToDest;
  private HashMap<String, TableSample> nameToSample;
  private HashMap<String, ASTNode> destToSelExpr;
  private HashMap<String, ASTNode> destToWhereExpr;
  private HashMap<String, ASTNode> destToGroupby;
  /**
   * ClusterBy is a short name for both DistributeBy and SortBy.  
   */
  private HashMap<String, ASTNode> destToClusterby;
  /**
   * DistributeBy controls the hashcode of the row, which determines which reducer
   * the rows will go to. 
   */
  private HashMap<String, ASTNode> destToDistributeby;
  /**
   * SortBy controls the reduce keys, which affects the order of rows 
   * that the reducer receives. 
   */

  private HashMap<String, ASTNode> destToSortby;

  /* Order by clause */
  private HashMap<String, ASTNode> destToOrderby;
  private HashMap<String, Integer>    destToLimit;
  private int outerQueryLimit;

  // used by GroupBy
  private LinkedHashMap<String, LinkedHashMap<String, ASTNode> > destToAggregationExprs;
  private HashMap<String, ASTNode> destToDistinctFuncExpr;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBParseInfo.class.getName());
  
  public QBParseInfo(String alias, boolean isSubQ) {
    this.aliasToSrc = new HashMap<String, ASTNode>();
    this.nameToDest = new HashMap<String, ASTNode>();
    this.nameToSample = new HashMap<String, TableSample>();
    this.destToSelExpr = new HashMap<String, ASTNode>();
    this.destToWhereExpr = new HashMap<String, ASTNode>();
    this.destToGroupby = new HashMap<String, ASTNode>();
    this.destToClusterby = new HashMap<String, ASTNode>();
    this.destToDistributeby = new HashMap<String, ASTNode>();
    this.destToSortby = new HashMap<String, ASTNode>();
    this.destToOrderby = new HashMap<String, ASTNode>();
    this.destToLimit = new HashMap<String, Integer>();
    
    this.destToAggregationExprs = new LinkedHashMap<String, LinkedHashMap<String, ASTNode> >();
    this.destToDistinctFuncExpr = new HashMap<String, ASTNode>();
    
    this.alias = alias;
    this.isSubQ = isSubQ;
    this.outerQueryLimit = -1;
  }

  public void setAggregationExprsForClause(String clause, LinkedHashMap<String, ASTNode> aggregationTrees) {
    this.destToAggregationExprs.put(clause, aggregationTrees);
  }

  public HashMap<String, ASTNode> getAggregationExprsForClause(String clause) {
    return this.destToAggregationExprs.get(clause);
  }

  public void setDistinctFuncExprForClause(String clause, ASTNode ast) {
    this.destToDistinctFuncExpr.put(clause, ast);
  }
  
  public ASTNode getDistinctFuncExprForClause(String clause) {
    return this.destToDistinctFuncExpr.get(clause);
  }
  
  public void setSelExprForClause(String clause, ASTNode ast) {
    this.destToSelExpr.put(clause, ast);
  }

  public void setWhrExprForClause(String clause, ASTNode ast) {
    this.destToWhereExpr.put(clause, ast);
  }

  public void setGroupByExprForClause(String clause, ASTNode ast) {
    this.destToGroupby.put(clause, ast);
  }

  public void setDestForClause(String clause, ASTNode ast) {
    this.nameToDest.put(clause, ast);
  }

  /**
   * Set the Cluster By AST for the clause.  
   * @param clause the name of the clause
   * @param ast the abstract syntax tree
   */
  public void setClusterByExprForClause(String clause, ASTNode ast) {
    this.destToClusterby.put(clause, ast);
  }

  /**
   * Set the Distribute By AST for the clause.  
   * @param clause the name of the clause
   * @param ast the abstract syntax tree
   */
  public void setDistributeByExprForClause(String clause, ASTNode ast) {
    this.destToDistributeby.put(clause, ast);
  }

  /**
   * Set the Sort By AST for the clause.  
   * @param clause the name of the clause
   * @param ast the abstract syntax tree
   */
  public void setSortByExprForClause(String clause, ASTNode ast) {
    this.destToSortby.put(clause, ast);
  }

  public void setOrderByExprForClause(String clause, ASTNode ast) {
    this.destToOrderby.put(clause, ast);
  }

  public void setSrcForAlias(String alias, ASTNode ast) {
    this.aliasToSrc.put(alias.toLowerCase(), ast);
  }

  public Set<String> getClauseNames() {
    return this.destToSelExpr.keySet();
  }

  public Set<String> getClauseNamesForDest() {
    return this.nameToDest.keySet();
  }

  public ASTNode getDestForClause(String clause) {
    return this.nameToDest.get(clause);
  }

  public ASTNode getWhrForClause(String clause) {
    return this.destToWhereExpr.get(clause);
  }

  public HashMap<String, ASTNode> getDestToWhereExpr() {
    return destToWhereExpr;
  }

  public ASTNode getGroupByForClause(String clause) {
    return this.destToGroupby.get(clause);
  }
  public HashMap<String, ASTNode> getDestToGroupBy() {
    return this.destToGroupby;
  }
  
  public ASTNode getSelForClause(String clause) {
    return this.destToSelExpr.get(clause);
  }

  /**
   * Get the Cluster By AST for the clause.  
   * @param clause the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getClusterByForClause(String clause) {
    return this.destToClusterby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToClusterBy() {
    return destToClusterby;
  }
  
  /**
   * Get the Distribute By AST for the clause.  
   * @param clause the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getDistributeByForClause(String clause) {
    return this.destToDistributeby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToDistributeBy() {
    return destToDistributeby;
  }
  
  /**
   * Get the Sort By AST for the clause.  
   * @param clause the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getSortByForClause(String clause) {
    return this.destToSortby.get(clause);
  }

  public ASTNode getOrderByForClause(String clause) {
    return this.destToOrderby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToSortBy() {
    return destToSortby;
  }
  
  public HashMap<String, ASTNode> getDestToOrderBy() {
    return destToOrderby;
  }
  
  public ASTNode getSrcForAlias(String alias) {
    return this.aliasToSrc.get(alias.toLowerCase());
  }

  public String getAlias() {
    return this.alias;
  }

  public boolean getIsSubQ() {
    return this.isSubQ;
  }

  public ASTNode getJoinExpr() {
    return this.joinExpr;
  }

  public void setJoinExpr(ASTNode joinExpr) {
    this.joinExpr = joinExpr;
  }

  public TableSample getTabSample(String alias) {
    return this.nameToSample.get(alias.toLowerCase());
  }
  
  public void setTabSample(String alias, TableSample tableSample) {
    this.nameToSample.put(alias.toLowerCase(), tableSample);
  }

  public void setDestLimit(String dest, Integer limit) {
    this.destToLimit.put(dest, limit);
  }

  public Integer getDestLimit(String dest) {
    return this.destToLimit.get(dest);
  }

	/**
	 * @return the outerQueryLimit
	 */
	public int getOuterQueryLimit() {
		return outerQueryLimit;
	}

	/**
	 * @param outerQueryLimit the outerQueryLimit to set
	 */
	public void setOuterQueryLimit(int outerQueryLimit) {
		this.outerQueryLimit = outerQueryLimit;
	}

  public boolean isSelectStarQuery() {
    if (isSubQ || 
       (joinExpr != null) ||
       (!nameToSample.isEmpty()) ||
       (!destToGroupby.isEmpty()) ||
       (!destToClusterby.isEmpty()))
      return false;
    
    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> aggrIter = destToAggregationExprs.entrySet().iterator();
    while (aggrIter.hasNext()) {
      HashMap<String, ASTNode> h = aggrIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }
      	
    if (!destToDistinctFuncExpr.isEmpty()) {
      Iterator<Map.Entry<String, ASTNode>> distn = destToDistinctFuncExpr.entrySet().iterator();
      while (distn.hasNext()) {
        ASTNode ct = distn.next().getValue();
        if (ct != null) 
          return false;
      }
    }
        
    Iterator<Map.Entry<String, ASTNode>> iter = nameToDest.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode v = entry.getValue();
      if (!(((ASTNode)v.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
        return false;
    }
      	
    iter = destToSelExpr.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode selExprList = entry.getValue();
      // Iterate over the selects
      for (int i = 0; i < selExprList.getChildCount(); ++i) {
        
        // list of the columns
        ASTNode selExpr = (ASTNode) selExprList.getChild(i);
        ASTNode sel = (ASTNode)selExpr.getChild(0);
        
        if (sel.getToken().getType() != HiveParser.TOK_ALLCOLREF)
          return false;
      }
    }

    return true;
  }

  public void setHints(ASTNode hint) {
    this.hints = hint;
  }

  public ASTNode getHints() {
    return hints;
  }
}
