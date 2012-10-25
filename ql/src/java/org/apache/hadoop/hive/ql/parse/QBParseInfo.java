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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;

/**
 * Implementation of the parse information related to a query block.
 *
 **/
public class QBParseInfo {

  private final boolean isSubQ;
  private final String alias;
  private ASTNode joinExpr;
  private ASTNode hints;
  private final HashMap<String, ASTNode> aliasToSrc;
  private final HashMap<String, ASTNode> nameToDest;
  private final HashMap<String, TableSample> nameToSample;
  private final Map<ASTNode, String> exprToColumnAlias;
  private final Map<String, ASTNode> destToSelExpr;
  private final HashMap<String, ASTNode> destToWhereExpr;
  private final HashMap<String, ASTNode> destToGroupby;
  private final Set<String> destRollups;
  private final Set<String> destCubes;
  private final Map<String, ASTNode> destToHaving;
  private final HashSet<String> insertIntoTables;

  private boolean isAnalyzeCommand; // used for the analyze command (statistics)
  private boolean isInsertToTable;  // used for insert overwrite command (statistics)

  private final HashMap<String, tableSpec> tableSpecs; // used for statistics

  /**
   * ClusterBy is a short name for both DistributeBy and SortBy.
   */
  private final HashMap<String, ASTNode> destToClusterby;
  /**
   * DistributeBy controls the hashcode of the row, which determines which
   * reducer the rows will go to.
   */
  private final HashMap<String, ASTNode> destToDistributeby;
  /**
   * SortBy controls the reduce keys, which affects the order of rows that the
   * reducer receives.
   */

  private final HashMap<String, ASTNode> destToSortby;

  /**
   * Maping from table/subquery aliases to all the associated lateral view nodes.
   */
  private final HashMap<String, ArrayList<ASTNode>> aliasToLateralViews;

  /* Order by clause */
  private final HashMap<String, ASTNode> destToOrderby;
  private final HashMap<String, Integer> destToLimit;
  private int outerQueryLimit;

  // used by GroupBy
  private final LinkedHashMap<String, LinkedHashMap<String, ASTNode>> destToAggregationExprs;
  private final HashMap<String, List<ASTNode>> destToDistinctFuncExprs;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBParseInfo.class.getName());

  public QBParseInfo(String alias, boolean isSubQ) {
    aliasToSrc = new HashMap<String, ASTNode>();
    nameToDest = new HashMap<String, ASTNode>();
    nameToSample = new HashMap<String, TableSample>();
    exprToColumnAlias = new HashMap<ASTNode, String>();
    destToSelExpr = new LinkedHashMap<String, ASTNode>();
    destToWhereExpr = new HashMap<String, ASTNode>();
    destToGroupby = new HashMap<String, ASTNode>();
    destToHaving = new HashMap<String, ASTNode>();
    destToClusterby = new HashMap<String, ASTNode>();
    destToDistributeby = new HashMap<String, ASTNode>();
    destToSortby = new HashMap<String, ASTNode>();
    destToOrderby = new HashMap<String, ASTNode>();
    destToLimit = new HashMap<String, Integer>();
    insertIntoTables = new HashSet<String>();
    destRollups = new HashSet<String>();
    destCubes = new HashSet<String>();

    destToAggregationExprs = new LinkedHashMap<String, LinkedHashMap<String, ASTNode>>();
    destToDistinctFuncExprs = new HashMap<String, List<ASTNode>>();

    this.alias = alias;
    this.isSubQ = isSubQ;
    outerQueryLimit = -1;

    aliasToLateralViews = new HashMap<String, ArrayList<ASTNode>>();

    tableSpecs = new HashMap<String, BaseSemanticAnalyzer.tableSpec>();
  }

  public void setAggregationExprsForClause(String clause,
      LinkedHashMap<String, ASTNode> aggregationTrees) {
    destToAggregationExprs.put(clause, aggregationTrees);
  }

  public void addAggregationExprsForClause(String clause,
      LinkedHashMap<String, ASTNode> aggregationTrees) {
    if (destToAggregationExprs.containsKey(clause)) {
      destToAggregationExprs.get(clause).putAll(aggregationTrees);
    } else {
      destToAggregationExprs.put(clause, aggregationTrees);
    }
  }

  public void addInsertIntoTable(String fullName) {
    insertIntoTables.add(fullName.toLowerCase());
  }

  public boolean isInsertIntoTable(String dbName, String table) {
    String fullName = dbName + "." + table;
    return insertIntoTables.contains(fullName.toLowerCase());
  }

  public HashMap<String, ASTNode> getAggregationExprsForClause(String clause) {
    return destToAggregationExprs.get(clause);
  }

  public void setDistinctFuncExprsForClause(String clause, List<ASTNode> ast) {
    destToDistinctFuncExprs.put(clause, ast);
  }

  public List<ASTNode> getDistinctFuncExprsForClause(String clause) {
    return destToDistinctFuncExprs.get(clause);
  }

  public void setSelExprForClause(String clause, ASTNode ast) {
    destToSelExpr.put(clause, ast);
  }

  public void setWhrExprForClause(String clause, ASTNode ast) {
    destToWhereExpr.put(clause, ast);
  }

  public void setHavingExprForClause(String clause, ASTNode ast) {
    destToHaving.put(clause, ast);
  }

  public void setGroupByExprForClause(String clause, ASTNode ast) {
    destToGroupby.put(clause, ast);
  }

  public void setDestForClause(String clause, ASTNode ast) {
    nameToDest.put(clause, ast);
  }

  /**
   * Set the Cluster By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @param ast
   *          the abstract syntax tree
   */
  public void setClusterByExprForClause(String clause, ASTNode ast) {
    destToClusterby.put(clause, ast);
  }

  /**
   * Set the Distribute By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @param ast
   *          the abstract syntax tree
   */
  public void setDistributeByExprForClause(String clause, ASTNode ast) {
    destToDistributeby.put(clause, ast);
  }

  /**
   * Set the Sort By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @param ast
   *          the abstract syntax tree
   */
  public void setSortByExprForClause(String clause, ASTNode ast) {
    destToSortby.put(clause, ast);
  }

  public void setOrderByExprForClause(String clause, ASTNode ast) {
    destToOrderby.put(clause, ast);
  }

  public void setSrcForAlias(String alias, ASTNode ast) {
    aliasToSrc.put(alias.toLowerCase(), ast);
  }

  public Set<String> getClauseNames() {
    return destToSelExpr.keySet();
  }

  public Set<String> getClauseNamesForDest() {
    return nameToDest.keySet();
  }

  public ASTNode getDestForClause(String clause) {
    return nameToDest.get(clause);
  }

  public ASTNode getWhrForClause(String clause) {
    return destToWhereExpr.get(clause);
  }

  public HashMap<String, ASTNode> getDestToWhereExpr() {
    return destToWhereExpr;
  }

  public ASTNode getGroupByForClause(String clause) {
    return destToGroupby.get(clause);
  }

  public Set<String> getDestRollups() {
    return destRollups;
  }

  public Set<String> getDestCubes() {
    return destCubes;
  }

  public HashMap<String, ASTNode> getDestToGroupBy() {
    return destToGroupby;
  }

  public ASTNode getHavingForClause(String clause) {
    return destToHaving.get(clause);
  }

  public Map<String, ASTNode> getDestToHaving() {
    return destToHaving;
  }

  public ASTNode getSelForClause(String clause) {
    return destToSelExpr.get(clause);
  }

  /**
   * Get the Cluster By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getClusterByForClause(String clause) {
    return destToClusterby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToClusterBy() {
    return destToClusterby;
  }

  /**
   * Get the Distribute By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getDistributeByForClause(String clause) {
    return destToDistributeby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToDistributeBy() {
    return destToDistributeby;
  }

  /**
   * Get the Sort By AST for the clause.
   *
   * @param clause
   *          the name of the clause
   * @return the abstract syntax tree
   */
  public ASTNode getSortByForClause(String clause) {
    return destToSortby.get(clause);
  }

  public ASTNode getOrderByForClause(String clause) {
    return destToOrderby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToSortBy() {
    return destToSortby;
  }

  public HashMap<String, ASTNode> getDestToOrderBy() {
    return destToOrderby;
  }

  public ASTNode getSrcForAlias(String alias) {
    return aliasToSrc.get(alias.toLowerCase());
  }

  public String getAlias() {
    return alias;
  }

  public boolean getIsSubQ() {
    return isSubQ;
  }

  public ASTNode getJoinExpr() {
    return joinExpr;
  }

  public void setJoinExpr(ASTNode joinExpr) {
    this.joinExpr = joinExpr;
  }

  public TableSample getTabSample(String alias) {
    return nameToSample.get(alias.toLowerCase());
  }

  public void setTabSample(String alias, TableSample tableSample) {
    nameToSample.put(alias.toLowerCase(), tableSample);
  }

  public String getExprToColumnAlias(ASTNode expr) {
    return exprToColumnAlias.get(expr);
  }

  public Map<ASTNode, String> getAllExprToColumnAlias() {
    return exprToColumnAlias;
  }

  public boolean hasExprToColumnAlias(ASTNode expr) {
    return exprToColumnAlias.containsKey(expr);
  }

  public void setExprToColumnAlias(ASTNode expr, String alias) {
    exprToColumnAlias.put(expr,  alias);
  }

  public void setDestLimit(String dest, Integer limit) {
    destToLimit.put(dest, limit);
  }

  public Integer getDestLimit(String dest) {
    return destToLimit.get(dest);
  }

  /**
   * @return the outerQueryLimit
   */
  public int getOuterQueryLimit() {
    return outerQueryLimit;
  }

  /**
   * @param outerQueryLimit
   *          the outerQueryLimit to set
   */
  public void setOuterQueryLimit(int outerQueryLimit) {
    this.outerQueryLimit = outerQueryLimit;
  }

  public boolean isSimpleSelectQuery() {
    if (isSubQ || (joinExpr != null)
        || (!destToGroupby.isEmpty()) || (!destToClusterby.isEmpty())
        || (!aliasToLateralViews.isEmpty())) {
      return false;
    }

    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> aggrIter = destToAggregationExprs
        .entrySet().iterator();
    while (aggrIter.hasNext()) {
      HashMap<String, ASTNode> h = aggrIter.next().getValue();
      if ((h != null) && (!h.isEmpty())) {
        return false;
      }
    }

    if (!destToDistinctFuncExprs.isEmpty()) {
      Iterator<Map.Entry<String, List<ASTNode>>> distn = destToDistinctFuncExprs
          .entrySet().iterator();
      while (distn.hasNext()) {
        List<ASTNode> ct = distn.next().getValue();
        if (!ct.isEmpty()) {
          return false;
        }
      }
    }

    Iterator<Map.Entry<String, ASTNode>> iter = nameToDest.entrySet()
        .iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode v = entry.getValue();
      if (!(((ASTNode) v.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE)) {
        return false;
      }
    }

    return true;
  }

  public void setHints(ASTNode hint) {
    hints = hint;
  }

  public ASTNode getHints() {
    return hints;
  }

  public Map<String, ArrayList<ASTNode>> getAliasToLateralViews() {
    return aliasToLateralViews;
  }

  public List<ASTNode> getLateralViewsForAlias(String alias) {
    return aliasToLateralViews.get(alias.toLowerCase());
  }

  public void addLateralViewForAlias(String alias, ASTNode lateralView) {
    ArrayList<ASTNode> lateralViews = aliasToLateralViews.get(alias);
    if (lateralViews == null) {
      lateralViews = new ArrayList<ASTNode>();
      aliasToLateralViews.put(alias, lateralViews);
    }
    lateralViews.add(lateralView);
  }

  public void setIsAnalyzeCommand(boolean isAnalyzeCommand) {
    this.isAnalyzeCommand = isAnalyzeCommand;
  }

  public boolean isAnalyzeCommand() {
    return isAnalyzeCommand;
  }

  public void setIsInsertToTable(boolean isInsertToTable) {
    this.isInsertToTable = isInsertToTable;
  }

  public boolean isInsertToTable() {
    return isInsertToTable;
  }

  public void addTableSpec(String tName, tableSpec tSpec) {
    tableSpecs.put(tName, tSpec);
  }

  public tableSpec getTableSpec(String tName) {
    return tableSpecs.get(tName);
  }

  /**
   * This method is used only for the anlayze command to get the partition specs
   */
  public tableSpec getTableSpec() {

    Iterator<String> tName = tableSpecs.keySet().iterator();
    return tableSpecs.get(tName.next());
  }

  public HashMap<String, Integer> getDestToLimit() {
    return destToLimit;
  }

  public LinkedHashMap<String, LinkedHashMap<String, ASTNode>> getDestToAggregationExprs() {
    return destToAggregationExprs;
  }

  public HashMap<String, List<ASTNode>> getDestToDistinctFuncExprs() {
    return destToDistinctFuncExprs;
  }

  public HashMap<String, TableSample> getNameToSample() {
    return nameToSample;
  }
}
