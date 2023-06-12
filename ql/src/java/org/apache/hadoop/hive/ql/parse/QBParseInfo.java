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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.unescapeIdentifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTION;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONDI;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_FUNCTIONSTAR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.AnalyzeRewriteContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;

/**
 * Implementation of the parse information related to a query block.
 *
 **/
public class QBParseInfo {

  private boolean isSubQ;
  private String alias;
  private ASTNode joinExpr;
  private ASTNode hints;
  private ASTNode colAliases;
  private List<ASTNode> hintList;
  private final Map<String, ASTNode> aliasToSrc;
  /**
   * insclause-0 -> TOK_TAB ASTNode
   */
  private final Map<String, ASTNode> nameToDest;
  /**
   * For 'insert into FOO(x,y) select ...' this stores the
   * insclause-0 -> x,y mapping
   */
  private final Map<String, List<String>> nameToDestSchema;
  private final Map<String, TableSample> nameToSample;
  private final Map<ASTNode, String> exprToColumnAlias;
  private final Map<String, ASTNode> destToSelExpr;
  private final Map<String, ASTNode> destToWhereExpr;
  private final Map<String, ASTNode> destToGroupby;
  private final Set<String> destRollups;
  private final Set<String> destCubes;
  private final Set<String> destGroupingSets;
  private final Map<String, ASTNode> destToHaving;
  private final Map<String, ASTNode> destToQualify;
  private final Map<String, Boolean> destToOpType;
  // insertIntoTables/insertOverwriteTables map a table's fullName to its ast;
  private final Map<String, ASTNode> insertIntoTables;
  private final Map<String, ASTNode> insertOverwriteTables;
  private ASTNode queryFromExpr;

  private boolean isAnalyzeCommand; // used for the analyze command (statistics)
  private boolean isNoScanAnalyzeCommand; // used for the analyze command (statistics) (noscan)

  private final Map<String, TableSpec> tableSpecs; // used for statistics

  private AnalyzeRewriteContext analyzeRewrite;


  /**
   * ClusterBy is a short name for both DistributeBy and SortBy.
   */
  private final Map<String, ASTNode> destToClusterby;
  /**
   * DistributeBy controls the hashcode of the row, which determines which
   * reducer the rows will go to.
   */
  private final Map<String, ASTNode> destToDistributeby;
  /**
   * SortBy controls the reduce keys, which affects the order of rows that the
   * reducer receives.
   */

  private final Map<String, ASTNode> destToSortby;

  /**
   * Maping from table/subquery aliases to all the associated lateral view nodes.
   */
  private final Map<String, List<ASTNode>> aliasToLateralViews;

  private final Map<String, ASTNode> destToLateralView;

  /* Order by clause */
  private final Map<String, ASTNode> destToOrderby;
  // Use SimpleEntry to save the offset and rowcount of limit clause
  // KEY of SimpleEntry: offset
  // VALUE of SimpleEntry: rowcount
  private final Map<String, SimpleEntry<Integer, Integer>> destToLimit;
  private int outerQueryLimit;

  // used by GroupBy
  private final Map<String, Map<String, ASTNode>> destToAggregationExprs;
  private final Map<String, List<ASTNode>> destToDistinctFuncExprs;

  // used by Windowing
  private final Map<String, Map<String, ASTNode>> destToWindowingExprs;

  // is the query insert overwrite directory
  private boolean isInsertOverwriteDir = false;


  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(QBParseInfo.class.getName());

  public QBParseInfo(String alias, boolean isSubQ) {
    aliasToSrc = new HashMap<String, ASTNode>();
    nameToDest = new HashMap<String, ASTNode>();
    nameToDestSchema = new HashMap<String, List<String>>();
    nameToSample = new HashMap<String, TableSample>();
    exprToColumnAlias = new HashMap<ASTNode, String>();
    destToLateralView = new HashMap<String, ASTNode>();
    destToSelExpr = new LinkedHashMap<String, ASTNode>();
    destToWhereExpr = new HashMap<String, ASTNode>();
    destToGroupby = new HashMap<String, ASTNode>();
    destToHaving = new HashMap<String, ASTNode>();
    destToQualify = new HashMap<>();
    destToClusterby = new HashMap<String, ASTNode>();
    destToDistributeby = new HashMap<String, ASTNode>();
    destToSortby = new HashMap<String, ASTNode>();
    destToOrderby = new HashMap<String, ASTNode>();
    destToLimit = new HashMap<String, SimpleEntry<Integer, Integer>>();
    destToOpType = new HashMap<>();
    insertIntoTables = new HashMap<String, ASTNode>();
    insertOverwriteTables = new HashMap<String, ASTNode>();
    destRollups = new HashSet<String>();
    destCubes = new HashSet<String>();
    destGroupingSets = new HashSet<String>();

    destToAggregationExprs = new LinkedHashMap<String, Map<String, ASTNode>>();
    destToWindowingExprs = new LinkedHashMap<String, Map<String, ASTNode>>();
    destToDistinctFuncExprs = new HashMap<String, List<ASTNode>>();

    this.alias = StringInternUtils.internIfNotNull(alias);
    this.isSubQ = isSubQ;
    outerQueryLimit = -1;

    aliasToLateralViews = new HashMap<String, List<ASTNode>>();

    tableSpecs = new HashMap<String, BaseSemanticAnalyzer.TableSpec>();

  }

/*
   * If a QB is such that the aggregation expressions need to be handled by
   * the Windowing PTF; we invoke this function to clear the AggExprs on the dest.
   */
  public void clearAggregationExprsForClause(String clause) {
    destToAggregationExprs.get(clause).clear();
  }

  public void setAggregationExprsForClause(String clause, Map<String, ASTNode> aggregationTrees) {
    destToAggregationExprs.put(clause, aggregationTrees);
  }

  public void addAggregationExprsForClause(String clause, Map<String, ASTNode> aggregationTrees) {
    if (destToAggregationExprs.containsKey(clause)) {
      destToAggregationExprs.get(clause).putAll(aggregationTrees);
    } else {
      destToAggregationExprs.put(clause, aggregationTrees);
    }
  }

  public void addInsertIntoTable(String fullName, ASTNode ast) {
    insertIntoTables.put(fullName.toLowerCase(), ast);
  }
  
  public void setDestToOpType(String clause, boolean value) {
	destToOpType.put(clause, value);
  }
  
  public boolean isDestToOpTypeInsertOverwrite(String clause) {
	if (destToOpType.containsKey(clause)) {
		return destToOpType.get(clause);
	} else {
	  return false;
	}
  }

  /**
   * See also {@link #getInsertOverwriteTables()}
   */
  public boolean isInsertIntoTable(String dbName, String table, String branchName) {
    String  fullName = dbName + "." + table;
    if (branchName != null) {
      fullName += "." + branchName;
    }
    return insertIntoTables.containsKey(fullName.toLowerCase());
  }

  /**
   * Check if a table is in the list to be inserted into
   * See also {@link #getInsertOverwriteTables()}
   * @param fullTableName table name in dbname.tablename format
   * @return
   */
  public boolean isInsertIntoTable(String fullTableName) {
    return insertIntoTables.containsKey(fullTableName.toLowerCase());
  }

  public void setInsertOverwriteDirectory(boolean isInsertOverwriteDir) {
    this.isInsertOverwriteDir = isInsertOverwriteDir;
  }

  public Map<String, ASTNode> getAggregationExprsForClause(String clause) {
    return destToAggregationExprs.get(clause);
  }

  public void addWindowingExprToClause(String clause, ASTNode windowingExprNode) {
    Map<String, ASTNode> windowingExprs = destToWindowingExprs.get(clause);
    if ( windowingExprs == null ) {
      windowingExprs = new LinkedHashMap<String, ASTNode>();
      destToWindowingExprs.put(clause, windowingExprs);
    }
    windowingExprs.put(windowingExprNode.toStringTree(), windowingExprNode);
  }

  public Map<String, ASTNode> getWindowingExprsForClause(String clause) {
    return destToWindowingExprs.get(clause);
  }

  public void clearDistinctFuncExprsForClause(String clause) {
    List<ASTNode> l = destToDistinctFuncExprs.get(clause);
    if ( l != null ) {
      l.clear();
    }
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

  public void setQueryFromExpr(ASTNode ast) {
    queryFromExpr = ast;
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

  List<String> setDestSchemaForClause(String clause, List<String> columnList) {
    return nameToDestSchema.put(clause, columnList);
  }
  List<String> getDestSchemaForClause(String clause) {
    return nameToDestSchema.get(clause);
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

  public Map<String, ASTNode> getDestToWhereExpr() {
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

  public Set<String> getDestGroupingSets() {
    return destGroupingSets;
  }

  public Map<String, ASTNode> getDestToGroupBy() {
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

  public ASTNode getQueryFrom() {
    return queryFromExpr;
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

  public Map<String, ASTNode> getDestToClusterBy() {
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

  public Map<String, ASTNode> getDestToDistributeBy() {
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

  public Map<String, ASTNode> getDestToSortBy() {
    return destToSortby;
  }

  public Map<String, ASTNode> getDestToOrderBy() {
    return destToOrderby;
  }

  public ASTNode getSrcForAlias(String alias) {
    return aliasToSrc.get(alias.toLowerCase());
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public boolean getIsSubQ() {
    return isSubQ;
  }

  public void setIsSubQ(boolean isSubQ) {
    this.isSubQ = isSubQ;
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
    exprToColumnAlias.put(expr,  StringInternUtils.internIfNotNull(alias));
  }

  public void setDestLimit(String dest, Integer offset, Integer limit) {
    destToLimit.put(dest, new SimpleEntry<>(offset, limit));
  }

  public Integer getDestLimit(String dest) {
    return destToLimit.get(dest) == null ? null : destToLimit.get(dest).getValue();
  }

  public Integer getDestLimitOffset(String dest) {
    return destToLimit.get(dest) == null ? 0 : destToLimit.get(dest).getKey();
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

  public boolean isTopLevelSimpleSelectStarQuery() {
    if (alias != null || destToSelExpr.size() != 1 || !isSimpleSelectQuery()) {
      return false;
    }
    for (ASTNode selExprs : destToSelExpr.values()) {
      if (selExprs.getChildCount() != 1) {
        return false;
      }
      Tree sel = selExprs.getChild(0).getChild(0);
      if (sel == null || sel.getType() != HiveParser.TOK_ALLCOLREF) {
        return false;
      }
    }
    return true;
  }

  // for fast check of possible existence of RS (will be checked again in SimpleFetchOptimizer)
  public boolean isSimpleSelectQuery() {
    if (joinExpr != null || !destToOrderby.isEmpty() || !destToSortby.isEmpty()
        || !destToGroupby.isEmpty() || !destToClusterby.isEmpty() || !destToDistributeby.isEmpty()
        || !destRollups.isEmpty() || !destCubes.isEmpty() || !destGroupingSets.isEmpty()
        || !destToHaving.isEmpty()) {
      return false;
    }

    for (Map<String, ASTNode> entry : destToAggregationExprs.values()) {
      if (entry != null && !entry.isEmpty()) {
        return false;
      }
    }

    for (Map<String, ASTNode> entry : destToWindowingExprs.values()) {
      if (entry != null && !entry.isEmpty()) {
        return false;
      }
    }

    for (List<ASTNode> ct : destToDistinctFuncExprs.values()) {
      if (!ct.isEmpty()) {
        return false;
      }
    }

    // exclude insert queries
    for (ASTNode v : nameToDest.values()) {
      if (!(v.getChild(0).getType() == HiveParser.TOK_TMP_FILE)) {
        return false;
      }
    }

    return true;
  }

  public void setHints(ASTNode hint) {
    hints = hint;
  }

  public void setHintList(List<ASTNode> hintList) {
    this.hintList = hintList;
  }

  public List<ASTNode> getHintList() {
    return hintList;
  }

  public ASTNode getHints() {
    return hints;
  }

  public Map<String, List<ASTNode>> getAliasToLateralViews() {
    return aliasToLateralViews;
  }

  public List<ASTNode> getLateralViewsForAlias(String alias) {
    return aliasToLateralViews.get(alias.toLowerCase());
  }

  public void addLateralViewForAlias(String alias, ASTNode lateralView) {
    List<ASTNode> lateralViews = aliasToLateralViews.get(alias);
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

  public void addTableSpec(String tName, TableSpec tSpec) {
    tableSpecs.put(tName, tSpec);
  }

  public TableSpec getTableSpec(String tName) {
    return tableSpecs.get(tName);
  }

  /**
   * This method is used only for the analyze command to get the partition specs
   */
  public TableSpec getTableSpec() {

    Iterator<String> tName = tableSpecs.keySet().iterator();
    return tableSpecs.get(tName.next());
  }

  public Map<String, SimpleEntry<Integer,Integer>> getDestToLimit() {
    return destToLimit;
  }

  public Map<String, Map<String, ASTNode>> getDestToAggregationExprs() {
    return destToAggregationExprs;
  }

  public Map<String, List<ASTNode>> getDestToDistinctFuncExprs() {
    return destToDistinctFuncExprs;
  }

  public Map<String, TableSample> getNameToSample() {
    return nameToSample;
  }

  public Map<String, ASTNode> getDestToLateralView() {
    return destToLateralView;
  }

  public void setQualifyExprForClause(String dest, ASTNode ast) {
    destToQualify.put(dest, ast);
  }

  public ASTNode getQualifyExprForClause(String dest) {
    return destToQualify.get(dest);
  }

  public boolean hasQualifyClause() {
    return !destToQualify.isEmpty();
  }

  protected static enum ClauseType {
    CLUSTER_BY_CLAUSE,
    DISTRIBUTE_BY_CLAUSE,
    ORDER_BY_CLAUSE,
    SORT_BY_CLAUSE
  }

  public AnalyzeRewriteContext getAnalyzeRewrite() {
    return analyzeRewrite;
  }

  public void setAnalyzeRewrite(AnalyzeRewriteContext analyzeRewrite) {
    this.analyzeRewrite = analyzeRewrite;
  }

  /**
   * @return the isNoScanAnalyzeCommand
   */
  public boolean isNoScanAnalyzeCommand() {
    return isNoScanAnalyzeCommand;
  }

  /**
   * @param isNoScanAnalyzeCommand the isNoScanAnalyzeCommand to set
   */
  public void setNoScanAnalyzeCommand(boolean isNoScanAnalyzeCommand) {
    this.isNoScanAnalyzeCommand = isNoScanAnalyzeCommand;
  }

  /**
   * See also {@link #isInsertIntoTable(String)}
   */
  public Map<String, ASTNode> getInsertOverwriteTables() {
    return insertOverwriteTables;
  }

  public boolean hasInsertTables() {
    return this.insertIntoTables.size() > 0 || this.insertOverwriteTables.size() > 0;
  }

  public boolean isInsertOverwriteDirectory() {
    return isInsertOverwriteDir;
  }

  /**
   * Check whether all the expressions in the select clause are aggregate function calls.
   * This method starts iterating through the AST nodes representing the expressions in the select clause stored in
   * this object. An expression is considered to be an aggregate function call if:
   * <ul>
   *   <li>the AST node type is either TOK_FUNCTION, TOK_FUNCTIONDI or TOK_FUNCTIONSTAR</li>
   *   <li>the first child of the node is the name of the function</li>
   *   <li>function is registered in Hive</li>
   *   <li>the registered function with the specified name is a Generic User Defined Aggregate</li>
   * </ul>
   * If any of the mentioned criteria fails to match to the current expression this function returns false.
   * @return true if all the expressions in the select clause are aggregate function calls.
   * @throws SemanticException - thrown when {@link FunctionRegistry#getFunctionInfo} fails.
   */
  public boolean isFullyAggregate() throws SemanticException {
    for (ASTNode selectClause : destToSelExpr.values()) {
      for (Node node : selectClause.getChildren()) {
        ASTNode selexpr = (ASTNode) node;
        Tree expressionTypeToken = selexpr.getChild(0);
        int selectExprType = expressionTypeToken.getType();
        if (selectExprType != TOK_FUNCTION && selectExprType != TOK_FUNCTIONDI && selectExprType != TOK_FUNCTIONSTAR) {
          return false;
        }

        if (expressionTypeToken.getChild(0).getType() != HiveParser.Identifier) {
          return false;
        }
        String functionName = unescapeIdentifier(expressionTypeToken.getChild(0).getText());
        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName);
        if (functionInfo == null) {
          return false;
        }
        if (functionInfo.getGenericUDAFResolver() == null) {
          return false;
        }
      }
    }

    return true;
  }

  public ASTNode getColAliases() {
    return colAliases;
  }

  public void setColAliases(ASTNode colAliases) {
    this.colAliases = colAliases;
  }
}


