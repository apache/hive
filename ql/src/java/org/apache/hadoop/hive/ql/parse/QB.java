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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

/**
 * Implementation of the query block.
 *
 **/

public class QB {

  private static final Log LOG = LogFactory.getLog("hive.ql.parse.QB");

  private final int numJoins = 0;
  private final int numGbys = 0;
  private int numSels = 0;
  private int numSelDi = 0;
  private HashMap<String, String> aliasToTabs;
  private HashMap<String, QBExpr> aliasToSubq;
  private HashMap<String, Map<String, String>> aliasToProps;
  private List<String> aliases;
  private QBParseInfo qbp;
  private QBMetaData qbm;
  private QBJoinTree qbjoin;
  private String id;
  private boolean isQuery;
  private boolean isAnalyzeRewrite;
  private CreateTableDesc tblDesc = null; // table descriptor of the final
  private CreateTableDesc localDirectoryDesc = null ;

  // used by PTFs
  /*
   * This map maintains the PTFInvocationSpec for each PTF chain invocation in this QB.
   */
  private HashMap<ASTNode, PTFInvocationSpec> ptfNodeToSpec;
  /*
   * the WindowingSpec used for windowing clauses in this QB.
   */
  private HashMap<String, WindowingSpec> destToWindowingSpec;

  /*
   * If this QB represents a SubQuery predicate then this will point to the SubQuery object.
   */
  private QBSubQuery subQueryPredicateDef;

  // results

  public void print(String msg) {
    LOG.info(msg + "alias=" + qbp.getAlias());
    for (String alias : getSubqAliases()) {
      QBExpr qbexpr = getSubqForAlias(alias);
      LOG.info(msg + "start subquery " + alias);
      qbexpr.print(msg + " ");
      LOG.info(msg + "end subquery " + alias);
    }
  }

  public QB() {
  }

  public QB(String outer_id, String alias, boolean isSubQ) {
    aliasToTabs = new HashMap<String, String>();
    aliasToSubq = new HashMap<String, QBExpr>();
    aliasToProps = new HashMap<String, Map<String, String>>();
    aliases = new ArrayList<String>();
    if (alias != null) {
      alias = alias.toLowerCase();
    }
    qbp = new QBParseInfo(alias, isSubQ);
    qbm = new QBMetaData();
    ptfNodeToSpec = new HashMap<ASTNode, PTFInvocationSpec>();
    destToWindowingSpec = new HashMap<String, WindowingSpec>();
    id = getAppendedAliasFromId(outer_id, alias);
  }

  // For sub-queries, the id. and alias should be appended since same aliases can be re-used
  // within different sub-queries.
  // For a query like:
  // select ...
  //   (select * from T1 a where ...) subq1
  //  join
  //   (select * from T2 a where ...) subq2
  // ..
  // the alias is modified to subq1:a and subq2:a from a, to identify the right sub-query.
  public static String getAppendedAliasFromId(String outer_id, String alias) {
    return (outer_id == null ? alias : outer_id + ":" + alias);
  }

  public QBParseInfo getParseInfo() {
    return qbp;
  }

  public QBMetaData getMetaData() {
    return qbm;
  }

  public void setQBParseInfo(QBParseInfo qbp) {
    this.qbp = qbp;
  }

  public void countSelDi() {
    numSelDi++;
  }

  public void countSel() {
    numSels++;
  }

  public boolean exists(String alias) {
    alias = alias.toLowerCase();
    if (aliasToTabs.get(alias) != null || aliasToSubq.get(alias) != null) {
      return true;
    }

    return false;
  }

  public void setTabAlias(String alias, String tabName) {
    aliasToTabs.put(alias.toLowerCase(), tabName);
  }

  public void setSubqAlias(String alias, QBExpr qbexpr) {
    aliasToSubq.put(alias.toLowerCase(), qbexpr);
  }

  public void setTabProps(String alias, Map<String, String> props) {
    aliasToProps.put(alias.toLowerCase(), props);
  }

  public void addAlias(String alias) {
    if (!aliases.contains(alias.toLowerCase())) {
      aliases.add(alias.toLowerCase());
    }
  }

  public String getId() {
    return id;
  }

  public int getNumGbys() {
    return numGbys;
  }

  public int getNumSelDi() {
    return numSelDi;
  }

  public int getNumSels() {
    return numSels;
  }

  public int getNumJoins() {
    return numJoins;
  }

  public Set<String> getSubqAliases() {
    return aliasToSubq.keySet();
  }

  public Set<String> getTabAliases() {
    return aliasToTabs.keySet();
  }

  public List<String> getAliases() {
    return aliases;
  }

  public QBExpr getSubqForAlias(String alias) {
    return aliasToSubq.get(alias.toLowerCase());
  }

  public String getTabNameForAlias(String alias) {
    return aliasToTabs.get(alias.toLowerCase());
  }

  public Map<String, String> getTabPropsForAlias(String alias) {
    return aliasToProps.get(alias.toLowerCase());
  }

  public void rewriteViewToSubq(String alias, String viewName, QBExpr qbexpr) {
    alias = alias.toLowerCase();
    String tableName = aliasToTabs.remove(alias);
    assert (viewName.equals(tableName));
    aliasToSubq.put(alias, qbexpr);
  }

  public QBJoinTree getQbJoinTree() {
    return qbjoin;
  }

  public void setQbJoinTree(QBJoinTree qbjoin) {
    this.qbjoin = qbjoin;
  }

  public void setIsQuery(boolean isQuery) {
    this.isQuery = isQuery;
  }

  public boolean getIsQuery() {
    return isQuery;
  }

  public boolean isSimpleSelectQuery() {
    return qbp.isSimpleSelectQuery() && aliasToSubq.isEmpty() && !isCTAS() &&
        !qbp.isAnalyzeCommand();
  }

  public boolean hasTableSample(String alias) {
    return qbp.getTabSample(alias) != null;
  }

  public CreateTableDesc getTableDesc() {
    return tblDesc;
  }

  public void setTableDesc(CreateTableDesc desc) {
    tblDesc = desc;
  }

  public CreateTableDesc getLLocalDirectoryDesc() {
    return localDirectoryDesc;
  }

  public void setLocalDirectoryDesc(CreateTableDesc localDirectoryDesc) {
    this.localDirectoryDesc = localDirectoryDesc;
  }

  /**
   * Whether this QB is for a CREATE-TABLE-AS-SELECT.
   */
  public boolean isCTAS() {
    return tblDesc != null;
  }

  /**
   * Retrieve skewed column name for a table.
   * @param alias table alias
   * @return
   */
  public List<String> getSkewedColumnNames(String alias) {
    List<String> skewedColNames = null;
    if (null != qbm &&
        null != qbm.getAliasToTable() &&
            qbm.getAliasToTable().size() > 0) {
      Table tbl = getMetaData().getTableForAlias(alias);
      skewedColNames = tbl.getSkewedColNames();
    }
    return skewedColNames;

  }

  public boolean isAnalyzeRewrite() {
    return isAnalyzeRewrite;
  }

  public void setAnalyzeRewrite(boolean isAnalyzeRewrite) {
    this.isAnalyzeRewrite = isAnalyzeRewrite;
  }

  public PTFInvocationSpec getPTFInvocationSpec(ASTNode node) {
    return ptfNodeToSpec == null ? null : ptfNodeToSpec.get(node);
  }

  public void addPTFNodeToSpec(ASTNode node, PTFInvocationSpec spec) {
    ptfNodeToSpec = ptfNodeToSpec == null ? new HashMap<ASTNode, PTFInvocationSpec>() : ptfNodeToSpec;
    ptfNodeToSpec.put(node, spec);
  }

  public HashMap<ASTNode, PTFInvocationSpec> getPTFNodeToSpec() {
    return ptfNodeToSpec;
  }

  public WindowingSpec getWindowingSpec(String dest) {
    return destToWindowingSpec.get(dest);
  }

  public void addDestToWindowingSpec(String dest, WindowingSpec windowingSpec) {
    destToWindowingSpec.put(dest, windowingSpec);
  }

  public boolean hasWindowingSpec(String dest) {
    return destToWindowingSpec.get(dest) != null;
  }

  public HashMap<String, WindowingSpec> getAllWindowingSpecs() {
    return destToWindowingSpec;
  }

  protected void setSubQueryDef(QBSubQuery subQueryPredicateDef) {
    this.subQueryPredicateDef = subQueryPredicateDef;
  }

  protected QBSubQuery getSubQueryPredicateDef() {
    return subQueryPredicateDef;
  }

}
