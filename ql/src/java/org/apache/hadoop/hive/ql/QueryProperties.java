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

package org.apache.hadoop.hive.ql;


import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * QueryProperties.
 *
 * A structure to contain features of a query that are determined
 * during parsing and may be useful for categorizing a query type
 *
 * These include whether the query contains:
 * a join clause, a group by clause, an order by clause, a sort by
 * clause, a group by clause following a join clause, and whether
 * the query uses a script for mapping/reducing
 */
public class QueryProperties {
  public enum QueryType {
    DQL("DQL"),
    DML("DML"),
    DDL("DDL"),
    DCL("DCL"),
    // strictly speaking, "ANALYZE TABLE" is DDL because it collects and stores metadata or statistical information,
    // but in Hive it's a special statement which is worth a separate query type
    STATS("STATS"),
    OTHER("");

    private final String name;

    QueryType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public enum QueryFeature {
    GROUP_BY,
    ORDER_BY,
    OUTER_ORDER_BY,
    SORT_BY,
    LIMIT,
    JOIN_FOLLOWED_BY_GROUP_BY,
    PTF,
    WINDOWING,
    QUALIFY,
    EXCEPT,
    INTERSECT,
    USES_SCRIPT,
    DISTRIBUTE_BY,
    CLUSTER_BY,
    MAP_GROUP_BY
  }

  boolean query;
  boolean analyzeCommand;
  boolean noScanAnalyzeCommand;
  boolean analyzeRewrite;
  boolean ctas;
  int outerQueryLimit;

  boolean hasJoin = false;
  private final EnumSet<QueryFeature> features = EnumSet.noneOf(QueryFeature.class);
  boolean mapJoinRemoved = false;

  private boolean hasLateralViews = false;
  private boolean cboSupportedLateralViews = true;

  private int noOfJoins = 0;
  private int noOfOuterJoins = 0;

  private boolean multiDestQuery;
  private boolean filterWithSubQuery;

  // True if this statement creates or replaces a materialized view
  private boolean isMaterializedView;
  private boolean isView;

  private QueryType queryType = null;

  // set of used tables, aliases are resolved to real table names
  private Set<String> usedTables = new HashSet<>();

  public boolean isQuery() {
    return query;
  }

  public void setQuery(boolean query) {
    this.query = query;
  }

  /**
   * The return value of either isAnalyzeCommand() or isAnalyzeRewrite() is always true for analyze commands:
   * isAnalyzeCommand=true for "compute statistics",
   * isAnalyzeRewrite=true for "compute statistics for columns".
   *
   * @return whether the query is an ANALYZE TABLE query
   */
  public boolean isAnalyze() {
    return isAnalyzeCommand() || isAnalyzeRewrite();
  }

  public boolean isAnalyzeCommand() {
    return analyzeCommand;
  }

  public void setAnalyzeCommand(boolean analyzeCommand) {
    this.analyzeCommand = analyzeCommand;
  }

  public boolean isNoScanAnalyzeCommand() {
    return noScanAnalyzeCommand;
  }

  public void setNoScanAnalyzeCommand(boolean noScanAnalyzeCommand) {
    this.noScanAnalyzeCommand = noScanAnalyzeCommand;
  }

  public boolean isAnalyzeRewrite() {
    return analyzeRewrite;
  }

  public void setAnalyzeRewrite(boolean analyzeRewrite) {
    this.analyzeRewrite = analyzeRewrite;
  }

  public boolean isCTAS() {
    return ctas;
  }

  public void setCTAS(boolean ctas) {
    this.ctas = ctas;
  }

  public int getOuterQueryLimit() {
    return outerQueryLimit;
  }

  public void setOuterQueryLimit(int outerQueryLimit) {
    this.outerQueryLimit = outerQueryLimit;
  }

  public boolean hasJoin() {
    return (noOfJoins > 0);
  }

  public void incrementJoinCount(boolean outerJoin) {
    noOfJoins++;
    if (outerJoin) {
      noOfOuterJoins++;
    }
  }

  public int getJoinCount() {
    return noOfJoins;
  }

  public int getOuterJoinCount() {
    return noOfOuterJoins;
  }

  public void setHasLateralViews(boolean hasLateralViews) {
    this.hasLateralViews = hasLateralViews;
  }

  public boolean hasLateralViews() {
    return hasLateralViews;
  }

  public void setCBOSupportedLateralViews(boolean cboSupportedLateralViews) {
    this.cboSupportedLateralViews = cboSupportedLateralViews;
  }

  public boolean isCBOSupportedLateralViews() {
    return cboSupportedLateralViews;
  }

  public void addFeature(QueryFeature feature) {
    features.add(feature);
  }

  public boolean hasFeature(QueryFeature feature) {
    return features.contains(feature);
  }

  public boolean hasGroupBy() {
    return hasFeature(QueryFeature.GROUP_BY);
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    setFeature(QueryFeature.GROUP_BY, hasGroupBy);
  }

  public boolean hasOrderBy() {
    return hasFeature(QueryFeature.ORDER_BY);
  }

  public void setHasOrderBy(boolean hasOrderBy) {
    setFeature(QueryFeature.ORDER_BY, hasOrderBy);
  }

  public boolean hasOuterOrderBy() {
    return hasFeature(QueryFeature.OUTER_ORDER_BY);
  }

  public void setHasOuterOrderBy(boolean hasOuterOrderBy) {
    setFeature(QueryFeature.OUTER_ORDER_BY, hasOuterOrderBy);
  }

  public boolean hasSortBy() {
    return hasFeature(QueryFeature.SORT_BY);
  }

  public void setHasSortBy(boolean hasSortBy) {
    setFeature(QueryFeature.SORT_BY, hasSortBy);
  }

  public void setHasLimit(boolean hasLimit) {
    setFeature(QueryFeature.LIMIT, hasLimit);
  }

  public boolean hasLimit() {
    return hasFeature(QueryFeature.LIMIT);
  }

  public boolean hasJoinFollowedByGroupBy() {
    return hasFeature(QueryFeature.JOIN_FOLLOWED_BY_GROUP_BY);
  }

  public void setHasJoinFollowedByGroupBy(boolean hasJoinFollowedByGroupBy) {
    setFeature(QueryFeature.JOIN_FOLLOWED_BY_GROUP_BY, hasJoinFollowedByGroupBy);
  }

  public boolean usesScript() {
    return hasFeature(QueryFeature.USES_SCRIPT);
  }

  public void setUsesScript(boolean usesScript) {
    setFeature(QueryFeature.USES_SCRIPT, usesScript);
  }

  public boolean hasDistributeBy() {
    return hasFeature(QueryFeature.DISTRIBUTE_BY);
  }

  public void setHasDistributeBy(boolean hasDistributeBy) {
    setFeature(QueryFeature.DISTRIBUTE_BY, hasDistributeBy);
  }

  public boolean hasClusterBy() {
    return hasFeature(QueryFeature.CLUSTER_BY);
  }

  public void setHasClusterBy(boolean hasClusterBy) {
    setFeature(QueryFeature.CLUSTER_BY, hasClusterBy);
  }

  public boolean hasPTF() {
    return hasFeature(QueryFeature.PTF);
  }

  public void setHasPTF(boolean hasPTF) {
    setFeature(QueryFeature.PTF, hasPTF);
  }

  public boolean hasWindowing() {
    return hasFeature(QueryFeature.WINDOWING);
  }

  public void setHasWindowing(boolean hasWindowing) {
    setFeature(QueryFeature.WINDOWING, hasWindowing);
  }

  public boolean hasQualify() {
    return hasFeature(QueryFeature.QUALIFY);
  }

  public void setHasQualify(boolean hasQualify) {
    setFeature(QueryFeature.QUALIFY, hasQualify);
  }

  public boolean hasExcept() {
    return hasFeature(QueryFeature.EXCEPT);
  }

  public void setHasExcept(boolean hasExcept) {
    setFeature(QueryFeature.EXCEPT, hasExcept);
  }

  public boolean hasIntersect() {
    return hasFeature(QueryFeature.INTERSECT);
  }

  public void setHasIntersect(boolean hasIntersect) {
    setFeature(QueryFeature.INTERSECT, hasIntersect);
  }

  public boolean isMapJoinRemoved() {
    return mapJoinRemoved;
  }

  public void setMapJoinRemoved(boolean mapJoinRemoved) {
    this.mapJoinRemoved = mapJoinRemoved;
  }

  public boolean isHasMapGroupBy() {
    return hasFeature(QueryFeature.MAP_GROUP_BY);
  }

  public void setHasMapGroupBy(boolean hasMapGroupBy) {
    setFeature(QueryFeature.MAP_GROUP_BY, hasMapGroupBy);
  }

  public boolean hasMultiDestQuery() {
    return this.multiDestQuery;
  }

  public void setMultiDestQuery(boolean multiDestQuery) {
    this.multiDestQuery = multiDestQuery;
  }

  public void setFilterWithSubQuery(boolean filterWithSubQuery) {
    this.filterWithSubQuery = filterWithSubQuery;
  }

  public boolean hasFilterWithSubQuery() {
    return this.filterWithSubQuery;
  }

  /**
   * True indicates this statement create or replaces a materialized view, not that it is a query
   * against a materialized view.
   */
  public boolean isMaterializedView() {
    return isMaterializedView;
  }

  public void setMaterializedView(boolean isMaterializedView) {
    this.isMaterializedView = isMaterializedView;
  }

  public boolean isView() {
    return isView;
  }

  public void setView(boolean view) {
    isView = view;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public Set<String> getUsedTables() {
    return usedTables;
  }

  public void setUsedTables(Set<String> usedTables) {
    this.usedTables = usedTables;
  }

  public void clear() {
    query = false;
    analyzeCommand = false;
    noScanAnalyzeCommand = false;
    analyzeRewrite = false;
    ctas = false;
    outerQueryLimit = -1;
    isMaterializedView = false;

    hasJoin = false;
    features.clear();
    mapJoinRemoved = false;

    noOfJoins = 0;
    noOfOuterJoins = 0;

    multiDestQuery = false;
    filterWithSubQuery = false;

    usedTables.clear();
  }

  private void setFeature(QueryFeature feature, boolean enabled) {
    if (enabled) {
      addFeature(feature);
    } else {
      features.remove(feature);
    }
  }
}
