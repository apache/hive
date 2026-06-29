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
    QUERY,
    JOIN,
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
    MAP_GROUP_BY,
    LATERAL_VIEW,
    MULTI_DEST_QUERY,
    FILTER_WITH_SUBQUERY,
    MATERIALIZED_VIEW,
    VIEW,
    CTAS,
    ANALYZE,
    NO_SCAN,
    REWRITE
  }

  int outerQueryLimit;

  private final EnumSet<QueryFeature> features = EnumSet.noneOf(QueryFeature.class);

  private boolean cboSupportedLateralViews = true;

  private int noOfJoins = 0;
  private int noOfOuterJoins = 0;

  private QueryType queryType = null;

  // set of used tables, aliases are resolved to real table names
  private Set<String> usedTables = new HashSet<>();

  public int getOuterQueryLimit() {
    return outerQueryLimit;
  }

  public void setOuterQueryLimit(int outerQueryLimit) {
    this.outerQueryLimit = outerQueryLimit;
  }

  public void incrementJoinCount(boolean outerJoin) {
    addFeature(QueryFeature.JOIN);
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

  public void markUnsupportedLateralViewsForCBO() {
    this.cboSupportedLateralViews = false;
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
    outerQueryLimit = -1;

    features.clear();
    cboSupportedLateralViews = true;

    noOfJoins = 0;
    noOfOuterJoins = 0;

    usedTables.clear();
  }
}
