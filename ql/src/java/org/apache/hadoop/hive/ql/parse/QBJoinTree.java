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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Internal representation of the join tree.
 *
 */
public class QBJoinTree implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private String leftAlias;
  private String[] rightAliases;
  private String[] leftAliases;
  private QBJoinTree joinSrc;
  private String[] baseSrc;
  private int nextTag;
  private JoinCond[] joinCond;
  private boolean noOuterJoin;
  private boolean noSemiJoin;
  private Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo;

  // The subquery identifier from QB.
  // It is of the form topSubQuery:innerSubQuery:....:innerMostSubQuery
  private String id;

  // keeps track of the right-hand-side table name of the left-semi-join, and
  // its list of join keys
  private transient final HashMap<String, ArrayList<ASTNode>> rhsSemijoin;

  // join conditions
  private transient ArrayList<ArrayList<ASTNode>> expressions;

  // key index to nullsafe join flag
  private ArrayList<Boolean> nullsafes;

  // filters
  private transient ArrayList<ArrayList<ASTNode>> filters;

  // outerjoin-pos = other-pos:filter-len, other-pos:filter-len, ...
  private int[][] filterMap;

  // filters for pushing
  private transient ArrayList<ArrayList<ASTNode>> filtersForPushing;

  // user asked for map-side join
  private boolean mapSideJoin;
  private List<String> mapAliases;

  // big tables that should be streamed
  private List<String> streamAliases;

  /*
   * when a QBJoinTree is merged into this one, its left(pos =0) filters can
   * refer to any of the srces in this QBJoinTree. If a particular filterForPushing refers
   * to multiple srces in this QBJoinTree, we collect them into 'postJoinFilters'
   * We then add a Filter Operator after the Join Operator for this QBJoinTree.
   */
  private final List<ASTNode> postJoinFilters;

  /**
   * constructor.
   */
  public QBJoinTree() {
    nextTag = 0;
    noOuterJoin = true;
    noSemiJoin = true;
    rhsSemijoin = new HashMap<String, ArrayList<ASTNode>>();
    aliasToOpInfo = new HashMap<String, Operator<? extends OperatorDesc>>();
    postJoinFilters = new ArrayList<ASTNode>();
  }

  /**
   * returns left alias if any - this is used for merging later on.
   *
   * @return left alias if any
   */
  public String getLeftAlias() {
    return leftAlias;
  }

  /**
   * set left alias for the join expression.
   *
   * @param leftAlias
   *          String
   */
  public void setLeftAlias(String leftAlias) {
    if ( this.leftAlias != null && !this.leftAlias.equals(leftAlias) ) {
      this.leftAlias = null;
    } else {
      this.leftAlias = leftAlias;
    }
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public ArrayList<ArrayList<ASTNode>> getExpressions() {
    return expressions;
  }

  public void setExpressions(ArrayList<ArrayList<ASTNode>> expressions) {
    this.expressions = expressions;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public QBJoinTree getJoinSrc() {
    return joinSrc;
  }

  public void setJoinSrc(QBJoinTree joinSrc) {
    this.joinSrc = joinSrc;
  }

  public int getNextTag() {
    return nextTag++;
  }

  public JoinCond[] getJoinCond() {
    return joinCond;
  }

  public void setJoinCond(JoinCond[] joinCond) {
    this.joinCond = joinCond;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  public boolean getNoSemiJoin() {
    return noSemiJoin;
  }

  public void setNoSemiJoin(boolean semi) {
    noSemiJoin = semi;
  }

  /**
   * @return the filters
   */
  public ArrayList<ArrayList<ASTNode>> getFilters() {
    return filters;
  }

  /**
   * @param filters
   *          the filters to set
   */
  public void setFilters(ArrayList<ArrayList<ASTNode>> filters) {
    this.filters = filters;
  }

  /**
   * @return the filters for pushing
   */
  public ArrayList<ArrayList<ASTNode>> getFiltersForPushing() {
    return filtersForPushing;
  }

  /**
   * @param filters for pushing
   *          the filters to set
   */
  public void setFiltersForPushing(ArrayList<ArrayList<ASTNode>> filters) {
    this.filtersForPushing = filters;
  }

  /**
   * @return the mapSidejoin
   */
  public boolean isMapSideJoin() {
    return mapSideJoin;
  }

  /**
   * @param mapSideJoin
   *          the mapSidejoin to set
   */
  public void setMapSideJoin(boolean mapSideJoin) {
    this.mapSideJoin = mapSideJoin;
  }

  /**
   * @return the mapAliases
   */
  public List<String> getMapAliases() {
    return mapAliases;
  }

  /**
   * @param mapAliases
   *          the mapAliases to set
   */
  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }

  public List<String> getStreamAliases() {
    return streamAliases;
  }

  public void setStreamAliases(List<String> streamAliases) {
    this.streamAliases = streamAliases;
  }

  /**
   * Insert only a key to the semijoin table name to column names map.
   *
   * @param alias
   *          table name alias.
   */
  public void addRHSSemijoin(String alias) {
    if (!rhsSemijoin.containsKey(alias)) {
      rhsSemijoin.put(alias, null);
    }
  }

  /**
   * Remeber the mapping of table alias to set of columns.
   *
   * @param alias
   * @param columns
   */
  public void addRHSSemijoinColumns(String alias, ArrayList<ASTNode> columns) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if (cols == null) {
      rhsSemijoin.put(alias, columns);
    } else {
      cols.addAll(columns);
    }
  }

  /**
   * Remeber the mapping of table alias to set of columns.
   *
   * @param alias
   * @param column
   */
  public void addRHSSemijoinColumns(String alias, ASTNode column) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if (cols == null) {
      cols = new ArrayList<ASTNode>();
      cols.add(column);
      rhsSemijoin.put(alias, cols);
    } else {
      cols.add(column);
    }
  }

  public ArrayList<ASTNode> getRHSSemijoinColumns(String alias) {
    return rhsSemijoin.get(alias);
  }

  /**
   * Merge the rhs tables from another join tree.
   *
   * @param src
   *          the source join tree
   */
  public void mergeRHSSemijoin(QBJoinTree src) {
    for (Entry<String, ArrayList<ASTNode>> e : src.rhsSemijoin.entrySet()) {
      String key = e.getKey();
      ArrayList<ASTNode> value = rhsSemijoin.get(key);
      if (value == null) {
        rhsSemijoin.put(key, e.getValue());
      } else {
        value.addAll(e.getValue());
      }
    }
  }

  public ArrayList<Boolean> getNullSafes() {
    return nullsafes;
  }

  public void setNullSafes(ArrayList<Boolean> nullSafes) {
    this.nullsafes = nullSafes;
  }

  public void addFilterMapping(int outer, int target, int length) {
    filterMap[outer] = new int[] { target, length };
  }

  public int[][] getFilterMap() {
    return filterMap;
  }

  public void setFilterMap(int[][] filterMap) {
    this.filterMap = filterMap;
  }

  public Map<String, Operator<? extends OperatorDesc>> getAliasToOpInfo() {
    return aliasToOpInfo;
  }

  public void setAliasToOpInfo(Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo) {
    this.aliasToOpInfo = aliasToOpInfo;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void addPostJoinFilter(ASTNode filter) {
    postJoinFilters.add(filter);
  }

  public List<ASTNode> getPostJoinFilters() {
    return postJoinFilters;
  }

  @Override
  public QBJoinTree clone() throws CloneNotSupportedException {
    QBJoinTree cloned = new QBJoinTree();

    // shallow copy aliasToOpInfo, we won't want to clone the operator tree here
    cloned.setAliasToOpInfo(aliasToOpInfo == null ? null :
        new HashMap<String, Operator<? extends OperatorDesc>>(aliasToOpInfo));

    cloned.setBaseSrc(baseSrc == null ? null : baseSrc.clone());

    // shallow copy ASTNode
    cloned.setExpressions(expressions);
    cloned.setFilters(filters);
    cloned.setFiltersForPushing(filtersForPushing);

    // clone filterMap
    int[][] clonedFilterMap = filterMap == null ? null : new int[filterMap.length][];
    if (filterMap != null) {
      for (int i = 0; i < filterMap.length; i++) {
        clonedFilterMap[i] = filterMap[i] == null ? null : filterMap[i].clone();
      }
    }
    cloned.setFilterMap(clonedFilterMap);

    cloned.setId(id);

    // clone joinCond
    JoinCond[] clonedJoinCond = joinCond == null ? null : new JoinCond[joinCond.length];
    if (joinCond != null) {
      for (int i = 0; i < joinCond.length; i++) {
        if(joinCond[i] == null) {
          continue;
        }
        JoinCond clonedCond = new JoinCond();
        clonedCond.setJoinType(joinCond[i].getJoinType());
        clonedCond.setLeft(joinCond[i].getLeft());
        clonedCond.setPreserved(joinCond[i].getPreserved());
        clonedCond.setRight(joinCond[i].getRight());
        clonedJoinCond[i] = clonedCond;
      }
    }
    cloned.setJoinCond(clonedJoinCond);

    cloned.setJoinSrc(joinSrc == null ? null : joinSrc.clone());
    cloned.setLeftAlias(leftAlias);
    cloned.setLeftAliases(leftAliases == null ? null : leftAliases.clone());
    cloned.setMapAliases(mapAliases == null ? null : new ArrayList<String>(mapAliases));
    cloned.setMapSideJoin(mapSideJoin);
    cloned.setNoOuterJoin(noOuterJoin);
    cloned.setNoSemiJoin(noSemiJoin);
    cloned.setNullSafes(nullsafes == null ? null : new ArrayList<Boolean>(nullsafes));
    cloned.setRightAliases(rightAliases == null ? null : rightAliases.clone());
    cloned.setStreamAliases(streamAliases == null ? null : new ArrayList<String>(streamAliases));

    // clone postJoinFilters
    for (ASTNode filter : postJoinFilters) {
      cloned.getPostJoinFilters().add(filter);
    }
    // clone rhsSemijoin
    for (Entry<String, ArrayList<ASTNode>> entry : rhsSemijoin.entrySet()) {
      cloned.addRHSSemijoinColumns(entry.getKey(), entry.getValue());
    }

    return cloned;
  }
}
