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

import java.util.Vector;
import java.util.List;

/**
 * Internal representation of the join tree
 *
 */
public class QBJoinTree 
{
  private String        leftAlias;
  private String[]      rightAliases;
  private String[]      leftAliases;
  private QBJoinTree    joinSrc;
  private String[]      baseSrc;
  private int           nextTag;
  private joinCond[]    joinCond;
  private boolean       noOuterJoin;
  
  // join conditions
  private Vector<Vector<ASTNode>> expressions;

  // filters
  private Vector<Vector<ASTNode>> filters;

  // user asked for map-side join
  private  boolean        mapSideJoin;
  private  List<String>   mapAliases;
  
  /**
   * constructor 
   */
  public QBJoinTree() { nextTag = 0;}

  /**
   * returns left alias if any - this is used for merging later on
   * @return left alias if any
   */
  public String getLeftAlias() {
    return leftAlias;
  }

  /**
   * set left alias for the join expression
   * @param leftAlias String
   */
  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
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

  public Vector<Vector<ASTNode>> getExpressions() {
    return expressions;
  }

  public void setExpressions(Vector<Vector<ASTNode>> expressions) {
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

  public String getJoinStreamDesc() {
    return "$INTNAME";
  }

  public joinCond[] getJoinCond() {
    return joinCond;
  }

  public void setJoinCond(joinCond[] joinCond) {
    this.joinCond = joinCond;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

	/**
	 * @return the filters
	 */
	public Vector<Vector<ASTNode>> getFilters() {
		return filters;
	}

	/**
	 * @param filters the filters to set
	 */
	public void setFilters(Vector<Vector<ASTNode>> filters) {
		this.filters = filters;
	}

  /**
   * @return the mapSidejoin
   */
  public boolean isMapSideJoin() {
    return mapSideJoin;
  }

  /**
   * @param mapSideJoin the mapSidejoin to set
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
   * @param mapAliases the mapAliases to set
   */
  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }
}


