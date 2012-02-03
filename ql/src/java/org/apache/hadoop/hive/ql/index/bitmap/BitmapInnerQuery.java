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

package org.apache.hadoop.hive.ql.index.bitmap;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapQuery;

/**
 * Representation of inner bitmap index SELECT query that scans bitmap index
 * tables for a pushed predicate
 */
public class BitmapInnerQuery implements BitmapQuery {
  private String tableName;
  private ExprNodeDesc predicate;
  private String alias;
  private String queryStr;

  public BitmapInnerQuery(String tableName, ExprNodeDesc predicate, String alias) {
    this.tableName = tableName;
    this.predicate = predicate;
    this.alias = alias;
    constructQueryStr();
  }

  /**
   * Return a string representation of the query string for compilation
   */
  public String toString() {
    return queryStr;
  }

  /**
   * Construct a string representation of the query to be compiled
   */
  private  void constructQueryStr() {
    StringBuilder sb = new StringBuilder();
    sb.append("(SELECT * FROM ");
    sb.append(HiveUtils.unparseIdentifier(tableName));
    sb.append(" WHERE ");
    sb.append(predicate.getExprString());
    sb.append(") ");
    sb.append(alias);
    queryStr = sb.toString();
  }

  /**
   * Return the assigned alias of the SELECT statement
   */
  public String getAlias() {
    return alias;
  }

}
