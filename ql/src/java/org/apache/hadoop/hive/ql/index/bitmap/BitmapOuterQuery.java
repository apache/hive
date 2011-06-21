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
import org.apache.hadoop.hive.ql.index.bitmap.BitmapQuery;

/**
 * Representation of the outer query on bitmap indexes that JOINs the result of
 * inner SELECT scans on bitmap indexes (represented in BitmapQuery objects)
 * using EWAH_* bitwise operations
 */
public class BitmapOuterQuery implements BitmapQuery {
  private String alias;
  private BitmapQuery lhs;
  private BitmapQuery rhs;
  private String queryStr;

  public BitmapOuterQuery(String alias, BitmapQuery lhs, BitmapQuery rhs) {
    this.alias = alias;
    this.lhs = lhs;
    this.rhs = rhs;
    constructQueryStr();
  }

  public String getAlias() {
    return alias;
  }

  /**
   * Return a string representation of the query for compilation
   */
  public String toString() {
    return queryStr;
  }

  /**
   * Construct a string representation of the query to be compiled
   */
  private void constructQueryStr() {
    StringBuilder sb = new StringBuilder();
    sb.append("(SELECT ");
    sb.append(lhs.getAlias());
    sb.append(".`_bucketname`, ");
    sb.append(rhs.getAlias());
    sb.append(".`_offset`, ");
    sb.append("EWAH_BITMAP_AND(");
    sb.append(lhs.getAlias());
    sb.append(".`_bitmaps`, ");
    sb.append(rhs.getAlias());
    sb.append(".`_bitmaps`) AS `_bitmaps` FROM ");
    sb.append(lhs.toString());
    sb.append(" JOIN ");
    sb.append(rhs.toString());
    sb.append(" ON ");
    sb.append(lhs.getAlias());
    sb.append(".`_bucketname` = ");
    sb.append(rhs.getAlias());
    sb.append(".`_bucketname` AND ");
    sb.append(lhs.getAlias());
    sb.append(".`_offset` = ");
    sb.append(rhs.getAlias());
    sb.append(".`_offset`) ");
    sb.append(this.alias);
    queryStr = sb.toString();
  }

}
