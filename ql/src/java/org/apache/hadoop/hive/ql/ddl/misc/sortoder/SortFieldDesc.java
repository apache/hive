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

package org.apache.hadoop.hive.ql.ddl.misc.sortoder;

public class SortFieldDesc {

  private String columnName;
  private NullOrder nullOrder;
  private SortDirection direction;

  public SortFieldDesc() {
  }
  
  public SortFieldDesc(String columnName, SortDirection direction, NullOrder nullOrder) {
    this.columnName = columnName;
    this.direction = direction;
    this.nullOrder = nullOrder;
  }

  public enum NullOrder {
    NULLS_FIRST, NULLS_LAST;
  }

  public enum SortDirection {
    ASC,
    DESC;
  }

  public String getColumnName() {
    return columnName;
  }

  public NullOrder getNullOrder() {
    return nullOrder;
  }

  public SortDirection getDirection() {
    return direction;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setNullOrder(NullOrder nullOrder) {
    this.nullOrder = nullOrder;
  }

  public void setDirection(SortDirection direction) {
    this.direction = direction;
  }

  @Override
  public String toString() {
    return String.format("{columnName:%s,direction:%s,nullOrder:%s}", columnName, direction, nullOrder);
  }
}
