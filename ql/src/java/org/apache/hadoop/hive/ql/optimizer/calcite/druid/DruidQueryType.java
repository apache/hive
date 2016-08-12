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
package org.apache.hadoop.hive.ql.optimizer.calcite.druid;

/**
 * Type of Druid query.
 *
 * TODO: to be removed when Calcite is upgraded to 1.9
 */
public enum DruidQueryType {
  SELECT("select"),
  TOP_N("topN"),
  GROUP_BY("groupBy"),
  TIMESERIES("timeseries");

  private final String queryName;

  private DruidQueryType(String queryName) {
    this.queryName = queryName;
  }

  public String getQueryName() {
    return this.queryName;
  }
}

// End QueryType.java