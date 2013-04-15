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

package org.apache.hadoop.hive.ql.plan;


/**
 * LimitDesc.
 *
 */
@Explain(displayName = "Limit")
public class LimitDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private int limit;
  private int leastRows = -1;

  public LimitDesc() {
  }

  public LimitDesc(final int limit) {
    this.limit = limit;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  public int getLeastRows() {
    return leastRows;
  }

  public void setLeastRows(int leastRows) {
    this.leastRows = leastRows;
  }

}
