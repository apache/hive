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

package org.apache.hadoop.hive.ql.parse;

public class SemiJoinHint {
  private String colName;
  private String target;
  private Integer numEntries;

  public SemiJoinHint(String colName, String target, Integer numEntries) {
    this.colName = colName;
    this.target = target;
    this.numEntries = numEntries;
  }

  public String getColName() {
    return colName;
  }
  public String getTarget() {
    return target;
  }
  public Integer getNumEntries() {
    return numEntries != null ? numEntries : -1;
  }

  @Override
  public String toString() {
    return "colName=" + colName + ", target=" + target + ", numEntries=" + numEntries;
  }
}
