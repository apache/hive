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

import java.util.List;

/**
 * NamedColsInJoin encapsulates information about using clause of join. e.g.,
 * select * from a join b using(c); This class describes tables a and b, column
 * c and join type.
 *
 */
public class NamedJoinInfo {
  private List<String> tableAliases;
  private List<String> namedColumns;
  private JoinType hiveJoinType;

  public NamedJoinInfo(List<String> aliases, List<String> namedColumns, JoinType hiveJoinType) {
    super();
    this.tableAliases = aliases;
    this.namedColumns = namedColumns;
    this.hiveJoinType = hiveJoinType;
  }

  public List<String> getAliases() {
    return tableAliases;
  }

  public void setAliases(List<String> aliases) {
    this.tableAliases = aliases;
  }

  public List<String> getNamedColumns() {
    return namedColumns;
  }

  public void setNamedColumns(List<String> namedColumns) {
    this.namedColumns = namedColumns;
  }

  public JoinType getHiveJoinType() {
    return hiveJoinType;
  }

  public void setHiveJoinType(JoinType hiveJoinType) {
    this.hiveJoinType = hiveJoinType;
  }

}
