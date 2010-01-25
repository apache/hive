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

import java.io.Serializable;

import org.apache.hadoop.fs.Path;

@Explain(displayName = "Show Partitions")
public class ShowPartitionsDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tabName;
  Path resFile;
  /**
   * table name for the result of show tables
   */
  private final String table = "showpartitions";
  /**
   * thrift ddl for the result of show tables
   */
  private final String schema = "partition#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * @param tabName
   *          Name of the table whose partitions need to be listed
   * @param resFile
   *          File to store the results in
   */
  public ShowPartitionsDesc(String tabName, Path resFile) {
    this.tabName = tabName;
    this.resFile = resFile;
  }

  /**
   * @return the name of the table
   */
  @Explain(displayName = "table")
  public String getTabName() {
    return tabName;
  }

  /**
   * @param tabName
   *          the table whose partitions have to be listed
   */
  public void setTabName(String tabName) {
    this.tabName = tabName;
  }

  /**
   * @return the results file
   */
  public Path getResFile() {
    return resFile;
  }

  @Explain(displayName = "result file", normalExplain = false)
  public String getResFileString() {
    return getResFile().getName();
  }

  /**
   * @param resFile
   *          the results file to be used to return the results
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
