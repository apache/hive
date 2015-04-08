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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowPartitionsDesc.
 *
 */
@Explain(displayName = "Show Partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowPartitionsDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tabName;
  String resFile;
  // Filter the partitions to show based on on supplied spec
  Map<String, String> partSpec;

  /**
   * table name for the result of show tables.
   */
  private static final String table = "showpartitions";
  /**
   * thrift ddl for the result of show tables.
   */
  private static final String schema = "partition#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  public ShowPartitionsDesc() {
  }

  /**
   * @param tabName
   *          Name of the table whose partitions need to be listed.
   * @param resFile
   *          File to store the results in
   */
  public ShowPartitionsDesc(String tabName, Path resFile,
      Map<String, String> partSpec) {
    this.tabName = tabName;
    this.resFile = resFile.toString();
    this.partSpec = partSpec;
  }

  /**
   * @return the name of the table.
   */
  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
   * @return the name of the table.
   */
  @Explain(displayName = "partSpec", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec the partSpec to set.
   */
  public void setPartSpec(Map<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return the results file
   */
  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          the results file to be used to return the results
   */
  public void setResFile(String resFile) {
    this.resFile = resFile;
  }
}
