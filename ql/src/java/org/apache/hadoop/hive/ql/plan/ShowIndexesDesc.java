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

/**
 * ShowIndexesDesc.
 * Returns table index information per SQL syntax.
 */
@Explain(displayName = "Show Indexes")
public class ShowIndexesDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tableName;
  String resFile;
  boolean isFormatted;

  /**
   * thrift ddl for the result of show indexes.
   */
  private static final String schema = "idx_name,tab_name,col_names,idx_tab_name,idx_type,comment"
                                        + "#string:string:string:string:string:string";

  public static String getSchema() {
    return schema;
  }

  public String getTableName() {
    return tableName;
  }

  public String getResFile() {
    return resFile;
  }

  public boolean isFormatted() {
    return isFormatted;
  }

  public void setFormatted(boolean isFormatted) {
    this.isFormatted = isFormatted;
  }

  /**
   *
   * @param tableName
   *          Name of the table whose indexes need to be listed.
   * @param resFile
   *          File to store the results in.
   */
  public ShowIndexesDesc(String tableName, Path resFile) {
    this.tableName = tableName;
    this.resFile = resFile.toString();
  }
}
