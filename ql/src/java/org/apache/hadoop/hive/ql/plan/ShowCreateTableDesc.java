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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowCreateTableDesc.
 *
 */
@Explain(displayName = "Show Create Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowCreateTableDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String resFile;
  String tableName;

  /**
   * table name for the result of showcreatetable.
   */
  private static final String table = "show_create_table";
  /**
   * thrift ddl for the result of showcreatetable.
   */
  private static final String schema = "createtab_stmt#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * For serialization use only.
   */
  public ShowCreateTableDesc() {
  }

  /**
   * @param resFile
   * @param tableName
   *          name of table to show
   */
  public ShowCreateTableDesc(String tableName, String resFile) {
    this.tableName = tableName;
    this.resFile = resFile;
  }

  /**
   * @return the resFile
   */
  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          the resFile to set
   */
  public void setResFile(String resFile) {
    this.resFile = resFile;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
