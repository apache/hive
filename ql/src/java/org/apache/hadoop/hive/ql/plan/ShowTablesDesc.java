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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowTablesDesc.
 *
 */
@Explain(displayName = "Show Tables", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTablesDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String pattern;
  String dbName;
  String resFile;
  TableType type;
  /**
   * table name for the result of show tables.
   */
  private static final String table = "show";
  /**
   * thrift ddl for the result of show tables.
   */
  private static final String schema = "tab_name#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  public ShowTablesDesc() {
  }

  /**
   * @param resFile
   */
  public ShowTablesDesc(Path resFile) {
    this.resFile = resFile.toString();
    pattern = null;
  }

  /**
   * @param dbName
   *          name of database to show tables of
   */
  public ShowTablesDesc(Path resFile, String dbName) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
  }

  /**
   * @param pattern
   *          names of tables to show
   */
  public ShowTablesDesc(Path resFile, String dbName, String pattern) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.pattern = pattern;
  }

  /**
   * @param type
   *          type of the tables to show
   */
  public ShowTablesDesc(Path resFile, String dbName, String pattern, TableType type) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.pattern = pattern;
    this.type    = type;
  }

  /**
   * @return the pattern
   */
  @Explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  /**
   * @param pattern
   *          the pattern to set
   */
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * @return the table type to be fetched
   */
  @Explain(displayName = "type")
  public TableType getType() {
    return type;
  }

  /**
   * @param type
   *          the table type to set
   */
  public void setType(TableType type) {
    this.type = type;
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
   * @return the dbName
   */
  @Explain(displayName = "database name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName
   *          the dbName to set
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }
}
