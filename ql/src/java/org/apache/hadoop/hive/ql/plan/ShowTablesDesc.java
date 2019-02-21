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

  /**
   * table name for the result of show tables.
   */
  private static final String table = "show";

  /**
   * thrift ddl for the result of show tables and show views.
   */
  private static final String TABLES_VIEWS_SCHEMA = "tab_name#string";

  /**
   * thrift ddl for the result of show extended tables.
   */
  private static final String EXTENDED_TABLES_SCHEMA = "tab_name,table_type#string,string";

  /**
   * thrift ddl for the result of show tables.
   */
  private static final String MATERIALIZED_VIEWS_SCHEMA =
      "mv_name,rewrite_enabled,mode#string:string:string";


  TableType type;
  String pattern;
  TableType typeFilter;
  String dbName;
  String resFile;
  boolean isExtended;

  public String getTable() {
    return table;
  }

  public String getSchema() {
    if (type != null && type == TableType.MATERIALIZED_VIEW) {
      return MATERIALIZED_VIEWS_SCHEMA;
    }
    return isExtended ? EXTENDED_TABLES_SCHEMA : TABLES_VIEWS_SCHEMA;
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
  public ShowTablesDesc(Path resFile, String dbName, String pattern, TableType typeFilter, boolean isExtended) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.pattern = pattern;
    this.typeFilter = typeFilter;
    this.isExtended = isExtended;
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

  /**
   * @return is extended
   */
  @Explain(displayName = "extended", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED }, displayOnlyOnTrue = true)
  public boolean isExtended() {
    return isExtended;
  }

  /**
   * @param isExtended
   *          whether extended modifier is enabled
   */
  public void setIsExtended(boolean isExtended) {
    this.isExtended = isExtended;
  }

  /**
   * @return table type filter, null if it is not filtered
   */
  @Explain(displayName = "table type filter", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableType getTypeFilter() {
    return typeFilter;
  }

  /**
   * @param typeFilter
   *          table type filter for show statement
   */
  public void setTypeFilter(TableType typeFilter) {
    this.typeFilter = typeFilter;
  }
}
