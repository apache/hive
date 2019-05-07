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

package org.apache.hadoop.hive.ql.ddl.table.info;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLTask2;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW TABLES commands.
 */
@Explain(displayName = "Show Tables", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTablesDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private static final String TABLES_VIEWS_SCHEMA = "tab_name#string";
  private static final String MATERIALIZED_VIEWS_SCHEMA = "mv_name,rewrite_enabled,mode#string:string:string";

  static {
    DDLTask2.registerOperation(ShowTablesDesc.class, ShowTablesOperation.class);
  }

  private final String resFile;
  private final String dbName;
  private final String pattern;
  private final TableType type;

  public ShowTablesDesc(Path resFile) {
    this(resFile, null, null, null);
  }

  public ShowTablesDesc(Path resFile, String dbName) {
    this(resFile, dbName, null, null);
  }

  public ShowTablesDesc(Path resFile, String dbName, TableType type) {
    this(resFile, dbName, null, type);
  }

  public ShowTablesDesc(Path resFile, String dbName, String pattern) {
    this(resFile, dbName, pattern, null);
  }

  public ShowTablesDesc(Path resFile, String dbName, String pattern, TableType type) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.pattern = pattern;
    this.type = type;
  }

  @Explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  @Explain(displayName = "type")
  public TableType getType() {
    return type;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "database name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  public String getSchema() {
    if (type != null && type == TableType.MATERIALIZED_VIEW) {
      return MATERIALIZED_VIEWS_SCHEMA;
    }
    return TABLES_VIEWS_SCHEMA;
  }
}
