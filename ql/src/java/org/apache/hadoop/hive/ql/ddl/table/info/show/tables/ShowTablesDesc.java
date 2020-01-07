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

package org.apache.hadoop.hive.ql.ddl.table.info.show.tables;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW TABLES commands.
 */
@Explain(displayName = "Show Tables", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTablesDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private static final String TABLES_VIEWS_SCHEMA = "tab_name#string";
  private static final String EXTENDED_TABLES_SCHEMA = "tab_name,table_type#string,string";

  private final String resFile;
  private final String dbName;
  private final String pattern;
  private final TableType typeFilter;
  private final boolean isExtended;

  public ShowTablesDesc(Path resFile, String dbName, String pattern, TableType typeFilter, boolean isExtended) {
    this.resFile = resFile.toString();
    this.dbName = dbName;
    this.pattern = pattern;
    this.typeFilter = typeFilter;
    this.isExtended = isExtended;
  }

  @Explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "database name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbName() {
    return dbName;
  }

  @Explain(displayName = "extended", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      displayOnlyOnTrue = true)
  public boolean isExtended() {
    return isExtended;
  }

  /** For explain only. */
  @Explain(displayName = "table type filter", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTypeFilterString() {
    return typeFilter.name();
  }

  public TableType getTypeFilter() {
    return typeFilter;
  }

  public String getSchema() {
    return isExtended ? EXTENDED_TABLES_SCHEMA : TABLES_VIEWS_SCHEMA;
  }
}
