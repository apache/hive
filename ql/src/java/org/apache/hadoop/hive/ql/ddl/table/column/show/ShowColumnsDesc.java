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

package org.apache.hadoop.hive.ql.ddl.table.column.show;

import java.io.Serializable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for SHOW COLUMNS commands.
 */
@Explain(displayName = "Show Columns", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowColumnsDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA = "Field#string";

  private final String resFile;
  private final String tableName;
  private final String pattern;
  private final boolean sorted;

  public ShowColumnsDesc(Path resFile, String tableName, String pattern, boolean sorted) {
    this.resFile = resFile.toString();
    this.pattern = pattern;
    this.tableName = tableName;
    this.sorted = sorted;
  }

  @Explain(displayName = "pattern", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPattern() {
    return pattern;
  }

  @Explain(displayName = "table name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "sorted", explainLevels = { Level.EXTENDED })
  public boolean isSorted() {
    return sorted;
  }
}
