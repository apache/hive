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
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowDatabasesDesc.
 *
 */
@Explain(displayName = "Show Databases", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowDatabasesDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String pattern;
  String resFile;

  /**
   * table name for the result of show databases.
   */
  private static final String table = "show_databases";

  /**
   * thrift ddl for the result of show databases.
   */
  private static final String schema = "database_name#string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  public ShowDatabasesDesc() {
  }

  /**
   * @param resFile
   */
  public ShowDatabasesDesc(Path resFile) {
    this.resFile = resFile.toString();
    pattern = null;
  }

  /**
   * @param pattern
   *          names of databases to show
   */
  public ShowDatabasesDesc(Path resFile, String pattern) {
    this.resFile = resFile.toString();
    this.pattern = pattern;
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
}
