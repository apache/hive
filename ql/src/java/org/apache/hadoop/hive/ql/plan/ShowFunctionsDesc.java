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
 * ShowFunctionsDesc.
 *
 */
@Explain(displayName = "Show Functions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowFunctionsDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String pattern;
  String resFile;
  /**
   * whether like keyword is specified
   */
  private boolean isLikePattern = false;
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

  public ShowFunctionsDesc() {
  }

  /**
   * @param resFile
   */
  public ShowFunctionsDesc(Path resFile) {
    this.resFile = resFile.toString();
    pattern = null;
  }

  /**
   * @param pattern
   *          names of tables to show
   */
  public ShowFunctionsDesc(Path resFile, String pattern) {
    this.resFile = resFile.toString();
    this.pattern = pattern;
  }

  /**
   * @param pattern
   *          names of tables to show
   * @param like
   *          is like keyword used
   */
  public ShowFunctionsDesc(Path resFile, String pattern, boolean isLikePattern) {
    this(resFile, pattern);
    this.isLikePattern = isLikePattern;
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

  /**
   * @return isLikePattern
   */
  public boolean getIsLikePattern() {
    return isLikePattern;
  }
}
