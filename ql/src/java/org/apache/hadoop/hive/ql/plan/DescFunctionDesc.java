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
 * DescFunctionDesc.
 *
 */
@Explain(displayName = "Describe Function", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescFunctionDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String name;
  String resFile;
  boolean isExtended;

  public boolean isExtended() {
    return isExtended;
  }

  public void setExtended(boolean isExtended) {
    this.isExtended = isExtended;
  }

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

  public DescFunctionDesc() {
  }

  /**
   * @param resFile
   */
  public DescFunctionDesc(Path resFile) {
    this.resFile = resFile.toString();
    name = null;
  }

  /**
   * @param name
   *          of the function to describe
   */
  public DescFunctionDesc(Path resFile, String name, boolean isExtended) {
    this.isExtended = isExtended;
    this.resFile = resFile.toString();
    this.name = name;
  }

  /**
   * @return the name
   */
  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  /**
   * @param name
   *          is the function name
   */
  public void setName(String name) {
    this.name = name;
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
