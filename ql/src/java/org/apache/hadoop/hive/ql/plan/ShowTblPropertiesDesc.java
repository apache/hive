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
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowTblPropertiesDesc.
 *
 */
@Explain(displayName = "Show Table Properties", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowTblPropertiesDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String resFile;
  String tableName;
  String propertyName;

  /**
   * table name for the result of showtblproperties.
   */
  private static final String table = "show_tableproperties";
  /**
   * thrift ddl for the result of showtblproperties.
   */
  private static final String schema = "prpt_name,prpt_value#string:string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * For serialization use only.
   */
  public ShowTblPropertiesDesc() {
  }

  /**
   * @param resFile
   * @param tableName
   *          name of table to show
   * @param propertyName
   *          name of property to show
   */
  public ShowTblPropertiesDesc(String resFile, String tableName, String propertyName) {
    this.resFile = resFile;
    this.tableName = tableName;
    this.propertyName = propertyName;
  }

  /**
   * @return the resFile
   */
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFileString() {
    return getResFile();
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

  /**
   * @return the propertyName
   */
  @Explain(displayName = "property name")
  public String getPropertyName() {
    return propertyName;
  }

  /**
   * @param propertyName
   *          the propertyName to set
   */
  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }
}
