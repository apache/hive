/**
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
 * DescDatabaseDesc.
 *
 */
@Explain(displayName = "Describe Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescDatabaseDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String dbName;
  String resFile;
  boolean isExt;

  /**
   * thrift ddl for the result of describe database.
   */
  private static final String schema = "db_name,comment,location,owner_name,owner_type,parameters#string:string:string:string:string:string";

  public DescDatabaseDesc() {
  }

  /**
   * @param resFile
   * @param dbName
   * @param isExt
   */
  public DescDatabaseDesc(Path resFile, String dbName, boolean isExt) {
    this.isExt = isExt;
    this.resFile = resFile.toString();
    this.dbName = dbName;
  }

  public static String getSchema() {
    return schema;
  }

  /**
   * @return the isExt
   */
  public boolean isExt() {
    return isExt;
  }

  /**
   * @param isExt
   *          the isExt to set
   */
  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

  /**
   * @return the tableName
   */
  @Explain(displayName = "database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return dbName;
  }

  /**
   * @param db
   *          the database name to set
   */
  public void setDatabaseName(String db) {
    this.dbName = db;
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
