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

import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ShowCreateDatabaseDesc.
 *
 */
@Explain(displayName = "Show Create Database",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ShowCreateDatabaseDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String resFile;
  String dbName;

  /**
   * thrift ddl for the result of showcreatedatabase.
   */
  private static final String schema = "createdb_stmt#string";

  public String getSchema() {
    return schema;
  }

  /**
   * For serialization use only.
   */
  public ShowCreateDatabaseDesc() {
  }

  /**
   * @param resFile
   * @param dbName
   *          name of database to show
   */
  public ShowCreateDatabaseDesc(String dbName, String resFile) {
    this.dbName = dbName;
    this.resFile = resFile;
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
   * @return the databaseName
   */
  @Explain(displayName = "database name",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDatabaseName() {
    return dbName;
  }

  /**
   * @param databaseName
   *          the dbName to set
   */
  public void setDatabaseName(String dbName) {
    this.dbName = dbName;
  }
}
