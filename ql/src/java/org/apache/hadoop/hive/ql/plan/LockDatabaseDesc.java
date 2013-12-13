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

/**
 * LockDatabaseDesc.
 *
 */
@Explain(displayName = "Lock Database")
public class LockDatabaseDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String databaseName;
  private String mode;
  private String queryId;
  private String queryStr;

  public LockDatabaseDesc() {
  }

  public LockDatabaseDesc(String databaseName, String mode, String queryId) {
    this.databaseName = databaseName;
    this.mode = mode;
    this.queryId = queryId;
  }

  @Explain(displayName = "database")
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getMode() {
    return mode;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getQueryStr() {
    return queryStr;
  }

  public void setQueryStr(String queryStr) {
    this.queryStr = queryStr;
  }
}
