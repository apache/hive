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
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * CreateDatabaseDesc.
 *
 */
@Explain(displayName = "Create Database", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateDatabaseDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String databaseName;
  String locationUri;
  String comment;
  boolean ifNotExists;
  Map<String, String> dbProperties;

  /**
   * For serialization only.
   */
  public CreateDatabaseDesc() {
  }

  public CreateDatabaseDesc(String databaseName, String comment,
      String locationUri, boolean ifNotExists) {
    super();
    this.databaseName = databaseName;
    this.comment = comment;
    this.locationUri = locationUri;
    this.ifNotExists = ifNotExists;
    this.dbProperties = null;
  }

  @Explain(displayName="if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public Map<String, String> getDatabaseProperties() {
    return dbProperties;
  }

  public void setDatabaseProperties(Map<String, String> dbProps) {
    this.dbProperties = dbProps;
  }

  @Explain(displayName="name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return databaseName;
  }

  public void setName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Explain(displayName="comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @Explain(displayName="locationUri")
  public String getLocationUri() {
    return locationUri;
  }

  public void setLocationUri(String locationUri) {
    this.locationUri = locationUri;
  }
}
