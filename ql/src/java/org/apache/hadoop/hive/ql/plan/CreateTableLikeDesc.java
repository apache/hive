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
 * CreateTableLikeDesc.
 *
 */
@Explain(displayName = "Create Table")
public class CreateTableLikeDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tableName;
  boolean isExternal;
  String location;
  boolean ifNotExists;
  String likeTableName;

  public CreateTableLikeDesc() {
  }

  public CreateTableLikeDesc(String tableName, boolean isExternal,
      String location, boolean ifNotExists, String likeTableName) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.location = location;
    this.ifNotExists = ifNotExists;
    this.likeTableName = likeTableName;
  }

  @Explain(displayName = "if not exists")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName = "name")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Explain(displayName = "isExternal")
  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  @Explain(displayName = "like")
  public String getLikeTableName() {
    return likeTableName;
  }

  public void setLikeTableName(String likeTableName) {
    this.likeTableName = likeTableName;
  }

}
