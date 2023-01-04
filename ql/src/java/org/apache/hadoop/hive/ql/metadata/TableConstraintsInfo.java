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

package org.apache.hadoop.hive.ql.metadata;

public class TableConstraintsInfo {
  private PrimaryKeyInfo primaryKeyInfo;
  private ForeignKeyInfo foreignKeyInfo;
  private UniqueConstraint uniqueConstraint;
  private DefaultConstraint defaultConstraint;
  private CheckConstraint checkConstraint;
  private NotNullConstraint notNullConstraint;

  public TableConstraintsInfo() {
  }

  public TableConstraintsInfo(PrimaryKeyInfo primaryKeyInfo, ForeignKeyInfo foreignKeyInfo,
      UniqueConstraint uniqueConstraint, DefaultConstraint defaultConstraint, CheckConstraint checkConstraint,
      NotNullConstraint notNullConstraint) {
    this.primaryKeyInfo = primaryKeyInfo;
    this.foreignKeyInfo = foreignKeyInfo;
    this.uniqueConstraint = uniqueConstraint;
    this.defaultConstraint = defaultConstraint;
    this.checkConstraint = checkConstraint;
    this.notNullConstraint = notNullConstraint;
  }

  public PrimaryKeyInfo getPrimaryKeyInfo() {
    return primaryKeyInfo;
  }

  public void setPrimaryKeyInfo(PrimaryKeyInfo primaryKeyInfo) {
    this.primaryKeyInfo = primaryKeyInfo;
  }

  public ForeignKeyInfo getForeignKeyInfo() {
    return foreignKeyInfo;
  }

  public void setForeignKeyInfo(ForeignKeyInfo foreignKeyInfo) {
    this.foreignKeyInfo = foreignKeyInfo;
  }

  public UniqueConstraint getUniqueConstraint() {
    return uniqueConstraint;
  }

  public void setUniqueConstraint(UniqueConstraint uniqueConstraint) {
    this.uniqueConstraint = uniqueConstraint;
  }

  public DefaultConstraint getDefaultConstraint() {
    return defaultConstraint;
  }

  public void setDefaultConstraint(DefaultConstraint defaultConstraint) {
    this.defaultConstraint = defaultConstraint;
  }

  public CheckConstraint getCheckConstraint() {
    return checkConstraint;
  }

  public void setCheckConstraint(CheckConstraint checkConstraint) {
    this.checkConstraint = checkConstraint;
  }

  public NotNullConstraint getNotNullConstraint() {
    return notNullConstraint;
  }

  public void setNotNullConstraint(NotNullConstraint notNullConstraint) {
    this.notNullConstraint = notNullConstraint;
  }

  public boolean isTableConstraintsInfoNotEmpty() {
    return PrimaryKeyInfo.isNotEmpty(this.getPrimaryKeyInfo()) ||
      ForeignKeyInfo.isNotEmpty(this.getForeignKeyInfo()) ||
      UniqueConstraint.isNotEmpty(this.getUniqueConstraint()) ||
      NotNullConstraint.isNotEmpty(this.getNotNullConstraint()) ||
      CheckConstraint.isNotEmpty(this.getCheckConstraint()) ||
      DefaultConstraint.isNotEmpty(this.getDefaultConstraint());
  }
}
