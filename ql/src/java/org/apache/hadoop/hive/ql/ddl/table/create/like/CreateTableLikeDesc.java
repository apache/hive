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

package org.apache.hadoop.hive.ql.ddl.table.create.like;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTLT;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for CREATE TABLE LIKE commands.
 */
@Explain(displayName = "Create Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateTableLikeDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  private final String tableName;
  private boolean isExternal;
  private final boolean isTemporary;
  private final String defaultInputFormat;
  private final String defaultOutputFormat;
  private final String location;
  private final String defaultSerName;
  private final Map<String, String> defaultSerdeProps;
  private final Map<String, String> tblProps;
  private final boolean ifNotExists;
  private final String likeTableName;
  private final boolean isUserStorageFormat;

  public CreateTableLikeDesc(String tableName, boolean isExternal, boolean isTemporary, String defaultInputFormat,
      String defaultOutputFormat, String location, String defaultSerName, Map<String, String> defaultSerdeProps,
      Map<String, String> tblProps, boolean ifNotExists, String likeTableName, boolean isUserStorageFormat) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.isTemporary = isTemporary;
    this.defaultInputFormat = defaultInputFormat;
    this.defaultOutputFormat = defaultOutputFormat;
    this.defaultSerName = defaultSerName;
    this.defaultSerdeProps = defaultSerdeProps;
    this.location = location;
    this.tblProps = tblProps;
    this.ifNotExists = ifNotExists;
    this.likeTableName = likeTableName;
    this.isUserStorageFormat = isUserStorageFormat;
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  @Explain(displayName = "default input format")
  public String getDefaultInputFormat() {
    return defaultInputFormat;
  }

  @Explain(displayName = "default output format")
  public String getDefaultOutputFormat() {
    return defaultOutputFormat;
  }

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  @Explain(displayName = "isExternal", displayOnlyOnTrue = true)
  public boolean isExternal() {
    return isExternal;
  }

  public void setIsExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  @Explain(displayName = "default serde name")
  public String getDefaultSerName() {
    return defaultSerName;
  }

  @Explain(displayName = "serde properties")
  public Map<String, String> getDefaultSerdeProps() {
    return defaultSerdeProps;
  }

  @Explain(displayName = "like")
  public String getLikeTableName() {
    return likeTableName;
  }

  public Map<String, String> getTblProps() {
    return tblProps;
  }

  @Explain(displayName = "table properties")
  public Map<String, String> getTblPropsExplain() {
    HashMap<String, String> copy = new HashMap<>(tblProps);
    copy.remove(TABLE_IS_CTAS);
    copy.remove(TABLE_IS_CTLT);
    return copy;
  }

  @Explain(displayName = "isTemporary", displayOnlyOnTrue = true)
  public boolean isTemporary() {
    return isTemporary;
  }

  public boolean isUserStorageFormat() {
    return this.isUserStorageFormat;
  }
}
