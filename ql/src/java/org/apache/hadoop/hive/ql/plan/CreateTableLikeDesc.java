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
 * CreateTableLikeDesc.
 *
 */
@Explain(displayName = "Create Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateTableLikeDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tableName;
  boolean isExternal;
  String defaultInputFormat;
  String defaultOutputFormat;
  String defaultSerName;
  Map<String, String> defaultSerdeProps;
  String location;
  Map<String, String> tblProps;
  boolean ifNotExists;
  String likeTableName;
  boolean isTemporary = false;
  boolean isUserStorageFormat = false;

  public CreateTableLikeDesc() {
  }

  public CreateTableLikeDesc(String tableName, boolean isExternal, boolean isTemporary,
      String defaultInputFormat, String defaultOutputFormat, String location,
      String defaultSerName, Map<String, String> defaultSerdeProps, Map<String, String> tblProps,
      boolean ifNotExists, String likeTableName, boolean isUserStorageFormat) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.isTemporary = isTemporary;
    this.defaultInputFormat=defaultInputFormat;
    this.defaultOutputFormat=defaultOutputFormat;
    this.defaultSerName=defaultSerName;
    this.defaultSerdeProps=defaultSerdeProps;
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

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Explain(displayName = "default input format")
  public String getDefaultInputFormat() {
    return defaultInputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.defaultInputFormat = inputFormat;
  }

  @Explain(displayName = "default output format")
  public String getDefaultOutputFormat() {
    return defaultOutputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.defaultOutputFormat = outputFormat;
  }

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Explain(displayName = "isExternal", displayOnlyOnTrue = true)
  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  /**
   * @return the default serDeName
   */
  @Explain(displayName = "default serde name")
  public String getDefaultSerName() {
    return defaultSerName;
  }

  /**
   * @param serName
   *          the serName to set
   */
  public void setDefaultSerName(String serName) {
    this.defaultSerName = serName;
  }

  /**
   * @return the default serDe properties
   */
  @Explain(displayName = "serde properties")
  public Map<String, String> getDefaultSerdeProps() {
    return defaultSerdeProps;
  }

  /**
   * @param serdeProps
   *          the default serde properties to set
   */
  public void setDefaultSerdeProps(Map<String, String> serdeProps) {
    this.defaultSerdeProps = serdeProps;
  }

  @Explain(displayName = "like")
  public String getLikeTableName() {
    return likeTableName;
  }

  public void setLikeTableName(String likeTableName) {
    this.likeTableName = likeTableName;
  }

  /**
   * @return the table properties
   */
  @Explain(displayName = "table properties")
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  /**
   * @param tblProps
   *          the table properties to set
   */
  public void setTblProps(Map<String, String> tblProps) {
    this.tblProps = tblProps;
  }

  /**
   * @return the isTemporary
   */
  @Explain(displayName = "isTemporary", displayOnlyOnTrue = true)
  public boolean isTemporary() {
    return isTemporary;
  }

  /**
   * @param isTemporary table is Temporary or not.
   */
  public void setTemporary(boolean isTemporary) {
    this.isTemporary = isTemporary;
  }

  /**
   * True if user has specified storage format in query
   * @return boolean
   */
  public boolean isUserStorageFormat() {
    return this.isUserStorageFormat;
  }
}
