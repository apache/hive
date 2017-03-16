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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * CreateViewDesc.
 *
 */
@Explain(displayName = "Create View", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateViewDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String viewName;
  private String originalText;
  private String expandedText;
  private boolean rewriteEnabled;
  private List<FieldSchema> schema;
  private Map<String, String> tblProps;
  private List<String> partColNames;
  private List<FieldSchema> partCols;
  private String comment;
  private boolean ifNotExists;
  private boolean orReplace;
  private boolean isAlterViewAs;
  private boolean isMaterialized;
  private String inputFormat;
  private String outputFormat;
  private String location; // only used for materialized views
  private String serde; // only used for materialized views
  private String storageHandler; // only used for materialized views
  private Map<String, String> serdeProps; // only used for materialized views
  private ReplicationSpec replicationSpec = null;

  /**
   * For serialization only.
   */
  public CreateViewDesc() {
  }

  /**
   * Used to create a materialized view descriptor
   * @param viewName
   * @param schema
   * @param comment
   * @param tblProps
   * @param partColNames
   * @param ifNotExists
   * @param orReplace
   * @param isAlterViewAs
   * @param inputFormat
   * @param outputFormat
   * @param location
   * @param serde
   * @param storageHandler
   * @param serdeProps
   */
  public CreateViewDesc(String viewName, List<FieldSchema> schema, String comment,
          Map<String, String> tblProps, List<String> partColNames,
          boolean ifNotExists, boolean orReplace, boolean rewriteEnabled, boolean isAlterViewAs,
          String inputFormat, String outputFormat, String location,
          String serde, String storageHandler, Map<String, String> serdeProps) {
    this.viewName = viewName;
    this.schema = schema;
    this.tblProps = tblProps;
    this.partColNames = partColNames;
    this.comment = comment;
    this.ifNotExists = ifNotExists;
    this.orReplace = orReplace;
    this.isMaterialized = true;
    this.rewriteEnabled = rewriteEnabled;
    this.isAlterViewAs = isAlterViewAs;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.location = location;
    this.serde = serde;
    this.storageHandler = storageHandler;
    this.serdeProps = serdeProps;
  }

  /**
   * Used to create a view descriptor
   * @param viewName
   * @param schema
   * @param comment
   * @param tblProps
   * @param partColNames
   * @param ifNotExists
   * @param orReplace
   * @param isAlterViewAs
   * @param inputFormat
   * @param outputFormat
   * @param serde
   */
  public CreateViewDesc(String viewName, List<FieldSchema> schema, String comment,
                        Map<String, String> tblProps, List<String> partColNames,
                        boolean ifNotExists, boolean orReplace, boolean isAlterViewAs,
                        String inputFormat, String outputFormat, String serde) {
    this.viewName = viewName;
    this.schema = schema;
    this.tblProps = tblProps;
    this.partColNames = partColNames;
    this.comment = comment;
    this.ifNotExists = ifNotExists;
    this.orReplace = orReplace;
    this.isAlterViewAs = isAlterViewAs;
    this.isMaterialized = false;
    this.rewriteEnabled = false;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.serde = serde;
  }

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewName() {
    return viewName;
  }

  public void setViewName(String viewName) {
    this.viewName = viewName;
  }

  @Explain(displayName = "original text", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getViewOriginalText() {
    return originalText;
  }

  public void setViewOriginalText(String originalText) {
    this.originalText = originalText;
  }

  @Explain(displayName = "expanded text")
  public String getViewExpandedText() {
    return expandedText;
  }

  public void setViewExpandedText(String expandedText) {
    this.expandedText = expandedText;
  }

  @Explain(displayName = "rewrite enabled")
  public boolean isRewriteEnabled() {
    return rewriteEnabled;
  }

  public void setRewriteEnabled(boolean rewriteEnabled) {
    this.rewriteEnabled = rewriteEnabled;
  }

  @Explain(displayName = "columns")
  public List<String> getSchemaString() {
    return Utilities.getFieldSchemaString(schema);
  }

  public List<FieldSchema> getSchema() {
    return schema;
  }

  public void setSchema(List<FieldSchema> schema) {
    this.schema = schema;
  }

  @Explain(displayName = "partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(partCols);
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  public List<String> getPartColNames() {
    return partColNames;
  }

  public void setPartColNames(List<String> partColNames) {
    this.partColNames = partColNames;
  }

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void setTblProps(Map<String, String> tblProps) {
    this.tblProps = tblProps;
  }

  @Explain(displayName = "table properties")
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @Explain(displayName = "or replace")
  public boolean getOrReplace() {
    return orReplace;
  }

  public void setOrReplace(boolean orReplace) {
    this.orReplace = orReplace;
  }

  @Explain(displayName = "is alter view as select", displayOnlyOnTrue = true)
  public boolean getIsAlterViewAs() {
    return isAlterViewAs;
  }

  public void setIsAlterViewAs(boolean isAlterViewAs) {
    this.isAlterViewAs = isAlterViewAs;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public boolean isMaterialized() {
    return isMaterialized;
  }

  public void setLocation(String location) {
    this.location = location;
  }
  public String getLocation() {
    return location;
  }

  public String getSerde() {
    return serde;
  }

  public String getStorageHandler() {
    return storageHandler;
  }

  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  /**
   * @param replicationSpec Sets the replication spec governing this create.
   * This parameter will have meaningful values only for creates happening as a result of a replication.
   */
  public void setReplicationSpec(ReplicationSpec replicationSpec) {
    this.replicationSpec = replicationSpec;
  }

  /**
   * @return what kind of replication spec this create is running under.
   */
  public ReplicationSpec getReplicationSpec(){
    if (replicationSpec == null){
      this.replicationSpec = new ReplicationSpec();
    }
    return this.replicationSpec;
  }
}
