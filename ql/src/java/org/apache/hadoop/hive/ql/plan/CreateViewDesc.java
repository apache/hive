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

/**
 * CreateViewDesc.
 *
 */
@Explain(displayName = "Create View")
public class CreateViewDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private String viewName;
  private String originalText;
  private String expandedText;
  private List<FieldSchema> schema;
  private Map<String, String> tblProps;
  private List<String> partColNames;
  private List<FieldSchema> partCols;
  private String comment;
  private boolean ifNotExists;
  private boolean orReplace;
  private boolean isAlterViewAs;

  /**
   * For serialization only.
   */
  public CreateViewDesc() {
  }

  public CreateViewDesc(String viewName, List<FieldSchema> schema,
      String comment, Map<String, String> tblProps,
      List<String> partColNames, boolean ifNotExists,
      boolean orReplace, boolean isAlterViewAs) {
    this.viewName = viewName;
    this.schema = schema;
    this.comment = comment;
    this.tblProps = tblProps;
    this.partColNames = partColNames;
    this.ifNotExists = ifNotExists;
    this.orReplace = orReplace;
    this.isAlterViewAs = isAlterViewAs;
  }

  @Explain(displayName = "name")
  public String getViewName() {
    return viewName;
  }

  public void setViewName(String viewName) {
    this.viewName = viewName;
  }

  @Explain(displayName = "original text")
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
}
