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

package org.apache.hadoop.hive.ql.ddl;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class DDLDescWithTableProperties implements DDLDesc {
  protected List<String> partColNames;
  protected Map<String, String> tblProps;
  private List<FieldSchema> cols;
  private List<FieldSchema> partCols;
  private String inputFormat;
  private String outputFormat;
  private String location;
  private String serde;
  private String storageHandler;
  private Map<String, String> serdeProps;
  private String comment;
  private boolean ifNotExists;
  private Long initialWriteId; // Initial write ID for CTAS/CMV and import.
  // The FSOP configuration for the FSOP that is going to write initial data during CTAS/CMV.
  // This is not needed beyond compilation, so it is transient.
  private transient FileSinkDesc writer;
  protected boolean isTemporary = false;
  protected boolean isMaterialization = false;
  protected String ownerName = null;

  protected DDLDescWithTableProperties() {
  }

  protected DDLDescWithTableProperties(List<FieldSchema> cols, List<FieldSchema> partCols, String comment, 
      String inputFormat, String outputFormat, String location, String serde, String storageHandler, 
      Map<String, String> serdeProps, Map<String, String> tblProps, boolean ifNotExists) {
    if (cols != null) {
      this.cols = new ArrayList<>(cols);
    }
    this.partCols = partCols;
    this.comment = comment;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.location = location;
    this.serde = serde;
    this.storageHandler = storageHandler;
    this.serdeProps = serdeProps;
    this.tblProps = tblProps;
    this.ifNotExists = ifNotExists;
  }

  public abstract TableName getFullTableName();

  @Explain(displayName = "columns")
  public List<String> getColsString() {
    return Utilities.getFieldSchemaString(getCols());
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
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

  @Explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }
  
  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @Explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  @Explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  @Explain(displayName = "storage handler")
  public String getStorageHandler() {
    return storageHandler;
  }

  public void setStorageHandler(String storageHandler) {
    this.storageHandler = storageHandler;
  }

  @Explain(displayName = "serde name")
  public String getSerde() {
    return serde;
  }

  public void setSerde(String serde) {
    this.serde = serde;
  }

  @Explain(displayName = "serde properties")
  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  public void setSerdeProps(Map<String, String> serdeProps) {
    this.serdeProps = serdeProps;
  }

  public void setInitialWriteId(Long writeId) {
    this.initialWriteId = writeId;
  }

  public Long getInitialWriteId() {
    return initialWriteId;
  }

  @Explain(displayName = "table properties")
  public Map<String, String> getTblPropsExplain() { // only for displaying plan
    return PlanUtils.getPropertiesForExplain(tblProps,
      hive_metastoreConstants.TABLE_IS_CTAS,
      hive_metastoreConstants.TABLE_BUCKETING_VERSION);
  }

  /**
   * @return the table properties
   */
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

  @Explain(displayName = "if not exists", displayOnlyOnTrue = true)
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public FileSinkDesc getAndUnsetWriter() {
    FileSinkDesc fsd = writer;
    writer = null;
    return fsd;
  }

  public List<String> getPartColNames() {
    return partColNames;
  }

  public void setPartColNames(List<String> partColNames) {
    this.partColNames = partColNames;
  }

  public void setWriter(FileSinkDesc writer) {
    this.writer = writer;
  }

  /**
   * @return the isTemporary
   */
  @Explain(displayName = "isTemporary", displayOnlyOnTrue = true)
  public boolean isTemporary() {
    return isTemporary;
  }

  /**
   * @return the isMaterialization
   */
  @Explain(displayName = "isMaterialization", displayOnlyOnTrue = true)
  public boolean isMaterialization() {
    return isMaterialization;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getOwnerName() {
    return this.ownerName;
  }
}
