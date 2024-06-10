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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

import java.util.List;
import java.util.Map;

public abstract class DDLDescWithTableProperties implements DDLDesc {
  protected List<String> partColNames;
  protected Map<String, String> tblProps;
  private String location;
  private Long initialWriteId; // Initial write ID for CTAS/CMV and import.
  // The FSOP configuration for the FSOP that is going to write initial data during CTAS/CMV.
  // This is not needed beyond compilation, so it is transient.
  private transient FileSinkDesc writer;
  protected boolean isTemporary = false;
  protected boolean isMaterialization = false;
  protected String ownerName = null;

  protected DDLDescWithTableProperties() {
  }

  protected DDLDescWithTableProperties(Map<String, String> tblProps, String location) {
    this.tblProps = tblProps;
    this.location = location;
  }

  protected DDLDescWithTableProperties(List<String> partColNames, Map<String, String> tblProps, String location) {
    this(tblProps, location);
    this.partColNames = partColNames;
  }

  public abstract TableName getFullTableName();

  @Explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
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
