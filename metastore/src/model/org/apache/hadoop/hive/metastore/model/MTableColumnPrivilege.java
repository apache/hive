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

package org.apache.hadoop.hive.metastore.model;

public class MTableColumnPrivilege {

  private String principalName; 

  private String principalType;

  private MTable table;
  
  private String columnName;

  private String privilege;

  private int createTime;
  
  private String grantor;
  
  private String grantorType;
  
  private boolean grantOption;

  public MTableColumnPrivilege() {
  }
  
  /**
   * @param principalName
   * @param isRole
   * @param isGroup
   * @param table
   * @param columnName
   * @param privileges
   * @param createTime
   * @param grantor
   */
  public MTableColumnPrivilege(String principalName, String principalType,
      MTable table, String columnName, String privileges, int createTime,
      String grantor, String grantorType, boolean grantOption) {
    super();
    this.principalName = principalName;
    this.principalType = principalType;
    this.table = table;
    this.columnName = columnName;
    this.privilege = privileges;
    this.createTime = createTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }
  
  /**
   * @return column name
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @param columnName column name
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  /**
   * @return a set of privileges this user/role/group has
   */
  public String getPrivilege() {
    return privilege;
  }

  /**
   * @param dbPrivileges a set of privileges this user/role/group has
   */
  public void setPrivilege(String dbPrivileges) {
    this.privilege = dbPrivileges;
  }

  /**
   * @return create time
   */
  public int getCreateTime() {
    return createTime;
  }

  /**
   * @param createTime create time
   */
  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }

  public MTable getTable() {
    return table;
  }

  public void setTable(MTable table) {
    this.table = table;
  }
  
  public String getGrantor() {
    return grantor;
  }

  public void setGrantor(String grantor) {
    this.grantor = grantor;
  }
  
  public String getGrantorType() {
    return grantorType;
  }

  public void setGrantorType(String grantorType) {
    this.grantorType = grantorType;
  }
  
  public boolean getGrantOption() {
    return grantOption;
  }

  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
  }
  
  public String getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(String principalType) {
    this.principalType = principalType;
  }

}
