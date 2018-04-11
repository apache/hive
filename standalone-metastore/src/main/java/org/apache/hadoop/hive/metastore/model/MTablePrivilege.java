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

package org.apache.hadoop.hive.metastore.model;

public class MTablePrivilege {

  private String principalName;

  private String principalType;

  private MTable table;

  private String privilege;

  private int createTime;

  private String grantor;

  private String grantorType;

  private boolean grantOption;

  public MTablePrivilege() {
  }

  public MTablePrivilege(String principalName, String principalType,
      MTable table, String privilege, int createTime,
      String grantor, String grantorType, boolean grantOption) {
    super();
    this.principalName = principalName;
    this.principalType = principalType;
    this.table = table;
    this.privilege = privilege;
    this.createTime = createTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }


  /**
   * @return a set of privileges this user/role/group has
   */
  public String getPrivilege() {
    return privilege;
  }

  /**
   * @param dbPrivilege a set of privileges this user/role/group has
   */
  public void setPrivilege(String dbPrivilege) {
    this.privilege = dbPrivilege;
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

  /**
   * @return the grantor
   */
  public String getGrantor() {
    return grantor;
  }

  /**
   * @param grantor
   */
  public void setGrantor(String grantor) {
    this.grantor = grantor;
  }

  public String getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(String principalType) {
    this.principalType = principalType;
  }

  public MTable getTable() {
    return table;
  }

  public void setTable(MTable table) {
    this.table = table;
  }

  public boolean getGrantOption() {
    return grantOption;
  }

  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
  }

  public String getGrantorType() {
    return grantorType;
  }

  public void setGrantorType(String grantorType) {
    this.grantorType = grantorType;
  }

}
