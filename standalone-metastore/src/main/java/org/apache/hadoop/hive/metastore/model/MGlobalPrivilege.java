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

/**
 * User global level privileges
 */
public class MGlobalPrivilege {

  //principal name, can be a user, group, or role
  private String principalName;

  private String principalType;

  private String privilege;

  private int createTime;

  private String grantor;

  private String grantorType;

  private boolean grantOption;

  public MGlobalPrivilege() {
    super();
  }

  public MGlobalPrivilege(String userName, String principalType,
      String dbPrivilege, int createTime, String grantor, String grantorType,
      boolean grantOption) {
    super();
    this.principalName = userName;
    this.principalType = principalType;
    this.privilege = dbPrivilege;
    this.createTime = createTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  /**
   * @return a set of global privileges granted to this user
   */
  public String getPrivilege() {
    return privilege;
  }

  /**
   * @param dbPrivilege set of global privileges to user
   */
  public void setPrivilege(String dbPrivilege) {
    this.privilege = dbPrivilege;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  public String getGrantor() {
    return grantor;
  }

  public void setGrantor(String grantor) {
    this.grantor = grantor;
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

  public String getGrantorType() {
    return grantorType;
  }

  public void setGrantorType(String grantorType) {
    this.grantorType = grantorType;
  }

}
