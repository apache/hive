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

public class MRoleMap {
  
  private String principalName;
  
  private String principalType;
  
  private MRole role;
  
  private int addTime;
  
  private String grantor;
  
  private String grantorType;
  
  private boolean grantOption;

  public MRoleMap() {
  }
  
  public MRoleMap(String principalName, String principalType, MRole role,
      int addTime, String grantor, String grantorType, boolean grantOption) {
    super();
    this.principalName = principalName;
    this.principalType = principalType;
    this.role = role;
    this.addTime = addTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  /**
   * @return principal name
   */
  public String getPrincipalName() {
    return principalName;
  }

  /**
   * @param userName principal name
   */
  public void setPrincipalName(String userName) {
    this.principalName = userName;
  }

  public String getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(String principalType) {
    this.principalType = principalType;
  }

  /**
   * @return add time
   */
  public int getAddTime() {
    return addTime;
  }

  /**
   * @param addTime
   */
  public void setAddTime(int addTime) {
    this.addTime = addTime;
  }

  public MRole getRole() {
    return role;
  }

  public void setRole(MRole role) {
    this.role = role;
  }
  
  public boolean getGrantOption() {
    return grantOption;
  }

  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
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

}
