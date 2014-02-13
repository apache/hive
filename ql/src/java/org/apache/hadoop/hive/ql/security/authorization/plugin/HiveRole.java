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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.metastore.api.Role;

import com.google.common.collect.ComparisonChain;

// same with thrift.Role
@LimitedPrivate(value = { "" })
@Evolving
public class HiveRole implements Comparable<HiveRole> {

  private String roleName;
  private int createTime;
  private String ownerName;
  private String principalName;
  private String principalType;
  private boolean grantOption;
  private int grantTime;
  private String grantor;

  public HiveRole() {}

  public HiveRole(Role role) {
    roleName = role.getRoleName();
    createTime = role.getCreateTime();
    ownerName = role.getOwnerName();
    principalName = role.getPrincipalName();
    principalType = role.getPrincipalType();
    grantOption = role.isGrantOption();
    grantTime = role.getGrantTime();
    grantor = role.getGrantor();
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public void setOwnerName(String ownerName) {
    this.ownerName = ownerName;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public void setPrincipalName(String principalName) {
    this.principalName = principalName;
  }

  public String getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(String principalType) {
    this.principalType = principalType;
  }

  public boolean isGrantOption() {
    return grantOption;
  }

  public void setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
  }

  public int getGrantTime() {
    return grantTime;
  }

  public void setGrantTime(int grantTime) {
    this.grantTime = grantTime;
  }

  public String getGrantor() {
    return grantor;
  }

  public void setGrantor(String grantor) {
    this.grantor = grantor;
  }

  @Override
  public int compareTo(HiveRole other) {
    if(other == null){
      return 1;
    }
    return ComparisonChain.start().compare(roleName, other.roleName)
        .compare(createTime, other.createTime)
        .compare(principalName, other.principalName)
        .compare(principalType, other.principalType)
        .compare(grantOption, other.grantOption)
        .compare(grantTime, other.grantTime)
        .compare(grantor, other.grantor)
        .result();

  }


}
