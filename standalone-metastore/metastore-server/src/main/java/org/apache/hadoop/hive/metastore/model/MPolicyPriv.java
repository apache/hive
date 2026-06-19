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
 * Privilege grant on a policy object. {@code policyId == 0} denotes the
 * wildcard {@code *} grant (applies to every policy); the sentinel value is
 * not a valid {@link MErasurePolicy} datastore identity, so no FK is declared.
 */
public class MPolicyPriv {

  private long   policyId;
  private String principalName;
  private String principalType;          // USER | ROLE
  private String privilege;              // POLICY_VALIDATE | POLICY_ACTIVATE | POLICY_MANAGE
  private long   createTime;
  private String grantor;
  private String grantorType;
  private boolean grantOption;

  public MPolicyPriv() {
  }

  public MPolicyPriv(long policyId, String principalName, String principalType,
                     String privilege, long createTime, String grantor,
                     String grantorType, boolean grantOption) {
    this.policyId = policyId;
    this.principalName = principalName;
    this.principalType = principalType;
    this.privilege = privilege;
    this.createTime = createTime;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  public long getPolicyId() { return policyId; }
  public void setPolicyId(long policyId) { this.policyId = policyId; }

  public String getPrincipalName() { return principalName; }
  public void setPrincipalName(String principalName) { this.principalName = principalName; }

  public String getPrincipalType() { return principalType; }
  public void setPrincipalType(String principalType) { this.principalType = principalType; }

  public String getPrivilege() { return privilege; }
  public void setPrivilege(String privilege) { this.privilege = privilege; }

  public long getCreateTime() { return createTime; }
  public void setCreateTime(long createTime) { this.createTime = createTime; }

  public String getGrantor() { return grantor; }
  public void setGrantor(String grantor) { this.grantor = grantor; }

  public String getGrantorType() { return grantorType; }
  public void setGrantorType(String grantorType) { this.grantorType = grantorType; }

  public boolean isGrantOption() { return grantOption; }
  public void setGrantOption(boolean grantOption) { this.grantOption = grantOption; }
}
