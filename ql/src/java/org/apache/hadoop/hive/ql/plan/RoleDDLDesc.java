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

import org.apache.hadoop.hive.metastore.api.PrincipalType;

@Explain(displayName = "Create Role")
public class RoleDDLDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  
  private PrincipalType principalType;
  
  private boolean group;

  private RoleOperation operation;
  
  private String resFile;
  
  private String roleOwnerName;

  public static enum RoleOperation {
    DROP_ROLE("drop_role"), CREATE_ROLE("create_role"), SHOW_ROLE_GRANT("show_roles");
    private String operationName;

    private RoleOperation() {
    }

    private RoleOperation(String operationName) {
      this.operationName = operationName;
    }

    public String getOperationName() {
      return operationName;
    }

    public String toString () {
      return this.operationName;
    }
  }
  
  public RoleDDLDesc(){
  }

  public RoleDDLDesc(String roleName, RoleOperation operation) {
    this(roleName, PrincipalType.USER, operation, null);
  }

  public RoleDDLDesc(String principalName, PrincipalType principalType,
      RoleOperation operation, String roleOwnerName) {
    this.name = principalName;
    this.principalType = principalType;
    this.operation = operation;
    this.roleOwnerName = roleOwnerName;
  }

  @Explain(displayName = "name")
  public String getName() {
    return name;
  }

  public void setName(String roleName) {
    this.name = roleName;
  }
  
  @Explain(displayName = "role operation")
  public RoleOperation getOperation() {
    return operation;
  }

  public void setOperation(RoleOperation operation) {
    this.operation = operation;
  }
  
  public PrincipalType getPrincipalType() {
    return principalType;
  }

  public void setPrincipalType(PrincipalType principalType) {
    this.principalType = principalType;
  }

  public boolean getGroup() {
    return group;
  }

  public void setGroup(boolean group) {
    this.group = group;
  }
  
  public String getResFile() {
    return resFile;
  }

  public void setResFile(String resFile) {
    this.resFile = resFile;
  }
  
  public String getRoleOwnerName() {
    return roleOwnerName;
  }

  public void setRoleOwnerName(String roleOwnerName) {
    this.roleOwnerName = roleOwnerName;
  }

}
