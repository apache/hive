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
import org.apache.hadoop.hive.ql.plan.Explain.Level;


@Explain(displayName = "Create Role", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class RoleDDLDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;

  private PrincipalType principalType;

  private boolean group;

  private RoleOperation operation;

  private String resFile;

  private String roleOwnerName;

  /**
   * thrift ddl for the result of show roles.
   */
  private static final String roleNameSchema = "role#string";

  /**
   * thrift ddl for the result of show role grant principalName
   */
  private static final String roleShowGrantSchema =
      "role,grant_option,grant_time,grantor#" +
      "string:boolean:bigint:string";

  /**
   * thrift ddl for the result of describe role roleName
   */
  private static final String roleShowRolePrincipals =
      "principal_name,principal_type,grant_option,grantor,grantor_type,grant_time#" +
      "string:string:boolean:string:string:bigint";

  public static String getRoleNameSchema() {
    return roleNameSchema;
  }

  public static String getRoleShowGrantSchema() {
    return roleShowGrantSchema;
  }

  public static String getShowRolePrincipalsSchema() {
    return roleShowRolePrincipals;
  }

  public static enum RoleOperation {
    DROP_ROLE("drop_role"), CREATE_ROLE("create_role"), SHOW_ROLE_GRANT("show_role_grant"),
    SHOW_ROLES("show_roles"), SET_ROLE("set_role"), SHOW_CURRENT_ROLE("show_current_role"),
    SHOW_ROLE_PRINCIPALS("show_role_principals");
    private String operationName;

    private RoleOperation() {
    }

    private RoleOperation(String operationName) {
      this.operationName = operationName;
    }

    public String getOperationName() {
      return operationName;
    }

    @Override
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

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  @Explain(displayName = "role operation", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
