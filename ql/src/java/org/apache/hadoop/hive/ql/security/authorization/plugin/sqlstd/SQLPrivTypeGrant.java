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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;


public enum SQLPrivTypeGrant {
  SELECT_NOGRANT(SQLPrivilegeType.SELECT, false),
  SELECT_WGRANT(SQLPrivilegeType.SELECT, true),
  INSERT_NOGRANT(SQLPrivilegeType.INSERT, false),
  INSERT_WGRANT(SQLPrivilegeType.INSERT, true),
  UPDATE_NOGRANT(SQLPrivilegeType.UPDATE, false),
  UPDATE_WGRANT(SQLPrivilegeType.UPDATE, true),
  DELETE_NOGRANT(SQLPrivilegeType.DELETE, false),
  DELETE_WGRANT(SQLPrivilegeType.DELETE, true),
  OWNER_PRIV("OBJECT OWNERSHIP"),
  ADMIN_PRIV("ADMIN PRIVILEGE"); // This one can be used to deny permission for performing the operation

  private final SQLPrivilegeType privType;
  private final boolean withGrant;

  private final String privDesc;
  SQLPrivTypeGrant(SQLPrivilegeType privType, boolean isGrant){
    this.privType = privType;
    this.withGrant = isGrant;
    this.privDesc = privType.toString() + (withGrant ? " with grant" : "");
  }

  /**
   * Constructor for privileges that are not the standard sql types, but are used by
   * authorization rules
   * @param privDesc
   */
  SQLPrivTypeGrant(String privDesc){
    this.privDesc = privDesc;
    this.privType = null;
    this.withGrant = false;
  }

  /**
   * Find matching enum
   * @param privType
   * @param isGrant
   * @return
   */
  public static SQLPrivTypeGrant getSQLPrivTypeGrant(
      SQLPrivilegeType privType, boolean isGrant) {
    String typeName = privType.name() + (isGrant ? "_WGRANT" : "_NOGRANT");
    return SQLPrivTypeGrant.valueOf(typeName);
  }

  /**
   * Find matching enum
   *
   * @param privTypeStr
   *          privilege type string
   * @param isGrant
   * @return
   * @throws HiveAuthzPluginException
   */
  public static SQLPrivTypeGrant getSQLPrivTypeGrant(String privTypeStr, boolean isGrant)
      throws HiveAuthzPluginException {
    SQLPrivilegeType ptype = SQLPrivilegeType.getRequirePrivilege(privTypeStr);
    return getSQLPrivTypeGrant(ptype, isGrant);
  }

  public SQLPrivilegeType getPrivType() {
    return privType;
  }

  public boolean isWithGrant() {
    return withGrant;
  }

  /**
   * @return String representation for use in error messages
   */
  @Override
  public String toString(){
    return privDesc;
  }

};
