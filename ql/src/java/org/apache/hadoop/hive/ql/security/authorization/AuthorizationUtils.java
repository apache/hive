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
package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;

/**
 * Utility code shared by hive internal code and sql standard authorization plugin implementation
 */
@LimitedPrivate(value = { "Sql standard authorization plugin" })
public class AuthorizationUtils {

  /**
   * Convert thrift principal type to authorization plugin principal type
   * @param type - thrift principal type
   * @return
   * @throws HiveException
   */
  public static HivePrincipalType getHivePrincipalType(PrincipalType type) throws HiveException {
    switch(type){
    case USER:
      return HivePrincipalType.USER;
    case ROLE:
      return HivePrincipalType.ROLE;
    case GROUP:
      throw new HiveException(ErrorMsg.UNNSUPPORTED_AUTHORIZATION_PRINCIPAL_TYPE_GROUP);
    default:
      //should not happen as we take care of all existing types
      throw new AssertionError("Unsupported authorization type specified");
    }
  }


  /**
   * Convert thrift object type to hive authorization plugin object type
   * @param type - thrift object type
   * @return
   */
  public static HivePrivilegeObjectType getHivePrivilegeObjectType(Type type) {
    switch(type){
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE_OR_VIEW;
    case LOCAL_DIR:
      return HivePrivilegeObjectType.LOCAL_URI;
    case DFS_DIR:
      return HivePrivilegeObjectType.DFS_URI;
    case PARTITION:
    case DUMMYPARTITION: //need to determine if a different type is needed for dummy partitions
      return HivePrivilegeObjectType.PARTITION;
    default:
      return null;
    }
  }


  /**
   * Convert authorization plugin principal type to thrift principal type
   * @param type
   * @return
   * @throws HiveException
   */
  public static PrincipalType getThriftPrincipalType(HivePrincipalType type) {
    if(type == null){
      return null;
    }
    switch(type){
    case USER:
      return PrincipalType.USER;
    case ROLE:
      return PrincipalType.ROLE;
    default:
      throw new AssertionError("Invalid principal type " + type);
    }
  }


  /**
   * Get thrift privilege grant info
   * @param privilege
   * @param grantorPrincipal
   * @param grantOption
   * @param grantTime
   * @return
   * @throws HiveException
   */
  public static PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption, int grantTime) throws HiveException {
    return new PrivilegeGrantInfo(privilege.getName(), grantTime,
        grantorPrincipal.getName(), getThriftPrincipalType(grantorPrincipal.getType()), grantOption);
  }


  /**
   * Convert plugin privilege object type to thrift type
   * @param type
   * @return
   * @throws HiveException
   */
  public static HiveObjectType getThriftHiveObjType(HivePrivilegeObjectType type) throws HiveException {
    if (type == null) {
      return null;
    }
    switch(type){
    case DATABASE:
      return HiveObjectType.DATABASE;
    case TABLE_OR_VIEW:
      return HiveObjectType.TABLE;
    case PARTITION:
      return HiveObjectType.PARTITION;
    case LOCAL_URI:
    case DFS_URI:
      throw new HiveException("Unsupported type " + type);
    default:
      //should not happen as we have accounted for all types
      throw new AssertionError("Unsupported type " + type);
    }
  }


  /**
   * Convert thrift HiveObjectRef to plugin HivePrivilegeObject
   * @param privObj
   * @return
   * @throws HiveException
   */
  public static HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj) throws HiveException {
    if (privObj == null) {
      return null;
    }
    HiveObjectType objType = getThriftHiveObjType(privObj.getType());
    return new HiveObjectRef(objType, privObj.getDbname(), privObj.getTableViewURI(), null, null);
  }


}
