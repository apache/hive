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
package org.apache.hadoop.hive.ql.security.authorization;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;

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
    if (type == null) {
      return null;
    }
    switch(type){
    case USER:
      return HivePrincipalType.USER;
    case ROLE:
      return HivePrincipalType.ROLE;
    case GROUP:
      return HivePrincipalType.GROUP;
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
    if (type == null){
      return null;
    }
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
    case FUNCTION:
      return HivePrivilegeObjectType.FUNCTION;
    case SERVICE_NAME:
      return HivePrivilegeObjectType.SERVICE_NAME;
    default:
      return null;
    }
  }

  public static HivePrivilegeObjectType getPrivObjectType(PrivilegeObjectDesc privSubjectDesc) {
    if (privSubjectDesc.getObject() == null) {
      return null;
    }
    return privSubjectDesc.getTable() ? HivePrivilegeObjectType.TABLE_OR_VIEW :
        HivePrivilegeObjectType.DATABASE;
  }

  public static List<HivePrivilege> getHivePrivileges(List<PrivilegeDesc> privileges,
      HiveAuthorizationTranslator trans) {
  List<HivePrivilege> hivePrivileges = new ArrayList<HivePrivilege>();
    for(PrivilegeDesc privilege : privileges){
      hivePrivileges.add(trans.getHivePrivilege(privilege));
    }
    return hivePrivileges;
  }

  public static List<HivePrincipal> getHivePrincipals(List<PrincipalDesc> principals,
      HiveAuthorizationTranslator trans)
      throws HiveException {
  ArrayList<HivePrincipal> hivePrincipals = new ArrayList<HivePrincipal>();
    for(PrincipalDesc principal : principals){
      hivePrincipals.add(trans.getHivePrincipal(principal));
    }
    return hivePrincipals;
  }


  public static HivePrincipal getHivePrincipal(String name, PrincipalType type) throws HiveException {
    return new HivePrincipal(name, AuthorizationUtils.getHivePrincipalType(type));
  }

  public static List<HivePrivilegeInfo> getPrivilegeInfos(List<HiveObjectPrivilege> privs)
      throws HiveException {
    List<HivePrivilegeInfo> hivePrivs = new ArrayList<HivePrivilegeInfo>();
    for (HiveObjectPrivilege priv : privs) {
      PrivilegeGrantInfo grantorInfo = priv.getGrantInfo();
      HiveObjectRef privObject = priv.getHiveObject();
      HivePrincipal hivePrincipal =
          getHivePrincipal(priv.getPrincipalName(), priv.getPrincipalType());
      HivePrincipal grantor =
          getHivePrincipal(grantorInfo.getGrantor(), grantorInfo.getGrantorType());
      HivePrivilegeObject object = getHiveObjectRef(privObject);
      HivePrivilege privilege = new HivePrivilege(grantorInfo.getPrivilege(), null);
      hivePrivs.add(new HivePrivilegeInfo(hivePrincipal, privilege, object, grantor,
          grantorInfo.isGrantOption(), grantorInfo.getCreateTime()));
    }
    return hivePrivs;
  }

  public static HivePrivilegeObject getHiveObjectRef(HiveObjectRef privObj) throws HiveException {
    if (privObj == null) {
      return null;
    }
    HivePrivilegeObjectType objType = getHiveObjType(privObj.getObjectType());
    return new HivePrivilegeObject(objType, privObj.getDbName(), privObj.getObjectName(),
        privObj.getPartValues(), privObj.getColumnName());
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
    case GROUP:
      return PrincipalType.GROUP;
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
    case GLOBAL:
      return HiveObjectType.GLOBAL;
    case DATABASE:
      return HiveObjectType.DATABASE;
    case TABLE_OR_VIEW:
      return HiveObjectType.TABLE;
    case PARTITION:
      return HiveObjectType.PARTITION;
    case COLUMN:
      return HiveObjectType.COLUMN;
    default:
      throw new HiveException("Unsupported type " + type);
    }
  }

  // V1 to V2 conversion.
  private static HivePrivilegeObjectType getHiveObjType(HiveObjectType type) throws HiveException {
    if (type == null) {
      return null;
    }
    switch(type){
      case GLOBAL:
        if (SessionState.get().getAuthorizationMode() == SessionState.AuthorizationMode.V2) {
          throw new HiveException(ErrorMsg.UNSUPPORTED_AUTHORIZATION_RESOURCE_TYPE_GLOBAL);
        }
        return HivePrivilegeObjectType.GLOBAL;
      case DATABASE:
        return HivePrivilegeObjectType.DATABASE;
      case TABLE:
        return HivePrivilegeObjectType.TABLE_OR_VIEW;
      case PARTITION:
        return HivePrivilegeObjectType.PARTITION;
      case COLUMN:
        if (SessionState.get().getAuthorizationMode() == SessionState.AuthorizationMode.V2) {
          throw new HiveException(ErrorMsg.UNSUPPORTED_AUTHORIZATION_RESOURCE_TYPE_COLUMN);
        }
        return HivePrivilegeObjectType.COLUMN;
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
    return new HiveObjectRef(objType, privObj.getDbname(), privObj.getObjectName(), null, null);
  }

  public static HivePrivObjectActionType getActionType(Entity privObject) {
    HivePrivObjectActionType actionType = HivePrivObjectActionType.OTHER;
    if (privObject instanceof WriteEntity) {
      WriteType writeType = ((WriteEntity) privObject).getWriteType();
      switch (writeType) {
      case INSERT:
        return HivePrivObjectActionType.INSERT;
      case INSERT_OVERWRITE:
        return HivePrivObjectActionType.INSERT_OVERWRITE;
      case UPDATE:
        return HivePrivObjectActionType.UPDATE;
      case DELETE:
        return HivePrivObjectActionType.DELETE;
      default:
        // Ignore other types for purposes of authorization
        break;
      }
    }
    return actionType;
  }

}
