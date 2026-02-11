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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAuthorizationCallEvent;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.rethrowException;
import static org.apache.hadoop.hive.metastore.HMSHandler.getIPAddress;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public abstract class PrivilegeHandler extends TransactionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeHandler.class);
  protected PrivilegeHandler(String name, Configuration conf) {
    super(name, conf);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName,
      List<String> groupNames) throws TException {
    firePreEvent(new PreAuthorizationCallEvent(this));
    String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : getDefaultCatalog(conf);
    HiveObjectType debug = hiveObject.getObjectType();
    if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
      String partName = getPartName(hiveObject);
      return this.get_column_privilege_set(catName, hiveObject.getDbName(), hiveObject
              .getObjectName(), partName, hiveObject.getColumnName(), userName,
          groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
      String partName = getPartName(hiveObject);
      return this.get_partition_privilege_set(catName, hiveObject.getDbName(),
          hiveObject.getObjectName(), partName, userName, groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
      return this.get_db_privilege_set(catName, hiveObject.getDbName(), userName,
          groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
      return this.get_connector_privilege_set(catName, hiveObject.getObjectName(), userName, groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
      return this.get_table_privilege_set(catName, hiveObject.getDbName(), hiveObject
          .getObjectName(), userName, groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
      return this.get_user_privilege_set(userName, groupNames);
    }
    return null;
  }

  private String getPartName(HiveObjectRef hiveObject) throws MetaException {
    String partName = null;
    List<String> partValue = hiveObject.getPartValues();
    if (partValue != null && partValue.size() > 0) {
      try {
        String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
            getDefaultCatalog(conf);
        GetTableRequest getTableRequest = new GetTableRequest(hiveObject.getDbName(), hiveObject.getObjectName());
        getTableRequest.setCatName(catName);
        Table table = get_table_core(getTableRequest);
        partName = Warehouse
            .makePartName(table.getPartitionKeys(), partValue);
      } catch (NoSuchObjectException e) {
        throw new MetaException(e.getMessage());
      }
    }
    return partName;
  }

  private PrincipalPrivilegeSet get_column_privilege_set(String catName, final String dbName,
      final String tableName, final String partName, final String columnName,
      final String userName, final List<String> groupNames) throws TException {
    incrementCounter("get_column_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getColumnPrivilegeSet(
          catName, dbName, tableName, partName, columnName, userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
      throws TException {

    incrementCounter("get_principals_in_role");
    firePreEvent(new PreAuthorizationCallEvent(this));
    Exception ex = null;
    GetPrincipalsInRoleResponse response = null;
    try {
      response = new GetPrincipalsInRoleResponse(getMS().listRoleMembers(request.getRoleName()));
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_principals_in_role", ex == null, ex);
    }
    return response;
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest request) throws TException {

    incrementCounter("get_role_grants_for_principal");
    firePreEvent(new PreAuthorizationCallEvent(this));
    Exception ex = null;
    List<RolePrincipalGrant> roleMaps = null;
    try {
      roleMaps = getMS().listRolesWithGrants(request.getPrincipal_name(), request.getPrincipal_type());
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_role_grants_for_principal", ex == null, ex);
    }

    //List<RolePrincipalGrant> roleGrantsList = getRolePrincipalGrants(roleMaps);
    return new GetRoleGrantsForPrincipalResponse(roleMaps);
  }

  private PrincipalPrivilegeSet get_db_privilege_set(String catName, final String dbName,
      final String userName, final List<String> groupNames) throws TException {
    incrementCounter("get_db_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getDBPrivilegeSet(catName, dbName, userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_connector_privilege_set(String catName, final String connectorName,
      final String userName, final List<String> groupNames) throws TException {
    incrementCounter("get_connector_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getConnectorPrivilegeSet(catName, connectorName, userName, groupNames);
    } catch (MetaException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return ret;

  }

  private PrincipalPrivilegeSet get_partition_privilege_set(
      String catName, final String dbName, final String tableName, final String partName,
      final String userName, final List<String> groupNames)
      throws TException {
    incrementCounter("get_partition_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getPartitionPrivilegeSet(catName, dbName, tableName, partName,
          userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_table_privilege_set(String catName, final String dbName,
      final String tableName, final String userName,
      final List<String> groupNames) throws TException {
    incrementCounter("get_table_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getTablePrivilegeSet(catName, dbName, tableName, userName,
          groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  @Override
  public boolean grant_role(final String roleName,
      final String principalName, final PrincipalType principalType,
      final String grantor, final PrincipalType grantorType, final boolean grantOption)
      throws TException {
    incrementCounter("add_role_member");
    firePreEvent(new PreAuthorizationCallEvent(this));
    if (PUBLIC.equals(roleName)) {
      throw new MetaException("No user can be added to " + PUBLIC +". Since all users implicitly"
          + " belong to " + PUBLIC + " role.");
    }
    Boolean ret;
    try {
      RawStore ms = getMS();
      Role role = ms.getRole(roleName);
      if(principalType == PrincipalType.ROLE){
        //check if this grant statement will end up creating a cycle
        if(isNewRoleAParent(principalName, roleName)){
          throw new MetaException("Cannot grant role " + principalName + " to " + roleName +
              " as " + roleName + " already belongs to the role " + principalName +
              ". (no cycles allowed)");
        }
      }
      ret = ms.grantRole(role, principalName, principalType, grantor, grantorType, grantOption);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  /**
   * Check if newRole is in parent hierarchy of curRole
   * @param newRole
   * @param curRole
   * @return true if newRole is curRole or present in its hierarchy
   * @throws MetaException
   */
  private boolean isNewRoleAParent(String newRole, String curRole) throws MetaException {
    if(newRole.equals(curRole)){
      return true;
    }
    //do this check recursively on all the parent roles of curRole
    List<Role> parentRoleMaps = getMS().listRoles(curRole, PrincipalType.ROLE);
    for(Role parentRole : parentRoleMaps){
      if(isNewRoleAParent(newRole, parentRole.getRoleName())){
        return true;
      }
    }
    return false;
  }

  @Override
  public List<Role> list_roles(final String principalName,
      final PrincipalType principalType) throws TException {
    incrementCounter("list_roles");
    firePreEvent(new PreAuthorizationCallEvent(this));
    return getMS().listRoles(principalName, principalType);
  }

  @Override
  public boolean create_role(final Role role) throws TException {
    incrementCounter("create_role");
    firePreEvent(new PreAuthorizationCallEvent(this));
    if (PUBLIC.equals(role.getRoleName())) {
      throw new MetaException(PUBLIC + " role implicitly exists. It can't be created.");
    }
    Boolean ret;
    try {
      ret = getMS().addRole(role.getRoleName(), role.getOwnerName());
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  @Override
  public boolean drop_role(final String roleName) throws TException {
    incrementCounter("drop_role");
    firePreEvent(new PreAuthorizationCallEvent(this));
    if (ADMIN.equals(roleName) || PUBLIC.equals(roleName)) {
      throw new MetaException(PUBLIC + "," + ADMIN + " roles can't be dropped.");
    }
    Boolean ret;
    try {
      ret = getMS().removeRole(roleName);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  @Override
  public List<String> get_role_names() throws TException {
    incrementCounter("get_role_names");
    firePreEvent(new PreAuthorizationCallEvent(this));
    List<String> ret;
    try {
      ret = getMS().listRoleNames();
      return ret;
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  @Override
  public boolean grant_privileges(final PrivilegeBag privileges) throws TException {
    incrementCounter("grant_privileges");
    firePreEvent(new PreAuthorizationCallEvent(this));
    Boolean ret;
    try {
      ret = getMS().grantPrivileges(privileges);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  @Override
  public boolean revoke_role(final String roleName, final String userName,
      final PrincipalType principalType) throws TException {
    return revoke_role(roleName, userName, principalType, false);
  }

  private boolean revoke_role(final String roleName, final String userName,
      final PrincipalType principalType, boolean grantOption) throws TException {
    incrementCounter("remove_role_member");
    firePreEvent(new PreAuthorizationCallEvent(this));
    if (PUBLIC.equals(roleName)) {
      throw new MetaException(PUBLIC + " role can't be revoked.");
    }
    Boolean ret;
    try {
      RawStore ms = getMS();
      Role mRole = ms.getRole(roleName);
      ret = ms.revokeRole(mRole, userName, principalType, grantOption);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request)
      throws TException {
    GrantRevokeRoleResponse response = new GrantRevokeRoleResponse();
    boolean grantOption = false;
    if (request.isSetGrantOption()) {
      grantOption = request.isGrantOption();
    }
    switch (request.getRequestType()) {
    case GRANT: {
      boolean result = grant_role(request.getRoleName(),
          request.getPrincipalName(), request.getPrincipalType(),
          request.getGrantor(), request.getGrantorType(), grantOption);
      response.setSuccess(result);
      break;
    }
    case REVOKE: {
      boolean result = revoke_role(request.getRoleName(), request.getPrincipalName(),
          request.getPrincipalType(), grantOption);
      response.setSuccess(result);
      break;
    }
    default:
      throw new MetaException("Unknown request type " + request.getRequestType());
    }

    return response;
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
      throws TException {
    GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
    switch (request.getRequestType()) {
    case GRANT: {
      boolean result = grant_privileges(request.getPrivileges());
      response.setSuccess(result);
      break;
    }
    case REVOKE: {
      boolean revokeGrantOption = false;
      if (request.isSetRevokeGrantOption()) {
        revokeGrantOption = request.isRevokeGrantOption();
      }
      boolean result = revoke_privileges(request.getPrivileges(), revokeGrantOption);
      response.setSuccess(result);
      break;
    }
    default:
      throw new MetaException("Unknown request type " + request.getRequestType());
    }

    return response;
  }

  @Override
  public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
      GrantRevokePrivilegeRequest grantRequest)
      throws TException {
    incrementCounter("refresh_privileges");
    firePreEvent(new PreAuthorizationCallEvent(this));
    GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
    try {
      boolean result = getMS().refreshPrivileges(objToRefresh, authorizer, grantRequest.getPrivileges());
      response.setSuccess(result);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return response;
  }

  @Override
  public boolean revoke_privileges(final PrivilegeBag privileges) throws TException {
    return revoke_privileges(privileges, false);
  }

  public boolean revoke_privileges(final PrivilegeBag privileges, boolean grantOption)
      throws TException {
    incrementCounter("revoke_privileges");
    firePreEvent(new PreAuthorizationCallEvent(this));
    Boolean ret;
    try {
      ret = getMS().revokePrivileges(privileges, grantOption);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_user_privilege_set(final String userName,
      final List<String> groupNames) throws TException {
    incrementCounter("get_user_privilege_set");
    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getUserPrivilegeSet(userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principalName,
      PrincipalType principalType, HiveObjectRef hiveObject)
      throws TException {
    firePreEvent(new PreAuthorizationCallEvent(this));
    String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : getDefaultCatalog(conf);
    if (hiveObject.getObjectType() == null) {
      return getAllPrivileges(principalName, principalType, catName);
    }
    if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
      return list_global_privileges(principalName, principalType);
    }
    if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
      return list_db_privileges(principalName, principalType, catName, hiveObject
          .getDbName());
    }
    if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
      return list_dc_privileges(principalName, principalType, hiveObject
          .getObjectName());
    }
    if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
      return list_table_privileges(principalName, principalType,
          catName, hiveObject.getDbName(), hiveObject.getObjectName());
    }
    if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
      return list_partition_privileges(principalName, principalType,
          catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
              .getPartValues());
    }
    if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
      if (hiveObject.getPartValues() == null || hiveObject.getPartValues().isEmpty()) {
        return list_table_column_privileges(principalName, principalType,
            catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject.getColumnName());
      }
      return list_partition_column_privileges(principalName, principalType,
          catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
              .getPartValues(), hiveObject.getColumnName());
    }
    return null;
  }

  private List<HiveObjectPrivilege> getAllPrivileges(String principalName,
      PrincipalType principalType, String catName) throws TException {
    List<HiveObjectPrivilege> privs = new ArrayList<>();
    privs.addAll(list_global_privileges(principalName, principalType));
    privs.addAll(list_db_privileges(principalName, principalType, catName, null));
    privs.addAll(list_dc_privileges(principalName, principalType, null));
    privs.addAll(list_table_privileges(principalName, principalType, catName, null, null));
    privs.addAll(list_partition_privileges(principalName, principalType, catName, null, null, null));
    privs.addAll(list_table_column_privileges(principalName, principalType, catName, null, null, null));
    privs.addAll(list_partition_column_privileges(principalName, principalType,
        catName, null, null, null, null));
    return privs;
  }

  private List<HiveObjectPrivilege> list_table_column_privileges(
      final String principalName, final PrincipalType principalType, String catName,
      final String dbName, final String tableName, final String columnName) throws TException {
    incrementCounter("list_table_column_privileges");

    try {
      if (dbName == null) {
        return getMS().listPrincipalTableColumnGrantsAll(principalName, principalType);
      }
      if (principalName == null) {
        return getMS().listTableColumnGrantsAll(catName, dbName, tableName, columnName);
      }
      return getMS().listPrincipalTableColumnGrants(principalName, principalType,
          catName, dbName, tableName, columnName);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_partition_column_privileges(
      final String principalName, final PrincipalType principalType,
      String catName, final String dbName, final String tableName, final List<String> partValues,
      final String columnName) throws TException {
    incrementCounter("list_partition_column_privileges");

    try {
      if (dbName == null) {
        return getMS().listPrincipalPartitionColumnGrantsAll(principalName, principalType);
      }
      GetTableRequest getTableRequest = new GetTableRequest(dbName, tableName);
      getTableRequest.setCatName(catName);
      Table tbl = get_table_core(getTableRequest);
      String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
      if (principalName == null) {
        return getMS().listPartitionColumnGrantsAll(catName, dbName, tableName, partName, columnName);
      }

      return getMS().listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName,
          tableName, partValues, partName, columnName);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_db_privileges(final String principalName,
      final PrincipalType principalType, String catName, final String dbName) throws TException {
    incrementCounter("list_security_db_grant");

    try {
      if (dbName == null) {
        return getMS().listPrincipalDBGrantsAll(principalName, principalType);
      }
      if (principalName == null) {
        return getMS().listDBGrantsAll(catName, dbName);
      } else {
        return getMS().listPrincipalDBGrants(principalName, principalType, catName, dbName);
      }
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_dc_privileges(final String principalName,
      final PrincipalType principalType, final String dcName) throws TException {
    incrementCounter("list_security_dc_grant");

    try {
      if (dcName == null) {
        return getMS().listPrincipalDCGrantsAll(principalName, principalType);
      }
      if (principalName == null) {
        return getMS().listDCGrantsAll(dcName);
      } else {
        return getMS().listPrincipalDCGrants(principalName, principalType, dcName);
      }
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_partition_privileges(
      final String principalName, final PrincipalType principalType,
      String catName, final String dbName, final String tableName, final List<String> partValues)
      throws TException {
    incrementCounter("list_security_partition_grant");

    try {
      if (dbName == null) {
        return getMS().listPrincipalPartitionGrantsAll(principalName, principalType);
      }
      GetTableRequest getTableRequest = new GetTableRequest(dbName, tableName);
      getTableRequest.setCatName(catName);
      Table tbl = get_table_core(getTableRequest);
      String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
      if (principalName == null) {
        return getMS().listPartitionGrantsAll(catName, dbName, tableName, partName);
      }
      return getMS().listPrincipalPartitionGrants(
          principalName, principalType, catName, dbName, tableName, partValues, partName);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_table_privileges(
      final String principalName, final PrincipalType principalType,
      String catName, final String dbName, final String tableName) throws TException {
    incrementCounter("list_security_table_grant");

    try {
      if (dbName == null) {
        return getMS().listPrincipalTableGrantsAll(principalName, principalType);
      }
      if (principalName == null) {
        return getMS().listTableGrantsAll(catName, dbName, tableName);
      }
      return getMS().listAllTableGrants(principalName, principalType, catName, dbName, tableName);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_global_privileges(
      final String principalName, final PrincipalType principalType) throws TException {
    incrementCounter("list_security_user_grant");

    try {
      if (principalName == null) {
        return getMS().listGlobalGrantsAll();
      }
      return getMS().listPrincipalGlobalGrants(principalName, principalType);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  @Override
  public void cancel_delegation_token(String token_str_form) throws TException {
    startFunction("cancel_delegation_token");
    boolean success = false;
    Exception ex = null;
    try {
      HiveMetaStore.cancelDelegationToken(token_str_form);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
    } finally {
      endFunction("cancel_delegation_token", success, ex);
    }
  }

  @Override
  public long renew_delegation_token(String token_str_form) throws TException {
    startFunction("renew_delegation_token");
    Long ret = null;
    Exception ex = null;
    try {
      ret = HiveMetaStore.renewDelegationToken(token_str_form);
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
    } finally {
      endFunction("renew_delegation_token", ret != null, ex);
    }
    return ret;
  }

  @Override
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
      throws TException {
    startFunction("get_delegation_token");
    String ret = null;
    Exception ex = null;
    try {
      ret =
          HiveMetaStore.getDelegationToken(token_owner,
              renewer_kerberos_principal_name, getIPAddress());
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class)
          .convertIfInstance(InterruptedException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_delegation_token", ret != null, ex);
    }
    return ret;
  }

  @Override
  public boolean add_token(String token_identifier, String delegation_token) throws TException {
    startFunction("add_token", ": " + token_identifier);
    boolean ret = false;
    Exception ex = null;
    try {
      ret = getMS().addToken(token_identifier, delegation_token);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("add_token", ret == true, ex);
    }
    return ret;
  }

  @Override
  public boolean remove_token(String token_identifier) throws TException {
    startFunction("remove_token", ": " + token_identifier);
    boolean ret = false;
    Exception ex = null;
    try {
      ret = getMS().removeToken(token_identifier);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("remove_token", ret == true, ex);
    }
    return ret;
  }

  @Override
  public String get_token(String token_identifier) throws TException {
    startFunction("get_token for", ": " + token_identifier);
    String ret = null;
    Exception ex = null;
    try {
      ret = getMS().getToken(token_identifier);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_token", ret != null, ex);
    }
    //Thrift cannot return null result
    return ret == null ? "" : ret;
  }

  @Override
  public List<String> get_all_token_identifiers() throws TException {
    startFunction("get_all_token_identifiers.");
    List<String> ret;
    Exception ex = null;
    try {
      ret = getMS().getAllTokenIdentifiers();
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_token_identifiers.", ex == null, ex);
    }
    return ret;
  }

  @Override
  public int add_master_key(String key) throws TException {
    startFunction("add_master_key.");
    int ret;
    Exception ex = null;
    try {
      ret = getMS().addMasterKey(key);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("add_master_key.", ex == null, ex);
    }
    return ret;
  }

  @Override
  public void update_master_key(int seq_number, String key) throws TException {
    startFunction("update_master_key.");
    Exception ex = null;
    try {
      getMS().updateMasterKey(seq_number, key);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("update_master_key.", ex == null, ex);
    }
  }

  @Override
  public boolean remove_master_key(int key_seq) throws TException {
    startFunction("remove_master_key.");
    Exception ex = null;
    boolean ret;
    try {
      ret = getMS().removeMasterKey(key_seq);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("remove_master_key.", ex == null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_master_keys() throws TException {
    startFunction("get_master_keys.");
    Exception ex = null;
    String [] ret = null;
    try {
      ret = getMS().getMasterKeys();
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_master_keys.", ret != null, ex);
    }
    return Arrays.asList(ret);
  }
}
