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

package org.apache.hadoop.hive.metastore.metastore.impl;

import com.google.common.base.Preconditions;

import javax.jdo.Query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDCPrivilege;
import org.apache.hadoop.hive.metastore.model.MDataConnector;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.metastore.GetHelper;
import org.apache.hadoop.hive.metastore.metastore.GetListHelper;
import org.apache.hadoop.hive.metastore.metastore.iface.PrivilegeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ObjectStore.convert;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class PrivilegeStoreImpl extends RawStoreAware implements PrivilegeStore {
  private static final Logger LOG = LoggerFactory.getLogger(PrivilegeStoreImpl.class);
  private Configuration conf;

  @Override
  public boolean addRole(String roleName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    MRole nameCheck = this.getMRole(roleName);
    if (nameCheck != null) {
      throw new InvalidObjectException("Role " + roleName + " already exists.");
    }
    int now = (int) (System.currentTimeMillis() / 1000);
    MRole mRole = new MRole(roleName, now, ownerName);
    pm.makePersistent(mRole);
    return true;
  }

  @Override
  public boolean grantRole(Role role, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, NoSuchObjectException,InvalidObjectException {
    MRoleMap roleMap = null;
    try {
      roleMap = this.getMSecurityUserRoleMap(userName, principalType, role
          .getRoleName());
    } catch (Exception e) {
    }
    if (roleMap != null) {
      throw new InvalidObjectException("Principal " + userName
          + " already has the role " + role.getRoleName());
    }
    if (principalType == PrincipalType.ROLE) {
      validateRole(userName);
    }
    MRole mRole = getMRole(role.getRoleName());
    long now = System.currentTimeMillis()/1000;
    MRoleMap roleMember = new MRoleMap(userName, principalType.toString(),
        mRole, (int) now, grantor, grantorType.toString(), grantOption);
    pm.makePersistent(roleMember);
    return true;
  }

  /**
   * Verify that role with given name exists, if not throw exception
   */
  private void validateRole(String roleName) throws NoSuchObjectException {
    // if grantee is a role, check if it exists
    MRole granteeRole = getMRole(roleName);
    if (granteeRole == null) {
      throw new NoSuchObjectException("Role " + roleName + " does not exist");
    }
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
      boolean grantOption) throws MetaException, NoSuchObjectException {
    MRoleMap roleMember = getMSecurityUserRoleMap(userName, principalType,
        role.getRoleName());
    if (grantOption) {
      // Revoke with grant option - only remove the grant option but keep the role.
      if (roleMember.getGrantOption()) {
        roleMember.setGrantOption(false);
      } else {
        throw new MetaException("User " + userName
            + " does not have grant option with role " + role.getRoleName());
      }
    } else {
      // No grant option in revoke, remove the whole role.
      pm.deletePersistent(roleMember);
    }
    return true;
  }

  private MRoleMap getMSecurityUserRoleMap(String userName, PrincipalType principalType,
      String roleName) {
    MRoleMap mRoleMember = null;
    Query query =
        pm.newQuery(MRoleMap.class,
            "principalName == t1 && principalType == t2 && role.roleName == t3");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    query.setUnique(true);
    mRoleMember = (MRoleMap) query.executeWithArray(userName, principalType.toString(), roleName);
    pm.retrieve(mRoleMember);;
    return mRoleMember;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException,
      NoSuchObjectException {
    try {
      MRole mRol = getMRole(roleName);
      pm.retrieve(mRol);
      if (mRol != null) {
        // first remove all the membership, the membership that this role has
        // been granted
        List<MRoleMap> roleMap = listMRoleMembers(mRol.getRoleName());
        if (CollectionUtils.isNotEmpty(roleMap)) {
          pm.deletePersistentAll(roleMap);
        }
        List<MRoleMap> roleMember = listMSecurityPrincipalMembershipRole(mRol
            .getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(roleMember)) {
          pm.deletePersistentAll(roleMember);
        }

        // then remove all the grants
        List<MGlobalPrivilege> userGrants = listPrincipalMGlobalGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(userGrants)) {
          pm.deletePersistentAll(userGrants);
        }

        List<MDBPrivilege> dbGrants = listPrincipalAllDBGrant(mRol
            .getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(dbGrants)) {
          pm.deletePersistentAll(dbGrants);
        }

        List<MDCPrivilege> dcGrants = listPrincipalAllDCGrant(mRol
            .getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(dcGrants)) {
          pm.deletePersistentAll(dcGrants);
        }

        List<MTablePrivilege> tabPartGrants = listPrincipalAllTableGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(tabPartGrants)) {
          pm.deletePersistentAll(tabPartGrants);
        }

        List<MPartitionPrivilege> partGrants = listPrincipalAllPartitionGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(partGrants)) {
          pm.deletePersistentAll(partGrants);
        }

        List<MTableColumnPrivilege> tblColumnGrants = listPrincipalAllTableColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(tblColumnGrants)) {
          pm.deletePersistentAll(tblColumnGrants);
        }

        List<MPartitionColumnPrivilege> partColumnGrants = listPrincipalAllPartitionColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(partColumnGrants)) {
          pm.deletePersistentAll(partColumnGrants);
        }

        // finally remove the role
        pm.deletePersistent(mRol);
      }
      return true;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  /**
   * Get all the roles in the role hierarchy that this user and groupNames belongs to
   */
  private Set<String> listAllRolesInHierarchy(String userName,
      List<String> groupNames) {
    List<MRoleMap> ret = new ArrayList<>();
    if(userName != null) {
      ret.addAll(listMRoles(userName, PrincipalType.USER));
    }
    if (groupNames != null) {
      for (String groupName: groupNames) {
        ret.addAll(listMRoles(groupName, PrincipalType.GROUP));
      }
    }
    // get names of these roles and its ancestors
    Set<String> roleNames = new HashSet<>();
    getAllRoleAncestors(roleNames, ret);
    return roleNames;
  }

  /**
   * Add role names of parentRoles and its parents to processedRoles
   */
  private void getAllRoleAncestors(Set<String> processedRoleNames, List<MRoleMap> parentRoles) {
    for (MRoleMap parentRole : parentRoles) {
      String parentRoleName = parentRole.getRole().getRoleName();
      if (!processedRoleNames.contains(parentRoleName)) {
        // unprocessed role: get its parents, add it to processed, and call this
        // function recursively
        List<MRoleMap> nextParentRoles = listMRoles(parentRoleName, PrincipalType.ROLE);
        processedRoleNames.add(parentRoleName);
        getAllRoleAncestors(processedRoleNames, nextParentRoles);
      }
    }
  }

  public List<MRoleMap> listMRoles(String principalName,
      PrincipalType principalType) {
    Query query = pm.newQuery(MRoleMap.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    query.setUnique(false);
    List<MRoleMap> mRoles =
        (List<MRoleMap>) query.executeWithArray(principalName, principalType.toString());
    pm.retrieveAll(mRoles);;
    List<MRoleMap> mRoleMember = new ArrayList<>(mRoles);

    if (principalType == PrincipalType.USER) {
      // All users belong to public role implicitly, add that role
      // TODO MS-SPLIT Change this back to HMSHandler.PUBLIC once HiveMetaStore has moved to
      // stand-alone metastore.
      //MRole publicRole = new MRole(HMSHandler.PUBLIC, 0, HMSHandler.PUBLIC);
      MRole publicRole = new MRole("public", 0, "public");
      mRoleMember.add(new MRoleMap(principalName, principalType.toString(), publicRole, 0, null,
          null, false));
    }

    return mRoleMember;
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    List<Role> result = new ArrayList<>();
    List<MRoleMap> roleMaps = listMRoles(principalName, principalType);
    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        MRole mrole = roleMap.getRole();
        Role role = new Role(mrole.getRoleName(), mrole.getCreateTime(), mrole.getOwnerName());
        result.add(role);
      }
    }
    return result;
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
      PrincipalType principalType) {
    List<RolePrincipalGrant> result = new ArrayList<>();
    List<MRoleMap> roleMaps = listMRoles(principalName, principalType);
    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        RolePrincipalGrant rolePrinGrant = new RolePrincipalGrant(
            roleMap.getRole().getRoleName(),
            roleMap.getPrincipalName(),
            PrincipalType.valueOf(roleMap.getPrincipalType()),
            roleMap.getGrantOption(),
            roleMap.getAddTime(),
            roleMap.getGrantor(),
            // no grantor type for public role, hence the null check
            roleMap.getGrantorType() == null ? null
                : PrincipalType.valueOf(roleMap.getGrantorType())
        );
        result.add(rolePrinGrant);
      }
    }
    return result;
  }

  private List<MRoleMap> listMSecurityPrincipalMembershipRole(final String roleName,
      final PrincipalType principalType) throws Exception {
    LOG.debug("Executing listMSecurityPrincipalMembershipRole");
    Query query = pm.newQuery(MRoleMap.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    final List<MRoleMap> mRoleMemebership = (List<MRoleMap>) query.execute(roleName, principalType.toString());

    LOG.debug("Retrieving all objects for listMSecurityPrincipalMembershipRole");
    pm.retrieveAll(mRoleMemebership);
    LOG.debug("Done retrieving all objects for listMSecurityPrincipalMembershipRole: {}", mRoleMemebership);

    return Collections.unmodifiableList(new ArrayList<>(mRoleMemebership));
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    MRole mRole = this.getMRole(roleName);
    if (mRole == null) {
      throw new NoSuchObjectException(roleName + " role can not be found.");
    }
    return new Role(mRole.getRoleName(), mRole.getCreateTime(), mRole
        .getOwnerName());
  }

  private MRole getMRole(String roleName) {
    MRole mrole = null;
    Query query = pm.newQuery(MRole.class, "roleName == t1");
    query.declareParameters("java.lang.String t1");
    query.setUnique(true);
    mrole = (MRole) query.execute(roleName);
    pm.retrieve(mrole);
    return mrole;
  }

  @Override
  public List<String> listRoleNames() {
    LOG.debug("Executing listAllRoleNames");
    Query query = pm.newQuery("select roleName from org.apache.hadoop.hive.metastore.model.MRole");
    query.setResult("roleName");
    Collection names = (Collection) query.execute();
    List<String> roleNames = new ArrayList<>();
    for (Iterator i = names.iterator(); i.hasNext();) {
      roleNames.add((String) i.next());
    }
    return roleNames;
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    if (userName != null) {
      List<MGlobalPrivilege> user = this.listPrincipalMGlobalGrants(userName, PrincipalType.USER);
      if(CollectionUtils.isNotEmpty(user)) {
        Map<String, List<PrivilegeGrantInfo>> userPriv = new HashMap<>();
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(user.size());
        for (int i = 0; i < user.size(); i++) {
          MGlobalPrivilege item = user.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        userPriv.put(userName, grantInfos);
        ret.setUserPrivileges(userPriv);
      }
    }
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> groupPriv = new HashMap<>();
      for(String groupName: groupNames) {
        List<MGlobalPrivilege> group =
            this.listPrincipalMGlobalGrants(groupName, PrincipalType.GROUP);
        if(CollectionUtils.isNotEmpty(group)) {
          List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(group.size());
          for (int i = 0; i < group.size(); i++) {
            MGlobalPrivilege item = group.get(i);
            grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
                .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
                .getGrantorType()), item.getGrantOption()));
          }
          groupPriv.put(groupName, grantInfos);
        }
      }
      ret.setGroupPrivileges(groupPriv);
    }
    return ret;
  }

  private List<PrivilegeGrantInfo> getDBPrivilege(String catName, String dbName,
      String principalName, PrincipalType principalType) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    if (principalName != null) {
      List<MDBPrivilege> userNameDbPriv = this.listPrincipalMDBGrants(
          principalName, principalType, catName, dbName);
      if (CollectionUtils.isNotEmpty(userNameDbPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameDbPriv.size());
        for (int i = 0; i < userNameDbPriv.size(); i++) {
          MDBPrivilege item = userNameDbPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return Collections.emptyList();
  }


  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    if (userName != null) {
      Map<String, List<PrivilegeGrantInfo>> dbUserPriv = new HashMap<>();
      dbUserPriv.put(userName, getDBPrivilege(catName, dbName, userName,
          PrincipalType.USER));
      ret.setUserPrivileges(dbUserPriv);
    }
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = new HashMap<>();
      for (String groupName : groupNames) {
        dbGroupPriv.put(groupName, getDBPrivilege(catName, dbName, groupName,
            PrincipalType.GROUP));
      }
      ret.setGroupPrivileges(dbGroupPriv);
    }
    Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
    if (CollectionUtils.isNotEmpty(roleNames)) {
      Map<String, List<PrivilegeGrantInfo>> dbRolePriv = new HashMap<>();
      for (String roleName : roleNames) {
        dbRolePriv
            .put(roleName, getDBPrivilege(catName, dbName, roleName, PrincipalType.ROLE));
      }
      ret.setRolePrivileges(dbRolePriv);
    }
    return ret;
  }

  private List<PrivilegeGrantInfo> getConnectorPrivilege(String catName, String connectorName,
      String principalName, PrincipalType principalType) {

    // normalize string name
    catName = normalizeIdentifier(catName);
    connectorName = normalizeIdentifier(connectorName);

    if (principalName != null) {
      // get all data connector granted privilege
      List<MDCPrivilege> userNameDcPriv = this.listPrincipalMDCGrants(
          principalName, principalType, catName, connectorName);

      // populate and return grantInfos
      if (CollectionUtils.isNotEmpty(userNameDcPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameDcPriv.size());
        for (int i = 0; i < userNameDcPriv.size(); i++) {
          MDCPrivilege item = userNameDcPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }

    // return empty list if no principalName
    return Collections.emptyList();
  }

  @Override
  public PrincipalPrivilegeSet getConnectorPrivilegeSet (String catName, String connectorName,
      String userName, List<String> groupNames)  throws InvalidObjectException,
      MetaException {
    catName = normalizeIdentifier(catName);
    connectorName = normalizeIdentifier(connectorName);

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    // get user privileges
    if (userName != null) {
      Map<String, List<PrivilegeGrantInfo>> connectorUserPriv = new HashMap<>();
      connectorUserPriv.put(userName, getConnectorPrivilege(catName, connectorName, userName,
          PrincipalType.USER));
      ret.setUserPrivileges(connectorUserPriv);
    }

    // get group privileges
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = new HashMap<>();
      for (String groupName : groupNames) {
        dbGroupPriv.put(groupName, getConnectorPrivilege(catName, connectorName, groupName,
            PrincipalType.GROUP));
      }
      ret.setGroupPrivileges(dbGroupPriv);
    }

    // get role privileges
    Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
    if (CollectionUtils.isNotEmpty(roleNames)) {
      Map<String, List<PrivilegeGrantInfo>> dbRolePriv = new HashMap<>();
      for (String roleName : roleNames) {
        dbRolePriv.put(roleName, getConnectorPrivilege(catName, connectorName, roleName,
            PrincipalType.ROLE));
      }
      ret.setRolePrivileges(dbRolePriv);
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(TableName table, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    String tableName = normalizeIdentifier(table.getTable());
    String dbName = normalizeIdentifier(table.getDb());
    String catName = normalizeIdentifier(table.getCat());
    if (userName != null) {
      Map<String, List<PrivilegeGrantInfo>> partUserPriv = new HashMap<>();
      partUserPriv.put(userName, getPartitionPrivilege(catName, dbName,
          tableName, partition, userName, PrincipalType.USER));
      ret.setUserPrivileges(partUserPriv);
    }
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> partGroupPriv = new HashMap<>();
      for (String groupName : groupNames) {
        partGroupPriv.put(groupName, getPartitionPrivilege(catName, dbName, tableName,
            partition, groupName, PrincipalType.GROUP));
      }
      ret.setGroupPrivileges(partGroupPriv);
    }
    Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
    if (CollectionUtils.isNotEmpty(roleNames)) {
      Map<String, List<PrivilegeGrantInfo>> partRolePriv = new HashMap<>();
      for (String roleName : roleNames) {
        partRolePriv.put(roleName, getPartitionPrivilege(catName, dbName, tableName,
            partition, roleName, PrincipalType.ROLE));
      }
      ret.setRolePrivileges(partRolePriv);
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(TableName table, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    String tableName = normalizeIdentifier(table.getTable());
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());

    if (userName != null) {
      Map<String, List<PrivilegeGrantInfo>> tableUserPriv = new HashMap<>();
      tableUserPriv.put(userName, getTablePrivilege(catName, dbName,
          tableName, userName, PrincipalType.USER));
      ret.setUserPrivileges(tableUserPriv);
    }
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> tableGroupPriv = new HashMap<>();
      for (String groupName : groupNames) {
        tableGroupPriv.put(groupName, getTablePrivilege(catName, dbName, tableName,
            groupName, PrincipalType.GROUP));
      }
      ret.setGroupPrivileges(tableGroupPriv);
    }
    Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
    if (CollectionUtils.isNotEmpty(roleNames)) {
      Map<String, List<PrivilegeGrantInfo>> tableRolePriv = new HashMap<>();
      for (String roleName : roleNames) {
        tableRolePriv.put(roleName, getTablePrivilege(catName, dbName, tableName,
            roleName, PrincipalType.ROLE));
      }
      ret.setRolePrivileges(tableRolePriv);
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(TableName table, String partitionName, String columnName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    String tableName = normalizeIdentifier(table.getTable());
    String dbName = normalizeIdentifier(table.getDb());
    columnName = normalizeIdentifier(columnName);
    String catName = normalizeIdentifier(table.getCat());

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    if (userName != null) {
      Map<String, List<PrivilegeGrantInfo>> columnUserPriv = new HashMap<>();
      columnUserPriv.put(userName, getColumnPrivilege(catName, dbName, tableName,
          columnName, partitionName, userName, PrincipalType.USER));
      ret.setUserPrivileges(columnUserPriv);
    }
    if (CollectionUtils.isNotEmpty(groupNames)) {
      Map<String, List<PrivilegeGrantInfo>> columnGroupPriv = new HashMap<>();
      for (String groupName : groupNames) {
        columnGroupPriv.put(groupName, getColumnPrivilege(catName, dbName, tableName,
            columnName, partitionName, groupName, PrincipalType.GROUP));
      }
      ret.setGroupPrivileges(columnGroupPriv);
    }
    Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
    if (CollectionUtils.isNotEmpty(roleNames)) {
      Map<String, List<PrivilegeGrantInfo>> columnRolePriv = new HashMap<>();
      for (String roleName : roleNames) {
        columnRolePriv.put(roleName, getColumnPrivilege(catName, dbName, tableName,
            columnName, partitionName, roleName, PrincipalType.ROLE));
      }
      ret.setRolePrivileges(columnRolePriv);
    }
    return ret;
  }

  private List<PrivilegeGrantInfo> getPartitionPrivilege(String catName, String dbName,
      String tableName, String partName, String principalName,
      PrincipalType principalType) {

    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    if (principalName != null) {
      List<MPartitionPrivilege> userNameTabPartPriv = this
          .listPrincipalMPartitionGrants(principalName, principalType,
              catName, dbName, tableName, partName);
      if (CollectionUtils.isNotEmpty(userNameTabPartPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MPartitionPrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(),
              getPrincipalTypeFromStr(item.getGrantorType()), item.getGrantOption()));

        }
        return grantInfos;
      }
    }
    return new ArrayList<>(0);
  }

  public static PrincipalType getPrincipalTypeFromStr(String str) {
    return str == null ? null : PrincipalType.valueOf(str);
  }

  private List<PrivilegeGrantInfo> getTablePrivilege(String catName, String dbName,
      String tableName, String principalName, PrincipalType principalType) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    if (principalName != null) {
      List<MTablePrivilege> userNameTabPartPriv = this
          .listAllMTableGrants(principalName, principalType,
              catName, dbName, tableName);
      if (CollectionUtils.isNotEmpty(userNameTabPartPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MTablePrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return Collections.emptyList();
  }

  private List<PrivilegeGrantInfo> getColumnPrivilege(String catName, String dbName,
      String tableName, String columnName, String partitionName,
      String principalName, PrincipalType principalType) {

    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    catName = normalizeIdentifier(catName);

    if (partitionName == null) {
      List<MTableColumnPrivilege> userNameColumnPriv = this
          .listPrincipalMTableColumnGrants(principalName, principalType,
              catName, dbName, tableName, columnName);
      if (CollectionUtils.isNotEmpty(userNameColumnPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MTableColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    } else {
      List<MPartitionColumnPrivilege> userNameColumnPriv = this
          .listPrincipalMPartitionColumnGrants(principalName,
              principalType, catName, dbName, tableName, partitionName, columnName);
      if (CollectionUtils.isNotEmpty(userNameColumnPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MPartitionColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return Collections.emptyList();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    int now = (int) (System.currentTimeMillis() / 1000);
    List<Object> persistentObjs = new ArrayList<>();

    List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();

    if (CollectionUtils.isNotEmpty(privilegeList)) {
      Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();
      Set<String> privSet = new HashSet<>();
      while (privIter.hasNext()) {
        HiveObjectPrivilege privDef = privIter.next();
        HiveObjectRef hiveObject = privDef.getHiveObject();
        String privilegeStr = privDef.getGrantInfo().getPrivilege();
        String[] privs = privilegeStr.split(",");
        String userName = privDef.getPrincipalName();
        String authorizer = privDef.getAuthorizer();
        PrincipalType principalType = privDef.getPrincipalType();
        String grantor = privDef.getGrantInfo().getGrantor();
        String grantorType = privDef.getGrantInfo().getGrantorType().toString();
        boolean grantOption = privDef.getGrantInfo().isGrantOption();
        privSet.clear();

        if(principalType == PrincipalType.ROLE){
          validateRole(userName);
        }

        String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
            getDefaultCatalog(conf);
        if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
          List<MGlobalPrivilege> globalPrivs = this
              .listPrincipalMGlobalGrants(userName, principalType, authorizer);
          for (MGlobalPrivilege priv : globalPrivs) {
            if (priv.getGrantor().equalsIgnoreCase(grantor)) {
              privSet.add(priv.getPrivilege());
            }
          }
          for (String privilege : privs) {
            if (privSet.contains(privilege)) {
              throw new InvalidObjectException(privilege
                  + " is already granted by " + grantor);
            }
            MGlobalPrivilege mGlobalPrivs = new MGlobalPrivilege(userName,
                principalType.toString(), privilege, now, grantor, grantorType, grantOption,
                authorizer);
            persistentObjs.add(mGlobalPrivs);
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
          MDatabase dbObj = baseStore.ensureGetMDatabase(catName, hiveObject.getDbName());
          List<MDBPrivilege> dbPrivs = this.listPrincipalMDBGrants(
              userName, principalType, catName, hiveObject.getDbName(), authorizer);
          for (MDBPrivilege priv : dbPrivs) {
            if (priv.getGrantor().equalsIgnoreCase(grantor)) {
              privSet.add(priv.getPrivilege());
            }
          }
          for (String privilege : privs) {
            if (privSet.contains(privilege)) {
              throw new InvalidObjectException(privilege
                  + " is already granted on database "
                  + hiveObject.getDbName() + " by " + grantor);
            }
            MDBPrivilege mDb = new MDBPrivilege(userName, principalType
                .toString(), dbObj, privilege, now, grantor, grantorType, grantOption, authorizer);
            persistentObjs.add(mDb);
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
          MDataConnector dcObj = convert(baseStore.getDataConnector(hiveObject.getObjectName()));
          List<MDCPrivilege> dcPrivs = this.listPrincipalMDCGrants(userName, principalType,
              hiveObject.getObjectName(), authorizer);
          for (MDCPrivilege priv : dcPrivs) {
            if (priv.getGrantor().equalsIgnoreCase(grantor)) {
              privSet.add(priv.getPrivilege());
            }
          }
          for (String privilege : privs) {
            if (privSet.contains(privilege)) {
              throw new InvalidObjectException(privilege
                  + " is already granted on data connector "
                  + hiveObject.getDbName() + " by " + grantor);
            }
            MDCPrivilege mDc = new MDCPrivilege(userName, principalType
                .toString(), dcObj, privilege, now, grantor, grantorType, grantOption, authorizer);
            persistentObjs.add(mDc);
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
          MTable tblObj = baseStore.ensureGetMTable(catName, hiveObject.getDbName(), hiveObject
              .getObjectName());
          if (tblObj != null) {
            List<MTablePrivilege> tablePrivs = this
                .listAllMTableGrants(userName, principalType,
                    catName, hiveObject.getDbName(), hiveObject.getObjectName(), authorizer);
            for (MTablePrivilege priv : tablePrivs) {
              if (priv.getGrantor() != null
                  && priv.getGrantor().equalsIgnoreCase(grantor)) {
                privSet.add(priv.getPrivilege());
              }
            }
            for (String privilege : privs) {
              if (privSet.contains(privilege)) {
                throw new InvalidObjectException(privilege
                    + " is already granted on table ["
                    + hiveObject.getDbName() + ","
                    + hiveObject.getObjectName() + "] by " + grantor);
              }
              MTablePrivilege mTab = new MTablePrivilege(
                  userName, principalType.toString(), tblObj,
                  privilege, now, grantor, grantorType, grantOption, authorizer);
              persistentObjs.add(mTab);
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
          MPartition partObj = baseStore.ensureGetMPartition(new TableName(catName, hiveObject.getDbName(),
              hiveObject.getObjectName()), hiveObject.getPartValues());
          String partName = null;
          if (partObj != null) {
            partName = partObj.getPartitionName();
            List<MPartitionPrivilege> partPrivs = this
                .listPrincipalMPartitionGrants(userName,
                    principalType, catName, hiveObject.getDbName(), hiveObject
                        .getObjectName(), partObj.getPartitionName(), authorizer);
            for (MPartitionPrivilege priv : partPrivs) {
              if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                privSet.add(priv.getPrivilege());
              }
            }
            for (String privilege : privs) {
              if (privSet.contains(privilege)) {
                throw new InvalidObjectException(privilege
                    + " is already granted on partition ["
                    + hiveObject.getDbName() + ","
                    + hiveObject.getObjectName() + ","
                    + partName + "] by " + grantor);
              }
              MPartitionPrivilege mTab = new MPartitionPrivilege(userName,
                  principalType.toString(), partObj, privilege, now, grantor,
                  grantorType, grantOption, authorizer);
              persistentObjs.add(mTab);
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
          MTable tblObj = baseStore.ensureGetMTable(catName, hiveObject.getDbName(), hiveObject
              .getObjectName());
          if (tblObj != null) {
            if (hiveObject.getPartValues() != null) {
              MPartition partObj = null;
              List<MPartitionColumnPrivilege> colPrivs = null;
              partObj = baseStore.ensureGetMPartition(new TableName(catName, hiveObject.getDbName(), hiveObject
                  .getObjectName()), hiveObject.getPartValues());
              if (partObj == null) {
                continue;
              }
              colPrivs = this.listPrincipalMPartitionColumnGrants(
                  userName, principalType, catName, hiveObject.getDbName(), hiveObject
                      .getObjectName(), partObj.getPartitionName(),
                  hiveObject.getColumnName(), authorizer);

              for (MPartitionColumnPrivilege priv : colPrivs) {
                if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                  privSet.add(priv.getPrivilege());
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on column "
                      + hiveObject.getColumnName() + " ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + ","
                      + partObj.getPartitionName() + "] by " + grantor);
                }
                MPartitionColumnPrivilege mCol = new MPartitionColumnPrivilege(userName,
                    principalType.toString(), partObj, hiveObject
                    .getColumnName(), privilege, now, grantor, grantorType,
                    grantOption, authorizer);
                persistentObjs.add(mCol);
              }

            } else {
              List<MTableColumnPrivilege> colPrivs = null;
              colPrivs = this.listPrincipalMTableColumnGrants(
                  userName, principalType, catName, hiveObject.getDbName(), hiveObject
                      .getObjectName(), hiveObject.getColumnName(), authorizer);

              for (MTableColumnPrivilege priv : colPrivs) {
                if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                  privSet.add(priv.getPrivilege());
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on column "
                      + hiveObject.getColumnName() + " ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + "] by " + grantor);
                }
                MTableColumnPrivilege mCol = new MTableColumnPrivilege(userName,
                    principalType.toString(), tblObj, hiveObject
                    .getColumnName(), privilege, now, grantor, grantorType,
                    grantOption, authorizer);
                persistentObjs.add(mCol);
              }
            }
          }
        }
      }
    }
    if (CollectionUtils.isNotEmpty(persistentObjs)) {
      pm.makePersistentAll(persistentObjs);
    }
    return true;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    List<Object> persistentObjs = new ArrayList<>();

    List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();

    if (CollectionUtils.isNotEmpty(privilegeList)) {
      Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();

      while (privIter.hasNext()) {
        HiveObjectPrivilege privDef = privIter.next();
        HiveObjectRef hiveObject = privDef.getHiveObject();
        String privilegeStr = privDef.getGrantInfo().getPrivilege();
        if (privilegeStr == null || privilegeStr.trim().equals("")) {
          continue;
        }
        String[] privs = privilegeStr.split(",");
        String userName = privDef.getPrincipalName();
        PrincipalType principalType = privDef.getPrincipalType();

        String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
            getDefaultCatalog(conf);
        if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
          List<MGlobalPrivilege> mSecUser = this.listPrincipalMGlobalGrants(
              userName, principalType);
          boolean found = false;
          for (String privilege : privs) {
            for (MGlobalPrivilege userGrant : mSecUser) {
              String userGrantPrivs = userGrant.getPrivilege();
              if (privilege.equals(userGrantPrivs)) {
                found = true;
                if (grantOption) {
                  if (userGrant.getGrantOption()) {
                    userGrant.setGrantOption(false);
                  } else {
                    throw new MetaException("User " + userName
                        + " does not have grant option with privilege " + privilege);
                  }
                }
                persistentObjs.add(userGrant);
                break;
              }
            }
            if (!found) {
              throw new InvalidObjectException(
                  "No user grant found for privileges " + privilege);
            }
          }

        } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
          String db = hiveObject.getDbName();
          boolean found = false;
          List<MDBPrivilege> dbGrants = this.listPrincipalMDBGrants(
              userName, principalType, catName, db);
          for (String privilege : privs) {
            for (MDBPrivilege dbGrant : dbGrants) {
              String dbGrantPriv = dbGrant.getPrivilege();
              if (privilege.equals(dbGrantPriv)) {
                found = true;
                if (grantOption) {
                  if (dbGrant.getGrantOption()) {
                    dbGrant.setGrantOption(false);
                  } else {
                    throw new MetaException("User " + userName
                        + " does not have grant option with privilege " + privilege);
                  }
                }
                persistentObjs.add(dbGrant);
                break;
              }
            }
            if (!found) {
              throw new InvalidObjectException(
                  "No database grant found for privileges " + privilege
                      + " on database " + db);
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
          String dc = hiveObject.getObjectName();
          boolean found = false;
          List<MDCPrivilege> dcGrants = this.listPrincipalMDCGrants(
              userName, principalType, catName, dc);
          for (String privilege : privs) {
            for (MDCPrivilege dcGrant : dcGrants) {
              String dcGrantPriv = dcGrant.getPrivilege();
              if (privilege.equals(dcGrantPriv)) {
                found = true;
                if (grantOption) {
                  if (dcGrant.getGrantOption()) {
                    dcGrant.setGrantOption(false);
                  } else {
                    throw new MetaException("User " + userName
                        + " does not have grant option with privilege " + privilege);
                  }
                }
                persistentObjs.add(dcGrant);
                break;
              }
            }
            if (!found) {
              throw new InvalidObjectException(
                  "No dataconnector grant found for privileges " + privilege
                      + " on data connector " + dc);
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
          boolean found = false;
          List<MTablePrivilege> tableGrants = this
              .listAllMTableGrants(userName, principalType,
                  catName, hiveObject.getDbName(), hiveObject.getObjectName());
          for (String privilege : privs) {
            for (MTablePrivilege tabGrant : tableGrants) {
              String tableGrantPriv = tabGrant.getPrivilege();
              if (privilege.equalsIgnoreCase(tableGrantPriv)) {
                found = true;
                if (grantOption) {
                  if (tabGrant.getGrantOption()) {
                    tabGrant.setGrantOption(false);
                  } else {
                    throw new MetaException("User " + userName
                        + " does not have grant option with privilege " + privilege);
                  }
                }
                persistentObjs.add(tabGrant);
                break;
              }
            }
            if (!found) {
              throw new InvalidObjectException("No grant (" + privilege
                  + ") found " + " on table " + hiveObject.getObjectName()
                  + ", database is " + hiveObject.getDbName());
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
          boolean found = false;
          Table tabObj = baseStore.unwrap(TableStore.class).getTable(
              new TableName(catName, hiveObject.getDbName(), hiveObject.getObjectName()), null, -1);
          String partName = null;
          if (hiveObject.getPartValues() != null) {
            partName = Warehouse.makePartName(tabObj.getPartitionKeys(), hiveObject.getPartValues());
          }
          List<MPartitionPrivilege> partitionGrants = this
              .listPrincipalMPartitionGrants(userName, principalType,
                  catName, hiveObject.getDbName(), hiveObject.getObjectName(), partName);
          for (String privilege : privs) {
            for (MPartitionPrivilege partGrant : partitionGrants) {
              String partPriv = partGrant.getPrivilege();
              if (partPriv.equalsIgnoreCase(privilege)) {
                found = true;
                if (grantOption) {
                  if (partGrant.getGrantOption()) {
                    partGrant.setGrantOption(false);
                  } else {
                    throw new MetaException("User " + userName
                        + " does not have grant option with privilege " + privilege);
                  }
                }
                persistentObjs.add(partGrant);
                break;
              }
            }
            if (!found) {
              throw new InvalidObjectException("No grant (" + privilege
                  + ") found " + " on table " + tabObj.getTableName()
                  + ", partition is " + partName + ", database is " + tabObj.getDbName());
            }
          }
        } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
          Table tabObj = baseStore.unwrap(TableStore.class).getTable(
              new TableName(catName, hiveObject.getDbName(), hiveObject.getObjectName()), null, -1);
          String partName = null;
          if (hiveObject.getPartValues() != null) {
            partName = Warehouse.makePartName(tabObj.getPartitionKeys(), hiveObject.getPartValues());
          }

          if (partName != null) {
            List<MPartitionColumnPrivilege> mSecCol = listPrincipalMPartitionColumnGrants(
                userName, principalType, catName, hiveObject.getDbName(), hiveObject
                    .getObjectName(), partName, hiveObject.getColumnName());
            boolean found = false;
            for (String privilege : privs) {
              for (MPartitionColumnPrivilege col : mSecCol) {
                String colPriv = col.getPrivilege();
                if (colPriv.equalsIgnoreCase(privilege)) {
                  found = true;
                  if (grantOption) {
                    if (col.getGrantOption()) {
                      col.setGrantOption(false);
                    } else {
                      throw new MetaException("User " + userName
                          + " does not have grant option with privilege " + privilege);
                    }
                  }
                  persistentObjs.add(col);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + tabObj.getTableName()
                    + ", partition is " + partName + ", column name = "
                    + hiveObject.getColumnName() + ", database is "
                    + tabObj.getDbName());
              }
            }
          } else {
            List<MTableColumnPrivilege> mSecCol = listPrincipalMTableColumnGrants(
                userName, principalType, catName, hiveObject.getDbName(), hiveObject
                    .getObjectName(), hiveObject.getColumnName());
            boolean found = false;
            for (String privilege : privs) {
              for (MTableColumnPrivilege col : mSecCol) {
                String colPriv = col.getPrivilege();
                if (colPriv.equalsIgnoreCase(privilege)) {
                  found = true;
                  if (grantOption) {
                    if (col.getGrantOption()) {
                      col.setGrantOption(false);
                    } else {
                      throw new MetaException("User " + userName
                          + " does not have grant option with privilege " + privilege);
                    }
                  }
                  persistentObjs.add(col);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + tabObj.getTableName()
                    + ", column name = "
                    + hiveObject.getColumnName() + ", database is "
                    + tabObj.getDbName());
              }
            }
          }

        }
      }
    }

    if (CollectionUtils.isNotEmpty(persistentObjs)) {
      if (grantOption) {
        // If grant option specified, only update the privilege, don't remove it.
        // Grant option has already been removed from the privileges in the section above
      } else {
        pm.deletePersistentAll(persistentObjs);
      }
    }
    return true;
  }

  class PrivilegeWithoutCreateTimeComparator implements Comparator<HiveObjectPrivilege> {
    @Override
    public int compare(HiveObjectPrivilege o1, HiveObjectPrivilege o2) {
      int createTime1 = o1.getGrantInfo().getCreateTime();
      int createTime2 = o2.getGrantInfo().getCreateTime();
      o1.getGrantInfo().setCreateTime(0);
      o2.getGrantInfo().setCreateTime(0);
      int result = o1.compareTo(o2);
      o1.getGrantInfo().setCreateTime(createTime1);
      o2.getGrantInfo().setCreateTime(createTime2);
      return result;
    }
  }

  @Override
  public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    Set<HiveObjectPrivilege> revokePrivilegeSet
        = new TreeSet<>(new PrivilegeWithoutCreateTimeComparator());
    Set<HiveObjectPrivilege> grantPrivilegeSet
        = new TreeSet<>(new PrivilegeWithoutCreateTimeComparator());

    List<HiveObjectPrivilege> grants = null;
    String catName = objToRefresh.isSetCatName() ? objToRefresh.getCatName() :
        getDefaultCatalog(conf);
    switch (objToRefresh.getObjectType()) {
    case DATABASE:
      try {
        grants = this.listDBGrantsAll(catName, objToRefresh.getDbName(), authorizer);
      } catch (Exception e) {
        throw new MetaException(e.getMessage());
      }
      break;
    case DATACONNECTOR:
      try {
        grants = this.listDCGrantsAll(objToRefresh.getObjectName(), authorizer);
      } catch (Exception e) {
        throw new MetaException(e.getMessage());
      }
      break;
    case TABLE:
      grants = listTableGrantsAll(new TableName(catName, objToRefresh.getDbName(), objToRefresh.getObjectName()), authorizer);
      break;
    case COLUMN:
      Preconditions.checkArgument(objToRefresh.getColumnName()==null, "columnName must be null");
      grants = getTableAllColumnGrants(catName, objToRefresh.getDbName(),
          objToRefresh.getObjectName(), authorizer);
      break;
    default:
      throw new MetaException("Unexpected object type " + objToRefresh.getObjectType());
    }
    revokePrivilegeSet.addAll(grants);

    // Optimize revoke/grant list, remove the overlapping
    if (grantPrivileges.getPrivileges() != null) {
      for (HiveObjectPrivilege grantPrivilege : grantPrivileges.getPrivileges()) {
        if (revokePrivilegeSet.contains(grantPrivilege)) {
          revokePrivilegeSet.remove(grantPrivilege);
        } else {
          grantPrivilegeSet.add(grantPrivilege);
        }
      }
    }
    if (!revokePrivilegeSet.isEmpty()) {
      LOG.debug("Found " + revokePrivilegeSet.size() + " new revoke privileges to be synced.");
      PrivilegeBag remainingRevokePrivileges = new PrivilegeBag();
      for (HiveObjectPrivilege revokePrivilege : revokePrivilegeSet) {
        remainingRevokePrivileges.addToPrivileges(revokePrivilege);
      }
      revokePrivileges(remainingRevokePrivileges, false);
    } else {
      LOG.debug("No new revoke privileges are required to be synced.");
    }
    if (!grantPrivilegeSet.isEmpty()) {
      LOG.debug("Found " + grantPrivilegeSet.size() + " new grant privileges to be synced.");
      PrivilegeBag remainingGrantPrivileges = new PrivilegeBag();
      for (HiveObjectPrivilege grantPrivilege : grantPrivilegeSet) {
        remainingGrantPrivileges.addToPrivileges(grantPrivilege);
      }
      grantPrivileges(remainingGrantPrivileges);
    } else {
      LOG.debug("No new grant privileges are required to be synced.");
    }
    return true;
  }

  private List<HiveObjectPrivilege> getTableAllColumnGrants(String catalog, String db,
      String tableName, String authorizer)
      throws MetaException, NoSuchObjectException {
    String catName = normalizeIdentifier(catalog);
    String dbName = normalizeIdentifier(db);
    String tblName = normalizeIdentifier(tableName);
    return new GetListHelper<HiveObjectPrivilege>(this, new TableName(catName, dbName, tableName)) {

      @Override
      protected String describeResult() {
        return "Table column privileges.";
      }

      @Override
      protected List<HiveObjectPrivilege> getSqlResult(GetHelper<List<HiveObjectPrivilege>> ctx)
          throws MetaException {
        return getDirectSql().getTableAllColumnGrants(catName, dbName, tblName, authorizer);
      }

      @Override
      protected List<HiveObjectPrivilege> getJdoResult(GetHelper<List<HiveObjectPrivilege>> ctx) {
        return convertTableCols(listTableAllColumnGrants(catName, dbName, tblName, authorizer));
      }
    }.run(false);
  }

  public List<MRoleMap> listMRoleMembers(String roleName) {
    Query query = null;
    List<MRoleMap> mRoleMemeberList = new ArrayList<>();
    query = pm.newQuery(MRoleMap.class, "role.roleName == t1");
    query.declareParameters("java.lang.String t1");
    query.setUnique(false);
    List<MRoleMap> mRoles = (List<MRoleMap>) query.execute(roleName);
    pm.retrieveAll(mRoles);
    mRoleMemeberList.addAll(mRoles);
    return mRoleMemeberList;
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    List<MRoleMap> roleMaps = listMRoleMembers(roleName);
    List<RolePrincipalGrant> rolePrinGrantList = new ArrayList<>();

    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        RolePrincipalGrant rolePrinGrant = new RolePrincipalGrant(
            roleMap.getRole().getRoleName(),
            roleMap.getPrincipalName(),
            PrincipalType.valueOf(roleMap.getPrincipalType()),
            roleMap.getGrantOption(),
            roleMap.getAddTime(),
            roleMap.getGrantor(),
            // no grantor type for public role, hence the null check
            roleMap.getGrantorType() == null ? null
                : PrincipalType.valueOf(roleMap.getGrantorType())
        );
        rolePrinGrantList.add(rolePrinGrant);

      }
    }
    return rolePrinGrantList;
  }

  private List<MGlobalPrivilege> listPrincipalMGlobalGrants(String principalName,
      PrincipalType principalType) {
    return listPrincipalMGlobalGrants(principalName, principalType, null);
  }

  private List<MGlobalPrivilege> listPrincipalMGlobalGrants(String principalName,
      PrincipalType principalType, String authorizer) {
    Query query;
    List<MGlobalPrivilege> userNameDbPriv = new ArrayList<>();
    List<MGlobalPrivilege> mPrivs = null;
    if (principalName != null) {
      if (authorizer != null) {
        query = pm.newQuery(MGlobalPrivilege.class, "principalName == t1 && principalType == t2 "
            + "&& authorizer == t3");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
        mPrivs = (List<MGlobalPrivilege>) query
            .executeWithArray(principalName, principalType.toString(), authorizer);
      } else {
        query = pm.newQuery(MGlobalPrivilege.class, "principalName == t1 && principalType == t2 ");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mPrivs = (List<MGlobalPrivilege>) query
            .executeWithArray(principalName, principalType.toString());
      }
      pm.retrieveAll(mPrivs);
    }
    if (mPrivs != null) {
      userNameDbPriv.addAll(mPrivs);
    }
    return userNameDbPriv;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {
    List<MGlobalPrivilege> mUsers =
        listPrincipalMGlobalGrants(principalName, principalType);
    if (mUsers.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mUsers.size(); i++) {
      MGlobalPrivilege sUsr = mUsers.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.GLOBAL, null, null, null, null);
      HiveObjectPrivilege secUser = new HiveObjectPrivilege(
          objectRef, sUsr.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sUsr.getPrivilege(), sUsr
              .getCreateTime(), sUsr.getGrantor(), PrincipalType
              .valueOf(sUsr.getGrantorType()), sUsr.getGrantOption()),
          sUsr.getAuthorizer());
      result.add(secUser);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    Query query = pm.newQuery(MGlobalPrivilege.class);
    List<MGlobalPrivilege> userNameDbPriv = (List<MGlobalPrivilege>) query.execute();
    pm.retrieveAll(userNameDbPriv);
    return convertGlobal(userNameDbPriv);
  }

  private List<HiveObjectPrivilege> convertGlobal(List<MGlobalPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MGlobalPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.GLOBAL, null, null, null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MDBPrivilege> listPrincipalMDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName) {
    return listPrincipalMDBGrants(principalName, principalType, catName, dbName, null);
  }

  private List<MDBPrivilege> listPrincipalMDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String authorizer) {
    Query query = null;
    List<MDBPrivilege> mSecurityDBList = new ArrayList<>();
    dbName = normalizeIdentifier(dbName);
    List<MDBPrivilege> mPrivs;
    if (authorizer != null) {
      query = pm.newQuery(MDBPrivilege.class,
          "principalName == t1 && principalType == t2 && database.name == t3 && " +
              "database.catalogName == t4 && authorizer == t5");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
              + "java.lang.String t5");
      mPrivs = (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(),
          dbName, catName, authorizer);
    } else {
      query = pm.newQuery(MDBPrivilege.class,
          "principalName == t1 && principalType == t2 && database.name == t3 && database.catalogName == t4");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
      mPrivs = (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(),
          dbName, catName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityDBList.addAll(mPrivs);
    return mSecurityDBList;
  }

  private List<MDCPrivilege> listPrincipalMDCGrants(String principalName,
      PrincipalType principalType, String dcName) {
    return listPrincipalMDCGrants(principalName, principalType, dcName, null);
  }

  private List<MDCPrivilege> listPrincipalMDCGrants(String principalName,
      PrincipalType principalType, String dcName, String authorizer) {
    Query query = null;
    List<MDCPrivilege> mSecurityDCList = new ArrayList<>();
    dcName = normalizeIdentifier(dcName);
    List<MDCPrivilege> mPrivs;
    if (authorizer != null) {
      query = pm.newQuery(MDCPrivilege.class,
          "principalName == t1 && principalType == t2 && dataConnector.name == t3 && " +
              "authorizer == t4");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3, "
              + "java.lang.String t4");
      mPrivs = (List<MDCPrivilege>) query.executeWithArray(principalName, principalType.toString(),
          dcName, authorizer);
    } else {
      query = pm.newQuery(MDCPrivilege.class,
          "principalName == t1 && principalType == t2 && dataConnector.name == t3");
      query.declareParameters(
          "java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mPrivs = (List<MDCPrivilege>) query.executeWithArray(principalName, principalType.toString(), dcName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityDCList.addAll(mPrivs);
    return mSecurityDCList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType,
      String catName, String dbName) {
    List<MDBPrivilege> mDbs = listPrincipalMDBGrants(principalName, principalType, catName, dbName);
    if (mDbs.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mDbs.size(); i++) {
      MDBPrivilege sDB = mDbs.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.DATABASE, dbName, null, null, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sDB.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sDB.getPrivilege(), sDB
              .getCreateTime(), sDB.getGrantor(), PrincipalType
              .valueOf(sDB.getGrantorType()), sDB.getGrantOption()), sDB.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName, PrincipalType principalType) {
    return convertDB(listPrincipalAllDBGrant(principalName, principalType));
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return listDBGrantsAll(catName, dbName, null);
  }

  private List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName, String authorizer) {
    return convertDB(listDatabaseGrants(catName, dbName, authorizer));
  }

  private List<HiveObjectPrivilege> convertDB(List<MDBPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MDBPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());
      String database = priv.getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.DATABASE, database,
          null, null, null);
      objectRef.setCatName(priv.getDatabase().getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MDBPrivilege> listPrincipalAllDBGrant(String principalName, PrincipalType principalType) {
    final List<MDBPrivilege> mSecurityDBList;

    LOG.debug("Executing listPrincipalAllDBGrant");
    Query query;
    if (principalName != null && principalType != null) {
      query = pm.newQuery(MDBPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityDBList = (List<MDBPrivilege>) query.execute(principalName, principalType.toString());
      pm.retrieveAll(mSecurityDBList);
      LOG.debug("Done retrieving all objects for listPrincipalAllDBGrant: {}", mSecurityDBList);
      return Collections.unmodifiableList(new ArrayList<>(mSecurityDBList));
    } else {
      query = pm.newQuery(MDBPrivilege.class);
      mSecurityDBList = (List<MDBPrivilege>) query.execute();
      pm.retrieveAll(mSecurityDBList);
      LOG.debug("Done retrieving all objects for listPrincipalAllDBGrant: {}", mSecurityDBList);
      return Collections.unmodifiableList(new ArrayList<>(mSecurityDBList));
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDCGrants(String principalName,
      PrincipalType principalType,
      String dcName) {
    List<MDCPrivilege> mDcs = listPrincipalMDCGrants(principalName, principalType, dcName);
    if (mDcs.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mDcs.size(); i++) {
      MDCPrivilege sDC = mDcs.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.DATACONNECTOR, null, dcName, null, null);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sDC.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sDC.getPrivilege(), sDC
              .getCreateTime(), sDC.getGrantor(), PrincipalType
              .valueOf(sDC.getGrantorType()), sDC.getGrantOption()), sDC.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDCGrantsAll(String principalName, PrincipalType principalType) {
    return convertDC(listPrincipalAllDCGrant(principalName, principalType));
  }

  @Override
  public List<HiveObjectPrivilege> listDCGrantsAll(String dcName) {
    return listDCGrantsAll(dcName, null);
  }

  private List<HiveObjectPrivilege> listDCGrantsAll(String dcName, String authorizer) {
    return convertDC(listDataConnectorGrants(dcName, authorizer));
  }

  private List<HiveObjectPrivilege> convertDC(List<MDCPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MDCPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());
      String dataConnectorName = priv.getDataConnector().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.DATACONNECTOR, null,
          dataConnectorName, null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MDCPrivilege> listPrincipalAllDCGrant(String principalName, PrincipalType principalType) {
    final List<MDCPrivilege> mSecurityDCList;

    LOG.debug("Executing listPrincipalAllDCGrant");

    if (principalName != null && principalType != null) {
      Query query = pm.newQuery(MDCPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityDCList = (List<MDCPrivilege>) query.execute(principalName, principalType.toString());
      pm.retrieveAll(mSecurityDCList);
      LOG.debug("Done retrieving all objects for listPrincipalAllDCGrant: {}", mSecurityDCList);
      return Collections.unmodifiableList(new ArrayList<>(mSecurityDCList));
    } else {
      Query query = pm.newQuery(MDCPrivilege.class);
      mSecurityDCList = (List<MDCPrivilege>) query.execute();
      pm.retrieveAll(mSecurityDCList);
      LOG.debug("Done retrieving all objects for listPrincipalAllDCGrant: {}", mSecurityDCList);
      return Collections.unmodifiableList(new ArrayList<>(mSecurityDCList));
    }
  }

  private List<MTableColumnPrivilege> listTableAllColumnGrants(
      String catName, String dbName, String tableName, String authorizer) {
    boolean success = false;
    Query query = null;
    List<MTableColumnPrivilege> mTblColPrivilegeList = new ArrayList<>();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MTableColumnPrivilege> mPrivs = null;
    if (authorizer != null) {
      String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
          "table.database.catalogName == t3 && authorizer == t4";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
          "java.lang.String t4");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName, authorizer);
    } else {
      String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
          "table.database.catalogName == t3";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName);
    }
    LOG.debug("Query to obtain objects for listTableAllColumnGrants finished");
    pm.retrieveAll(mPrivs);
    LOG.debug("RetrieveAll on all the objects for listTableAllColumnGrants finished");
    mTblColPrivilegeList.addAll(mPrivs);
    return mTblColPrivilegeList;
  }

  @Override
  public List<MDBPrivilege> listDatabaseGrants(String catName, String dbName, String authorizer) {
    LOG.debug("Executing listDatabaseGrants");

    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    final Query query;
    final String[] args;

    if (authorizer != null) {
      query = pm.newQuery(MDBPrivilege.class, "database.name == t1 && database.catalogName == t2 && authorizer == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      args = new String[] { dbName, catName, authorizer };
    } else {
      query = pm.newQuery(MDBPrivilege.class, "database.name == t1 && database.catalogName == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      args = new String[] { dbName, catName };
    }

    final List<MDBPrivilege> mSecurityDBList = (List<MDBPrivilege>) query.executeWithArray(args);
    pm.retrieveAll(mSecurityDBList);
    LOG.debug("Done retrieving all objects for listDatabaseGrants: {}", mSecurityDBList);
    return Collections.unmodifiableList(new ArrayList<>(mSecurityDBList));
  }

  @Override
  public List<MDCPrivilege> listDataConnectorGrants(String dcName, String authorizer) {
    LOG.debug("Executing listDataConnectorGrants");

    dcName = normalizeIdentifier(dcName);

    final Query query;
    String[] args = null;
    final List<MDCPrivilege> mSecurityDCList;

    if (authorizer != null) {
      query = pm.newQuery(MDCPrivilege.class, "dataConnector.name == t1 && authorizer == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      args = new String[] { dcName, authorizer };
    } else {
      query = pm.newQuery(MDCPrivilege.class, "dataConnector.name == t1");
      query.declareParameters("java.lang.String t1");
    }
    if (args != null) {
      mSecurityDCList = (List<MDCPrivilege>) query.executeWithArray(args);
    } else {
      mSecurityDCList = (List<MDCPrivilege>) query.execute(dcName);
    }
    pm.retrieveAll(mSecurityDCList);
    LOG.debug("Done retrieving all objects for listDataConnectorGrants: {}", mSecurityDCList);
    return Collections.unmodifiableList(new ArrayList<>(mSecurityDCList));
  }

  private List<MTablePrivilege> listAllMTableGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName) {
    return listAllMTableGrants(principalName, principalType, catName, dbName, tableName, null);
  }

  private List<MTablePrivilege> listAllMTableGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String authorizer) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    Query query = null;
    List<MTablePrivilege> mSecurityTabPartList = new ArrayList<>();
    LOG.debug("Executing listAllTableGrants");
    List<MTablePrivilege> mPrivs;
    if (authorizer != null) {
      query = pm.newQuery(MTablePrivilege.class,
          "principalName == t1 && principalType == t2 && table.tableName == t3 &&" +
              "table.database.name == t4 && table.database.catalogName == t5 && authorizer == t6");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3," +
          "java.lang.String t4, java.lang.String t5, java.lang.String t6");
      mPrivs = (List<MTablePrivilege>) query.executeWithArray(principalName, principalType.toString(),
          tableName, dbName, catName, authorizer);
    } else {
      query = pm.newQuery(MTablePrivilege.class,
          "principalName == t1 && principalType == t2 && table.tableName == t3 &&" +
              "table.database.name == t4 && table.database.catalogName == t5");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3," +
          "java.lang.String t4, java.lang.String t5");
      mPrivs = (List<MTablePrivilege>) query.executeWithArray(principalName, principalType.toString(),
          tableName, dbName, catName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityTabPartList.addAll(mPrivs);
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, TableName table) {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    List<MTablePrivilege> mTbls =
        listAllMTableGrants(principalName, principalType, catName, dbName, tableName);
    if (mTbls.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mTbls.size(); i++) {
      MTablePrivilege sTbl = mTbls.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.TABLE, dbName, tableName, null, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sTbl.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sTbl.getPrivilege(), sTbl.getCreateTime(), sTbl
              .getGrantor(), PrincipalType.valueOf(sTbl
              .getGrantorType()), sTbl.getGrantOption()), sTbl.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  private List<MPartitionPrivilege> listPrincipalMPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partName) {
    return listPrincipalMPartitionGrants(principalName, principalType, catName, dbName, tableName, partName, null);
  }

  private List<MPartitionPrivilege> listPrincipalMPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partName, String authorizer) {
    Query query;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<>();
    List<MPartitionPrivilege> mPrivs;
    if (authorizer != null) {
      query = pm.newQuery(MPartitionPrivilege.class,
          "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
              + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5"
              + "&& partition.partitionName == t6 && authorizer == t7");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
          + "java.lang.String t5, java.lang.String t6, java.lang.String t7");
      mPrivs = (List<MPartitionPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, partName, authorizer);
    } else {
      query = pm.newQuery(MPartitionPrivilege.class,
          "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
              + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5"
              + "&& partition.partitionName == t6");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
          + "java.lang.String t5, java.lang.String t6");
      mPrivs = (List<MPartitionPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, partName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityTabPartList.addAll(mPrivs);
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType,
      TableName table,
      List<String> partValues,
      String partName) {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    List<MPartitionPrivilege> mParts = listPrincipalMPartitionGrants(principalName,
        principalType, catName, dbName, tableName, partName);
    if (mParts.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mParts.size(); i++) {
      MPartitionPrivilege sPart = mParts.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.PARTITION, dbName, tableName, partValues, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sPart.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sPart.getPrivilege(), sPart
              .getCreateTime(), sPart.getGrantor(), PrincipalType
              .valueOf(sPart.getGrantorType()), sPart
              .getGrantOption()), sPart.getAuthorizer());

      result.add(secObj);
    }
    return result;
  }

  private List<MTableColumnPrivilege> listPrincipalMTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName) {
    return listPrincipalMTableColumnGrants(principalName, principalType, catName, dbName, tableName,
        columnName, null);
  }

  private List<MTableColumnPrivilege> listPrincipalMTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName, String authorizer) {
    Query query;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    List<MTableColumnPrivilege> mSecurityColList = new ArrayList<>();
    List<MTableColumnPrivilege> mPrivs;
    if (authorizer != null) {
      String queryStr =
          "principalName == t1 && principalType == t2 && "
              + "table.tableName == t3 && table.database.name == t4 &&  " +
              "table.database.catalogName == t5 && columnName == t6 && authorizer == t7";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, columnName, authorizer);
    } else {
      String queryStr =
          "principalName == t1 && principalType == t2 && "
              + "table.tableName == t3 && table.database.name == t4 &&  " +
              "table.database.catalogName == t5 && columnName == t6 ";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5, java.lang.String t6");
      mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, columnName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityColList.addAll(mPrivs);
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType,
      TableName table,
      String columnName) {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    List<MTableColumnPrivilege> mTableCols =
        listPrincipalMTableColumnGrants(principalName, principalType, catName, dbName, tableName, columnName);
    if (mTableCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mTableCols.size(); i++) {
      MTableColumnPrivilege sCol = mTableCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, null, sCol.getColumnName());
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(
          objectRef, sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol
              .getGrantOption()), sCol.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  private List<MPartitionColumnPrivilege> listPrincipalMPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partitionName, String columnName) {
    return listPrincipalMPartitionColumnGrants(principalName, principalType, catName, dbName,
        tableName, partitionName, columnName, null);
  }

  private List<MPartitionColumnPrivilege> listPrincipalMPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partitionName, String columnName, String authorizer) {
    Query query = null;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    catName = normalizeIdentifier(catName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<>();
    List<MPartitionColumnPrivilege> mPrivs;
    if (authorizer != null) {
      query = pm.newQuery(
          MPartitionColumnPrivilege.class,
          "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
              + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5" +
              " && partition.partitionName == t6 && columnName == t7 && authorizer == t8");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7, "
          + "java.lang.String t8");
      mPrivs = (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, partitionName, columnName, authorizer);
    } else {
      query = pm.newQuery(
          MPartitionColumnPrivilege.class,
          "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
              + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5" +
              " && partition.partitionName == t6 && columnName == t7");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7");
      mPrivs = (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
          principalType.toString(), tableName, dbName, catName, partitionName, columnName);
    }
    pm.retrieveAll(mPrivs);
    mSecurityColList.addAll(mPrivs);
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
      PrincipalType principalType,
      TableName table,
      List<String> partValues,
      String partitionName,
      String columnName) {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    List<MPartitionColumnPrivilege> mPartitionCols =
        listPrincipalMPartitionColumnGrants(principalName, principalType, catName, dbName, tableName,
            partitionName, columnName);
    if (mPartitionCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mPartitionCols.size(); i++) {
      MPartitionColumnPrivilege sCol = mPartitionCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, partValues, sCol.getColumnName());
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol.getGrantOption()), sCol.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    Query query = null;
    LOG.debug("Executing listPrincipalPartitionColumnGrantsAll");
    List<MPartitionColumnPrivilege> mSecurityTabPartList;
    if (principalName != null && principalType != null) {
      query =
          pm.newQuery(MPartitionColumnPrivilege.class,
              "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
              principalType.toString());
    } else {
      query = pm.newQuery(MPartitionColumnPrivilege.class);
      mSecurityTabPartList = (List<MPartitionColumnPrivilege>) query.execute();
    }
    LOG.debug("Done executing query for listPrincipalPartitionColumnGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertPartCols(mSecurityTabPartList);
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
     TableName table, String partitionName, String columnName) {
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    LOG.debug("Executing listPartitionColumnGrantsAll");
    Query query =
        pm.newQuery(MPartitionColumnPrivilege.class,
            "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                + "partition.table.database.name == t5 && "
                + "partition.partitionName == t6 && columnName == t7");
    query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5," +
        "java.lang.String t6, java.lang.String t7");
    List<MPartitionColumnPrivilege> mSecurityTabPartList =
        (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName, catName,
            partitionName, columnName);
    LOG.debug("Done executing query for listPartitionColumnGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertPartCols(mSecurityTabPartList);
    return result;
  }

  private List<HiveObjectPrivilege> convertPartCols(List<MPartitionColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MPartitionColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), priv.getColumnName());
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MTablePrivilege> listPrincipalAllTableGrants(String principalName, PrincipalType principalType) {
    LOG.debug("Executing listPrincipalAllTableGrants");
    Query query = pm.newQuery(MTablePrivilege.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    final List<MTablePrivilege> mSecurityTabPartList =
        (List<MTablePrivilege>) query.execute(principalName, principalType.toString());

    pm.retrieveAll(mSecurityTabPartList);

    LOG.debug("Done retrieving all objects for listPrincipalAllTableGrants");

    return Collections.unmodifiableList(new ArrayList<>(mSecurityTabPartList));
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
      PrincipalType principalType) {
    Query query;
    LOG.debug("Executing listPrincipalAllTableGrants");
    List<MTablePrivilege> mSecurityTabPartList;
    if (principalName != null && principalType != null) {
      query = pm.newQuery(MTablePrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList =
          (List<MTablePrivilege>) query.execute(principalName, principalType.toString());
    } else {
      query = pm.newQuery(MTablePrivilege.class);
      mSecurityTabPartList = (List<MTablePrivilege>) query.execute();
    }
    List<HiveObjectPrivilege> result = convertTable(mSecurityTabPartList);
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(TableName table) {
    return listTableGrantsAll(table, null);
  }

  private List<HiveObjectPrivilege> listTableGrantsAll(TableName table,
      String authorizer) {
    Query query;
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    LOG.debug("Executing listTableGrantsAll");
    List<MTablePrivilege> mSecurityTabPartList = null;
    if (authorizer != null) {
      query = pm.newQuery(MTablePrivilege.class,
          "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3" +
              " && authorizer == t4");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
          "java.lang.String t4");
      mSecurityTabPartList = (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName, authorizer);
    } else {
      query = pm.newQuery(MTablePrivilege.class,
          "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mSecurityTabPartList = (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName);
    }
    LOG.debug("Done executing query for listTableGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertTable(mSecurityTabPartList);
    return result;
  }

  private List<HiveObjectPrivilege> convertTable(List<MTablePrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MTablePrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      String table = priv.getTable().getTableName();
      String database = priv.getTable().getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.TABLE, database, table,
          null, null);
      objectRef.setCatName(priv.getTable().getDatabase().getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MPartitionPrivilege> listPrincipalAllPartitionGrants(String principalName, PrincipalType principalType) {
    LOG.debug("Executing listPrincipalAllPartitionGrants");

    Query query = pm.newQuery(MPartitionPrivilege.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    final List<MPartitionPrivilege> mSecurityTabPartList =
        (List<MPartitionPrivilege>) query.execute(principalName, principalType.toString());

    pm.retrieveAll(mSecurityTabPartList);
    LOG.debug("Done retrieving all objects for listPrincipalAllPartitionGrants");

    return Collections.unmodifiableList(new ArrayList<>(mSecurityTabPartList));
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
      PrincipalType principalType) {
    Query query = null;
    LOG.debug("Executing listPrincipalPartitionGrantsAll");
    List<MPartitionPrivilege> mSecurityTabPartList;
    if (principalName != null && principalType != null) {
      query =
          pm.newQuery(MPartitionPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList =
          (List<MPartitionPrivilege>) query.execute(principalName, principalType.toString());
    } else {
      query = pm.newQuery(MPartitionPrivilege.class);
      mSecurityTabPartList = (List<MPartitionPrivilege>) query.execute();
    }
    LOG.debug("Done executing query for listPrincipalPartitionGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertPartition(mSecurityTabPartList);
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(TableName table, String partitionName) {
    String tableName = normalizeIdentifier(table.getTable());
    String dbName = normalizeIdentifier(table.getDb());
    String catName = normalizeIdentifier(table.getCat());
    LOG.debug("Executing listPrincipalPartitionGrantsAll");
    Query query =
        pm.newQuery(MPartitionPrivilege.class,
            "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                + "partition.table.database.catalogName == t5 && partition.partitionName == t6");
    query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5, " +
        "java.lang.String t6");
    List<MPartitionPrivilege> mSecurityTabPartList =
        (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName, catName, partitionName);
    LOG.debug("Done executing query for listPrincipalPartitionGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertPartition(mSecurityTabPartList);
    return result;
  }

  private List<HiveObjectPrivilege> convertPartition(List<MPartitionPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MPartitionPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.PARTITION,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), null);
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MTableColumnPrivilege> listPrincipalAllTableColumnGrants(String principalName,
      PrincipalType principalType) {

    LOG.debug("Executing listPrincipalAllTableColumnGrants");


    Query query = pm.newQuery(MTableColumnPrivilege.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    final List<MTableColumnPrivilege> mSecurityColumnList =
        (List<MTableColumnPrivilege>) query.execute(principalName, principalType.toString());

    pm.retrieveAll(mSecurityColumnList);
    LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");

    return Collections.unmodifiableList(new ArrayList<>(mSecurityColumnList));
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    Query query = null;
    LOG.debug("Executing listPrincipalTableColumnGrantsAll");

    List<MTableColumnPrivilege> mSecurityTabPartList;
    if (principalName != null && principalType != null) {
      query =
          pm.newQuery(MTableColumnPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList =
          (List<MTableColumnPrivilege>) query.execute(principalName, principalType.toString());
    } else {
      query = pm.newQuery(MTableColumnPrivilege.class);
      mSecurityTabPartList = (List<MTableColumnPrivilege>) query.execute();
    }
    LOG.debug("Done executing query for listPrincipalTableColumnGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertTableCols(mSecurityTabPartList);
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(TableName table,
      String columnName) {
    Query query = null;
    String catName = normalizeIdentifier(table.getCat());
    String dbName = normalizeIdentifier(table.getDb());
    String tableName = normalizeIdentifier(table.getTable());
    LOG.debug("Executing listPrincipalTableColumnGrantsAll");
    query =
        pm.newQuery(MTableColumnPrivilege.class,
            "table.tableName == t3 && table.database.name == t4 && " +
                "table.database.catalogName == t5 && columnName == t6");
    query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5, " +
        "java.lang.String t6");
    List<MTableColumnPrivilege> mSecurityTabPartList =
        (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName,
            catName, columnName);
    LOG.debug("Done executing query for listPrincipalTableColumnGrantsAll");
    pm.retrieveAll(mSecurityTabPartList);
    List<HiveObjectPrivilege> result = convertTableCols(mSecurityTabPartList);
    return result;
  }

  private List<HiveObjectPrivilege> convertTableCols(List<MTableColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MTableColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MTable mtable = priv.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), null, priv.getColumnName());
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MPartitionColumnPrivilege> listPrincipalAllPartitionColumnGrants(String principalName,
      PrincipalType principalType) {
    LOG.debug("Executing listPrincipalAllTableColumnGrants");

    Query query = pm.newQuery(MPartitionColumnPrivilege.class, "principalName == t1 && principalType == t2");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    final List<MPartitionColumnPrivilege> mSecurityColumnList =
        (List<MPartitionColumnPrivilege>) query.execute(principalName, principalType.toString());

    pm.retrieveAll(mSecurityColumnList);
    LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");

    return Collections.unmodifiableList(new ArrayList<>(mSecurityColumnList));
  }

  @Override
  public void setBaseStore(RawStore store) {
    super.setBaseStore(store);
    this.conf = baseStore.getConf();
  }
}
