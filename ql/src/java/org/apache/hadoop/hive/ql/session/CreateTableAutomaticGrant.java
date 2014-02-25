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

package org.apache.hadoop.hive.ql.session;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeRegistry;

public class CreateTableAutomaticGrant {
  private Map<String, List<PrivilegeGrantInfo>> userGrants;
  private Map<String, List<PrivilegeGrantInfo>> groupGrants;
  private Map<String, List<PrivilegeGrantInfo>> roleGrants;

  // the owner can change, also owner might appear in user grants as well
  // so keep owner privileges separate from userGrants
  private List<PrivilegeGrantInfo> ownerGrant;

  public static CreateTableAutomaticGrant create(HiveConf conf)
      throws HiveException {
    CreateTableAutomaticGrant grants = new CreateTableAutomaticGrant();
    grants.userGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_USER_GRANTS));
    grants.groupGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_GROUP_GRANTS));
    grants.roleGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_ROLE_GRANTS));

    grants.ownerGrant = getGrantorInfoList(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS));

    return grants;
  }

  private static Map<String, List<PrivilegeGrantInfo>> getGrantMap(String grantMapStr)
      throws HiveException {
    if (grantMapStr != null && !grantMapStr.trim().equals("")) {
      String[] grantArrayStr = grantMapStr.split(";");
      Map<String, List<PrivilegeGrantInfo>> grantsMap = new HashMap<String, List<PrivilegeGrantInfo>>();
      for (String grantStr : grantArrayStr) {
        String[] principalListAndPrivList = grantStr.split(":");
        if (principalListAndPrivList.length != 2
            || principalListAndPrivList[0] == null
            || principalListAndPrivList[0].trim().equals("")) {
          throw new HiveException(
              "Can not understand the config privilege definition " + grantStr);
        }
        String userList = principalListAndPrivList[0];
        String privList = principalListAndPrivList[1];
        List<PrivilegeGrantInfo> grantInfoList = getGrantorInfoList(privList);
        if(grantInfoList != null) {
          String[] users = userList.split(",");
          for (String user : users) {
            grantsMap.put(user, grantInfoList);
          }
        }
      }
      return grantsMap;
    }
    return null;
  }

  private static List<PrivilegeGrantInfo> getGrantorInfoList(String privList)
      throws HiveException {
    if (privList == null || privList.trim().equals("")) {
      return null;
    }
    validatePrivilege(privList);
    String[] grantArray = privList.split(",");
    List<PrivilegeGrantInfo> grantInfoList = new ArrayList<PrivilegeGrantInfo>();
    String grantor = SessionState.getUserFromAuthenticator();

    for (String grant : grantArray) {
      grantInfoList.add(new PrivilegeGrantInfo(grant, -1, grantor,
          PrincipalType.USER, true));
    }
    return grantInfoList;
  }

  private static void validatePrivilege(String ownerGrantsInConfig)
      throws HiveException {
    String[] ownerGrantArray = ownerGrantsInConfig.split(",");
    // verify the config
    for (String ownerGrant : ownerGrantArray) {
      Privilege prive = PrivilegeRegistry.getPrivilege(ownerGrant);
      if (prive == null) {
        throw new HiveException("Privilege " + ownerGrant + " is not found.");
      }
    }
  }

  public Map<String, List<PrivilegeGrantInfo>> getUserGrants() {
    Map<String, List<PrivilegeGrantInfo>> curUserGrants = new HashMap<String, List<PrivilegeGrantInfo>>();
    String owner = SessionState.getUserFromAuthenticator();
    if (owner != null && ownerGrant != null) {
      curUserGrants.put(owner, ownerGrant);
    }
    if (userGrants != null) {
      curUserGrants.putAll(userGrants);
    }
    return curUserGrants;
  }

  public Map<String, List<PrivilegeGrantInfo>> getGroupGrants() {
    return groupGrants;
  }

  public Map<String, List<PrivilegeGrantInfo>> getRoleGrants() {
    return roleGrants;
  }
}