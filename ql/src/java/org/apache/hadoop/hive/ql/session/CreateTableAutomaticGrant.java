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

  public static CreateTableAutomaticGrant create(HiveConf conf)
      throws HiveException {
    CreateTableAutomaticGrant grants = new CreateTableAutomaticGrant();
    grants.userGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_USER_GRANTS));
    grants.groupGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_GROUP_GRANTS));
    grants.roleGrants = getGrantMap(HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTHORIZATION_TABLE_ROLE_GRANTS));
    
    List<PrivilegeGrantInfo> ownerGrantInfoList = new ArrayList<PrivilegeGrantInfo>();
    String grantor = null;
    if (SessionState.get() != null
        && SessionState.get().getAuthenticator() != null) {
      grantor = SessionState.get().getAuthenticator().getUserName();
      ownerGrantInfoList.add(new PrivilegeGrantInfo(Privilege.ALL.getPriv(), -1, grantor,
          PrincipalType.USER, true));
      if (grants.userGrants == null) {
        grants.userGrants = new HashMap<String, List<PrivilegeGrantInfo>>();
      }
      grants.userGrants.put(grantor, ownerGrantInfoList);
    }
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
        checkPrivilege(privList);
        
        String[] grantArray = privList.split(",");
        List<PrivilegeGrantInfo> grantInfoList = new ArrayList<PrivilegeGrantInfo>();
        String grantor = null;
        if (SessionState.get().getAuthenticator() != null) {
          grantor = SessionState.get().getAuthenticator().getUserName();  
        }
        for (String grant : grantArray) {
          grantInfoList.add(new PrivilegeGrantInfo(grant, -1, grantor,
              PrincipalType.USER, true));
        }
        
        String[] users = userList.split(",");
        for (String user : users) {
          grantsMap.put(user, grantInfoList);
        }
      }
      return grantsMap;
    }
    return null;
  }

  private static void checkPrivilege(String ownerGrantsInConfig)
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
    return userGrants;
  }

  public Map<String, List<PrivilegeGrantInfo>> getGroupGrants() {
    return groupGrants;
  }

  public Map<String, List<PrivilegeGrantInfo>> getRoleGrants() {
    return roleGrants;
  }
}