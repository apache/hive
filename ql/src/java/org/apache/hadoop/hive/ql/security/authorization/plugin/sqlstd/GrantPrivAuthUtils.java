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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

/**
 * Utility class to authorize grant/revoke privileges
 */
public class GrantPrivAuthUtils {

  static void authorize(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, boolean grantOption, IMetaStoreClient metastoreClient,
      String userName, List<String> curRoles, boolean isAdmin)
          throws HiveAuthzPluginException, HiveAccessControlException {

    // check if this user has grant privileges for this privileges on this
    // object

    // map priv being granted to required privileges
    RequiredPrivileges reqPrivs = getGrantRequiredPrivileges(hivePrivileges);

    // check if this user has necessary privileges (reqPrivs) on this object
    checkRequiredPrivileges(reqPrivs, hivePrivObject, metastoreClient, userName, curRoles, isAdmin);
  }

  private static void checkRequiredPrivileges(
      RequiredPrivileges reqPrivileges, HivePrivilegeObject hivePrivObject,
      IMetaStoreClient metastoreClient, String userName, List<String> curRoles, boolean isAdmin)
          throws HiveAuthzPluginException, HiveAccessControlException {

    // keep track of the principals on which privileges have been checked for
    // this object

    // get privileges for this user and its roles on this object
    RequiredPrivileges availPrivs = SQLAuthorizationUtils.getPrivilegesFromMetaStore(
        metastoreClient, userName, hivePrivObject, curRoles, isAdmin);

    // check if required privileges is subset of available privileges
    Collection<SQLPrivTypeGrant> missingPrivs = reqPrivileges.findMissingPrivs(availPrivs);
    SQLAuthorizationUtils.assertNoMissingPrivilege(missingPrivs, new HivePrincipal(userName,
        HivePrincipalType.USER), hivePrivObject);
  }

  private static RequiredPrivileges getGrantRequiredPrivileges(List<HivePrivilege> hivePrivileges)
      throws HiveAuthzPluginException {
    RequiredPrivileges reqPrivs = new RequiredPrivileges();
    for (HivePrivilege hivePriv : hivePrivileges) {
      reqPrivs.addPrivilege(hivePriv.getName(), true /* grant priv required */);
    }
    return reqPrivs;
  }

}
