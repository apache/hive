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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationValidator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal.HivePrincipalType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.Operation2Privilege.IOType;

public class SQLStdHiveAuthorizationValidator implements HiveAuthorizationValidator {

  private final HiveMetastoreClientFactory metastoreClientFactory;
  private final HiveConf conf;
  private final HiveAuthenticationProvider authenticator;
  private final SQLStdHiveAccessController privController;
  public static final Log LOG = LogFactory.getLog(SQLStdHiveAuthorizationValidator.class);

  public SQLStdHiveAuthorizationValidator(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator,
      SQLStdHiveAccessController privController) {

    this.metastoreClientFactory = metastoreClientFactory;
    this.conf = conf;
    this.authenticator = authenticator;
    this.privController = privController;
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs) throws HiveAuthzPluginException,
      HiveAccessControlException {

    if (LOG.isDebugEnabled()) {
      String msg = "Checking privileges for operation " + hiveOpType + " by user "
          + authenticator.getUserName() + " on " + " input objects " + inputHObjs
          + " and output objects " + outputHObjs;
      LOG.debug(msg);
    }

    String userName = authenticator.getUserName();
    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();

    // check privileges on input and output objects
    checkPrivileges(hiveOpType, inputHObjs, metastoreClient, userName, IOType.INPUT);
    checkPrivileges(hiveOpType, outputHObjs, metastoreClient, userName, IOType.OUTPUT);

  }

  private void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> hiveObjects,
      IMetaStoreClient metastoreClient, String userName, IOType ioType)
      throws HiveAuthzPluginException, HiveAccessControlException {

    // Compare required privileges and available privileges for each hive object
    for (HivePrivilegeObject hiveObj : hiveObjects) {

      RequiredPrivileges requiredPrivs = Operation2Privilege.getRequiredPrivs(hiveOpType, hiveObj,
          ioType);

      // find available privileges
      RequiredPrivileges availPrivs;
      if (hiveObj.getType() == HivePrivilegeObjectType.LOCAL_URI
          || hiveObj.getType() == HivePrivilegeObjectType.DFS_URI) {
        availPrivs = SQLAuthorizationUtils.getPrivilegesFromFS(new Path(hiveObj.getTableViewURI()),
            conf, userName);
      } else if (hiveObj.getType() == HivePrivilegeObjectType.PARTITION) {
        // sql std authorization is managing privileges at the table/view levels
        // only
        // ignore partitions
        continue;
      } else {
        // get the privileges that this user has on the object
        availPrivs = SQLAuthorizationUtils.getPrivilegesFromMetaStore(metastoreClient, userName,
            hiveObj, privController.getCurrentRoleNames(), privController.isUserAdmin());
      }

      // Verify that there are no missing privileges
      Collection<SQLPrivTypeGrant> missingPriv = requiredPrivs.findMissingPrivs(availPrivs);
      SQLAuthorizationUtils.assertNoMissingPrivilege(missingPriv, new HivePrincipal(userName,
          HivePrincipalType.USER), hiveObj);

    }
  }

}
