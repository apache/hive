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

package org.apache.hadoop.hive.ql.ddl.privilege.role.revoke;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeUtils;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;

/**
 * Operation process of revoking a role.
 */
public class RevokeRoleOperation extends DDLOperation<RevokeRoleDesc> {
  public RevokeRoleOperation(DDLOperationContext context, RevokeRoleDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    HiveAuthorizer authorizer = PrivilegeUtils.getSessionAuthorizer(context.getConf());

    List<HivePrincipal> principals = AuthorizationUtils.getHivePrincipals(desc.getPrincipals(),
        PrivilegeUtils.getAuthorizationTranslator(authorizer));
    HivePrincipal grantorPrincipal = null;
    if (desc.getGrantor() != null) {
      grantorPrincipal =
          new HivePrincipal(desc.getGrantor(), AuthorizationUtils.getHivePrincipalType(PrincipalType.USER));
    }

    authorizer.revokeRole(principals, desc.getRoles(), desc.isGrantOption(), grantorPrincipal);

    return 0;
  }
}
