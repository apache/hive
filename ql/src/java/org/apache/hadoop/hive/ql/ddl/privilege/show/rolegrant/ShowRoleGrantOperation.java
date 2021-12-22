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

package org.apache.hadoop.hive.ql.ddl.privilege.show.rolegrant;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;

/**
 * Operation process of showing the role grants.
 */
public class ShowRoleGrantOperation extends DDLOperation<ShowRoleGrantDesc> {
  public ShowRoleGrantOperation(DDLOperationContext context, ShowRoleGrantDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    HiveAuthorizer authorizer = PrivilegeUtils.getSessionAuthorizer(context.getConf());
    boolean testMode = context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
    List<HiveRoleGrant> roles = authorizer.getRoleGrantInfoForPrincipal(
        AuthorizationUtils.getHivePrincipal(desc.getName(), desc.getPrincipalType()));
    ShowUtils.writeToFile(writeRolesGrantedInfo(roles, testMode), desc.getResFile(), context);

    return 0;
  }

  private String writeRolesGrantedInfo(List<HiveRoleGrant> roles, boolean testMode) {
    if (roles == null || roles.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    //sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(roles);
    for (HiveRoleGrant role : roles) {
      ShowUtils.appendNonNull(builder, role.getRoleName(), true);
      ShowUtils.appendNonNull(builder, role.isGrantOption());
      ShowUtils.appendNonNull(builder, testMode ? -1 : role.getGrantTime() * 1000L);
      ShowUtils.appendNonNull(builder, role.getGrantor());
    }
    return builder.toString();
  }
}
