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

package org.apache.hadoop.hive.ql.ddl.privilege.show.principals;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;

/**
 * Operation process of showing the principals.
 */
public class ShowPrincipalsOperation extends DDLOperation<ShowPrincipalsDesc> {
  public ShowPrincipalsOperation(DDLOperationContext context, ShowPrincipalsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException, IOException {
    HiveAuthorizer authorizer = PrivilegeUtils.getSessionAuthorizer(context.getConf());
    boolean testMode = context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
    List<HiveRoleGrant> roleGrants = authorizer.getPrincipalGrantInfoForRole(desc.getName());
    ShowUtils.writeToFile(writeHiveRoleGrantInfo(roleGrants, testMode), desc.getResFile(), context);

    return 0;
  }

  private String writeHiveRoleGrantInfo(List<HiveRoleGrant> roleGrants, boolean testMode) {
    if (roleGrants == null || roleGrants.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    // sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(roleGrants);
    for (HiveRoleGrant roleGrant : roleGrants) {
      // schema: principal_name,principal_type,grant_option,grantor,grantor_type,grant_time
      ShowUtils.appendNonNull(builder, roleGrant.getPrincipalName(), true);
      ShowUtils.appendNonNull(builder, roleGrant.getPrincipalType());
      ShowUtils.appendNonNull(builder, roleGrant.isGrantOption());
      ShowUtils.appendNonNull(builder, roleGrant.getGrantor());
      ShowUtils.appendNonNull(builder, roleGrant.getGrantorType());
      ShowUtils.appendNonNull(builder, testMode ? -1 : roleGrant.getGrantTime() * 1000L);
    }
    return builder.toString();
  }
}
