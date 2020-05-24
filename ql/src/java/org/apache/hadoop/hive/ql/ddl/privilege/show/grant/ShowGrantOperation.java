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

package org.apache.hadoop.hive.ql.ddl.privilege.show.grant;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.privilege.PrivilegeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;

/**
 * Operation process of showing a grant.
 */
public class ShowGrantOperation extends DDLOperation<ShowGrantDesc> {
  public ShowGrantOperation(DDLOperationContext context, ShowGrantDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    HiveAuthorizer authorizer = PrivilegeUtils.getSessionAuthorizer(context.getConf());
    try {
      List<HivePrivilegeInfo> privInfos = authorizer.showPrivileges(
          PrivilegeUtils.getAuthorizationTranslator(authorizer).getHivePrincipal(desc.getPrincipalDesc()),
          PrivilegeUtils.getAuthorizationTranslator(authorizer).getHivePrivilegeObject(desc.getHiveObj()));
      boolean testMode = context.getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
      DDLUtils.writeToFile(writeGrantInfo(privInfos, testMode), desc.getResFile(), context);
    } catch (IOException e) {
      throw new HiveException("Error in show grant statement", e);
    }
    return 0;
  }

  private String writeGrantInfo(List<HivePrivilegeInfo> privileges, boolean testMode) {
    if (CollectionUtils.isEmpty(privileges)) {
      return "";
    }

    //sort the list to get sorted (deterministic) output (for ease of testing)
    Collections.sort(privileges, new Comparator<HivePrivilegeInfo>() {
      @Override
      public int compare(HivePrivilegeInfo o1, HivePrivilegeInfo o2) {
        int compare = o1.getObject().compareTo(o2.getObject());
        if (compare == 0) {
          compare = o1.getPrincipal().compareTo(o2.getPrincipal());
        }
        if (compare == 0) {
          compare = o1.getPrivilege().compareTo(o2.getPrivilege());
        }
        return compare;
      }
    });

    StringBuilder builder = new StringBuilder();
    for (HivePrivilegeInfo privilege : privileges) {
      HivePrincipal principal = privilege.getPrincipal();
      HivePrivilegeObject resource = privilege.getObject();
      HivePrincipal grantor = privilege.getGrantorPrincipal();

      DDLUtils.appendNonNull(builder, resource.getDbname(), true);
      DDLUtils.appendNonNull(builder, resource.getObjectName());
      DDLUtils.appendNonNull(builder, resource.getPartKeys());
      DDLUtils.appendNonNull(builder, resource.getColumns());
      DDLUtils.appendNonNull(builder, principal.getName());
      DDLUtils.appendNonNull(builder, principal.getType());
      DDLUtils.appendNonNull(builder, privilege.getPrivilege().getName());
      DDLUtils.appendNonNull(builder, privilege.isGrantOption());
      DDLUtils.appendNonNull(builder, testMode ? -1 : privilege.getGrantTime() * 1000L);
      DDLUtils.appendNonNull(builder, grantor.getName());
    }
    return builder.toString();
  }
}
