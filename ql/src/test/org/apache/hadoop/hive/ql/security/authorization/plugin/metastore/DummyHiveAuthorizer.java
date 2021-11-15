/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.Arrays;
import java.util.List;

/**
 * Test HiveAuthorizer for invoking checkPrivilege Methods for authorization call
 * Authorizes user sam and rob.
 */
public class DummyHiveAuthorizer extends FallbackHiveAuthorizer {

  static final List<String> allowedUsers = Arrays.asList("sam","rob");

  DummyHiveAuthorizer(HiveConf hiveConf, HiveAuthenticationProvider hiveAuthenticator,
                      HiveAuthzSessionContext ctx) {
    super(hiveConf,hiveAuthenticator, ctx);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                              List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws
          HiveAuthzPluginException, HiveAccessControlException {

    String user         = null;
    String errorMessage = "";
    try {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (Exception e) {
      throw  new HiveAuthzPluginException("Unable to get UserGroupInformation");
    }

    if (!isOperationAllowed(user)) {
      errorMessage = "Operation type " + hiveOpType + " not allowed for user:" + user;
      throw  new HiveAuthzPluginException(errorMessage);
    }
  }

  private boolean isOperationAllowed(String user) {
      return allowedUsers.contains(user);
  }

}
