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
package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeObjectDesc;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationTranslator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;


/**
 * Default implementation of HiveAuthorizationTranslator
 */
public class DefaultHiveAuthorizationTranslator implements HiveAuthorizationTranslator {

  @Override
  public HivePrincipal getHivePrincipal(PrincipalDesc principal) throws HiveException {
    return AuthorizationUtils.getHivePrincipal(principal);
  }

  @Override
  public HivePrivilege getHivePrivilege(PrivilegeDesc privilege) {
    return AuthorizationUtils.getHivePrivilege(privilege);
  }

  @Override
  public HivePrivilegeObject getHivePrivilegeObject(PrivilegeObjectDesc privSubjectDesc)
      throws HiveException {
    return AuthorizationUtils.getHivePrivilegeObject(privSubjectDesc);
  }


}
