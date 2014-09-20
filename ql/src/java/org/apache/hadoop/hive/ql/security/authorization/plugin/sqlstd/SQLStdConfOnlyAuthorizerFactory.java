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

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

/**
 * Authorization class that can be used from hive cli, so that configuration
 * in cli mode is set appropriately for SQL standards authorization.
 * This ensures that new tables and views have proper privileges for the table/view owner.
 *
 * Uses DummyHiveAuthorizationValidator for no-op authorization checks. Authorization using
 * sql standards based authorization mode can't be done securely with hive-cli, as hive-cli
 * users have direct access to the file system.
 */
@Private
public class SQLStdConfOnlyAuthorizerFactory implements HiveAuthorizerFactory {
  @Override
  public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {

    SQLStdHiveAccessControllerWrapper privilegeManager =
        new SQLStdHiveAccessControllerWrapper(metastoreClientFactory, conf, authenticator, ctx);
    return new HiveAuthorizerImpl(privilegeManager, new DummyHiveAuthorizationValidator());
  }
}
