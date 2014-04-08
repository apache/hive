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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessController;

/**
 * Extends SQLStdHiveAccessController to relax the restriction of not being able to run dfs
 * and set commands, so that it is easy to test using .q file tests.
 * To be used for testing purposes only!
 */
@Private
public class SQLStdHiveAccessControllerForTest extends SQLStdHiveAccessController {

  SQLStdHiveAccessControllerForTest(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    super(metastoreClientFactory, conf, authenticator);
  }


  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    super.applyAuthorizationConfigPolicy(hiveConf);

    // allow set and dfs commands
    hiveConf.setVar(ConfVars.HIVE_SECURITY_COMMAND_WHITELIST, "set,dfs");

    // remove restrictions on the variables that can be set using set command
    hiveConf.setIsModWhiteListEnabled(false);

  }

}
