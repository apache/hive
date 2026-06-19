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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.Builder;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.junit.Test;

/**
 * Test SQLStdHiveAccessController
 */
public class TestSQLStdHiveAccessControllerCLI {

  /**
   * Test that SQLStdHiveAccessController is not applying config restrictions on CLI
   *
   * @throws HiveAuthzPluginException
   */
  @Test
  public void testConfigProcessing() throws HiveAuthzPluginException {
    HiveConf processedConf = new HiveConf();
    SQLStdHiveAccessController accessController = new SQLStdHiveAccessController(null,
        processedConf, new HadoopDefaultAuthenticator(), getCLISessionCtx()
        );
    accessController.applyAuthorizationConfigPolicy(processedConf);

    // check that hook to disable transforms has not been added
    assertFalse("Check for transform query disabling hook",
        processedConf.getVar(ConfVars.PRE_EXEC_HOOKS).contains(DisallowTransformHook.class.getName()));

    // verify that some dummy param can be set
    processedConf.verifyAndSet("dummy.param", "dummy.val");
    processedConf.verifyAndSet(ConfVars.HIVE_AUTHORIZATION_ENABLED.varname, "true");

  }

  private HiveAuthzSessionContext getCLISessionCtx() {
    Builder ctxBuilder = new HiveAuthzSessionContext.Builder();
    ctxBuilder.setClientType(CLIENT_TYPE.HIVECLI);
    return ctxBuilder.build();
  }

  /**
   * Verify that no exception is thrown if authorization is enabled from hive cli,
   * when sql std auth is used
   */
  @Test
  public void testAuthEnable() throws Exception {
    HiveConf processedConf = new HiveConf();
    processedConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    HiveAuthorizerFactory authorizerFactory = new SQLStdHiveAuthorizerFactory();
    HiveAuthorizer authorizer = authorizerFactory.createHiveAuthorizer(null, processedConf,
        new HadoopDefaultAuthenticator(), getCLISessionCtx());
  }

}
