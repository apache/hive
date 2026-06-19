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

package org.apache.hive.jdbc.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.io.SessionStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
/**
 * Test context information that gets passed to authorization factory
 */
public class TestCLIAuthzSessionContext {
  private static HiveAuthzSessionContext sessionCtx;
  private static CliDriver driver;

  /**
   * This factory captures the HiveAuthzSessionContext argument and returns mocked
   * HiveAuthorizer class
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestCLIAuthzSessionContext.sessionCtx = ctx;
      HiveAuthorizer mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return mockedAuthorizer;
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = new HiveConfForTest(TestCLIAuthzSessionContext.class);
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    // once SessionState for thread is set, CliDriver picks conf from it
    CliSessionState ss = new CliSessionState(conf);
    ss.err = new SessionStream(System.err);
    ss.out = new SessionStream(System.out);
    SessionState.start(ss);
    TestCLIAuthzSessionContext.driver = new CliDriver();
 }

  @AfterClass
  public static void afterTest() throws Exception {
  }

  @Test
  public void testAuthzSessionContextContents() throws Exception {
    driver.processCmd("show tables");
    // session string is supposed to be unique, so its got to be of some reasonable size
    assertTrue("session string size check", sessionCtx.getSessionString().length() > 10);
    assertEquals("Client type ", HiveAuthzSessionContext.CLIENT_TYPE.HIVECLI, sessionCtx.getClientType());
  }

}
