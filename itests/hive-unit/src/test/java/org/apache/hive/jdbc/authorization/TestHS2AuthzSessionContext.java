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

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
/**
 * Test context information that gets passed to authorization factory
 */
public class TestHS2AuthzSessionContext {
  private static MiniHS2 miniHS2 = null;
  private static HiveAuthzSessionContext sessionCtx;

  /**
   * This factory captures the HiveAuthzSessionContext argument and returns mocked
   * HiveAuthorizer class
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestHS2AuthzSessionContext.sessionCtx = ctx;
      HiveAuthorizer mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return mockedAuthorizer;
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

    miniHS2 = new MiniHS2(conf);
    miniHS2.start(new HashMap<String, String>());
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testAuthzSessionContextContents() throws Exception {
    // session string is supposed to be unique, so its got to be of some reasonable size
    assertTrue("session string size check", sessionCtx.getSessionString().length() > 10);
    assertEquals("Client type ", HiveAuthzSessionContext.CLIENT_TYPE.HIVESERVER2, sessionCtx.getClientType());
  }

}
