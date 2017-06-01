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

package org.apache.hive.jdbc.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
/**
 * Test context information that gets passed to authorization api
 */
public class TestHS2AuthzContext {
  private static MiniHS2 miniHS2 = null;
  static HiveAuthorizer mockedAuthorizer;
  static HiveAuthenticationProvider authenticator;
  static final String TABLE1_NAME = "TestHS2AuthzContextTab";

  /**
   * This factory creates a mocked HiveAuthorizer class.
   * Use the mocked class to capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestHS2AuthzContext.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      TestHS2AuthzContext.authenticator = authenticator;
      return TestHS2AuthzContext.mockedAuthorizer;
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
    // create tables as user1
    Connection hs2Conn = getConnection("user1");
    Statement stmt = hs2Conn.createStatement();
    stmt.execute("create table " + TABLE1_NAME + "(i int) ");
    stmt.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testAuthzContextContentsDriverCmd() throws Exception {
    String cmd = "show tables";
    verifyContextContents(cmd, cmd);
  }

  @Test
  public void testAuthzContextContentsCmdProcessorCmd() throws Exception {
    verifyContextContents("dfs -ls /", "-ls /");
  }

  @Test
  public void testGrantContextContents() throws Exception {
    String cmd = "grant all on table " + TABLE1_NAME + " to user user2"; 
    verifyContextContents(cmd, cmd);
  }


  @Test
  public void testRevokeContextContents() throws Exception {
    String cmd = "revoke all on table " + TABLE1_NAME + " from user user2"; 
    verifyContextContents(cmd, cmd);
  }

  @Test
  public void testRolesMgmtContextContents() throws Exception {
    verifyContextContents("create role newrole");
    verifyContextContents("grant role newrole to user user2 with admin option");
    verifyContextContents("show role grant user user2");
    verifyContextContents("show principals newrole");
    verifyContextContents("revoke role newrole from user user2");
    verifyContextContents("show roles");
    verifyContextContents("drop role newrole");
  }

  private void verifyContextContents(String cmd) throws HiveAuthzPluginException, HiveAccessControlException, Exception {
    verifyContextContents(cmd, cmd);
  }

  private void verifyContextContents(final String cmd, String ctxCmd) throws Exception,
      HiveAuthzPluginException, HiveAccessControlException {
    Connection hs2Conn = getConnection("user1");
    Statement stmt = hs2Conn.createStatement();

    stmt.execute(cmd);
    stmt.close();
    hs2Conn.close();

    ArgumentCaptor<HiveAuthzContext> contextCapturer = ArgumentCaptor
        .forClass(HiveAuthzContext.class);

    verify(mockedAuthorizer).checkPrivileges(any(HiveOperationType.class),
        Matchers.anyListOf(HivePrivilegeObject.class),
        Matchers.anyListOf(HivePrivilegeObject.class), contextCapturer.capture());

    HiveAuthzContext context = contextCapturer.getValue();

    assertEquals("Command ", ctxCmd, context.getCommandString());
    assertTrue("ip address pattern check", context.getIpAddress().matches("[.:a-fA-F0-9]+"));
    // ip address size check - check for something better than non zero
    assertTrue("ip address size check", context.getIpAddress().length() > 7);

  }

  private static Connection getConnection(String userName) throws Exception {
    return DriverManager.getConnection(miniHS2.getJdbcURL(), userName, "bar");
  }

}
