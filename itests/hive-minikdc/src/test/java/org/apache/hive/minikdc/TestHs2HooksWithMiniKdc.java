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

package org.apache.hive.minikdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests information retrieved from hooks, in Kerberos mode.
 */
public class TestHs2HooksWithMiniKdc {
  public static class PostExecHook implements ExecuteWithHookContext {
    public static String userName = null;
    public static String ipAddress = null;

    public void run(HookContext hookContext) {
      if (hookContext.getHookType().equals(HookType.POST_EXEC_HOOK)) {
        Assert.assertNotNull(hookContext.getIpAddress(), "IP Address is null");
        ipAddress = hookContext.getIpAddress();
        Assert.assertNotNull(hookContext.getUserName(), "Username is null");
        userName = hookContext.getUserName();
      }
    }
  }

  public static class PreExecHook implements ExecuteWithHookContext {
    public static String userName = null;
    public static String ipAddress = null;

    public void run(HookContext hookContext) {
      if (hookContext.getHookType().equals(HookType.PRE_EXEC_HOOK)) {
        Assert.assertNotNull(hookContext.getIpAddress(), "IP Address is null");
        ipAddress = hookContext.getIpAddress();
        Assert.assertNotNull(hookContext.getUserName(), "Username is null");
        userName = hookContext.getUserName();
      }
    }
  }
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;
  private static Map<String, String> confOverlay = new HashMap<String, String>();
  private Connection hs2Conn;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    confOverlay.put(ConfVars.POSTEXECHOOKS.varname, PostExecHook.class.getName());
    confOverlay.put(ConfVars.PREEXECHOOKS.varname, PreExecHook.class.getName());

    HiveConf hiveConf = new HiveConf();
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf);
    miniHS2.start(confOverlay);
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      try {
        hs2Conn.close();
      } catch (Exception e) {
        // Ignore shutdown errors since there are negative tests
      }
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  /**
   * Test get IpAddress and username from hook.
   * @throws Exception
   */
  @Test
  public void testIpUserName() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());

    Statement stmt = hs2Conn.createStatement();
    stmt.executeQuery("show tables");

    Assert.assertEquals(MiniHiveKdc.HIVE_TEST_USER_1, PostExecHook.userName);
    Assert.assertNotNull(PostExecHook.ipAddress);
    Assert.assertTrue(PostExecHook.ipAddress.contains("127.0.0.1"));

    Assert.assertEquals(MiniHiveKdc.HIVE_TEST_USER_1, PreExecHook.userName);
    Assert.assertNotNull(PreExecHook.ipAddress);
    Assert.assertTrue(PreExecHook.ipAddress.contains("127.0.0.1"));
  }
}
