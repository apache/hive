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

//The tests here are heavily based on some timing, so there is some chance to fail.
package org.apache.hadoop.hive.hooks;

import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests information retrieved from hooks.
 */
public class TestHs2Hooks {

  private static HiveServer2 hiveServer2;

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

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS,
        PreExecHook.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS,
        PostExecHook.class.getName());

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(2000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  /**
   * Test get IpAddress and username from hook.
   * @throws Exception
   */
  @Test
  public void testIpUserName() throws Exception {
    Properties connProp = new Properties();
    connProp.setProperty("user", System.getProperty("user.name"));
    connProp.setProperty("password", "");
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", connProp);

    connection.createStatement().execute("show tables");

    Assert.assertEquals(System.getProperty("user.name"), PostExecHook.userName);
    Assert.assertNotNull(PostExecHook.ipAddress);
    Assert.assertTrue(PostExecHook.ipAddress.contains("127.0.0.1"));

    Assert.assertEquals(System.getProperty("user.name"), PreExecHook.userName);
    Assert.assertNotNull(PreExecHook.ipAddress);
    Assert.assertTrue(PreExecHook.ipAddress.contains("127.0.0.1"));

    connection.close();
  }
}

