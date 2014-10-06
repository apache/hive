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
import java.sql.Statement;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests information retrieved from hooks.
 */
public class TestHs2Hooks {
  private static final Logger LOG = LoggerFactory.getLogger(TestHs2Hooks.class);
  private static HiveServer2 hiveServer2;

  public static class PostExecHook implements ExecuteWithHookContext {
    private static String userName;
    private static String ipAddress;
    private static String operation;
    private static Throwable error;

    public void run(HookContext hookContext) {
      try {
        if (hookContext.getHookType().equals(HookType.POST_EXEC_HOOK)) {
          ipAddress = hookContext.getIpAddress();
          userName = hookContext.getUserName();
          operation = hookContext.getOperationName();
        }
      } catch (Throwable t) {
        LOG.error("Error in PostExecHook: " + t, t);
        error = t;
      }
    }
  }

  public static class PreExecHook implements ExecuteWithHookContext {
    private static String userName;
    private static String ipAddress;
    private static String operation;
    private static Throwable error;

    public void run(HookContext hookContext) {
      try {
        if (hookContext.getHookType().equals(HookType.PRE_EXEC_HOOK)) {
          ipAddress = hookContext.getIpAddress();
          userName = hookContext.getUserName();
          operation = hookContext.getOperationName();
        }
      } catch (Throwable t) {
        LOG.error("Error in PreExecHook: " + t, t);
        error = t;
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
   */
  @Test
  public void testIpUserName() throws Throwable {
    Properties connProp = new Properties();
    connProp.setProperty("user", System.getProperty("user.name"));
    connProp.setProperty("password", "");
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", connProp);

    Statement stmt = connection.createStatement();
    stmt.executeQuery("show databases");
    stmt.executeQuery("show tables");
    Throwable error = PostExecHook.error;
    if (error != null) {
      throw error;
    }
    error = PreExecHook.error;
    if (error != null) {
      throw error;
    }

    Assert.assertEquals(System.getProperty("user.name"), PostExecHook.userName);
    Assert.assertNotNull(PostExecHook.ipAddress, "ipaddress is null");
    Assert.assertNotNull(PostExecHook.userName, "userName is null");
    Assert.assertNotNull(PostExecHook.operation , "operation is null");
    Assert.assertTrue(PostExecHook.ipAddress, PostExecHook.ipAddress.contains("127.0.0.1"));
    Assert.assertEquals("SHOWTABLES", PostExecHook.operation);

    Assert.assertEquals(System.getProperty("user.name"), PreExecHook.userName);
    Assert.assertNotNull(PreExecHook.ipAddress, "ipaddress is null");
    Assert.assertNotNull(PreExecHook.userName, "userName is null");
    Assert.assertNotNull(PreExecHook.operation , "operation is null");
    Assert.assertTrue(PreExecHook.ipAddress, PreExecHook.ipAddress.contains("127.0.0.1"));
    Assert.assertEquals("SHOWTABLES", PreExecHook.operation);
  }
}

