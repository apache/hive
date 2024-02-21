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

package org.apache.hive.minikdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.hooks.TestHs2Hooks.PostExecHook;
import org.apache.hadoop.hive.hooks.TestHs2Hooks.PreExecHook;
import org.apache.hadoop.hive.hooks.TestHs2Hooks.SemanticAnalysisHook;
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
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;
  private static Map<String, String> confOverlay = new HashMap<String, String>();
  private Connection hs2Conn;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    confOverlay.put(ConfVars.POST_EXEC_HOOKS.varname, PostExecHook.class.getName());
    confOverlay.put(ConfVars.PRE_EXEC_HOOKS.varname, PreExecHook.class.getName());
    confOverlay.put(ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        SemanticAnalysisHook.class.getName());
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "" + Boolean.FALSE);
    confOverlay.put(ConfVars.HIVE_FETCH_TASK_CACHING.varname, "" + false);

    miniHiveKdc = new MiniHiveKdc();
    HiveConf hiveConf = new HiveConf();
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf);
    miniHS2.start(confOverlay);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    miniHS2.stop();
  }

  @Before
  public void setUpTest() throws Exception {
    PreExecHook.userName = null;
    PreExecHook.ipAddress = null;
    PreExecHook.operation = null;
    PreExecHook.error = null;
    PostExecHook.userName = null;
    PostExecHook.ipAddress = null;
    PostExecHook.operation = null;
    PostExecHook.error = null;
    SemanticAnalysisHook.userName = null;
    SemanticAnalysisHook.ipAddress = null;
    SemanticAnalysisHook.command = null;
    SemanticAnalysisHook.preAnalyzeError = null;
    SemanticAnalysisHook.postAnalyzeError = null;
  }

  @After
  public void tearDownTest() throws Exception {
    if (hs2Conn != null) {
      try {
        hs2Conn.close();
      } catch (Exception e) {
        // Ignore shutdown errors since there are negative tests
      }
    }
  }

  /**
   * Test that hook context properties are correctly set.
   */
  @Test
  public void testHookContexts() throws Throwable {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());

    Statement stmt = hs2Conn.createStatement();
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

    Assert.assertNotNull("ipaddress is null", PostExecHook.ipAddress);
    Assert.assertNotNull("userName is null", PostExecHook.userName);
    Assert.assertNotNull("operation is null", PostExecHook.operation);
    Assert.assertEquals(MiniHiveKdc.HIVE_TEST_USER_1, PostExecHook.userName);
    Assert.assertTrue(PostExecHook.ipAddress, PostExecHook.ipAddress.contains("127.0.0.1"));
    Assert.assertEquals("SHOWTABLES", PostExecHook.operation);

    Assert.assertNotNull("ipaddress is null", PreExecHook.ipAddress);
    Assert.assertNotNull("userName is null", PreExecHook.userName);
    Assert.assertNotNull("operation is null", PreExecHook.operation);
    Assert.assertEquals(MiniHiveKdc.HIVE_TEST_USER_1, PreExecHook.userName);
    Assert.assertTrue(PreExecHook.ipAddress, PreExecHook.ipAddress.contains("127.0.0.1"));
    Assert.assertEquals("SHOWTABLES", PreExecHook.operation);

    error = SemanticAnalysisHook.preAnalyzeError;
    if (error != null) {
      throw error;
    }
    error = SemanticAnalysisHook.postAnalyzeError;
    if (error != null) {
      throw error;
    }

    Assert.assertNotNull("semantic hook context ipaddress is null",
        SemanticAnalysisHook.ipAddress);
    Assert.assertNotNull("semantic hook context userName is null",
        SemanticAnalysisHook.userName);
    Assert.assertNotNull("semantic hook context command is null",
        SemanticAnalysisHook.command);
    Assert.assertNotNull("semantic hook context commandType is null",
        SemanticAnalysisHook.commandType);
    Assert.assertTrue(SemanticAnalysisHook.ipAddress,
        SemanticAnalysisHook.ipAddress.contains("127.0.0.1"));
    Assert.assertEquals("show tables", SemanticAnalysisHook.command);
  }
}