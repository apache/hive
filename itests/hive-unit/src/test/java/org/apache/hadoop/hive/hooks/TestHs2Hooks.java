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

//The tests here are heavily based on some timing, so there is some chance to fail.
package org.apache.hadoop.hive.hooks;

import java.io.Serializable;
import java.lang.Override;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.server.HiveServer2;
import org.junit.AfterClass;
import org.junit.Before;
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
    public static String userName;
    public static String ipAddress;
    public static String operation;
    public static Throwable error;

    @Override
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
    public static String userName;
    public static String ipAddress;
    public static String operation;
    public static Throwable error;

    @Override
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

  public static class SemanticAnalysisHook implements HiveSemanticAnalyzerHook {
    public static String userName;
    public static String command;
    public static HiveOperation commandType;
    public static String ipAddress;
    public static Throwable preAnalyzeError;
    public static Throwable postAnalyzeError;

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
        ASTNode ast) throws SemanticException {
      try {
        userName = context.getUserName();
        ipAddress = context.getIpAddress();
        command = context.getCommand();
        commandType = context.getHiveOperation();
      } catch (Throwable t) {
        LOG.error("Error in semantic analysis hook preAnalyze: " + t, t);
        preAnalyzeError = t;
      }
      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<? extends Serializable>> rootTasks) throws SemanticException {
      try {
        userName = context.getUserName();
        ipAddress = context.getIpAddress();
        command = context.getCommand();
        commandType = context.getHiveOperation();
      } catch (Throwable t) {
        LOG.error("Error in semantic analysis hook postAnalyze: " + t, t);
        postAnalyzeError = t;
      }
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.PREEXECHOOKS,
        PreExecHook.class.getName());
    hiveConf.setVar(ConfVars.POSTEXECHOOKS,
        PostExecHook.class.getName());
    hiveConf.setVar(ConfVars.SEMANTIC_ANALYZER_HOOK,
        SemanticAnalysisHook.class.getName());
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setBoolVar(ConfVars.HIVESTATSCOLAUTOGATHER, false);

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
    SemanticAnalysisHook.commandType = null;
    SemanticAnalysisHook.preAnalyzeError = null;
    SemanticAnalysisHook.postAnalyzeError = null;
  }

  /**
   * Test that hook context properties are correctly set.
   */
  @Test
  public void testHookContexts() throws Throwable {
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
    Assert.assertNotNull("ipaddress is null", PreExecHook.ipAddress);
    Assert.assertNotNull("userName is null", PreExecHook.userName);
    Assert.assertNotNull("operation is null", PreExecHook.operation);
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

    stmt.close();
    connection.close();
  }

  @Test
  public void testPostAnalysisHookContexts() throws Throwable {
    Properties connProp = new Properties();
    connProp.setProperty("user", System.getProperty("user.name"));
    connProp.setProperty("password", "");

    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", connProp);
    Statement stmt = connection.createStatement();
    stmt.execute("create table testPostAnalysisHookContexts as select '3'");
    Throwable error = PostExecHook.error;
    if (error != null) {
      throw error;
    }
    error = PreExecHook.error;
    if (error != null) {
      throw error;
    }

    Assert.assertEquals(HiveOperation.CREATETABLE_AS_SELECT, SemanticAnalysisHook.commandType);

    error = SemanticAnalysisHook.preAnalyzeError;
    if (error != null) {
      throw error;
    }
    error = SemanticAnalysisHook.postAnalyzeError;
    if (error != null) {
      throw error;
    }
    stmt.close();
    connection.close();
  }
}
