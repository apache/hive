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
package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * VerifyHooksRunInOrder.
 *
 * Has to subclasses RunFirst and RunSecond which can be run as either pre or post hooks.
 * Verifies that RunFirst is executed before RunSecond as the same type of hook.  I.e. if they are
 * run as both Pre and Post hooks, RunSecond checks that RunFirst was run as a Pre or Post hook
 * respectively.
 *
 * When running this, be sure to specify RunFirst before RunSecond in the configuration variable.
 */
public class VerifyHooksRunInOrder {

  private static boolean preHookRunFirstRan = false;
  private static boolean postHookRunFirstRan = false;
  private static boolean staticAnalysisPreHookFirstRan = false;
  private static boolean staticAnalysisPostHookFirstRan = false;
  private static boolean driverRunPreHookFirstRan = false;
  private static boolean driverRunPostHookFirstRan = false;

  public static class RunFirst implements ExecuteWithHookContext {
    public void run(HookContext hookContext) {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunFirst for " + hookContext.getHookType());

      if (hookContext.getHookType() == HookType.PRE_EXEC_HOOK) {
        preHookRunFirstRan = true;
      } else {
        postHookRunFirstRan = true;
      }
    }
  }

  public static class RunSecond implements ExecuteWithHookContext {
    public void run(HookContext hookContext) throws Exception {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunSecond for " + hookContext.getHookType());

      if (hookContext.getHookType() == HookType.PRE_EXEC_HOOK) {
        Assert.assertTrue("Pre hooks did not run in the order specified.", preHookRunFirstRan);
      } else {
        Assert.assertTrue("Post hooks did not run in the order specified.", postHookRunFirstRan);
      }
    }
  }

  public static class RunFirstSemanticAnalysisHook extends AbstractSemanticAnalyzerHook {
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,ASTNode ast)
        throws SemanticException {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return ast;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunFirst for Pre Analysis Hook");

      staticAnalysisPreHookFirstRan = true;

      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<? extends Serializable>> rootTasks) throws SemanticException {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunFirst for Post Analysis Hook");

      staticAnalysisPostHookFirstRan = true;
    }
  }

  public static class RunSecondSemanticAnalysisHook extends AbstractSemanticAnalyzerHook {
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,ASTNode ast)
        throws SemanticException {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return ast;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunSecond for Pre Analysis Hook");

      Assert.assertTrue("Pre Analysis Hooks did not run in the order specified.",
                        staticAnalysisPreHookFirstRan);

      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<? extends Serializable>> rootTasks) throws SemanticException {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunSecond for Post Analysis Hook");

      Assert.assertTrue("Post Analysis Hooks did not run in the order specified.",
                        staticAnalysisPostHookFirstRan);
    }
  }

  public static class RunFirstDriverRunHook implements HiveDriverRunHook {

    @Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunFirst for Pre Driver Run Hook");

      driverRunPreHookFirstRan = true;
    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunFirst for Post Driver Run Hook");

      driverRunPostHookFirstRan = true;
    }

  }

  public static class RunSecondDriverRunHook implements HiveDriverRunHook {

    @Override
    public void preDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunSecond for Pre Driver Run Hook");

      Assert.assertTrue("Driver Run Hooks did not run in the order specified.",
          driverRunPreHookFirstRan);
    }

    @Override
    public void postDriverRun(HiveDriverRunHookContext hookContext) throws Exception {
      LogHelper console = SessionState.getConsole();

      if (console == null) {
        return;
      }

      // This is simply to verify that the hooks were in fact run
      console.printError("Running RunSecond for Post Driver Run Hook");

      Assert.assertTrue("Driver Run Hooks did not run in the order specified.",
          driverRunPostHookFirstRan);
    }

  }
}
