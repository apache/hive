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

package org.apache.hadoop.hive.ql;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HiveHooks;
import org.apache.hadoop.hive.ql.hooks.MetricsQueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContextImpl;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookWithParseHooks;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;

import static org.apache.hadoop.hive.ql.hooks.HookContext.HookType.*;

/**
 * Handles hook executions for {@link Driver}.
 */
public class HookRunner {

  private static final String CLASS_NAME = Driver.class.getName();
  private final HiveConf conf;
  private final HiveHooks hooks;

  /**
   * Constructs a {@link HookRunner} that loads all hooks to be run via a {@link HiveHooks}.
   */
  HookRunner(HiveConf conf, SessionState.LogHelper console) {
    this.conf = conf;
    this.hooks = new HiveHooks(conf, console);
    addLifeTimeHook(new HiveQueryLifeTimeHook());
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED)) {
      addLifeTimeHook(new MetricsQueryLifeTimeHook());
    }
  }

  /**
   * If {@link QueryLifeTimeHookWithParseHooks} have been loaded via the {@link HiveHooks} then invoke the
   * {@link QueryLifeTimeHookWithParseHooks#beforeParse(QueryLifeTimeHookContext)} method for each
   * {@link QueryLifeTimeHookWithParseHooks}.
   *
   * @param command the Hive command that is being run
   */
  void runBeforeParseHook(String command) {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc =
          new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        if (hook instanceof QueryLifeTimeHookWithParseHooks) {
          ((QueryLifeTimeHookWithParseHooks) hook).beforeParse(qhc);
        }
      }
    }
  }

  /**
   * If {@link QueryLifeTimeHookWithParseHooks} have been loaded via the {@link HiveHooks} then invoke the
   * {@link QueryLifeTimeHookWithParseHooks#afterParse(QueryLifeTimeHookContext, boolean)} method for each
   * {@link QueryLifeTimeHookWithParseHooks}.
   *
   * @param command the Hive command that is being run
   * @param parseError true if there was an error while parsing the command, false otherwise
   */
  void runAfterParseHook(String command, boolean parseError) {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc =
          new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        if (hook instanceof QueryLifeTimeHookWithParseHooks) {
          ((QueryLifeTimeHookWithParseHooks) hook).afterParse(qhc, parseError);
        }
      }
    }
  }

  /**
   * Dispatches {@link QueryLifeTimeHook#beforeCompile(QueryLifeTimeHookContext)}.
   *
   * @param command the Hive command that is being run
   */
  void runBeforeCompileHook(String command) {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc =
          new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.beforeCompile(qhc);
      }
    }
  }

  /**
   * Dispatches {@link QueryLifeTimeHook#afterCompile(QueryLifeTimeHookContext, boolean)}.
   *
   * @param driverContext the DriverContext used for generating the HookContext
   * @param analyzerContext the SemanticAnalyzer context for this query
   * @param compileException the exception if one was thrown during the compilation
   * @throws Exception during {@link PrivateHookContext} creation
   */
  void runAfterCompilationHook(DriverContext driverContext, Context analyzerContext, Throwable compileException)
      throws Exception {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      HookContext hookContext = new PrivateHookContext(driverContext, analyzerContext);
      hookContext.setException(compileException);

      QueryLifeTimeHookContext qhc =
          new QueryLifeTimeHookContextImpl.Builder()
              .withHiveConf(conf)
              .withCommand(analyzerContext.getCmd())
              .withHookContext(hookContext)
              .build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterCompile(qhc, compileException != null);
      }
    }
  }

  /**
   * Dispatches {@link QueryLifeTimeHook#beforeExecution(QueryLifeTimeHookContext)}.
   *
   * @param command the Hive command that is being run
   * @param hookContext the {@link HookContext} of the command being run
   */
  void runBeforeExecutionHook(String command, HookContext hookContext) {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command)
          .withHookContext(hookContext).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.beforeExecution(qhc);
      }
    }
  }

  /**
   * Dispatches {@link QueryLifeTimeHook#afterExecution(QueryLifeTimeHookContext, boolean)}.
   *
   * @param command the Hive command that is being run
   * @param hookContext the {@link HookContext} of the command being run
   * @param executionError true if there was an error while executing the command, false otherwise
   */
  void runAfterExecutionHook(String command, HookContext hookContext, boolean executionError) {
    List<QueryLifeTimeHook> queryHooks = hooks.getHooks(QUERY_LIFETIME_HOOKS);
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command)
          .withHookContext(hookContext).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterExecution(qhc, executionError);
      }
    }
  }

  public ASTNode runPreAnalyzeHooks(HiveSemanticAnalyzerHookContext hookCtx, ASTNode tree) throws HiveException {
    try {
      for (HiveSemanticAnalyzerHook hook :
          hooks.getHooks(SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class)) {
        tree = hook.preAnalyze(hookCtx, tree);
      }
      return tree;
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PreAnalyzeHooks:" + HiveStringUtils.stringifyException(e), e);
    }
  }

  public boolean hasPreAnalyzeHooks() {
    return !hooks.getHooks(SEMANTIC_ANALYZER_HOOK).isEmpty();
  }

  public void runPostAnalyzeHooks(HiveSemanticAnalyzerHookContext hookCtx,
      List<Task<?>> allRootTasks) throws HiveException {
    try {
      for (HiveSemanticAnalyzerHook hook :
          hooks.getHooks(SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class)) {
        hook.postAnalyze(hookCtx, allRootTasks);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PostAnalyzeHooks:" + HiveStringUtils.stringifyException(e), e);
    }

  }

  public void runPreDriverHooks(HiveDriverRunHookContext hookContext) throws HiveException {
    try {
      for (HiveDriverRunHook driverRunHook : hooks.getHooks(DRIVER_RUN_HOOKS, HiveDriverRunHook.class)) {
        driverRunHook.preDriverRun(hookContext);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PreDriverHooks:" + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void runPostDriverHooks(HiveDriverRunHookContext hookContext) throws HiveException {
    try {
      for (HiveDriverRunHook driverRunHook : hooks.getHooks(DRIVER_RUN_HOOKS, HiveDriverRunHook.class)) {
        driverRunHook.postDriverRun(hookContext);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PostDriverHooks:" + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void runPreHooks(HookContext hookContext) throws HiveException {
    invokeGeneralHook(hooks.getHooks(PRE_EXEC_HOOK), PerfLogger.PRE_HOOK, hookContext);
  }

  public void runPostExecHooks(HookContext hookContext) throws HiveException {
    invokeGeneralHook(hooks.getHooks(POST_EXEC_HOOK), PerfLogger.POST_HOOK, hookContext);
  }

  public void runFailureHooks(HookContext hookContext) throws HiveException {
    invokeGeneralHook(hooks.getHooks(ON_FAILURE_HOOK), PerfLogger.FAILURE_HOOK, hookContext);
  }

  private static void invokeGeneralHook(List<ExecuteWithHookContext> hooks, String prefix, HookContext hookContext)
      throws HiveException {
    if (hooks.isEmpty()) {
      return;
    }
    try {
      PerfLogger perfLogger = SessionState.getPerfLogger();

      for (ExecuteWithHookContext hook : hooks) {
        perfLogger.perfLogBegin(CLASS_NAME, prefix + hook.getClass().getName());
        hook.run(hookContext);
        perfLogger.perfLogEnd(CLASS_NAME, prefix + hook.getClass().getName());
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking " + prefix + " hooks: " + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void addLifeTimeHook(QueryLifeTimeHook hook) {
    hooks.addHook(QUERY_LIFETIME_HOOKS, hook);
  }

  public void addPreHook(ExecuteWithHookContext hook) {
    hooks.addHook(PRE_EXEC_HOOK, hook);
  }

  public void addPostHook(ExecuteWithHookContext hook) {
    hooks.addHook(POST_EXEC_HOOK, hook);
  }

  public void addOnFailureHook(ExecuteWithHookContext hook) {
    hooks.addHook(ON_FAILURE_HOOK, hook);
  }

  public void addSemanticAnalyzerHook(HiveSemanticAnalyzerHook hook) {
    hooks.addHook(SEMANTIC_ANALYZER_HOOK, hook);
  }

}
