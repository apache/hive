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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.MetricsQueryLifeTimeHook;
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
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hive.common.util.HiveStringUtils;

/**
 * Handles hook executions for {@link Driver}.
 */
public class HookRunner {

  private static final String CLASS_NAME = Driver.class.getName();
  private final HiveConf conf;
  private LogHelper console;
  private List<QueryLifeTimeHook> queryHooks = new ArrayList<>();
  private List<HiveSemanticAnalyzerHook> saHooks = new ArrayList<>();
  private List<HiveDriverRunHook> driverRunHooks = new ArrayList<>();
  private List<ExecuteWithHookContext> preExecHooks = new ArrayList<>();
  private List<ExecuteWithHookContext> postExecHooks = new ArrayList<>();
  private List<ExecuteWithHookContext> onFailureHooks = new ArrayList<>();
  private boolean initialized = false;

  /**
   * Constructs a {@link HookRunner} that loads all hooks to be run via a {@link HooksLoader}.
   */
  HookRunner(HiveConf conf, SessionState.LogHelper console) {
    this.conf = conf;
    this.console = console;
  }

  public void initialize() {
    if (initialized) {
      return;
    }
    initialized = true;
    queryHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, QueryLifeTimeHook.class));
    saHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class));
    driverRunHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS, HiveDriverRunHook.class));
    preExecHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.PREEXECHOOKS, ExecuteWithHookContext.class));
    postExecHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.POSTEXECHOOKS, ExecuteWithHookContext.class));
    onFailureHooks.addAll(loadHooksFromConf(HiveConf.ConfVars.ONFAILUREHOOKS, ExecuteWithHookContext.class));

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED)) {
      queryHooks.add(new MetricsQueryLifeTimeHook());
    }
  }


  private <T extends Hook> List<T> loadHooksFromConf(ConfVars hookConfVar, Class<T> clazz) {
    try {
      return HookUtils.readHooksFromConf(conf, hookConfVar);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      String message = "Error loading hooks(" + hookConfVar + "): " + HiveStringUtils.stringifyException(e);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * If {@link QueryLifeTimeHookWithParseHooks} have been loaded via the {@link HooksLoader} then invoke the
   * {@link QueryLifeTimeHookWithParseHooks#beforeParse(QueryLifeTimeHookContext)} method for each
   * {@link QueryLifeTimeHookWithParseHooks}.
   *
   * @param command the Hive command that is being run
   */
  void runBeforeParseHook(String command) {
    initialize();
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
   * If {@link QueryLifeTimeHookWithParseHooks} have been loaded via the {@link HooksLoader} then invoke the
   * {@link QueryLifeTimeHookWithParseHooks#afterParse(QueryLifeTimeHookContext, boolean)} method for each
   * {@link QueryLifeTimeHookWithParseHooks}.
   *
   * @param command the Hive command that is being run
   * @param parseError true if there was an error while parsing the command, false otherwise
   */
  void runAfterParseHook(String command, boolean parseError) {
    initialize();
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
    initialize();
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
  * @param command the Hive command that is being run
  * @param compileError true if there was an error while compiling the command, false otherwise
  */
  void runAfterCompilationHook(String command, boolean compileError) {
    initialize();
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc =
          new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterCompile(qhc, compileError);
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
    initialize();
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
    initialize();
    if (!queryHooks.isEmpty()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(command)
          .withHookContext(hookContext).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterExecution(qhc, executionError);
      }
    }
  }

  public ASTNode runPreAnalyzeHooks(HiveSemanticAnalyzerHookContext hookCtx, ASTNode tree) throws HiveException {
    initialize();
    try {
      for (HiveSemanticAnalyzerHook hook : saHooks) {
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
    return !saHooks.isEmpty();
  }

  public void runPostAnalyzeHooks(HiveSemanticAnalyzerHookContext hookCtx,
      List<Task<? extends Serializable>> allRootTasks) throws HiveException {
    initialize();
    try {
      for (HiveSemanticAnalyzerHook hook : saHooks) {
        hook.postAnalyze(hookCtx, allRootTasks);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PostAnalyzeHooks:" + HiveStringUtils.stringifyException(e), e);
    }

  }

  public void runPreDriverHooks(HiveDriverRunHookContext hookContext) throws HiveException {
    initialize();
    try {
      for (HiveDriverRunHook driverRunHook : driverRunHooks) {
        driverRunHook.preDriverRun(hookContext);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PreDriverHooks:" + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void runPostDriverHooks(HiveDriverRunHookContext hookContext) throws HiveException {
    initialize();
    try {
      for (HiveDriverRunHook driverRunHook : driverRunHooks) {
        driverRunHook.postDriverRun(hookContext);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking PostDriverHooks:" + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void runPreHooks(HookContext hookContext) throws HiveException {
    initialize();
    invokeGeneralHook(preExecHooks, PerfLogger.PRE_HOOK, hookContext);
  }

  public void runPostExecHooks(HookContext hookContext) throws HiveException {
    initialize();
    invokeGeneralHook(postExecHooks, PerfLogger.POST_HOOK, hookContext);
  }

  public void runFailureHooks(HookContext hookContext) throws HiveException {
    initialize();
    invokeGeneralHook(onFailureHooks, PerfLogger.FAILURE_HOOK, hookContext);
  }

  private static void invokeGeneralHook(List<ExecuteWithHookContext> hooks, String prefix, HookContext hookContext)
      throws HiveException {
    if (hooks.isEmpty()) {
      return;
    }
    try {
      PerfLogger perfLogger = SessionState.getPerfLogger();

      for (ExecuteWithHookContext hook : hooks) {
        perfLogger.PerfLogBegin(CLASS_NAME, prefix + hook.getClass().getName());
        hook.run(hookContext);
        perfLogger.PerfLogEnd(CLASS_NAME, prefix + hook.getClass().getName());
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException("Error while invoking " + prefix + " hooks: " + HiveStringUtils.stringifyException(e), e);
    }
  }

  public void addLifeTimeHook(QueryLifeTimeHook hook) {
    queryHooks.add(hook);
  }

  public void addPreHook(ExecuteWithHookContext hook) {
    preExecHooks.add(hook);
  }

  public void addPostHook(ExecuteWithHookContext hook) {
    postExecHooks.add(hook);
  }

  public void addOnFailureHook(ExecuteWithHookContext hook) {
    onFailureHooks.add(hook);
  }
}
