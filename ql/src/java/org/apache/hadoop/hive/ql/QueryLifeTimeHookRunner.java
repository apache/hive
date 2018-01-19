/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.hadoop.hive.ql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HooksLoader;
import org.apache.hadoop.hive.ql.hooks.MaterializedViewRegistryUpdateHook;
import org.apache.hadoop.hive.ql.hooks.MetricsQueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContextImpl;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookWithParseHooks;
import org.apache.hadoop.hive.ql.session.SessionState;


/**
 * A runner class for {@link QueryLifeTimeHook}s and {@link QueryLifeTimeHookWithParseHooks}. The class has run methods
 * for each phase of a {@link QueryLifeTimeHook} and {@link QueryLifeTimeHookWithParseHooks}. Each run method checks if
 * a list of hooks has be specified, and if so invokes the appropriate callback method of each hook. Each method
 * constructs a {@link QueryLifeTimeHookContext} object and pass it to the callback functions.
 */
class QueryLifeTimeHookRunner {

  private final HiveConf conf;
  private final List<QueryLifeTimeHook> queryHooks;

  /**
   * Constructs a {@link QueryLifeTimeHookRunner} that loads all hooks to be run via a {@link HooksLoader}.
   *
   * @param conf the {@link HiveConf} to use when creating {@link QueryLifeTimeHookContext} objects
   * @param hooksLoader the {@link HooksLoader} to use when loading all hooks to be run
   * @param console the {@link SessionState.LogHelper} to use when running {@link HooksLoader#getHooks(HiveConf.ConfVars)}
   */
  QueryLifeTimeHookRunner(HiveConf conf, HooksLoader hooksLoader, SessionState.LogHelper console) {
    this.conf = conf;
    this.queryHooks = new ArrayList<>();

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED)) {
      queryHooks.add(new MetricsQueryLifeTimeHook());
    }
    queryHooks.add(new MaterializedViewRegistryUpdateHook());

    List<QueryLifeTimeHook> propertyDefinedHoooks;
    try {
      propertyDefinedHoooks = hooksLoader.getHooks(
          HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, console, QueryLifeTimeHook.class);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
    if (propertyDefinedHoooks != null) {
      Iterables.addAll(queryHooks, propertyDefinedHoooks);
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
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).build();

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
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        if (hook instanceof QueryLifeTimeHookWithParseHooks) {
          ((QueryLifeTimeHookWithParseHooks) hook).afterParse(qhc, parseError);
        }
      }
    }
  }

  /**
   * Invoke the {@link QueryLifeTimeHook#beforeCompile(QueryLifeTimeHookContext)} method for each {@link QueryLifeTimeHook}
   *
   * @param command the Hive command that is being run
   */
  void runBeforeCompileHook(String command) {
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.beforeCompile(qhc);
      }
    }
  }

   /**
   * Invoke the {@link QueryLifeTimeHook#afterCompile(QueryLifeTimeHookContext, boolean)} method for each {@link QueryLifeTimeHook}
   *
   * @param command the Hive command that is being run
   * @param compileError true if there was an error while compiling the command, false otherwise
   */
  void runAfterCompilationHook(String command, boolean compileError) {
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterCompile(qhc, compileError);
      }
    }
  }

  /**
   * Invoke the {@link QueryLifeTimeHook#beforeExecution(QueryLifeTimeHookContext)} method for each {@link QueryLifeTimeHook}
   *
   * @param command the Hive command that is being run
   * @param hookContext the {@link HookContext} of the command being run
   */
  void runBeforeExecutionHook(String command, HookContext hookContext) {
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).withHookContext(hookContext).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.beforeExecution(qhc);
      }
    }
  }

  /**
   * Invoke the {@link QueryLifeTimeHook#afterExecution(QueryLifeTimeHookContext, boolean)} method for each {@link QueryLifeTimeHook}
   *
   * @param command the Hive command that is being run
   * @param hookContext the {@link HookContext} of the command being run
   * @param executionError true if there was an error while executing the command, false otherwise
   */
  void runAfterExecutionHook(String command, HookContext hookContext, boolean executionError) {
    if (containsHooks()) {
      QueryLifeTimeHookContext qhc = new QueryLifeTimeHookContextImpl.Builder().withHiveConf(conf).withCommand(
              command).withHookContext(hookContext).build();

      for (QueryLifeTimeHook hook : queryHooks) {
        hook.afterExecution(qhc, executionError);
      }
    }
  }

  private boolean containsHooks() {
    return queryHooks != null && !queryHooks.isEmpty();
  }
}
