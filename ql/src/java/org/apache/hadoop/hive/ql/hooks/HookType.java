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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The hook types
 *
 */
public enum HookType {

  OOM(HiveConf.ConfVars.HIVE_SERVER2_OOM_HOOKS, Runnable.class, true,
      "Hooks that will be run when HiveServer2 stops due to OutOfMemoryError"),
  QUERY_LIFETIME_HOOKS(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, QueryLifeTimeHook.class, false,
      "Hooks that will be triggered before/after query compilation and before/after query execution"),
  QUERY_LIFETIME_HOOKS_WITH_PARSE(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, QueryLifeTimeHookWithParseHooks.class, false,
      "Hooks that will be invoked during pre and post query parsing"),
  SEMANTIC_ANALYZER_HOOK(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class, false,
      "Hooks that invoked before/after Hive performs its own semantic analysis on a statement"),
  DRIVER_RUN_HOOKS(HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS, HiveDriverRunHook.class, false,
      "Hooks that Will be run at the beginning and end of Driver.run"),
  PREEXECHOOKS(HiveConf.ConfVars.PREEXECHOOKS, ExecuteWithHookContext.class, false,
      "Pre-execution hooks to be invoked for each statement"),
  POSTEXECHOOKS(HiveConf.ConfVars.POSTEXECHOOKS, ExecuteWithHookContext.class, false,
      "Post-execution hooks to be invoked for each statement"),
  ONFAILUREHOOKS(HiveConf.ConfVars.ONFAILUREHOOKS, ExecuteWithHookContext.class, false,
      "On-failure hooks to be invoked for each statement");
  //
  private final HiveConf.ConfVars hookConfVar;
  // the super class or interface of the corresponding hooks
  private final Class hookClass;
  // true for making added or loaded hooks store in hooks mainly for latter use,
  // otherwise false.
  private final boolean isGlobal;
  private final String description;
  // store the hooks when isGlobal is true
  private List hooks = null;
  // flag that indicates whether the hooks have been loaded from configuration or not
  private boolean loadedFromConf = false;

  HookType(HiveConf.ConfVars hookConfVar, Class hookClass, boolean isGlobal, String description) {
    this.hookConfVar = hookConfVar;
    this.hookClass = hookClass;
    this.isGlobal = isGlobal;
    this.description = description;
    if (isGlobal) {
     hooks = Collections.synchronizedList(new ArrayList());
    }
  }

  /**
   * Get all hooks corresponding to this hook type
   * @return the hooks or null if isGlobal is false
   */
  public List getHooks() {
    return this.hooks;
  }

  public boolean hasLoadedFromConf() {
    return loadedFromConf;
  }

  public void setLoadedFromConf(boolean loadedFromConf) {
    this.loadedFromConf = loadedFromConf;
  }

  public boolean isGlobal() {
    return this.isGlobal;
  }

  public Class getHookClass() {
    return this.hookClass;
  }

  public HiveConf.ConfVars getHookConfVar() {
    return hookConfVar;
  }

  public String getDescription() {
    return description;
  }
}
