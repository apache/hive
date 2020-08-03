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

/**
 * The hook types
 */
public enum HookType {

  QUERY_LIFETIME_HOOKS(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, QueryLifeTimeHook.class,
      "Hooks that will be triggered before/after query compilation and before/after query execution"),
  SEMANTIC_ANALYZER_HOOK(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK, HiveSemanticAnalyzerHook.class,
      "Hooks that invoked before/after Hive performs its own semantic analysis on a statement"),
  DRIVER_RUN_HOOKS(HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS, HiveDriverRunHook.class,
      "Hooks that Will be run at the beginning and end of Driver.run"),
  PRE_EXEC_HOOK(HiveConf.ConfVars.PREEXECHOOKS, ExecuteWithHookContext.class,
      "Pre-execution hooks to be invoked for each statement"),
  POST_EXEC_HOOK(HiveConf.ConfVars.POSTEXECHOOKS, ExecuteWithHookContext.class,
      "Post-execution hooks to be invoked for each statement"),
  ON_FAILURE_HOOK(HiveConf.ConfVars.ONFAILUREHOOKS, ExecuteWithHookContext.class,
      "On-failure hooks to be invoked for each statement"),
  REDACTOR(HiveConf.ConfVars.QUERYREDACTORHOOKS, Redactor.class,
      "Hooks to be invoked for each query which can tranform the query before it's placed in the job.xml file"),
  // The HiveSessionHook.class cannot access, use Hook.class instead
  HIVE_SERVER2_SESSION_HOOK(HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK, Hook.class,
      "Hooks to be executed when session manager starts a new session"),
  OOM(HiveConf.ConfVars.HIVE_SERVER2_OOM_HOOKS, Runnable.class,
      "Hooks that will be run when HiveServer2 stops due to OutOfMemoryError");

  private final HiveConf.ConfVars confVar;
  // the super class or interface of the corresponding hooks
  private final Class hookClass;

  private final String description;

  HookType(HiveConf.ConfVars confVar, Class hookClass, String description) {
    this.confVar = confVar;
    this.description = description;
    this.hookClass = hookClass;
  }

  public Class getHookClass() {
    return this.hookClass;
  }

  public HiveConf.ConfVars getHookConfVar() {
    return this.confVar;
  }

  public String getDescription() {
    return this.description;
  }

}
