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

import org.apache.hadoop.hive.ql.hooks.Hook;

/**
 * HiveDriverRunHook allows Hive to be extended with custom
 * logic for processing commands.
 *
 *<p>
 *
 * Note that the lifetime of an instantiated hook object is scoped to
 * the analysis of a single statement; hook instances are never reused.
 */
public interface HiveDriverRunHook extends Hook {
  /**
   * Invoked before Hive begins any processing of a command in the Driver,
   * notably before compilation and any customizable performance logging.
   */
  public void preDriverRun(
    HiveDriverRunHookContext hookContext) throws Exception;

  /**
   * Invoked after Hive performs any processing of a command, just before a
   * response is returned to the entity calling the Driver.
   */
  public void postDriverRun(
    HiveDriverRunHookContext hookContext) throws Exception;
}
