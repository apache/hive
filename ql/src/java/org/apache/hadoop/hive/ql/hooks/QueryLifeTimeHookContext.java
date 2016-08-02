/**
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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Hook context for {@link QueryLifeTimeHook}.
 */
public interface QueryLifeTimeHookContext {
  /**
   * Get the current Hive configuration
   *
   * @return the Hive configuration being used
   */
  HiveConf getHiveConf();

  /**
   * Set Hive configuration
   */
  void setHiveConf(HiveConf conf);

  /**
   * Get the current command.
   *
   * @return the current query command
   */
  String getCommand();

  /**
   * Set the current command
   *
   * @param command the query command to set
   */
  void setCommand(String command);


  /**
   * Get the hook context for query execution.
   * Note: this result value is null during query compilation phase.
   *
   * @return a {@link HookContext} instance containing information such as query
   * plan, list of tasks, etc.
   */
  HookContext getHookContext();

  /**
   * Set the hook context
   *
   * @param hc a {@link HookContext} containing information for the current query.
   */
  void setHookContext(HookContext hc);
}
