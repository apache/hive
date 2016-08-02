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

/**
 * A type of hook which triggers before query compilation and after query execution.
 */
public interface QueryLifeTimeHook extends Hook {

  /**
   * Invoked before a query enters the compilation phase.
   *
   * @param ctx the context for the hook
   */
  void beforeCompile(QueryLifeTimeHookContext ctx);

  /**
   * Invoked after a query compilation. Note: if 'hasError' is true,
   * the query won't enter the following execution phase.
   *
   * @param ctx the context for the hook
   * @param hasError whether any error occurred during compilation.
   */
  void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError);

  /**
   * Invoked before a query enters the execution phase.
   *
   * @param ctx the context for the hook
   */
  void beforeExecution(QueryLifeTimeHookContext ctx);

  /**
   * Invoked after a query finishes its execution.
   *
   * @param ctx the context for the hook
   * @param hasError whether any error occurred during query execution.
   */
  void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError);

}
