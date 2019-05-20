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


import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

/**
 * Extension of {@link QueryLifeTimeHook} that has hooks for pre and post parsing of a query.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface QueryLifeTimeHookWithParseHooks extends QueryLifeTimeHook {

  /**
   * Invoked before a query enters the parse phase.
   *
   * @param ctx the context for the hook
   */
  void beforeParse(QueryLifeTimeHookContext ctx);

  /**
   * Invoked after a query parsing. Note: if 'hasError' is true,
   * the query won't enter the following compilation phase.
   *
   * @param ctx the context for the hook
   * @param hasError whether any error occurred during compilation.
   */
  void afterParse(QueryLifeTimeHookContext ctx, boolean hasError);
}
