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

package org.apache.hadoop.hive.ql.reexec;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;

/**
 * Defines an interface for re-execution logics.
 *
 * FIXME: rethink methods.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface IReExecutionPlugin {

  /**
   * Called when the {@link Driver} is being initialized
   *
   * The plugin may add hooks/etc to tap into the system.
   */
  void initialize(Driver driver);

  /**
   * Called before executing the query.
   */
  default void beforeExecute(int executionIndex, boolean explainReOptimization) {
    // default noop
  }

  /**
   * The query have failed, does this plugin advises to re-execute it again?
   */
  default boolean shouldReExecute(int executionNum) {
    // default no
    return false;
  }

  /**
   * The plugin should prepare for the re-compilation of the query.
   */
  default void prepareToReExecute() {
    // default noop
  }

  /**
   * The query has failed; and have been recompiled - does this plugin advises to re-execute it again?
   */
  default boolean shouldReExecuteAfterCompile(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    // default no
    return false;
  }

  /**
   * Called after the driver executed the query - delivers the status and the plan data.
   * @param planMapper
   * @param successful
   */
  default void afterExecute(PlanMapper planMapper, boolean successful) {
    // default noop
  }

  /**
   * Called before the compilation happen.
   * @param compilationNum Number of the previous compilations
   */
  default void beforeCompile(int compilationNum) {
    // default noop
  }

  /**
   * The query has failed. Should we try recompilation?
   * @param compilationNum Number of the previous compilations
   * @return
   */
  default boolean shouldReCompile(int compilationNum) {
    // default no
    return false;
  }

  /**
   * The plugin should prepare for the recompilation of the query
   */
  default void prepareToReCompile() {
    // default noop
  }

  /**
   * Called after the compilation - delivers the result of the new compilation.
   * @param successful
   */
  default void afterCompile(boolean successful) {
    // default noop
  }
}
