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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toMap;

public enum ReExecutionStrategyType {
  OVERLAY("overlay", ReExecutionOverlayPlugin.class),
  REOPTIMIZE("reoptimize", ReOptimizePlugin.class),
  REEXECUTE_LOST_AM("reexecute_lost_am", ReExecuteLostAMQueryPlugin.class),
  RECOMPILE_WITHOUT_CBO("recompile_without_cbo", ReCompileWithoutCBOPlugin.class),
  DAGSUBMIT("dagsubmit", ReExecutionDagSubmitPlugin.class),
  WRITE_CONFLICT("write_conflict", ReExecuteOnWriteConflictPlugin.class);

  private static final Map<String, ? extends Class<? extends IReExecutionPlugin>> STRATEGY_LOOKUP =
      Arrays.stream(ReExecutionStrategyType.values())
          .collect(toMap(ReExecutionStrategyType::getStrategy, ReExecutionStrategyType::getPluginClass));

  ReExecutionStrategyType(String strategy, Class<? extends IReExecutionPlugin> pluginClass) {
    this.strategy = strategy;
    this.pluginClass = pluginClass;
  }

  private final String strategy;
  private final Class<? extends IReExecutionPlugin> pluginClass;

  public String getStrategy() {
    return strategy;
  }

  public Class<? extends IReExecutionPlugin> getPluginClass() {
    return pluginClass;
  }

  public static Class<? extends IReExecutionPlugin> fromStrategyName(String strategy) {
    return Optional.of(STRATEGY_LOOKUP.get(strategy.toLowerCase())).orElseThrow(IllegalArgumentException::new);
  }
}
