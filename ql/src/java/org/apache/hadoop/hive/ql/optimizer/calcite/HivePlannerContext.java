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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;

public class HivePlannerContext implements Context {
  private HiveAlgorithmsConf algoConfig;
  private HiveRulesRegistry registry;
  private CalciteConnectionConfig calciteConfig;
  private HiveConfPlannerContext isCorrelatedColumns;
  private StatsSource statsSource;

  public HivePlannerContext(HiveAlgorithmsConf algoConfig, HiveRulesRegistry registry,
      CalciteConnectionConfig calciteConfig,
      HiveConfPlannerContext isCorrelatedColumns, StatsSource statsSource) {
    this.algoConfig = algoConfig;
    this.registry = registry;
    this.calciteConfig = calciteConfig;
    this.statsSource = statsSource;
    this.isCorrelatedColumns = isCorrelatedColumns;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(algoConfig)) {
      return clazz.cast(algoConfig);
    }
    if (clazz.isInstance(registry)) {
      return clazz.cast(registry);
    }
    if (clazz.isInstance(calciteConfig)) {
      return clazz.cast(calciteConfig);
    }
    if(clazz.isInstance(isCorrelatedColumns)) {
      return clazz.cast(isCorrelatedColumns);
    }
    if (clazz.isInstance(statsSource)) {
      return clazz.cast(statsSource);
    }
    return null;
  }
}
