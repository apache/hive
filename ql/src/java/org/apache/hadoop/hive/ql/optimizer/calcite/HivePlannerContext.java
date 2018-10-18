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
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;

import java.util.Set;


public class HivePlannerContext implements Context {
  private HiveAlgorithmsConf algoConfig;
  private HiveRulesRegistry registry;
  private CalciteConnectionConfig calciteConfig;
  private SubqueryConf subqueryConfig;
  private HiveConfPlannerContext isCorrelatedColumns;

  public HivePlannerContext(HiveAlgorithmsConf algoConfig, HiveRulesRegistry registry,
      CalciteConnectionConfig calciteConfig, Set<RelNode> corrScalarRexSQWithAgg,
      Set<RelNode> scalarAggNoGbyWindowing, HiveConfPlannerContext isCorrelatedColumns) {
    this.algoConfig = algoConfig;
    this.registry = registry;
    this.calciteConfig = calciteConfig;
    // this is to keep track if a subquery is correlated and contains aggregate
    // this is computed in CalcitePlanner while planning and is later required by subuery remove rule
    // hence this is passed using HivePlannerContext
    this.subqueryConfig = new SubqueryConf(corrScalarRexSQWithAgg, scalarAggNoGbyWindowing);
    this.isCorrelatedColumns = isCorrelatedColumns;
  }

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
    if(clazz.isInstance(subqueryConfig)) {
      return clazz.cast(subqueryConfig);
    }
    if(clazz.isInstance(isCorrelatedColumns)) {
      return clazz.cast(isCorrelatedColumns);
    }
    return null;
  }
}