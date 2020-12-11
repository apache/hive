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

import com.google.common.collect.Lists;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveAlgorithmsConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;
import java.util.Set;


public class HivePlannerContext implements Context {
  private final HiveAlgorithmsConf algoConfig;
  private final HiveRulesRegistry registry;
  private final CalciteConnectionConfig calciteConfig;
  private final SubqueryConf subqueryConfig;
  private final HiveConfPlannerContext isCorrelatedColumns;
  private final FunctionHelper functionHelper;
  private final EngineEventSequence timeline;
  private final List<Object> resources = Lists.newArrayList();

  public HivePlannerContext(HiveAlgorithmsConf algoConfig, HiveRulesRegistry registry,
      CalciteConnectionConfig calciteConfig, Set<RelNode> corrScalarRexSQWithAgg,
      HiveConfPlannerContext isCorrelatedColumns, FunctionHelper functionHelper,
      EngineEventSequence timeline) {
    this.algoConfig = algoConfig;
    this.registry = registry;
    this.calciteConfig = calciteConfig;
    // this is to keep track if a subquery is correlated and contains aggregate
    // this is computed in CalcitePlanner while planning and is later required by subuery remove rule
    // hence this is passed using HivePlannerContext
    this.subqueryConfig = new SubqueryConf(corrScalarRexSQWithAgg);
    this.isCorrelatedColumns = isCorrelatedColumns;
    this.functionHelper = functionHelper;
    this.timeline = timeline;
  }

  public void addResource(Object resource) {
    resources.add(resource);
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
    if (clazz.isInstance(subqueryConfig)) {
      return clazz.cast(subqueryConfig);
    }
    if (clazz.isInstance(isCorrelatedColumns)) {
      return clazz.cast(isCorrelatedColumns);
    }
    if (clazz.isInstance(functionHelper)) {
      return clazz.cast(functionHelper);
    }
    if (clazz.isInstance(timeline)) {
      return clazz.cast(timeline);
    }
    for (Object resource : resources) {
      if (clazz.isInstance(resource)) {
        return clazz.cast((T) resource);
      }
    }
    return null;
  }
}
