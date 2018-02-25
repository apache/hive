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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.adapter.druid.DruidRules.DruidAggregateFilterTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidAggregateProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidAggregateRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidFilterAggregateTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidFilterProjectTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidFilterRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidPostAggregationProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidProjectFilterTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidProjectRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidProjectSortTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidSortProjectTransposeRule;
import org.apache.calcite.adapter.druid.DruidRules.DruidSortRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * Druid rules with Hive builder factory.
 */
public class HiveDruidRules {

  public static final DruidFilterRule FILTER =
      new DruidFilterRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidProjectRule PROJECT =
      new DruidProjectRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidAggregateRule AGGREGATE =
      new DruidAggregateRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidAggregateProjectRule AGGREGATE_PROJECT =
      new DruidAggregateProjectRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidSortRule SORT =
      new DruidSortRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidSortProjectTransposeRule SORT_PROJECT_TRANSPOSE =
      new DruidSortProjectTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidProjectSortTransposeRule PROJECT_SORT_TRANSPOSE =
      new DruidProjectSortTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidProjectFilterTransposeRule PROJECT_FILTER_TRANSPOSE =
      new DruidProjectFilterTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidFilterProjectTransposeRule FILTER_PROJECT_TRANSPOSE =
      new DruidFilterProjectTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidAggregateFilterTransposeRule AGGREGATE_FILTER_TRANSPOSE =
      new DruidAggregateFilterTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidFilterAggregateTransposeRule FILTER_AGGREGATE_TRANSPOSE =
      new DruidFilterAggregateTransposeRule(HiveRelFactories.HIVE_BUILDER);

  public static final DruidPostAggregationProjectRule POST_AGGREGATION_PROJECT =
      new DruidPostAggregationProjectRule(HiveRelFactories.HIVE_BUILDER);
}
