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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewProjectFilterRule;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewOnlyFilterRule;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewProjectJoinRule;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule.MaterializedViewOnlyAggregateRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * Enable join and aggregate materialized view rewriting
 */
public class HiveMaterializedViewRule {

  public static final MaterializedViewProjectFilterRule INSTANCE_PROJECT_FILTER =
      new MaterializedViewProjectFilterRule(HiveRelFactories.HIVE_BUILDER, true);

  public static final MaterializedViewOnlyFilterRule INSTANCE_FILTER =
      new MaterializedViewOnlyFilterRule(HiveRelFactories.HIVE_BUILDER, true);

  public static final MaterializedViewProjectJoinRule INSTANCE_PROJECT_JOIN =
      new MaterializedViewProjectJoinRule(HiveRelFactories.HIVE_BUILDER, true);

  public static final MaterializedViewOnlyJoinRule INSTANCE_JOIN =
      new MaterializedViewOnlyJoinRule(HiveRelFactories.HIVE_BUILDER, true);

  public static final MaterializedViewProjectAggregateRule INSTANCE_PROJECT_AGGREGATE =
      new MaterializedViewProjectAggregateRule(HiveRelFactories.HIVE_BUILDER, true);

  public static final MaterializedViewOnlyAggregateRule INSTANCE_AGGREGATE =
      new MaterializedViewOnlyAggregateRule(HiveRelFactories.HIVE_BUILDER, true);

}
