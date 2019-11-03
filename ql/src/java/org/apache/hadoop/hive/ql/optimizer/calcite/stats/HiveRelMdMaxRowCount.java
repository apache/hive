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
/* TODO: CALC-2991 created some optimizations.  This file bypasses
   the change for now (see HIVE-22408)
*/
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import com.google.common.collect.ImmutableList;

public class HiveRelMdMaxRowCount extends RelMdMaxRowCount {

  private static final HiveRelMdMaxRowCount INSTANCE =
      new HiveRelMdMaxRowCount();

  public static final RelMetadataProvider SOURCE =
          ChainedRelMetadataProvider.of(
                  ImmutableList.of(
                          ReflectiveRelMetadataProvider.reflectiveSource(
                                  BuiltInMethod.MAX_ROW_COUNT.method, new HiveRelMdMaxRowCount()),
                          RelMdMaxRowCount.SOURCE));

  private HiveRelMdMaxRowCount() {
    super();
  }

  @Override
  public Double getMaxRowCount(Aggregate rel, RelMetadataQuery mq) {
    if (rel.getGroupSet().isEmpty()) {
      // Aggregate with no GROUP BY always returns 1 row (even on empty table).
      return 1D;
    }

    final Double rowCount = mq.getMaxRowCount(rel.getInput());
    if (rowCount == null) {
      return null;
    }
    return rowCount * rel.getGroupSets().size();
  }

}
