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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.Optional;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.HivePlannerContext;
import org.apache.hadoop.hive.ql.optimizer.signature.RelTreeSignature;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRelMdRuntimeRowCount extends HiveRelMdRowCount {

  protected static final Logger LOG  = LoggerFactory.getLogger(HiveRelMdRuntimeRowCount.class.getName());

  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new HiveRelMdRuntimeRowCount());

  protected HiveRelMdRuntimeRowCount() {
    super();
  }

  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    Optional<Long> runtimeRowCount = getRuntimeRowCount(rel);
    if (runtimeRowCount.isPresent()) {
      return runtimeRowCount.get().doubleValue();
    } else {
      return super.getRowCount(rel, mq);
    }
  }

  public Optional<Long> getRuntimeRowCount(RelNode rel) {
    RelOptCluster cluster = rel.getCluster();
    Context context = cluster.getPlanner().getContext();
    if (context instanceof HivePlannerContext) {
      StatsSource ss = ((HivePlannerContext) context).unwrap(StatsSource.class);
      if (ss.canProvideStatsFor(rel.getClass())) {
        Optional<OperatorStats> os = ss.lookup(RelTreeSignature.of(rel));
        if (os.isPresent()) {
          long outputRecords = os.get().getOutputRecords();
          return Optional.of(outputRecords);
        }
      }
    }
    return Optional.empty();
  }

}
