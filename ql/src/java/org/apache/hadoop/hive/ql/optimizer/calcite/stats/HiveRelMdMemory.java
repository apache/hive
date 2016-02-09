/**
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

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdMemory;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

public class HiveRelMdMemory extends RelMdMemory {
  
  private static final HiveRelMdMemory INSTANCE = new HiveRelMdMemory();

  public static final RelMetadataProvider SOURCE =
          ReflectiveRelMetadataProvider.reflectiveSource(INSTANCE,
                  BuiltInMethod.MEMORY.method,
                  BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
                  BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method);

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdMemory() {}

  //~ Methods ----------------------------------------------------------------

  public Double memory(HiveTableScan tableScan, RelMetadataQuery mq) {
    return 0.0d;
  }

  public Double memory(HiveAggregate aggregate, RelMetadataQuery mq) {
    final Double avgRowSize = mq.getAverageRowSize(aggregate.getInput());
    final Double rowCount = mq.getRowCount(aggregate.getInput());
    if (avgRowSize == null || rowCount == null) {
      return null;
    }
    return avgRowSize * rowCount;
  }

  public Double memory(HiveFilter filter, RelMetadataQuery mq) {
    return 0.0;
  }

  public Double memory(HiveJoin join, RelMetadataQuery mq) {
    return join.getMemory();
  }

  public Double cumulativeMemoryWithinPhaseSplit(HiveJoin join) {
    return join.getCumulativeMemoryWithinPhaseSplit();
  }

  public Double memory(HiveProject project, RelMetadataQuery mq) {
    return 0.0;
  }

  public Double memory(HiveSortLimit sort, RelMetadataQuery mq) {
    if (sort.getCollation() != RelCollations.EMPTY) {
      // It sorts
      final Double avgRowSize = mq.getAverageRowSize(sort.getInput());
      final Double rowCount = mq.getRowCount(sort.getInput());
      if (avgRowSize == null || rowCount == null) {
        return null;
      }
      return avgRowSize * rowCount;
    }
    // It does not sort, memory footprint is zero
    return 0.0;
  }

  public Double memory(HiveUnion union, RelMetadataQuery mq) {
    return 0.0;
  }

}
