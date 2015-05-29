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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
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

  public Double memory(HiveTableScan tableScan) {
    return 0.0d;
  }

  public Double memory(HiveAggregate aggregate) {
    final Double avgRowSize = RelMetadataQuery.getAverageRowSize(aggregate.getInput());
    final Double rowCount = RelMetadataQuery.getRowCount(aggregate.getInput());
    if (avgRowSize == null || rowCount == null) {
      return null;
    }
    return avgRowSize * rowCount;
  }

  public Double memory(HiveFilter filter) {
    return 0.0;
  }

  public Double memory(HiveJoin join) {
    return join.getMemory();
  }

  public Double cumulativeMemoryWithinPhaseSplit(HiveJoin join) {
    return join.getCumulativeMemoryWithinPhaseSplit();
  }

  public Double memory(HiveLimit limit) {
    return 0.0;
  }

  public Double memory(HiveProject project) {
    return 0.0;
  }

  public Double memory(HiveSort sort) {
    if (sort.getCollation() != RelCollations.EMPTY) {
      // It sorts
      final Double avgRowSize = RelMetadataQuery.getAverageRowSize(sort.getInput());
      final Double rowCount = RelMetadataQuery.getRowCount(sort.getInput());
      if (avgRowSize == null || rowCount == null) {
        return null;
      }
      return avgRowSize * rowCount;
    }
    // It does not sort, memory footprint is zero
    return 0.0;
  }

  public Double memory(HiveUnion union) {
    return 0.0;
  }

}
