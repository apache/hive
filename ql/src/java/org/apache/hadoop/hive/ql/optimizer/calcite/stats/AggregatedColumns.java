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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

/**
 * Metadata about whether a set of columns originates from aggregation functions.
 */
public interface AggregatedColumns extends Metadata {

  MetadataDef<AggregatedColumns> DEF = MetadataDef.of(AggregatedColumns.class, AggregatedColumns.Handler.class,
      MetadataMethod.AGGREGATED_COLUMNS.method());

  /**
   * Determines whether the specified set of columns originates from aggregation functions.
   *
   * @param columns column mask representing a subset of columns for the current relational expression.
   * @return whether the specified columns originate from aggregate functions or null if there is not enough information
   * to infer the origin function.
   */
  @Nullable
  Boolean areColumnsAggregated(final ImmutableBitSet columns);

  interface Handler extends MetadataHandler<AggregatedColumns> {
    @Nullable
    Boolean areColumnsAggregated(RelNode r, RelMetadataQuery mq, final ImmutableBitSet columns);
  }
}
