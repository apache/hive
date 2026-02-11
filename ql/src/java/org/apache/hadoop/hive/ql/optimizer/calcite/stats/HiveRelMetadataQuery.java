/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

public class HiveRelMetadataQuery extends RelMetadataQuery {
  private AggregatedColumns.Handler aggOriginHandler; 
  
  public HiveRelMetadataQuery() {
    super();
    this.aggOriginHandler = initialHandler(AggregatedColumns.Handler.class);
  }

  /**
   * Returns the {@link AggregatedColumns} statistic.
   *
   * @param input the input relational expression
   * @param columns mask representing a subset of columns for the input relational expression.
   * @return whether the specified columns originate from aggregate functions or null if there is not enough information
   * to infer the origin function.
   */
  @Nullable
  public Boolean areColumnsAggregated(RelNode input, ImmutableBitSet columns) {
    for (;;) {
      try {
        return aggOriginHandler.areColumnsAggregated(input, this, columns);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        aggOriginHandler = revise(e.relClass, AggregatedColumns.DEF);
      }
    }
  }
}
