/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

public class HiveRelMdAggregatedColumns implements MetadataHandler<AggregatedColumns> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new HiveRelMdAggregatedColumns(),
          MetadataMethod.AGGREGATED_COLUMNS.method());
  @Nullable
  public Boolean areColumnsAggregated(Project rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    ImmutableBitSet.Builder iRefs = ImmutableBitSet.builder();
    for (int bit : columns) {
      RexNode e = rel.getProjects().get(bit);
      if (e instanceof RexInputRef) {
        iRefs.set(((RexInputRef) e).getIndex());
      } else {
        return false;
      }
    }
    return ((HiveRelMetadataQuery) mq).areColumnsAggregated(rel.getInput(), iRefs.build());
  }
  @Nullable
  public Boolean areColumnsAggregated(Aggregate aggregate, RelMetadataQuery mq, ImmutableBitSet columns) {
    ImmutableBitSet aggColumns =
        ImmutableBitSet.range(aggregate.getGroupCount(), aggregate.getGroupCount() + aggregate.getAggCallList().size());
    return aggColumns.equals(columns);
  }
  @Nullable
  public Boolean areColumnsAggregated(Filter rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return ((HiveRelMetadataQuery) mq).areColumnsAggregated(rel.getInput(), columns);
  }
  @Nullable
  public Boolean areColumnsAggregated(HepRelVertex rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return ((HiveRelMetadataQuery) mq).areColumnsAggregated(rel.getCurrentRel(), columns);
  }
  @Nullable
  public Boolean areColumnsAggregated(RelSubset rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return ((HiveRelMetadataQuery) mq).areColumnsAggregated(rel.getOriginal(), columns);
  }
  @Nullable
  public Boolean areColumnsAggregated(RelNode rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return null;
  }

  @Override
  public MetadataDef<AggregatedColumns> getDef() {
    return AggregatedColumns.DEF;
  }

}
