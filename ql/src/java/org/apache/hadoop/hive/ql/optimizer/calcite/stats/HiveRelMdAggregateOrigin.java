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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

public class HiveRelMdAggregateOrigin implements MetadataHandler<AggregateOrigins> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(new HiveRelMdAggregateOrigin(),
          MetadataMethod.AGGREGATE_ORIGINS.method());

  public SqlOperator getAggregateOrigins(Project rel, RelMetadataQuery mq, int col) {
    if (col < rel.getProjects().size()) {
      RexNode exp = rel.getProjects().get(col);
      if (exp instanceof RexInputRef) {
        RexInputRef ref = (RexInputRef) exp;
        return ((HiveRelMetadataQuery) mq).getAggregateOrigins(rel.getInput(), ref.getIndex());
      }
    }
    return null;
  }

  public SqlOperator getAggregateOrigins(Aggregate aggregate, RelMetadataQuery mq, int col) {
    if (col >= aggregate.getGroupCount()) {
      AggregateCall call = aggregate.getAggCallList().get(col - aggregate.getGroupCount());
      return call.getAggregation();
    }
    return null;
  }

  public SqlOperator getAggregateOrigins(Filter rel, RelMetadataQuery mq, int col) {
    return ((HiveRelMetadataQuery) mq).getAggregateOrigins(rel.getInput(), col);
  }

  public SqlOperator getAggregateOrigins(HepRelVertex rel, RelMetadataQuery mq, int col) {
    return ((HiveRelMetadataQuery) mq).getAggregateOrigins(rel.getCurrentRel(), col);
  }

  public SqlOperator getAggregateOrigins(RelSubset rel, RelMetadataQuery mq, int col) {
    return ((HiveRelMetadataQuery) mq).getAggregateOrigins(rel.getOriginal(), col);
  }

  public SqlOperator getAggregateOrigins(RelNode rel, RelMetadataQuery mq, int col) {
    return null;
  }

  @Override
  public MetadataDef<AggregateOrigins> getDef() {
    return AggregateOrigins.DEF;
  }

}
