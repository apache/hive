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


import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import java.util.Set;

public final class HiveRelMdExpressionLineage
    implements MetadataHandler<BuiltInMetadata.ExpressionLineage> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.EXPRESSION_LINEAGE.method, new HiveRelMdExpressionLineage());

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdExpressionLineage() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.ExpressionLineage> getDef() {
    return BuiltInMetadata.ExpressionLineage.DEF;
  }

  // Union returns NULL instead of actually determining the lineage because
  // Union may return multiple lineage expressions - one corresponding to each branch
  // this could cause exponential possible combinations of lineage expressions
  // as you go up in the operator tree.
  // As the possible number of expressions increases it could lead to OOM.
  // To prevent this UNION returns NULL.
  // sample query could be found in union_lineage.q
  public Set<RexNode> getExpressionLineage(HiveUnion rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    return null;
  }
}

// End HiveRelMdExpressionLineage

