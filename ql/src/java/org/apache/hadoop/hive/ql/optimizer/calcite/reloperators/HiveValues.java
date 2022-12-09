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

package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;

import java.util.List;

/**
 * Subclass of {@link org.apache.calcite.rel.core.Values}.
 * Specialized to Hive engine.
 */
public class HiveValues extends Values implements HiveRelNode {

  public HiveValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
                    RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HiveValues(getCluster(), getRowType(), tuples, getTraitSet());
  }

  public RelNode copy(List<String> newColumnNames) throws CalciteSemanticException {
    if (newColumnNames.size() != getRowType().getFieldCount()) {
      throw new CalciteSemanticException("The number of new column names and columns in the schema does not match!");
    }

    RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

    for (int i = 0; i < getRowType().getFieldCount(); ++i) {
      builder.add(newColumnNames.get(i), getRowType().getFieldList().get(i).getType());
    }

    return new HiveValues(getCluster(), builder.uniquify().build(), tuples, getTraitSet());
  }
}
