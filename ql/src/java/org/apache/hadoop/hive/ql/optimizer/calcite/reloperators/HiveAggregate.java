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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import com.google.common.collect.Sets;

public class HiveAggregate extends Aggregate implements HiveRelNode {

  private LinkedHashSet<Integer> aggregateColumnsOrder;


  public HiveAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, false,
            groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input,
          ImmutableBitSet groupSet,
          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new HiveAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }

  // getRows will call estimateRowCount
  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getDistinctRowCount(this, groupSet, getCluster().getRexBuilder().makeLiteral(true));
  }

  public boolean isBucketedInput() {
    final RelMetadataQuery mq = this.getInput().getCluster().getMetadataQuery();
    return mq.distribution(this).getKeys().
            containsAll(groupSet.asList());
  }

  @Override
  protected RelDataType deriveRowType() {
    return deriveRowType(getCluster().getTypeFactory(), getInput().getRowType(),
        indicator, groupSet, aggCalls);
  }

  public static RelDataType deriveRowType(RelDataTypeFactory typeFactory,
      final RelDataType inputRowType, boolean indicator,
      ImmutableBitSet groupSet,
      final List<AggregateCall> aggCalls) {
    final List<Integer> groupList = groupSet.asList();
    assert groupList.size() == groupSet.cardinality();
    final RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    final List<RelDataTypeField> fieldList = inputRowType.getFieldList();
    final Set<String> containedNames = Sets.newHashSet();
    for (int groupKey : groupList) {
      containedNames.add(fieldList.get(groupKey).getName());
      builder.add(fieldList.get(groupKey));
    }
    if (indicator) {
      for (int groupKey : groupList) {
        final RelDataType booleanType =
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
        String name = "i$" + fieldList.get(groupKey).getName();
        int i = 0;
        StringBuilder nameBuilder = new StringBuilder(name);
        while (containedNames.contains(name)) {
          nameBuilder.append('_').append(i++);
        }
        containedNames.add(nameBuilder.toString());
        builder.add(name, booleanType);
      }
    }
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      String name;
      if (aggCall.e.name != null) {
        name = aggCall.e.name;
      } else {
        name = "$f" + (groupList.size() + aggCall.i);
      }
      int i = 0;
      while (containedNames.contains(name)) {
        name += "_" + i++;
      }
      containedNames.add(name);
      builder.add(name, aggCall.e.type);
    }
    return builder.build();
  }

  public void setAggregateColumnsOrder(LinkedHashSet<Integer> aggregateColumnsOrder) {
    this.aggregateColumnsOrder = aggregateColumnsOrder;
  }

  public LinkedHashSet<Integer> getAggregateColumnsOrder() {
    return this.aggregateColumnsOrder;
  }

  //required for HiveRelDecorrelator
  @Override public RelNode accept(RelShuttle shuttle) {
    if(shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }

}
