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
package org.apache.hadoop.hive.ql.optimizer.calcite.druid;

import java.util.List;
import java.util.Set;

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Table mapped onto a Druid table.
 *
 * TODO: to be removed when Calcite is upgraded to 1.9
 */
public class DruidTable extends AbstractTable implements TranslatableTable {

  public static final String DEFAULT_TIMESTAMP_COLUMN = "__time";
  public static final Interval DEFAULT_INTERVAL = new Interval(
          new DateTime("1900-01-01"),
          new DateTime("3000-01-01")
  );

  final DruidSchema schema;
  final String dataSource;
  final RelDataType rowType;
  final RelProtoDataType protoRowType;
  final ImmutableSet<String> metricFieldNames;
  final ImmutableList<Interval> intervals;
  final String timestampFieldName;

  /**
   * Creates a Druid table.
   *
   * @param schema Druid schema that contains this table
   * @param dataSource Druid data source name
   * @param protoRowType Field names and types
   * @param metricFieldNames Names of fields that are metrics
   * @param interval Default interval if query does not constrain the time
   * @param timestampFieldName Name of the column that contains the time
   */
  public DruidTable(DruidSchema schema, String dataSource,
      RelProtoDataType protoRowType, Set<String> metricFieldNames,
      List<Interval> intervals, String timestampFieldName) {
    this.schema = Preconditions.checkNotNull(schema);
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.rowType = null;
    this.protoRowType = protoRowType;
    this.metricFieldNames = ImmutableSet.copyOf(metricFieldNames);
    this.intervals = ImmutableList.copyOf(intervals);
    this.timestampFieldName = Preconditions.checkNotNull(timestampFieldName);
  }

  public DruidTable(DruidSchema schema, String dataSource,
      RelDataType rowType, Set<String> metricFieldNames,
      List<Interval> intervals, String timestampFieldName) {
    this.schema = Preconditions.checkNotNull(schema);
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.rowType = Preconditions.checkNotNull(rowType);
    this.protoRowType = null;
    this.metricFieldNames = ImmutableSet.copyOf(metricFieldNames);
    this.intervals = ImmutableList.copyOf(intervals);
    this.timestampFieldName = Preconditions.checkNotNull(timestampFieldName);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType thisRowType;
    if (rowType != null) {
      thisRowType = rowType;
    } else {
      // Generate
      thisRowType = protoRowType.apply(typeFactory);
    }
    final List<String> fieldNames = thisRowType.getFieldNames();
    Preconditions.checkArgument(fieldNames.contains(timestampFieldName));
    Preconditions.checkArgument(fieldNames.containsAll(metricFieldNames));
    return thisRowType;
  }

  public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    final TableScan scan = LogicalTableScan.create(cluster, relOptTable);
    return DruidQuery.create(cluster,
        cluster.traitSetOf(BindableConvention.INSTANCE), relOptTable, this,
        ImmutableList.<RelNode>of(scan));
  }

}

// End DruidTable.java