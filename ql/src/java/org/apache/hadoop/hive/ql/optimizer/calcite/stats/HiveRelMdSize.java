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

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import com.google.common.collect.ImmutableList;

public class HiveRelMdSize extends RelMdSize {

  private static final HiveRelMdSize INSTANCE = new HiveRelMdSize();

  public static final RelMetadataProvider SOURCE =
          ReflectiveRelMetadataProvider.reflectiveSource(INSTANCE,
                  BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
                  BuiltInMethod.AVERAGE_ROW_SIZE.method);

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdSize() {}

  //~ Methods ----------------------------------------------------------------

  public List<Double> averageColumnSizes(HiveTableScan scan) {
    List<Integer> neededcolsLst = scan.getNeededColIndxsFrmReloptHT();
    List<ColStatistics> columnStatistics = ((RelOptHiveTable) scan.getTable())
        .getColStat(neededcolsLst, true);

    // Obtain list of col stats, or use default if they are not available
    final ImmutableList.Builder<Double> list = ImmutableList.builder();
    int indxRqdCol = 0;
    int nFields = scan.getRowType().getFieldCount();
    for (int i = 0; i < nFields; i++) {
      if (neededcolsLst.contains(i)) {
        ColStatistics columnStatistic = columnStatistics.get(indxRqdCol);
        indxRqdCol++;
        if (columnStatistic == null) {
          RelDataTypeField field = scan.getRowType().getFieldList().get(i);
          list.add(averageTypeValueSize(field.getType()));
        } else {
          list.add(columnStatistic.getAvgColLen());
        }
      } else {
        list.add(new Double(0));
      }
    }

    return list.build();
  }

  public List<Double> averageColumnSizes(HiveJoin rel) {
    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();
    final List<Double> lefts =
        RelMetadataQuery.getAverageColumnSizes(left);
    List<Double> rights = null;
    if (!rel.isLeftSemiJoin()) {
        rights = RelMetadataQuery.getAverageColumnSizes(right);
    }
    if (lefts == null && rights == null) {
      return null;
    }
    final int fieldCount = rel.getRowType().getFieldCount();
    Double[] sizes = new Double[fieldCount];
    if (lefts != null) {
      lefts.toArray(sizes);
    }
    if (rights != null) {
      final int leftCount = left.getRowType().getFieldCount();
      for (int i = 0; i < rights.size(); i++) {
        sizes[leftCount + i] = rights.get(i);
      }
    }
    return ImmutableNullableList.copyOf(sizes);
  }

  // TODO: remove when averageTypeValueSize method RelMdSize
  //       supports all types
  public Double averageTypeValueSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case FLOAT:
    case REAL:
    case DECIMAL:
    case DATE:
    case TIME:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case INTERVAL_DAY_TIME:
    case INTERVAL_YEAR_MONTH:
      return 8d;
    case BINARY:
      return (double) type.getPrecision();
    case VARBINARY:
      return Math.min((double) type.getPrecision(), 100d);
    case CHAR:
      return (double) type.getPrecision() * BYTES_PER_CHARACTER;
    case VARCHAR:
      // Even in large (say VARCHAR(2000)) columns most strings are small
      return Math.min((double) type.getPrecision() * BYTES_PER_CHARACTER, 100d);
    case ROW:
      Double average = 0.0;
      for (RelDataTypeField field : type.getFieldList()) {
        average += averageTypeValueSize(field.getType());
      }
      return average;
    default:
      return null;
    }
  }

}
