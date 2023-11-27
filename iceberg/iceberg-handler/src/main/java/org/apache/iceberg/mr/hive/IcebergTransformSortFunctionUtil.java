/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergBucket;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergDay;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergHour;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergMonth;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergTruncate;
import org.apache.iceberg.mr.hive.udf.GenericUDFIcebergYear;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * A utility class which provides Iceberg transform sort functions.
 */
public final class IcebergTransformSortFunctionUtil {

  private IcebergTransformSortFunctionUtil() {
    // not called
  }

  /**
   * Function template for producing a custom sort expression function:
   * Takes the source column index and the bucket count to create a function where Iceberg transform UDF is used to
   * build the sort expression, e.g. iceberg_bucket(_col2, 5)
   */
  private static final transient BiFunction<Integer, Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      BUCKET_SORT_EXPR =
          (idx, bucket) -> cols -> {
            try {
              ExprNodeDesc icebergBucketSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergBucket(), "iceberg_bucket",
                  Lists.newArrayList(
                      icebergBucketSourceCol,
                      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, bucket)
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final transient BiFunction<Integer, Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      TRUNCATE_SORT_EXPR =
          (idx, truncateLength) -> cols -> {
            try {
              ExprNodeDesc icebergTruncateSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergTruncate(), "iceberg_truncate",
                  Lists.newArrayList(
                      icebergTruncateSourceCol,
                      new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, truncateLength)
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final transient Function<Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      YEAR_SORT_EXPR =
          idx -> cols -> {
            try {
              ExprNodeDesc icebergYearSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergYear(), "iceberg_year",
                  Lists.newArrayList(
                      icebergYearSourceCol
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final transient Function<Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      MONTH_SORT_EXPR =
          idx -> cols -> {
            try {
              ExprNodeDesc icebergMonthSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergMonth(), "iceberg_month",
                  Lists.newArrayList(
                      icebergMonthSourceCol
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final transient Function<Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      DAY_SORT_EXPR =
          idx -> cols -> {
            try {
              ExprNodeDesc icebergDaySourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergDay(), "iceberg_day",
                  Lists.newArrayList(
                      icebergDaySourceCol
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  private static final transient Function<Integer, Function<List<ExprNodeDesc>, ExprNodeDesc>>
      HOUR_SORT_EXPR =
          idx -> cols -> {
            try {
              ExprNodeDesc icebergHourSourceCol = cols.get(idx);
              return ExprNodeGenericFuncDesc.newInstance(new GenericUDFIcebergHour(), "iceberg_hour",
                  Lists.newArrayList(
                      icebergHourSourceCol
                  ));
            } catch (UDFArgumentException e) {
              throw new RuntimeException(e);
            }
          };

  public static Function<List<ExprNodeDesc>, ExprNodeDesc> getCustomSortExprs(TransformSpec spec, int index) {
    switch (spec.getTransformType()) {
      case BUCKET:
        return BUCKET_SORT_EXPR.apply(index, spec.getTransformParam().get());
      case TRUNCATE:
        return TRUNCATE_SORT_EXPR.apply(index, spec.getTransformParam().get());
      case YEAR:
        return YEAR_SORT_EXPR.apply(index);
      case MONTH:
        return MONTH_SORT_EXPR.apply(index);
      case DAY:
        return DAY_SORT_EXPR.apply(index);
      case HOUR:
        return HOUR_SORT_EXPR.apply(index);
      default:
        return cols -> cols.get(index).clone();
    }
  }

}
