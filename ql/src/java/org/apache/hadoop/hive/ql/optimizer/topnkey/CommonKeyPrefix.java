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
package org.apache.hadoop.hive.ql.optimizer.topnkey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;

/**
 * Holds result of a common key prefix of two operators.
 * Provides factory methods for mapping TopNKey operator keys to GroupBy and ReduceSink operator keys.
 */
public final class CommonKeyPrefix {

  /**
   * Factory method to map a {@link org.apache.hadoop.hive.ql.exec.TopNKeyOperator}'s and a
   * {@link org.apache.hadoop.hive.ql.exec.GroupByOperator}'s keys.
   * This method calls the {@link #map(List, String, String, List, Map, String, String)} method to do the mapping.
   * Since the {@link GroupByDesc} does not contains any ordering information {@link TopNKeyDesc} ordering is passed
   * for both operators.
   * @param topNKeyDesc {@link TopNKeyDesc} contains {@link org.apache.hadoop.hive.ql.exec.TopNKeyOperator} keys.
   * @param groupByDesc {@link GroupByDesc} contains {@link org.apache.hadoop.hive.ql.exec.GroupByOperator} keys.
   * @return {@link CommonKeyPrefix} object containing common key prefix of the mapped operators.
   */
  public static CommonKeyPrefix map(TopNKeyDesc topNKeyDesc, GroupByDesc groupByDesc) {
    return map(topNKeyDesc.getKeyColumns(), topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder(),
            groupByDesc.getKeys(), groupByDesc.getColumnExprMap(),
            topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder());
  }

  /**
   * Factory method to map a {@link org.apache.hadoop.hive.ql.exec.TopNKeyOperator}'s and
   * a {@link org.apache.hadoop.hive.ql.exec.ReduceSinkOperator}'s keys.
   * This method calls the {@link #map(List, String, String, List, Map, String, String)} method to do the mapping.
   * @param topNKeyDesc {@link TopNKeyDesc} contains {@link org.apache.hadoop.hive.ql.exec.TopNKeyOperator} keys.
   * @param reduceSinkDesc {@link ReduceSinkDesc} contains
   *   {@link org.apache.hadoop.hive.ql.exec.ReduceSinkOperator} keys.
   * @return {@link CommonKeyPrefix} object containing common key prefix of the mapped operators.
   */
  public static CommonKeyPrefix map(TopNKeyDesc topNKeyDesc, ReduceSinkDesc reduceSinkDesc) {
    return map(topNKeyDesc.getKeyColumns(), topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder(),
            reduceSinkDesc.getKeyCols(), reduceSinkDesc.getColumnExprMap(),
            reduceSinkDesc.getOrder(), reduceSinkDesc.getNullOrder());
  }

  /**
   * General factory method to map two operator keys.
   * Two keys are considered to be equal
   * - if parent operator's <code>parentColExprMap</code> has an entry with the operator key column name
   * - and that entry value has the same index as the operator key column index.
   * - and both key columns has the same ordering
   * - and both key columns has the same null ordering
   *
   * Ex.: op1: a, b, c, d
   *      op2: a, b, e
   *      result: a, b
   *
   *      opKeys: Column[_col0], Column[_col1], Column[_col2], Column[_col3]
   *      parentKeys: Column[KEY._col0], Column[KEY._col1], Column[KEY._col4]
   *      parentColExprMap: {_col0 -&gt; Column[KEY._col0]}, {_col1 -&gt; Column[KEY._col1]}, {_col4 -&gt; Column[KEY._col4]}
   *
   * Column ordering and null ordering is given by a string where each character represents a column order/null order.
   * Ex.: a ASC NULLS FIRST, b DESC NULLS LAST, c ASC NULLS LAST -&gt; order="+-+", null order="azz"
   *
   * When <code>parentColExprMap</code> is null this method falls back to
   * {@link #map(List, String, String, List, String, String)}.
   *
   * @param opKeys {@link List} of {@link ExprNodeDesc}. contains the operator's key columns
   * @param opOrder operator's key column ordering in {@link String} format
   * @param opNullOrder operator's key column null ordering in {@link String} format
   * @param parentKeys {@link List} of {@link ExprNodeDesc}. contains the parent operator's key columns
   * @param parentColExprMap {@link Map} of {@link String} -&gt; {@link ExprNodeDesc}.
   *                                    contains parent operator's key column name {@link ExprNodeDesc} mapping
   * @param parentOrder parent operator's key column ordering in {@link String} format
   * @param parentNullOrder parent operator's key column null ordering in {@link String} format
   * @return {@link CommonKeyPrefix} object containing the common key prefix of the mapped operators.
   */
  public static CommonKeyPrefix map(
          List<ExprNodeDesc> opKeys, String opOrder, String opNullOrder,
          List<ExprNodeDesc> parentKeys, Map<String, ExprNodeDesc> parentColExprMap,
          String parentOrder, String parentNullOrder) {

    if (parentColExprMap == null) {
      return map(opKeys, opOrder, opNullOrder, parentKeys, parentOrder, parentNullOrder);
    }

    CommonKeyPrefix commonPrefix = new CommonKeyPrefix();
    int size = Stream.of(opKeys.size(), opOrder.length(), opNullOrder.length(),
            parentKeys.size(), parentColExprMap.size(), parentOrder.length(), parentNullOrder.length())
            .min(Integer::compareTo)
            .orElse(0);

    for (int i = 0; i < size; ++i) {
      ExprNodeDesc column = opKeys.get(i);
      String columnName = column.getExprString();
      ExprNodeDesc parentKey = parentKeys.get(i);
      if (parentKey != null && parentKey.isSame(parentColExprMap.get(columnName)) &&
              opOrder.charAt(i) == parentOrder.charAt(i) &&
              opNullOrder.charAt(i) == parentNullOrder.charAt(i)) {
        commonPrefix.add(parentKey, opOrder.charAt(i), opNullOrder.charAt(i));
      } else {
        return commonPrefix;
      }
    }
    return commonPrefix;
  }

  // General factory method to map two operator keys. Operator's and parent operator's {@link ExprNodeDesc}s are
  // compared using the
  // {@link ExprNodeDesc.isSame} method.
  public static CommonKeyPrefix map(
          List<ExprNodeDesc> opKeys, String opOrder, String opNullOrder,
          List<ExprNodeDesc> parentKeys,
          String parentOrder, String parentNullOrder) {

    CommonKeyPrefix commonPrefix = new CommonKeyPrefix();
    int size = Stream.of(opKeys.size(), opOrder.length(), opNullOrder.length(),
            parentKeys.size(), parentOrder.length(), parentNullOrder.length())
            .min(Integer::compareTo)
            .orElse(0);

    for (int i = 0; i < size; ++i) {
      ExprNodeDesc opKey = opKeys.get(i);
      ExprNodeDesc parentKey = parentKeys.get(i);
      if (opKey != null && opKey.isSame(parentKey) &&
              opOrder.charAt(i) == parentOrder.charAt(i) &&
              opNullOrder.charAt(i) == parentNullOrder.charAt(i)) {
        commonPrefix.add(parentKey, opOrder.charAt(i), opNullOrder.charAt(i));
      } else {
        return commonPrefix;
      }
    }
    return commonPrefix;
  }

  private List<ExprNodeDesc> mappedColumns = new ArrayList<>();
  private StringBuilder mappedOrder = new StringBuilder();
  private StringBuilder mappedNullOrder = new StringBuilder();

  private CommonKeyPrefix() {
  }

  public void add(ExprNodeDesc column, char order, char nullOrder) {
    mappedColumns.add(column);
    mappedOrder.append(order);
    mappedNullOrder.append(nullOrder);
  }

  public boolean isEmpty() {
    return mappedColumns.isEmpty();
  }

  public List<ExprNodeDesc> getMappedColumns() {
    return mappedColumns;
  }

  public String getMappedOrder() {
    return mappedOrder.toString();
  }

  public String getMappedNullOrder() {
    return mappedNullOrder.toString();
  }

  public int size() {
    return mappedColumns.size();
  }
}
