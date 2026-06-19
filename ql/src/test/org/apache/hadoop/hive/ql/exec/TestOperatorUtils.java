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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

class TestOperatorUtils {
  @Test
  void testHasMoreGBYsReturnsFalseWhenLimitIs0() {
    // RS-SEL-LIM-FIL
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    Operator<?> limit = OperatorFactory.get(context, LimitDesc.class);
    filter.setParentOperators(singletonList(limit));
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    limit.setParentOperators(singletonList(select));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    select.setParentOperators(singletonList(rs));

    assertFalse(OperatorUtils.hasMoreOperatorsThan(filter, GroupByOperator.class, 0));
  }

  @Test
  void testHasMoreGBYsReturnsFalseWhenNoGBYInBranchAndLimitIsMoreThan0() {
    // RS-SEL-LIM-FIL
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    Operator<?> limit = OperatorFactory.get(context, LimitDesc.class);
    filter.setParentOperators(singletonList(limit));
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    limit.setParentOperators(singletonList(select));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    select.setParentOperators(singletonList(rs));

    assertFalse(OperatorUtils.hasMoreOperatorsThan(filter, GroupByOperator.class, 1));
  }

  @Test
  void testHasMoreGBYsReturnsFalseWhenNumberOfGBYIsLessThanLimit() {
    // RS-GBY-SEL-LIM-FIL
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    Operator<?> limit = OperatorFactory.get(context, LimitDesc.class);
    filter.setParentOperators(singletonList(limit));
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    limit.setParentOperators(singletonList(select));
    Operator<?> gby = OperatorFactory.get(context, GroupByDesc.class);
    select.setParentOperators(singletonList(gby));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    gby.setParentOperators(singletonList(rs));

    assertFalse(OperatorUtils.hasMoreOperatorsThan(filter, GroupByOperator.class, 1));
  }

  @Test
  void testHasMoreGBYsReturnsTrueWhenNumberOfGBYIsEqualsWithLimit() {
    // RS-GBY-FIL-SEL-GBY
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> gby1 = OperatorFactory.get(context, GroupByDesc.class);
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    gby1.setParentOperators(singletonList(select));
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    select.setParentOperators(singletonList(filter));
    Operator<?> gby2 = OperatorFactory.get(context, GroupByDesc.class);
    filter.setParentOperators(singletonList(gby2));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    gby2.setParentOperators(singletonList(rs));

    assertTrue(OperatorUtils.hasMoreOperatorsThan(gby1, GroupByOperator.class, 1));
  }

  @Test
  void testHasMoreGBYsReturnsFalseWhenNumberOfGBYIsEqualsWithLimitButHasAnRSInTheMiddle() {
    // TS-GBY-RS-SEL-GBY
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> gby1 = OperatorFactory.get(context, GroupByDesc.class);
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    gby1.setParentOperators(singletonList(select));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    select.setParentOperators(singletonList(rs));
    Operator<?> gby2 = OperatorFactory.get(context, GroupByDesc.class);
    rs.setParentOperators(singletonList(gby2));
    Operator<?> ts = OperatorFactory.get(context, TableScanDesc.class);
    gby2.setParentOperators(singletonList(ts));

    assertFalse(OperatorUtils.hasMoreOperatorsThan(gby1, GroupByOperator.class, 1));
  }

  @Test
  void testHasMoreGBYsReturnsTrueWhenBranchHasJoinAndNumberOfGBYIsEqualsWithLimit() {
    // RS-GBY-FIL--JOIN-GBY
    //     RS-SEL-/
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> gby1 = OperatorFactory.get(context, GroupByDesc.class);
    Operator<?> join = OperatorFactory.get(context, CommonMergeJoinDesc.class);
    gby1.setParentOperators(singletonList(join));

    // Join branch #1 has the second GBY
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    Operator<?> gby2 = OperatorFactory.get(context, GroupByDesc.class);
    filter.setParentOperators(singletonList(gby2));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    gby2.setParentOperators(singletonList(rs));

    // Join branch #2
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    Operator<?> rs2 = OperatorFactory.get(context, ReduceSinkDesc.class);
    select.setParentOperators(singletonList(rs2));

    join.setParentOperators(asList(filter, select));

    assertTrue(OperatorUtils.hasMoreOperatorsThan(gby1, GroupByOperator.class, 1));
  }

  @Test
  void testHasMoreGBYsReturnsFalseWhenBranchHasJoinAndBothJoinBranchesHasLessGBYThanLimit() {
    // RS-GBY-SEL--JOIN
    // RS-GBY-FIL-/
    CompilationOpContext context = new CompilationOpContext();
    Operator<?> join = OperatorFactory.get(context, CommonMergeJoinDesc.class);

    // Join branch #1
    Operator<?> filter = OperatorFactory.get(context, FilterDesc.class);
    Operator<?> gby1 = OperatorFactory.get(context, GroupByDesc.class);
    filter.setParentOperators(singletonList(gby1));
    Operator<?> rs = OperatorFactory.get(context, ReduceSinkDesc.class);
    gby1.setParentOperators(singletonList(rs));

    // Join branch #2
    Operator<?> select = OperatorFactory.get(context, SelectDesc.class);
    Operator<?> gby2 = OperatorFactory.get(context, GroupByDesc.class);
    select.setParentOperators(singletonList(gby2));
    Operator<?> rs2 = OperatorFactory.get(context, ReduceSinkDesc.class);
    gby2.setParentOperators(singletonList(rs2));

    join.setParentOperators(asList(filter, select));

    assertFalse(OperatorUtils.hasMoreOperatorsThan(join, GroupByOperator.class, 1));
  }
}