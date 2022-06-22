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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

/**
 * Rule that adds sorting to GROUP BY col0 LIMIT n in presence of aggregate functions.
 * Ex.: SELECT id, count(1) FROM t_table GROUP BY id LIMIT 2
 *
 * Above query has a physical plan like Reducer 2 &lt;- Map 1 (SIMPLE_EDGE)
 * Both mapper and reducer edges may have multiple Mapper and Reducer instances to enable parallel process of data.
 * Aggregate function results are calculated in two steps:
 * 1) first mappers calculate a partial result from the rows processed by each instance.
 *    The result is going to be filtered by Top N optimization in the mappers.
 * 2) In the second step reducers aggregate the partial results coming from the mappers. However, some of partial
 *    results are filtered out by Top N optimization.
 * Each reducer generates an output file and in the last stage Fetch Operator choose one af the to be the result of the
 * query. In these result files only the first n row has correct aggregation results the ones which has a key value
 * falls in the top n key.
 *
 * In order to get correct aggregation results this rule adds sorting to the HiveSortLimit above the HiveAggregate
 * which enables hive to sort merge the results of the reducers and take the first n rows of the merged result.
 *
 * from:
 * HiveSortLimit(fetch=[2])
 *   HiveAggregate(group=[{0}], agg#0=[count()])
 *
 * to:
 * HiveSortLimit(sort0=[$0], dir0=[ASC], fetch=[2])
 *   HiveAggregate(group=[{0}], agg#0=[count()])
 */
public class HiveAggregateSortLimitRule extends RelOptRule {

  private final RelFieldCollation.NullDirection defaultAscNullDirection;

  public HiveAggregateSortLimitRule(boolean nullsLast) {
    super(operand(HiveSortLimit.class, operand(HiveAggregate.class, any())),
            HiveRelFactories.HIVE_BUILDER, "HiveAggregateSortRule");
    this.defaultAscNullDirection =
        nullsLast ? RelFieldCollation.NullDirection.LAST : RelFieldCollation.NullDirection.FIRST;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);
    final HiveAggregate aggregate = call.rel(1);
    final RelBuilder relBuilder = call.builder();

    if (sortLimit.getFetchExpr() == null && sortLimit.getOffsetExpr() == null) {
      // No limit, offset -> No Top N -> all rows are forwarded from all RS to Reducers
      return;
    }

    if (aggregate.getAggCallList().isEmpty()) {
      // No aggregate functions, any Group By key can be in the final result
      return;
    }

    if (!sortLimit.getSortExps().isEmpty()) {
      // Sort keys already present
      return;
    }

    ImmutableList.Builder<RelFieldCollation> newSortKeyBuilder = ImmutableList.builder();
    for (int i : aggregate.getGroupSet()) {
      RelFieldCollation fieldCollation =
              new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING, defaultAscNullDirection);
      newSortKeyBuilder.add(fieldCollation);
    }

    HiveRelCollation newCollation = new HiveRelCollation(newSortKeyBuilder.build());
    HiveSortLimit newSortLimit = sortLimit.copy(sortLimit.getTraitSet(),
            aggregate, newCollation, sortLimit.offset, sortLimit.fetch);

    call.transformTo(newSortLimit);
  }
}
