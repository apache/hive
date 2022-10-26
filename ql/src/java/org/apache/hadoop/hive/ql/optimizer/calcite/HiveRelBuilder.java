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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.Iterables;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeableAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumEmptyIsZeroAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Builder for relational expressions in Hive.
 *
 * <p>{@code RelBuilder} does not make possible anything that you could not
 * also accomplish by calling the factory methods of the particular relational
 * expression. But it makes common tasks more straightforward and concise.
 *
 * <p>It is not thread-safe.
 */
public class HiveRelBuilder extends RelBuilder {

  private HiveRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  /** Creates a RelBuilder. */
  public static RelBuilder create(FrameworkConfig config) {
    final RelOptCluster[] clusters = {null};
    final RelOptSchema[] relOptSchemas = {null};
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>(config) {
          @Override
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, CalciteServerStatement statement) {
            clusters[0] = cluster;
            relOptSchemas[0] = relOptSchema;
            return null;
          }
        });
    return new HiveRelBuilder(config.getContext(), clusters[0], relOptSchemas[0]);
  }

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return new RelBuilderFactory() {
      @Override
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        Context confContext = Contexts.of(Config.DEFAULT.withPruneInputOfAggregate(Bug.CALCITE_4513_FIXED));
        return new HiveRelBuilder(Contexts.chain(context, confContext), cluster, schema);
      }
    };
  }

  /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
  public static RelBuilderFactory proto(Object... factories) {
    return proto(Contexts.of(factories));
  }

  @Override
  public RelBuilder filter(Iterable<? extends RexNode> predicates) {
    final RexNode x = RexUtil.composeConjunction(
        cluster.getRexBuilder(), predicates, false);
    if (!x.isAlwaysTrue()) {
      final RelNode input = build();
      final RelNode filter = HiveRelFactories.HIVE_FILTER_FACTORY.createFilter(input, x);
      return this.push(filter);
    }
    return this;
  }

  public static SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
    switch (flag) {
      case YEAR:
        return HiveFloorDate.YEAR;
      case QUARTER:
        return HiveFloorDate.QUARTER;
      case MONTH:
        return HiveFloorDate.MONTH;
      case DAY:
        return HiveFloorDate.DAY;
      case HOUR:
        return HiveFloorDate.HOUR;
      case MINUTE:
        return HiveFloorDate.MINUTE;
      case SECOND:
        return HiveFloorDate.SECOND;
    }
    return SqlStdOperatorTable.FLOOR;
  }

  public static SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation instanceof HiveMergeableAggregate) {
      HiveMergeableAggregate mAgg = (HiveMergeableAggregate) aggregation;
      return mAgg.getMergeAggFunction();
    }
    if (aggregation instanceof HiveSqlSumAggFunction
        || aggregation instanceof HiveSqlMinMaxAggFunction
        || aggregation instanceof HiveSqlSumEmptyIsZeroAggFunction) {
      return aggregation;
    }
    if (aggregation instanceof HiveSqlCountAggFunction) {
      HiveSqlCountAggFunction countAgg = (HiveSqlCountAggFunction) aggregation;
      return new HiveSqlSumEmptyIsZeroAggFunction(countAgg.isDistinct(), countAgg.getReturnTypeInference(),
          countAgg.getOperandTypeInference(), countAgg.getOperandTypeChecker());
    }
    return null;
  }

  /** Creates a {@link Join} with correlating variables. */
  @Override
  public RelBuilder join(JoinRelType joinType, RexNode condition,
      Set<CorrelationId> variablesSet) {
    if (Bug.CALCITE_4574_FIXED) {
      throw new IllegalStateException("Method overriding should be removed once CALCITE-4574 is fixed");
    }
    RelNode right = this.peek(0);
    RelNode left = this.peek(1);
    final boolean correlate = variablesSet.size() == 1;
    RexNode postCondition = literal(true);
    if (correlate) {
      final CorrelationId id = Iterables.getOnlyElement(variablesSet);
      if (!RelOptUtil.notContainsCorrelation(left, id, Litmus.IGNORE)) {
        throw new IllegalArgumentException("variable " + id
            + " must not be used by left input to correlation");
      }
      // Correlate does not have an ON clause.
      switch (joinType) {
      case LEFT:
      case SEMI:
      case ANTI:
        // For a LEFT/SEMI/ANTI, predicate must be evaluated first.
        filter(condition.accept(new Shifter(left, id, right)));
        right = this.peek(0);
        break;
      case INNER:
        // For INNER, we can defer.
        postCondition = condition;
        break;
      default:
        throw new IllegalArgumentException("Correlated " + joinType + " join is not supported");
      }
      final ImmutableBitSet requiredColumns = RelOptUtil.correlationColumns(id, right);
      List<RexNode> leftFields = this.fields(2, 0);
      List<RexNode> requiredFields = new ArrayList<>();
      for (int i = 0; i < leftFields.size(); i++) {
        if (requiredColumns.get(i)) {
          requiredFields.add(leftFields.get(i));
        }
      }
      correlate(joinType, id, requiredFields);
      filter(postCondition);
    } else {
      // When there is no correlation use the default logic which works OK for now
      // Cannot copy-paste the respective code here cause we don't have access to stack,
      // Frame etc. and we might lose existing aliases in the builder
      assert variablesSet.isEmpty();
      super.join(joinType,condition, variablesSet);
    }
    return this;
  }

  /** Shuttle that shifts a predicate's inputs to the left, replacing early
   * ones with references to a
   * {@link RexCorrelVariable}. */
  private class Shifter extends RexShuttle {
    private final RelNode left;
    private final CorrelationId id;
    private final RelNode right;

    Shifter(RelNode left, CorrelationId id, RelNode right) {
      this.left = left;
      this.id = id;
      this.right = right;
      if (Bug.CALCITE_4574_FIXED) {
        throw new IllegalStateException("Class should be redundant once CALCITE-4574 is fixed");
      }
    }

    public RexNode visitInputRef(RexInputRef inputRef) {
      final RelDataType leftRowType = left.getRowType();
      final RexBuilder rexBuilder = getRexBuilder();
      final int leftCount = leftRowType.getFieldCount();
      if (inputRef.getIndex() < leftCount) {
        final RexNode v = rexBuilder.makeCorrel(leftRowType, id);
        return rexBuilder.makeFieldAccess(v, inputRef.getIndex());
      } else {
        return rexBuilder.makeInputRef(right, inputRef.getIndex() - leftCount);
      }
    }
  }

}
