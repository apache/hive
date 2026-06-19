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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.AVAILABLE;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.INSERT_ONLY;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.IncrementalRebuildMode.NOT_AVAILABLE;

/**
 * This class is a helper to check whether a materialized view rebuild
 * can be transformed from INSERT OVERWRITE to INSERT INTO.
 *
 * We are verifying that:
 * <ul>
 *   <li>Plan only uses legal operators (i.e., Filter, Project, Join, and TableScan)</li>
 *   <li>Whether the plan has aggregate</li>
 *   <li>Whether the plan has count(*) aggregate function call</li>
 *   <li>Check whether aggregate functions are supported</li>
 * </ul>
 */
public class MaterializedViewIncrementalRewritingRelVisitor implements ReflectiveVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewIncrementalRewritingRelVisitor.class);

  private final ReflectUtil.MethodDispatcher<Result> dispatcher;

  public MaterializedViewIncrementalRewritingRelVisitor() {
    this.dispatcher = ReflectUtil.createMethodDispatcher(
        Result.class, this, "visit", RelNode.class);
  }

  /**
   * Starts an iteration.
   */
  public Result go(RelNode relNode) {
    Result result = dispatcher.invoke(relNode);
    if (result.containsAggregate) {
      return result;
    }

    if (result.incrementalRebuildMode == AVAILABLE) {
      // Incremental rebuild of non-aggregate MV is not supported when any source table has delete operations.
      return new Result(INSERT_ONLY);
    }

    return result;
  }

  public Result visit(RelNode relNode) {
    // Only TS, Filter, Join, Project and Aggregate are supported
    LOG.debug("Plan has unsupported operator {}", relNode);
    return new Result(NOT_AVAILABLE);
  }

  private Result visitChildOf(RelNode rel) {
    return visitChildOf(rel, 0);
  }

  private Result visitChildOf(RelNode rel, int index) {
    return dispatcher.invoke(rel.getInput(index));
  }

  public Result visit(HiveTableScan scan) {
    RelOptHiveTable hiveTable = (RelOptHiveTable) scan.getTable();

    Table hiveTableMD = hiveTable.getHiveTableMD();
    if (hiveTableMD.getStorageHandler() != null) {
      if (hiveTableMD.getStorageHandler().areSnapshotsSupported()) {
        // Incremental rebuild of materialized views with non-native source tables are not implemented
        // when any of the source tables has delete/update operation since the last rebuild
        LOG.debug("Table scan of non-native table {} with {} storage handler supports insert only materialized view" +
            " incremental rebuild.",
            hiveTableMD.getTableName(), hiveTableMD.getStorageHandler().getClass().getSimpleName());
        return new Result(INSERT_ONLY);
      } else {
        LOG.debug("Unsupported table type: non-native table {} with storage handler {}",
            hiveTableMD.getTableName(), hiveTableMD.getStorageHandler().getClass().getSimpleName());
        return new Result(NOT_AVAILABLE);
      }
    }

    return new Result(AVAILABLE);
  }

  public Result visit(HiveProject project) {
    return visitChildOf(project);
  }

  public Result visit(HiveFilter filter) {
    return visitChildOf(filter);
  }

  public Result visit(HiveJoin join) {
    if (join.getJoinType() != JoinRelType.INNER) {
      LOG.debug("Unsupported join type {}", join.getJoinType());
      return new Result(NOT_AVAILABLE);
    }

    Result leftResult = visitChildOf(join, 0);
    Result rightResult = visitChildOf(join, 1);

    boolean containsAggregate = leftResult.containsAggregate || rightResult.containsAggregate;
    if (leftResult.incrementalRebuildMode == NOT_AVAILABLE || rightResult.incrementalRebuildMode == NOT_AVAILABLE) {
      return new Result(NOT_AVAILABLE, containsAggregate);
    }
    if (leftResult.incrementalRebuildMode == INSERT_ONLY || rightResult.incrementalRebuildMode == INSERT_ONLY) {
      return new Result(INSERT_ONLY, containsAggregate);
    }
    return new Result(AVAILABLE, containsAggregate);
  }

  public Result visit(HiveAggregate aggregate) {
    Result result = visitChildOf(aggregate);
    if (result.incrementalRebuildMode == NOT_AVAILABLE) {
      return new Result(result.incrementalRebuildMode, true, -1);
    }

    Map<Integer, Set<SqlKind>> columnRefByAggregateCall = new HashMap<>(aggregate.getRowType().getFieldCount());

    int countStarIndex = -1;
    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      if (aggregateCall.getAggregation().getKind() == SqlKind.COUNT &&
          aggregateCall.getArgList().isEmpty() &&
          !aggregateCall.isDistinct() &&
          !aggregateCall.isApproximate()) {
        countStarIndex = i;
        continue;
      }

      for (Integer argIndex : aggregateCall.getArgList()) {
        columnRefByAggregateCall.computeIfAbsent(argIndex, integer -> new HashSet<>());
        Set<SqlKind> aggregates = columnRefByAggregateCall.get(argIndex);
        aggregates.add(aggregateCall.getAggregation().getKind());
      }
    }

    IncrementalRebuildMode incrementalRebuildMode =
        result.incrementalRebuildMode == INSERT_ONLY || countStarIndex == -1 ? INSERT_ONLY : AVAILABLE;
    LOG.debug("Initial incremental rebuild mode {} input's incremental rebuild mode {} count star index {}",
        incrementalRebuildMode, result.incrementalRebuildMode, countStarIndex);

    incrementalRebuildMode = updateBasedOnAggregates(aggregate, columnRefByAggregateCall, incrementalRebuildMode);

    return new Result(incrementalRebuildMode, true, countStarIndex);
  }

  private IncrementalRebuildMode updateBasedOnAggregates(
      HiveAggregate aggregate,
      Map<Integer, Set<SqlKind>> columnRefByAggregateCall,
      IncrementalRebuildMode incrementalRebuildMode) {

    for (int i = 0; i < aggregate.getAggCallList().size(); ++i) {
      AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
      switch (aggregateCall.getAggregation().getKind()) {
        case COUNT:
          if (aggregateCall.isDistinct() || aggregateCall.isApproximate()) {
            LOG.debug("Unsupported aggregate function COUNT with distinct {} or approximate {}",
                aggregateCall.isDistinct(), aggregateCall.isApproximate());
            return NOT_AVAILABLE;
          }

        case SUM:
        case SUM0:
          break;

        case AVG:
          Set<SqlKind> aggregates = columnRefByAggregateCall.get(aggregateCall.getArgList().get(0));
          if (!(aggregates.contains(SqlKind.SUM) && aggregates.contains(SqlKind.COUNT))) {
            // We don't check if the Count is distinct or approximate here since these are not supported currently
            // see count handling
            LOG.debug("Unsupported aggregate function AVG: missing SUM and COUNT of the same column.");
            return NOT_AVAILABLE;
          }
          break;

        case MIN:
        case MAX:
          incrementalRebuildMode = INSERT_ONLY;
          LOG.debug("Found {} aggregate function. Incremental materialized view rebuild is supported in " +
              "the presence of insert operations only", aggregateCall.getAggregation().getKind());
          break;

        default:
          LOG.debug("Unsupported aggregate function {}.", aggregateCall.getAggregation().getKind());
          return NOT_AVAILABLE;
      }
    }

    return incrementalRebuildMode;
  }

  public static final class Result {
    private final IncrementalRebuildMode incrementalRebuildMode;
    private final boolean containsAggregate;
    private final int countStarIndex;

    private Result(IncrementalRebuildMode incrementalRebuildMode) {
      this(incrementalRebuildMode, false, -1);
    }

    private Result(IncrementalRebuildMode incrementalRebuildMode, boolean containsAggregate) {
      this(incrementalRebuildMode, containsAggregate, -1);
    }

    public Result(
        IncrementalRebuildMode incrementalRebuildMode,
        boolean containsAggregate,
        int countStarIndex) {
      this.incrementalRebuildMode = incrementalRebuildMode;
      this.containsAggregate = containsAggregate;
      this.countStarIndex = countStarIndex;
    }

    public IncrementalRebuildMode getIncrementalRebuildMode() {
      return incrementalRebuildMode;
    }

    public boolean containsAggregate() {
      return containsAggregate;
    }

    public int getCountStarIndex() {
      return countStarIndex;
    }

    @Override
    public String toString() {
      return "Result{" +
          "incrementalRebuildMode=" + incrementalRebuildMode +
          ", containsAggregate=" + containsAggregate +
          ", countStarIndex=" + countStarIndex +
          '}';
    }
  }
}
