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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.MaterializationValidationResult;
import org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import java.util.EnumSet;

import static org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm.TEXT;

/**
 * Checks the query plan for conditions that would make the plan unsuitable for
 * materialized views or query caching:
 * - References to temporary or external tables
 * - References to non-deterministic functions.
 */
public class HiveRelOptMaterializationValidator extends HiveRelShuttleImpl {
  private static final String UNSUPPORTED_BY_CALCITE_FORMAT =
      "Only query text based automatic rewriting is available for materialized view. " +
          "Statement has unsupported %s: %s.";

  protected String resultCacheInvalidReason;
  protected MaterializationValidationResult materializationValidationResult;

  public void validate(RelNode relNode) {
    try {
      materializationValidationResult = new MaterializationValidationResult(RewriteAlgorithm.ALL, "");
      relNode.accept(this);
    } catch (Util.FoundOne e) {
      // Can ignore - the check failed.
    }
  }

  @Override
  public RelNode visit(TableScan scan) {
    // TableScan of a non-Hive table - don't support for materializations.
    fail(scan.getTable().getQualifiedName() + " is a table scan of a non-Hive table.");
    return scan;
  }

  @Override
  public RelNode visit(HiveTableScan hiveScan) {
    RelOptHiveTable relOptHiveTable = (RelOptHiveTable) hiveScan.getTable();
    Table tab = relOptHiveTable.getHiveTableMD();
    if (tab.isTemporary()) {
      fail(tab.getTableName() + " is a temporary table");
    }
    if (tab.getTableType() == TableType.EXTERNAL_TABLE &&
        !(tab.getStorageHandler() != null && tab.getStorageHandler().areSnapshotsSupported())) {
      fail(tab.getFullyQualifiedName() + " is an external table and does not support snapshots");
    }
    return hiveScan;
  }

  @Override
  public RelNode visit(HiveProject project) {
    for (RexNode expr : project.getProjects()) {
      checkExpr(expr);
    }
    return super.visit(project);
  }

  @Override
  public RelNode visit(HiveFilter filter) {
    checkExpr(filter.getCondition());
    return super.visit(filter);
  }

  @Override
  public RelNode visit(HiveJoin join) {
    if (join.getJoinType() != JoinRelType.INNER) {
      unsupportedByCalciteRewrite("join type", join.getJoinType().toString());
    }
    checkExpr(join.getCondition());
    return super.visit(join);
  }

  @Override
  public RelNode visit(HiveAggregate aggregate) {
    return super.visit(aggregate);
  }

  @Override
  public RelNode visit(RelNode node) {
    // There are several Hive RelNode types which do not have their own visit() method
    // defined in the HiveRelShuttle interface, which need to be handled appropriately here.
    // Per jcamachorodriguez we should not encounter HiveMultiJoin/HiveSortExchange
    // during these checks, so no need to add those here.
    if (node instanceof HiveUnion) {
      return visit((HiveUnion) node);
    } else if (node instanceof HiveSortLimit) {
      return visit((HiveSortLimit) node);
    } else if (node instanceof HiveSortExchange) {
      return visit((HiveSortExchange) node);
    } else if (node instanceof HiveSemiJoin) {
      return visit((HiveSemiJoin) node);
    } else if (node instanceof HiveExcept) {
      return visit((HiveExcept) node);
    } else if (node instanceof HiveAntiJoin) {
      return visit((HiveAntiJoin) node);
    } else if (node instanceof HiveIntersect) {
      return visit((HiveIntersect) node);
    }

    // Fall-back for an unexpected RelNode type
    return fail(node);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    unsupportedByCalciteRewrite("expression", "window function");
    checkExpr(scan.getCall());
    return super.visit(scan);
  }

  @Override
  public RelNode visit(LogicalValues values) {
    // Not expected to be encountered for Hive - fail
    return fail(values);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    // Not expected to be encountered for Hive - fail
    return fail(filter);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    // Not expected to be encountered for Hive - fail
    return fail(project);
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    // Not expected to be encountered for Hive - fail
    return fail(join);
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    // Not expected to be encountered for Hive - fail
    return fail(correlate);
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    // Not expected to be encountered for Hive - fail
    return fail(union);
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    // Not expected to be encountered for Hive - fail
    return fail(intersect);
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    // Not expected to be encountered for Hive - fail
    return fail(minus);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    // Not expected to be encountered for Hive - fail
    return fail(aggregate);
  }

  @Override
  public RelNode visit(LogicalMatch match) {
    // Not expected to be encountered for Hive - fail
    return fail(match);
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    // Not expected to be encountered for Hive - fail
    return fail(sort);
  }

  @Override
  public RelNode visit(LogicalExchange exchange) {
    // Not expected to be encountered for Hive - fail
    return fail(exchange);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveUnion union) {
    unsupportedByCalciteRewrite("operator", "union");
    return visitChildren(union);
  }

  @Override
  public RelNode visit(HiveSortLimit sort) {
    unsupportedByCalciteRewrite("clause","order by");
    checkExpr(sort.getFetchExpr());
    checkExpr(sort.getOffsetExpr());
    return visitChildren(sort);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveSortExchange sort) {
    unsupportedByCalciteRewrite("clause", "sort by");
    return visitChildren(sort);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveSemiJoin semiJoin) {
    unsupportedByCalciteRewrite("join type", "semi join");
    checkExpr(semiJoin.getCondition());
    checkExpr(semiJoin.getJoinFilter());
    return visitChildren(semiJoin);
  }

  private RelNode visit(HiveAntiJoin antiJoin) {
    unsupportedByCalciteRewrite("join type", "anti join");
    checkExpr(antiJoin.getCondition());
    checkExpr(antiJoin.getJoinFilter());
    return visitChildren(antiJoin);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveExcept except) {
    unsupportedByCalciteRewrite("operator", "except");
    return visitChildren(except);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveIntersect intersect) {
    unsupportedByCalciteRewrite("operator", "intersect");
    return visitChildren(intersect);
  }

  private void fail(String reason) {
    setResultCacheInvalidReason(reason);
    this.materializationValidationResult = new MaterializationValidationResult(
        EnumSet.noneOf(RewriteAlgorithm.class), "Cannot enable automatic rewriting for materialized view. " + reason);
    throw Util.FoundOne.NULL;
  }

  private RelNode fail(RelNode node) {
    setResultCacheInvalidReason("Unsupported RelNode type " + node.getRelTypeName() +
        " encountered in the query plan");
    this.materializationValidationResult =
        new MaterializationValidationResult(EnumSet.noneOf(RewriteAlgorithm.class),
            String.format("Cannot enable automatic rewriting for materialized view. " +
                "Unsupported RelNode type %s encountered in the query plan", node.getRelTypeName()));
    throw Util.FoundOne.NULL;
  }

  private void checkExpr(RexNode expr) {
    RexCall invalidCall = HiveCalciteUtil.checkMaterializable(expr);
    if (invalidCall != null) {
      fail(invalidCall.getOperator().getName() + " is not a deterministic function");
    }
  }

  public String getResultCacheInvalidReason() {
    return resultCacheInvalidReason;
  }

  public void setResultCacheInvalidReason(String resultCacheInvalidReason) {
    this.resultCacheInvalidReason = resultCacheInvalidReason;
  }

  public boolean isValidForQueryCaching() {
    return resultCacheInvalidReason == null;
  }

  public MaterializationValidationResult getAutomaticRewritingValidationResult() {
    return materializationValidationResult;
  }

  public void unsupportedByCalciteRewrite(String sqlPartType, String sqlPart) {
    if (isValidForAutomaticRewriting()) {
      String errorMessage = String.format(UNSUPPORTED_BY_CALCITE_FORMAT, sqlPartType, sqlPart);
      this.materializationValidationResult =
          new MaterializationValidationResult(EnumSet.of(TEXT), errorMessage);
    }
  }

  public boolean isValidForAutomaticRewriting() {
    return RewriteAlgorithm.ALL.equals(materializationValidationResult.getSupportedRewriteAlgorithms());
  }
}
