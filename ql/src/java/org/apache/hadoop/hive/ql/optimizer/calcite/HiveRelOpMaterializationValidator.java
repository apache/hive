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

package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
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

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExcept;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIntersect;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the query plan for conditions that would make the plan unsuitable for
 * materialized views or query caching:
 * - References to temporary or external tables
 * - References to non-determinisitc functions.
 */
public class HiveRelOpMaterializationValidator extends HiveRelShuttleImpl {
  static final Logger LOG = LoggerFactory.getLogger(HiveRelOpMaterializationValidator.class);

  protected String invalidMaterializationReason;

  public void validateQueryMaterialization(RelNode relNode) {
    try {
      relNode.accept(this);
    } catch (Util.FoundOne e) {
      // Can ignore - the check failed.
    }
  }

  @Override
  public RelNode visit(TableScan scan) {
    if (scan instanceof HiveTableScan) {
      HiveTableScan hiveScan = (HiveTableScan) scan;
      RelOptHiveTable relOptHiveTable = (RelOptHiveTable) hiveScan.getTable();
      Table tab = relOptHiveTable.getHiveTableMD();
      if (tab.isTemporary()) {
        fail(tab.getTableName() + " is a temporary table");
      }
      TableType tt = tab.getTableType();
      if (tab.getTableType() == TableType.EXTERNAL_TABLE) {
        fail(tab.getFullyQualifiedName() + " is an external table");
      }
      return scan;
    }

    // TableScan of a non-Hive table - don't support for materializations.
    fail(scan.getTable().getQualifiedName() + " is a table scan of a non-Hive table.");
    return scan;
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
    checkExpr(join.getCondition());
    return super.visit(join);
  }

  @Override
  public RelNode visit(HiveAggregate aggregate) {
    // Is there anything to check here?
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
    } else if (node instanceof HiveSemiJoin) {
      return visit((HiveSemiJoin) node);
    } else if (node instanceof HiveExcept) {
      return visit((HiveExcept) node);
    } else if (node instanceof HiveIntersect) {
      return visit((HiveIntersect) node);
    }

    // Fall-back for an unexpected RelNode type
    return fail(node);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
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
    return visitChildren(union);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveSortLimit sort) {
    checkExpr(sort.getFetchExpr());
    checkExpr(sort.getOffsetExpr());
    return visitChildren(sort);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveSemiJoin semiJoin) {
    checkExpr(semiJoin.getCondition());
    checkExpr(semiJoin.getJoinFilter());
    return visitChildren(semiJoin);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveExcept except) {
    return visitChildren(except);
  }

  // Note: Not currently part of the HiveRelNode interface
  private RelNode visit(HiveIntersect intersect) {
    return visitChildren(intersect);
  }

  private void fail(String reason) {
    setInvalidMaterializationReason(reason);
    throw Util.FoundOne.NULL;
  }

  private RelNode fail(RelNode node) {
    setInvalidMaterializationReason("Unsupported RelNode type " + node.getRelTypeName() +
        " encountered in the query plan");
    throw Util.FoundOne.NULL;
  }

  private void checkExpr(RexNode expr) {
    RexCall invalidCall = HiveCalciteUtil.checkMaterializable(expr);
    if (invalidCall != null) {
      fail(invalidCall.getOperator().getName() + " is not a deterministic function");
    }
  }

  public String getInvalidMaterializationReason() {
    return invalidMaterializationReason;
  }

  public void setInvalidMaterializationReason(String invalidMaterializationReason) {
    this.invalidMaterializationReason = invalidMaterializationReason;
  }

  public boolean isValidMaterialization() {
    return invalidMaterializationReason == null;
  }
}
