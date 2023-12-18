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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static org.apache.hadoop.hive.ql.metadata.RewriteAlgorithm.NON_CALCITE;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewUtils.extractTable;

/**
 * Traverse the plan and tries to rewrite subtrees of the plan to materialized view scans.
 *
 * The rewrite depends on whether the subtree's corresponding AST match with any materialized view
 * definitions AST.
 */
public class HiveMaterializedViewASTSubQueryRewriteShuttle extends HiveRelShuttleImpl {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewASTSubQueryRewriteShuttle.class);

  private final Map<RelNode, ASTNode> subQueryMap;
  private final ASTNode originalAST;
  private final ASTNode expandedAST;
  private final RelBuilder relBuilder;
  private final Hive db;
  private final Set<TableName> tablesUsedByOriginalPlan;
  private final HiveTxnManager txnManager;

  public HiveMaterializedViewASTSubQueryRewriteShuttle(
          Map<RelNode, ASTNode> subQueryMap,
          ASTNode originalAST,
          ASTNode expandedAST,
          RelBuilder relBuilder,
          Hive db,
          Set<TableName> tablesUsedByOriginalPlan,
          HiveTxnManager txnManager) {
    this.subQueryMap = unmodifiableMap(subQueryMap);
    this.originalAST = originalAST;
    this.expandedAST = expandedAST;
    this.relBuilder = relBuilder;
    this.db = db;
    this.tablesUsedByOriginalPlan = unmodifiableSet(tablesUsedByOriginalPlan);
    this.txnManager = txnManager;
  }

  public RelNode rewrite(RelNode relNode) {
    return relNode.accept(this);
  }

  @Override
  public RelNode visit(HiveProject project) {
    if (!subQueryMap.containsKey(project)) {
      // No AST found for this subtree
      return super.visit(project);
    }

    // The AST associated to the RelNode is part of the original AST, but we need the expanded one
    // 1. Collect the path elements of this node in the original AST
    ArrayDeque<Integer> path = new ArrayDeque<>();
    ASTNode curr = subQueryMap.get(project);
    while (curr != null && curr != originalAST) {
      path.push(curr.getType());
      curr = (ASTNode) curr.getParent();
    }

    // 2. Search for the node in the expanded tree
    ASTNode expandedSubqAST = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(expandedAST, path);
    if (expandedSubqAST == null) {
      return super.visit(project);
    }

    // 3. Lookup MV in the registry by AST subtree coming from the expanded tree.
    // Deal only with MVs which are not supported by the Calcite based rewrite algorithm since that algorithm
    // also makes cost based decisions and can produce better plans.
    RelNode match = getMaterializedViewByAST(
            expandedSubqAST, relBuilder.getCluster(), NON_CALCITE, db, tablesUsedByOriginalPlan, txnManager);
    if (match != null) {
      return match;
    }

    return super.visit(project);
  }

    @Override
  public RelNode visit(HiveFilter filter) {

    RexNode newCond = filter.getCondition().accept(new HiveMaterializedViewASTSubQueryRewriteRexShuttle(this));
    return relBuilder
            .push(filter.getInput().accept(this))
            .filter(newCond)
            .build();
  }

  /**
   * 1. Look up MVs in the registry by AST
   * 2. Check if we have the correct privilege to scan those MVs
   * 3. Validate if they are up-to-date
   */
  public static RelNode getMaterializedViewByAST(
          ASTNode expandedAST,
          RelOptCluster optCluster,
          Predicate<Set<RewriteAlgorithm>> filter,
          Hive db,
          Set<TableName> tablesUsedByOriginalPlan,
          HiveTxnManager txnManager) {
    try {
      List<HiveRelOptMaterialization> relOptMaterializationList = db.getMaterializedViewsByAST(
              expandedAST, tablesUsedByOriginalPlan, txnManager);
      for (HiveRelOptMaterialization relOptMaterialization : relOptMaterializationList) {
        if (!filter.test(relOptMaterialization.getScope())) {
          LOG.debug("Filter out materialized view {} scope {}",
                  relOptMaterialization.qualifiedTableName, relOptMaterialization.getScope());
          continue;
        }

        try {
          Table hiveTableMD = extractTable(relOptMaterialization);
          if (HiveMaterializedViewUtils.checkPrivilegeForMaterializedViews(singletonList(hiveTableMD))) {
            Set<TableName> sourceTables = new HashSet<>(1);
            sourceTables.add(hiveTableMD.getFullTableName());
            if (db.validateMaterializedViewsFromRegistry(
                    singletonList(hiveTableMD), sourceTables, txnManager)) {
              return relOptMaterialization.copyToNewCluster(optCluster).tableRel;
            }
          } else {
            LOG.debug("User does not have privilege to use materialized view {}",
                    relOptMaterialization.qualifiedTableName);
          }
        } catch (HiveException e) {
          LOG.warn("Skipping materialized view due to validation failure: " +
                  relOptMaterialization.qualifiedTableName, e);
        }
      }
    } catch (HiveException e) {
      LOG.warn(String.format("Exception while looking up materialized views for query '%s'", expandedAST), e);
    }

    return null;
  }
}
