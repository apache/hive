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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveRelOptMaterialization;
import org.apache.hadoop.hive.common.MaterializationSnapshot;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.MaterializedViewMetadata;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.DruidSqlOperatorConverter;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.TxnIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveMaterializedViewUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewUtils.class);


  private HiveMaterializedViewUtils() {}

  public static Table extractTable(RelOptMaterialization materialization) {
    RelOptHiveTable cachedMaterializedViewTable;
    if (materialization.tableRel instanceof Project) {
      // There is a Project on top (due to nullability)
      cachedMaterializedViewTable = (RelOptHiveTable) materialization.tableRel.getInput(0).getTable();
    } else {
      cachedMaterializedViewTable = (RelOptHiveTable) materialization.tableRel.getTable();
    }
    return cachedMaterializedViewTable.getHiveTableMD();
  }

  /**
   * Utility method that returns whether a materialized view is outdated (true), not outdated
   * (false), or it cannot be determined (null). The latest case may happen e.g. when the
   * materialized view definition uses external tables.
   */
  public static Boolean isOutdatedMaterializedView(
          String validTxnsList, HiveTxnManager txnMgr, Hive db,
          Set<TableName> tablesUsed, Table materializedViewTable) throws HiveException {

    MaterializedViewMetadata mvMetadata = materializedViewTable.getMVMetadata();
    MaterializationSnapshot snapshot = mvMetadata.getSnapshot();

    if (snapshot != null && snapshot.getTableSnapshots() != null && !snapshot.getTableSnapshots().isEmpty()) {
      return isOutdatedMaterializedView(snapshot, db, tablesUsed, materializedViewTable);
    }

    String materializationTxnList = snapshot != null ? snapshot.getValidTxnList() : null;
    return isOutdatedMaterializedView(
        materializationTxnList, validTxnsList, txnMgr, tablesUsed, materializedViewTable);
  }

  private static Boolean isOutdatedMaterializedView(
      String materializationTxnList, String validTxnsList, HiveTxnManager txnMgr,
      Set<TableName> tablesUsed, Table materializedViewTable) throws LockException {
    List<String> tablesUsedNames = tablesUsed.stream()
        .map(tableName -> TableName.getDbTable(tableName.getDb(), tableName.getTable()))
        .collect(Collectors.toList());
    ValidTxnWriteIdList currentTxnWriteIds = txnMgr.getValidWriteIds(tablesUsedNames, validTxnsList);
    if (currentTxnWriteIds == null) {
      LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
              " ignored for rewriting as we could not obtain current txn ids");
      return null;
    }

    Set<String> storedTablesUsed = materializedViewTable.getMVMetadata().getSourceTableFullNames();
    if (materializationTxnList == null || materializationTxnList.isEmpty()) {
      LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
              " ignored for rewriting as we could not obtain materialization txn ids");
      return null;
    }
    ValidTxnWriteIdList materializationTxnWriteIds = new ValidTxnWriteIdList(materializationTxnList);
    boolean ignore = false;
    for (String fullyQualifiedTableName : tablesUsedNames) {
      // Note. If the materialized view does not contain a table that is contained in the query,
      // we do not need to check whether that specific table is outdated or not. If a rewriting
      // is produced in those cases, it is because that additional table is joined with the
      // existing tables with an append-columns only join, i.e., PK-FK + not null.
      if (!storedTablesUsed.contains(fullyQualifiedTableName)) {
        continue;
      }
      ValidWriteIdList tableCurrentWriteIds = currentTxnWriteIds.getTableValidWriteIdList(fullyQualifiedTableName);
      if (tableCurrentWriteIds == null) {
        // Uses non-transactional table, cannot be considered
        LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as it is outdated and cannot be considered for " +
                " rewriting because it uses non-transactional table " + fullyQualifiedTableName);
        ignore = true;
        break;
      }
      ValidWriteIdList tableWriteIds = materializationTxnWriteIds.getTableValidWriteIdList(fullyQualifiedTableName);
      if (tableWriteIds == null) {
        // This should not happen, but we ignore for safety
        LOG.warn("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " ignored for rewriting as details about txn ids for table " + fullyQualifiedTableName +
                " could not be found in " + materializationTxnWriteIds);
        ignore = true;
        break;
      }
      if (!TxnIdUtils.checkEquivalentWriteIds(tableCurrentWriteIds, tableWriteIds)) {
        LOG.debug("Materialized view " + materializedViewTable.getFullyQualifiedName() +
                " contents are outdated");
        return true;
      }
    }
    if (ignore) {
      return null;
    }

    return false;
  }

  private static Boolean isOutdatedMaterializedView(
          MaterializationSnapshot snapshot, Hive db,
          Set<TableName> tablesUsed, Table materializedViewTable) throws HiveException {
    List<String> tablesUsedNames = tablesUsed.stream()
        .map(tableName -> TableName.getDbTable(tableName.getDb(), tableName.getTable()))
        .collect(Collectors.toList());

    Map<String, SnapshotContext> snapshotMap = snapshot.getTableSnapshots();
    if (snapshotMap == null || snapshotMap.isEmpty()) {
      LOG.debug("Materialized view {} ignored for rewriting as we could not obtain current snapshot ids",
              materializedViewTable.getFullyQualifiedName());
      return null;
    }

    Set<String> storedTablesUsed = materializedViewTable.getMVMetadata().getSourceTableFullNames();
    for (String fullyQualifiedTableName : tablesUsedNames) {
      // Note. If the materialized view does not contain a table that is contained in the query,
      // we do not need to check whether that specific table is outdated or not. If a rewriting
      // is produced in those cases, it is because that additional table is joined with the
      // existing tables with an append-columns only join, i.e., PK-FK + not null.
      if (!storedTablesUsed.contains(fullyQualifiedTableName)) {
        continue;
      }

      Table table = db.getTable(fullyQualifiedTableName);
      if (table.getStorageHandler() == null) {
        LOG.debug("Materialized view {} ignored for rewriting as we could not get storage handler of table {}",
                materializedViewTable.getFullyQualifiedName(), fullyQualifiedTableName);
        return null;
      }
      if (!table.getStorageHandler().areSnapshotsSupported()) {
        LOG.debug("Materialized view {} ignored for rewriting as storage handler of table {} " +
                        "does not support snapshots.",
                materializedViewTable.getFullyQualifiedName(), fullyQualifiedTableName);
        return null;
      }
      SnapshotContext currentTableSnapshot = table.getStorageHandler().getCurrentSnapshotContext(table);
      SnapshotContext storedTableSnapshot = snapshotMap.get(fullyQualifiedTableName);
      if (!Objects.equals(currentTableSnapshot, storedTableSnapshot)) {
        LOG.debug("Materialized view {} contents are outdated", materializedViewTable.getFullyQualifiedName());
        return true;
      }
    }

    return false;
  }

  /**
   * Method to enrich the materialization query contained in the input with
   * its invalidation.
   */
  public static HiveRelOptMaterialization augmentMaterializationWithTimeInformation(
      HiveRelOptMaterialization materialization, String validTxnsList,
      MaterializationSnapshot snapshot) throws LockException {

    RelNode modifiedQueryRel;
    if (snapshot != null && snapshot.getTableSnapshots() != null && !snapshot.getTableSnapshots().isEmpty()) {
      modifiedQueryRel = applyRule(
              materialization.queryRel, HiveAugmentSnapshotMaterializationRule.with(snapshot.getTableSnapshots()));
    } else {
      String materializationTxnList = snapshot != null ? snapshot.getValidTxnList() : null;
      modifiedQueryRel = augmentMaterializationWithTimeInformation(
              materialization, validTxnsList, new ValidTxnWriteIdList(materializationTxnList));
    }

    return new HiveRelOptMaterialization(materialization.tableRel, modifiedQueryRel,
            null, materialization.qualifiedTableName, materialization.getScope(), materialization.getRebuildMode(),
            materialization.getAst());
  }

  /**
   * Method to enrich the materialization query contained in the input with
   * its invalidation when materialization has native acid source tables.
   */
  private static RelNode augmentMaterializationWithTimeInformation(
      HiveRelOptMaterialization materialization, String validTxnsList,
      ValidTxnWriteIdList materializationTxnList) throws LockException {
    // Extract tables used by the query which will in turn be used to generate
    // the corresponding txn write ids
    List<String> tablesUsed = new ArrayList<>();
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
          TableScan ts = (TableScan) node;
          tablesUsed.add(((RelOptHiveTable) ts.getTable()).getHiveTableMD().getFullyQualifiedName());
        }
        super.visit(node, ordinal, parent);
      }
    }.go(materialization.queryRel);
    ValidTxnWriteIdList currentTxnList =
        SessionState.get().getTxnMgr().getValidWriteIds(tablesUsed, validTxnsList);
    // Augment
    final RexBuilder rexBuilder = materialization.queryRel.getCluster().getRexBuilder();
    return applyRule(
            materialization.queryRel, new HiveAugmentMaterializationRule(rexBuilder, currentTxnList, materializationTxnList));
  }

  /**
   * Method to apply a rule to a query plan.
   */
  @VisibleForTesting
  static RelNode applyRule(
          RelNode basePlan, RelOptRule relOptRule) {
    final HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(relOptRule);
    final HepPlanner planner = new HepPlanner(
        programBuilder.build());
    planner.setRoot(basePlan);
    return planner.findBestExp();
  }

  /**
   * If a materialization does not contain grouping sets, it returns the materialization
   * itself. Otherwise, it will create one materialization for each grouping set.
   * For each grouping set, the query for the materialization will consist of the group by
   * columns in the grouping set, followed by a projection to recreate the literal null
   * values. The view scan will consist of the scan over the materialization followed by a
   * filter on the grouping id value corresponding to that grouping set.
   */
  public static List<HiveRelOptMaterialization> deriveGroupingSetsMaterializedViews(
      HiveRelOptMaterialization materialization) {
    final RelNode query = materialization.queryRel;
    final Project project;
    final Aggregate aggregate;
    if (query instanceof Aggregate) {
      project = null;
      aggregate = (Aggregate) query;
    } else if (query instanceof Project && query.getInput(0) instanceof Aggregate) {
      project = (Project) query;
      aggregate = (Aggregate) query.getInput(0);
    } else {
      project = null;
      aggregate = null;
    }
    if (aggregate == null) {
      // Not an aggregate materialized view, return original materialization
      return Collections.singletonList(materialization);
    }
    if (aggregate.getGroupType() == Group.SIMPLE) {
      // Not a grouping sets materialized view, return original materialization
      return Collections.singletonList(materialization);
    }
    int aggregateGroupingIdIndex = -1;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      if (aggregate.getAggCallList().get(i).getAggregation() == HiveGroupingID.INSTANCE) {
        aggregateGroupingIdIndex = aggregate.getGroupCount() + i;
        break;
      }
    }
    Preconditions.checkState(aggregateGroupingIdIndex != -1);
    int projectGroupingIdIndex = -1;
    if (project != null) {
      for (int i = 0; i < project.getProjects().size(); i++) {
        RexNode expr = project.getProjects().get(i);
        if (expr instanceof RexInputRef) {
          RexInputRef ref = (RexInputRef) expr;
          if (ref.getIndex() == aggregateGroupingIdIndex) {
            // Grouping id is present
            projectGroupingIdIndex = i;
            break;
          }
        }
      }
      if (projectGroupingIdIndex == -1) {
        // Grouping id is not present, return original materialization
        return Collections.singletonList(materialization);
      }
    }
    // Create multiple materializations
    final List<HiveRelOptMaterialization> materializationList = new ArrayList<>();
    final RelBuilder builder = HiveRelFactories.HIVE_BUILDER.create(aggregate.getCluster(), null);
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<AggregateCall> aggregateCalls = new ArrayList<>(aggregate.getAggCallList());
    aggregateCalls.remove(aggregateGroupingIdIndex - aggregate.getGroupCount());
    for (ImmutableBitSet targetGroupSet : aggregate.getGroupSets()) {
      // Compute the grouping id value
      long groupingIdValue = convert(targetGroupSet, aggregate.getGroupSet());
      // First we modify the MV query
      Aggregate newAggregate = aggregate.copy(
          aggregate.getTraitSet(), aggregate.getInput(), targetGroupSet,
          null, aggregateCalls);
      builder.push(newAggregate);
      List<RexNode> exprs = new ArrayList<>();
      for (int pos = 0; pos < aggregate.getGroupCount(); pos++) {
        int ref = aggregate.getGroupSet().nth(pos);
        if (targetGroupSet.get(ref)) {
          exprs.add(
              rexBuilder.makeInputRef(
                  newAggregate, targetGroupSet.indexOf(ref)));
        } else {
          exprs.add(
              rexBuilder.makeNullLiteral(
                  aggregate.getRowType().getFieldList().get(pos).getType()));
        }
      }
      int pos = targetGroupSet.cardinality();
      for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
        if (aggregateCall.getAggregation() == HiveGroupingID.INSTANCE) {
          exprs.add(
              rexBuilder.makeBigintLiteral(new BigDecimal(groupingIdValue)));
        } else {
          exprs.add(
              rexBuilder.makeInputRef(newAggregate, pos++));
        }
      }
      if (project != null) {
        // Include projections from top operator
        Project bottomProject = (Project) builder
            .project(exprs, ImmutableList.of(), true)
            .build();
        List<RexNode> newNodes =
            RelOptUtil.pushPastProject(project.getProjects(), bottomProject);
        builder.push(bottomProject.getInput())
            .project(newNodes);
      } else {
        builder.project(exprs);
      }
      final RelNode newQueryRel = builder.build();
      // Second we modify the MV scan
      builder.push(materialization.tableRel);
      RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(materialization.tableRel,
              project != null ? projectGroupingIdIndex : aggregateGroupingIdIndex),
          rexBuilder.makeBigintLiteral(new BigDecimal(groupingIdValue)));
      builder.filter(condition);
      final RelNode newTableRel = builder.build();
      final Table scanTable = extractTable(materialization);
      materializationList.add(
          new HiveRelOptMaterialization(newTableRel, newQueryRel, null,
              ImmutableList.of(scanTable.getDbName(), scanTable.getTableName(),
                  "#" + materializationList.size()), materialization.getScope(), materialization.getRebuildMode(),
                  materialization.getAst()));
    }
    return materializationList;
  }

  /**
   * Computes the results of the grouping function given the grouping set and the
   * group by columns.
   */
  private static long convert(ImmutableBitSet targetGroupSet, ImmutableBitSet groupSet) {
    long value = 0L;
    for (int i = 0; i < groupSet.length(); ++i) {
      int bit = groupSet.nth(i);
      value += targetGroupSet.get(bit) ? 0L : (1L << groupSet.length() - i - 1);
    }
    return value;
  }

  /**
   * Method that will recreate the plan rooted at node using the cluster given
   * as a parameter.
   */
  public static RelNode copyNodeNewCluster(RelOptCluster optCluster, RelNode node) {
    if (node instanceof Filter) {
      final Filter f = (Filter) node;
      return new HiveFilter(optCluster, f.getTraitSet(),
          copyNodeNewCluster(optCluster, f.getInput()), f.getCondition());
    } else if (node instanceof Project) {
      final Project p = (Project) node;
      return HiveProject.create(optCluster, copyNodeNewCluster(optCluster, p.getInput()),
          p.getProjects(), p.getRowType(), Collections.emptyList());
    } else {
      return copyNodeScanNewCluster(optCluster, node);
    }
  }

  /**
   * Validate if given materialized view has SELECT privileges for current user
   * @param cachedMVTableList
   * @return false if user does not have privilege otherwise true
   * @throws HiveException
   */
  public static boolean checkPrivilegeForMaterializedViews(List<Table> cachedMVTableList) throws HiveException {
    List<HivePrivilegeObject> privObjects = new ArrayList<HivePrivilegeObject>();

    for (Table cachedMVTable:cachedMVTableList) {
      List<String> colNames =
          cachedMVTable.getAllCols().stream()
              .map(FieldSchema::getName)
              .collect(Collectors.toList());

      HivePrivilegeObject privObject = new HivePrivilegeObject(cachedMVTable.getDbName(),
          cachedMVTable.getTableName(), colNames);
      privObjects.add(privObject);
    }

    try {
      SessionState.get().getAuthorizerV2().
          checkPrivileges(HiveOperationType.QUERY, privObjects, privObjects, new HiveAuthzContext.Builder().build());
    } catch (HiveException e) {
      if (e instanceof HiveAccessControlException) {
        return false;
      }
      throw e;
    }
    return true;
  }

  private static RelNode copyNodeScanNewCluster(RelOptCluster optCluster, RelNode scan) {
    final RelNode newScan;
    if (scan instanceof DruidQuery) {
      final DruidQuery dq = (DruidQuery) scan;
      // Ideally we should use HiveRelNode convention. However, since Volcano planner
      // throws in that case because DruidQuery does not implement the interface,
      // we set it as Bindable. Currently, we do not use convention in Hive, hence that
      // should be fine.
      // TODO: If we want to make use of convention (e.g., while directly generating operator
      // tree instead of AST), this should be changed.
      newScan = DruidQuery.create(optCluster, optCluster.traitSetOf(BindableConvention.INSTANCE),
          scan.getTable(), dq.getDruidTable(), ImmutableList.of(dq.getTableScan()),
          DruidSqlOperatorConverter.getDefaultMap());
    } else {
      newScan = new HiveTableScan(optCluster, optCluster.traitSetOf(HiveRelNode.CONVENTION),
          (RelOptHiveTable) scan.getTable(), ((RelOptHiveTable) scan.getTable()).getName(),
          null, false, false);
    }
    return newScan;
  }

  public static MaterializationSnapshot getSnapshotOf(DDLOperationContext context, Set<TableName> tables)
          throws HiveException {
    Map<String, SnapshotContext> snapshot = getSnapshotOf(context.getDb(), tables);
    if (snapshot.isEmpty()) {
      return new MaterializationSnapshot(context.getConf().get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY));
    }

    return new MaterializationSnapshot(snapshot);
  }

  private static Map<String, SnapshotContext> getSnapshotOf(Hive db, Set<TableName> tables) throws HiveException {
    Map<String, SnapshotContext> snapshot = new HashMap<>(tables.size());
    for (TableName tableName : tables) {
      Table table = db.getTable(tableName);
      if (table.getStorageHandler() != null) {
        HiveStorageHandler storageHandler = table.getStorageHandler();
        if (!storageHandler.areSnapshotsSupported()) {
          return Collections.emptyMap();
        }
        snapshot.put(table.getFullyQualifiedName(), storageHandler.getCurrentSnapshotContext(table));
      } else {
        return Collections.emptyMap();
      }
    }
    return snapshot;
  }
}
