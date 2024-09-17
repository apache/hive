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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * This rule will rewrite the materialized view with information about
 * its invalidation data. In particular, if any of the tables used by the
 * materialization has been updated since the materialization was created,
 * it will introduce a filter operator on top of that table in the materialization
 * definition, making explicit the data contained in it so the rewriting
 * algorithm can use this information to rewrite the query as a combination of the
 * outdated materialization data and the new original data in the source tables.
 * If the data in the source table matches the current data in the snapshot,
 * no filter is created.
 * In case of tables supports snapshots the filtering should be performed in the
 * TableScan operator to read records only from the relevant snapshots.
 * However, the union rewrite algorithm needs a so-called compensation predicate in
 * a Filter operator to build the union branch produces the delta records.
 * After union rewrite algorithm is executed the predicates on SnapshotIds
 * are pushed down to the corresponding TableScan operator and removed from the Filter
 * operator. So the reference to the {@link VirtualColumn#SNAPSHOT_ID} is temporary in the
 * logical plan.
 *
 * @see HivePushdownSnapshotFilterRule
 */
public class HiveAugmentSnapshotMaterializationRule extends RelRule<HiveAugmentSnapshotMaterializationRule.Config> {

  public static RelOptRule with(Map<String, SnapshotContext> mvMetaStoredSnapshot) {
    return RelRule.Config.EMPTY.as(HiveAugmentSnapshotMaterializationRule.Config.class)
            .withMvMetaStoredSnapshot(mvMetaStoredSnapshot)
            .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
            .withOperandSupplier(operandBuilder -> operandBuilder.operand(TableScan.class).anyInputs())
            .withDescription("HiveAugmentSnapshotMaterializationRule")
            .toRule();
  }

  public interface Config extends RelRule.Config {

    HiveAugmentSnapshotMaterializationRule.Config withMvMetaStoredSnapshot(
            Map<String, SnapshotContext> mvMetaStoredSnapshot);

    @ImmutableBeans.Property
    Map<String, SnapshotContext> getMvMetaStoredSnapshot();

    @Override default HiveAugmentSnapshotMaterializationRule toRule() {
      return new HiveAugmentSnapshotMaterializationRule(this);
    }
  }

  private static RelDataType snapshotIdType = null;

  @VisibleForTesting
  static RelDataType snapshotIdType(RelDataTypeFactory typeFactory) {
    if (snapshotIdType == null) {
      try {
        snapshotIdType = typeFactory.createSqlType(
            TypeConverter.convert(VirtualColumn.SNAPSHOT_ID.getTypeInfo(),
                typeFactory).getSqlTypeName());
      } catch (CalciteSemanticException e) {
        throw new RuntimeException(e);
      }
    }

    return snapshotIdType;
  }

  private final Set<RelNode> visited;
  private final Map<String, SnapshotContext> mvMetaStoredSnapshot;

  public HiveAugmentSnapshotMaterializationRule(Config config) {
    super(config);
    this.mvMetaStoredSnapshot = config.getMvMetaStoredSnapshot();
    this.visited = new HashSet<>();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final TableScan tableScan = call.rel(0);
    if (!visited.add(tableScan)) {
      // Already visited
      return;
    }

    RelOptHiveTable hiveTable = (RelOptHiveTable) tableScan.getTable();
    Table table = hiveTable.getHiveTableMD();

    SnapshotContext mvMetaTableSnapshot = mvMetaStoredSnapshot.get(table.getFullyQualifiedName());
    if (table.getStorageHandler() == null) {
      throw new UnsupportedOperationException(String.format("Table %s does not have Storage handler defined. " +
          "Mixing native and non-native tables in a materialized view definition is currently not supported!",
          table.getFullyQualifiedName()));
    }
    if (Objects.equals(mvMetaTableSnapshot, table.getStorageHandler().getCurrentSnapshotContext(table))) {
      return;
    }

    Long snapshotId = mvMetaTableSnapshot != null ? mvMetaTableSnapshot.getSnapshotId() : null;
    table.setVersionIntervalFrom(Objects.toString(snapshotId, null));

    RexBuilder rexBuilder = call.builder().getRexBuilder();
    int snapshotIdIndex = tableScan.getTable().getRowType().getField(
        VirtualColumn.SNAPSHOT_ID.getName(), false, false).getIndex();
    RexNode snapshotIdInputRef = rexBuilder.makeInputRef(
        tableScan.getTable().getRowType().getFieldList().get(snapshotIdIndex).getType(), snapshotIdIndex);

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(tableScan);
    final RexNode snapshotIdLiteral = rexBuilder.makeLiteral(
        snapshotId, snapshotIdType(relBuilder.getTypeFactory()), false);
    final RexNode predicateWithSnapShotId = rexBuilder.makeCall(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL, snapshotIdInputRef, snapshotIdLiteral);
    relBuilder.filter(singletonList(predicateWithSnapShotId));
    call.transformTo(relBuilder.build());
  }
}