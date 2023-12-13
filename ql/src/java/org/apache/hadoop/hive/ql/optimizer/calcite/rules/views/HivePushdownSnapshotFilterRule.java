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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

import java.util.Objects;
import java.util.Set;

/**
 * Calcite rule to push down predicates contains {@link VirtualColumn#SNAPSHOT_ID} reference to TableScan.
 * <p>
 * This rule traverse the logical expression in {@link HiveFilter} operators and search for
 * predicates like
 * <p>
 * <code>
 *   snapshotId &lt;= 12345677899
 * </code>
 * <p>
 * The literal is set in the {@link RelOptHiveTable#getHiveTableMD()} object wrapped by
 * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan}
 * and the original predicate in the {@link HiveFilter} is replaced with literal true.
 *
 * @see HiveAugmentSnapshotMaterializationRule
 */
public class HivePushdownSnapshotFilterRule extends RelRule<HivePushdownSnapshotFilterRule.Config> {

  public static final RelOptRule INSTANCE =
          RelRule.Config.EMPTY.as(HivePushdownSnapshotFilterRule.Config.class)
            .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
            .withOperandSupplier(operandBuilder -> operandBuilder.operand(HiveFilter.class).anyInputs())
            .withDescription("HivePushdownSnapshotFilterRule")
            .toRule();

  public interface Config extends RelRule.Config {
    @Override
    default HivePushdownSnapshotFilterRule toRule() {
      return new HivePushdownSnapshotFilterRule(this);
    }
  }

  private HivePushdownSnapshotFilterRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveFilter filter = call.rel(0);
    RexNode newCondition = filter.getCondition().accept(new SnapshotIdShuttle(call.builder().getRexBuilder(), call.getMetadataQuery(), filter));
    call.transformTo(call.builder().push(filter.getInput()).filter(newCondition).build());
  }

  static class SnapshotIdShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final RelMetadataQuery metadataQuery;
    private final RelNode startNode;

    public SnapshotIdShuttle(RexBuilder rexBuilder, RelMetadataQuery metadataQuery, RelNode startNode) {
      this.rexBuilder = rexBuilder;
      this.metadataQuery = metadataQuery;
      this.startNode = startNode;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.operands.size() == 2 &&
              (setSnapShotId(call.operands.get(0), call.operands.get(1)) ||
                      setSnapShotId(call.operands.get(1), call.operands.get(0)))) {
        return rexBuilder.makeLiteral(true);
      }

      return super.visitCall(call);
    }

    private boolean setSnapShotId(RexNode op1, RexNode op2) {
      if (!op1.isA(SqlKind.LITERAL)) {
        return false;
      }

      RexLiteral literal = (RexLiteral) op1;
      if (literal.getType().getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC) {
        return false;
      }

      Long snapshotId = literal.getValueAs(Long.class);

      RelOptTable relOptTable = getRelOptTableOf(op2);
      if (relOptTable == null) {
        return false;
      }

      RelOptHiveTable hiveTable = (RelOptHiveTable) relOptTable;
      hiveTable.getHiveTableMD().setVersionIntervalFrom(Objects.toString(snapshotId, null));
      return true;
    }

    private RelOptTable getRelOptTableOf(RexNode rexNode) {
      if (!(rexNode instanceof RexInputRef)) {
        return null;
      }

      RexInputRef rexInputRef = (RexInputRef) rexNode;
      Set<RexNode> rexNodeSet = metadataQuery.getExpressionLineage(startNode, rexInputRef);
      if (rexNodeSet == null || rexNodeSet.size() != 1) {
        return null;
      }

      RexNode resultRexNode = rexNodeSet.iterator().next();
      if (!(resultRexNode instanceof RexTableInputRef)) {
        return null;
      }
      RexTableInputRef tableInputRef = (RexTableInputRef) resultRexNode;

      RelOptTable relOptTable = tableInputRef.getTableRef().getTable();
      RelDataTypeField snapshotIdField = relOptTable.getRowType().getField(
              VirtualColumn.SNAPSHOT_ID.getName(), false, false);
      if (snapshotIdField == null) {
        return null;
      }

      return snapshotIdField.getIndex() == tableInputRef.getIndex() ? relOptTable : null;
    }
  }
}
