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
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HivePushdownSnapshotFilterRule extends RelOptRule {

  public HivePushdownSnapshotFilterRule() {
    super(operand(HiveFilter.class, operand(TableScan.class, any())),
        HiveRelFactories.HIVE_BUILDER, "HivePushdownSnapshotFilterRule");
  }

  static class SnapshotContextVisitor extends RexVisitorImpl<Long> {

    private final int snapshotIdIndex;

    protected SnapshotContextVisitor(int snapshotIdIndex) {
      super(true);
      this.snapshotIdIndex = snapshotIdIndex;
    }

    @Override
    public Long visitCall(RexCall call) {
      if (call.operands.size() == 2) {
        Long r = getSnapshotId(call.operands.get(0), call.operands.get(1));
        if (r != null) {
          return r;
        }
        r = getSnapshotId(call.operands.get(1), call.operands.get(0));
        if (r != null) {
          return r;
        }
      }

      for (RexNode operand : call.operands) {
        Long r = operand.accept(this);
        if (r != null) {
          return r;
        }
      }
      return null;
    }

    private Long getSnapshotId(RexNode op1, RexNode op2) {
      if (!(op1 instanceof RexInputRef)) {
        return null;
      }

      RexInputRef inputRef = (RexInputRef) op1;
      if (inputRef.getIndex() != snapshotIdIndex) {
        return null;
      }

      if (!(op2 instanceof RexLiteral)) {
        return null;
      }

      RexLiteral literal = (RexLiteral) op2;
      if (literal.getType().getSqlTypeName().getFamily() != SqlTypeFamily.NUMERIC) {
        return null;
      }

      return literal.getValueAs(Long.class);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveTableScan tableScan = call.rel(1);
    RelDataTypeField snapshotIdField = tableScan.getTable().getRowType().getField(
        VirtualColumn.SNAPSHOT_ID.getName(), false, false);
    if (snapshotIdField == null) {
      return;
    }

    HiveFilter filter = call.rel(0);
    Long snapshotId = filter.getCondition().accept(new SnapshotContextVisitor(snapshotIdField.getIndex()));

    if (snapshotId == null) {
      return;
    }

    RelOptHiveTable hiveTable = (RelOptHiveTable) tableScan.getTable();
    Table table = hiveTable.getHiveTableMD();
    table.setVersionIntervalFrom(Long.toString(snapshotId));

    call.transformTo(call.builder().push(tableScan).build());
  }
}