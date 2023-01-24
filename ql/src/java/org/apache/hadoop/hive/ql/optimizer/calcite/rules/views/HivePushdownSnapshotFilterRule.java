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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class HivePushdownSnapshotFilterRule extends RelOptRule {

  public HivePushdownSnapshotFilterRule() {
    super(operand(HiveFilter.class, operand(TableScan.class, any())),
        HiveRelFactories.HIVE_BUILDER, "HivePushdownSnapshotFilterRule");
  }

  static class SnapshotContextVisitor extends RexVisitorImpl<String> {

    protected SnapshotContextVisitor() {
      super(true);
    }

    @Override
    public String visitLiteral(RexLiteral literal) {
      if (literal.getType().getSqlTypeName().getFamily() != SqlTypeFamily.CHARACTER) {
        return null;
      }
      String value = literal.getValueAs(String.class);
      if (value.contains(SnapshotContext.class.getSimpleName())) {
        return value;
      }

      return null;
    }

    @Override public String visitCall(RexCall call) {
      for (RexNode operand : call.operands) {
        String r = operand.accept(this);
        if (isNotBlank(r)) {
          return r;
        }
      }
      return null;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HiveFilter filter = call.rel(0);
    String snapshotContextText = filter.getCondition().accept(new SnapshotContextVisitor());

    if (isBlank(snapshotContextText)) {
      return;
    }

    snapshotContextText = snapshotContextText.substring(snapshotContextText.indexOf('=') + 1);
    snapshotContextText = snapshotContextText.substring(0, snapshotContextText.length() - 1);

    TableScan tableScan = call.rel(1);
    RelOptHiveTable hiveTable = (RelOptHiveTable) tableScan.getTable();
    Table table = hiveTable.getHiveTableMD();
    table.setVersionIntervalFrom(snapshotContextText);

    call.transformTo(call.builder().push(tableScan).build());
  }
}