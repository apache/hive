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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import org.apache.calcite.tools.RelBuilderFactory;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaAggregateRel;

import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaHdfsScanRel;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaProjectRel;
import org.apache.hadoop.hive.ql.plan.impala.node.ImpalaSortRel;

/**
 * Impala specific transformation rules.
 */
public class HiveImpalaRules {

  /**
   * Rule to transform a Project-Filter-Scan logical plan into Impala's
   * HdfsScanNode that can process filters and projects directly within the scan
   */
  public static class ImpalaFilterScanRule extends RelOptRule {

    private Hive db;

    public ImpalaFilterScanRule(RelBuilderFactory relBuilderFactory, Hive db) {
      super(operand(HiveFilter.class, operand(HiveTableScan.class, none())),
              relBuilderFactory, null);
      this.db = db;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveTableScan scan = call.rel(1);

      // Only support HDFS for now.
      ImpalaHdfsScanRel newScan = new ImpalaHdfsScanRel(scan, filter, db);

      call.transformTo(newScan);
    }
  }

  public static class ImpalaScanRule extends RelOptRule {

    private Hive db;

    public ImpalaScanRule(RelBuilderFactory relBuilderFactory, Hive db) {
      super(operand(HiveTableScan.class, none()),
          relBuilderFactory, null);
      this.db = db;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveTableScan scan = call.rel(0);

      // Only support HDFS for now.
      ImpalaHdfsScanRel newScan = new ImpalaHdfsScanRel(scan, db);

      call.transformTo(newScan);
    }
  }

  public static class ImpalaFilterAggRule extends RelOptRule {

    public ImpalaFilterAggRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveFilter.class, operand(HiveAggregate.class, none())),
              relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveAggregate agg = call.rel(1);

      ImpalaAggregateRel newAgg = new ImpalaAggregateRel(agg, filter);

      call.transformTo(newAgg);
    }
  }

  public static class ImpalaAggRule extends RelOptRule {

    public ImpalaAggRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveAggregate.class, any()),
              relBuilderFactory, null);
    }
    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveAggregate agg = call.rel(0);

      ImpalaAggregateRel newAgg = new ImpalaAggregateRel(agg);

      call.transformTo(newAgg);
    }
  }

  public static class ImpalaProjectRule extends RelOptRule {
    public ImpalaProjectRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveProject.class, any()),
          relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveProject project = call.rel(0);

      ImpalaProjectRel newProject = new ImpalaProjectRel(project);

      call.transformTo(newProject);
    }
  }

  public static class ImpalaSortLimitRule extends RelOptRule {

    public ImpalaSortLimitRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveSortLimit.class, any()),
          relBuilderFactory, "ImpalaSortLimitRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveSortLimit sort = call.rel(0);

      ImpalaSortRel newSort = new ImpalaSortRel(sort);

      call.transformTo(newSort);
    }
  }
}
