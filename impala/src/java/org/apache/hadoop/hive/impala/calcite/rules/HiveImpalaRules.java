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
package org.apache.hadoop.hive.impala.calcite.rules;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.apache.hadoop.hive.impala.node.ImpalaAggregateRel;
import org.apache.hadoop.hive.impala.node.ImpalaHdfsScanRel;
import org.apache.hadoop.hive.impala.node.ImpalaJoinRel;
import org.apache.hadoop.hive.impala.node.ImpalaProjectPassthroughRel;
import org.apache.hadoop.hive.impala.node.ImpalaProjectRel;
import org.apache.hadoop.hive.impala.node.ImpalaSortRel;
import org.apache.hadoop.hive.impala.node.ImpalaTableFunctionScanRel;
import org.apache.hadoop.hive.impala.node.ImpalaUnionRel;

import java.util.List;

/**
 * Impala specific transformation rules.
 */
public class HiveImpalaRules {

  /**
   * Rule to transform a Project-Filter-Scan logical plan into Impala's
   * HdfsScanNode that can process filters and projects directly within the scan
   */
  public static class ImpalaFilterScanRule extends RelOptRule {

    private final Hive db;

    public ImpalaFilterScanRule(RelBuilderFactory relBuilderFactory, Hive db) {
      super(operand(HiveFilter.class, operand(HiveTableScan.class, none())),
              relBuilderFactory, null);
      this.db = db;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveTableScan scan = call.rel(1);
      RelNode newRelNode = null;
      // Impala uses the Union node for tables with no "from" clause.
      if (((RelOptHiveTable) scan.getTable()).isDummyTable()) {
        newRelNode = new ImpalaUnionRel(scan, filter);
      } else {
        // Only support HDFS for now.
        newRelNode = new ImpalaHdfsScanRel(scan, filter, db);
      }

      call.transformTo(newRelNode);
    }
  }

  public static class ImpalaScanRule extends RelOptRule {

    private final Hive db;

    public ImpalaScanRule(RelBuilderFactory relBuilderFactory, Hive db) {
      super(operand(HiveTableScan.class, none()),
          relBuilderFactory, null);
      this.db = db;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveTableScan scan = call.rel(0);

      RelNode newRelNode = null;
      // Impala uses the Union node for tables with no "from" clause.
      if (((RelOptHiveTable) scan.getTable()).isDummyTable()) {
        newRelNode = new ImpalaUnionRel(scan);
      } else {
        // Only support HDFS for now.
        newRelNode = new ImpalaHdfsScanRel(scan, db);
      }

      call.transformTo(newRelNode);
    }
  }

  public static class ImpalaTableFunctionRule extends RelOptRule {

    public ImpalaTableFunctionRule(RelBuilderFactory relBuilderFactory, Hive db) {
      super(operand(HiveTableFunctionScan.class, operand(ImpalaUnionRel.class, none())),
          relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveTableFunctionScan scan = call.rel(0);

      ImpalaTableFunctionScanRel newRelNode = new ImpalaTableFunctionScanRel(scan);

      call.transformTo(newRelNode);
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

  public static class ImpalaFilterJoinRule extends RelOptRule {

    public ImpalaFilterJoinRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveFilter.class, operand(HiveJoin.class, none())),
              relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveJoin join = call.rel(1);

      ImpalaJoinRel newJoin = new ImpalaJoinRel(join, filter);

      call.transformTo(newJoin);
    }
  }


  public static class ImpalaFilterProjectRule extends RelOptRule {

    public ImpalaFilterProjectRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveFilter.class, operand(HiveProject.class, none())),
              relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveProject project = call.rel(1);

      ImpalaProjectRel newProject = new ImpalaProjectRel(project, filter);

      call.transformTo(newProject);
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

      ImpalaProjectPassthroughRel newProject = new ImpalaProjectPassthroughRel(project);

      call.transformTo(newProject);
    }
  }

  public static class ImpalaJoinRule extends RelOptRule {

    public ImpalaJoinRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveJoin.class, any()),
          relBuilderFactory, "ImpalaJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveJoin join = call.rel(0);

      // here we create the intermediate join rel; final physical join such as
      // hash join, nested loop join will be created at the end by ImpalaJoinRel
      ImpalaJoinRel newJoin = new ImpalaJoinRel(join);
      call.transformTo(newJoin);
    }
  }

  public static class ImpalaSemiJoinRule extends RelOptRule {

    public ImpalaSemiJoinRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveSemiJoin.class, any()),
          relBuilderFactory, "ImpalaSemiJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveSemiJoin join = call.rel(0);

      // here we create the intermediate join rel; final physical join such as
      // hash join, nested loop join will be created at the end by ImpalaJoinRel
      ImpalaJoinRel newJoin = new ImpalaJoinRel(join);
      call.transformTo(newJoin);
    }
  }

  public static class ImpalaUnionRule extends RelOptRule {

    public ImpalaUnionRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveUnion.class, any()),
          relBuilderFactory, "ImpalaUnionRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveUnion union = call.rel(0);

      ImpalaUnionRel newUnion = new ImpalaUnionRel(union);

      call.transformTo(newUnion);
    }
  }

  public static class ImpalaSortRule extends RelOptRule {
    public ImpalaSortRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveSortLimit.class, operand(RelNode.class, any())),
          relBuilderFactory, "ImpalaSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveSortLimit sort = call.rel(0);
      final RelNode inputNode = call.rel(1);

      List<RelNode> inputNodes = sort.getInputs();
      if (inputNode instanceof HiveProject) {
        ImpalaProjectRel newProject = new ImpalaProjectRel((HiveProject) inputNode);
        inputNodes = ImmutableList.of(newProject);
      }

      RelNode newSort  = new ImpalaSortRel(sort, inputNodes, null);

      call.transformTo(newSort);
    }
  }

  public static class ImpalaFilterSortRule extends RelOptRule {
    public ImpalaFilterSortRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveFilter.class, operand(HiveSortLimit.class, operand(RelNode.class, any()))),
          relBuilderFactory, "ImpalaFilterSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveFilter filter = call.rel(0);
      final HiveSortLimit sort = call.rel(1);
      final RelNode inputNode = call.rel(2);

      List<RelNode> inputNodes = sort.getInputs();
      if (inputNode instanceof HiveProject) {
        ImpalaProjectRel newProject = new ImpalaProjectRel((HiveProject) inputNode);
        inputNodes = ImmutableList.of(newProject);
      }

      RelNode newSort = new ImpalaSortRel(sort, inputNodes, filter);

      call.transformTo(newSort);
    }
  }

  public static class ImpalaProjectProjectRule extends RelOptRule {
    public ImpalaProjectProjectRule(RelBuilderFactory relBuilderFactory) {
      super(operand(HiveProject.class, operand(HiveProject.class, any())),
          relBuilderFactory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveProject topProject = call.rel(0);
      final HiveProject bottomProject = call.rel(1);

      if (!topProject.containsOver()) {
        // Bail out
        return;
      }

      ImpalaProjectRel newBottomProject = new ImpalaProjectRel(bottomProject);
      RelNode newTopProject = topProject.copy(
          topProject.getTraitSet(), newBottomProject,
          topProject.getChildExps(), topProject.getRowType());

      call.transformTo(newTopProject);
    }
  }

}
