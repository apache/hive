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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestCBORuleFiredOnlyOnce {


  @Test
  public void testRuleFiredOnlyOnce() {

    HiveConf conf = new HiveConf();

    // Create HepPlanner
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
    programBuilder = programBuilder.addRuleCollection(
            ImmutableList.<RelOptRule>of(DummyRule.INSTANCE));

    // Create rules registry to not trigger a rule more than once
    HiveRulesRegistry registry = new HiveRulesRegistry();
    HivePlannerContext context = new HivePlannerContext(null, registry, null, null, null);
    HepPlanner planner = new HepPlanner(programBuilder.build(), context);

    // Cluster
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    // Create MD provider
    HiveDefaultRelMetadataProvider mdProvider = new HiveDefaultRelMetadataProvider(conf, null);
    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(mdProvider.getMetadataProvider());
    planner.registerMetadataProviders(list);
    RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);

    final RelNode node = new DummyNode(cluster, cluster.traitSet());

    node.getCluster().setMetadataProvider(
        new CachingRelMetadataProvider(chainedProvider, planner));

    planner.setRoot(node);

    planner.findBestExp();

    // Matches 2 times: one time the original node, one time the new node created by the rule
    assertEquals(2, DummyRule.INSTANCE.numberMatches);
    // It is fired only once: on the original node
    assertEquals(1, DummyRule.INSTANCE.numberOnMatch);
  }

  public static class DummyRule extends RelOptRule {

    public static final DummyRule INSTANCE =
            new DummyRule();

    public int numberMatches;
    public int numberOnMatch;

    private DummyRule() {
      super(operand(RelNode.class, any()));
      numberMatches = 0;
      numberOnMatch = 0;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final RelNode node = call.rel(0);

      numberMatches++;

      HiveRulesRegistry registry = call.getPlanner().
              getContext().unwrap(HiveRulesRegistry.class);

      // If this operator has been visited already by the rule,
      // we do not need to apply the optimization
      if (registry != null && registry.getVisited(this).contains(node)) {
        return false;
      }

      return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final RelNode node = call.rel(0);

      numberOnMatch++;

      // If we have fired it already once, we return and the test will fail
      if (numberOnMatch > 1) {
        return;
      }

      // Register that we have visited this operator in this rule
      HiveRulesRegistry registry = call.getPlanner().
              getContext().unwrap(HiveRulesRegistry.class);
      if (registry != null) {
        registry.registerVisited(this, node);
      }

      // We create a new op if it is the first time we fire the rule
      final RelNode newNode = new DummyNode(node.getCluster(), node.getTraitSet());
      // We register it so we do not fire the rule on it again
      if (registry != null) {
        registry.registerVisited(this, newNode);
      }

      call.transformTo(newNode);

    }
  }

  public static class DummyNode extends AbstractRelNode {

    protected DummyNode(RelOptCluster cluster, RelTraitSet traits) {
      super(cluster, cluster.traitSet());
    }

    @Override
    protected RelDataType deriveRowType() {
      return new RelRecordType(Lists.<RelDataTypeField>newArrayList());
    }
  }


}
