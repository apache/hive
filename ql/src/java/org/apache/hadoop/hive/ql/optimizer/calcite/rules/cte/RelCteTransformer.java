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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.cte;

import com.cloudera.insights.advisor.materializations.AdvisorConf;
import com.cloudera.insights.advisor.materializations.MaterializationsAdvisor;
import com.cloudera.insights.advisor.materializations.tools.Driver;
import com.cloudera.insights.advisor.materializations.tools.WorkloadInput;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewRule;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.type.HiveFunctionHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RelCteTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(RelCteTransformer.class);

  private final HiveConf conf;
  private final RelMetadataProvider provider;
  private List<RelOptMaterialization> ctes;

  public RelCteTransformer(HiveConf conf, RelMetadataProvider provider) {
    this.conf = conf;
    this.provider = provider;
  }

  public RelNode rewrite(RelNode input) {
    this.ctes = extractCTEs(input);
    // TODO: Check if we need consider other MV rules
    RelNode optimized =
        apply(PlannerFactory.COSTBASED, input, HiveMaterializedViewRule.MATERIALIZED_VIEW_REWRITING_RULES);
    LOG.info("MV rewrite using ctes: {}", RelOptUtil.toString(optimized));
    optimized = apply(PlannerFactory.HEURISTIC, optimized, new TableScanToSpoolRule());
    // TODO: The plan under the spool operator (mv.queryRel) is not optimized (since it comes from the advisor) so
    // this step kind of destroys optimizations that were done so far.
    LOG.info("Spool introduction: {}", RelOptUtil.toString(optimized));
    Map<String, Long> tableCounts =
        RelOptUtil.findAllTables(optimized).stream().map(t -> t.getQualifiedName().toString())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    optimized = apply(PlannerFactory.HEURISTIC, optimized, new SpoolRemoveRule(tableCounts));
    LOG.info("Redundant spool removal: {}", RelOptUtil.toString(optimized));
    return optimized;
  }

  private static List<RelOptMaterialization> extractCTEs(RelNode input) {
    RelOptCluster cl = Driver.createCluster();
    List<WorkloadInput> wi = Collections.singletonList(
        WorkloadInput.builder().inputName("none").jsonPlan("none").runtime(0).plan(input).build());
    AdvisorConf conf = new AdvisorConf();
    conf.set(AdvisorConf.Property.MATERIALIZED_VIEW_NAME_PREFIX, "cte_candidate_");
    MaterializationsAdvisor advisor = new MaterializationsAdvisor(conf, cl, wi, Collections.emptySet());
    return advisor.generateRecommendations();
  }

  private enum PlannerFactory {
    HEURISTIC {
      @Override RelOptPlanner create(HiveConf conf, RexBuilder builder, RelOptRule... rules) {
        HepProgram p = HepProgram.builder().addRuleCollection(Arrays.asList(rules)).build();
        return new HepPlanner(p, null, true, null, RelOptCostImpl.FACTORY);
      }
    }, COSTBASED {
      @Override RelOptPlanner create(HiveConf conf, RexBuilder builder, RelOptRule... rules) {
        RelOptPlanner planner = CalcitePlanner.createPlanner(conf, new HiveFunctionHelper(builder));
        Arrays.stream(rules).forEach(planner::addRule);
        return planner;
      }
    };

    abstract RelOptPlanner create(HiveConf conf, RexBuilder builder, RelOptRule... rules);
  }

  private RelNode apply(PlannerFactory plannerFactory, RelNode plan, RelOptRule... rules) {
    RexBuilder xBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    RelOptPlanner planner = plannerFactory.create(conf, xBuilder, rules);
    // DANGER: Creating a cluster has HUGE side effects since it switches the metadata provider for the whole
    // thread and obviously nukes out any previous cost model that is in place. Moving this RelOptCluster.create around
    // can easily break this class or even worse parts outside this class.
    RelOptCluster cluster = RelOptCluster.create(planner, xBuilder);
    // TODO: The cost-model that we use for deciding which CTEs to use will probably require a bit of fine tuning
    cluster.setMetadataProvider(provider);
    HiveRelCopier copier = new HiveRelCopier(cluster);
    // Copy ctes to the new cluster and register them to the planner. Copying is necessary cause VolcanoPlanner does
    // not allow using expressions from different cluster and throws expeptions.
    ctes.stream().map(copier::copy).forEach(planner::addMaterialization);
    planner.setRoot(plan.accept(copier));
    return planner.findBestExp();
  }
}
