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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.cloudera.insights.advisor.materializations.AdvisorConf;
import com.cloudera.insights.advisor.materializations.MaterializationsAdvisor;
import com.cloudera.insights.advisor.materializations.rel.metadata.DASMetadataProvider;
import com.cloudera.insights.advisor.materializations.tools.Driver;
import com.cloudera.insights.advisor.materializations.tools.WorkloadInput;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.HiveMaterializedViewRule;
import org.apache.hadoop.hive.ql.parse.QueryTables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CteRewriteRule {
  private static final Logger LOG = LoggerFactory.getLogger(CteRewriteRule.class);
  public static RelNode rewrite(RelNode input){
    RelOptCluster cl = Driver.createCluster();
    List<WorkloadInput> wi = Collections.singletonList(WorkloadInput.builder().inputName("none").jsonPlan("none").runtime(0).plan(input).build());
    AdvisorConf conf = new AdvisorConf();
//    conf.set(AdvisorConf.Property.SUBSET_CARDINALITY_THRESHOLD, "0");
    final String cteIdPrefix = "cte_candidate_";
    conf.set(AdvisorConf.Property.MATERIALIZED_VIEW_NAME_PREFIX, cteIdPrefix);
    MaterializationsAdvisor advisor = new MaterializationsAdvisor(conf, cl, wi, Collections.emptySet());
    List<RelOptMaterialization> mvs = advisor.generateRecommendations();
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
    // DANGER: Creating a cluster has HUGE side effects since it switches the metadata provider for the whole
    // thread and obviously nukes out recommenders cost model.
    RelOptCluster newcl = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    // Do we need to use the advisor's cost model? Maybe we do cause otherwise I don't know how we can ensure that the
    // rewritting will be performed.
    newcl.setMetadataProvider(DASMetadataProvider.DEFAULT);
    ClusterCopier copier = new ClusterCopier(newcl);
    Map<String, RelOptMaterialization> nameToMv = mvs.stream().map(
            m -> new RelOptMaterialization(m.tableRel.accept(copier), m.queryRel.accept(copier), m.starRelOptTable,
                m.qualifiedTableName)).peek(planner::addMaterialization)
        .collect(Collectors.toMap(m -> m.qualifiedTableName.toString(), Function.identity()));
    // Whose rewritting rules we should use Advisors or Hives?
    for (RelOptRule rule : HiveMaterializedViewRule.MATERIALIZED_VIEW_REWRITING_RULES) {
      planner.addRule(rule);
    }
    planner.setRoot(input.accept(copier));
    RelNode optimized = planner.findBestExp();
    LOG.info("MV rewrite using ctes: {}", RelOptUtil.toString(optimized));
    optimized = optimized.accept(new RelHomogeneousShuttle(){
      Set<String> spools = new HashSet<>();
      @Override public RelNode visit(TableScan scan) {
        String tableName = scan.getTable().getQualifiedName().toString();
        if(tableName.contains(cteIdPrefix) && spools.add(tableName)){
          LOG.info("Creating spool for: {}", RelOptUtil.toString(scan));
          RelOptMaterialization mv = nameToMv.get(tableName);
          // The Spool types are not used at the moment so choice between LAZY/EAGER does not affect anything
          RelOptTableImpl cteTable =
              RelOptTableImpl.create(null, scan.getRowType(), scan.getTable().getQualifiedName(), null);
          return new LogicalTableSpool(newcl, newcl.traitSet(), mv.queryRel, Spool.Type.LAZY, Spool.Type.LAZY, cteTable);
        }
        return super.visit(scan);
      }
    });
    // TODO Two problems:
    // 1. The mv.queryRel is not optimized so putting into the plan as it is kind of destroys the optimizations done so far
    // 2. The mv.queryRel is using DAS operators so these cannot really run with Hive.
    LOG.info("Spool introduction: {}", RelOptUtil.toString(optimized));
    Map<String, Long> cteCounts = RelOptUtil.findAllTables(optimized).stream().map(t -> t.getQualifiedName().toString())
        .filter(s -> s.contains(cteIdPrefix))
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    optimized = optimized.accept(new RelHomogeneousShuttle() {
      @Override public RelNode visit(RelNode other) {
        if (other instanceof LogicalTableSpool) {
          LogicalTableSpool tspool = (LogicalTableSpool) other;
          if (cteCounts.getOrDefault(tspool.getTable().getQualifiedName().toString(), 0L) < 1) {
            return tspool.getInput();
          } else {
            return other;
          }
        }
        return super.visit(other);
      }
    });
    LOG.info("Redundant spool removal: {}", RelOptUtil.toString(optimized));
    return optimized;
  }

  private static class ClusterCopier extends RelHomogeneousShuttle {
    private final RelOptCluster newcl;
    ClusterCopier(RelOptCluster cluster){
      this.newcl = cluster;
    }
    @Override public RelNode visit(RelNode other) {
      other = super.visit(other);
      RelTraitSet traitSet = other.getTraitSet().replace(HiveRelNode.CONVENTION);
      if (other instanceof Aggregate) {
        Aggregate agg = (Aggregate) other;
        return new HiveAggregate(newcl, traitSet, agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList());
      } else if (other instanceof Filter) {
        Filter fil = (Filter) other;
        return new HiveFilter(newcl, traitSet, fil.getInput(), fil.getCondition());
      } else if (other instanceof Project) {
        Project pro = (Project) other;
        return new HiveProject(newcl, traitSet, pro.getInput(), pro.getProjects(), pro.getRowType(), pro.getFlags());
      } else if (other instanceof HiveTableScan) {
        HiveTableScan scan = (HiveTableScan) other;
        return new HiveTableScan(newcl, traitSet, (RelOptHiveTable) scan.getTable(), scan.getTableAlias(),
            scan.getConcatQbIDAlias(), false, scan.isInsideView());
      } else if (other instanceof TableScan) {
        // The advisor code will create DasTableScan or something similar it is not easy to create a Hive scan unless
        // we hack it badly. Obviously if here we create a LogicalTableScan it has to be something temporary cause
        // there is no way to execute something with it.
        // The traitset though needs to contain the HiveConvention otherwise the plan may be considered non-implementatable
        // and thus have infinite cost.
        TableScan scan = (TableScan) other;
        LOG.info("Found a unknown scan {}", other);
        return new HiveTableScan(newcl, traitSet,
            tmpOptTable(scan.getTable().getQualifiedName(), scan.getRowType(), newcl.getTypeFactory(),
                scan.getTable().getRowCount()), scan.getTable().getQualifiedName().get(0), null, false, false);
      } else if (other instanceof TableFunctionScan) {
        TableFunctionScan tfs = (TableFunctionScan) other;
        try {
          return HiveTableFunctionScan.create(newcl, traitSet, tfs.getInputs(), tfs.getCall(), tfs.getElementType(),
              tfs.getRowType(), tfs.getColumnMappings());
        } catch (CalciteSemanticException e) {
          throw new RuntimeException(e);
        }
      } else if (other instanceof Join) {
        Join j = (Join) other;
        return HiveJoin.getJoin(newcl, j.getLeft(), j.getRight(), j.getCondition(), j.getJoinType());
      } else if (other instanceof Sort) {
        Sort s = (Sort) other;
        return new HiveSortLimit(newcl, traitSet, s.getInput(), s.getCollation(), s.offset, s.fetch);
      } else {
        return other;
      }
    }
  }

  private static RelOptHiveTable tmpOptTable(List<String> qname, RelDataType type, RelDataTypeFactory typeFactory, double rowCount) {
    Table tblMetadata = new Table("dummy", qname.get(0));
    // The TableType.CTE is a hack to make the cost model read the size of the table instead of returning infinite
    // for the materialized view. Obviously this has to change to use the metadata providers.
    RelOptHiveTable tbl = new RelOptHiveTable(null, typeFactory, qname, type, tblMetadata, Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), new HiveConf(), Hive.getThreadLocal(), new QueryTables(true),
        Collections.emptyMap(), Collections.emptyMap(), new AtomicInteger(), RelOptHiveTable.TableType.CTE,
        new HivePartitionPruneRuleHelper());
    tbl.setRowCount(rowCount);
    return tbl;
  }
}
