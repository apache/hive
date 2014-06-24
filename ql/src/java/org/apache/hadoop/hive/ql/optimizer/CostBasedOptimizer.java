/**
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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Frameworks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.optiq.HiveDefaultRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HiveMergeProjectRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HivePullUpProjectsAboveJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HivePushJoinThroughJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.rules.HiveSwapJoinRule;
import org.apache.hadoop.hive.ql.optimizer.optiq.translator.ASTConverter;
import org.apache.hadoop.hive.ql.optimizer.optiq.translator.RelNodeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.CachingRelMetadataProvider;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexBuilder;

/* 
 * Entry point to Optimizations using Optiq.  
 */
public class CostBasedOptimizer implements Frameworks.PlannerAction<RelNode> {
  private static final Set<OperatorType> m_unsupportedOpTypes = ImmutableSet.of(OperatorType.DEMUX,
                                                                  OperatorType.FORWARD,
                                                                  OperatorType.LATERALVIEWFORWARD,
                                                                  OperatorType.LATERALVIEWJOIN,
                                                                  OperatorType.MUX,
                                                                  OperatorType.PTF,
                                                                  OperatorType.SCRIPT,
                                                                  OperatorType.UDTF,
                                                                  OperatorType.UNION);

  @SuppressWarnings("rawtypes")
  private final Operator                 m_sinkOp;
  private final SemanticAnalyzer         m_semanticAnalyzer;
  private final ParseContext             m_ParseContext;

  public CostBasedOptimizer(@SuppressWarnings("rawtypes") Operator sinkOp,
      SemanticAnalyzer semanticAnalyzer, ParseContext pCtx) {
    m_sinkOp = sinkOp;
    m_semanticAnalyzer = semanticAnalyzer;
    m_ParseContext = pCtx;
  }

  /*
   * Currently contract is given a Hive Operator Tree, it returns an optimal
   * plan as an Hive AST.
   */
  public static ASTNode optimize(@SuppressWarnings("rawtypes") Operator sinkOp,
      SemanticAnalyzer semanticAnalyzer, ParseContext pCtx, List<FieldSchema> resultSchema) {
    ASTNode optiqOptimizedAST = null;
    RelNode optimizedOptiqPlan = Frameworks.withPlanner(new CostBasedOptimizer(sinkOp,
        semanticAnalyzer, pCtx));
    optiqOptimizedAST = ASTConverter.convert(optimizedOptiqPlan, resultSchema);

    return optiqOptimizedAST;
  }

  @Override
  @SuppressWarnings("unchecked")
  public RelNode apply(RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus schema) {
    RelOptPlanner planner = HiveVolcanoPlanner.createPlanner();

    /*
     * recreate cluster, so that it picks up the additional traitDef
     */
    final RelOptQuery query = new RelOptQuery(planner);
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    cluster = query.createCluster(rexBuilder.getTypeFactory(), rexBuilder);
    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(HiveDefaultRelMetadataProvider.INSTANCE);
    planner.registerMetadataProviders(list);

    RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
    cluster.setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, planner));

    RelNode opTreeInOptiq = RelNodeConverter.convert(m_sinkOp, cluster, relOptSchema,
        m_semanticAnalyzer, m_ParseContext);

    planner.clearRules();
    planner.addRule(HiveSwapJoinRule.INSTANCE);
    planner.addRule(HivePushJoinThroughJoinRule.LEFT);
    planner.addRule(HivePushJoinThroughJoinRule.RIGHT);
    if (HiveConf.getBoolVar(m_ParseContext.getConf(),
        HiveConf.ConfVars.HIVE_CBO_PULLPROJECTABOVEJOIN_RULE)) {
      planner.addRule(HivePullUpProjectsAboveJoinRule.BOTH_PROJECT);
      planner.addRule(HivePullUpProjectsAboveJoinRule.LEFT_PROJECT);
      planner.addRule(HivePullUpProjectsAboveJoinRule.RIGHT_PROJECT);
      planner.addRule(HiveMergeProjectRule.INSTANCE);
    }

    RelTraitSet desiredTraits = cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);

    RelNode rootRel = opTreeInOptiq;
    if (!rootRel.getTraitSet().equals(desiredTraits)) {
      rootRel = planner.changeTraits(opTreeInOptiq, desiredTraits);
    }
    planner.setRoot(rootRel);

    return planner.findBestExp();
  }

  public static boolean canHandleOpTree(@SuppressWarnings("rawtypes") Operator sinkOp, HiveConf conf,
      QueryProperties qp) {
    boolean runOptiq = false;

    if ((qp.getJoinCount() > 1) && (qp.getJoinCount() < HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_CBO_MAX_JOINS_SUPPORTED))
        && (qp.getOuterJoinCount() == 0)
        && !qp.hasClusterBy() && !qp.hasDistributeBy() && !qp.hasSortBy() && !qp.hasWindowing()) {
      @SuppressWarnings("rawtypes")
      final HashSet<Operator> start = new HashSet<Operator>();

      start.add(sinkOp);
      // TODO: use queryproperties instead of walking the tree
      if (!CostBasedOptimizer.operatorExists(start, true, m_unsupportedOpTypes)) {
        runOptiq = true;
      }
    }

    return runOptiq;
  }

  /*
   * TODO: moved this out of OperatorUtils for now HIVE-6403 is going to bring
   * in iterateParents: https://reviews.apache.org/r/18137/diff/#index_header
   * Will just use/enhance that once it is in. hb 2/15
   */
  /**
   * Check if operator tree, in the direction specified forward/backward,
   * contains any operator specified in the targetOPTypes.
   * 
   * @param start
   *          list of operators to start checking from
   * @param backward
   *          direction of DAG traversal; if true implies get parent ops for
   *          traversal otherwise children will be used
   * @param targetOPTypes
   *          Set of operator types to look for
   * 
   * @return true if any of the operator or its parent/children is of the name
   *         specified in the targetOPTypes
   * 
   *         NOTE: 1. This employs breadth first search 2. By using HashSet for
   *         "start" we avoid revisiting same operator twice. However it doesn't
   *         prevent revisiting the same node more than once for some complex
   *         dags.
   */
  @SuppressWarnings("unchecked")
  public static boolean operatorExists(@SuppressWarnings("rawtypes") final HashSet<Operator> start,
      final boolean backward, final Set<OperatorType> targetOPTypes) {
    @SuppressWarnings("rawtypes")
    HashSet<Operator> nextSetOfOperators = new HashSet<Operator>();

    for (@SuppressWarnings("rawtypes")
    Operator op : start) {
      if (targetOPTypes.contains(op.getType())) {
        return true;
      }

      if (backward) {
        if (op.getParentOperators() != null) {
          nextSetOfOperators.addAll(op.getParentOperators());
        }
      } else {
        if (op.getChildOperators() != null) {
          nextSetOfOperators.addAll(op.getChildOperators());
        }
      }
    }

    if (!nextSetOfOperators.isEmpty()) {
      return operatorExists(nextSetOfOperators, backward, targetOPTypes);
    }

    return false;
  }

}
