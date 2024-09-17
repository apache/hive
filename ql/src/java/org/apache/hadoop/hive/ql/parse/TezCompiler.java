/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.CompositeProcessor;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.BucketVersionPopulator;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagate;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcCtx.ConstantPropagateOption;
import org.apache.hadoop.hive.ql.optimizer.ConvertJoinMapJoin;
import org.apache.hadoop.hive.ql.optimizer.DynamicPartitionPruningOptimization;
import org.apache.hadoop.hive.ql.optimizer.FiltertagAppenderProc;
import org.apache.hadoop.hive.ql.optimizer.MergeJoinProc;
import org.apache.hadoop.hive.ql.optimizer.NonBlockingOpDeDupProc;
import org.apache.hadoop.hive.ql.optimizer.ParallelEdgeFixer;
import org.apache.hadoop.hive.ql.optimizer.ReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.optimizer.RemoveDynamicPruningBySize;
import org.apache.hadoop.hive.ql.optimizer.SemiJoinReductionMerge;
import org.apache.hadoop.hive.ql.optimizer.SetHashGroupByMinReduction;
import org.apache.hadoop.hive.ql.optimizer.SetReducerParallelism;
import org.apache.hadoop.hive.ql.optimizer.SharedWorkOptimizer;
import org.apache.hadoop.hive.ql.optimizer.SortedDynPartitionOptimizer;
import org.apache.hadoop.hive.ql.optimizer.topnkey.TopNKeyProcessor;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkDeDuplication;
import org.apache.hadoop.hive.ql.optimizer.topnkey.TopNKeyPushdownProcessor;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkJoinDeDuplication;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.physical.AnnotateRunTimeStatsOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.CrossProductHandler;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapClusterStateForCompile;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapPreVectorizationPass;
import org.apache.hadoop.hive.ql.optimizer.physical.MemoryDecider;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.NullScanOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.SerializeFilter;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.mapper.AuxOpTreeSignature;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.EquivGroup;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.exec.FunctionRegistry.BLOOM_FILTER_FUNCTION;

/**
 * TezCompiler translates the operator plan into TezTasks.
 */
public class TezCompiler extends TaskCompiler {

  protected static final Logger LOG = LoggerFactory.getLogger(TezCompiler.class);

  public TezCompiler() {
  }

  @Override
  public void init(QueryState queryState, LogHelper console, Hive db) {
    super.init(queryState, console, db);

    // Tez requires us to use RPC for the query plan
    HiveConf.setBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN, true);

    // We require the use of recursive input dirs for union processing
    conf.setBoolean("mapred.input.dir.recursive", true);
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx) throws SemanticException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    // Create the context for the walker
    OptimizeTezProcContext procCtx = new OptimizeTezProcContext(conf, pCtx);

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    runTopNKeyOptimization(procCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Run top n key optimization");

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    // setup dynamic partition pruning where possible
    runDynamicPartitionPruning(procCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Setup dynamic partition pruning");

    if(procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION_MULTICOLUMN)) {
      SemiJoinReductionMerge sjmerge = new SemiJoinReductionMerge();
      sjmerge.beginPerfLogging();
      sjmerge.transform(procCtx.parseContext);
      sjmerge.endPerfLogging("Merge single column semi-join reducers to composite");
    }

    // need to run this; to get consistent filterop conditions(for operator tree matching)
    if (procCtx.conf.getBoolVar(ConfVars.HIVE_OPT_CONSTANT_PROPAGATION)) {
      new ConstantPropagate(ConstantPropagateOption.SHORTCUT).transform(procCtx.parseContext);
    }

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    // setup stats in the operator plan
    runStatsAnnotation(procCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Setup stats in the operator plan");

    // run Sorted dynamic partition optimization
    if(HiveConf.getBoolVar(procCtx.conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING) &&
        HiveConf.getVar(procCtx.conf, HiveConf.ConfVars.DYNAMIC_PARTITIONING_MODE).equals("nonstrict") &&
        !HiveConf.getBoolVar(procCtx.conf, HiveConf.ConfVars.HIVE_OPT_LIST_BUCKETING)) {
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      new SortedDynPartitionOptimizer().transform(procCtx.parseContext);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Sorted dynamic partition optimization");
    }

    if(HiveConf.getBoolVar(procCtx.conf, HiveConf.ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION)) {
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      // Dynamic sort partition adds an extra RS therefore need to de-dup
      new ReduceSinkDeDuplication().transform(procCtx.parseContext);
      // there is an issue with dedup logic wherein SELECT is created with wrong columns
      // NonBlockingOpDeDupProc fixes that
      // (kind of hackish, the issue in de-dup should be fixed but it needs more investigation)
      new NonBlockingOpDeDupProc().transform(procCtx.parseContext);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Reduce Sink de-duplication");
    }

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    // run the optimizations that use stats for optimization
    runStatsDependentOptimizations(procCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Run the optimizations that use stats for optimization");

    // repopulate bucket versions; join conversion may have created some new reducesinks
    new BucketVersionPopulator().transform(pCtx);

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    if(procCtx.conf.getBoolVar(ConfVars.HIVE_OPT_JOIN_REDUCE_DEDUPLICATION)) {
      new ReduceSinkJoinDeDuplication().transform(procCtx.parseContext);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Run reduce sink after join algorithm selection");

    semijoinRemovalBasedTransformations(procCtx);

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    if (procCtx.conf.getBoolVar(ConfVars.HIVE_SHARED_WORK_OPTIMIZATION)) {
      new SharedWorkOptimizer().transform(procCtx.parseContext);
      new ParallelEdgeFixer().transform(procCtx.parseContext);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Shared scans optimization");

    extendParentReduceSinkOfMapJoin(procCtx);

    // need a new run of the constant folding because we might have created lots
    // of "and true and true" conditions.
    // Rather than run the full constant folding just need to shortcut AND/OR expressions
    // involving constant true/false values.
    if(procCtx.conf.getBoolVar(ConfVars.HIVE_OPT_CONSTANT_PROPAGATION)) {
      new ConstantPropagate(ConstantPropagateOption.SHORTCUT).transform(procCtx.parseContext);
    }

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    AuxOpTreeSignature.linkAuxSignatures(procCtx.parseContext);
    markOperatorsWithUnstableRuntimeStats(procCtx);
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "markOperatorsWithUnstableRuntimeStats");

    if (procCtx.conf.getBoolVar(ConfVars.HIVE_IN_TEST)) {
      bucketingVersionSanityCheck(procCtx);
    }
  }

  private void runCycleAnalysisForPartitionPruning(OptimizeTezProcContext procCtx) throws SemanticException {
    // Semijoins may have created task level cycles, examine those
    connectTerminalOps(procCtx.parseContext);
    boolean cycleFree = false;
    while (!cycleFree) {
      cycleFree = true;
      Set<Set<Operator<?>>> components = getComponents(procCtx);
      for (Set<Operator<?>> component : components) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Component: ");
          for (Operator<?> co : component) {
            LOG.debug("Operator: " + co.getName() + ", " + co.getIdentifier());
          }
        }
        if (component.size() != 1) {
          LOG.info("Found cycle in operator plan...");
          cycleFree = false;
          removeCycleOperator(component, procCtx);
          break;
        }
      }
      LOG.info("Cycle free: " + cycleFree);
    }
  }

  private void removeCycleOperator(Set<Operator<?>> component, OptimizeTezProcContext context) throws SemanticException {
    AppMasterEventOperator victimAM = null;
    TableScanOperator victimTS = null;
    ReduceSinkOperator victimRS = null;

    // If there is a hint and no operator is removed then throw error
    boolean hasHint = false;
    boolean removed = false;
    for (Operator<?> o : component) {
      // Look for AppMasterEventOperator or ReduceSinkOperator
      if (o instanceof AppMasterEventOperator) {
        if (victimAM == null
                || o.getStatistics().getDataSize() < victimAM.getStatistics()
                .getDataSize()) {
          victimAM = (AppMasterEventOperator) o;
          removed = true;
        }
      } else if (o instanceof ReduceSinkOperator) {

        SemiJoinBranchInfo sjInfo =
                context.parseContext.getRsToSemiJoinBranchInfo().get(o);
        if (sjInfo == null ) {
          continue;
        }
        if (sjInfo.getIsHint()) {
          // Skipping because of hint. Mark this info,
          hasHint = true;
          continue;
        }

        TableScanOperator ts = sjInfo.getTsOp();
        // Sanity check
        assert component.contains(ts);

        if (victimRS == null ||
                ts.getStatistics().getDataSize() <
                        victimTS.getStatistics().getDataSize()) {
          victimRS = (ReduceSinkOperator) o;
          victimTS = ts;
          removed = true;
        }
      }
    }

    // Always set the semijoin optimization as victim.
    Operator<?> victim = victimRS;

    if (victimRS == null && victimAM != null ) {
        victim = victimAM;
    } else if (victimAM == null) {
      // do nothing
    } else {
      // Cycle consists of atleast one dynamic partition pruning(DPP)
      // optimization and atleast one min/max optimization.
      // DPP is a better optimization unless it ends up scanning the
      // bigger table for keys instead of the smaller table.

      // Get the parent TS of victimRS.
      Operator<?> op = victimRS;
      while(!(op instanceof TableScanOperator)) {
        op = op.getParentOperators().get(0);
      }
      if ((2 * op.getStatistics().getDataSize()) <
              victimAM.getStatistics().getDataSize()) {
        victim = victimAM;
      }
    }

    if (hasHint && !removed) {
      // There is hint but none of the operators removed. Throw error
      throw new SemanticException("The user hint is causing an operator cycle. Please fix it and retry");
    }

    if (victim == null ||
            (!context.pruningOpsRemovedByPriorOpt.isEmpty() &&
                    context.pruningOpsRemovedByPriorOpt.contains(victim))) {
      return;
    }

    GenTezUtils.removeBranch(victim);

    if (victim == victimRS) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cycle found. Removing semijoin "
            + OperatorUtils.getOpNamePretty(victimRS) + " - " + OperatorUtils.getOpNamePretty(victimTS));
      }
      GenTezUtils.removeSemiJoinOperator(context.parseContext, victimRS, victimTS);
    } else {
      // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
      LOG.info("Disabling dynamic pruning for: "
          + ((DynamicPruningEventDesc) victim.getConf()).getTableScan().toString()
          + ". Needed to break cyclic dependency");
    }
  }

  // Tarjan's algo
  private Set<Set<Operator<?>>> getComponents(OptimizeTezProcContext procCtx) {
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    AtomicInteger index = new AtomicInteger();
    Map<Operator<?>, Integer> indexes = new HashMap<Operator<?>, Integer>();
    Map<Operator<?>, Integer> lowLinks = new HashMap<Operator<?>, Integer>();
    Stack<Operator<?>> nodes = new Stack<Operator<?>>();
    Set<Set<Operator<?>>> components = new LinkedHashSet<Set<Operator<?>>>();

    for (Operator<?> o : deque) {
      if (!indexes.containsKey(o)) {
        connect(o, index, nodes, indexes, lowLinks, components, procCtx.parseContext);
      }
    }

    return components;
  }

  private void connect(Operator<?> o, AtomicInteger index, Stack<Operator<?>> nodes,
      Map<Operator<?>, Integer> indexes, Map<Operator<?>, Integer> lowLinks,
      Set<Set<Operator<?>>> components, ParseContext parseContext) {

    indexes.put(o, index.get());
    lowLinks.put(o, index.get());
    index.incrementAndGet();
    nodes.push(o);

    List<Operator<?>> children;
    if (o instanceof AppMasterEventOperator) {
      children = new ArrayList<>((o.getChildOperators()));
      TableScanOperator ts = ((DynamicPruningEventDesc) o.getConf()).getTableScan();
      LOG.debug("Adding special edge: " + o.getName() + " --> " + ts.toString());
      children.add(ts);
    } else if (o instanceof TerminalOperator) {
      children = new ArrayList<>((o.getChildOperators()));
      for (ReduceSinkOperator rs : parseContext.getTerminalOpToRSMap().get((TerminalOperator<?>)o)) {
        // add an edge
        LOG.debug("Adding special edge: From terminal op to semijoin edge "  + o.getName() + " --> " + rs.toString());
        children.add(rs);
      }
      if (o instanceof ReduceSinkOperator) {
        // semijoin case
        SemiJoinBranchInfo sjInfo = parseContext.getRsToSemiJoinBranchInfo().get(o);
        if (sjInfo != null) {
          TableScanOperator ts = sjInfo.getTsOp();
          LOG.debug("Adding special edge: " + o.getName() + " --> " + ts.toString());
          children.add(ts);
        }
      }
    } else {
      children = o.getChildOperators();
    }

    for (Operator<?> child : children) {
      if (!indexes.containsKey(child)) {
        connect(child, index, nodes, indexes, lowLinks, components, parseContext);
        lowLinks.put(o, Math.min(lowLinks.get(o), lowLinks.get(child)));
      } else if (nodes.contains(child)) {
        lowLinks.put(o, Math.min(lowLinks.get(o), indexes.get(child)));
      }
    }

    if (lowLinks.get(o).equals(indexes.get(o))) {
      Set<Operator<?>> component = new LinkedHashSet<Operator<?>>();
      components.add(component);
      Operator<?> current;
      do {
        current = nodes.pop();
        component.add(current);
      } while (current != o);
    }
  }

  private void runStatsAnnotation(OptimizeTezProcContext procCtx) throws SemanticException {
    new AnnotateWithStatistics().transform(procCtx.parseContext);
    new AnnotateWithOpTraits().transform(procCtx.parseContext);
  }

  private void runStatsDependentOptimizations(OptimizeTezProcContext procCtx) throws SemanticException {

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("Set parallelism - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"),
        new SetReducerParallelism());
    opRules.put(new RuleRegExp("Convert Join to Map-join",
        JoinOperator.getOperatorName() + "%"), new ConvertJoinMapJoin());
    if (procCtx.conf.getBoolVar(ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_STATS_ADJUST)) {
      opRules.put(new RuleRegExp("Set min reduction - GBy (Hash)",
          GroupByOperator.getOperatorName() + "%"),
          new SetHashGroupByMinReduction());
    }

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new ForwardWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private void extendParentReduceSinkOfMapJoin(OptimizeTezProcContext procCtx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<>();
    opRules.put(
        new RuleRegExp("Extend parent RS of MapJoin", MapJoinOperator.getOperatorName() + "%"),
        new FiltertagAppenderProc());

    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    List<Node> topNodes = new ArrayList<>(procCtx.parseContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }

  private void semijoinRemovalBasedTransformations(OptimizeTezProcContext procCtx) throws SemanticException {
    PerfLogger perfLogger = SessionState.getPerfLogger();

    final boolean dynamicPartitionPruningEnabled =
        procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING);
    final boolean semiJoinReductionEnabled = dynamicPartitionPruningEnabled &&
        procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION) &&
            procCtx.parseContext.getRsToSemiJoinBranchInfo().size() != 0;
    final boolean extendedReductionEnabled = dynamicPartitionPruningEnabled &&
        procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING_EXTENDED);

    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    if (dynamicPartitionPruningEnabled) {
      runRemoveDynamicPruningOptimization(procCtx);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Run remove dynamic pruning by size");

    if (semiJoinReductionEnabled) {
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      markSemiJoinForDPP(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Mark certain semijoin edges important based ");

      // Remove any semi join edges from Union Op
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      removeSemiJoinEdgesForUnion(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER,
                            "Remove any semi join edge between Union and RS");

      // Remove any parallel edge between semijoin and mapjoin.
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      removeSemijoinsParallelToMapJoin(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Remove any parallel edge between semijoin and mapjoin");

      // Remove semijoin optimization if SMB join is created.
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      removeSemijoinOptimizationFromSMBJoins(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Remove semijoin optimizations if needed");

      // Remove bloomfilter if no stats generated
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      removeSemiJoinIfNoStats(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Remove bloom filter optimizations if needed");

      // Removing semijoin optimization when it may not be beneficial
      perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
      removeSemijoinOptimizationByBenefit(procCtx);
      perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Remove Semijoins based on cost benefits");
    }

    // after the stats phase we might have some cyclic dependencies that we need
    // to take care of.
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    if (dynamicPartitionPruningEnabled) {
      runCycleAnalysisForPartitionPruning(procCtx);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Run cycle analysis for partition pruning");

    // remove redundant dpp and semijoins
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    if (extendedReductionEnabled) {
      removeRedundantSemijoinAndDpp(procCtx);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "Remove redundant semijoin reduction");
  }

  private void runRemoveDynamicPruningOptimization(OptimizeTezProcContext procCtx) throws SemanticException {

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
        new RuleRegExp("Remove dynamic pruning by size",
        AppMasterEventOperator.getOperatorName() + "%"),
        new RemoveDynamicPruningBySize());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new ForwardWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private void runDynamicPartitionPruning(OptimizeTezProcContext procCtx) throws SemanticException {

    if (!procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING)) {
      return;
    }

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
        new RuleRegExp(new String("Dynamic Partition Pruning"), FilterOperator.getOperatorName()
            + "%"), new DynamicPartitionPruningOptimization());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new ForwardWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  @Override
  protected void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {

	PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenTezUtils utils = new GenTezUtils();
    GenTezWork genTezWork = new GenTezWork(utils);

    GenTezProcContext procCtx = new GenTezProcContext(
        conf, tempParseContext, mvTask, rootTasks, inputs, outputs);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("Split Work - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"),
        genTezWork);

    opRules.put(new RuleRegExp("No more walking on ReduceSink-MapJoin",
        MapJoinOperator.getOperatorName() + "%"), new ReduceSinkMapJoinProc());

    opRules.put(new RuleRegExp("Recognize a Sorted Merge Join operator to setup the right edge and"
        + " stop traversing the DummyStore-MapJoin", CommonMergeJoinOperator.getOperatorName()
        + "%"), new MergeJoinProc());

    opRules.put(new RuleRegExp("Split Work + Move/Merge - FileSink",
        FileSinkOperator.getOperatorName() + "%"),
        new CompositeProcessor(new FileSinkProcessor(), genTezWork));

    opRules.put(new RuleRegExp("Split work - DummyStore", DummyStoreOperator.getOperatorName()
        + "%"), genTezWork);

    opRules.put(new RuleRegExp("Handle Potential Analyze Command",
        TableScanOperator.getOperatorName() + "%"),
        new ProcessAnalyzeTable(utils));

    opRules.put(new RuleRegExp("Remember union",
        UnionOperator.getOperatorName() + "%"),
        new UnionProcessor());

    opRules.put(new RuleRegExp("AppMasterEventOperator",
        AppMasterEventOperator.getOperatorName() + "%"),
        new AppMasterEventProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    SemanticGraphWalker ogw = new GenTezWorkWalker(disp, procCtx);
    ogw.startWalking(topNodes, null);

    // we need to specify the reserved memory for each work that contains Map Join
    for (List<BaseWork> baseWorkList : procCtx.mapJoinWorkMap.values()) {
      for (BaseWork w : baseWorkList) {
        // work should be the smallest unit for memory allocation
        w.setReservedMemoryMB(
            (int)(conf.getLongVar(ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD) / (1024 * 1024)));
      }
    }

    // we need to clone some operator plans and remove union operators still
    int indexForTezUnion = 0;
    for (BaseWork w: procCtx.workWithUnionOperators) {
      GenTezUtils.removeUnionOperators(procCtx, w, indexForTezUnion++);
    }

    // then we make sure the file sink operators are set up right
    for (FileSinkOperator fileSink: procCtx.fileSinkSet) {
      GenTezUtils.processFileSink(procCtx, fileSink);
    }

    // Connect any edges required for min/max pushdown
    if (pCtx.getRsToRuntimeValuesInfoMap().size() > 0) {
      for (ReduceSinkOperator rs : pCtx.getRsToRuntimeValuesInfoMap().keySet()) {
        // Process min/max
        GenTezUtils.processDynamicSemiJoinPushDownOperator(
                procCtx, pCtx.getRsToRuntimeValuesInfoMap().get(rs), rs);
      }
    }
    // and finally we hook up any events that need to be sent to the tez AM
    LOG.debug("There are " + procCtx.eventOperatorSet.size() + " app master events.");
    for (AppMasterEventOperator event : procCtx.eventOperatorSet) {
      LOG.debug("Handling AppMasterEventOperator: " + event);
      GenTezUtils.processAppMasterEvent(procCtx, event);
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "generateTaskTree");
  }

  void setInputFormatForMapWork(BaseWork work) {
    if (work instanceof MapWork) {
      MapWork mapWork = (MapWork) work;
      Map<String, Operator<? extends OperatorDesc>> opMap = mapWork.getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends OperatorDesc> op : opMap.values()) {
          setInputFormat(mapWork, op);
        }
      }
    }
  }

  @Override
  protected void setInputFormat(Task<?> task) {
    if (task instanceof TezTask) {
      TezWork work = ((TezTask)task).getWork();
      List<BaseWork> all = work.getAllWork();
      for (BaseWork w: all) {
        if (w instanceof MergeJoinWork) {
          MergeJoinWork mj = (MergeJoinWork)w;
          setInputFormatForMapWork(mj.getMainWork());
          for (BaseWork bw : mj.getBaseWorkList()) {
            setInputFormatForMapWork(bw);
          }
        } else {
          setInputFormatForMapWork(w);
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<?>> listTasks
        = ((ConditionalTask) task).getListTasks();
      for (Task<?> tsk : listTasks) {
        setInputFormat(tsk);
      }
    }

    if (task.getChildTasks() != null) {
      for (Task<?> childTask : task.getChildTasks()) {
        setInputFormat(childTask);
      }
    }
  }

  private void setInputFormat(MapWork work, Operator<? extends OperatorDesc> op) {
    if (op == null) {
      return;
    }
    if (op.isUseBucketizedHiveInputFormat()) {
      work.setUseBucketizedHiveInputFormat(true);
      return;
    }

    if (op.getChildOperators() != null) {
      for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
        setInputFormat(work, childOp);
      }
    }
  }

  @Override
  protected void decideExecMode(List<Task<?>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx)
      throws SemanticException {
    // currently all Tez work is on the cluster
    return;
  }

  @Override
  protected void optimizeTaskPlan(List<Task<?>> rootTasks, ParseContext pCtx,
      Context ctx) throws SemanticException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.TEZ_COMPILER);
    PhysicalContext physicalCtx = new PhysicalContext(conf, pCtx, pCtx.getContext(), rootTasks,
       pCtx.getFetchTask());

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_NULL_SCAN_OPTIMIZE)) {
      physicalCtx = new NullScanOptimizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping null scan query optimization");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_METADATA_ONLY_QUERIES)) {
      physicalCtx = new MetadataOnlyOptimizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping metadata only query optimization");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_CHECK_CROSS_PRODUCT)) {
      physicalCtx = new CrossProductHandler().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping cross product analysis");
    }

    if ("llap".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_MODE))) {
      physicalCtx = new LlapPreVectorizationPass().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping llap pre-vectorization pass");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
      physicalCtx = new Vectorizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping vectorization");
    }

    if (!"none".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVE_STAGE_ID_REARRANGE))) {
      physicalCtx = new StageIDsRearranger().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping stage id rearranger");
    }

    if ((conf.getBoolVar(HiveConf.ConfVars.HIVE_TEZ_ENABLE_MEMORY_MANAGER))
        && (conf.getBoolVar(HiveConf.ConfVars.HIVE_USE_HYBRIDGRACE_HASHJOIN))) {
      physicalCtx = new MemoryDecider().resolve(physicalCtx);
    }

    if ("llap".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_MODE))) {
      LlapClusterStateForCompile llapInfo = LlapClusterStateForCompile.getClusterInfo(conf);
      physicalCtx = new LlapDecider(llapInfo).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping llap decider");
    }

    //  This optimizer will serialize all filters that made it to the
    //  table scan operator to avoid having to do it multiple times on
    //  the backend. If you have a physical optimization that changes
    //  table scans or filters, you have to invoke it before this one.
    physicalCtx = new SerializeFilter().resolve(physicalCtx);

    if (physicalCtx.getContext().getExplainAnalyze() != null) {
      new AnnotateRunTimeStatsOptimizer().resolve(physicalCtx);
    }

    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.TEZ_COMPILER, "optimizeTaskPlan");
    return;
  }

  private static class SMBJoinOpProcContext implements NodeProcessorCtx {
    HashMap<CommonMergeJoinOperator, TableScanOperator> JoinOpToTsOpMap = new HashMap<CommonMergeJoinOperator, TableScanOperator>();
  }

  private static class SMBJoinOpProc implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      SMBJoinOpProcContext ctx = (SMBJoinOpProcContext) procCtx;
      ctx.JoinOpToTsOpMap.put((CommonMergeJoinOperator) nd,
              (TableScanOperator) stack.get(0));
      return null;
    }
  }

  private static void removeSemijoinOptimizationFromSMBJoins(
          OptimizeTezProcContext procCtx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
            new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%" +
                    ".*" + TezDummyStoreOperator.getOperatorName() + "%" +
                    CommonMergeJoinOperator.getOperatorName() + "%"),
            new SMBJoinOpProc());

    SMBJoinOpProcContext ctx = new SMBJoinOpProcContext();
    // The dispatcher finds SMB and if there is semijoin optimization before it, removes it.
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, ctx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new PreOrderOnceWalker(disp);
    ogw.startWalking(topNodes, null);

    List<TableScanOperator> tsOps = new ArrayList<>();
    // Iterate over the map and remove semijoin optimizations if needed.
    for (CommonMergeJoinOperator joinOp : ctx.JoinOpToTsOpMap.keySet()) {
      // Get one top level TS Op directly from the stack
      tsOps.add(ctx.JoinOpToTsOpMap.get(joinOp));

      // Get the other one by examining Join Op
      List<Operator<?>> parents = joinOp.getParentOperators();
      for (Operator<?> parent : parents) {
        if (parent instanceof TezDummyStoreOperator) {
          // already accounted for
          continue;
        }

        while (parent != null) {
          if (parent instanceof TableScanOperator) {
            tsOps.add((TableScanOperator) parent);
            break;
          }
          parent = parent.getParentOperators().get(0);
        }
      }
    }
    // Now the relevant TableScanOperators are known, find if there exists
    // a semijoin filter on any of them, if so, remove it.

    ParseContext pctx = procCtx.parseContext;
    Set<ReduceSinkOperator> rsSet = new HashSet<>(pctx.getRsToSemiJoinBranchInfo().keySet());
    for (TableScanOperator ts : tsOps) {
      for (ReduceSinkOperator rs : rsSet) {
        SemiJoinBranchInfo sjInfo = pctx.getRsToSemiJoinBranchInfo().get(rs);
        if (sjInfo != null && ts == sjInfo.getTsOp()) {
          // match!
          if (sjInfo.getIsHint()) {
            throw new SemanticException("Removing hinted semijoin as it is with SMB join " + rs + " : " + ts);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Semijoin optimization found going to SMB join. Removing semijoin "
                    + OperatorUtils.getOpNamePretty(rs) + " - " + OperatorUtils.getOpNamePretty(ts));
          }
          GenTezUtils.removeBranch(rs);
          GenTezUtils.removeSemiJoinOperator(pctx, rs, ts);
        }
      }
    }
  }

  private static class TerminalOpsInfo {
    public Set<TerminalOperator<?>> terminalOps;

    TerminalOpsInfo(Set<TerminalOperator<?>> terminalOps) {
      this.terminalOps = terminalOps;
    }
  }

  private void connectTerminalOps(ParseContext pCtx) {
    // The map which contains the virtual edges from non-semijoin terminal ops to semjoin RSs.
    Multimap<TerminalOperator<?>, ReduceSinkOperator> terminalOpToRSMap = ArrayListMultimap.create();

    // Map of semijoin RS to work ops to ensure no work is examined more than once.
    Map<ReduceSinkOperator, TerminalOpsInfo> rsToTerminalOpsInfo = new HashMap<>();

    // Get all the terminal ops
    for (ReduceSinkOperator rs : pCtx.getRsToSemiJoinBranchInfo().keySet()) {
      TerminalOpsInfo terminalOpsInfo = rsToTerminalOpsInfo.get(rs);
      if (terminalOpsInfo != null) {
        continue; // done with this one
      }

      Set<ReduceSinkOperator> workRSOps = new HashSet<>();
      Set<TerminalOperator<?>> workTerminalOps = new HashSet<>();
      // Get the SEL Op in the semijoin-branch, SEL->GBY1->RS1->GBY2->RS2
      SelectOperator selOp = OperatorUtils.ancestor(rs, SelectOperator.class, 0, 0, 0, 0);
      OperatorUtils.findWorkOperatorsAndSemiJoinEdges(selOp,
              pCtx.getRsToSemiJoinBranchInfo(), workRSOps, workTerminalOps);

      TerminalOpsInfo candidate = new TerminalOpsInfo(workTerminalOps);

      // A work may contain multiple semijoin edges, traverse rsOps and add for each
      for (ReduceSinkOperator rsFound : workRSOps) {
        rsToTerminalOpsInfo.put(rsFound, candidate);
        for (TerminalOperator<?> terminalOp : candidate.terminalOps) {
          terminalOpToRSMap.put(terminalOp, rsFound);
        }
      }
    }

    pCtx.setTerminalOpToRSMap(terminalOpToRSMap);
  }

  private void removeSemiJoinIfNoStats(OptimizeTezProcContext procCtx)
          throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
            new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%" +
                    ReduceSinkOperator.getOperatorName() + "%" +
                    GroupByOperator.getOperatorName() + "%" +
                    ReduceSinkOperator.getOperatorName() + "%"),
            new SemiJoinRemovalProc(true, false));
    SemiJoinRemovalContext ctx =
        new SemiJoinRemovalContext(procCtx.parseContext);
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, ctx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new PreOrderOnceWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private static class CollectAll implements SemanticNodeProcessor {
    private PlanMapper planMapper;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      ParseContext pCtx = ((OptimizeTezProcContext) procCtx).parseContext;
      planMapper = pCtx.getContext().getPlanMapper();
      FilterOperator fop = (FilterOperator) nd;
      OpTreeSignature sig = planMapper.getSignatureOf(fop);
      List<EquivGroup> ar = getGroups(planMapper, HiveFilter.class);


      return nd;
    }

    private List<EquivGroup> getGroups(PlanMapper planMapper2, Class<HiveFilter> class1) {
      Iterator<EquivGroup> it = planMapper.iterateGroups();
      List<EquivGroup> ret = new ArrayList<PlanMapper.EquivGroup>();
      while (it.hasNext()) {
        EquivGroup g = it.next();
        if (g.getAll(class1).size() > 0) {
          ret.add(g);
        }
      }
      return ret;
    }
  }

  private static class MarkRuntimeStatsAsIncorrect implements SemanticNodeProcessor {

    private PlanMapper planMapper;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      ParseContext pCtx = ((OptimizeTezProcContext) procCtx).parseContext;
      planMapper = pCtx.getContext().getPlanMapper();
      if (nd instanceof ReduceSinkOperator) {
        ReduceSinkOperator rs = (ReduceSinkOperator) nd;
        SemiJoinBranchInfo sjInfo = pCtx.getRsToSemiJoinBranchInfo().get(rs);
        if (sjInfo == null) {
          return null;
        }
        walkSubtree(sjInfo.getTsOp());
      }
      if (nd instanceof AppMasterEventOperator) {
        AppMasterEventOperator ame = (AppMasterEventOperator) nd;
        AppMasterEventDesc c = ame.getConf();
        if (c instanceof DynamicPruningEventDesc) {
          DynamicPruningEventDesc dped = (DynamicPruningEventDesc) c;
          mark(dped.getTableScan());
        }
      }
      if (nd instanceof TableScanOperator) {
        // If the tablescan operator is making use of filtering capabilities of readers then
        // we will not see the actual incoming rowcount which was processed - so we may not use it for relNodes
        TableScanOperator ts = (TableScanOperator) nd;
        if (ts.getConf().getPredicateString() != null) {
          planMapper.link(ts, new OperatorStats.MayNotUseForRelNodes());
        }
      }
      return null;
    }

    private void walkSubtree(Operator<?> root) {
      Deque<Operator<?>> deque = new LinkedList<>();
      deque.add(root);
      while (!deque.isEmpty()) {
        Operator<?> op = deque.pollLast();
        mark(op);
        if (op instanceof ReduceSinkOperator) {
          // Done with this branch
        } else {
          deque.addAll(op.getChildOperators());
        }
      }
    }

    private void mark(Operator<?> op) {
      planMapper.link(op, new OperatorStats.IncorrectRuntimeStatsMarker());
    }

  }

  private void markOperatorsWithUnstableRuntimeStats(OptimizeTezProcContext procCtx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
        new RuleRegExp("R1",
            ReduceSinkOperator.getOperatorName() + "%"),
        new MarkRuntimeStatsAsIncorrect());
    opRules.put(
        new RuleRegExp("R2",
            AppMasterEventOperator.getOperatorName() + "%"),
        new MarkRuntimeStatsAsIncorrect());
    opRules.put(
        new RuleRegExp("R3",
            TableScanOperator.getOperatorName() + "%"),
        new MarkRuntimeStatsAsIncorrect());
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new PreOrderOnceWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private class SemiJoinRemovalProc implements SemanticNodeProcessor {

    private final boolean removeBasedOnStats;
    private final boolean removeRedundant;

    private SemiJoinRemovalProc (boolean removeBasedOnStats, boolean removeRedundant) {
      this.removeBasedOnStats = removeBasedOnStats;
      this.removeRedundant = removeRedundant;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator rs = (ReduceSinkOperator) nd;
      SemiJoinRemovalContext rCtx = (SemiJoinRemovalContext) procCtx;
      ParseContext pCtx = rCtx.parseContext;
      SemiJoinBranchInfo sjInfo = pCtx.getRsToSemiJoinBranchInfo().get(rs);
      if (sjInfo == null) {
        // nothing to do here.
        return null;
      }
      TableScanOperator targetTSOp = sjInfo.getTsOp();

      // This is a semijoin branch. The stack should look like,
      // <Parent Ops>-SEL-GB1-RS1-GB2-RS2
      GroupByOperator gbOp = (GroupByOperator) stack.get(stack.size() - 2);
      GroupByDesc gbDesc = gbOp.getConf();
      List<AggregationDesc> aggregationDescs = gbDesc.getAggregators();
      for (AggregationDesc agg : aggregationDescs) {
        if (!isBloomFilterAgg(agg)) {
          continue;
        }

        GenericUDAFBloomFilterEvaluator udafBloomFilterEvaluator =
            (GenericUDAFBloomFilterEvaluator) agg.getGenericUDAFEvaluator();
        if (udafBloomFilterEvaluator.hasHintEntries()) {
          return null; // Created using hint, skip it
        }

        if (removeBasedOnStats) {
          long expectedEntries = udafBloomFilterEvaluator.getExpectedEntries();
          if (expectedEntries == -1 || expectedEntries >
              pCtx.getConf().getLongVar(ConfVars.TEZ_MAX_BLOOM_FILTER_ENTRIES)) {
            if (sjInfo.getIsHint() && expectedEntries == -1) {
              throw new SemanticException("Removing hinted semijoin due to lack to stats" +
                  " or exceeding max bloom filter entries");
            } else if(sjInfo.getIsHint()) {
              // do not remove if hint is provided
              continue;
            }
            // Remove the semijoin optimization branch along with ALL the mappings
            // The parent GB2 has all the branches. Collect them and remove them.
            for (Node node : gbOp.getChildren()) {
              ReduceSinkOperator rsFinal = (ReduceSinkOperator) node;
              TableScanOperator ts = pCtx.getRsToSemiJoinBranchInfo().
                  get(rsFinal).getTsOp();
              if (LOG.isDebugEnabled()) {
                LOG.debug("expectedEntries=" + expectedEntries + ". "
                    + "Either stats unavailable or expectedEntries exceeded max allowable bloomfilter size. "
                    + "Removing semijoin "
                    + OperatorUtils.getOpNamePretty(rs) + " - " + OperatorUtils.getOpNamePretty(ts));
              }
              GenTezUtils.removeBranch(rsFinal);
              GenTezUtils.removeSemiJoinOperator(pCtx, rsFinal, ts);
            }
            return null;
          }
        }
      }

      if (removeBasedOnStats) {
        // At this point, hinted semijoin case has been handled already
        // Check if big table is big enough that runtime filtering is
        // worth it.
        TableScanOperator ts = sjInfo.getTsOp();
        if (ts.getStatistics() != null) {
          long numRows = ts.getStatistics().getNumRows();
          if (numRows < pCtx.getConf().getLongVar(ConfVars.TEZ_BIGTABLE_MIN_SIZE_SEMIJOIN_REDUCTION)) {
            if (sjInfo.getShouldRemove()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Insufficient rows (" + numRows + ") to justify semijoin optimization. Removing semijoin "
                    + OperatorUtils.getOpNamePretty(rs) + " - " + OperatorUtils.getOpNamePretty(ts));
              }
              GenTezUtils.removeBranch(rs);
              GenTezUtils.removeSemiJoinOperator(pCtx, rs, ts);
            }
          }
        }
      }

      if (removeRedundant) {
        // Look for RS ops above the current semijoin branch
        Set<ReduceSinkOperator> rsOps = OperatorUtils.findOperators(
            ((Operator<?>) stack.get(stack.size() - 5)).getParentOperators().get(0),
            ReduceSinkOperator.class);
        for (Operator<?> otherRSOp : rsOps) {
          SemiJoinBranchInfo otherSjInfo = pCtx.getRsToSemiJoinBranchInfo().get(otherRSOp);
          // First conjunct prevents SJ RS from removing itself
          if (otherRSOp != rs && otherSjInfo != null && otherSjInfo.getTsOp() == targetTSOp) {
            if (rCtx.opsToRemove.containsKey(otherRSOp)) {
              // We found siblings, since we are removing the other operator, no need to remove this one
              continue;
            }
            List<ExprNodeDesc> thisTargetColumns = pCtx.getRsToRuntimeValuesInfoMap().get(rs).getTargetColumns();
            List<ExprNodeDesc> otherTargetColumns =
                pCtx.getRsToRuntimeValuesInfoMap().get(otherRSOp).getTargetColumns();
            if (!ExprNodeDescUtils.isSame(thisTargetColumns, otherTargetColumns)) {
              // Filter should be on the same columns, otherwise we do not proceed
              continue;
            }
            rCtx.opsToRemove.put(rs, targetTSOp);
            break;
          }
        }
      }

      return null;
    }
  }

  private static boolean isBloomFilterAgg(AggregationDesc agg) {
    return BLOOM_FILTER_FUNCTION.equals(agg.getGenericUDAFName());
  }

  private static class DynamicPruningRemovalRedundantProc implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      AppMasterEventOperator event = (AppMasterEventOperator) nd;
      if (!(event.getConf() instanceof DynamicPruningEventDesc)) {
        return null;
      }

      SemiJoinRemovalContext rCtx = (SemiJoinRemovalContext) procCtx;

      DynamicPruningEventDesc desc = (DynamicPruningEventDesc) event.getConf();
      TableScanOperator targetTSOp = desc.getTableScan();
      String targetColumnName = desc.getTargetColumnName();

      // Look for event ops above the current event op branch
      Operator<?> op = event.getParentOperators().get(0);
      while (op.getChildOperators().size() < 2) {
        op = op.getParentOperators().get(0);
      }
      Set<AppMasterEventOperator> eventOps = OperatorUtils.findOperators(
          op, AppMasterEventOperator.class);
      for (AppMasterEventOperator otherEvent : eventOps) {
        if (!(otherEvent.getConf() instanceof DynamicPruningEventDesc)) {
          continue;
        }
        DynamicPruningEventDesc otherDesc = (DynamicPruningEventDesc) otherEvent.getConf();
        if (otherEvent != event && otherDesc.getTableScan() == targetTSOp &&
            otherDesc.getTargetColumnName().equals(targetColumnName)) {
          if (rCtx.opsToRemove.containsKey(otherEvent)) {
            // We found siblings, since we are removing the other operator, no need to remove this one
            continue;
          }
          rCtx.opsToRemove.put(event, targetTSOp);
          break;
        }
      }

      return null;
    }
  }

  private void removeRedundantSemijoinAndDpp(OptimizeTezProcContext procCtx)
      throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<>();
    opRules.put(
        new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%" +
            ReduceSinkOperator.getOperatorName() + "%" +
            GroupByOperator.getOperatorName() + "%" +
            ReduceSinkOperator.getOperatorName() + "%"),
        new SemiJoinRemovalProc(false, true));
    opRules.put(
        new RuleRegExp("R2",
            AppMasterEventOperator.getOperatorName() + "%"),
        new DynamicPruningRemovalRedundantProc());

    // Gather
    SemiJoinRemovalContext ctx =
        new SemiJoinRemovalContext(procCtx.parseContext);
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, ctx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new PreOrderOnceWalker(disp);
    ogw.startWalking(topNodes, null);

    // Remove
    for (Map.Entry<Operator<?>, TableScanOperator> p : ctx.opsToRemove.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing redundant " + OperatorUtils.getOpNamePretty(p.getKey()) + " - " + OperatorUtils.getOpNamePretty(p.getValue()));
      }
      GenTezUtils.removeBranch(p.getKey());
      if (p.getKey() instanceof AppMasterEventOperator) {
        GenTezUtils.removeSemiJoinOperator(procCtx.parseContext, (AppMasterEventOperator) p.getKey(), p.getValue());
      } else if (p.getKey() instanceof ReduceSinkOperator) {
        GenTezUtils.removeSemiJoinOperator(procCtx.parseContext, (ReduceSinkOperator) p.getKey(), p.getValue());
      } else {
        throw new SemanticException("Unexpected error - type for branch could not be recognized");
      }
    }
  }

  private class SemiJoinRemovalContext implements NodeProcessorCtx {
    private final ParseContext parseContext;
    private final Map<Operator<?>, TableScanOperator> opsToRemove;

    private SemiJoinRemovalContext(final ParseContext parseContext) {
      this.parseContext = parseContext;
      this.opsToRemove = new HashMap<>();
    }
  }

  private static void runTopNKeyOptimization(OptimizeTezProcContext procCtx)
      throws SemanticException {
    if (!procCtx.conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_TOPNKEY)) {
      return;
    }

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(
        new RuleRegExp("Top n key optimization", ReduceSinkOperator.getOperatorName() + "%"),
        new TopNKeyProcessor(
          HiveConf.getIntVar(procCtx.conf, HiveConf.ConfVars.HIVE_MAX_TOPN_ALLOWED),
          HiveConf.getFloatVar(procCtx.conf, ConfVars.HIVE_TOPN_EFFICIENCY_THRESHOLD),
          HiveConf.getIntVar(procCtx.conf, ConfVars.HIVE_TOPN_EFFICIENCY_CHECK_BATCHES),
          HiveConf.getIntVar(procCtx.conf, ConfVars.HIVE_TOPN_MAX_NUMBER_OF_PARTITIONS)));
    opRules.put(
            new RuleRegExp("Top n key pushdown", TopNKeyOperator.getOperatorName() + "%"),
            new TopNKeyPushdownProcessor());


    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private boolean findParallelSemiJoinBranch(Operator<?> mapjoin, TableScanOperator bigTableTS,
                                             ParseContext parseContext,
                                             Map<ReduceSinkOperator, TableScanOperator> semijoins,
                                             Map<TableScanOperator, List<MapJoinOperator>> probeDecodeMJoins) {

    boolean parallelEdges = false;
    for (Operator<?> op : mapjoin.getParentOperators()) {
      if (!(op instanceof ReduceSinkOperator)) {
        continue;
      }

      op = op.getParentOperators().get(0);

      // Follow the Reducesink operator upstream which is on small table side.
      while (!(op instanceof ReduceSinkOperator) &&
              !(op instanceof TableScanOperator) &&
              !(op.getChildren() != null && op.getChildren().size() > 1)) {
        if (op instanceof MapJoinOperator) {
          // Pick the correct parent, only one of the parents is not
          // ReduceSink, that is what we are looking for.
          for (Operator<?> parentOp : op.getParentOperators()) {
            if (parentOp instanceof ReduceSinkOperator) {
              continue;
            }
            op = parentOp; // parent in current pipeline
            continue;
          }
        }
        op = op.getParentOperators().get(0);
      }

      // Bail out if RS or TS is encountered.
      if (op instanceof ReduceSinkOperator || op instanceof TableScanOperator) {
        continue;
      }

      // A branch is hit.
      for (Node nd : op.getChildren()) {
        if (nd instanceof SelectOperator) {
          Operator<?> child = (Operator<?>) nd;

          while (child.getChildOperators().size() > 0) {
            child = child.getChildOperators().get(0);
          }

          // If not ReduceSink Op, skip
          if (!(child instanceof ReduceSinkOperator)) {
            // This still could be DPP.
            if (child instanceof AppMasterEventOperator &&
                    ((AppMasterEventOperator) child).getConf() instanceof DynamicPruningEventDesc) {
              // DPP indeed, Set parallel edges true
              parallelEdges = true;
            }
            continue;
          }

          ReduceSinkOperator rs = (ReduceSinkOperator) child;
          SemiJoinBranchInfo sjInfo = parseContext.getRsToSemiJoinBranchInfo().get(rs);
          if (sjInfo == null) {
            continue;
          }

          TableScanOperator ts = sjInfo.getTsOp();
          if (ts != bigTableTS) {
            // skip, not the one we are looking for.
            continue;
          }

          parallelEdges = true;

          // Keep track of Mj to probeDecode TS
          if (!probeDecodeMJoins.containsKey(ts)){
            probeDecodeMJoins.put(ts, new ArrayList<>());
          }
          probeDecodeMJoins.get(ts).add((MapJoinOperator) mapjoin);

          // Skip adding to SJ removal map when created by hint
          if (!sjInfo.getIsHint() && sjInfo.getShouldRemove()) {
            semijoins.put(rs, ts);
          }
        }
      }
    }
    return parallelEdges;
  }

  /*
   * Given an operator this method removes all semi join edges downstream (children) until it hits RS
   */
  private void removeSemiJoinEdges(Operator<?> op, OptimizeTezProcContext procCtx,
                                   Map<ReduceSinkOperator, TableScanOperator> sjToRemove) throws SemanticException {
    if(op instanceof ReduceSinkOperator && op.getNumChild() == 0) {
      Map<ReduceSinkOperator, SemiJoinBranchInfo> sjMap = procCtx.parseContext.getRsToSemiJoinBranchInfo();
      if(sjMap.get(op) != null) {
        sjToRemove.put((ReduceSinkOperator)op, sjMap.get(op).getTsOp());
      }
    }

    for(Operator<?> child:op.getChildOperators()) {
      removeSemiJoinEdges(child, procCtx, sjToRemove);
    }
  }

  private void removeSemiJoinEdgesForUnion(OptimizeTezProcContext procCtx) throws SemanticException{
    // Get all the TS ops.
    List<Operator<?>> topOps = new ArrayList<>();
    topOps.addAll(procCtx.parseContext.getTopOps().values());
    Set<Operator<?>> unionOps = new HashSet<>();

    Map<ReduceSinkOperator, TableScanOperator> sjToRemove = new HashMap<>();
    for (Operator<?> parent : topOps) {
      Deque<Operator<?>> deque = new LinkedList<>();
      deque.add(parent);
      while (!deque.isEmpty()) {
        Operator<?> op = deque.pollLast();
        if (op instanceof UnionOperator && !unionOps.contains(op)) {
          unionOps.add(op);
          removeSemiJoinEdges(op, procCtx, sjToRemove);
        }
        deque.addAll(op.getChildOperators());
      }
    }
    // remove sj
    if (sjToRemove.size() > 0) {
      for (Map.Entry<ReduceSinkOperator, TableScanOperator> entry : sjToRemove.entrySet()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Semijoin optimization with Union operator. Removing semijoin "
                        + OperatorUtils.getOpNamePretty(entry.getKey()) + " - "
                        + OperatorUtils.getOpNamePretty(sjToRemove.get(entry.getKey())));
        }
        GenTezUtils.removeBranch(entry.getKey());
        GenTezUtils.removeSemiJoinOperator(procCtx.parseContext, entry.getKey(), entry.getValue());
      }
    }
  }

  /*
   *  The algorithm looks at all the mapjoins in the operator pipeline until
   *  it hits RS Op and for each mapjoin examines if it has paralllel semijoin
   *  edge or dynamic partition pruning.
   *
   *  As an extension, the algorithm also looks for suitable table scan operators that
   *  could reduce the number of rows decoded at runtime using the information provided by
   *  the MapJoin operators of the branch when ProbeDecode feature is enabled.
   */
  private void removeSemijoinsParallelToMapJoin(OptimizeTezProcContext procCtx)
          throws SemanticException {
    if (!procCtx.conf.getBoolVar(ConfVars.HIVE_CONVERT_JOIN)) {
      // Not needed without mapjoin conversion
      return;
    }

    // Get all the TS ops.
    List<Operator<?>> topOps = new ArrayList<>();
    topOps.addAll(procCtx.parseContext.getTopOps().values());

    Map<ReduceSinkOperator, TableScanOperator> semijoins = new HashMap<>();
    Map<TableScanOperator, List<MapJoinOperator>> probeDecodeMJoins = new HashMap<>();
    for (Operator<?> parent : topOps) {
      // A TS can have multiple branches due to DPP Or Semijoin Opt.
      // USe DFS to traverse all the branches until RS is hit.
      Deque<Operator<?>> deque = new LinkedList<>();
      deque.add(parent);
      while (!deque.isEmpty()) {
        Operator<?> op = deque.pollLast();
        if (op instanceof ReduceSinkOperator) {
          // Done with this branch
          continue;
        }

        if (op instanceof MapJoinOperator) {
          // A candidate.
          if (!findParallelSemiJoinBranch(op, (TableScanOperator) parent,
                  procCtx.parseContext, semijoins, probeDecodeMJoins)) {
            // No parallel edge was found for the given mapjoin op,
            // no need to go down further, skip this TS operator pipeline.
            break;
          }
        }
        deque.addAll(op.getChildOperators());
      }
    }
    //  No need to remove SJ branches when we have semi-join reduction or when semijoins are enabled for parallel mapjoins.
    if (!procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION_FOR_MAPJOIN)) {
      if (semijoins.size() > 0) {
        for (Entry<ReduceSinkOperator, TableScanOperator> semiEntry : semijoins.entrySet()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Semijoin optimization with parallel edge to map join. Removing semijoin " +
                OperatorUtils.getOpNamePretty(semiEntry.getKey()) + " - " + OperatorUtils.getOpNamePretty(semiEntry.getValue()));
          }
          GenTezUtils.removeBranch(semiEntry.getKey());
          GenTezUtils.removeSemiJoinOperator(procCtx.parseContext, semiEntry.getKey(), semiEntry.getValue());
        }
      }
    }
    if (LlapHiveUtils.isLlapMode(procCtx.conf) && procCtx.conf.getBoolVar(ConfVars.HIVE_OPTIMIZE_SCAN_PROBEDECODE)) {
      if (probeDecodeMJoins.size() > 0) {
        // When multiple MJ, select one based on a policy
        for (Map.Entry<TableScanOperator, List<MapJoinOperator>> probeTsMap : probeDecodeMJoins.entrySet()){
          TableScanOperator.ProbeDecodeContext tsCntx = null;
          // Currently supporting: LowestRatio policy
          // TODO: Add more policies and make the selection a conf property
          tsCntx = selectLowestRatioProbeDecodeMapJoin(probeTsMap.getKey(), probeTsMap.getValue());
          if (tsCntx != null) {
            LOG.debug("ProbeDecode MJ for TS {}  with CacheKey {} MJ Pos {} ColName {} with Ratio {}",
                    probeTsMap.getKey().getName(), tsCntx.getMjSmallTableCacheKey(), tsCntx.getMjSmallTablePos(),
                    tsCntx.getMjBigTableKeyColName(), tsCntx.getKeyRatio());
            probeTsMap.getKey().setProbeDecodeContext(tsCntx);
            probeTsMap.getKey().getConf().setProbeDecodeContext(tsCntx);
          }
        }
      }
    }
  }

  private static TableScanOperator.ProbeDecodeContext selectLowestRatioProbeDecodeMapJoin(TableScanOperator tsOp,
      List<MapJoinOperator> mjOps) throws SemanticException {
    MapJoinOperator selectedMJOp = null;
    double selectedMJOpRatio = 0;
    for (MapJoinOperator currMJOp : mjOps) {
      if (!isValidProbeDecodeMapJoin(currMJOp)) {
        continue;
      }
      // At this point we know it is a single Key MapJoin
      if (selectedMJOp == null) {
        // Set the first valid MJ
        selectedMJOp = currMJOp;
        selectedMJOpRatio = getProbeDecodeNDVRatio(tsOp, currMJOp);
        LOG.debug("ProbeDecode MJ {} with Ratio {}", selectedMJOp, selectedMJOpRatio);
      } else {
        double currMJRatio = getProbeDecodeNDVRatio(tsOp, currMJOp);
        if (currMJRatio < selectedMJOpRatio){
          LOG.debug("ProbeDecode MJ {} Ratio {} is lower than existing MJ {} with Ratio {}",
              currMJOp, currMJRatio, selectedMJOp, selectedMJOpRatio);
          selectedMJOp = currMJOp;
          selectedMJOpRatio = currMJRatio;
        }
      }
    }

    TableScanOperator.ProbeDecodeContext tsProbeDecodeCtx = null;
    // If there a valid MJ to be used for TS probeDecode make sure the MJ cache key is generated and
    // then propagate the new ProbeDecodeContext (to be used by LLap IO when executing the TSop)
    if (selectedMJOp != null) {
      String mjCacheKey = selectedMJOp.getConf().getCacheKey();
      if (mjCacheKey == null) {
        // Generate cache key if it has not been yet generated
        mjCacheKey = MapJoinDesc.generateCacheKey(selectedMJOp.getOperatorId());
        // Set in the conf of the map join operator
        selectedMJOp.getConf().setCacheKey(mjCacheKey);
      }

      byte posBigTable = (byte) selectedMJOp.getConf().getPosBigTable();
      Byte[] order = selectedMJOp.getConf().getTagOrder();
      Byte mjSmallTablePos = (order[0] == posBigTable ? order[1] : order[0]);

      List<ExprNodeDesc> keyDesc = selectedMJOp.getConf().getKeys().get(posBigTable);
      ExprNodeColumnDesc keyCol = (ExprNodeColumnDesc) keyDesc.get(0);
      ExprNodeColumnDesc originTSColExpr = OperatorUtils.findTableOriginColExpr(keyCol, selectedMJOp, tsOp);
      if (originTSColExpr == null) {
        LOG.warn("ProbeDecode could not find origTSCol for mjCol: {} with MJ Schema: {}",
            keyCol, selectedMJOp.getSchema());
      } else if (!TypeInfoUtils.doPrimitiveCategoriesMatch(keyCol.getTypeInfo(), originTSColExpr.getTypeInfo())) {
        // src Col -> HT key Col needs explicit or implicit (Casting) conversion
        // as a result we cannot perform direct lookups on the HT
        LOG.warn("ProbeDecode origTSCol {} type missmatch mjCol {}", originTSColExpr, keyCol);
      } else {
        tsProbeDecodeCtx = new TableScanOperator.ProbeDecodeContext(mjCacheKey, mjSmallTablePos,
            originTSColExpr.getColumn(), selectedMJOpRatio);
      }
    }
    return tsProbeDecodeCtx;
  }

  // Return the ratio of: (distinct) JOIN_probe_key_column_rows / (distinct) JOIN_TS_target_column_rows
  private static double getProbeDecodeNDVRatio(TableScanOperator tsOp, MapJoinOperator mjOp) {
    long mjKeyCardinality = mjOp.getStatistics().getNumRows();
    long tsKeyCardinality = tsOp.getStatistics().getNumRows();

    byte posBigTable = (byte) mjOp.getConf().getPosBigTable();

    Byte[] order = mjOp.getConf().getTagOrder();
    Byte mjSmallTablePos = (order[0] == posBigTable ? order[1] : order[0]);
    Byte mjBigTablePos = (order[0] == posBigTable ? order[0] : order[1]);

    // Single Key MJ at this point
    List<ExprNodeDesc> tsKeyDesc = mjOp.getConf().getKeys().get(mjBigTablePos);
    List<ExprNodeDesc> mjKeyDesc = mjOp.getConf().getKeys().get(mjSmallTablePos);
    if (mjKeyDesc.get(0) instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc tsKeyCol = (ExprNodeColumnDesc) tsKeyDesc.get(0);
      ExprNodeColumnDesc mjKeyCol = (ExprNodeColumnDesc) mjKeyDesc.get(0);

      ColStatistics mjStats = mjOp.getStatistics().getColumnStatisticsFromColName(mjKeyCol.getColumn());
      ColStatistics tsStats = tsOp.getStatistics().getColumnStatisticsFromColName(tsKeyCol.getColumn());

      if (canUseNDV(mjStats)) {
        mjKeyCardinality = mjStats.getCountDistint();
      }
      if (canUseNDV(tsStats)) {
        tsKeyCardinality = tsStats.getCountDistint();
      }
    }
    return mjKeyCardinality / (double) tsKeyCardinality;
  }

  /**
   * Returns true for a MapJoin operator that can be used for ProbeDecode.
   * MapJoin should be a single Key join, where the bigTable keyCol is only a ExprNodeColumnDesc
   * @param mapJoinOp
   * @return true for a valid MapJoin
   */
  private static boolean isValidProbeDecodeMapJoin(MapJoinOperator mapJoinOp) {
    Map<Byte, List<ExprNodeDesc>> keyExprs = mapJoinOp.getConf().getKeys();
    List<ExprNodeDesc> bigTableKeyExprs = keyExprs.get( (byte) mapJoinOp.getConf().getPosBigTable());
    return (bigTableKeyExprs.size() == 1) && (bigTableKeyExprs.get(0) instanceof ExprNodeColumnDesc);
  }

  private static boolean canUseNDV(ColStatistics colStats) {
    return (colStats != null) && (colStats.getCountDistint() >= 0);
  }

  private static double getBloomFilterCost(
      SelectOperator sel) {
    double cost = -1;
    Statistics selStats = sel.getStatistics();
    if (selStats != null) {
      cost = selStats.getNumRows();

      // Some other things that could be added here to model cost:
      // Cost of computing/sending partial BloomFilter results? BloomFilterSize * # mappers
      // For reduce-side join, add the cost of the semijoin table scan/dependent tablescans?
    }
    return cost;
  }

  private static long getCombinedKeyDomainCardinality(
      ColStatistics selColStat,
      ColStatistics selColSourceStat,
      ColStatistics tsColStat) {
    long keyDomainCardinality = -1;
    if (!canUseNDV(selColStat) || !canUseNDV(tsColStat)) {
      return -1;
    }

    long selColSourceNdv = canUseNDV(selColSourceStat) ? selColSourceStat.getCountDistint() : -1;
    boolean semiJoinKeyIsPK = StatsUtils.inferForeignKey(selColStat, tsColStat);
    if (semiJoinKeyIsPK) {
      // PK/FQ relationship: NDV of selColSourceStat is a superset of what is in tsColStat
      if (selColSourceNdv >= 0) {
        // Most accurate domain cardinality would be source column NDV if available.
        keyDomainCardinality = selColSourceNdv;
      }
    } else {
      if (selColSourceNdv >= 0) {
        // If semijoin keys and ts keys completely unrelated, the cardinality of both sets
        // could be obtained by adding both cardinalities. Would there be an average case?
        keyDomainCardinality = selColSourceNdv + tsColStat.getCountDistint();

        // Don't exceed the range if we have one.
        if (StatsUtils.hasDiscreteRange(selColStat)
            && StatsUtils.hasDiscreteRange(tsColStat)) {
          long range = 0;
          // Trying using the cardinality from the value range.
          ColStatistics.Range combinedRange = StatsUtils.combineRange(selColStat.getRange(), tsColStat.getRange());
          if (combinedRange != null) {
            range = StatsUtils.getRangeDelta(combinedRange);
          } else {
            range = StatsUtils.getRangeDelta(selColStat.getRange())
                + StatsUtils.getRangeDelta(tsColStat.getRange());
          }
          keyDomainCardinality = Math.min(keyDomainCardinality, range);
        }
      }
      // Otherwise, we tried ..
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Computing key domain cardinality, keyDomainCardinality=" + keyDomainCardinality
          + ", semiJoinKeyIsPK=" + semiJoinKeyIsPK
          + ", selColStat=" + selColStat
          + ", selColSourceStat=" + selColSourceStat
          + ", tsColStat=" + tsColStat);
    }

    return keyDomainCardinality;
  }

  private static double getBloomFilterSelectivity(
      SelectOperator sel, ExprNodeDesc selExpr,
      Statistics filStats, ExprNodeDesc tsExpr) {
    Statistics selStats = sel.getStatistics();
    assert selStats != null;
    assert filStats != null;
    // For cardinality values use numRows as default, try to use ColStats if available
    long selKeyCardinality = selStats.getNumRows();
    long tsKeyCardinality = filStats.getNumRows();
    long keyDomainCardinality = selKeyCardinality + tsKeyCardinality;

    ExprNodeColumnDesc selCol = ExprNodeDescUtils.getColumnExpr(selExpr);
    ExprNodeColumnDesc tsCol = ExprNodeDescUtils.getColumnExpr(tsExpr);
    if (selCol != null && tsCol != null) {
      // Check if there are column stats available for these columns
      ColStatistics selColStat = selStats.getColumnStatisticsFromColName(selCol.getColumn());
      ColStatistics filColStat = filStats.getColumnStatisticsFromColName(tsCol.getColumn());
      if (canUseNDV(selColStat)) {
        selKeyCardinality = selColStat.getCountDistint();
      }
      // Get colstats for the original table column for selCol if possible, this would have
      // more accurate information about the original NDV of the column before any filtering.
      ColStatistics selColSourceStat = null;
      if (selColStat != null) {
        ExprNodeDescUtils.ColumnOrigin selColSource = ExprNodeDescUtils.findColumnOrigin(selCol, sel);
        if (selColSource != null && selColSource.op.getStatistics() != null) {
          selColSourceStat = selColSource.op.getStatistics().getColumnStatisticsFromColName(
              selColSource.col.getColumn());
        }
      }
      long domainCardinalityFromColStats = getCombinedKeyDomainCardinality(
          selColStat, selColSourceStat, filColStat);
      if (domainCardinalityFromColStats >= 0) {
        keyDomainCardinality = domainCardinalityFromColStats;
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("BloomFilter selectivity for " + selCol + " to " + tsCol + ", selKeyCardinality=" + selKeyCardinality
          + ", tsKeyCardinality=" + tsKeyCardinality + ", keyDomainCardinality=" + keyDomainCardinality);
    }
    // Selectivity: key cardinality of semijoin / domain cardinality
    return selKeyCardinality / (double) keyDomainCardinality;
  }

  /**
   * Computes the benefit of applying the bloom filter.
   * <p>
   * The benefit is defined as the number of tuples that are filtered/removed from the bloom.
   * </p>
   */
  private static double getBloomFilterBenefit(SelectOperator sel, List<ExprNodeDesc> selExpr, Statistics filStats,
      List<ExprNodeDesc> tsExpr) {
    if (sel.getStatistics() == null || filStats == null) {
      LOG.debug("No stats available to compute BloomFilter benefit");
      return -1;
    }
    // Find the semijoin column with the smallest number of matches and keep its selectivity
    double selectivity = 1.0;
    for (int i = 0; i < tsExpr.size(); i++) {
      selectivity = Math.min(selectivity, getBloomFilterSelectivity(sel, selExpr.get(i), filStats, tsExpr.get(i)));
    }
    // Decrease the min selectivity by 5% for each additional column in the semijoin.
    // Consider the following semijoins:
    //  SJ1(author.name, author.age);
    //  SJ2(author.name).
    // Intuitively even if the min selectivity of both is 0.8 the semijoin with two columns (SJ1)
    // will match less tuples than the semijoin with one column (SJ2).
    selectivity -= selectivity * (tsExpr.size() - 1) * 0.05;
    // Selectivity cannot be less than 0.0
    selectivity = Math.max(0.0, selectivity);
    // Benefit (rows filtered from ts): (1 - selectivity) * # ts rows
    return filStats.getNumRows() * (1 - selectivity);
  }

  private static double computeBloomFilterNetBenefit(
      SelectOperator sel, List<ExprNodeDesc> selExpr,
      Statistics filStats, List<ExprNodeDesc> tsExpr) {
    double netBenefit = 0;
    double benefit = getBloomFilterBenefit(sel, selExpr, filStats, tsExpr);
    if (benefit > 0 && filStats != null) {
      double cost = getBloomFilterCost(sel);
      if (cost > 0) {
        long filDataSize = filStats.getNumRows();
        netBenefit = Math.max(benefit - cost, 0) / filDataSize;
        LOG.debug("BloomFilter benefit=" + benefit
            + ", cost=" + cost
            + ", tsDataSize=" + filDataSize
            + ", netBenefit=" + (benefit - cost));
      }
    }
    LOG.debug("netBenefit=" + netBenefit);
    return netBenefit;
  }

  /**
   * Sort semijoin filters depending on the benefit (computed depending on selectivity and cost)
   * that they provide. We create three blocks: first all normal predicates, second between clauses
   * for the min/max dynamic values, and finally the in bloom filter predicates. The intuition is
   * that evaluating the between clause will be cheaper than evaluating the bloom filter predicates.
   * Hence, after this method runs, normal predicates come first (possibly sorted by Calcite),
   * then we will have sorted between clauses, and finally sorted in bloom filter clauses.
   */
  private static void sortSemijoinFilters(OptimizeTezProcContext procCtx,
      ListMultimap<FilterOperator, SemijoinOperatorInfo> globalReductionFactorMap) throws SemanticException {
    for (Entry<FilterOperator, Collection<SemijoinOperatorInfo>> e : globalReductionFactorMap.asMap().entrySet()) {
      FilterOperator filterOp = e.getKey();
      Collection<SemijoinOperatorInfo> semijoinInfos = e.getValue();

      ExprNodeDesc pred = filterOp.getConf().getPredicate();
      if (FunctionRegistry.isOpAnd(pred)) {
        LinkedHashSet<ExprNodeDesc> allPreds = new LinkedHashSet<>(pred.getChildren());
        List<ExprNodeDesc> betweenPreds = new ArrayList<>();
        List<ExprNodeDesc> inBloomFilterPreds = new ArrayList<>();
        // We check whether we can find semijoin predicates
        for (SemijoinOperatorInfo roi : semijoinInfos) {
          for (ExprNodeDesc expr : pred.getChildren()) {
            if (FunctionRegistry.isOpBetween(expr) &&
                expr.getChildren().get(2) instanceof ExprNodeDynamicValueDesc) {
              // BETWEEN in SJ
              String dynamicValueIdFromExpr = ((ExprNodeDynamicValueDesc) expr.getChildren().get(2))
                  .getDynamicValue().getId();
              List<String> dynamicValueIdsFromMap = procCtx.parseContext.getRsToRuntimeValuesInfoMap()
                  .get(roi.rsOperator).getDynamicValueIDs();
              for (String dynamicValueIdFromMap : dynamicValueIdsFromMap) {
                if (dynamicValueIdFromExpr.equals(dynamicValueIdFromMap)) {
                  betweenPreds.add(expr);
                  allPreds.remove(expr);
                  break;
                }
              }
            } else if (FunctionRegistry.isOpInBloomFilter(expr) &&
                expr.getChildren().get(1) instanceof ExprNodeDynamicValueDesc) {
              // IN_BLOOM_FILTER in SJ
              String dynamicValueIdFromExpr = ((ExprNodeDynamicValueDesc) expr.getChildren().get(1))
                  .getDynamicValue().getId();
              List<String> dynamicValueIdsFromMap = procCtx.parseContext.getRsToRuntimeValuesInfoMap()
                  .get(roi.rsOperator).getDynamicValueIDs();
              for (String dynamicValueIdFromMap : dynamicValueIdsFromMap) {
                if (dynamicValueIdFromExpr.equals(dynamicValueIdFromMap)) {
                  inBloomFilterPreds.add(expr);
                  allPreds.remove(expr);
                  break;
                }
              }
            }
          }
        }

        List<ExprNodeDesc> newAndArgs = new ArrayList<>(allPreds); // First rest of predicates
        newAndArgs.addAll(betweenPreds);  // Then sorted between predicates
        newAndArgs.addAll(inBloomFilterPreds); // Finally, sorted in bloom predicates

        ExprNodeDesc andExpr = ExprNodeGenericFuncDesc.newInstance(
            FunctionRegistry.getFunctionInfo("and").getGenericUDF(), newAndArgs);
        filterOp.getConf().setPredicate(andExpr);
      }
    }
  }

  private void removeSemijoinOptimizationByBenefit(OptimizeTezProcContext procCtx)
      throws SemanticException {

    Map<ReduceSinkOperator, SemiJoinBranchInfo> map = procCtx.parseContext.getRsToSemiJoinBranchInfo();
    if (map.isEmpty()) {
      // Nothing to do
      return;
    }

    // Scale down stats for tables with DPP
    Map<FilterOperator, Statistics> adjustedStatsMap = new HashMap<>();
    List<ReduceSinkOperator> semijoinRsToRemove = new ArrayList<>();
    double semijoinReductionThreshold = procCtx.conf.getFloatVar(
        HiveConf.ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION_THRESHOLD);
    // Using SortedSet to make iteration order deterministic
    final Comparator<ReduceSinkOperator> rsOpComp =
        (ReduceSinkOperator o1, ReduceSinkOperator o2) -> (o1.toString().compareTo(o2.toString()));
    SortedSet<ReduceSinkOperator> semiJoinRsOps = new TreeSet<>(rsOpComp);
    semiJoinRsOps.addAll(map.keySet());
    ListMultimap<FilterOperator, SemijoinOperatorInfo> globalReductionFactorMap = ArrayListMultimap.create();
    while (!semiJoinRsOps.isEmpty()) {
      // We will gather the SJs to keep in the plan in the following map
      Map<FilterOperator, SemijoinOperatorInfo> reductionFactorMap = new HashMap<>();
      SortedSet<ReduceSinkOperator> semiJoinRsOpsNewIter = new TreeSet<>(rsOpComp);
      for (ReduceSinkOperator rs : semiJoinRsOps) {
        SemiJoinBranchInfo sjInfo = map.get(rs);
        if (sjInfo.getIsHint() || !sjInfo.getShouldRemove()) {
          // Semijoin created using hint or marked useful, skip it
          continue;
        }
        // rs is semijoin optimization branch, which should look like <Parent>-SEL-GB1-RS1-GB2-RS2
        SelectOperator sel = OperatorUtils.ancestor(rs, SelectOperator.class, 0, 0, 0, 0);

        // Check the ndv/rows from the SEL vs the destination tablescan the semijoin opt is going to.
        TableScanOperator ts = sjInfo.getTsOp();
        RuntimeValuesInfo rti = procCtx.parseContext.getRsToRuntimeValuesInfoMap().get(rs);
        List<ExprNodeDesc> targetColumns = rti.getTargetColumns();
        // In semijoin branches the SEL operator has the following forms:
        // SEL[c1] - single column semijoin reduction
        // SEL[c1, c2,..., ck, hash(hash(hash(c1, c2),...),ck)] - multi column semijoin reduction
        // The source columns in the above cases are c1, c2,...,ck.
        // We need to exclude the hash(...) expression, if it is present.
        List<ExprNodeDesc> sourceColumns = sel.getConf().getColList().subList(0, targetColumns.size());

        if (LOG.isDebugEnabled()) {
          LOG.debug("Computing BloomFilter cost/benefit for " + OperatorUtils.getOpNamePretty(rs)
              + " - " + OperatorUtils.getOpNamePretty(ts) + " " + targetColumns + " ");
        }

        FilterOperator filterOperator = (FilterOperator) ts.getChildOperators().get(0);
        Statistics filterStats = adjustedStatsMap.get(filterOperator);
        if (filterStats == null && filterOperator.getStatistics() != null) {
          filterStats = filterOperator.getStatistics().clone();
          adjustedStatsMap.put(filterOperator, filterStats);
        }
        double reductionFactor = computeBloomFilterNetBenefit(sel, sourceColumns, filterStats, targetColumns);
        if (reductionFactor < semijoinReductionThreshold) {
          // This semijoin optimization should be removed. Do it after we're done iterating
          semijoinRsToRemove.add(rs);
        } else {
          // This semijoin qualifies, add it to the result set
          if (filterStats != null) {
            ImmutableSet.Builder<String> colNames = ImmutableSet.builder();
            for (ExprNodeDesc tsExpr : targetColumns) {
              Set<ExprNodeColumnDesc> allReferencedColumns = ExprNodeDescUtils.findAllColumnDescs(tsExpr);
              for (ExprNodeColumnDesc col : allReferencedColumns) {
                colNames.add(col.getColumn());
              }
            }
            // We check whether there was already another SJ over this TS that was selected
            // in previous iteration
            SemijoinOperatorInfo prevResult = reductionFactorMap.get(filterOperator);
            if (prevResult != null) {
              if (prevResult.reductionFactor < reductionFactor) {
                // We should pick up new SJ as its reduction factor is greater than the previous one
                // that we found. We add the previous RS where SJ was originating to RS ops for new
                // iteration
                reductionFactorMap.put(filterOperator, new SemijoinOperatorInfo(rs, filterOperator,
                    filterStats, colNames.build(), reductionFactor));
                semiJoinRsOpsNewIter.add(prevResult.rsOperator);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding " + OperatorUtils.getOpNamePretty(prevResult.rsOperator)
                      + " for re-iteration");
                }
              } else {
                // We should pick up old SJ. We just need to add new RS where SJ was originating
                // to RS ops for new iteration
                semiJoinRsOpsNewIter.add(rs);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Adding " + OperatorUtils.getOpNamePretty(rs) + " for re-iteration");
                }
              }
            } else {
              // Another SJ did not exist for this TS, hence just add it to SJs to keep
              reductionFactorMap.put(filterOperator, new SemijoinOperatorInfo(rs, filterOperator,
                  filterStats, colNames.build(), reductionFactor));
            }
          }
        }
      }

      for (SemijoinOperatorInfo roi : reductionFactorMap.values()) {
        // This semijoin will be kept
        // We are going to adjust the filter statistics
        long newNumRows = (long) (1.0 - roi.reductionFactor) * roi.filterStats.getNumRows();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Old stats for {}: {}", roi.filterOperator, roi.filterStats);
          LOG.debug("Number of rows reduction: {}/{}", newNumRows, roi.filterStats.getNumRows());
        }
        StatsUtils.updateStats(roi.filterStats, newNumRows,
            true, roi.filterOperator, roi.colNames);
        if (LOG.isDebugEnabled()) {
          LOG.debug("New stats for {}: {}", roi.filterOperator, roi.filterStats);
        }
        adjustedStatsMap.put(roi.filterOperator, roi.filterStats);
        globalReductionFactorMap.put(roi.filterOperator, roi);
      }

      semiJoinRsOps = semiJoinRsOpsNewIter;
    }

    for (ReduceSinkOperator rs : semijoinRsToRemove) {
      TableScanOperator ts = map.get(rs).getTsOp();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Reduction factor not satisfied for " + OperatorUtils.getOpNamePretty(rs)
            + "-" + OperatorUtils.getOpNamePretty(ts) + ". Removing semijoin optimization.");
      }
      GenTezUtils.removeBranch(rs);
      GenTezUtils.removeSemiJoinOperator(procCtx.parseContext, rs, ts);
    }

    if (!globalReductionFactorMap.isEmpty()) {
      sortSemijoinFilters(procCtx, globalReductionFactorMap);
    }
  }

  /**
   * Internal class to encapsulate information needed to evaluate stats
   * about a SJ that will be kept in the tree.
   */
  private class SemijoinOperatorInfo {
    final ReduceSinkOperator rsOperator;
    final FilterOperator filterOperator;
    final ImmutableSet<String> colNames;
    final Statistics filterStats;
    final double reductionFactor;

    private SemijoinOperatorInfo(ReduceSinkOperator rsOperator, FilterOperator filterOperator,
          Statistics filterStats, Collection<String> colNames, double reductionFactor) {
      this.rsOperator = rsOperator;
      this.filterOperator = filterOperator;
      this.colNames = ImmutableSet.copyOf(colNames);
      this.filterStats = filterStats;
      this.reductionFactor = reductionFactor;
    }
  }

  private void markSemiJoinForDPP(OptimizeTezProcContext procCtx)
          throws SemanticException {
    // Stores the Tablescan operators processed to avoid redoing them.
    Map<ReduceSinkOperator, SemiJoinBranchInfo> map = procCtx.parseContext.getRsToSemiJoinBranchInfo();

    for (ReduceSinkOperator rs : map.keySet()) {
      SemiJoinBranchInfo sjInfo = map.get(rs);
      TableScanOperator ts = sjInfo.getTsOp();

      if (sjInfo.getIsHint() || !sjInfo.getShouldRemove()) {
        continue;
      }

      // A TS can have multiple branches due to DPP Or Semijoin Opt.
      // Use DFS to traverse all the branches until RS or DPP is hit.
      Deque<Operator<?>> deque = new LinkedList<>();
      deque.add(ts);
      while (!deque.isEmpty()) {
        Operator<?> op = deque.pollLast();
        if (op instanceof AppMasterEventOperator &&
                ((AppMasterEventOperator) op).getConf() instanceof DynamicPruningEventDesc) {
          // DPP. Now look up nDVs on both sides to see the selectivity.
          // <Parent Ops>-SEL-GB1-RS1-GB2-RS2
          SelectOperator selOp = OperatorUtils.ancestor(rs, SelectOperator.class, 0, 0, 0, 0);

          try {
            // Get nDVs on Semijoin edge side
            Statistics stats = selOp.getStatistics();
            if (stats == null) {
              // No stats found on semijoin edge, do nothing
              break;
            }
            String selCol = ExprNodeDescUtils.extractColName(
                    selOp.getConf().getColList().get(0));
            ColStatistics colStatisticsSJ = stats
                    .getColumnStatisticsFromColName(selCol);
            if (colStatisticsSJ == null) {
              // No column stats found for semijoin edge
              break;
            }
            long nDVs = colStatisticsSJ.getCountDistint();
            if (nDVs > 0) {
              // Lookup nDVs on TS side.
              RuntimeValuesInfo rti = procCtx.parseContext
                      .getRsToRuntimeValuesInfoMap().get(rs);
              // TODO Handle multi column semi-joins as part of HIVE-23934
              ExprNodeDesc tsExpr = rti.getTargetColumns().get(0);
              FilterOperator fil = (FilterOperator) (ts.getChildOperators().get(0));
              Statistics filStats = fil.getStatistics();
              if (filStats == null) {
                // No stats found on target, do nothing
                break;
              }
              String colName = ExprNodeDescUtils.extractColName(tsExpr);
              ColStatistics colStatisticsTarget = filStats
                      .getColumnStatisticsFromColName(colName);
              if (colStatisticsTarget == null) {
                // No column stats found on target
                break;
              }
              long nDVsOfTS = colStatisticsTarget.getCountDistint();
              double nDVsOfTSFactored = nDVsOfTS * procCtx.conf.getFloatVar(
                      ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION_FOR_DPP_FACTOR);
              if ((long)nDVsOfTSFactored > nDVs) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("nDVs = " + nDVs + ", nDVsOfTS = " + nDVsOfTS + " and nDVsOfTSFactored = " + nDVsOfTSFactored
                  + "Adding semijoin branch from ReduceSink " + rs + " to TS " + sjInfo.getTsOp());
                }
                sjInfo.setShouldRemove(false);
              }
            }
          } catch (NullPointerException e) {
            // Do nothing
            if (LOG.isDebugEnabled()) {
              LOG.debug("Caught NPE in markSemiJoinForDPP from ReduceSink " + rs + " to TS " + sjInfo.getTsOp());
            }
          }
          break;
        }
        if (op instanceof TerminalOperator) {
          // Done with this branch
          continue;
        }
        deque.addAll(op.getChildOperators());
      }
    }
  }

  private void bucketingVersionSanityCheck(OptimizeTezProcContext procCtx) throws SemanticException {
    // Fetch all the FileSinkOperators.
    Set<FileSinkOperator> fsOpsAll = new HashSet<>();
    for (TableScanOperator ts : procCtx.parseContext.getTopOps().values()) {
      Set<FileSinkOperator> fsOps = OperatorUtils.findOperators(
          ts, FileSinkOperator.class);
      fsOpsAll.addAll(fsOps);
    }

    Map<Operator<?>, Integer> processedOperators = new IdentityHashMap<>();
    for (FileSinkOperator fsOp : fsOpsAll) {
      // Look for direct parent ReduceSinkOp
      // If there are more than 1 parent, bail out.
      Operator<?> parent = fsOp;
      List<Operator<?>> parentOps = parent.getParentOperators();
      while (parentOps != null && parentOps.size() == 1) {
        parent = parentOps.get(0);
        if (!(parent instanceof ReduceSinkOperator)) {
          parentOps = parent.getParentOperators();
          continue;
        }

        // Found the target RSOp 0
        int bucketingVersion = fsOp.getConf().getTableInfo().getBucketingVersion();
        if (fsOp.getConf().getTableInfo().getBucketingVersion() == -1) {
          break;
        }
        if (fsOp.getConf().getTableInfo().getBucketingVersion() != fsOp.getConf().getBucketingVersion()) {
          throw new RuntimeException("FsOp bucketingVersions is inconsistent with its tableinfo");
        }
        if (processedOperators.containsKey(parent) && processedOperators.get(parent) != bucketingVersion) {
          throw new SemanticException(String.format(
              "Operator (%s) is already processed and is using bucketingVersion(%d); so it can't be changed to %d ",
              parent, processedOperators.get(parent), bucketingVersion));
        }
        processedOperators.put(parent, bucketingVersion);

        break;
      }
    }
  }
}
