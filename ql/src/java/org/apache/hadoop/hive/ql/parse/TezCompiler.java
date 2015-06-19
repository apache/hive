/**
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.CompositeProcessor;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagate;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcCtx.ConstantPropagateOption;
import org.apache.hadoop.hive.ql.optimizer.ConvertJoinMapJoin;
import org.apache.hadoop.hive.ql.optimizer.DynamicPartitionPruningOptimization;
import org.apache.hadoop.hive.ql.optimizer.MergeJoinProc;
import org.apache.hadoop.hive.ql.optimizer.ReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.optimizer.RemoveDynamicPruningBySize;
import org.apache.hadoop.hive.ql.optimizer.SetReducerParallelism;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.physical.CrossProductCheck;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.NullScanOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * TezCompiler translates the operator plan into TezTasks.
 */
public class TezCompiler extends TaskCompiler {

  protected final Log LOG = LogFactory.getLog(TezCompiler.class);

  public TezCompiler() {
  }

  @Override
  public void init(HiveConf conf, LogHelper console, Hive db) {
    super.init(conf, console, db);

    // Tez requires us to use RPC for the query plan
    HiveConf.setBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN, true);

    // We require the use of recursive input dirs for union processing
    conf.setBoolean("mapred.input.dir.recursive", true);
    HiveConf.setBoolVar(conf, ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES, true);
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {

    // Create the context for the walker
    OptimizeTezProcContext procCtx = new OptimizeTezProcContext(conf, pCtx, inputs, outputs);

    // setup dynamic partition pruning where possible
    runDynamicPartitionPruning(procCtx, inputs, outputs);

    // setup stats in the operator plan
    runStatsAnnotation(procCtx);

    // run the optimizations that use stats for optimization
    runStatsDependentOptimizations(procCtx, inputs, outputs);

    // after the stats phase we might have some cyclic dependencies that we need
    // to take care of.
    runCycleAnalysisForPartitionPruning(procCtx, inputs, outputs);

  }

  private void runCycleAnalysisForPartitionPruning(OptimizeTezProcContext procCtx,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {

    if (!procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING)) {
      return;
    }

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
          removeEventOperator(component, procCtx);
          break;
        }
      }
      LOG.info("Cycle free: " + cycleFree);
    }
  }

  private void removeEventOperator(Set<Operator<?>> component, OptimizeTezProcContext context) {
    AppMasterEventOperator victim = null;
    for (Operator<?> o : component) {
      if (o instanceof AppMasterEventOperator) {
        if (victim == null
            || o.getConf().getStatistics().getDataSize() < victim.getConf().getStatistics()
                .getDataSize()) {
          victim = (AppMasterEventOperator) o;
        }
      }
    }

    if (victim == null ||
        (!context.pruningOpsRemovedByPriorOpt.isEmpty() &&
         context.pruningOpsRemovedByPriorOpt.contains(victim))) {
      return;
    }

    GenTezUtils.getUtils().removeBranch(victim);
    // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
    LOG.info("Disabling dynamic pruning for: "
        + ((DynamicPruningEventDesc) victim.getConf()).getTableScan().toString()
        + ". Needed to break cyclic dependency");
  }

  // Tarjan's algo
  private Set<Set<Operator<?>>> getComponents(OptimizeTezProcContext procCtx) {
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    AtomicInteger index = new AtomicInteger();
    Map<Operator<?>, Integer> indexes = new HashMap<Operator<?>, Integer>();
    Map<Operator<?>, Integer> lowLinks = new HashMap<Operator<?>, Integer>();
    Stack<Operator<?>> nodes = new Stack<Operator<?>>();
    Set<Set<Operator<?>>> components = new HashSet<Set<Operator<?>>>();

    for (Operator<?> o : deque) {
      if (!indexes.containsKey(o)) {
        connect(o, index, nodes, indexes, lowLinks, components);
      }
    }

    return components;
  }

  private void connect(Operator<?> o, AtomicInteger index, Stack<Operator<?>> nodes,
      Map<Operator<?>, Integer> indexes, Map<Operator<?>, Integer> lowLinks,
      Set<Set<Operator<?>>> components) {

    indexes.put(o, index.get());
    lowLinks.put(o, index.get());
    index.incrementAndGet();
    nodes.push(o);

    List<Operator<?>> children;
    if (o instanceof AppMasterEventOperator) {
      children = new ArrayList<Operator<?>>();
      children.addAll(o.getChildOperators());
      TableScanOperator ts = ((DynamicPruningEventDesc) o.getConf()).getTableScan();
      LOG.debug("Adding special edge: " + o.getName() + " --> " + ts.toString());
      children.add(ts);
    } else {
      children = o.getChildOperators();
    }

    for (Operator<?> child : children) {
      if (!indexes.containsKey(child)) {
        connect(child, index, nodes, indexes, lowLinks, components);
        lowLinks.put(o, Math.min(lowLinks.get(o), lowLinks.get(child)));
      } else if (nodes.contains(child)) {
        lowLinks.put(o, Math.min(lowLinks.get(o), indexes.get(child)));
      }
    }

    if (lowLinks.get(o).equals(indexes.get(o))) {
      Set<Operator<?>> component = new HashSet<Operator<?>>();
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

  private void runStatsDependentOptimizations(OptimizeTezProcContext procCtx,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Set parallelism - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"),
        new SetReducerParallelism());

    opRules.put(new RuleRegExp("Convert Join to Map-join",
        JoinOperator.getOperatorName() + "%"), new ConvertJoinMapJoin());

    opRules.put(
        new RuleRegExp("Remove dynamic pruning by size",
        AppMasterEventOperator.getOperatorName() + "%"),
        new RemoveDynamicPruningBySize());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    GraphWalker ogw = new ForwardWalker(disp);
    ogw.startWalking(topNodes, null);
  }

  private void runDynamicPartitionPruning(OptimizeTezProcContext procCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {

    if (!procCtx.conf.getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING)) {
      return;
    }

    // Sequence of TableScan operators to be walked
    Deque<Operator<?>> deque = new LinkedList<Operator<?>>();
    deque.addAll(procCtx.parseContext.getTopOps().values());

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(
        new RuleRegExp(new String("Dynamic Partition Pruning"), FilterOperator.getOperatorName()
            + "%"), new DynamicPartitionPruningOptimization());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(procCtx.parseContext.getTopOps().values());
    GraphWalker ogw = new ForwardWalker(disp);
    ogw.startWalking(topNodes, null);

    // need a new run of the constant folding because we might have created lots
    // of "and true and true" conditions.
    // Rather than run the full constant folding just need to shortcut AND/OR expressions
    // involving constant true/false values.
    if(procCtx.conf.getBoolVar(ConfVars.HIVEOPTCONSTANTPROPAGATION)) {
      new ConstantPropagate(ConstantPropagateOption.SHORTCUT).transform(procCtx.parseContext);
    }
  }

  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {

    GenTezUtils.getUtils().resetSequenceNumber();

    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenTezWork genTezWork = new GenTezWork(GenTezUtils.getUtils());

    GenTezProcContext procCtx = new GenTezProcContext(
        conf, tempParseContext, mvTask, rootTasks, inputs, outputs);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Split Work - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"),
        genTezWork);

    opRules.put(new RuleRegExp("No more walking on ReduceSink-MapJoin",
        MapJoinOperator.getOperatorName() + "%"), new ReduceSinkMapJoinProc());

    opRules.put(new RuleRegExp("Recoginze a Sorted Merge Join operator to setup the right edge and"
        + " stop traversing the DummyStore-MapJoin", CommonMergeJoinOperator.getOperatorName()
        + "%"), new MergeJoinProc());

    opRules.put(new RuleRegExp("Split Work + Move/Merge - FileSink",
        FileSinkOperator.getOperatorName() + "%"),
        new CompositeProcessor(new FileSinkProcessor(), genTezWork));

    opRules.put(new RuleRegExp("Split work - DummyStore", DummyStoreOperator.getOperatorName()
        + "%"), genTezWork);

    opRules.put(new RuleRegExp("Handle Potential Analyze Command",
        TableScanOperator.getOperatorName() + "%"),
        new ProcessAnalyzeTable(GenTezUtils.getUtils()));

    opRules.put(new RuleRegExp("Remember union",
        UnionOperator.getOperatorName() + "%"),
        new UnionProcessor());

    opRules.put(new RuleRegExp("AppMasterEventOperator",
        AppMasterEventOperator.getOperatorName() + "%"),
        new AppMasterEventProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    GraphWalker ogw = new GenTezWorkWalker(disp, procCtx);
    ogw.startWalking(topNodes, null);

    // we need to clone some operator plans and remove union operators still
    for (BaseWork w: procCtx.workWithUnionOperators) {
      GenTezUtils.getUtils().removeUnionOperators(conf, procCtx, w);
    }

    // then we make sure the file sink operators are set up right
    for (FileSinkOperator fileSink: procCtx.fileSinkSet) {
      GenTezUtils.getUtils().processFileSink(procCtx, fileSink);
    }

    // and finally we hook up any events that need to be sent to the tez AM
    LOG.debug("There are " + procCtx.eventOperatorSet.size() + " app master events.");
    for (AppMasterEventOperator event : procCtx.eventOperatorSet) {
      LOG.debug("Handling AppMasterEventOperator: " + event);
      GenTezUtils.getUtils().processAppMasterEvent(procCtx, event);
    }
  }

  @Override
  protected void setInputFormat(Task<? extends Serializable> task) {
    if (task instanceof TezTask) {
      TezWork work = ((TezTask)task).getWork();
      List<BaseWork> all = work.getAllWork();
      for (BaseWork w: all) {
        if (w instanceof MapWork) {
          MapWork mapWork = (MapWork) w;
          HashMap<String, Operator<? extends OperatorDesc>> opMap = mapWork.getAliasToWork();
          if (!opMap.isEmpty()) {
            for (Operator<? extends OperatorDesc> op : opMap.values()) {
              setInputFormat(mapWork, op);
            }
          }
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks
        = ((ConditionalTask) task).getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        setInputFormat(tsk);
      }
    }

    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> childTask : task.getChildTasks()) {
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
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx)
      throws SemanticException {
    // currently all Tez work is on the cluster
    return;
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      Context ctx) throws SemanticException {
    PhysicalContext physicalCtx = new PhysicalContext(conf, pCtx, pCtx.getContext(), rootTasks,
       pCtx.getFetchTask());

    if (conf.getBoolVar(HiveConf.ConfVars.HIVENULLSCANOPTIMIZE)) {
      physicalCtx = new NullScanOptimizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping null scan query optimization");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES)) {
      physicalCtx = new MetadataOnlyOptimizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping metadata only query optimization");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_CHECK_CROSS_PRODUCT)) {
      physicalCtx = new CrossProductCheck().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping cross product analysis");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
      physicalCtx = new Vectorizer().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping vectorization");
    }

    if (!"none".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE))) {
      physicalCtx = new StageIDsRearranger().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping stage id rearranger");
    }
    return;
  }
}
