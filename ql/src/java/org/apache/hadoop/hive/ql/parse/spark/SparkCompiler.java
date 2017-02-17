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
package org.apache.hadoop.hive.ql.parse.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.CompositeProcessor;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagate;
import org.apache.hadoop.hive.ql.optimizer.DynamicPartitionPruningOptimization;
import org.apache.hadoop.hive.ql.optimizer.SparkRemoveDynamicPruningBySize;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.physical.AnnotateRunTimeStatsOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.NullScanOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.SparkCrossProductCheck;
import org.apache.hadoop.hive.ql.optimizer.physical.SparkMapJoinResolver;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.optimizer.spark.CombineEquivalentWorkResolver;
import org.apache.hadoop.hive.ql.optimizer.spark.SetSparkReducerParallelism;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkJoinHintOptimizer;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkJoinOptimizer;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkSkewJoinResolver;
import org.apache.hadoop.hive.ql.optimizer.spark.SplitSparkWorkResolver;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * SparkCompiler translates the operator plan into SparkTasks.
 *
 * Cloned from TezCompiler.
 */
public class SparkCompiler extends TaskCompiler {
  private static final String CLASS_NAME = SparkCompiler.class.getName();
  private static final PerfLogger PERF_LOGGER = SessionState.getPerfLogger();

  public SparkCompiler() {
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_OPERATOR_TREE);

    OptimizeSparkProcContext procCtx = new OptimizeSparkProcContext(conf, pCtx, inputs, outputs);

    // Run Spark Dynamic Partition Pruning
    runDynamicPartitionPruning(procCtx);

    // Annotation OP tree with statistics
    runStatsAnnotation(procCtx);

    // Set reducer parallelism
    runSetReducerParallelism(procCtx);

    // Run Join releated optimizations
    runJoinOptimizations(procCtx);

    // Remove cyclic dependencies for DPP
    runCycleAnalysisForPartitionPruning(procCtx);

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_OPERATOR_TREE);
  }

  private void runCycleAnalysisForPartitionPruning(OptimizeSparkProcContext procCtx) {
    if (!conf.getBoolVar(HiveConf.ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING)) {
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
          removeDPPOperator(component, procCtx);
          break;
        }
      }
      LOG.info("Cycle free: " + cycleFree);
    }
  }

  private void removeDPPOperator(Set<Operator<?>> component, OptimizeSparkProcContext context) {
    SparkPartitionPruningSinkOperator toRemove = null;
    for (Operator<?> o : component) {
      if (o instanceof SparkPartitionPruningSinkOperator) {
        // we want to remove the DPP with bigger data size
        if (toRemove == null
            || o.getConf().getStatistics().getDataSize() > toRemove.getConf().getStatistics()
            .getDataSize()) {
          toRemove = (SparkPartitionPruningSinkOperator) o;
        }
      }
    }

    if (toRemove == null) {
      return;
    }

    OperatorUtils.removeBranch(toRemove);
    // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
    LOG.info("Disabling dynamic pruning for: "
        + toRemove.getConf().getTableScan().toString() + ". Needed to break cyclic dependency");
  }

  // Tarjan's algo
  private Set<Set<Operator<?>>> getComponents(OptimizeSparkProcContext procCtx) {
    AtomicInteger index = new AtomicInteger();
    Map<Operator<?>, Integer> indexes = new HashMap<Operator<?>, Integer>();
    Map<Operator<?>, Integer> lowLinks = new HashMap<Operator<?>, Integer>();
    Stack<Operator<?>> nodes = new Stack<Operator<?>>();
    Set<Set<Operator<?>>> components = new HashSet<Set<Operator<?>>>();

    for (Operator<?> o : procCtx.getParseContext().getTopOps().values()) {
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
    if (o instanceof SparkPartitionPruningSinkOperator) {
      children = new ArrayList<>();
      children.addAll(o.getChildOperators());
      TableScanOperator ts = ((SparkPartitionPruningSinkDesc) o.getConf()).getTableScan();
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

  private void runStatsAnnotation(OptimizeSparkProcContext procCtx) throws SemanticException {
    new AnnotateWithStatistics().transform(procCtx.getParseContext());
    new AnnotateWithOpTraits().transform(procCtx.getParseContext());
  }

  private void runDynamicPartitionPruning(OptimizeSparkProcContext procCtx)
      throws SemanticException {
    if (!conf.getBoolVar(HiveConf.ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING)) {
      return;
    }

    ParseContext parseContext = procCtx.getParseContext();
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(
        new RuleRegExp(new String("Dynamic Partition Pruning"),
            FilterOperator.getOperatorName() + "%"),
        new DynamicPartitionPruningOptimization());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new ForwardWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(parseContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    // need a new run of the constant folding because we might have created lots
    // of "and true and true" conditions.
    if(procCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVEOPTCONSTANTPROPAGATION)) {
      new ConstantPropagate().transform(parseContext);
    }
  }

  private void runSetReducerParallelism(OptimizeSparkProcContext procCtx) throws SemanticException {
    ParseContext pCtx = procCtx.getParseContext();
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Set parallelism - ReduceSink",
            ReduceSinkOperator.getOperatorName() + "%"),
        new SetSparkReducerParallelism(pCtx.getConf()));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }

  private void runJoinOptimizations(OptimizeSparkProcContext procCtx) throws SemanticException {
    ParseContext pCtx = procCtx.getParseContext();
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules.put(new TypeRule(JoinOperator.class), new SparkJoinOptimizer(pCtx));

    opRules.put(new TypeRule(MapJoinOperator.class), new SparkJoinHintOptimizer(pCtx));

    opRules.put(new RuleRegExp("Disabling Dynamic Partition Pruning By Size",
        SparkPartitionPruningSinkOperator.getOperatorName() + "%"),
        new SparkRemoveDynamicPruningBySize());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }

  /**
   * TODO: need to turn on rules that's commented out and add more if necessary.
   */
  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_GENERATE_TASK_TREE);

    GenSparkUtils utils = GenSparkUtils.getUtils();
    utils.resetSequenceNumber();

    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenSparkProcContext procCtx = new GenSparkProcContext(
        conf, tempParseContext, mvTask, rootTasks, inputs, outputs, pCtx.getTopOps());

    // -------------------------------- First Pass ---------------------------------- //
    // Identify SparkPartitionPruningSinkOperators, and break OP tree if necessary

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Clone OP tree for PartitionPruningSink",
            SparkPartitionPruningSinkOperator.getOperatorName() + "%"),
        new SplitOpTreeForDPP());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new GenSparkWorkWalker(disp, procCtx);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    // -------------------------------- Second Pass ---------------------------------- //
    // Process operator tree in two steps: first we process the extra op trees generated
    // in the first pass. Then we process the main op tree, and the result task will depend
    // on the task generated in the first pass.
    topNodes.clear();
    topNodes.addAll(procCtx.topOps.values());
    generateTaskTreeHelper(procCtx, topNodes);

    // If this set is not empty, it means we need to generate a separate task for collecting
    // the partitions used.
    if (!procCtx.clonedPruningTableScanSet.isEmpty()) {
      SparkTask pruningTask = SparkUtilities.createSparkTask(conf);
      SparkTask mainTask = procCtx.currentTask;
      pruningTask.addDependentTask(procCtx.currentTask);
      procCtx.rootTasks.remove(procCtx.currentTask);
      procCtx.rootTasks.add(pruningTask);
      procCtx.currentTask = pruningTask;

      topNodes.clear();
      topNodes.addAll(procCtx.clonedPruningTableScanSet);
      generateTaskTreeHelper(procCtx, topNodes);

      procCtx.currentTask = mainTask;
    }

    // -------------------------------- Post Pass ---------------------------------- //

    // we need to clone some operator plans and remove union operators still
    for (BaseWork w : procCtx.workWithUnionOperators) {
      GenSparkUtils.getUtils().removeUnionOperators(procCtx, w);
    }

    // we need to fill MapWork with 'local' work and bucket information for SMB Join.
    GenSparkUtils.getUtils().annotateMapWork(procCtx);

    // finally make sure the file sink operators are set up right
    for (FileSinkOperator fileSink : procCtx.fileSinkSet) {
      GenSparkUtils.getUtils().processFileSink(procCtx, fileSink);
    }

    // Process partition pruning sinks
    for (Operator<?> prunerSink : procCtx.pruningSinkSet) {
      utils.processPartitionPruningSink(procCtx, (SparkPartitionPruningSinkOperator) prunerSink);
    }

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_GENERATE_TASK_TREE);
  }

  private void generateTaskTreeHelper(GenSparkProcContext procCtx, List<Node> topNodes)
    throws SemanticException {
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    GenSparkWork genSparkWork = new GenSparkWork(GenSparkUtils.getUtils());

    opRules.put(new RuleRegExp("Split Work - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"), genSparkWork);

    opRules.put(new RuleRegExp("Split Work - SparkPartitionPruningSink",
        SparkPartitionPruningSinkOperator.getOperatorName() + "%"), genSparkWork);

    opRules.put(new TypeRule(MapJoinOperator.class), new SparkReduceSinkMapJoinProc());

    opRules.put(new RuleRegExp("Split Work + Move/Merge - FileSink",
        FileSinkOperator.getOperatorName() + "%"),
        new CompositeProcessor(new SparkFileSinkProcessor(), genSparkWork));

    opRules.put(new RuleRegExp("Handle Analyze Command",
        TableScanOperator.getOperatorName() + "%"),
        new SparkProcessAnalyzeTable(GenSparkUtils.getUtils()));

    opRules.put(new RuleRegExp("Remember union", UnionOperator.getOperatorName() + "%"),
        new NodeProcessor() {
          @Override
          public Object process(Node n, Stack<Node> s,
                                NodeProcessorCtx procCtx, Object... os) throws SemanticException {
            GenSparkProcContext context = (GenSparkProcContext) procCtx;
            UnionOperator union = (UnionOperator) n;

            // simply need to remember that we've seen a union.
            context.currentUnionOperators.add(union);
            return null;
          }
        }
    );

    /**
     *  SMB join case:   (Big)   (Small)  (Small)
     *                     TS       TS       TS
     *                      \       |       /
     *                       \      DS     DS
     *                         \   |    /
     *                         SMBJoinOP
     *
     * Some of the other processors are expecting only one traversal beyond SMBJoinOp.
     * We need to traverse from the big-table path only, and stop traversing on the
     * small-table path once we reach SMBJoinOp.
     * Also add some SMB join information to the context, so we can properly annotate
     * the MapWork later on.
     */
    opRules.put(new TypeRule(SMBMapJoinOperator.class),
      new NodeProcessor() {
        @Override
        public Object process(Node currNode, Stack<Node> stack,
                              NodeProcessorCtx procCtx, Object... os) throws SemanticException {
          GenSparkProcContext context = (GenSparkProcContext) procCtx;
          SMBMapJoinOperator currSmbNode = (SMBMapJoinOperator) currNode;
          SparkSMBMapJoinInfo smbMapJoinCtx = context.smbMapJoinCtxMap.get(currSmbNode);
          if (smbMapJoinCtx == null) {
            smbMapJoinCtx = new SparkSMBMapJoinInfo();
            context.smbMapJoinCtxMap.put(currSmbNode, smbMapJoinCtx);
          }

          for (Node stackNode : stack) {
            if (stackNode instanceof DummyStoreOperator) {
              //If coming from small-table side, do some book-keeping, and skip traversal.
              smbMapJoinCtx.smallTableRootOps.add(context.currentRootOperator);
              return true;
            }
          }
          //If coming from big-table side, do some book-keeping, and continue traversal
          smbMapJoinCtx.bigTableRootOp = context.currentRootOperator;
          return false;
        }
      }
    );

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new GenSparkWorkWalker(disp, procCtx);
    ogw.startWalking(topNodes, null);
  }

  @Override
  protected void setInputFormat(Task<? extends Serializable> task) {
    if (task instanceof SparkTask) {
      SparkWork work = ((SparkTask)task).getWork();
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
      GlobalLimitCtx globalLimitCtx) throws SemanticException {
    // currently all Spark work is on the cluster
    return;
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      Context ctx) throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_TASK_TREE);
    PhysicalContext physicalCtx = new PhysicalContext(conf, pCtx, pCtx.getContext(), rootTasks,
       pCtx.getFetchTask());

    physicalCtx = new SplitSparkWorkResolver().resolve(physicalCtx);

    if (conf.getBoolVar(HiveConf.ConfVars.HIVESKEWJOIN)) {
      (new SparkSkewJoinResolver()).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping runtime skew join optimization");
    }

    physicalCtx = new SparkMapJoinResolver().resolve(physicalCtx);

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
      physicalCtx = new SparkCrossProductCheck().resolve(physicalCtx);
    } else {
      LOG.debug("Skipping cross product analysis");
    }

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)
        && ctx.getExplainAnalyze() == null) {
      (new Vectorizer()).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping vectorization");
    }

    if (!"none".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE))) {
      (new StageIDsRearranger()).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping stage id rearranger");
    }

    new CombineEquivalentWorkResolver().resolve(physicalCtx);

    if (physicalCtx.getContext().getExplainAnalyze() != null) {
      new AnnotateRunTimeStatsOptimizer().resolve(physicalCtx);
    }

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_TASK_TREE);
    return;
  }
}
