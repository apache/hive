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
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.CompositeProcessor;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.optimizer.physical.MetadataOnlyOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.NullScanOptimizer;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.SparkCrossProductCheck;
import org.apache.hadoop.hive.ql.optimizer.physical.SparkMapJoinResolver;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.apache.hadoop.hive.ql.optimizer.spark.SetSparkReducerParallelism;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkJoinHintOptimizer;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkJoinOptimizer;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkReduceSinkMapJoinProc;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkSkewJoinResolver;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkSortMergeJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.spark.SplitSparkWorkResolver;
import org.apache.hadoop.hive.ql.parse.GlobalLimitCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TaskCompiler;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * SparkCompiler translates the operator plan into SparkTasks.
 *
 * Cloned from TezCompiler.
 */
public class SparkCompiler extends TaskCompiler {
  private static final String CLASS_NAME = SparkCompiler.class.getName();
  private static final PerfLogger PERF_LOGGER = PerfLogger.getPerfLogger();
  private static final Log LOGGER = LogFactory.getLog(SparkCompiler.class);

  public SparkCompiler() {
  }

  @Override
  protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_OPERATOR_TREE);
    // Sequence of TableScan operators to be walked
    Deque<Operator<? extends OperatorDesc>> deque = new LinkedList<Operator<? extends OperatorDesc>>();
    deque.addAll(pCtx.getTopOps().values());

    OptimizeSparkProcContext procCtx = new OptimizeSparkProcContext(conf, pCtx, inputs, outputs, deque);
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Set parallelism - ReduceSink",
      ReduceSinkOperator.getOperatorName() + "%"),
      new SetSparkReducerParallelism());

    opRules.put(new TypeRule(JoinOperator.class), new SparkJoinOptimizer(pCtx));

    opRules.put(new TypeRule(MapJoinOperator.class), new SparkJoinHintOptimizer(pCtx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, procCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_OPERATOR_TREE);
  }

  /**
   * TODO: need to turn on rules that's commented out and add more if necessary.
   */
  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
      throws SemanticException {
    PERF_LOGGER.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_GENERATE_TASK_TREE);
    GenSparkUtils.getUtils().resetSequenceNumber();

    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenSparkWork genSparkWork = new GenSparkWork(GenSparkUtils.getUtils());

    GenSparkProcContext procCtx = new GenSparkProcContext(
        conf, tempParseContext, mvTask, rootTasks, inputs, outputs, pCtx.getTopOps());

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("Split Work - ReduceSink",
        ReduceSinkOperator.getOperatorName() + "%"), genSparkWork);

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
     * We need to traverse from the big-table path only, and stop traversing on the small-table path once we reach SMBJoinOp.
     * Also add some SMB join information to the context, so we can properly annotate the MapWork later on.
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
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    GraphWalker ogw = new GenSparkWorkWalker(disp, procCtx);
    ogw.startWalking(topNodes, null);

    // we need to clone some operator plans and remove union operators still
    for (BaseWork w: procCtx.workWithUnionOperators) {
      GenSparkUtils.getUtils().removeUnionOperators(conf, procCtx, w);
    }

    // we need to fill MapWork with 'local' work and bucket information for SMB Join.
    GenSparkUtils.getUtils().annotateMapWork(procCtx);

    // finally make sure the file sink operators are set up right
    for (FileSinkOperator fileSink: procCtx.fileSinkSet) {
      GenSparkUtils.getUtils().processFileSink(procCtx, fileSink);
    }

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_GENERATE_TASK_TREE);
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

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
      (new Vectorizer()).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping vectorization");
    }

    if (!"none".equalsIgnoreCase(conf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE))) {
      (new StageIDsRearranger()).resolve(physicalCtx);
    } else {
      LOG.debug("Skipping stage id rearranger");
    }

    PERF_LOGGER.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_OPTIMIZE_TASK_TREE);
    return;
  }
}
