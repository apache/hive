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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.GenMRFileSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMROperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink1;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink2;
import org.apache.hadoop.hive.ql.optimizer.GenMRRedSink3;
import org.apache.hadoop.hive.ql.optimizer.GenMRTableScan1;
import org.apache.hadoop.hive.ql.optimizer.GenMRUnion1;
import org.apache.hadoop.hive.ql.optimizer.MapJoinFactory;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalOptimizer;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.shims.ShimLoader;

public class MapReduceCompiler extends TaskCompiler {

  protected final Logger LOG = LoggerFactory.getLogger(MapReduceCompiler.class);

  public MapReduceCompiler() {
  }

  // loop over all the tasks recursively
  @Override
  protected void setInputFormat(Task<? extends Serializable> task) {
    if (task instanceof ExecDriver) {
      MapWork work = ((MapredWork) task.getWork()).getMapWork();
      HashMap<String, Operator<? extends OperatorDesc>> opMap = work.getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends OperatorDesc> op : opMap.values()) {
          setInputFormat(work, op);
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

  // loop over all the tasks recursively
  private void breakTaskTree(Task<? extends Serializable> task) {

    if (task instanceof ExecDriver) {
      HashMap<String, Operator<? extends OperatorDesc>> opMap = ((MapredWork) task
          .getWork()).getMapWork().getAliasToWork();
      if (!opMap.isEmpty()) {
        for (Operator<? extends OperatorDesc> op : opMap.values()) {
          breakOperatorTree(op);
        }
      }
    } else if (task instanceof ConditionalTask) {
      List<Task<? extends Serializable>> listTasks = ((ConditionalTask) task)
          .getListTasks();
      for (Task<? extends Serializable> tsk : listTasks) {
        breakTaskTree(tsk);
      }
    }

    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      breakTaskTree(childTask);
    }
  }

  // loop over all the operators recursively
  private void breakOperatorTree(Operator<? extends OperatorDesc> topOp) {
    if (topOp instanceof ReduceSinkOperator) {
      topOp.setChildOperators(null);
    }

    for (Operator<? extends OperatorDesc> op : topOp.getChildOperators()) {
      breakOperatorTree(op);
    }
  }

  /**
   * Make a best guess at trying to find the number of reducers
   */
  private static int getNumberOfReducers(MapredWork mrwork, HiveConf conf) {
    if (mrwork.getReduceWork() == null) {
      return 0;
    }

    if (mrwork.getReduceWork().getNumReduceTasks() >= 0) {
      return mrwork.getReduceWork().getNumReduceTasks();
    }

    return conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
  }

  @Override
  protected void decideExecMode(List<Task<? extends Serializable>> rootTasks, Context ctx,
      GlobalLimitCtx globalLimitCtx)
      throws SemanticException {

    // bypass for explain queries for now
    if (ctx.isExplainSkipExecution()) {
      return;
    }

    // user has told us to run in local mode or doesn't want auto-local mode
    if (ctx.isLocalOnlyExecutionMode() ||
        !conf.getBoolVar(HiveConf.ConfVars.LOCALMODEAUTO)) {
      return;
    }

    final Context lCtx = ctx;
    PathFilter p = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return !lCtx.isMRTmpFileURI(file.toUri().getPath());
      }
    };
    List<ExecDriver> mrtasks = Utilities.getMRTasks(rootTasks);

    // map-reduce jobs will be run locally based on data size
    // first find out if any of the jobs needs to run non-locally
    boolean hasNonLocalJob = false;
    for (ExecDriver mrtask : mrtasks) {
      try {
        ContentSummary inputSummary = Utilities.getInputSummary
            (ctx, mrtask.getWork().getMapWork(), p);
        int numReducers = getNumberOfReducers(mrtask.getWork(), conf);

        long estimatedInput;

        if (globalLimitCtx != null && globalLimitCtx.isEnable()) {
          // If the global limit optimization is triggered, we will
          // estimate input data actually needed based on limit rows.
          // estimated Input = (num_limit * max_size_per_row) * (estimated_map + 2)
          //
          long sizePerRow = HiveConf.getLongVar(conf,
              HiveConf.ConfVars.HIVELIMITMAXROWSIZE);
          estimatedInput = (globalLimitCtx.getGlobalOffset() +
              globalLimitCtx.getGlobalLimit()) * sizePerRow;
          long minSplitSize = HiveConf.getLongVar(conf,
              HiveConf.ConfVars.MAPREDMINSPLITSIZE);
          long estimatedNumMap = inputSummary.getLength() / minSplitSize + 1;
          estimatedInput = estimatedInput * (estimatedNumMap + 1);
        } else {
          estimatedInput = inputSummary.getLength();
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Task: " + mrtask.getId() + ", Summary: " +
              inputSummary.getLength() + "," + inputSummary.getFileCount() + ","
              + numReducers + ", estimated Input: " + estimatedInput);
        }

        if (MapRedTask.isEligibleForLocalMode(conf, numReducers,
            estimatedInput, inputSummary.getFileCount()) != null) {
          hasNonLocalJob = true;
          break;
        } else {
          mrtask.setLocalMode(true);
        }
      } catch (IOException e) {
        throw new SemanticException(e);
      }
    }

    if (!hasNonLocalJob) {
      // Entire query can be run locally.
      // Save the current tracker value and restore it when done.
      ctx.setOriginalTracker(ShimLoader.getHadoopShims().getJobLauncherRpcAddress(conf));
      ShimLoader.getHadoopShims().setJobLauncherRpcAddress(conf, "local");
      console.printInfo("Automatically selecting local only mode for query");
    }
  }

  @Override
  protected void optimizeTaskPlan(List<Task<? extends Serializable>> rootTasks,
      ParseContext pCtx, Context ctx) throws SemanticException {
    // reduce sink does not have any kids - since the plan by now has been
    // broken up into multiple
    // tasks, iterate over all tasks.
    // For each task, go over all operators recursively
    for (Task<? extends Serializable> rootTask : rootTasks) {
      breakTaskTree(rootTask);
    }


    PhysicalContext physicalContext = new PhysicalContext(conf,
        getParseContext(pCtx, rootTasks), ctx, rootTasks, pCtx.getFetchTask());
    PhysicalOptimizer physicalOptimizer = new PhysicalOptimizer(
        physicalContext, conf);
    physicalOptimizer.optimize();

  }

  @Override
  protected void generateTaskTree(List<Task<? extends Serializable>> rootTasks, ParseContext pCtx,
      List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {

    // generate map reduce plans
    ParseContext tempParseContext = getParseContext(pCtx, rootTasks);
    GenMRProcContext procCtx = new GenMRProcContext(
        conf,
        // Must be deterministic order map for consistent q-test output across Java versions
        new LinkedHashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>>(),
        tempParseContext, mvTask, rootTasks,
        new LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx>(),
        inputs, outputs);

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack.
    // The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp(new String("R1"),
        TableScanOperator.getOperatorName() + "%"),
        new GenMRTableScan1());
    opRules.put(new RuleRegExp(new String("R2"),
        TableScanOperator.getOperatorName() + "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
        new GenMRRedSink1());
    opRules.put(new RuleRegExp(new String("R3"),
        ReduceSinkOperator.getOperatorName() + "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
        new GenMRRedSink2());
    opRules.put(new RuleRegExp(new String("R4"),
        FileSinkOperator.getOperatorName() + "%"),
        new GenMRFileSink1());
    opRules.put(new RuleRegExp(new String("R5"),
        UnionOperator.getOperatorName() + "%"),
        new GenMRUnion1());
    opRules.put(new RuleRegExp(new String("R6"),
        UnionOperator.getOperatorName() + "%.*" + ReduceSinkOperator.getOperatorName() + "%"),
        new GenMRRedSink3());
    opRules.put(new RuleRegExp(new String("R7"),
        MapJoinOperator.getOperatorName() + "%"),
        MapJoinFactory.getTableScanMapJoin());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(new GenMROperator(), opRules,
        procCtx);

    GraphWalker ogw = new GenMapRedWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }
}
