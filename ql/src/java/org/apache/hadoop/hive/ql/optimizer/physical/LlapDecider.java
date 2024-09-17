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

package org.apache.hadoop.hive.ql.optimizer.physical;

import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.all;
import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.auto;
import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.only;
import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.map;
import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.none;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LlapDecider takes care of tagging certain vertices in the execution graph as
 * "llap", which in turn causes them to be submitted to an llap daemon instead
 * of a regular yarn container.
 *
 * The actual algorithm used is driven by LLAP_EXECUTION_MODE. "all", "none" and
 * "map" mechanically tag those elements. "auto" tries to be smarter by looking
 * for suitable vertices.
 *
 * Regardless of the algorithm used, it's always ensured that there's not user
 * code that will be sent to the daemon (ie.: script operators, temporary
 * functions, etc)
 */
public class LlapDecider implements PhysicalPlanResolver {

  protected static transient final Logger LOG = LoggerFactory.getLogger(LlapDecider.class);

  private HiveConf conf;

  public enum LlapMode {
    map, // map operators only
    all, // all operators. Launch containers if user code etc prevents running inside llap.
    none, // no operators
    only, // Try running everything in llap, fail if that is not possible (non blessed user code, script, etc)
    auto // please hive, choose for me
  }

  private LlapMode mode;
  private final LlapClusterStateForCompile clusterState;

  public LlapDecider(LlapClusterStateForCompile clusterState) {
    this.clusterState = clusterState;
  }


  class LlapDecisionDispatcher implements SemanticDispatcher {
    private final HiveConf conf;
    private final boolean doSkipUdfCheck;
    private final boolean arePermanentFnsAllowed;
    private final boolean shouldUber;
    private final float minReducersPerExec;
    private final int executorsPerNode;
    private List<MapJoinOperator> mapJoinOpList;
    private final Map<SemanticRule, SemanticNodeProcessor> rules;

    public LlapDecisionDispatcher(PhysicalContext pctx, LlapMode mode) {
      conf = pctx.getConf();
      doSkipUdfCheck = HiveConf.getBoolVar(conf, ConfVars.LLAP_SKIP_COMPILE_UDF_CHECK);
      arePermanentFnsAllowed = HiveConf.getBoolVar(conf, ConfVars.LLAP_ALLOW_PERMANENT_FNS);
      // Don't user uber in "all" mode - everything can go into LLAP, which is better than uber.
      shouldUber = HiveConf.getBoolVar(conf, ConfVars.LLAP_AUTO_ALLOW_UBER) && (mode != all);
      minReducersPerExec = HiveConf.getFloatVar(
          conf, ConfVars.TEZ_LLAP_MIN_REDUCER_PER_EXECUTOR);
      executorsPerNode = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
      mapJoinOpList = new ArrayList<MapJoinOperator>();
      rules = getRules();
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
      @SuppressWarnings("unchecked")
      Task<?> currTask = (Task<?>) nd;
      if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w: work.getAllWork()) {
          handleWork(work, w);
        }
      }
      return null;
    }

    private void handleWork(TezWork tezWork, BaseWork work) throws SemanticException {
      boolean workCanBeDoneInLlap = evaluateWork(tezWork, work);
      LOG.debug(
          "Work " + work + " " + (workCanBeDoneInLlap ? "can" : "cannot") + " be done in LLAP");
      if (workCanBeDoneInLlap) {
        for (MapJoinOperator graceMapJoinOp : mapJoinOpList) {
          LOG.debug("Disabling hybrid grace hash join in case of LLAP "
              + "and non-dynamic partition hash join.");
          graceMapJoinOp.getConf().setHybridHashJoin(false);
        }
        adjustAutoParallelism(work);
        
        convertWork(tezWork, work);
      }
      mapJoinOpList.clear();
    }

    private void adjustAutoParallelism(BaseWork work) {
      if (minReducersPerExec <= 0 || !(work instanceof ReduceWork)) return;
      ReduceWork reduceWork = (ReduceWork)work;
      if (reduceWork.isAutoReduceParallelism() == false && reduceWork.isUniformDistribution() == false) {
        return; // Not based on ARP and cannot assume uniform distribution, bail.
      }
      clusterState.initClusterInfo();
      final int targetCount;
      final int executorCount;
      final int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAX_REDUCERS);
      if (!clusterState.hasClusterInfo()) {
        LOG.warn("Cannot determine LLAP cluster information");
        executorCount = executorsPerNode; // assume 1 node
      } else {
        executorCount =
            clusterState.getKnownExecutorCount() + executorsPerNode
                * clusterState.getNodeCountWithUnknownExecutors();
      }
      targetCount = Math.min(maxReducers, (int) Math.ceil(minReducersPerExec * executorCount));
      // We only increase the targets here, but we stay below maxReducers
      if (reduceWork.isAutoReduceParallelism()) {
        // Do not exceed the configured max reducers.
        int newMin = Math.min(maxReducers, Math.max(reduceWork.getMinReduceTasks(), targetCount));
        if (newMin < reduceWork.getMaxReduceTasks()) {
          reduceWork.setMinReduceTasks(newMin);
          reduceWork.getEdgePropRef().setAutoReduce(conf, true, newMin,
              reduceWork.getMaxReduceTasks(), conf.getLongVar(HiveConf.ConfVars.BYTES_PER_REDUCER),
              reduceWork.getMinSrcFraction(), reduceWork.getMaxSrcFraction());
        } else {
          reduceWork.setAutoReduceParallelism(false);
          reduceWork.setNumReduceTasks(newMin);
          // TODO: is this correct? based on the same logic as HIVE-14200
          reduceWork.getEdgePropRef().setAutoReduce(null, false, 0, 0, 0, 0.0f, 0.0f);
        }
      } else {
        // UNIFORM || AUTOPARALLEL (maxed out)
        reduceWork.setNumReduceTasks(Math.max(reduceWork.getNumReduceTasks(), targetCount));
      }
    }


    private void convertWork(TezWork tezWork, BaseWork work)
      throws SemanticException {

      if (shouldUber) {
        // let's see if we can go one step further and just uber this puppy
        if (tezWork.getChildren(work).isEmpty()
            && work instanceof ReduceWork
            && ((ReduceWork) work).getNumReduceTasks() == 1) {
          LOG.info("Converting work to uber: {}", work);
          work.setUberMode(true);
        }
      }

      // always mark as llap
      work.setLlapMode(true);
    }

    private boolean evaluateWork(TezWork tezWork, BaseWork work)
      throws SemanticException {

      LOG.info("Evaluating work item: " + work.getName());

      // no means no
      if (mode == none) {
        return false;
      }


      // first we check if we *can* run in llap. If we need to use
      // user code to do so (script/udf) we don't.
      /*if (work instanceof MapWork && ((MapWork)work).isUseOneNullRowInputFormat()) {
        // LLAP doesn't support file-based splits that this forces.
        return false;
      }*/

      if (!evaluateOperators(work)) {
        LOG.info("some operators cannot be run in llap");
        if (mode == only) {
          throw new RuntimeException("Cannot run all parts of query in llap. Failing since " +
              ConfVars.LLAP_EXECUTION_MODE.varname + " is set to " + only.name());
        }

        return false;
      }

      // --- From here on out we choose whether we *want* to run in llap

      // if mode is all just run it
      if (EnumSet.of(all, only).contains(mode)) {
        LOG.info("LLAP mode set to '" + mode + "' so can convert any work.");
        return true;
      }

      // if map mode run iff work is map work
      if (mode == map) {
        return (work instanceof MapWork);
      }

      // --- From here we evaluate the auto mode
      assert mode == auto : "Mode must be " + auto.name() + " at this point";

      // if parents aren't in llap neither should the child
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_AUTO_ENFORCE_TREE)
          && !checkParentsInLlap(tezWork, work)) {
        LOG.info("Parent not in llap.");
        return false;
      }

      // only vectorized orc input is cached. so there's a reason to
      // limit to that for now.
      if (work instanceof MapWork
          && HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_AUTO_ENFORCE_VECTORIZED)
          && !checkInputsVectorized((MapWork) work)) {
        LOG.info("Inputs not vectorized.");
        return false;
      }

      // check if there's at least some degree of stats available
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_AUTO_ENFORCE_STATS)
          && !checkPartialStatsAvailable(work)) {
        LOG.info("No column stats available.");
        return false;
      }

      // now let's take a look at input sizes
      long maxInput = HiveConf.getLongVar(conf, HiveConf.ConfVars.LLAP_AUTO_MAX_INPUT);
      long expectedInput = computeInputSize(work);
      if (maxInput >= 0 && (expectedInput > maxInput)) {
        LOG.info(String.format("Inputs too big (%d > %d)", expectedInput, maxInput));
        return false;
      }

      // and finally let's check output sizes
      long maxOutput = HiveConf.getLongVar(conf, HiveConf.ConfVars.LLAP_AUTO_MAX_OUTPUT);
      long expectedOutput = computeOutputSize(work);
      if (maxOutput >= 0 && (expectedOutput > maxOutput)) {
        LOG.info(String.format("Outputs too big (%d > %d)", expectedOutput, maxOutput));
        return false;
      }

      // couldn't convince you otherwise? well then let's llap.
      LOG.info("Can run work " + work.getName() + " in llap mode.");
      return true;
    }

    private boolean checkExpression(ExprNodeDesc expr) {
      Deque<ExprNodeDesc> exprs = new LinkedList<ExprNodeDesc>();
      exprs.add(expr);
      while (!exprs.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Checking '{}'",expr.getExprString());
        }

        ExprNodeDesc cur = exprs.removeFirst();
        if (cur == null) continue;
        if (cur.getChildren() != null) {
          exprs.addAll(cur.getChildren());
        }

        if (!doSkipUdfCheck && cur instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)cur;
          boolean isBuiltIn = FunctionRegistry.isBuiltInFuncExpr(funcDesc);
          if (!isBuiltIn) {
            if (!arePermanentFnsAllowed) {
              LOG.info("Not a built-in function: " + cur.getExprString()
                + " (permanent functions are disabled)");
              return false;
            }
            if (!FunctionRegistry.isPermanentFunction(funcDesc)) {
              LOG.info("Not a built-in or permanent function: " + cur.getExprString());
              return false;
            }
          }
        }
      }
      return true;
    }

    private boolean checkAggregator(AggregationDesc agg) throws SemanticException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Checking '{}'", agg.getExprString());
      }

      boolean result = checkExpressions(agg.getParameters());
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(agg.getGenericUDAFName());
      result = result && (fi != null) && fi.isNative();
      if (!result) {
        LOG.info("Aggregator is not native: " + agg.getExprString());
      }
      return result;
    }

    private boolean checkExpressions(Collection<ExprNodeDesc> exprs) {
      for (ExprNodeDesc expr : exprs) {
        if (!checkExpression(expr)) return false;
      }
      return true;
    }

    private boolean checkAggregators(Collection<AggregationDesc> aggs) {
      try {
        for (AggregationDesc agg: aggs) {
          if (!checkAggregator(agg)) return false;
        }
      } catch (SemanticException e) {
        LOG.warn("Exception testing aggregators.",e);
        return false;
      }
      return true;
    }

    private Map<SemanticRule, SemanticNodeProcessor> getRules() {
      Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
      opRules.put(new RuleRegExp("No scripts", ScriptOperator.getOperatorName() + "%"),
          new SemanticNodeProcessor() {
          @Override
          public Object process(Node n, Stack<Node> s, NodeProcessorCtx c,
              Object... os) {
            LOG.debug("Cannot run operator [" + n + "] in llap mode.");
              return Boolean.FALSE;
          }
        });
      opRules.put(new RuleRegExp("No user code in fil", FilterOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
        @Override
        public Object process(Node n, Stack<Node> s, NodeProcessorCtx c, Object... os) {
          ExprNodeDesc expr = ((FilterOperator) n).getConf().getPredicate();
          boolean retval = checkExpression(expr);
          if (!retval) {
            LOG.info("Cannot run filter operator [" + n + "] in llap mode");
          }
          return Boolean.valueOf(retval);
        }
      });
      opRules.put(new RuleRegExp("No user code in gby", GroupByOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
        @Override
        public Object process(Node n, Stack<Node> s, NodeProcessorCtx c, Object... os) {
          @SuppressWarnings("unchecked")
          List<AggregationDesc> aggs = ((Operator<GroupByDesc>) n).getConf().getAggregators();
          boolean retval = checkAggregators(aggs);
          if (!retval) {
            LOG.info("Cannot run group by operator [" + n + "] in llap mode");
          }
          return Boolean.valueOf(retval);
        }
      });
      opRules.put(new RuleRegExp("No user code in select", SelectOperator.getOperatorName() + "%"),
          new SemanticNodeProcessor() {
            @Override
            public Object process(Node n, Stack<Node> s, NodeProcessorCtx c, Object... os) {
              @SuppressWarnings({"unchecked"})
              List<ExprNodeDesc> exprs = ((Operator<SelectDesc>) n).getConf().getColList();
              boolean retval = checkExpressions(exprs);
              if (!retval) {
                LOG.info("Cannot run select operator [" + n + "] in llap mode");
              }
              return Boolean.valueOf(retval);
            }
          });

      if (!conf.getBoolVar(HiveConf.ConfVars.LLAP_ENABLE_GRACE_JOIN_IN_LLAP)) {
        opRules.put(new RuleRegExp("Disable grace hash join if LLAP mode and not dynamic partition hash join",
            MapJoinOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
              @Override
              public Object process(Node n, Stack<Node> s, NodeProcessorCtx c, Object... os) {
                MapJoinOperator mapJoinOp = (MapJoinOperator) n;
                if (mapJoinOp.getConf().isHybridHashJoin() && !(mapJoinOp.getConf().isDynamicPartitionHashJoin())) {
                  mapJoinOpList.add((MapJoinOperator) n);
                }
                return Boolean.TRUE;
              }
            });
      }

      return opRules;
    }

    private boolean evaluateOperators(BaseWork work) throws SemanticException {
      // lets take a look at the operators. we're checking for user
      // code in those. we will not run that in llap.
      SemanticDispatcher disp = new DefaultRuleDispatcher(null, rules, null);
      SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(work.getAllRootOperators());

      HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      for (Node n : nodeOutput.keySet()) {
        if (nodeOutput.get(n) != null) {
          if (!((Boolean)nodeOutput.get(n))) {
            return false;
          }
        }
      }
      return true;
    }

    private boolean checkParentsInLlap(TezWork tezWork, BaseWork base) {
      for (BaseWork w: tezWork.getParents(base)) {
        if (!w.getLlapMode()) {
          LOG.info("Not all parents are run in llap");
          return false;
        }
      }
      return true;
    }

    private boolean checkInputsVectorized(MapWork mapWork) {
      boolean mayWrap = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED);
      Collection<Class<?>> excludedInputFormats = Utilities.getClassNamesFromConfig(conf, ConfVars.HIVE_VECTORIZATION_VECTORIZED_INPUT_FILE_FORMAT_EXCLUDES);
      for (PartitionDesc pd : mapWork.getPathToPartitionInfo().values()) {
        if ((Utilities.isInputFileFormatVectorized(pd) && !excludedInputFormats
            .contains(pd.getInputFileFormatClass())) || (mayWrap && HiveInputFormat
            .canWrapForLlap(pd.getInputFileFormatClass(), true))) {
          continue;
        }
        LOG.info("Input format: " + pd.getInputFileFormatClassName()
          + ", doesn't provide vectorized input");
        return false;
      }
      return true;
    }

    private boolean checkPartialStatsAvailable(BaseWork base) {
      for (Operator<?> o: base.getAllRootOperators()) {
        if (o.getStatistics().getColumnStatsState() == Statistics.State.NONE) {
          return false;
        }
      }
      return true;
    }

    private long computeEdgeSize(BaseWork base, boolean input) {
      long size = 0;
      for (Operator<?> o: (input ? base.getAllRootOperators() : base.getAllLeafOperators())) {
        if (o.getStatistics() == null) {
          // return worst case if unknown
          return Long.MAX_VALUE;
        }

        long currSize = o.getStatistics().getDataSize();
        if ((currSize < 0) || ((Long.MAX_VALUE - size) < currSize)) {
          // overflow
          return Long.MAX_VALUE;
        }
        size += currSize;
      }
      return size;
    }

    private long computeInputSize(BaseWork base) {
      return computeEdgeSize(base, true);
    }

    private long computeOutputSize(BaseWork base) {
      return computeEdgeSize(base, false);
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.conf = pctx.getConf();

    this.mode = LlapMode.valueOf(HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_EXECUTION_MODE));
    Preconditions.checkState(this.mode != null, "Unrecognized LLAP mode configuration: " +
        HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_EXECUTION_MODE));
    LOG.info("llap mode: " + this.mode);

    if (mode == none) {
      LOG.info("LLAP disabled.");
      return pctx;
    }

    // create dispatcher and graph walker
    SemanticDispatcher disp = new LlapDecisionDispatcher(pctx, mode);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }
}
