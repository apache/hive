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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.fs.FSStatsPublisher;

public class AnnotateRunTimeStatsOptimizer implements PhysicalPlanResolver {
  private static final Logger LOG = LoggerFactory.getLogger(AnnotateRunTimeStatsOptimizer.class);

  private class AnnotateRunTimeStatsDispatcher implements SemanticDispatcher {

    private final PhysicalContext physicalContext;

    public AnnotateRunTimeStatsDispatcher(PhysicalContext context, Map<SemanticRule, SemanticNodeProcessor> rules) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<?> currTask = (Task<?>) nd;
      Set<Operator<? extends OperatorDesc>> ops = new HashSet<>();

      if (currTask instanceof MapRedTask) {
        MapRedTask mr = (MapRedTask) currTask;
        ops.addAll(mr.getWork().getAllOperators());
      } else if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w : work.getAllWork()) {
          ops.addAll(w.getAllOperators());
        }
      } else if (currTask instanceof SparkTask) {
        SparkWork sparkWork = (SparkWork) currTask.getWork();
        for (BaseWork w : sparkWork.getAllWork()) {
          ops.addAll(w.getAllOperators());
        }
      }

      setOrAnnotateStats(ops, physicalContext.getParseContext());
      return null;
    }

  }

  public static void setOrAnnotateStats(Set<Operator<? extends OperatorDesc>> ops, ParseContext pctx)
      throws SemanticException {
    for (Operator<? extends OperatorDesc> op : ops) {
      if (pctx.getContext().getExplainAnalyze() == AnalyzeState.RUNNING) {
        setRuntimeStatsDir(op, pctx);
      } else if (pctx.getContext().getExplainAnalyze() == AnalyzeState.ANALYZING) {
        annotateRuntimeStats(op, pctx);
      } else {
        throw new SemanticException("Unexpected stats in AnnotateWithRunTimeStatistics.");
      }
    }
  }

  private static void setRuntimeStatsDir(Operator<? extends OperatorDesc> op, ParseContext pctx)
      throws SemanticException {
    try {
      OperatorDesc conf = op.getConf();
      if (conf != null) {
        LOG.info("setRuntimeStatsDir for " + op.getOperatorId());
        String path = new Path(pctx.getContext().getExplainConfig().getExplainRootPath(),
            op.getOperatorId()).toString();
        StatsPublisher statsPublisher = new FSStatsPublisher();
        StatsCollectionContext runtimeStatsContext = new StatsCollectionContext(pctx.getConf());
        runtimeStatsContext.setStatsTmpDir(path);
        if (!statsPublisher.init(runtimeStatsContext)) {
          LOG.error("StatsPublishing error: StatsPublisher is not initialized.");
          throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
        }
        conf.setRuntimeStatsTmpDir(path);
      } else {
        LOG.debug("skip setRuntimeStatsDir for " + op.getOperatorId()
            + " because OperatorDesc is null");
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  private static void annotateRuntimeStats(Operator<? extends OperatorDesc> op, ParseContext pctx) {
    Long runTimeNumRows = pctx.getContext().getExplainConfig().getOpIdToRuntimeNumRows()
        .get(op.getOperatorId());
    if (op.getConf() != null && op.getConf().getStatistics() != null && runTimeNumRows != null) {
      LOG.info("annotateRuntimeStats for " + op.getOperatorId());
      op.getConf().getStatistics().setRunTimeNumRows(runTimeNumRows);
    } else {
      LOG.debug("skip annotateRuntimeStats for " + op.getOperatorId());
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    SemanticDispatcher disp = new AnnotateRunTimeStatsDispatcher(pctx, opRules);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  public void resolve(Set<Operator<?>> opSet, ParseContext pctx) throws SemanticException {
    Set<Operator<?>> ops = getAllOperatorsForSimpleFetch(opSet);
    setOrAnnotateStats(ops, pctx);
  }

  private Set<Operator<?>> getAllOperatorsForSimpleFetch(Set<Operator<?>> opSet) {
    Set<Operator<?>> returnSet = new LinkedHashSet<Operator<?>>();
    Stack<Operator<?>> opStack = new Stack<Operator<?>>();
    // add all children
    opStack.addAll(opSet);
    while (!opStack.empty()) {
      Operator<?> op = opStack.pop();
      returnSet.add(op);
      if (op.getChildOperators() != null) {
        opStack.addAll(op.getChildOperators());
      }
    }
    return returnSet;
  }
}
