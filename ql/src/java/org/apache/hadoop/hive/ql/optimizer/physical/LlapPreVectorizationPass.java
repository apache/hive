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

import static org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode.none;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.physical.LlapDecider.LlapMode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For any LLAP-related transformations which need to occur before vectorization.
 */
public class LlapPreVectorizationPass implements PhysicalPlanResolver {
  protected static transient final Logger LOG = LoggerFactory.getLogger(LlapPreVectorizationPass.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    HiveConf conf = pctx.getConf();
    LlapMode mode = LlapMode.valueOf(HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_EXECUTION_MODE));
    if (mode == none) {
      LOG.info("LLAP disabled.");
      return pctx;
    }

    SemanticDispatcher disp = new LlapPreVectorizationPassDispatcher(pctx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  class LlapPreVectorizationPassDispatcher implements SemanticDispatcher {
    HiveConf conf;

    LlapPreVectorizationPassDispatcher(PhysicalContext pctx) {
      conf = pctx.getConf();
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

    private void handleWork(TezWork tezWork, BaseWork work)
        throws SemanticException {
      Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

      if (conf.getVar(HiveConf.ConfVars.LLAP_EXECUTION_MODE).equals("only")
          && !conf.getBoolVar(HiveConf.ConfVars.LLAP_ENABLE_GRACE_JOIN_IN_LLAP)) {
        // In LLAP only mode, grace hash join will be disabled later on by the LlapDispatcher anyway.
        // Since the presence of Grace Hash Join disables some "native" vectorization optimizations,
        // we will disable the grace hash join now, before vectorization is done.
        opRules.put(new RuleRegExp("Disable grace hash join if LLAP mode and not dynamic partition hash join",
            MapJoinOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
              @Override
              public Object process(Node n, Stack<Node> s, NodeProcessorCtx c, Object... os) {
                MapJoinOperator mapJoinOp = (MapJoinOperator) n;
                if (mapJoinOp.getConf().isHybridHashJoin() && !(mapJoinOp.getConf().isDynamicPartitionHashJoin())) {
                  mapJoinOp.getConf().setHybridHashJoin(false);
                }
                return Boolean.TRUE;
              }
            });
      }

      if (!opRules.isEmpty()) {
        SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
        SemanticGraphWalker ogw = new DefaultGraphWalker(disp);
        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.addAll(work.getAllRootOperators());
        ogw.startWalking(topNodes, null);
      }
    }
  }
}
