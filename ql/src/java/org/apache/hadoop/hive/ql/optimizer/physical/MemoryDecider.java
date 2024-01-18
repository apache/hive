/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
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
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MemoryDecider is a simple physical optimizer that adjusts the memory layout of tez tasks.
 * Currently it only cares about hash table sizes for the graceful hash join.
 * It tried to keep hashtables that are small and early in the operator pipeline completely
 * in memory.
 */
public class MemoryDecider implements PhysicalPlanResolver {

  protected static transient final Logger LOG = LoggerFactory.getLogger(MemoryDecider.class);

  public class MemoryCalculator implements SemanticDispatcher {

    private final long totalAvailableMemory; // how much to we have
    private final long minimumHashTableSize; // minimum size of ht completely in memory
    private final double inflationFactor; // blowout factor datasize -> memory size
    private final PhysicalContext pctx;

    public MemoryCalculator(PhysicalContext pctx) {
      this.pctx = pctx;
      this.totalAvailableMemory = HiveConf.getLongVar(pctx.conf, HiveConf.ConfVars.HIVE_CONVERT_JOIN_NOCONDITIONAL_TASK_THRESHOLD);
      this.minimumHashTableSize = HiveConf.getIntVar(pctx.conf, HiveConf.ConfVars.HIVE_HYBRIDGRACE_HASHJOIN_MIN_NUM_PARTITIONS)
        * HiveConf.getIntVar(pctx.conf, HiveConf.ConfVars.HIVE_HYBRIDGRACE_HASHJOIN_MIN_WB_SIZE);
      this.inflationFactor = HiveConf.getFloatVar(pctx.conf, HiveConf.ConfVars.HIVE_HASH_TABLE_INFLATION_FACTOR);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
      Task<?> currTask = (Task<?>) nd;
      if (currTask instanceof StatsTask) {
        currTask = ((StatsTask) currTask).getWork().getSourceTask();
      }
      if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w : work.getAllWork()) {
          evaluateWork(w);
        }
      }
      return null;
    }

    private void evaluateWork(BaseWork w) throws SemanticException {

      if (w instanceof MapWork) {
        evaluateMapWork((MapWork) w);
      } else if (w instanceof ReduceWork) {
        evaluateReduceWork((ReduceWork) w);
      } else if (w instanceof MergeJoinWork) {
        evaluateMergeWork((MergeJoinWork) w);
      } else {
        LOG.info("We are not going to evaluate this work type: " + w.getClass().getCanonicalName());
      }
    }

    private void evaluateMergeWork(MergeJoinWork w) throws SemanticException {
      for (BaseWork baseWork : w.getBaseWorkList()) {
        evaluateOperators(baseWork, pctx);
      }
    }

    private void evaluateReduceWork(ReduceWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateMapWork(MapWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateOperators(BaseWork w, PhysicalContext pctx) throws SemanticException {
      // lets take a look at the operator memory requirements.
      SemanticDispatcher disp = null;
      final Set<MapJoinOperator> mapJoins = new LinkedHashSet<MapJoinOperator>();

      LinkedHashMap<SemanticRule, SemanticNodeProcessor> rules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
      rules.put(new RuleRegExp("Map join memory estimator",
              MapJoinOperator.getOperatorName() + "%"), new SemanticNodeProcessor() {
          @Override
          public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
              Object... nodeOutputs) {
            mapJoins.add((MapJoinOperator) nd);
            return null;
          }
        });
      disp = new DefaultRuleDispatcher(null, rules, null);

      SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(w.getAllRootOperators());

      LinkedHashMap<Node, Object> nodeOutput = new LinkedHashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      if (mapJoins.size() == 0) {
        return;
      }

      try {
        long total = 0;
        final Map<MapJoinOperator, Long> sizes = new HashMap<MapJoinOperator, Long>();
        final Map<MapJoinOperator, Integer> positions = new HashMap<MapJoinOperator, Integer>();

        int i = 0;
        for (MapJoinOperator mj : mapJoins) {
          long size = computeSizeToFitInMem(mj);
          sizes.put(mj, size);
          positions.put(mj, i++);
          total += size;
        }

        Comparator<MapJoinOperator> comp = new Comparator<MapJoinOperator>() {
            @Override
            public int compare(MapJoinOperator mj1, MapJoinOperator mj2) {
              if (mj1 == null || mj2 == null) {
                throw new NullPointerException();
              }

              int res = Long.compare(sizes.get(mj1), sizes.get(mj2));
              if (res == 0) {
                res = Integer.compare(positions.get(mj1), positions.get(mj2));
              }
              return res;
            }
          };

        SortedSet<MapJoinOperator> sortedMapJoins = new TreeSet<MapJoinOperator>(comp);
        sortedMapJoins.addAll(mapJoins);

        long remainingSize = totalAvailableMemory / 2;

        Iterator<MapJoinOperator> it = sortedMapJoins.iterator();

        long totalLargeJoins = 0;

        while (it.hasNext()) {
          MapJoinOperator mj = it.next();
          long size = sizes.get(mj);
          if (LOG.isDebugEnabled()) {
            LOG.debug("MapJoin: " + mj + ", size: " + size + ", remaining: " + remainingSize);
          }

          if (size < remainingSize) {
            LOG.info("Setting " + size + " bytes needed for " + mj + " (in-mem)");

            mj.getConf().setMemoryNeeded(size);
            remainingSize -= size;
            it.remove();
          } else {
            totalLargeJoins += sizes.get(mj);
          }
        }

        if (sortedMapJoins.isEmpty()) {
          // all of the joins fit into half the memory. Let's be safe and scale them out.
          sortedMapJoins.addAll(mapJoins);
          totalLargeJoins = total;

          if (totalLargeJoins > totalAvailableMemory) {
            // this shouldn't happen
            throw new HiveException();
          }
          remainingSize = totalAvailableMemory / 2;
        }

        // we used half the mem for small joins, now let's scale the rest
        double weight = (remainingSize + totalAvailableMemory / 2) / (double) totalLargeJoins;

        for (MapJoinOperator mj : sortedMapJoins) {
          long size = (long)(weight * sizes.get(mj));
          LOG.info("Setting " + size + " bytes needed for " + mj + " (spills)");

          mj.getConf().setMemoryNeeded(size);
        }
      } catch (HiveException e) {
        // if we have issues with stats, just scale linearily
        long size = totalAvailableMemory / mapJoins.size();
        LOG.info("Scaling mapjoin memory w/o stats");

        for (MapJoinOperator mj : mapJoins) {
          LOG.info("Setting " + size + " bytes needed for " + mj + " (fallback)");
          mj.getConf().setMemoryNeeded(size);
        }
      }
    }

    private long computeSizeToFitInMem(MapJoinOperator mj) throws HiveException {
      return (long) (Math.max(this.minimumHashTableSize, computeInputSize(mj)) * this.inflationFactor);
    }

    private long computeInputSize(MapJoinOperator mj) throws HiveException {
      long size = 0;

      if (mj.getConf() != null && mj.getConf().getParentDataSizes() != null) {
        for (long l: mj.getConf().getParentDataSizes().values()) {
          size += l;
        }
      }

      if (size == 0) {
        throw new HiveException("No data sizes");
      }
      return size;
    }

    public class DefaultRule implements SemanticNodeProcessor {

      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        return null;
      }
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    pctx.getConf();

    // create dispatcher and graph walker
    SemanticDispatcher disp = new MemoryCalculator(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }

}
