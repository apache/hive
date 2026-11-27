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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.junit.Assert;
import org.junit.Test;

public class TestLlapDecider {

  private static final int EXECUTORS = 288;
  // ceil(hive.tez.llap.min.reducer.per.executor (0.33) * executors)
  private static final int FLOOR = 96;

  /**
   * A reduce-side merge join's vertex is built from its main work, which gets the minimum reducers for the cluster's
   * executors, as a lone reduce work does.
   */
  @Test
  public void testMergeJoinMainWorkGetsTheExecutorReducerFloor() throws Exception {
    HiveConf conf = conf();
    ReduceWork main = autoReduceWork("Reducer 3", 104);
    ReduceWork lone = autoReduceWork("Reducer 4", 104);

    resolve(conf, mergeJoin(main), lone);

    for (ReduceWork work : List.of(main, lone)) {
      Assert.assertTrue(work.getName(), work.isAutoReduceParallelism());
      Assert.assertEquals(work.getName(), FLOOR, work.getMinReduceTasks());
    }
  }

  /** A floor at or above the main work's maximum fixes the merge join's vertex at the floor. */
  @Test
  public void testMergeJoinFixedAtTheFloor() throws Exception {
    HiveConf conf = conf();
    ReduceWork main = autoReduceWork("Reducer 3", 78);

    resolve(conf, mergeJoin(main));

    Assert.assertFalse(main.isAutoReduceParallelism());
    Assert.assertEquals(FLOOR, main.getNumReduceTasks().intValue());
  }

  /**
   * Without auto-reduce, a join's main work is evenly distributed (its ReduceSink is UNIFORM): it runs at least the
   * floor, as a lone reduce work does.
   */
  @Test
  public void testUniformMergeJoinMainWorkRunsAtTheFloor() throws Exception {
    HiveConf conf = conf();
    ReduceWork main = uniformWork("Reducer 3");
    ReduceWork lone = uniformWork("Reducer 4");

    resolve(conf, mergeJoin(main), lone);

    for (ReduceWork work : List.of(main, lone)) {
      Assert.assertFalse(work.getName(), work.isAutoReduceParallelism());
      Assert.assertEquals(work.getName(), FLOOR, work.getNumReduceTasks().intValue());
    }
  }

  private static HiveConf conf() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.LLAP_EXECUTION_MODE, "all");
    return conf;
  }

  /** Auto-reduces between 13 tasks and the given maximum. */
  private static ReduceWork autoReduceWork(String name, int maxReduceTasks) {
    ReduceWork work = new ReduceWork(name);
    work.setReducer(OperatorFactory.get(new CompilationOpContext(), new SelectDesc(new ArrayList<>(),
        new ArrayList<>())));
    work.setAutoReduceParallelism(true);
    work.setMinReduceTasks(13);
    work.setMaxReduceTasks(maxReduceTasks);
    work.setNumReduceTasks(maxReduceTasks);
    return work;
  }

  /** Evenly distributed on 2 tasks, without auto-reduce. */
  private static ReduceWork uniformWork(String name) {
    ReduceWork work = new ReduceWork(name);
    work.setReducer(OperatorFactory.get(new CompilationOpContext(), new SelectDesc(new ArrayList<>(),
        new ArrayList<>())));
    work.setUniformDistribution(true);
    work.setNumReduceTasks(2);
    return work;
  }

  private static MergeJoinWork mergeJoin(ReduceWork main) {
    MergeJoinWork mergeJoin = new MergeJoinWork();
    mergeJoin.addMergedWork(main, null, new HashMap<>());
    return mergeJoin;
  }

  private static void resolve(HiveConf conf, BaseWork... works) throws Exception {
    TezWork tezWork = new TezWork("q");
    for (BaseWork work : works) {
      tezWork.add(work);
    }
    List<Task<?>> rootTasks = new ArrayList<>();
    rootTasks.add(TaskFactory.get(tezWork));
    LlapClusterStateForCompile cluster = mock(LlapClusterStateForCompile.class);
    when(cluster.initClusterInfo()).thenReturn(true);
    when(cluster.hasClusterInfo()).thenReturn(true);
    when(cluster.getKnownExecutorCount()).thenReturn(EXECUTORS);
    when(cluster.getNodeCountWithUnknownExecutors()).thenReturn(0);
    new LlapDecider(cluster).resolve(new PhysicalContext(conf, null, null, rootTasks, null));
  }
}
