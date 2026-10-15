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
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.junit.Assert;
import org.junit.Test;

public class TestLlapDecider {

  private static final int EXECUTORS = 288;
  // ceil(hive.tez.llap.min.reducer.per.executor (0.33) * executors)
  private static final int FLOOR = 96;

  /**
   * A reduce-side merge join is one vertex fed by the edges of its reduce works: each of them gets the minimum
   * reducers for the cluster's executors, as a lone reduce work does.
   */
  @Test
  public void testMergeJoinGetsTheExecutorReducerFloor() throws Exception {
    HiveConf conf = conf();
    ReduceWork main = autoReduceWork("Reducer 3", conf);
    ReduceWork merged = autoReduceWork("Reducer 7", conf);
    MergeJoinWork mergeJoin = new MergeJoinWork();
    mergeJoin.addMergedWork(main, null, new HashMap<>());
    mergeJoin.addMergedWork(null, merged, new HashMap<>());
    ReduceWork lone = autoReduceWork("Reducer 4", conf);

    resolve(conf, mergeJoin, lone);

    for (ReduceWork work : List.of(main, merged, lone)) {
      Assert.assertEquals(work.getName(), FLOOR, work.getMinReduceTasks());
      Assert.assertEquals(work.getName(), FLOOR, work.getEdgePropRef().getMinReducer());
    }
  }

  private static HiveConf conf() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.LLAP_EXECUTION_MODE, "all");
    return conf;
  }

  /** Auto-reduces between 13 and 104 tasks, as a reducer estimated at 52 does with the default factors. */
  private static ReduceWork autoReduceWork(String name, HiveConf conf) {
    ReduceWork work = new ReduceWork(name);
    work.setReducer(OperatorFactory.get(new CompilationOpContext(), new SelectDesc(new ArrayList<>(),
        new ArrayList<>())));
    work.setAutoReduceParallelism(true);
    work.setMinReduceTasks(13);
    work.setMaxReduceTasks(104);
    work.setNumReduceTasks(104);
    work.setEdgePropRef(new TezEdgeProperty(conf, EdgeType.SIMPLE_EDGE, true, true, 13, 104, 256_000_000L,
        0.25f, 0.75f));
    return work;
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
