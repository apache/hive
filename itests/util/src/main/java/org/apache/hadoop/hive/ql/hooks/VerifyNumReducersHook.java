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
package org.apache.hadoop.hive.ql.hooks;

import java.util.Map;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezProgressMonitor;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.tez.dag.api.client.Progress;

/**
 *
 * VerifyNumReducersHook.
 *
 * Provided a query involves exactly 1 map reduce job, this hook can be used to verify that the
 * number of reducers matches what is expected.
 *
 * Use the config VerifyNumReducersHook.num.reducers to specify the expected number of reducers.
 */
public class VerifyNumReducersHook implements ExecuteWithHookContext {

  public static final String BUCKET_CONFIG = "VerifyNumReducersHook.num.reducers";

  public void run(HookContext hookContext) {
    SessionState ss = SessionState.get();
    Assert.assertNotNull("SessionState returned null");

    int expectedReducers = hookContext.getConf().getInt(BUCKET_CONFIG, 0);
    if (ss.getConf().get(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname).equals("mr")) {
      Map<String, MapRedStats> stats = ss.getMapRedStats();
      Assert.assertEquals("Number of MapReduce jobs is incorrect", 1, stats.size());

      MapRedStats stat = stats.values().iterator().next();
      Assert.assertEquals("NumReducers is incorrect", expectedReducers, stat.getNumReduce());
    } else if (ss.getConf().get(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname).equals("tez")) {
      TezProgressMonitor tezProgressMonitor = (TezProgressMonitor) ss.getProgressMonitor();
      Map<String, Progress> progressMap = tezProgressMonitor.getStatus().getVertexProgress();
      int totalReducers = 0;
      for (Map.Entry<String, Progress> entry : progressMap.entrySet()) {
        // relying on the name of vertex is fragile, but this will do for now for the tests
        if (entry.getKey().contains("Reducer")) {
          totalReducers += entry.getValue().getTotalTaskCount();
        }
      }
      Assert.assertEquals("Number of reducers is incorrect", expectedReducers, totalReducers);
    }
  }
}
