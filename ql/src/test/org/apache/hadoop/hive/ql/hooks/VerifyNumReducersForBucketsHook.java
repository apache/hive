/**
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

import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *
 * VerifyNumReducersForBucketsHook.
 *
 * This hook is meant to be used with bucket_num_reducers.q . It checks whether the
 * number of reducers has been correctly set.
 */
public class VerifyNumReducersForBucketsHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    SessionState ss = SessionState.get();
    Assert.assertNotNull("SessionState returned null");

    List<MapRedStats> stats = ss.getLastMapRedStatsList();
    Assert.assertEquals("Number of MapReduce jobs is incorrect", 1, stats.size());

    Assert.assertEquals("NumReducers is incorrect", 10, stats.get(0).getNumReduce());
  }
}
