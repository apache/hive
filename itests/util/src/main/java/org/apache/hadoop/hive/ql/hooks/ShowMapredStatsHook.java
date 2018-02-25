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

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.Map;

public class ShowMapredStatsHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    SessionState ss = SessionState.get();
    Assert.assertNotNull("SessionState returned null");

    Map<String, MapRedStats> stats = ss.getMapRedStats();
    if (stats != null && !stats.isEmpty()) {
      for (Map.Entry<String, MapRedStats> stat : stats.entrySet()) {
        SessionState.getConsole().printError(
            stat.getKey() + "=" + stat.getValue().getTaskNumbers());
      }
    }
  }
}
