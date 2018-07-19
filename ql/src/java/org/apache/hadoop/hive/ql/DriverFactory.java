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

package org.apache.hadoop.hive.ql;

import java.util.ArrayList;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.reexec.IReExecutionPlugin;
import org.apache.hadoop.hive.ql.reexec.ReExecDriver;
import org.apache.hadoop.hive.ql.reexec.ReExecutionOverlayPlugin;
import org.apache.hadoop.hive.ql.reexec.ReOptimizePlugin;

import com.google.common.base.Strings;

/**
 * Constructs a driver for ql clients.
 */
public class DriverFactory {

  public static IDriver newDriver(HiveConf conf) {
    return newDriver(getNewQueryState(conf), null, null);
  }

  public static IDriver newDriver(QueryState queryState, String userName, QueryInfo queryInfo) {
    boolean enabled = queryState.getConf().getBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED);
    if (!enabled) {
      return new Driver(queryState, userName, queryInfo);
    }

    String strategies = queryState.getConf().getVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES);
    strategies = Strings.nullToEmpty(strategies).trim().toLowerCase();
    ArrayList<IReExecutionPlugin> plugins = new ArrayList<>();
    for (String string : strategies.split(",")) {
      if (string.trim().isEmpty()) {
        continue;
      }
      plugins.add(buildReExecPlugin(string));
    }

    return new ReExecDriver(queryState, userName, queryInfo, plugins);
  }

  private static IReExecutionPlugin buildReExecPlugin(String name) throws RuntimeException {
    if (name.equals("overlay")) {
      return new ReExecutionOverlayPlugin();
    }
    if (name.equals("reoptimize")) {
      return new ReOptimizePlugin();
    }
    throw new RuntimeException(
        "Unknown re-execution plugin: " + name + " (" + ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES.varname + ")");
  }

  private static QueryState getNewQueryState(HiveConf conf) {
    return new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(conf).build();
  }
}
