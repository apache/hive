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

package org.apache.hadoop.hive.metastore.hooks;

import java.util.AbstractMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionContext;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionListener;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.hooks.conf.FBHiveConf;

/*
 * MetaStoreEndFunctionListener that uses the StatsManager to collect fb303 counters for
 * the number of successes and failures for each metastore thrift function, bucketed by time.
 */
public class CounterMetaStoreEndFunctionListener extends MetaStoreEndFunctionListener {

  StatsManager stats = null;

  public CounterMetaStoreEndFunctionListener(Configuration config) {
    super(config);
    String statsMgr = config.get(FBHiveConf.METASTORE_LISTENER_STATS_MANAGER);
    if ((statsMgr == null) || (statsMgr.isEmpty())) {
      return;
    }

    stats = HookUtils.getObject(config, statsMgr);
  }

  @Override
  public void onEndFunction(String functionName, MetaStoreEndFunctionContext context) {
    if (stats == null) {
      return;
    }

    // Construct the counter name, as <functionName> for success
    // and <functionName.failure> for failure
    String statName = functionName + (context.isSuccess() ? "" : ".failure");

    // If this is the first time this counter name has been seen, initialize it
    if (!stats.containsKey(statName)) {
      stats.addCountStatType(statName);
    }

    stats.addStatValue(statName, 1);
  }

  @Override
  public void exportCounters(AbstractMap<String, Long> counters) {
    if (stats == null) {
      return;
    }

    // For each counter the StatsManager has collected, add it to the map of fb303 counters
    for (Entry<String, Long> entry : stats.getCounters().entrySet()) {
      counters.put(entry.getKey(), entry.getValue());
    }
  }

}
