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
package org.apache.hadoop.hive.ql.profiler;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.OperatorHook;
import org.apache.hadoop.hive.ql.exec.OperatorHookContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class HiveProfiler implements OperatorHook {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private static final HiveProfilePublisher pub = new HiveProfilePublisher();

  private final Map<String, HiveProfilerEntry> operatorCallStack =
    new ConcurrentHashMap<String, HiveProfilerEntry>();

  // Aggregates stats for each operator in memory so that stats are written to DB
  // all at once - this allows the profiler to be extremely lightweight in
  // communication with the DB
  private final Map<String, HiveProfilerStats> aggrStats =
    new ConcurrentHashMap<String, HiveProfilerStats>();

  public void enter(OperatorHookContext opHookContext) throws HiveException {
    String opLevelAnnoName = HiveProfilerUtils.getLevelAnnotatedName(opHookContext);
    HiveProfilerEntry curEntry = new HiveProfilerEntry(opHookContext);
    assert(operatorCallStack.get(opLevelAnnoName) == null);
    operatorCallStack.put(opLevelAnnoName, curEntry);
  }

  private void exit(HiveProfilerEntry curEntry) {
    OperatorHookContext opHookContext = curEntry.getOperatorHookContext();
    // update the metrics we are
    long exitTime = System.nanoTime();
    long wallTime = exitTime - curEntry.wallStartTime;

    String opName = opHookContext.getOperatorName();

    Configuration conf = opHookContext.getOperator().getConfiguration();

    String opLevelAnnoName = HiveProfilerUtils.getLevelAnnotatedName(opHookContext);

    if (aggrStats.containsKey(opLevelAnnoName)) {
      aggrStats.get(opLevelAnnoName).updateStats(wallTime, 1);
    } else {
      HiveProfilerStats stats =
        new HiveProfilerStats(opHookContext, 1, wallTime, conf);
      aggrStats.put(opLevelAnnoName, stats);
    }

  }
  public void exit(OperatorHookContext opHookContext) throws HiveException {
    if (operatorCallStack.isEmpty()) {
      LOG.error("Unexpected state: Operator Call Stack is empty on exit.");
    }
    String opLevelAnnoName = HiveProfilerUtils.getLevelAnnotatedName(opHookContext);

    HiveProfilerEntry curEntry = operatorCallStack.get(opLevelAnnoName);

    if (!curEntry.getOperatorHookContext().equals(opHookContext)) {
      LOG.error("Expected to exit from: " + curEntry.getOperatorHookContext().toString() +
        " but exit called on " + opHookContext.toString());
    }

    exit(curEntry);
    operatorCallStack.remove(opLevelAnnoName);
  }

  public void close(OperatorHookContext opHookContext) {
    Configuration conf = opHookContext.getOperator().getConfiguration();

    Collection<HiveProfilerStats> stats = aggrStats.values();
    // example:
    // queryId=pamelavagata_20130115163838_4a1cb4ae-43c1-4656-bfae-118557896eec,
    // operatorName=TS,
    // id=3,
    // parentName="" (root),
    // inclTime=1202710
    // callCount

    Iterator<HiveProfilerStats> statsIter = stats.iterator();
    while (statsIter.hasNext()) {
      HiveProfilerStats stat = statsIter.next();
      pub.initialize(conf);
      boolean published = pub.publishStat(null, stat.getStatsMap(), conf);
      LOG.info((published ? "did " : "did not ") + "publish stat for: " + stat.toString());
      pub.closeConnection();
    }
    stats.clear();

  }

  private class HiveProfilerEntry {
    OperatorHookContext ctxt;
    protected long wallStartTime;

    protected HiveProfilerEntry(OperatorHookContext opHookContext) {
      this.ctxt = opHookContext;
      this.wallStartTime = System.nanoTime();
    }

    protected OperatorHookContext getOperatorHookContext() {
      return ctxt;
    }
  }
}
