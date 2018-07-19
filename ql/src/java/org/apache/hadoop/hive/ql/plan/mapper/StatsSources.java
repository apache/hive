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

package org.apache.hadoop.hive.ql.plan.mapper;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.EquivGroup;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class StatsSources {

  private static final Logger LOG = LoggerFactory.getLogger(StatsSources.class);

  static enum StatsSourceMode {
    query, hiveserver, metastore;
  }

  public static void initialize(HiveConf hiveConf) {
    // requesting for the stats source will implicitly initialize it
    getStatsSource(hiveConf);
  }

  public static StatsSource getStatsSource(HiveConf conf) {
    String mode = conf.getVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_PERSISTENCE);
    int cacheSize = conf.getIntVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_CACHE_SIZE);
    int batchSize = conf.getIntVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_CACHE_BATCH_SIZE);
    switch (mode) {
    case "query":
      return new MapBackedStatsSource();
    case "hiveserver":
      return StatsSources.globalStatsSource(cacheSize);
    case "metastore":
      return StatsSources.metastoreBackedStatsSource(cacheSize, batchSize, StatsSources.globalStatsSource(cacheSize));
    default:
      throw new RuntimeException("Unknown StatsSource setting: " + mode);
    }
  }

  public static StatsSource getStatsSourceContaining(StatsSource currentStatsSource, PlanMapper pm) {
    StatsSource statsSource = currentStatsSource;
    if (currentStatsSource  == EmptyStatsSource.INSTANCE) {
      statsSource = new MapBackedStatsSource();
    }

    Map<OpTreeSignature, OperatorStats> statMap = extractStatMapFromPlanMapper(pm);
    statsSource.putAll(statMap);
    return statsSource;
  }

  private static Map<OpTreeSignature, OperatorStats> extractStatMapFromPlanMapper(PlanMapper pm) {
    Builder<OpTreeSignature, OperatorStats> map = ImmutableMap.builder();
    Iterator<EquivGroup> it = pm.iterateGroups();
    while (it.hasNext()) {
      EquivGroup e = it.next();
      List<OperatorStats> stat = e.getAll(OperatorStats.class);
      List<OpTreeSignature> sig = e.getAll(OpTreeSignature.class);

      if (stat.size() > 1 || sig.size() > 1) {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("expected(stat-sig) 1-1, got {}-{} ;", stat.size(), sig.size()));
        for (OperatorStats s : stat) {
          sb.append(s);
          sb.append(";");
        }
        for (OpTreeSignature s : sig) {
          sb.append(s);
          sb.append(";");
        }
        LOG.debug(sb.toString());
      }
      if (stat.size() < 1 || sig.size() < 1) {
        continue;
      }
      if (e.getAll(OperatorStats.IncorrectRuntimeStatsMarker.class).size() > 0) {
        LOG.debug("Ignoring {}, marked with OperatorStats.IncorrectRuntimeStatsMarker", sig.get(0));
        continue;
      }
      map.put(sig.get(0), stat.get(0));
    }
    return map.build();
  }

  private static StatsSource globalStatsSource;
  private static MetastoreStatsConnector metastoreStatsConnector;

  public static StatsSource globalStatsSource(int cacheSize) {
    if (globalStatsSource == null) {
      globalStatsSource = new CachingStatsSource(cacheSize);
    }
    return globalStatsSource;
  }

  public static StatsSource metastoreBackedStatsSource(int cacheSize, int batchSize, StatsSource parent) {
    if (metastoreStatsConnector == null) {
      metastoreStatsConnector = new MetastoreStatsConnector(cacheSize, batchSize, parent);
    }
    return metastoreStatsConnector;
  }

  @VisibleForTesting
  public static void clearGlobalStats() {
    if (metastoreStatsConnector != null) {
      metastoreStatsConnector.destroy();
    }
    globalStatsSource = null;
    metastoreStatsConnector = null;
  }

}
