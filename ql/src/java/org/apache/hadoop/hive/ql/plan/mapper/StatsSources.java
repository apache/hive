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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.EquivGroup;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class StatsSources {

  public static class MapBackedStatsSource implements StatsSource {

    private Map<OpTreeSignature, OperatorStats> map = new HashMap<>();

    @Override
    public boolean canProvideStatsFor(Class<?> clazz) {
      if (Operator.class.isAssignableFrom(clazz)) {
        return true;
      }
      return false;
    }

    @Override
    public Optional<OperatorStats> lookup(OpTreeSignature treeSig) {
      return Optional.ofNullable(map.get(treeSig));
    }

    @Override
    public void putAll(Map<OpTreeSignature, OperatorStats> map) {
      map.putAll(map);
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(StatsSources.class);

  public static StatsSource getStatsSourceContaining(StatsSource currentStatsSource, PlanMapper pm) {
    if (currentStatsSource instanceof CachingStatsSource) {
      CachingStatsSource sessionStatsSource = (CachingStatsSource) currentStatsSource;
      loadFromPlanMapper(sessionStatsSource, pm);
      return sessionStatsSource;
    } else {
      return new SimpleRuntimeStatsSource(pm);
    }
  }

  public static void loadFromPlanMapper(CachingStatsSource sessionStatsSource, PlanMapper pm) {
    Map<OpTreeSignature, OperatorStats> map = extractStatMapFromPlanMapper(pm);
    sessionStatsSource.putAll(map);
  }


  private static Map<OpTreeSignature, OperatorStats> extractStatMapFromPlanMapper(PlanMapper pm) {
    Map<OpTreeSignature, OperatorStats> map = new HashMap<OpTreeSignature, OperatorStats>();
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
      if (stat.size() >= 1 && sig.size() >= 1) {
        map.put(sig.get(0), stat.get(0));
      }
    }
    return map;
  }

  private static StatsSource globalStatsSource;

  public static StatsSource globalStatsSource(HiveConf conf) {
    if (globalStatsSource == null) {
      globalStatsSource = new CachingStatsSource(conf);
    }
    return globalStatsSource;
  }

  @VisibleForTesting
  public static void clearAllStats() {
    globalStatsSource = null;
  }
}
