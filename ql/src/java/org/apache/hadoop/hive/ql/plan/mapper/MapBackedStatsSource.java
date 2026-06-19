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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.RelTreeSignature;
import org.apache.hadoop.hive.ql.stats.OperatorStats;

public class MapBackedStatsSource implements StatsSource {

  private Map<OpTreeSignature, OperatorStats> map = new ConcurrentHashMap<>();
  private Map<RelTreeSignature, OperatorStats> map2 = new ConcurrentHashMap<>();

  @Override
  public boolean canProvideStatsFor(Class<?> clazz) {
    if (Operator.class.isAssignableFrom(clazz)) {
      return true;
    }
    if (HiveFilter.class.isAssignableFrom(clazz)) {
      return true;
    }
    return false;
  }

  @Override
  public Optional<OperatorStats> lookup(OpTreeSignature treeSig) {
    return Optional.ofNullable(map.get(treeSig));
  }


  @Override
  public Optional<OperatorStats> lookup(RelTreeSignature of) {
    return Optional.ofNullable(map2.get(of));
  }

  @Override
  public void load(List<PersistedRuntimeStats> statMap) {
    for (PersistedRuntimeStats persistedRuntimeStats : statMap) {
      if (persistedRuntimeStats.sig != null) {
        map.put(persistedRuntimeStats.sig, persistedRuntimeStats.stat);
      }
      if (persistedRuntimeStats.rSig != null) {
        map2.put(persistedRuntimeStats.rSig, persistedRuntimeStats.stat);
      }
    }
  }
}