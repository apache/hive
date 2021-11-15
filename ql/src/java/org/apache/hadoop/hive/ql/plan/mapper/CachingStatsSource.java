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
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.RelTreeSignature;
import org.apache.hadoop.hive.ql.stats.OperatorStats;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class CachingStatsSource implements StatsSource {

  private final Cache<Object, OperatorStats> cache;

  public CachingStatsSource(int cacheSize) {
    cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build();
  }

  @Override
  public Optional<OperatorStats> lookup(OpTreeSignature treeSig) {
    return Optional.ofNullable(cache.getIfPresent(treeSig));
  }

  @Override
  public Optional<OperatorStats> lookup(RelTreeSignature treeSig) {
    return Optional.ofNullable(cache.getIfPresent(treeSig));
  }

  @Override
  public boolean canProvideStatsFor(Class<?> clazz) {
    if (cache.size() > 0 && Operator.class.isAssignableFrom(clazz)) {
      return true;
    }
    return false;
  }

  private void put(OpTreeSignature sig, OperatorStats opStat) {
    cache.put(sig, opStat);
  }

  @Override
  public void load(List<PersistedRuntimeStats> statMap) {
    for (PersistedRuntimeStats entry : statMap) {
      if (entry.rSig != null) {
        cache.put(entry.rSig, entry.stat);
      }
      if (entry.sig != null) {
        cache.put(entry.sig, entry.stat);
      }
    }
  }

}
