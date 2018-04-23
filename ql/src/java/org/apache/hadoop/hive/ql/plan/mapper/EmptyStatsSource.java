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

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.stats.OperatorStats;

public final class EmptyStatsSource implements StatsSource {

  public static StatsSource INSTANCE = new EmptyStatsSource();

  private EmptyStatsSource() {
  }

  @Override
  public boolean canProvideStatsFor(Class<?> class1) {
    return false;
  }

  @Override
  public Optional<OperatorStats> lookup(OpTreeSignature treeSig) {
    return Optional.empty();
  }

  @Override
  public void putAll(Map<OpTreeSignature, OperatorStats> map) {
    throw new RuntimeException("This is an empty source!");
  }

}
