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
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.stats.OperatorStats;

public class SimpleRuntimeStatsSource implements StatsSource {

  private final PlanMapper pm;


  public SimpleRuntimeStatsSource(PlanMapper pm) {
    this.pm = pm;
  }

  @Override
  public Optional<OperatorStats> lookup(OpTreeSignature sig) {
    try {
      List<OperatorStats> v = pm.lookupAll(OperatorStats.class, sig);
      if (v.size() > 0) {
        return Optional.of(v.get(0));
      }
      return Optional.empty();
    } catch (NoSuchElementException | IllegalArgumentException iae) {
      return Optional.empty();
    }
  }

  @Override
  public boolean canProvideStatsFor(Class<?> class1) {
    if (Operator.class.isAssignableFrom(class1)) {
      return true;
    }
    return false;
  }

}
