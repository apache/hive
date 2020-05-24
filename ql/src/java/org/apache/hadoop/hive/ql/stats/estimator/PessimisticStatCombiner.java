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

package org.apache.hadoop.hive.ql.stats.estimator;

import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Combines {@link ColStatistics} objects to provide the most pessimistic estimate.
 */
public class PessimisticStatCombiner {

  private boolean inited;
  private ColStatistics result;

  public void add(ColStatistics stat) {
    if (!inited) {
      inited = true;
      result = stat.clone();
      result.setRange(null);
      result.setIsEstimated(true);
      return;
    } else {
      if (stat.getAvgColLen() > result.getAvgColLen()) {
        result.setAvgColLen(stat.getAvgColLen());
      }
      if (stat.getCountDistint() > result.getCountDistint()) {
        result.setCountDistint(stat.getCountDistint());
      }
      if (stat.getNumNulls() > result.getNumNulls()) {
        result.setNumNulls(stat.getNumNulls());
      }
      if (stat.getNumTrues() > result.getNumTrues()) {
        result.setNumTrues(stat.getNumTrues());
      }
      if (stat.getNumFalses() > result.getNumFalses()) {
        result.setNumFalses(stat.getNumFalses());
      }
      if (stat.isFilteredColumn()) {
        result.setFilterColumn();
      }

    }

  }
  public Optional<ColStatistics> getResult() {
    return Optional.of(result);

  }
}
