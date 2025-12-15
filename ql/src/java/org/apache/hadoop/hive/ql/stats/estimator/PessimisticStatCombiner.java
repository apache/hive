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
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Combines {@link ColStatistics} objects using MAX (pessimistic estimate).
 *
 * <p>Subclasses can override individual combine methods to change behavior for specific fields.
 * For example, {@link NdvStatCombiner} overrides {@link #combineCountDistinct} to sum NDVs
 * for mutually exclusive branches (IF/CASE/WHEN).</p>
 */
public class PessimisticStatCombiner {

  protected ColStatistics result;

  public void add(ColStatistics stat) {
    if (result == null) {
      init(stat);
      return;
    }

    combineAvgColLen(stat);
    combineCountDistinct(stat);
    combineNumNulls(stat);
    combineNumTrues(stat);
    combineNumFalses(stat);
    combineFiltered(stat);
  }

  protected void init(ColStatistics stat) {
    result = stat.clone();
    result.setRange(null);
    result.setIsEstimated(true);
  }

  protected void combineAvgColLen(ColStatistics stat) {
    max(stat.getAvgColLen(), result.getAvgColLen(), result::setAvgColLen);
  }

  protected void combineCountDistinct(ColStatistics stat) {
    max(stat.getCountDistint(), result.getCountDistint(), result::setCountDistint);
  }

  protected void combineNumNulls(ColStatistics stat) {
    max(stat.getNumNulls(), result.getNumNulls(), result::setNumNulls);
  }

  protected void combineNumTrues(ColStatistics stat) {
    max(stat.getNumTrues(), result.getNumTrues(), result::setNumTrues);
  }

  protected void combineNumFalses(ColStatistics stat) {
    max(stat.getNumFalses(), result.getNumFalses(), result::setNumFalses);
  }

  protected void combineFiltered(ColStatistics stat) {
    if (stat.isFilteredColumn()) {
      result.setFilterColumn();
    }
  }

  protected void max(long incoming, long current, LongConsumer setter) {
    if (incoming > current) {
      setter.accept(incoming);
    }
  }

  protected void max(double incoming, double current, DoubleConsumer setter) {
    if (incoming > current) {
      setter.accept(incoming);
    }
  }

  public Optional<ColStatistics> getResult() {
    return Optional.of(result);
  }
}
