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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public abstract class ColumnStatsMerger<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsMerger.class);

  public abstract void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats);

  protected KllHistogramEstimator mergeHistogramEstimator(
      String columnName, KllHistogramEstimator oldEst, KllHistogramEstimator newEst) {
    if (oldEst != null && newEst != null) {
      if (oldEst.canMerge(newEst)) {
        LOG.trace("Merging old sketch {} with new sketch {}...", oldEst.getSketch(), newEst.getSketch());
        oldEst.mergeEstimators(newEst);
        LOG.trace("Resulting sketch is {}", oldEst.getSketch());
        return oldEst;
      }
      LOG.debug("Merging histograms of column {}", columnName);
    } else if (newEst != null) {
      LOG.trace("Old sketch is empty, the new sketch is used {}", newEst.getSketch());
      return newEst;
    }
    return oldEst;
  }

  protected long mergeNumDistinctValueEstimator(String columnName, List<NumDistinctValueEstimator> estimators,
      long oldNumDVs, long newNumDVs) {
    if (estimators == null || estimators.size() != 2) {
      throw new IllegalArgumentException("NDV estimators list must be set and contain exactly two elements, " +
          "found " + (estimators == null ? "null" :
          estimators.stream().map(NumDistinctValueEstimator::toString).collect(Collectors.joining(", "))));
    }

    NumDistinctValueEstimator oldEst = estimators.get(0);
    NumDistinctValueEstimator newEst = estimators.get(1);
    if (oldEst == null && newEst == null) {
      return mergeNumDVs(oldNumDVs, newNumDVs);
    }

    if (oldEst == null) {
      estimators.set(0, newEst);
      return mergeNumDVs(oldNumDVs, newEst.estimateNumDistinctValues());
    }

    final long ndv;
    if (oldEst.canMerge(newEst)) {
      oldEst.mergeEstimators(newEst);
      ndv = oldEst.estimateNumDistinctValues();
      return ndv;
    } else {
      ndv = mergeNumDVs(oldNumDVs, newNumDVs);
    }
    LOG.debug("Use bitvector to merge column {}'s ndvs of {} and {} to be {}", columnName,
        oldNumDVs, newNumDVs, ndv);
    return ndv;
  }

  public T mergeLowValue(T oldValue, T newValue) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public T mergeHighValue(T oldValue, T newValue) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public long mergeNumDVs(long oldValue, long newValue) {
    return Math.max(oldValue, newValue);
  }

  public long mergeNumNulls(long oldValue, long newValue) {
    return oldValue + newValue;
  }

  public long mergeMaxColLen(long oldValue, long newValue) {
    return Math.max(oldValue, newValue);
  }

  public double mergeAvgColLen(double oldValue, double newValue) {
    return Math.max(oldValue, newValue);
  }
}
