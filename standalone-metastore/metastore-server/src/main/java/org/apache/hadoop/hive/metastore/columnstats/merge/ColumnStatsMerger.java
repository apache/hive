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
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ColumnStatsMerger {

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
}
