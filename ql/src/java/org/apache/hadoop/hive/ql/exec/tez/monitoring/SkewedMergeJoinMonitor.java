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

package org.apache.hadoop.hive.ql.exec.tez.monitoring;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkewedMergeJoinMonitor implements SkewedJoinMonitor {

  private final long mergeJoinSkewThreshold;
  private final boolean mergeJoinSkewAbort;
  private final long mergeJoinSkewCheckInterval;
  private final boolean[] skewedKeyFlagged;

  private static final Logger LOG = LoggerFactory.getLogger(SkewedMergeJoinMonitor.class.getName());

  public SkewedMergeJoinMonitor(long mergeJoinSkewThreshold, boolean mergeJoinSkewAbort,
      long mergeJoinSkewCheckInterval, int maxAlias) {
    this.mergeJoinSkewThreshold = mergeJoinSkewThreshold;
    this.mergeJoinSkewAbort = mergeJoinSkewAbort;
    this.mergeJoinSkewCheckInterval = mergeJoinSkewCheckInterval > 0 ? mergeJoinSkewCheckInterval : 1;
    skewedKeyFlagged = new boolean[maxAlias];
  }

  @VisibleForTesting
  public boolean isActive() {
    return mergeJoinSkewThreshold > 0;
  }

  @VisibleForTesting
  public boolean shouldBeFlagged(byte alias, long rowCount) {
    return rowCount >= mergeJoinSkewThreshold && !skewedKeyFlagged[alias];
  }

  @VisibleForTesting
  public boolean isFlagged(int alias) {
    return skewedKeyFlagged[alias];
  }

  @VisibleForTesting
  public boolean isDueForCheck(long rowCount) {
    return (rowCount % mergeJoinSkewCheckInterval == 0) || (rowCount >= mergeJoinSkewThreshold);
  }

  @Override
  public void checkMergeJoinSkew(byte alias, long rowCount, String joinKeyColumns, String tableAlias)
      throws HiveException {
    if (!isActive()) {
      return;
    }
    if (skewedKeyFlagged[alias]) {
      return;
    }
    if (!isDueForCheck(rowCount)) {
      return;
    }
    if (!shouldBeFlagged(alias, rowCount)) {
      return;
    }

    skewedKeyFlagged[alias] = true;

    String msg = String.format(
        "Data skew detected in merge join: %d rows accumulated for join column(s) [%s]"
            + " in table alias [%s]. Consider reviewing data distribution.",
        rowCount, joinKeyColumns, tableAlias);

    if (mergeJoinSkewAbort) {
      throw new HiveException(msg);
    } else {
      LOG.warn(msg);
    }
  }

  public static SkewedJoinMonitor createSkewedJoinMonitor(long mergeJoinSkewThreshold, boolean mergeJoinSkewAbort,
      long mergeJoinSkewCheckInterval, int maxAlias) {
    if (mergeJoinSkewThreshold > 0) {
      return new SkewedMergeJoinMonitor(mergeJoinSkewThreshold, mergeJoinSkewAbort, mergeJoinSkewCheckInterval,
          maxAlias);
    } else {
      return new NoopSkewedMergeJoinMonitor();
    }
  }
}
