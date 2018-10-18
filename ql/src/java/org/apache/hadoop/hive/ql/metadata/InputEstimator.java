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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.mapred.JobConf;

// Plugin interface for storage handler which supports input estimation
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface InputEstimator {

  /**
   * Estimate input size based on filter and projection on table scan operator
   *
   * @param remaining Early exit condition. If it has positive value, further estimation
   *                  can be canceled on the point of exceeding it. In this case,
   *                  return any bigger length value then this (Long.MAX_VALUE, for eaxmple).
   */
  Estimation estimate(JobConf job, TableScanOperator ts, long remaining) throws HiveException;

  public static class Estimation {

    private int rowCount;
    private long totalLength;

    public Estimation(int rowCount, long totalLength) {
      this.rowCount = rowCount;
      this.totalLength = totalLength;
    }

    public int getRowCount() {
      return rowCount;
    }

    public long getTotalLength() {
      return totalLength;
    }
  }
}
