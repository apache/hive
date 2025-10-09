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

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Enables statistics related computation on UDFs
 */
public interface StatEstimator {

  /**
   * Computes the output statistics of the actual UDF.
   *
   * The estimator should return with a preferably overestimated {@link ColStatistics} object if possible.
   * The actual estimation logic may decide to not give an estimation; it should return with {@link Optional#empty()}.
   *
   * Note: at the time of the call there will be {@link ColStatistics} for all the arguments; if that is not available - the estimation is skipped.
   *
   * @param argStats the statistics for every argument of the UDF
   * @return {@link ColStatistics} estimate for the actual UDF.
   */
  public Optional<ColStatistics> estimate(List<ColStatistics> argStats);
}
