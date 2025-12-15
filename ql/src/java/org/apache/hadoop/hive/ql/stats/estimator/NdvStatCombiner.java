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

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Combines {@link ColStatistics} by summing NDV (Number of Distinct Values).
 *
 * <p>Use this combiner for expressions with mutually exclusive branches
 * (CASE/WHEN/IF) where exactly one branch executes per row. Since branches
 * are disjoint, each contributes its own distinct values to the output,
 * so the total NDV is the sum of branch NDVs.</p>
 *
 * <p>Example: {@code CASE WHEN c1 THEN 'A' WHEN c2 THEN 'B' ELSE 'C' END}
 * has 3 constant branches, each with NDV=1. The output NDV = 1+1+1 = 3.</p>
 *
 * <p>Contrast with {@link PessimisticStatCombiner} which takes MAX of NDVs,
 * appropriate when values may overlap (e.g., COALESCE).</p>
 */
public class NdvStatCombiner extends PessimisticStatCombiner {

  @Override
  protected void combineCountDistinct(ColStatistics stat) {
    result.setCountDistint(result.getCountDistint() + stat.getCountDistint());
  }
}
