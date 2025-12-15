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

package org.apache.hadoop.hive.ql.udf.generic;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.junit.Test;

public class TestGenericUDFCoalesceStatEstimator {

  private ColStatistics createStats(long ndv) {
    ColStatistics stats = new ColStatistics("col", "string");
    stats.setCountDistint(ndv);
    return stats;
  }

  @Test
  public void testTakesMaxNdv() {
    StatEstimator estimator = new GenericUDFCoalesce().getStatEstimator();

    ColStatistics result = estimator.estimate(Arrays.asList(
        createStats(10),
        createStats(30),
        createStats(20)
    )).get();

    assertEquals(30, result.getCountDistint()); // MAX, not SUM
  }
}
