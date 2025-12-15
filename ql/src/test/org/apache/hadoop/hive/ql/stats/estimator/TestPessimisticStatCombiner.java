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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.Test;

public class TestPessimisticStatCombiner {

  private ColStatistics createStats(long ndv, long avgColLen, long numNulls,
      long numTrues, long numFalses) {
    ColStatistics stats = new ColStatistics("col", "string");
    stats.setCountDistint(ndv);
    stats.setAvgColLen(avgColLen);
    stats.setNumNulls(numNulls);
    stats.setNumTrues(numTrues);
    stats.setNumFalses(numFalses);
    return stats;
  }

  @Test
  public void testTakesMax() {
    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(createStats(10, 5, 100, 50, 50));
    combiner.add(createStats(20, 3, 50, 80, 20));
    combiner.add(createStats(5, 8, 75, 30, 70));

    ColStatistics result = combiner.getResult().get();
    assertEquals(20, result.getCountDistint());
    assertEquals(8, (long) result.getAvgColLen());
    assertEquals(100, result.getNumNulls());
    assertEquals(80, result.getNumTrues());
    assertEquals(70, result.getNumFalses());
    assertTrue(result.isEstimated());
  }

  @Test
  public void testFilteredColumnPropagates() {
    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    ColStatistics stat1 = createStats(10, 5, 0, 0, 0);
    ColStatistics stat2 = createStats(20, 3, 0, 0, 0);
    stat2.setFilterColumn();

    combiner.add(stat1);
    combiner.add(stat2);

    assertTrue(combiner.getResult().get().isFilteredColumn());
  }

  @Test
  public void testSingleStat() {
    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(createStats(10, 5, 100, 50, 50));

    ColStatistics result = combiner.getResult().get();
    assertEquals(10, result.getCountDistint());
    assertEquals(5, (long) result.getAvgColLen());
    assertEquals(100, result.getNumNulls());
  }
}
