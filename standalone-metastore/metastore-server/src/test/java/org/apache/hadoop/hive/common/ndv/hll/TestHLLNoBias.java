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
package org.apache.hadoop.hive.common.ndv.hll;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestHLLNoBias {

  // 1.5% tolerance for long range bias (when no bias enabled) and 5% when (no
  // bias is disabled) and
  // 0.5% for short range bias
  private float noBiaslongRangeTolerance = 2.0f;
  private float biasedlongRangeTolerance = 5.0f;
  private float shortRangeTolerance = 0.5f;

  private int size;

  public TestHLLNoBias(int n) {
    this.size = n;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 30000 }, { 41000 }, { 50000 }, { 60000 }, { 75000 },
        { 80000 }, { 81920 } };
    return Arrays.asList(data);
  }

  @Test
  public void testHLLAdd() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().build();
    int size = 100;
    for (int i = 0; i < size; i++) {
      hll.addLong(rand.nextLong());
    }
    double threshold = size > 40000 ? noBiaslongRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) hll.count(), delta);
  }

  @Test
  public void testHLLAddHalfDistinct() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().build();
    int unique = size / 2;
    Set<Long> hashset = new HashSet<>();
    for (int i = 0; i < size; i++) {
      long val = rand.nextInt(unique);
      hashset.add(val);
      hll.addLong(val);
    }
    double threshold = size > 40000 ? noBiaslongRangeTolerance : shortRangeTolerance;
    double delta = threshold * hashset.size() / 100;
    assertEquals((double) hashset.size(), (double) hll.count(), delta);
  }

  @Test
  public void testHLLNoBiasDisabled() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().enableNoBias(false).build();
    int size = 100;
    for (int i = 0; i < size; i++) {
      hll.addLong(rand.nextLong());
    }
    double threshold = size > 40000 ? biasedlongRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals((double) size, (double) hll.count(), delta);
  }

  @Test
  public void testHLLNoBiasDisabledHalfDistinct() {
    Random rand = new Random(size);
    HyperLogLog hll = HyperLogLog.builder().enableNoBias(false).build();
    int unique = size / 2;
    Set<Long> hashset = new HashSet<>();
    for (int i = 0; i < size; i++) {
      long val = rand.nextInt(unique);
      hashset.add(val);
      hll.addLong(val);
    }
    double threshold = size > 40000 ? biasedlongRangeTolerance : shortRangeTolerance;
    double delta = threshold * hashset.size() / 100;
    assertEquals((double) hashset.size(), (double) hll.count(), delta);
  }

}
