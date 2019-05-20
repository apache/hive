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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHyperLogLogMerge {
  // 5% tolerance for estimated count
  private float longRangeTolerance = 5.0f;
  private float shortRangeTolerance = 2.0f;

  int size;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { 1_000 }, { 10_000 }, { 100_000 }, { 1_000_000 }, { 10_000_000 }
      // { 100_000_000 }, { 1_000_000_000 } 1B passed but is super slow
    });
  }

  public TestHyperLogLogMerge(int size) {
    this.size = size;
  }

  @Test
  public void testHLLMergeDisjoint() {
    HyperLogLog hll1 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll1.addLong(i);
    }
    HyperLogLog hll2 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = size; i < 2 * size; i++) {
      hll2.addLong(i);
    }
    hll1.merge(hll2);
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    long expected = 2 * size;
    long actual = hll1.count();
    assertEquals(expected, actual, delta);
  }

  @Test
  public void testHLLMerge25PercentOverlap() {
    HyperLogLog hll1 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll1.addLong(i);
    }
    HyperLogLog hll2 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    int start = (int) (0.75 * size);
    int end = (int) (size * 1.75);
    for (int i = start; i < end; i++) {
      hll2.addLong(i);
    }
    hll1.merge(hll2);
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    long expected = (long) (1.75 * size);
    long actual = hll1.count();
    assertEquals(expected, actual, delta);
  }

  @Test
  public void testHLLMerge50PercentOverlap() {
    HyperLogLog hll1 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll1.addLong(i);
    }
    HyperLogLog hll2 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    int start = (int) (0.5 * size);
    int end = (int) (size * 1.5);
    for (int i = start; i < end; i++) {
      hll2.addLong(i);
    }
    hll1.merge(hll2);
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    long expected = (long) (1.5 * size);
    long actual = hll1.count();
    assertEquals(expected, actual, delta);
  }


  @Test
  public void testHLLMerge75PercentOverlap() {
    HyperLogLog hll1 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll1.addLong(i);
    }
    HyperLogLog hll2 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    int start = (int) (0.25 * size);
    int end = (int) (size * 1.25);
    for (int i = start; i < end; i++) {
      hll2.addLong(i);
    }
    hll1.merge(hll2);
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    long expected = (long) (1.25 * size);
    long actual = hll1.count();
    assertEquals(expected, actual, delta);
  }


  @Test
  public void testHLLMerge100PercentOverlap() {
    HyperLogLog hll1 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll1.addLong(i);
    }
    HyperLogLog hll2 = HyperLogLog.builder().setNumRegisterIndexBits(16).build();
    for (int i = 0; i < size; i++) {
      hll2.addLong(i);
    }
    hll1.merge(hll2);
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    long expected = size;
    long actual = hll1.count();
    assertEquals(expected, actual, delta);
  }

}
