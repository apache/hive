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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog.EncodingType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestHyperLogLog {
  // 5% tolerance for estimated count
  private float longRangeTolerance = 5.0f;
  private float shortRangeTolerance = 2.0f;

  @Test(expected = IllegalArgumentException.class)
  public void testHLLDenseMerge() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll2 = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll3 = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll4 = HyperLogLog.builder().setNumRegisterIndexBits(16)
        .setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll5 = HyperLogLog.builder().setNumRegisterIndexBits(12)
        .setEncoding(EncodingType.DENSE).build();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
      hll2.addLong(size + i);
      hll3.addLong(2 * size + i);
      hll4.addLong(3 * size + i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    double delta4 = threshold * (4*size) / 100;
    assertEquals(size, hll.count(), delta);
    assertEquals(size, hll2.count(), delta);

    // merge
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // merge should update registers and hence the count
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // new merge
    hll.merge(hll3);
    assertEquals((double) 3 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // valid merge -- register set size gets bigger (also 4k items
    hll.merge(hll4);
    assertEquals((double) 4 * size, hll.count(), delta4);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // invalid merge -- smaller register merge to bigger
    hll.merge(hll5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHLLSparseMerge() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll2 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll3 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll4 = HyperLogLog.builder().setNumRegisterIndexBits(16)
        .setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll5 = HyperLogLog.builder().setNumRegisterIndexBits(12)
        .setEncoding(EncodingType.SPARSE).build();
    int size = 500;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
      hll2.addLong(size + i);
      hll3.addLong(2 * size + i);
      hll4.addLong(3 * size + i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    double delta4 = threshold * (4*size) / 100;
    assertEquals(size, hll.count(), delta);
    assertEquals(size, hll2.count(), delta);

    // merge
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // merge should update registers and hence the count
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // new merge
    hll.merge(hll3);
    assertEquals((double) 3 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // valid merge -- register set size gets bigger & dense automatically
    hll.merge(hll4);
    assertEquals((double) 4 * size, hll.count(), delta4);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // invalid merge -- smaller register merge to bigger
    hll.merge(hll5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHLLSparseDenseMerge() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll2 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll3 = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll4 = HyperLogLog.builder().setNumRegisterIndexBits(16)
        .setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll5 = HyperLogLog.builder().setNumRegisterIndexBits(12)
        .setEncoding(EncodingType.DENSE).build();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
      hll2.addLong(size + i);
      hll3.addLong(2 * size + i);
      hll4.addLong(3 * size + i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals(size, hll.count(), delta);
    assertEquals(size, hll2.count(), delta);

    // sparse-sparse merge
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // merge should update registers and hence the count
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // sparse-dense merge
    hll.merge(hll3);
    assertEquals((double) 3 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // merge should convert hll2 to DENSE
    hll2.merge(hll4);
    assertEquals((double) 2 * size, hll2.count(), delta);
    assertEquals(EncodingType.DENSE, hll2.getEncoding());

    // invalid merge -- smaller register merge to bigger
    hll.merge(hll5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHLLDenseSparseMerge() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll2 = HyperLogLog.builder().setEncoding(EncodingType.DENSE).build();
    HyperLogLog hll3 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll4 = HyperLogLog.builder().setNumRegisterIndexBits(16)
        .setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll5 = HyperLogLog.builder().setNumRegisterIndexBits(12)
        .setEncoding(EncodingType.SPARSE).build();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
      hll2.addLong(size + i);
      hll3.addLong(2 * size + i);
      hll4.addLong(3 * size + i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals(size, hll.count(), delta);
    assertEquals(size, hll2.count(), delta);

    // sparse-sparse merge
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // merge should update registers and hence the count
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // sparse-dense merge
    hll.merge(hll3);
    assertEquals((double) 3 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // merge should convert hll3 to DENSE
    hll3.merge(hll4);
    assertEquals((double) 2 * size, hll3.count(), delta);
    assertEquals(EncodingType.DENSE, hll3.getEncoding());

    // invalid merge -- smaller register merge to bigger
    hll.merge(hll5);

  }

  @Test(expected = IllegalArgumentException.class)
  public void testHLLSparseOverflowMerge() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll2 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll3 = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll4 = HyperLogLog.builder().setNumRegisterIndexBits(16)
        .setEncoding(EncodingType.SPARSE).build();
    HyperLogLog hll5 = HyperLogLog.builder().setNumRegisterIndexBits(12)
        .setEncoding(EncodingType.SPARSE).build();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
      hll2.addLong(size + i);
      hll3.addLong(2 * size + i);
      hll4.addLong(3 * size + i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals(size, hll.count(), delta);
    assertEquals(size, hll2.count(), delta);

    // sparse-sparse merge
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // merge should update registers and hence the count
    hll.merge(hll2);
    assertEquals((double) 2 * size, hll.count(), delta);
    assertEquals(EncodingType.SPARSE, hll.getEncoding());

    // sparse-sparse overload to dense
    hll.merge(hll3);
    assertEquals((double) 3 * size, hll.count(), delta);
    assertEquals(EncodingType.DENSE, hll.getEncoding());

    // merge should convert hll2 to DENSE
    hll2.merge(hll4);
    assertEquals((double) 2 * size, hll2.count(), delta);
    assertEquals(EncodingType.DENSE, hll2.getEncoding());

    // invalid merge -- smaller register merge to bigger
    hll.merge(hll5);
  }

  @Test
  public void testHLLSparseMoreRegisterBits() {
    HyperLogLog hll = HyperLogLog.builder().setEncoding(EncodingType.SPARSE)
        .setNumRegisterIndexBits(16).build();
    int size = 1000;
    for (int i = 0; i < size; i++) {
      hll.addLong(i);
    }
    double threshold = size > 40000 ? longRangeTolerance : shortRangeTolerance;
    double delta = threshold * size / 100;
    assertEquals(size, hll.count(), delta);
  }

  @Test
  public void testHLLSquash() {

    int[] sizes = new int[] { 500, 1000, 2300, 4096};
    int minBits = 9;
    for (final int size : sizes) {

      HyperLogLog hlls[] = new HyperLogLog[16];
      for (int k = minBits; k < hlls.length; k++) {
        final HyperLogLog hll = HyperLogLog.builder()
            .setEncoding(EncodingType.DENSE).setNumRegisterIndexBits(k).build();
        for (int i = 0; i < size; i++) {
          hll.addLong(i);
        }
        hlls[k] = hll;
      }

      for (int k = minBits; k < hlls.length; k++) {
        for (int j = k + 1; j < hlls.length; j++) {
          final HyperLogLog large = hlls[j];
          final HyperLogLog small = hlls[k];
          final HyperLogLog mush = large
              .squash(small.getNumRegisterIndexBits());
          assertEquals(small.count(), mush.count(), 0);
          double delta = Math.ceil(small.getStandardError()*size);
          assertEquals(size, mush.count(), delta);
        }
      }
    }
  }

  @Test
  public void testHLLDenseDenseSquash() {
    HyperLogLog p14HLL = HyperLogLog.builder().setEncoding(EncodingType.DENSE).setNumRegisterIndexBits(14).build();
    HyperLogLog p10HLL = HyperLogLog.builder().setEncoding(EncodingType.DENSE).setNumRegisterIndexBits(10).build();
    int size = 1_000_000;
    for (int i = 0; i < size; i++) {
      p14HLL.addLong(i);
    }

    for (int i = 0; i < 10_000; i++) {
      p10HLL.addLong(i);
    }

    p14HLL.squash(p10HLL.getNumRegisterIndexBits());
    assertEquals(size, p14HLL.count(), longRangeTolerance * size / 100.0);
  }

  @Test
  public void testHLLSparseDenseSquash() {
    HyperLogLog p14HLL = HyperLogLog.builder().setEncoding(EncodingType.SPARSE).setNumRegisterIndexBits(14).build();
    HyperLogLog p10HLL = HyperLogLog.builder().setEncoding(EncodingType.DENSE).setNumRegisterIndexBits(10).build();
    int size = 2000;
    for (int i = 0; i < size; i++) {
      p14HLL.addLong(i);
    }

    for (int i = 0; i < 10_000; i++) {
      p10HLL.addLong(i);
    }

    p14HLL.squash(p10HLL.getNumRegisterIndexBits());
    assertEquals(size, p14HLL.count(), longRangeTolerance * size / 100.0);
  }

  @Test
  public void testAbletoRetainAccuracyUpToSwitchThreshold70() {
    testRetainAccuracy(70);
  }

  @Test
  public void testAbletoRetainAccuracyUpToSwitchThresholdMaxPer2() {
    int maxThreshold = HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold();
    testRetainAccuracy(maxThreshold / 2);
  }

  @Test
  public void testAbletoRetainAccuracyUpToSwitchThresholdMax() {
    int maxThreshold = HyperLogLog.builder().setSizeOptimized().build().getEncodingSwitchThreshold();
    testRetainAccuracy(maxThreshold);
  }

  private void testRetainAccuracy(int numElements) {
    HyperLogLog h = HyperLogLog.builder().setSizeOptimized().build();
    assertTrue(numElements <= h.getEncodingSwitchThreshold());
    for (int ia = 0; ia <= 10; ia++) {
      for (int i = 1; i <= numElements; i++) {
        h.addLong(i);
      }
    }
    assertEquals(numElements, h.estimateNumDistinctValues());
  }

}
