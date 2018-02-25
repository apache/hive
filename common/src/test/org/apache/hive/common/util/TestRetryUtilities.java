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
package org.apache.hive.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hive.common.util.RetryUtilities.ExponentiallyDecayingBatchWork;
import org.apache.hive.common.util.RetryUtilities.RetryException;
import org.junit.Assert;
import org.junit.Test;

public class TestRetryUtilities {

  private class DummyExponentiallyDecayingBatchWork extends ExponentiallyDecayingBatchWork<Void> {
    public DummyExponentiallyDecayingBatchWork(int batchSize, int reducingFactor,
        int throwException) {
      super(batchSize, reducingFactor, 0);
      this.exceptionCount = throwException;
    }

    public DummyExponentiallyDecayingBatchWork(int batchSize, int reducingFactor,
        int throwException, int maxRetries) {
      super(batchSize, reducingFactor, maxRetries);
      this.exceptionCount = throwException;
    }

    final List<Integer> batchSizes = new ArrayList<>();
    int exceptionCount = 0;

    @Override
    public Void execute(int size) throws Exception {
      batchSizes.add(size);
      if (exceptionCount > 0) {
        exceptionCount--;
        throw new Exception("Dummy exception");
      }
      return null;
    }

    public int getCount() {
      return batchSizes.size();
    }

    public int[] getBatchSizes() {
      int[] ret = new int[batchSizes.size()];
      int i = 0;
      for (int b : batchSizes) {
        ret[i++] = b;
      }
      return ret;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroBatchSize() {
    new DummyExponentiallyDecayingBatchWork(0, 2, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeBatchSize() {
    new DummyExponentiallyDecayingBatchWork(-1, 2, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroDecayingFactor() {
    new DummyExponentiallyDecayingBatchWork(5, 0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOneDecayingFactor() {
    new DummyExponentiallyDecayingBatchWork(10, 1, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxRetries() {
    new DummyExponentiallyDecayingBatchWork(10, 2, 0, -1);
  }

  @Test
  public void testNumberOfAttempts() throws Exception {
    // test perfectly divisible batchsize and decaying factor
    DummyExponentiallyDecayingBatchWork dummy = new DummyExponentiallyDecayingBatchWork(10, 2, 0);
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 1, dummy.getCount());
    // there were no exception. Batchsize doesn't change until there is an exception
    Assert.assertArrayEquals(new int[] { 10 }, dummy.getBatchSizes());
    // test batchsize is not divisible by decaying factor
    dummy = new DummyExponentiallyDecayingBatchWork(11, 2, 0);
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 1, dummy.getCount());
    // there were no exception. Batchsize doesn't change until there is an exception
    Assert.assertArrayEquals(new int[] { 11 }, dummy.getBatchSizes());

    dummy = new DummyExponentiallyDecayingBatchWork(11, 3, 1);
    // batches will be sized 11,3
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 2, dummy.getCount());
    Assert.assertArrayEquals(new int[] { 11, 3 }, dummy.getBatchSizes());

    dummy = new DummyExponentiallyDecayingBatchWork(11, 3, 2);
    // batches will be sized 11,3,1
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 3, dummy.getCount());
    Assert.assertArrayEquals(new int[] { 11, 3, 1 }, dummy.getBatchSizes());

    dummy = new DummyExponentiallyDecayingBatchWork(12, 3, 2);
    // batches will be sized 12,4,1
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 3, dummy.getCount());
    Assert.assertArrayEquals(new int[] { 12, 4, 1 }, dummy.getBatchSizes());
  }

  @Test
  public void testZeroMaxRetriesValue() throws Exception {
    DummyExponentiallyDecayingBatchWork dummy = new DummyExponentiallyDecayingBatchWork(10, 2, 3, 0);
    dummy.run();
    // batches will be sized 10, 5, 2, 1
    Assert.assertEquals("Unexpected number of executions of execute method", 4, dummy.getCount());
    Assert.assertArrayEquals(new int[] { 10, 5, 2, 1 }, dummy.getBatchSizes());

    dummy = new DummyExponentiallyDecayingBatchWork(17, 2, 4, 0);
    // batches will be sized 17, 8, 4, 2, 1
    dummy.run();
    Assert.assertEquals("Unexpected number of executions of execute method", 5, dummy.getCount());
    Assert.assertArrayEquals(new int[] { 17, 8, 4, 2, 1 }, dummy.getBatchSizes());
  }

  @Test(expected = RetryException.class)
  public void testRetriesExhausted() throws Exception {
    // attempts at execute will be made using batchsizes 11, 3, 1, throws retry exception
    DummyExponentiallyDecayingBatchWork dummy = new DummyExponentiallyDecayingBatchWork(11, 3, 3);
    dummy.run();
  }
}
