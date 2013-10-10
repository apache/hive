/**
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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromObjectIterables;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.junit.Test;

/**
 * Unit test for the vectorized LIMIT operator.
 */
public class TestVectorLimitOperator {

  @Test
  public void testLimitLessThanBatchSize() throws HiveException {
    validateVectorLimitOperator(2, 5, 2);
  }

  @Test
  public void testLimitGreaterThanBatchSize() throws HiveException {
    validateVectorLimitOperator(100, 3, 3);
  }

  @Test
  public void testLimitWithZeroBatchSize() throws HiveException {
    validateVectorLimitOperator(5, 0, 0);
  }

  private void validateVectorLimitOperator(int limit, int batchSize, int expectedBatchSize)
      throws HiveException {

    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables frboi = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"tinyint", "double"},
        Arrays.asList(new Object[] {1, 2, 3, 4}),
        Arrays.asList(new Object[] {323.0, 34.5, null, 89.3}));

    // Get next batch
    VectorizedRowBatch vrb = frboi.produceNextBatch();

    // Create limit desc with limit value
    LimitDesc ld = new LimitDesc(limit);
    VectorLimitOperator lo = new VectorLimitOperator(null, ld);
    lo.initialize(new Configuration(), null);

    // Process the batch
    lo.processOp(vrb, 0);

    // Verify batch size
    Assert.assertEquals(vrb.size, expectedBatchSize);
  }
}

