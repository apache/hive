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

package org.apache.hadoop.hive.ql.exec.vector;

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.ObjectCache;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.exec.vector.util.FakeVectorRowBatchFromObjectIterables;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.VectorLimitDesc;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.junit.Test;

/**
 * Unit test for the vectorized LIMIT operator.
 */
public class TestVectorLimitOperator {

  private Random random = new Random(TestVectorLimitOperator.class.toString().hashCode());

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

  @Test
  public void testGlobalLimitReached() throws HiveException {
    // no offset
    testGlobalLimitReachedInDaemonOrContainer(true, 0, 2);
    testGlobalLimitReachedInDaemonOrContainer(false, 0, 2);

    // offset < limit
    testGlobalLimitReachedInDaemonOrContainer(true, 1, 2);
    testGlobalLimitReachedInDaemonOrContainer(false, 1, 2);

    // offset = limit
    testGlobalLimitReachedInDaemonOrContainer(true, 2, 2);
    testGlobalLimitReachedInDaemonOrContainer(false, 2, 2);

    // offset > limit
    testGlobalLimitReachedInDaemonOrContainer(true, 3, 2);
    testGlobalLimitReachedInDaemonOrContainer(false, 3, 2);
  }

  private void testGlobalLimitReachedInDaemonOrContainer(boolean isDaemon, int offset, int limit)
      throws HiveException {
    int actualNumberOfElements = 4; // from FakeVectorRowBatchFromObjectIterables

    LlapProxy.setDaemon(isDaemon);
    if (!isDaemon) {// init tez object registry
      ObjectCache.setupObjectRegistry(new ObjectRegistryImpl());
    }

    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "query-" + random.nextInt(10000));
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.set(TezProcessor.HIVE_TEZ_VERTEX_NAME, "Map 1");

    VectorLimitOperator lo1 = new VectorLimitOperator(new CompilationOpContext(),
        new LimitDesc(offset, limit), null, new VectorLimitDesc());
    lo1.initialize(conf, null);
    lo1.initializeOp(conf);

    VectorLimitOperator lo2 = new VectorLimitOperator(new CompilationOpContext(),
        new LimitDesc(offset, limit), null, new VectorLimitDesc());
    lo2.initialize(conf, null);
    lo2.initializeOp(conf);

    // operator id is important, as it's the base of the limit cache key
    // these operator instances represent the same operator running in different tasks
    Assert.assertEquals("LIM_0", lo1.getOperatorId());
    Assert.assertEquals("LIM_0", lo2.getOperatorId());

    lo1.process(getBatch(500).produceNextBatch(), 0);
    // lo1 is not done, as that's not checked after forwarding, only before next batch
    Assert.assertFalse(lo1.getDone());
    // number of processed rows properly set to global cache and is equal to limit+offset or equal
    // to batch size if limit+offset > batch size (because the operator cannot read through the
    // current batch obviously)
    Assert.assertEquals(Math.min(limit + offset, actualNumberOfElements), lo1.getCurrentCount().get());

    // if lo1 already processed enough rows, lo2 will turn to done without processing any elements
    lo2.process(getBatch(500).produceNextBatch(), 0);
    Assert.assertEquals(limit + offset <= actualNumberOfElements  ? true : false, lo2.getDone());

    // lo1 is done now, as limit is check before processing batch
    lo1.process(getBatch(500).produceNextBatch(), 0);
    Assert.assertTrue(lo1.getDone());
  }

  private void validateVectorLimitOperator(int limit, int batchSize, int expectedBatchSize)
      throws HiveException {

    FakeVectorRowBatchFromObjectIterables frboi = getBatch(batchSize);

    // Get next batch
    VectorizedRowBatch vrb = frboi.produceNextBatch();

    // Create limit desc with limit value
    LimitDesc ld = new LimitDesc(limit);
    VectorLimitDesc vectorDesc = new VectorLimitDesc();
    VectorLimitOperator lo = new VectorLimitOperator(
        new CompilationOpContext(), ld, null, vectorDesc);
    // make sure an object registry is present for the test
    ObjectCache.setupObjectRegistry(new ObjectRegistryImpl());
    lo.initialize(new Configuration(), null);

    // Process the batch
    lo.process(vrb, 0);

    // Verify batch size
    Assert.assertEquals(vrb.size, expectedBatchSize);
  }

  private FakeVectorRowBatchFromObjectIterables getBatch(int batchSize) throws HiveException {
    @SuppressWarnings("unchecked")
    FakeVectorRowBatchFromObjectIterables frboi = new FakeVectorRowBatchFromObjectIterables(
        batchSize,
        new String[] {"tinyint", "double"},
        Arrays.asList(new Object[] {1, 2, 3, 4}),
        Arrays.asList(new Object[] {323.0, 34.5, null, 89.3}));
    return frboi;
  }
}

