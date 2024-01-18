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
package org.apache.hadoop.hive.ql.exec;

import java.util.Random;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.tez.ObjectCache;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.junit.Assert;
import org.junit.Test;

public class TestLimitOperator {

  private static final Object fakeObjectToPass = new Object();
  private Random random = new Random(TestLimitOperator.class.toString().hashCode());

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
    int numProcessedElements = 0; // from FakeVectorRowBatchFromObjectIterables

    LlapProxy.setDaemon(isDaemon);
    if (!isDaemon) {// init tez object registry
      ObjectCache.setupObjectRegistry(new ObjectRegistryImpl());
    }

    HiveConf conf = new HiveConf();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "query-" + random.nextInt(10000));
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.set(TezProcessor.HIVE_TEZ_VERTEX_NAME, "Map 1");

    LimitOperator lo1 = new LimitOperator(new CompilationOpContext());
    lo1.setConf(new LimitDesc(offset, limit));
    lo1.initialize(conf, null);
    lo1.initializeOp(conf);

    LimitOperator lo2 = new LimitOperator(new CompilationOpContext());
    lo2.setConf(new LimitDesc(offset, limit));
    lo2.initialize(conf, null);
    lo2.initializeOp(conf);

    Assert.assertEquals(0, lo1.currCount);
    Assert.assertEquals(0, lo2.currCount);

    // operator id is important, as it's the base of the limit cache key
    // these operator instances represent the same operator running in different tasks
    Assert.assertEquals("LIM_0", lo1.getOperatorId());
    Assert.assertEquals("LIM_0", lo2.getOperatorId());

    // assertion no.1: unlike VectorLimitOperator, we op.process checks limit before every element,
    // so we can notice limit reached while processing the offset+limit+1st element, so op.getDone()
    // is true if we already processed at least limit + offset

    // assertion no.2: number of processed rows properly set to global cache and is equal to the
    // count by which op.process was called

    // assertion no.3: the local counter is in sync with the global counter (in this test case, no
    // other tasks work concurrently)

    // element: 1,2
    processRowNTimes(lo1, 2);
    numProcessedElements += 2;
    Assert.assertEquals(numProcessedElements > limit + offset, lo1.getDone());
    Assert.assertEquals(Math.min(numProcessedElements, limit + offset),
        lo1.getCurrentCount().get());
    Assert.assertEquals(lo1.getCurrentCount().get(), lo1.currCount);

    // element: 3
    processRowNTimes(lo1, 1);
    numProcessedElements += 1;
    Assert.assertEquals(numProcessedElements > limit + offset, lo1.getDone());
    Assert.assertEquals(Math.min(numProcessedElements, limit + offset),
        lo1.getCurrentCount().get());
    Assert.assertEquals(lo1.getCurrentCount().get(), lo1.currCount);

    // element: 4
    processRowNTimes(lo1, 1);
    numProcessedElements += 1;
    Assert.assertEquals(numProcessedElements > limit + offset, lo1.getDone());
    Assert.assertEquals(Math.min(numProcessedElements, limit + offset),
        lo1.getCurrentCount().get());
    Assert.assertEquals(lo1.getCurrentCount().get(), lo1.currCount);

    // if lo1 already processed enough rows, lo2 will turn to done without processing any elements
    // lo2.getCurrentCount().get() should return the same as lo1.getCurrentCount().get()
    Assert.assertEquals(Math.min(numProcessedElements, limit + offset),
        lo2.getCurrentCount().get());
    // ...but lo2's current count hasn't been touched yet, as process hasn't been called
    Assert.assertEquals(0, lo2.currCount);
    // getDone() = false before processing
    Assert.assertEquals(false, lo2.getDone());

    // try to process one more element with op2
    processRowNTimes(lo2, 1);
    // op2 will be noticed as done only if "numProcessedElements" (the number of elements processed
    // by lo1) is more than limit + offset + 1, because in that case lo2 has nothing to do
    boolean lo2DoneExpected = numProcessedElements > limit + offset + 1;
    Assert.assertEquals(lo2DoneExpected, lo2.getDone());
    // if lo2 is done, it hasn't processed any elements (currCount=0), otherwise it processed the
    // new element
    int lo2Count = lo2.currCount;
    Assert.assertEquals(lo2DoneExpected ? 0 : 1, lo2.currCount);

    // repeat once more (to test cases where limit+offset+1 < number of all elements to process
    processRowNTimes(lo2, 1);
    if (!lo2DoneExpected) {// if lo2 had the chance to process one more element (!done) ...
      // ... let's count that in
      numProcessedElements += 1;
      if (lo2.getDone()) { // turn to done after processing => hasn't processed any element
        Assert.assertEquals(lo2Count, lo2.currCount);
      } else { // hasn't turned to done after processing => processed 1 more element
        Assert.assertEquals(lo2Count + 1, lo2.currCount);
      }
    } else {
      // current count hasn't changed
      Assert.assertEquals(lo2Count, lo2.currCount);
    }
    lo2DoneExpected = numProcessedElements > limit + offset + 1;
    Assert.assertEquals(lo2DoneExpected, lo2.getDone());
  }

  private void processRowNTimes(LimitOperator op, int n) throws HiveException {
    for (int i = 0; i < n; i++) {
      op.process(fakeObjectToPass, 0);
    }
  }
}
