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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.RecordProcessor.TrackedCache;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.junit.Test;

/**
 * Tests for {@link RecordProcessor}. Behaviour exercised here lives on the
 * base class and is inherited by every subclass — {@link MapRecordProcessor},
 * {@link ReduceRecordProcessor}, and {@link MergeFileRecordProcessor} — so
 * covering it once here is enough.
 */
public class TestRecordProcessor {

  private static <T> Callable<T> constant(T value) {
    return () -> value;
  }

  /**
   * {@code TrackedCache.retrieve} must delegate to the underlying cache and
   * return the value the underlying cache produced — no wrapping, no
   * substitution. Every subclass assigns this return value directly to a
   * plan field ({@code mapWork}, {@code reduceWork}), so any drift here would
   * be a silent bug.
   */
  @Test
  public void retrieveReturnsUnderlyingValue() throws Exception {
    ObjectCache backend = mock(ObjectCache.class);
    Object sentinel = new Object();
    when(backend.retrieve(anyString(), any())).thenReturn(sentinel);
    TrackedCache planCache = new TrackedCache(backend);

    Object got = planCache.retrieve("k", constant(sentinel));

    assertEquals(sentinel, got);
    verify(backend).retrieve(any(), any());
  }

  /**
   * The plan cache and the dynamic-value cache must track their keys
   * independently — releasing one must not touch the other. Map/Reduce
   * processors rely on this invariant when they retrieve their plan plus
   * the dynamic value registry from two separate {@link TrackedCache}s.
   */
  @Test
  public void planAndDynamicValueCachesAreIndependent() throws Exception {
    ObjectCache planBackend = mock(ObjectCache.class);
    ObjectCache dvBackend = mock(ObjectCache.class);
    when(planBackend.retrieve(anyString(), any())).thenReturn("plan");
    when(dvBackend.retrieve(anyString(), any())).thenReturn("dv");

    TrackedCache planCache = new TrackedCache(planBackend);
    TrackedCache dynamicValueCache = new TrackedCache(dvBackend);

    planCache.retrieve("Map 1__MAP_PLAN__", constant("plan"));
    dynamicValueCache.retrieve("dyn-values", constant("dv"));

    planCache.releaseAll();
    verify(planBackend).release("Map 1__MAP_PLAN__");
    verify(dvBackend, never()).release(anyString()); // dv registry key not touched yet
    // dv still holds its key until we call its own releaseAll
    dynamicValueCache.releaseAll();
    verify(dvBackend).release("dyn-values");
  }

  /**
   * Regression guard for the plan cache's LLAP wiring. Two processors of the
   * same query on the same LLAP daemon must NOT share a deserialised plan —
   * sharing would let concurrent fragments race on the per-fragment operator
   * state ({@link org.apache.hadoop.hive.ql.exec.FileSinkOperator#fsp},
   * {@code VectorGroupByOperator.aggregator}, {@code VectorTopNKeyOperator}
   * filter state, {@link org.apache.hadoop.hive.ql.exec.Operator#childOperatorsArray})
   * that {@code initializeOp()} resets, producing the
   * {@code FileAlreadyExistsException} / {@code NullPointerException} class of
   * failures HIVE-14433 documents.
   *
   * <p>The test drives the real code path — it flips {@link LlapProxy} into
   * daemon mode, constructs two {@link StubRecordProcessor} instances through
   * the production constructor, and calls {@code retrieve()} on their
   * {@code planCache} fields. Each must load its own copy and the loader must
   * fire once per processor. If {@code RecordProcessor}'s constructor ever
   * flips its plan cache back to {@code llapCacheAlwaysEnabled=true}, this
   * test fails.
   */
  @Test
  public void plansAreNotSharedAcrossProcessorsOnLlap() throws Exception {
    String queryId = "record-processor-test-" + System.nanoTime();
    LlapProxy.setDaemon(true);
    try {
      JobConf conf = new JobConf();
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
      ProcessorContext ctx = mock(ProcessorContext.class);

      RecordProcessor procOne = new StubRecordProcessor(conf, ctx);
      RecordProcessor procTwo = new StubRecordProcessor(conf, ctx);

      String key = "Reducer 2__REDUCE_PLAN__";
      AtomicInteger loaderInvocations = new AtomicInteger();
      Callable<Object> loader = () -> {
        loaderInvocations.incrementAndGet();
        return new Object();
      };

      Object planFromOne = procOne.planCache.retrieve(key, loader);
      Object planFromTwo = procTwo.planCache.retrieve(key, loader);

      assertEquals("each processor must load its own plan",
          2, loaderInvocations.get());
      assertNotSame("plans must not be shared across processors on LLAP",
          planFromOne, planFromTwo);
    } finally {
      ObjectCacheFactory.removeLlapQueryCache(queryId);
      LlapProxy.setDaemon(false);
    }
  }

  /**
   * Minimal {@link RecordProcessor} subclass so the test can construct one
   * without pulling in a live Map/Reduce operator tree. The parent constructor
   * is the only behaviour under test.
   */
  private static final class StubRecordProcessor extends RecordProcessor {
    StubRecordProcessor(JobConf jconf, ProcessorContext context) {
      super(jconf, context);
    }

    @Override
    void init(MRTaskReporter mrReporter,
        Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) {
    }

    @Override
    void run() {
    }

    @Override
    void close() {
    }
  }
}
