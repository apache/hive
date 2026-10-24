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
package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * A drain thread that dies still leaves the executor terminated and its Future done, so the
 * loader has to read the futures back before sealing the table; otherwise the join runs against
 * a hash table missing one partition's rows.
 */
public class TestVectorMapJoinFastHashTableLoader {

  @Test
  public void testSuccessfulFuturesAreSilent() throws Exception {
    List<Future<?>> futures =
        Arrays.asList(CompletableFuture.completedFuture(null), CompletableFuture.completedFuture(null));
    VectorMapJoinFastHashTableLoader.rethrowDrainFailures(futures);
    VectorMapJoinFastHashTableLoader.rethrowDrainFailures(Collections.emptyList());
  }

  @Test
  public void testMemoryExhaustionErrorPropagatesUnchanged() {
    // MapJoinMemoryExhaustionError is an Error, so the drain lambda never catches it and it
    // reaches the future unwrapped. It must reach the caller as the very same instance: the
    // Tez task's error handling keys off the type.
    MapJoinMemoryExhaustionError error = new MapJoinMemoryExhaustionError("over budget");
    List<Future<?>> futures = Arrays.asList(CompletableFuture.completedFuture(null),
        CompletableFuture.failedFuture(error));
    try {
      VectorMapJoinFastHashTableLoader.rethrowDrainFailures(futures);
      fail("expected the MapJoinMemoryExhaustionError to propagate");
    } catch (MapJoinMemoryExhaustionError e) {
      assertSame(error, e);
    } catch (HiveException e) {
      fail("the Error was wrapped instead of rethrown: " + e);
    }
  }

  @Test
  public void testCheckedFailureSurfacesAsHiveException() {
    IOException cause = new IOException("partition 1 died");
    List<Future<?>> futures = Collections.singletonList(CompletableFuture.failedFuture(cause));
    try {
      VectorMapJoinFastHashTableLoader.rethrowDrainFailures(futures);
      fail("expected a HiveException");
    } catch (HiveException e) {
      assertSame(cause, e.getCause());
    }
  }

  /**
   * Partition order must not decide which failure the caller sees: re-execution reads the type,
   * so the Error wins over the checked failure that happened to come first.
   */
  @Test
  public void testAnErrorOutranksAnEarlierCheckedFailure() {
    IOException checked = new IOException("partition 0 died");
    MapJoinMemoryExhaustionError error = new MapJoinMemoryExhaustionError("partition 2 over budget");
    List<Future<?>> futures = Arrays.asList(CompletableFuture.failedFuture(checked),
        CompletableFuture.completedFuture(null), CompletableFuture.failedFuture(error));
    try {
      VectorMapJoinFastHashTableLoader.rethrowDrainFailures(futures);
      fail("expected the MapJoinMemoryExhaustionError to propagate");
    } catch (MapJoinMemoryExhaustionError e) {
      assertSame(error, e);
      assertSame(checked, e.getSuppressed()[0]);
    } catch (HiveException e) {
      fail("the checked failure won over the Error: " + e);
    }
  }
}
