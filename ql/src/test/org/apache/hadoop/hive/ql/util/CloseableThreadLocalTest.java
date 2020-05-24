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
package org.apache.hadoop.hive.ql.util;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class CloseableThreadLocalTest {

  private static class AutoCloseableStub implements AutoCloseable {

    private boolean closed = false;

    public boolean isClosed() {
      return closed;
    }

    @Override
    public void close() throws Exception {
      closed = true;
    }
  }

  @Test
  public void testResourcesAreInitiallyNotClosed() {
    CloseableThreadLocal<AutoCloseableStub> closeableThreadLocal =
            new CloseableThreadLocal<>(AutoCloseableStub::new, 1);

    assertThat(closeableThreadLocal.get().isClosed(), is(false));
  }

  @Test
  public void testAfterCallingCloseAllInstancesAreClosed() throws ExecutionException, InterruptedException {
    CloseableThreadLocal<AutoCloseableStub> closeableThreadLocal =
            new CloseableThreadLocal<>(AutoCloseableStub::new, 2);

    AutoCloseableStub asyncInstance = CompletableFuture.supplyAsync(closeableThreadLocal::get).get();
    AutoCloseableStub syncInstance = closeableThreadLocal.get();

    closeableThreadLocal.close();

    assertThat(asyncInstance.isClosed(), is(true));
    assertThat(syncInstance.isClosed(), is(true));
  }

  @Test
  public void testSubsequentGetsInTheSameThreadGivesBackTheSameObject() {
    CloseableThreadLocal<AutoCloseableStub> closeableThreadLocal =
            new CloseableThreadLocal<>(AutoCloseableStub::new, 2);

    AutoCloseableStub ref1 = closeableThreadLocal.get();
    AutoCloseableStub ref2 = closeableThreadLocal.get();
    assertThat(ref1, is(ref2));
  }

  @Test
  public void testDifferentThreadsHasDifferentInstancesOfTheResource() throws ExecutionException, InterruptedException {
    CloseableThreadLocal<AutoCloseableStub> closeableThreadLocal =
            new CloseableThreadLocal<>(AutoCloseableStub::new, 2);

    AutoCloseableStub asyncInstance = CompletableFuture.supplyAsync(closeableThreadLocal::get).get();
    AutoCloseableStub syncInstance = closeableThreadLocal.get();
    assertThat(asyncInstance, is(not(syncInstance)));
  }
}
