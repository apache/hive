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
package org.apache.hive.service.cli.thrift;

import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Test that the oom hook is triggered when the task throws an OutOfMemoryError
 */
public class TestThreadPoolExecutorWithOomHook {

  @Test
  public void testThreadPoolExecutorWithOomHook() {
    String threadPoolName = "TestThreadPoolExecutorWithOomHook";
    OomRunner oomRunner = new OomRunner();
    ThreadPoolExecutorWithOomHook exec = new ThreadPoolExecutorWithOomHook(2, 10,
        60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new ThreadFactoryWithGarbageCleanup(threadPoolName), oomRunner);
    Runnable task = new Runnable() {
      @Override
      public void run() {
        throw new OutOfMemoryError();
      }
    };

    Throwable t = null;
    Future future = exec.submit(task);
    exec.shutdown();
    try {
      future.get();
    } catch (Throwable e) {
      t = e;
    }

    Assert.assertTrue(t instanceof ExecutionException);

    Assert.assertTrue("The root cause of throwable should be an OutOfMemoryError", exec.isOutOfMemoryError(t));

  }

  private class OomRunner implements Runnable {
    @Override
    public void run() {
      // no op
    }
  }

}
