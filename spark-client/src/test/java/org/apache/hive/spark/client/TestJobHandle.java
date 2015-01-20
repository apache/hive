/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import java.io.Serializable;

import io.netty.util.concurrent.Promise;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestJobHandle {

  @Mock private SparkClientImpl client;
  @Mock private Promise<Serializable> promise;
  @Mock private JobHandle.Listener<Serializable> listener;
  @Mock private JobHandle.Listener<Serializable> listener2;

  @Test
  public void testStateChanges() throws Exception {
    JobHandleImpl<Serializable> handle = new JobHandleImpl<Serializable>(client, promise, "job");
    handle.addListener(listener);

    assertTrue(handle.changeState(JobHandle.State.QUEUED));
    verify(listener).onJobQueued(handle);

    assertTrue(handle.changeState(JobHandle.State.STARTED));
    verify(listener).onJobStarted(handle);

    handle.addSparkJobId(1);
    verify(listener).onSparkJobStarted(same(handle), eq(1));

    assertTrue(handle.changeState(JobHandle.State.CANCELLED));
    verify(listener).onJobCancelled(handle);

    assertFalse(handle.changeState(JobHandle.State.STARTED));
    assertFalse(handle.changeState(JobHandle.State.FAILED));
    assertFalse(handle.changeState(JobHandle.State.SUCCEEDED));
  }

  @Test
  public void testFailedJob() throws Exception {
    JobHandleImpl<Serializable> handle = new JobHandleImpl<Serializable>(client, promise, "job");
    handle.addListener(listener);

    Throwable cause = new Exception();
    when(promise.cause()).thenReturn(cause);

    assertTrue(handle.changeState(JobHandle.State.FAILED));
    verify(promise).cause();
    verify(listener).onJobFailed(handle, cause);
  }

  @Test
  public void testSucceededJob() throws Exception {
    JobHandleImpl<Serializable> handle = new JobHandleImpl<Serializable>(client, promise, "job");
    handle.addListener(listener);

    Serializable result = new Exception();
    when(promise.get()).thenReturn(result);

    assertTrue(handle.changeState(JobHandle.State.SUCCEEDED));
    verify(promise).get();
    verify(listener).onJobSucceeded(handle, result);
  }

  @Test
  public void testImmediateCallback() throws Exception {
    JobHandleImpl<Serializable> handle = new JobHandleImpl<Serializable>(client, promise, "job");
    assertTrue(handle.changeState(JobHandle.State.QUEUED));
    handle.addListener(listener);
    verify(listener).onJobQueued(handle);

    handle.changeState(JobHandle.State.STARTED);
    handle.addSparkJobId(1);
    handle.changeState(JobHandle.State.CANCELLED);

    handle.addListener(listener2);
    InOrder inOrder = inOrder(listener2);
    inOrder.verify(listener2).onSparkJobStarted(same(handle), eq(1));
    inOrder.verify(listener2).onJobCancelled(same(handle));
  }

}
