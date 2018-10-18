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

package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hive.llap.AsyncPbRpcProxy.ExecuteRequestCallback;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryResponseProto;

import org.apache.hadoop.conf.Configuration;

import org.junit.Test;


public class TestGuaranteedTaskAllocator {

  static class MockCommunicator implements LlapPluginEndpointClient {
    HashMap<Integer, Integer> messages = new HashMap<>();

    @Override
    public void sendUpdateQuery(UpdateQueryRequestProto request,
        AmPluginNode node,
        UpdateRequestContext callback) {
      WmTezSession session = (WmTezSession)node;
      messages.put(Integer.parseInt(session.getSessionId()), request.getGuaranteedTaskCount());
      callback.setResponse(UpdateQueryResponseProto.getDefaultInstance());
    }
  }

  static class GuaranteedTasksAllocatorForTest extends GuaranteedTasksAllocator {
    int executorCount = 0;

    public GuaranteedTasksAllocatorForTest(LlapPluginEndpointClient amCommunicator) {
      super(new Configuration(), amCommunicator);
    }

    // Override external stuff. These could also be injected as extra classes.

    @Override
    protected int getExecutorCount(boolean allowUpdate) {
      return executorCount;
    }
  }

  @Test
  public void testEqualAllocations() {
    testEqualAllocation(8, 5, 1.0f);
    testEqualAllocation(0, 3, 1.0f);
    testEqualAllocation(3, 1, 1.0f);
    testEqualAllocation(5, 5, 1.0f);
    testEqualAllocation(7, 10, 1.0f);
    testEqualAllocation(98, 10, 1.0f);
    testEqualAllocation(40, 5, 0.5f);
    testEqualAllocation(40, 5, 0.25f);
    testEqualAllocation(40, 5, 0.1f);
    testEqualAllocation(40, 5, 0.01f);
  }

  @Test
  public void testAllocations() {
    testAllocation(8, 1.0f,
        new double[] { 0.5f, 0.25f, 0.25f }, new int[] { 4, 2, 2 });
    testAllocation(10, 1.0f,
        new double[] { 0.33f, 0.4f, 0.27f }, new int[] { 3, 4, 3 });
    // Test incorrect totals. We don't normalize; just make sure we don't under- or overshoot.
    testAllocation(10, 1.0f,
        new double[] { 0.5f, 0.5f, 0.5f }, new int[] { 5, 5, 0 });
    testAllocation(100, 0.5f,
        new double[] { 0.15f, 0.15f, 0.15f }, new int[] { 15, 15, 20 });
  }

  private void testAllocation(int ducks, double total, double[] in, int[] out) {
    MockCommunicator comm = new MockCommunicator();
    GuaranteedTasksAllocatorForTest qam = new GuaranteedTasksAllocatorForTest(comm);
    List<WmTezSession> sessionsToUpdate = new ArrayList<>();
    comm.messages.clear();
    for (int i = 0; i < in.length; ++i) {
      addSession(in[i], sessionsToUpdate, i);
    }
    qam.executorCount = ducks;
    qam.updateSessionsAsync(total, sessionsToUpdate);
    Integer[] results = getAllocationResults(comm, in.length);
    for (int i = 0; i < results.length; ++i) {
      assertNotNull(results[i]);
      assertEquals(out[i], results[i].intValue());
    }
  }

  private void testEqualAllocation(int ducks, int sessions, double total) {
    MockCommunicator comm = new MockCommunicator();
    GuaranteedTasksAllocatorForTest qam = new GuaranteedTasksAllocatorForTest(comm);
    List<WmTezSession> sessionsToUpdate = new ArrayList<>();
    comm.messages.clear();
    double fraction = total / sessions;
    for (int i = 0; i < sessions; ++i) {
      addSession(fraction, sessionsToUpdate, i);
    }
    qam.executorCount = ducks;
    qam.updateSessionsAsync(total, sessionsToUpdate);
    Integer[] results = getAllocationResults(comm, sessions);
    int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, totalAssigned = 0;
    for (int i = 0; i < results.length; ++i) {
      assertNotNull(results[i]);
      int val = results[i];
      min = Math.min(val, min);
      max = Math.max(val, max);
      totalAssigned += val;
    }
    assertTrue((max - min) <= 1);
    assertTrue(Math.abs(total * ducks - totalAssigned) <= 0.5f);
  }

  private Integer[] getAllocationResults(MockCommunicator comm, int sessions) {
    assertEquals(sessions, comm.messages.size());
    Integer[] results = new Integer[sessions];
    for (Entry<Integer, Integer> e : comm.messages.entrySet()) {
      assertNull(results[e.getKey()]);
      results[e.getKey()] = e.getValue();
    }
    return results;
  }

  private void addSession(double alloc, List<WmTezSession> sessionsToUpdate, int i) {
    SampleTezSessionState session = new SampleTezSessionState("" + i, null, null);
    session.setClusterFraction(alloc);
    sessionsToUpdate.add(session);
  }

}
