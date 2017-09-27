/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.Message;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.junit.Assert;
import org.junit.Test;

public class TestAsyncPbRpcProxy {

  @Test (timeout = 5000)
  public void testMultipleNodes() throws Exception {
    RequestManagerForTest requestManager = new RequestManagerForTest(1);

    LlapNodeId nodeId1 = LlapNodeId.getInstance("host1", 1025);
    LlapNodeId nodeId2 = LlapNodeId.getInstance("host2", 1025);

    Message mockMessage = mock(Message.class);
    LlapProtocolClientProxy.ExecuteRequestCallback mockExecuteRequestCallback = mock(
        LlapProtocolClientProxy.ExecuteRequestCallback.class);

    // Request two messages
    requestManager.queueRequest(
        new CallableRequestForTest(nodeId1, mockMessage, mockExecuteRequestCallback));
    requestManager.queueRequest(
        new CallableRequestForTest(nodeId2, mockMessage, mockExecuteRequestCallback));

    // Should go through in a single process call
    requestManager.process();
    assertEquals(2, requestManager.numSubmissionsCounters);
    assertNotNull(requestManager.numInvocationsPerNode.get(nodeId1));
    assertNotNull(requestManager.numInvocationsPerNode.get(nodeId2));
    Assert.assertEquals(1, requestManager.numInvocationsPerNode.get(nodeId1).getValue().intValue());
    Assert.assertEquals(1, requestManager.numInvocationsPerNode.get(nodeId2).getValue().intValue());
    assertEquals(0, requestManager.currentLoopSkippedRequests.size());
    assertEquals(0, requestManager.currentLoopSkippedRequests.size());
    assertEquals(0, requestManager.currentLoopDisabledNodes.size());
  }

  @Test(timeout = 5000)
  public void testSingleInvocationPerNode() throws Exception {
    RequestManagerForTest requestManager = new RequestManagerForTest(1);

    LlapNodeId nodeId1 = LlapNodeId.getInstance("host1", 1025);

    Message mockMessage = mock(Message.class);
    LlapProtocolClientProxy.ExecuteRequestCallback mockExecuteRequestCallback = mock(
        LlapProtocolClientProxy.ExecuteRequestCallback.class);

    // First request for host.
    requestManager.queueRequest(
        new CallableRequestForTest(nodeId1, mockMessage, mockExecuteRequestCallback));
    requestManager.process();
    assertEquals(1, requestManager.numSubmissionsCounters);
    assertNotNull(requestManager.numInvocationsPerNode.get(nodeId1));
    Assert.assertEquals(1, requestManager.numInvocationsPerNode.get(nodeId1).getValue().intValue());
    assertEquals(0, requestManager.currentLoopSkippedRequests.size());

    // Second request for host. Single invocation since the last has not completed.
    requestManager.queueRequest(
        new CallableRequestForTest(nodeId1, mockMessage, mockExecuteRequestCallback));
    requestManager.process();
    assertEquals(1, requestManager.numSubmissionsCounters);
    assertNotNull(requestManager.numInvocationsPerNode.get(nodeId1));
    Assert.assertEquals(1, requestManager.numInvocationsPerNode.get(nodeId1).getValue().intValue());
    assertEquals(1, requestManager.currentLoopSkippedRequests.size());
    assertEquals(1, requestManager.currentLoopDisabledNodes.size());
    assertTrue(requestManager.currentLoopDisabledNodes.contains(nodeId1));

    // Complete first request. Second pending request should go through.
    requestManager.requestFinished(nodeId1);
    requestManager.process();
    assertEquals(2, requestManager.numSubmissionsCounters);
    assertNotNull(requestManager.numInvocationsPerNode.get(nodeId1));
    Assert.assertEquals(2, requestManager.numInvocationsPerNode.get(nodeId1).getValue().intValue());
    assertEquals(0, requestManager.currentLoopSkippedRequests.size());
    assertEquals(0, requestManager.currentLoopDisabledNodes.size());
    assertFalse(requestManager.currentLoopDisabledNodes.contains(nodeId1));
  }


  static class RequestManagerForTest extends LlapProtocolClientProxy.RequestManager {

    int numSubmissionsCounters = 0;
    private Map<LlapNodeId, MutableInt> numInvocationsPerNode = new HashMap<>();

    public RequestManagerForTest(int numThreads) {
      super(numThreads, 1);
    }

    protected void submitToExecutor(LlapProtocolClientProxy.CallableRequest request, LlapNodeId nodeId) {
      numSubmissionsCounters++;
      MutableInt nodeCount = numInvocationsPerNode.get(nodeId);
      if (nodeCount == null) {
        nodeCount = new MutableInt(0);
        numInvocationsPerNode.put(nodeId, nodeCount);
      }
      nodeCount.increment();
    }

    void reset() {
      numSubmissionsCounters = 0;
      numInvocationsPerNode.clear();
    }

  }

  static class CallableRequestForTest extends LlapProtocolClientProxy.NodeCallableRequest<Message, Message> {

    protected CallableRequestForTest(LlapNodeId nodeId, Message message,
                                     LlapProtocolClientProxy.ExecuteRequestCallback<Message> callback) {
      super(nodeId, message, callback);
    }

    @Override
    public Message call() throws Exception {
      return null;
    }
  }

}
