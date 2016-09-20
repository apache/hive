/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.DagInfo;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestLlapTaskCommunicator {

  @Test (timeout = 5000)
  public void testEntityTracker1() {
    LlapTaskCommunicator.EntityTracker entityTracker = new LlapTaskCommunicator.EntityTracker();

    String host1 = "host1";
    int port = 1451;


    // Simple container registration and un-registration without any task attempt being involved.
    ContainerId containerId101 = constructContainerId(101);
    entityTracker.registerContainer(containerId101, host1, port);
    assertEquals(LlapNodeId.getInstance(host1, port), entityTracker.getNodeIdForContainer(containerId101));

    entityTracker.unregisterContainer(containerId101);
    assertNull(entityTracker.getContainerAttemptMapForNode(LlapNodeId.getInstance(host1, port)));
    assertNull(entityTracker.getNodeIdForContainer(containerId101));
    assertEquals(0, entityTracker.nodeMap.size());
    assertEquals(0, entityTracker.attemptToNodeMap.size());
    assertEquals(0, entityTracker.containerToNodeMap.size());


    // Simple task registration and un-registration.
    ContainerId containerId1 = constructContainerId(1);
    TezTaskAttemptID taskAttemptId1 = constructTaskAttemptId(1);
    entityTracker.registerTaskAttempt(containerId1, taskAttemptId1, host1, port);
    assertEquals(LlapNodeId.getInstance(host1, port), entityTracker.getNodeIdForContainer(containerId1));
    assertEquals(LlapNodeId.getInstance(host1, port), entityTracker.getNodeIdForTaskAttempt(taskAttemptId1));

    entityTracker.unregisterTaskAttempt(taskAttemptId1);
    assertNull(entityTracker.getContainerAttemptMapForNode(LlapNodeId.getInstance(host1, port)));
    assertNull(entityTracker.getNodeIdForContainer(containerId1));
    assertNull(entityTracker.getNodeIdForTaskAttempt(taskAttemptId1));
    assertEquals(0, entityTracker.nodeMap.size());
    assertEquals(0, entityTracker.attemptToNodeMap.size());
    assertEquals(0, entityTracker.containerToNodeMap.size());

    // Register taskAttempt, unregister container. TaskAttempt should also be unregistered
    ContainerId containerId201 = constructContainerId(201);
    TezTaskAttemptID taskAttemptId201 = constructTaskAttemptId(201);
    entityTracker.registerTaskAttempt(containerId201, taskAttemptId201, host1, port);
    assertEquals(LlapNodeId.getInstance(host1, port), entityTracker.getNodeIdForContainer(containerId201));
    assertEquals(LlapNodeId.getInstance(host1, port), entityTracker.getNodeIdForTaskAttempt(taskAttemptId201));

    entityTracker.unregisterContainer(containerId201);
    assertNull(entityTracker.getContainerAttemptMapForNode(LlapNodeId.getInstance(host1, port)));
    assertNull(entityTracker.getNodeIdForContainer(containerId201));
    assertNull(entityTracker.getNodeIdForTaskAttempt(taskAttemptId201));
    assertEquals(0, entityTracker.nodeMap.size());
    assertEquals(0, entityTracker.attemptToNodeMap.size());
    assertEquals(0, entityTracker.containerToNodeMap.size());

    entityTracker.unregisterTaskAttempt(taskAttemptId201); // No errors
  }


  @Test(timeout = 5000)
  public void testFinishableStateUpdateFailure() throws Exception {

    LlapTaskCommunicatorWrapperForTest wrapper = null;

    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();
    final AtomicBoolean opDone = new AtomicBoolean(false);

    LlapProtocolClientProxy proxy = mock(LlapProtocolClientProxy.class,
        new FinishableStatusUpdateTestAnswer(lock, condition, opDone));

    try {
      wrapper = new LlapTaskCommunicatorWrapperForTest(proxy);

      // Register tasks on 2 nodes, with a dependency on vertex1 completing.
      ContainerId cId11 = wrapper.registerContainer(1, 0);
      TaskSpec ts11 = wrapper.registerRunningTaskAttemptWithSourceVertex(cId11, 1);

      ContainerId cId12 = wrapper.registerContainer(2, 0);
      TaskSpec ts12 = wrapper.registerRunningTaskAttemptWithSourceVertex(cId12, 2);

      ContainerId cId21 = wrapper.registerContainer(3, 1);
      TaskSpec ts21 = wrapper.registerRunningTaskAttemptWithSourceVertex(cId21, 3);

      // Send a state update for vertex1 completion. This triggers a status update to be sent out.
      VertexStateUpdate vertexStateUpdate =
          new VertexStateUpdate(LlapTaskCommunicatorWrapperForTest.VERTEX_NAME1,
              VertexState.SUCCEEDED);
      wrapper.getTaskCommunicator().onVertexStateUpdated(vertexStateUpdate);

      // Wait for all invocations to complete.
      lock.lock();
      try {
        while (!opDone.get()) {
          condition.await();
        }
      } finally {
        lock.unlock();
      }
      // Verify that a task kill went out for all nodes running on the specified host.

      verify(wrapper.getTaskCommunicatorContext(), times(2))
          .taskKilled(any(TezTaskAttemptID.class), any(TaskAttemptEndReason.class),
              any(String.class));

      verify(wrapper.getTaskCommunicatorContext()).taskKilled(eq(ts11.getTaskAttemptID()),
          eq(TaskAttemptEndReason.NODE_FAILED), any(String.class));
      verify(wrapper.getTaskCommunicatorContext()).taskKilled(eq(ts12.getTaskAttemptID()),
          eq(TaskAttemptEndReason.NODE_FAILED), any(String.class));

      wrapper.getTaskCommunicator().sendStateUpdate(LlapNodeId
              .getInstance(LlapTaskCommunicatorWrapperForTest.HOSTS[1],
                  LlapTaskCommunicatorWrapperForTest.RPC_PORT),
          LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto.getDefaultInstance());

      // Verify no more invocations in case of success.
      verify(wrapper.getTaskCommunicatorContext(), times(2))
          .taskKilled(any(TezTaskAttemptID.class), any(TaskAttemptEndReason.class),
              any(String.class));

    } finally {
      if (wrapper != null) {
        wrapper.shutdown();
      }
    }
  }

  static class FinishableStatusUpdateTestAnswer implements Answer<Void> {

    final Lock lock;
    final Condition condition;
    final AtomicBoolean opDone;

    final AtomicBoolean successInvoked = new AtomicBoolean(false);
    final AtomicBoolean failInvoked = new AtomicBoolean(false);


    FinishableStatusUpdateTestAnswer(Lock lock, Condition condition, AtomicBoolean opDone) {
      this.lock = lock;
      this.condition = condition;
      this.opDone = opDone;
    }

    void reset() {
      opDone.set(false);
      successInvoked.set(false);
      failInvoked.set(false);
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      if (invocation.getMethod().getName().equals("sendSourceStateUpdate")) {
        LlapNodeId nodeId = (LlapNodeId) invocation.getArguments()[1];
        final LlapProtocolClientProxy.ExecuteRequestCallback callback =
            (LlapProtocolClientProxy.ExecuteRequestCallback) invocation.getArguments()[2];

        if (nodeId.getHostname().equals(LlapTaskCommunicatorWrapperForTest.HOSTS[0])) {
          new Thread() {
            public void run() {
              callback.indicateError(
                  new IOException("Force failing " + LlapTaskCommunicatorWrapperForTest.HOSTS[0]));
              successInvoked.set(true);
              signalOpDoneIfBothInvoked();
            }
          }.start();
        } else {
          new Thread() {
            public void run() {
              // Report success for all other cases.
              callback.setResponse(
                  LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto.getDefaultInstance());
              failInvoked.set(true);
              signalOpDoneIfBothInvoked();
            }
          }.start();
        }
      }
      return null;
    }

    private void signalOpDoneIfBothInvoked() {
      lock.lock();
      try {
        if (failInvoked.get() && successInvoked.get()) {
          opDone.set(true);
          condition.signal();
        }
      } finally {
        lock.unlock();
      }
    }
  }


  /**
   * Wrapper class which is responsible for setting up various mocks required for different tests.
   */
  private static class LlapTaskCommunicatorWrapperForTest {

    static final String[] HOSTS = new String[]{"host1", "host2", "host3"};
    static final int RPC_PORT = 15002;
    static final String DAG_NAME = "dagName";
    static final String VERTEX_NAME1 = "vertexName1";
    static final String VERTEX_NAME2 = "vertexName2";

    final TaskCommunicatorContext taskCommunicatorContext = mock(TaskCommunicatorContext.class);

    final ApplicationId appId = ApplicationId.newInstance(1000, 1);
    final ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appId, 100);
    final TezDAGID dagid = TezDAGID.getInstance(appId, 200);
    final TezVertexID vertexId1 = TezVertexID.getInstance(dagid, 300);
    final TezVertexID vertexId2 = TezVertexID.getInstance(dagid, 301);
    final Configuration conf = new Configuration(false);
    final UserPayload userPayload;

    final LlapTaskCommunicatorForTest taskCommunicator;

    public LlapTaskCommunicatorWrapperForTest(LlapProtocolClientProxy llapProxy) throws Exception {

      HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS, "fake-non-zk-cluster");
      userPayload = TezUtils.createUserPayloadFromConf(conf);

      doReturn(appAttemptId).when(taskCommunicatorContext).getApplicationAttemptId();
      doReturn(new Credentials()).when(taskCommunicatorContext).getAMCredentials();
      doReturn(userPayload).when(taskCommunicatorContext).getInitialUserPayload();
      doReturn(appId.toString()).when(taskCommunicatorContext).getCurrentAppIdentifier();
      DagInfo dagInfo = mock(DagInfo.class);
      doReturn(dagInfo).when(taskCommunicatorContext).getCurrentDagInfo();
      doReturn(DAG_NAME).when(dagInfo).getName();
      doReturn(new Credentials()).when(dagInfo).getCredentials();
      doReturn(new LinkedList<String>()).when(taskCommunicatorContext)
          .getInputVertexNames(any(String.class));


      this.taskCommunicator = new LlapTaskCommunicatorForTest(taskCommunicatorContext, llapProxy);
      this.taskCommunicator.initialize();
      this.taskCommunicator.start();
    }

    void shutdown() {
      this.taskCommunicator.shutdown();
    }

    TaskCommunicatorContext getTaskCommunicatorContext() {
      return taskCommunicatorContext;
    }

    LlapTaskCommunicatorForTest getTaskCommunicator() {
      return taskCommunicator;
    }

    ContainerId registerContainer(int containerIdx, int hostIdx) {
      ContainerId containerId = ContainerId.newInstance(appAttemptId, containerIdx);
      taskCommunicator.registerRunningContainer(containerId, HOSTS[hostIdx], RPC_PORT);
      return containerId;
    }

    /*
    Sets up a TaskSpec which has vertex1 as it's input, and tasks belonging to vertex2
     */
    TaskSpec registerRunningTaskAttemptWithSourceVertex(ContainerId containerId, int taskIdx) {
      TaskSpec taskSpec = createBaseTaskSpec(VERTEX_NAME2, vertexId2, taskIdx);

      InputSpec inputSpec =
          new InputSpec(VERTEX_NAME1, InputDescriptor.create("fakeInputClassName"), 3);
      List<InputSpec> inputs = Lists.newArrayList(inputSpec);

      doReturn(inputs).when(taskSpec).getInputs();

      taskCommunicator
          .registerRunningTaskAttempt(containerId, taskSpec, new HashMap<String, LocalResource>(),
              new Credentials(), false, 2);
      return taskSpec;
    }

    /*
    Sets up a TaskSpec with no inputs, and tasks belonging to vertex1
     */
    TaskSpec registerRunningTaskAttempt(ContainerId containerId, int taskIdx) {

      TaskSpec taskSpec = createBaseTaskSpec(VERTEX_NAME1, vertexId1, taskIdx);

      taskCommunicator
          .registerRunningTaskAttempt(containerId, taskSpec, new HashMap<String, LocalResource>(),
              new Credentials(), false, 2);
      return taskSpec;
    }

    private TaskSpec createBaseTaskSpec(String vertexName, TezVertexID vertexId, int taskIdx) {
      TaskSpec taskSpec = mock(TaskSpec.class);
      Configuration conf = new Configuration(false);
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYID, "fakeQueryId");
      UserPayload userPayload;
      try {
        userPayload = TezUtils.createUserPayloadFromConf(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(
          TezTaskID.getInstance(vertexId, taskIdx), 0);
      doReturn(taskAttemptId).when(taskSpec).getTaskAttemptID();
      doReturn(DAG_NAME).when(taskSpec).getDAGName();
      doReturn(vertexName).when(taskSpec).getVertexName();
      ProcessorDescriptor processorDescriptor = ProcessorDescriptor.create("fakeClassName").setUserPayload(userPayload);
      doReturn(processorDescriptor).when(taskSpec).getProcessorDescriptor();
      return taskSpec;
    }
  }



  private ContainerId constructContainerId(int id) {
    ContainerId containerId = mock(ContainerId.class);
    doReturn(id).when(containerId).getId();
    doReturn((long)id).when(containerId).getContainerId();
    return containerId;
  }

  private TezTaskAttemptID constructTaskAttemptId(int id) {
    TezTaskAttemptID taskAttemptId = mock(TezTaskAttemptID.class);
    doReturn(id).when(taskAttemptId).getId();
    return taskAttemptId;
  }


  private static class LlapTaskCommunicatorForTest extends LlapTaskCommunicator {

    private final LlapProtocolClientProxy llapProxy;

    public LlapTaskCommunicatorForTest(
        TaskCommunicatorContext taskCommunicatorContext) {
      this(taskCommunicatorContext, mock(LlapProtocolClientProxy.class));
    }

    public LlapTaskCommunicatorForTest(
        TaskCommunicatorContext taskCommunicatorContext, LlapProtocolClientProxy llapProxy) {
      super(taskCommunicatorContext);
      this.llapProxy = llapProxy;
    }

    @Override
    protected void startRpcServer() {
    }

    @Override
    protected LlapProtocolClientProxy createLlapProtocolClientProxy(int numThreads,
                                                                    Configuration conf) {
      return llapProxy;
    }

    @Override
    public InetSocketAddress getAddress() {
      return InetSocketAddress.createUnresolved("localhost", 15001);
    }

    @Override
    public String getAmHostString() {
      return "localhost";
    }
  }

}
