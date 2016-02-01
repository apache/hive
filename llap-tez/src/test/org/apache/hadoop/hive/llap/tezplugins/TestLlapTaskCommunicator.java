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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.junit.Test;

public class TestLlapTaskCommunicator {

  @Test (timeout = 5000)
  public void testEntityTracker1() {
    LlapTaskCommunicator.EntityTracker entityTracker = new LlapTaskCommunicator.EntityTracker();

    String host1 = "host1";
    String host2 = "host2";
    String host3 = "host3";
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

}
