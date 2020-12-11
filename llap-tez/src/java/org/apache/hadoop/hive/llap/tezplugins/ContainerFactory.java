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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

class ContainerFactory {
  final ApplicationAttemptId customAppAttemptId;
  AtomicLong nextId;

  private static final int GUARANTEED_WIDTH = 1;

  private static final long GUARANTEED_BIT_MASK = (1L << GUARANTEED_WIDTH) - 1;

  /**
   * This is a hack to pass initial guaranteed information from {@link LlapTaskSchedulerService}
   * to {@link LlapTaskCommunicator}. Otherwise, we ended up synchronizing communication and
   * scheduling. This workaround can be removed after TEZ-4192 and HIVE-23589 are merged.
   *
   * Note: This method is reliable only for initial allocation of the task. Guaranteed status
   * may change via separate requests later. Therefore, do not rely on this method other
   * than creating initial submit work request.
   *
   * Even containerId -> guaranteed
   * Odd containerId -> not guaranteed
   *
   * @param containerId
   * @return {@code true} if the task associated with container was guaranteed initially.
   */
  public static boolean isContainerInitializedAsGuaranteed(ContainerId containerId) {
    return (containerId.getContainerId() & GUARANTEED_BIT_MASK) == 0;
  }

  public ContainerFactory(ApplicationAttemptId appAttemptId, long appIdLong) {
    this.nextId = new AtomicLong(1);
    ApplicationId appId =
        ApplicationId.newInstance(appIdLong, appAttemptId.getApplicationId().getId());
    this.customAppAttemptId =
        ApplicationAttemptId.newInstance(appId, appAttemptId.getAttemptId());
  }

  public Container createContainer(Resource capability, Priority priority, String hostname,
      int port, String nodeHttpAddress, boolean isGuaranteed) {
    ContainerId containerId =
        ContainerId.newContainerId(customAppAttemptId, nextContainerId(isGuaranteed));
    NodeId nodeId = NodeId.newInstance(hostname, port);

    Container container =
        Container.newInstance(containerId, nodeId, nodeHttpAddress, capability, priority, null);

    return container;
  }

  /**
   * See {@link #isContainerInitializedAsGuaranteed(ContainerId)}
   * @param isInitialGuaranteed
   * @return
   */
  private long nextContainerId(boolean isInitialGuaranteed) {
    long candidate = nextId.getAndIncrement();
    candidate <<= GUARANTEED_WIDTH;
    if (!isInitialGuaranteed) {
      candidate |= 1;
    }
    return candidate;
  }
}
