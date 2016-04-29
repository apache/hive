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

  public ContainerFactory(ApplicationAttemptId appAttemptId, long appIdLong) {
    this.nextId = new AtomicLong(1);
    ApplicationId appId =
        ApplicationId.newInstance(appIdLong, appAttemptId.getApplicationId().getId());
    this.customAppAttemptId =
        ApplicationAttemptId.newInstance(appId, appAttemptId.getAttemptId());
  }

  public Container createContainer(Resource capability, Priority priority, String hostname,
      int port, String nodeHttpAddress) {
    ContainerId containerId =
        ContainerId.newContainerId(customAppAttemptId, nextId.getAndIncrement());
    NodeId nodeId = NodeId.newInstance(hostname, port);

    Container container =
        Container.newInstance(containerId, nodeId, nodeHttpAddress, capability, priority, null);

    return container;
  }
}
