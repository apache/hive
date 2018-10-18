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

package org.apache.hadoop.hive.llap.tezplugins;

import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapContainerLauncher extends ContainerLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(LlapContainerLauncher.class);

  public LlapContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    super(containerLauncherContext);
  }

  @Override
  public void launchContainer(ContainerLaunchRequest containerLaunchRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("No-op launch for container: " +
          containerLaunchRequest.getContainerId() +
          " succeeded on host: " + containerLaunchRequest.getNodeId());
    }
    getContext().containerLaunched(containerLaunchRequest.getContainerId());
  }

  @Override
  public void stopContainer(ContainerStopRequest containerStopRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("No-op stopContainer invocation for containerId={}",
          containerStopRequest.getContainerId());
    }
    // Nothing to do here.
  }
}