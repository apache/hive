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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.launcher.ContainerLauncher;
import org.apache.tez.dag.app.rm.NMCommunicatorEvent;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventLaunched;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.events.ContainerLaunchedEvent;

public class LlapContainerLauncher extends AbstractService implements ContainerLauncher {
  static final Log LOG = LogFactory.getLog(LlapContainerLauncher.class);

  private final AppContext context;
  private final Clock clock;

  public LlapContainerLauncher(AppContext appContext, Configuration conf,
                               TaskAttemptListener tal) {
    super(LlapContainerLauncher.class.getName());
    this.context = appContext;
    this.clock = appContext.getClock();
  }

  @Override
  public void handle(NMCommunicatorEvent event) {
    switch(event.getType()) {
      case CONTAINER_LAUNCH_REQUEST:
        final NMCommunicatorLaunchRequestEvent launchEvent = (NMCommunicatorLaunchRequestEvent) event;
        LOG.info("No-op launch for container: " + launchEvent.getContainerId() + " succeeded on host: " + launchEvent.getNodeId());
        context.getEventHandler().handle(new AMContainerEventLaunched(launchEvent.getContainerId()));
        ContainerLaunchedEvent lEvt = new ContainerLaunchedEvent(
            launchEvent.getContainerId(), clock.getTime(), context.getApplicationAttemptId());
        context.getHistoryHandler().handle(new DAGHistoryEvent(
            null, lEvt));
        break;
      case CONTAINER_STOP_REQUEST:
        LOG.info("DEBUG: Ignoring STOP_REQUEST for event: " + event);
        context.getEventHandler().handle(new AMContainerEvent(event.getContainerId(),
            AMContainerEventType.C_NM_STOP_SENT));
        break;
    }
  }
}
