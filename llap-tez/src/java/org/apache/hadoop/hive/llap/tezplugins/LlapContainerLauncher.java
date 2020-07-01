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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tez.common.DagContainerLauncher;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezReflectionException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.app.launcher.DeletionTracker;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LlapContainerLauncher extends DagContainerLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(LlapContainerLauncher.class);
  private Configuration conf;
  private DeletionTracker deletionTracker;

  public LlapContainerLauncher(ContainerLauncherContext containerLauncherContext) {
    super(containerLauncherContext);
    try {
      conf = TezUtils.createConfFromUserPayload(containerLauncherContext.getInitialUserPayload());
    } catch (IOException e) {
      throw new TezUncheckedException(
          "Failed to parse user payload for " + LlapContainerLauncher.class.getSimpleName(), e);
    }

    try {
      //todo: Do we need to put this behind the flag?
      String deletionTrackerClassName = conf.get(TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS,
          TezConfiguration.TEZ_AM_DELETION_TRACKER_CLASS_DEFAULT);
      deletionTracker = ReflectionUtils.createClazzInstance(
          deletionTrackerClassName, new Class[]{Configuration.class}, new Object[]{conf});
    } catch (TezReflectionException e) {
      LOG.warn("Unable to initialize dag deletionTracker class. Dag deletion on completion won't work", e);
    }
  }
  @Override
  public void launchContainer(ContainerLaunchRequest containerLaunchRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("No-op launch for container: " +
          containerLaunchRequest.getContainerId() +
          " succeeded on host: " + containerLaunchRequest.getNodeId());
    }
    getContext().containerLaunched(containerLaunchRequest.getContainerId());

    if (deletionTracker != null) {
      deletionTracker.addNodeShufflePort(containerLaunchRequest.getNodeId(),
          HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_YARN_SHUFFLE_PORT));
    }
  }

  @Override
  public void stopContainer(ContainerStopRequest containerStopRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("No-op stopContainer invocation for containerId={}",
          containerStopRequest.getContainerId());
    }
    // Nothing to do here.
  }

  @Override
  public void dagComplete(TezDAGID dag, JobTokenSecretManager jobTokenSecretManager) {
    if (deletionTracker != null) {
      deletionTracker.dagComplete(dag, jobTokenSecretManager);
    }
  }
}