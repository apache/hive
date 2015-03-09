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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.app.TezTaskCommunicatorImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class LlapTaskCommunicator extends TezTaskCommunicatorImpl {

  private static final Log LOG = LogFactory.getLog(LlapTaskCommunicator.class);

  private final SubmitWorkRequestProto BASE_SUBMIT_WORK_REQUEST;
  private final ConcurrentMap<String, ByteBuffer> credentialMap;

  private TaskCommunicator communicator;

  public LlapTaskCommunicator(
      TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);

    SubmitWorkRequestProto.Builder baseBuilder = SubmitWorkRequestProto.newBuilder();

    // TODO Avoid reading this from the environment
    baseBuilder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));
    baseBuilder.setApplicationIdString(
        taskCommunicatorContext.getApplicationAttemptId().getApplicationId().toString());
    baseBuilder
        .setAppAttemptNumber(taskCommunicatorContext.getApplicationAttemptId().getAttemptId());
    baseBuilder.setTokenIdentifier(getTokenIdentifier());

    BASE_SUBMIT_WORK_REQUEST = baseBuilder.build();

    credentialMap = new ConcurrentHashMap<String, ByteBuffer>();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    int numThreads = conf.getInt(LlapDaemonConfiguration.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS,
        LlapDaemonConfiguration.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS_DEFAULT);
    this.communicator = new TaskCommunicator(numThreads);
    this.communicator.init(conf);
  }

  @Override
  public void serviceStart() {
    super.serviceStart();
    this.communicator.start();
  }

  @Override
  public void serviceStop() {
    super.serviceStop();
    if (this.communicator != null) {
      this.communicator.stop();
    }
  }


  @Override
  public void registerRunningContainer(ContainerId containerId, String hostname, int port) {
    super.registerRunningContainer(containerId, hostname, port);
  }

  @Override
  public void registerContainerEnd(ContainerId containerId) {
    super.registerContainerEnd(containerId);
  }

  @Override
  public void registerRunningTaskAttempt(final ContainerId containerId, final TaskSpec taskSpec,
                                         Map<String, LocalResource> additionalResources,
                                         Credentials credentials,
                                         boolean credentialsChanged)  {
    super.registerRunningTaskAttempt(containerId, taskSpec, additionalResources, credentials,
        credentialsChanged);
    SubmitWorkRequestProto requestProto = null;
    try {
      requestProto = constructSubmitWorkRequest(containerId, taskSpec);
    } catch (IOException e) {
      throw new RuntimeException("Failed to construct request", e);
    }
    ContainerInfo containerInfo = getContainerInfo(containerId);
    String host;
    int port;
    if (containerInfo != null) {
      synchronized (containerInfo) {
        host = containerInfo.host;
        port = containerInfo.port;
      }
    } else {
      // TODO Handle this properly
      throw new RuntimeException("ContainerInfo not found for container: " + containerId +
          ", while trying to launch task: " + taskSpec.getTaskAttemptID());
    }
    communicator.submitWork(requestProto, host, port,
        new TaskCommunicator.ExecuteRequestCallback<SubmitWorkResponseProto>() {
          @Override
          public void setResponse(SubmitWorkResponseProto response) {
            LOG.info("Successfully launched task: " + taskSpec.getTaskAttemptID());
            getTaskCommunicatorContext()
                .taskStartedRemotely(taskSpec.getTaskAttemptID(), containerId);
          }

          @Override
          public void indicateError(Throwable t) {
            // TODO Handle this error. This is where an API on the context to indicate failure / rejection comes in.
            LOG.info("Failed to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " +
                containerId, t);
          }
        });
  }

  @Override
  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID) {
    super.unregisterRunningTaskAttempt(taskAttemptID);
    // Nothing else to do for now. The push API in the test does not support termination of a running task
  }

  private SubmitWorkRequestProto constructSubmitWorkRequest(ContainerId containerId,
                                                            TaskSpec taskSpec) throws
      IOException {
    SubmitWorkRequestProto.Builder builder =
        SubmitWorkRequestProto.newBuilder(BASE_SUBMIT_WORK_REQUEST);
    builder.setContainerIdString(containerId.toString());
    builder.setAmHost(getAddress().getHostName());
    builder.setAmPort(getAddress().getPort());
    Credentials taskCredentials = new Credentials();
    // Credentials can change across DAGs. Ideally construct only once per DAG.
    taskCredentials.addAll(getTaskCommunicatorContext().getCredentials());

    ByteBuffer credentialsBinary = credentialMap.get(taskSpec.getDAGName());
    if (credentialsBinary == null) {
      credentialsBinary = serializeCredentials(getTaskCommunicatorContext().getCredentials());
      credentialMap.putIfAbsent(taskSpec.getDAGName(), credentialsBinary.duplicate());
    } else {
      credentialsBinary = credentialsBinary.duplicate();
    }
    builder.setCredentialsBinary(ByteString.copyFrom(credentialsBinary));
    builder.setFragmentSpec(Converters.convertTaskSpecToProto(taskSpec));
    return builder.build();
  }

  private ByteBuffer serializeCredentials(Credentials credentials) throws IOException {
    Credentials containerCredentials = new Credentials();
    containerCredentials.addAll(credentials);
    DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
    containerCredentials.writeTokenStorageToStream(containerTokens_dob);
    ByteBuffer containerCredentialsBuffer = ByteBuffer.wrap(containerTokens_dob.getData(), 0,
        containerTokens_dob.getLength());
    return containerCredentialsBuffer;
  }
}
