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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.tezplugins.helpers.SourceStateTracker;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.TezTaskCommunicatorImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;

public class LlapTaskCommunicator extends TezTaskCommunicatorImpl {

  private static final Log LOG = LogFactory.getLog(LlapTaskCommunicator.class);

  private final SubmitWorkRequestProto BASE_SUBMIT_WORK_REQUEST;
  private final ConcurrentMap<String, ByteBuffer> credentialMap;

  // Tracks containerIds and taskAttemptIds, so can be kept independent of the running DAG.
  // When DAG specific cleanup happens, it'll be better to link this to a DAG though.
  private final EntityTracker entityTracker = new EntityTracker();
  private final SourceStateTracker sourceStateTracker;

  private TaskCommunicator communicator;
  private final LlapTaskUmbilicalProtocol umbilical;

  private volatile String currentDagName;

  public LlapTaskCommunicator(
      TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);

    umbilical = new LlapTaskUmbilicalProtocolImpl(getUmbilical());
    SubmitWorkRequestProto.Builder baseBuilder = SubmitWorkRequestProto.newBuilder();

    // TODO Avoid reading this from the environment
    baseBuilder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));
    baseBuilder.setApplicationIdString(
        taskCommunicatorContext.getApplicationAttemptId().getApplicationId().toString());
    baseBuilder
        .setAppAttemptNumber(taskCommunicatorContext.getApplicationAttemptId().getAttemptId());
    baseBuilder.setTokenIdentifier(getTokenIdentifier());

    BASE_SUBMIT_WORK_REQUEST = baseBuilder.build();

    credentialMap = new ConcurrentHashMap<>();
    sourceStateTracker = new SourceStateTracker(getTaskCommunicatorContext(), this);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    int numThreads = conf.getInt(LlapConfiguration.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS,
        LlapConfiguration.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS_DEFAULT);
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
  protected void startRpcServer() {
    Configuration conf = getConfig();
    try {
      JobTokenSecretManager jobTokenSecretManager =
          new JobTokenSecretManager();
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, sessionToken);

      server = new RPC.Builder(conf)
          .setProtocol(LlapTaskUmbilicalProtocol.class)
          .setBindAddress("0.0.0.0")
          .setPort(0)
          .setInstance(umbilical)
          .setNumHandlers(
              conf.getInt(TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
                  TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT))
          .setSecretManager(jobTokenSecretManager).build();

      // Do serviceACLs need to be refreshed, like in Tez ?

      server.start();
      this.address = NetUtils.getConnectAddress(server);
      LOG.info("Started LlapUmbilical: " + umbilical.getClass().getName() + " at address: " + address);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, String hostname, int port) {
    super.registerRunningContainer(containerId, hostname, port);
    entityTracker.registerContainer(containerId, hostname, port);

  }

  @Override
  public void registerContainerEnd(ContainerId containerId) {
    super.registerContainerEnd(containerId);
    entityTracker.unregisterContainer(containerId);
  }


  @Override
  public void registerRunningTaskAttempt(final ContainerId containerId, final TaskSpec taskSpec,
                                         Map<String, LocalResource> additionalResources,
                                         Credentials credentials,
                                         boolean credentialsChanged,
                                         int priority)  {
    super.registerRunningTaskAttempt(containerId, taskSpec, additionalResources, credentials,
        credentialsChanged, priority);
    if (taskSpec.getDAGName() != currentDagName) {
      resetCurrentDag(taskSpec.getDAGName());
    }

    SubmitWorkRequestProto requestProto;
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

    entityTracker.registerTaskAttempt(containerId, taskSpec.getTaskAttemptID(), host, port);

    sourceStateTracker.addTask(host, port, taskSpec.getInputs());

    // Have to register this up front right now. Otherwise, it's possible for the task to start
    // sending out status/DONE/KILLED/FAILED messages before TAImpl knows how to handle them.
    getTaskCommunicatorContext()
        .taskStartedRemotely(taskSpec.getTaskAttemptID(), containerId);
    communicator.submitWork(requestProto, host, port,
        new TaskCommunicator.ExecuteRequestCallback<SubmitWorkResponseProto>() {
          @Override
          public void setResponse(SubmitWorkResponseProto response) {
            LOG.info("Successfully launched task: " + taskSpec.getTaskAttemptID());
          }

          @Override
          public void indicateError(Throwable t) {
            LOG.info("Failed to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " + containerId, t);
            if (t instanceof ServiceException) {
              ServiceException se = (ServiceException) t;
              t = se.getCause();
            }
            if (t instanceof RemoteException) {
              RemoteException re = (RemoteException)t;
              String message = re.toString();
              // RejectedExecutions from the remote service treated as KILLED
              if (message.contains(RejectedExecutionException.class.getName())) {
                getTaskCommunicatorContext().taskKilled(taskSpec.getTaskAttemptID(),
                    TaskAttemptEndReason.SERVICE_BUSY, "Service Busy");
              } else {
                // All others from the remote service cause the task to FAIL.
                getTaskCommunicatorContext()
                    .taskFailed(taskSpec.getTaskAttemptID(), TaskAttemptEndReason.OTHER,
                        t.toString());
              }
            } else {
              // Exception from the RPC layer - communication failure, consider as KILLED / service down.
              if (t instanceof IOException) {
                getTaskCommunicatorContext().taskKilled(taskSpec.getTaskAttemptID(),
                    TaskAttemptEndReason.COMMUNICATION_ERROR, "Communication Error");
              } else {
                // Anything else is a FAIL.
                getTaskCommunicatorContext()
                    .taskFailed(taskSpec.getTaskAttemptID(), TaskAttemptEndReason.OTHER,
                        t.getMessage());
              }
            }
          }
        });
  }

  @Override
  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID) {
    super.unregisterRunningTaskAttempt(taskAttemptID);
    entityTracker.unregisterTaskAttempt(taskAttemptID);
    // TODO Inform the daemon that this task is no longer running.
    // Currently, a task will end up moving into the RUNNING queue and will
    // be told that it needs to die since it isn't recognized.
  }

  @Override
  public void onVertexStateUpdated(VertexStateUpdate vertexStateUpdate) {
    // Delegate updates over to the source state tracker.
    sourceStateTracker
        .sourceStateUpdated(vertexStateUpdate.getVertexName(), vertexStateUpdate.getVertexState());
  }

  public void sendStateUpdate(final String host, final int port,
                              final SourceStateUpdatedRequestProto request) {
    communicator.sendSourceStateUpdate(request, host, port,
        new TaskCommunicator.ExecuteRequestCallback<SourceStateUpdatedResponseProto>() {
          @Override
          public void setResponse(SourceStateUpdatedResponseProto response) {
          }

          @Override
          public void indicateError(Throwable t) {
            // TODO HIVE-10280.
            // Ideally, this should be retried for a while, after which the node should be marked as failed.
            // Considering tasks are supposed to run fast. Failing the task immediately may be a good option.
            LOG.error(
                "Failed to send state update to node: " + host + ":" + port + ", StateUpdate=" +
                    request, t);
          }
        });
  }



  private void resetCurrentDag(String newDagName) {
    // Working on the assumption that a single DAG runs at a time per AM.
    currentDagName = newDagName;
    sourceStateTracker.resetState(newDagName);
    LOG.info("CurrentDag set to: " + newDagName);
    // TODO Additional state reset. Potentially sending messages to node to reset.
    // Is it possible for heartbeats to come in from lost tasks - those should be told to die, which
    // is likely already happening.
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
    return ByteBuffer.wrap(containerTokens_dob.getData(), 0, containerTokens_dob.getLength());
  }

  protected class LlapTaskUmbilicalProtocolImpl implements LlapTaskUmbilicalProtocol {

    private final TezTaskUmbilicalProtocol tezUmbilical;

    public LlapTaskUmbilicalProtocolImpl(TezTaskUmbilicalProtocol tezUmbilical) {
      this.tezUmbilical = tezUmbilical;
    }

    @Override
    public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
      return tezUmbilical.canCommit(taskid);
    }

    @Override
    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) throws IOException,
        TezException {
      return tezUmbilical.heartbeat(request);
    }

    @Override
    public void nodeHeartbeat(Text hostname, int port) {
      entityTracker.nodePinged(hostname.toString(), port);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received heartbeat from [" + hostname + ":" + port +"]");
      }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, protocol,
          clientVersion, clientMethodsHash);
    }
  }

  private final class EntityTracker {
    private final ConcurrentMap<TezTaskAttemptID, LlapNodeId> attemptToNodeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<ContainerId, LlapNodeId> containerToNodeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<LlapNodeId, BiMap<ContainerId, TezTaskAttemptID>> nodeMap = new ConcurrentHashMap<>();

    void registerTaskAttempt(ContainerId containerId, TezTaskAttemptID taskAttemptId, String host, int port) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering " + containerId + ", " + taskAttemptId + " for node: " + host + ":" + port);
      }
      LlapNodeId llapNodeId = LlapNodeId.getInstance(host, port);
      attemptToNodeMap.putIfAbsent(taskAttemptId, llapNodeId);
      BiMap<ContainerId, TezTaskAttemptID> tmpMap = HashBiMap.create();
      BiMap<ContainerId, TezTaskAttemptID> old = nodeMap.putIfAbsent(llapNodeId, tmpMap);
      BiMap<ContainerId, TezTaskAttemptID> usedInstance;
      usedInstance = old == null ? tmpMap : old;
      synchronized(usedInstance) {
        usedInstance.put(containerId, taskAttemptId);
      }
      // Make sure to put the instance back again, in case it was removed as part of a
      // containerEnd/taskEnd invocation.
      nodeMap.putIfAbsent(llapNodeId, usedInstance);
    }

    void unregisterTaskAttempt(TezTaskAttemptID attemptId) {
      LlapNodeId llapNodeId = attemptToNodeMap.remove(attemptId);
      if (llapNodeId == null) {
        // Possible since either container / task can be unregistered.
        return;
      }

      BiMap<ContainerId, TezTaskAttemptID> bMap = nodeMap.get(llapNodeId);
      ContainerId matched = null;
      if (bMap != null) {
        synchronized(bMap) {
          matched = bMap.inverse().remove(attemptId);
        }
      }
      // Removing here. Registration into the map has to make sure to put
      if (bMap.isEmpty()) {
        nodeMap.remove(llapNodeId);
      }

      // Remove the container mapping
      if (matched != null) {
        containerToNodeMap.remove(matched);
      }

    }

    void registerContainer(ContainerId containerId, String hostname, int port) {
      containerToNodeMap.putIfAbsent(containerId, LlapNodeId.getInstance(hostname, port));
    }

    void unregisterContainer(ContainerId containerId) {
      LlapNodeId llapNodeId = containerToNodeMap.remove(containerId);
      if (llapNodeId == null) {
        // Possible since either container / task can be unregistered.
        return;
      }

      BiMap<ContainerId, TezTaskAttemptID> bMap = nodeMap.get(llapNodeId);
      TezTaskAttemptID matched = null;
      if (bMap != null) {
        synchronized(bMap) {
          matched = bMap.remove(containerId);
        }
      }
      // Removing here. Registration into the map has to make sure to put
      if (bMap.isEmpty()) {
        nodeMap.remove(llapNodeId);
      }

      // Remove the container mapping
      if (matched != null) {
        attemptToNodeMap.remove(matched);
      }
    }

    private final AtomicLong nodeNotFoundLogTime = new AtomicLong(0);
    void nodePinged(String hostname, int port) {
      LlapNodeId nodeId = LlapNodeId.getInstance(hostname, port);
      BiMap<ContainerId, TezTaskAttemptID> biMap = nodeMap.get(nodeId);
      if (biMap != null) {
        synchronized(biMap) {
          for (Map.Entry<ContainerId, TezTaskAttemptID> entry : biMap.entrySet()) {
            getTaskCommunicatorContext().taskAlive(entry.getValue());
            getTaskCommunicatorContext().containerAlive(entry.getKey());
          }
        }
      } else {
        if (System.currentTimeMillis() > nodeNotFoundLogTime.get() + 5000l) {
          LOG.warn("Recevied ping from unknown node: " + hostname + ":" + port +
              ". Could be caused by pre-emption by the AM," +
              " or a mismatched hostname. Enable debug logging for mismatched host names");
          nodeNotFoundLogTime.set(System.currentTimeMillis());
        }
      }
    }
  }
}