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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.helpers.SourceStateTracker;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.TezTaskCommunicatorImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapTaskCommunicator extends TezTaskCommunicatorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskCommunicator.class);

  private static final boolean isInfoEnabled = LOG.isInfoEnabled();
  private static final boolean isDebugEnabed = LOG.isDebugEnabled();

  private final SubmitWorkRequestProto BASE_SUBMIT_WORK_REQUEST;

  private final ConcurrentMap<QueryIdentifierProto, ByteBuffer> credentialMap;

  // Tracks containerIds and taskAttemptIds, so can be kept independent of the running DAG.
  // When DAG specific cleanup happens, it'll be better to link this to a DAG though.
  private final EntityTracker entityTracker = new EntityTracker();
  private final SourceStateTracker sourceStateTracker;
  private final Set<LlapNodeId> nodesForQuery = new HashSet<>();

  private LlapProtocolClientProxy communicator;
  private long deleteDelayOnDagComplete;
  private final LlapTaskUmbilicalProtocol umbilical;
  private final Token<LlapTokenIdentifier> token;

  // These two structures track the list of known nodes, and the list of nodes which are sending in keep-alive heartbeats.
  // Primarily for debugging purposes a.t.m, since there's some unexplained TASK_TIMEOUTS which are currently being observed.
  private final ConcurrentMap<LlapNodeId, Long> knownNodeMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<LlapNodeId, PingingNodeInfo> pingedNodeMap = new ConcurrentHashMap<>();


  private volatile int currentDagId;
  private volatile QueryIdentifierProto currentQueryIdentifierProto;

  public LlapTaskCommunicator(
      TaskCommunicatorContext taskCommunicatorContext) {
    super(taskCommunicatorContext);
    Credentials credentials = taskCommunicatorContext.getCredentials();
    if (credentials != null) {
      @SuppressWarnings("unchecked")
      Token<LlapTokenIdentifier> llapToken =
          (Token<LlapTokenIdentifier>)credentials.getToken(LlapTokenIdentifier.KIND_NAME);
      this.token = llapToken;
    } else {
      this.token = null;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Task communicator with a token " + token);
    }
    Preconditions.checkState((token != null) == UserGroupInformation.isSecurityEnabled());

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
    sourceStateTracker = new SourceStateTracker(getContext(), this);
  }

  @Override
  public void initialize() throws Exception {
    super.initialize();
    Configuration conf = getConf();
    int numThreads = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_COMMUNICATOR_NUM_THREADS);
    this.communicator = new LlapProtocolClientProxy(numThreads, conf, token);
    this.deleteDelayOnDagComplete = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_FILE_CLEANUP_DELAY_SECONDS, TimeUnit.SECONDS);
    LOG.info("Running LlapTaskCommunicator with "
        + "fileCleanupDelay=" + deleteDelayOnDagComplete
        + ", numCommunicatorThreads=" + numThreads);
    this.communicator.init(conf);
  }

  @Override
  public void start() {
    super.start();
    this.communicator.start();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (this.communicator != null) {
      this.communicator.stop();
    }
  }

  @Override
  protected void startRpcServer() {
    Configuration conf = getConf();
    try {
      JobTokenSecretManager jobTokenSecretManager =
          new JobTokenSecretManager();
      jobTokenSecretManager.addTokenForJob(tokenIdentifier, sessionToken);

      int numHandlers = conf.getInt(TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
          TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT);
      server = new RPC.Builder(conf)
          .setProtocol(LlapTaskUmbilicalProtocol.class)
          .setBindAddress("0.0.0.0")
          .setPort(0)
          .setInstance(umbilical)
          .setNumHandlers(numHandlers)
          .setSecretManager(jobTokenSecretManager).build();

      if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
        server.refreshServiceAcl(conf, new LlapUmbilicalPolicyProvider());
      }

      server.start();
      this.address = NetUtils.getConnectAddress(server);
      LOG.info(
          "Started LlapUmbilical: " + umbilical.getClass().getName() + " at address: " + address +
              " with numHandlers=" + numHandlers);
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
  public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason, String diagnostics) {
    super.registerContainerEnd(containerId, endReason, diagnostics);
    if (endReason == ContainerEndReason.INTERNAL_PREEMPTION) {
      LOG.info("Processing containerEnd for container {} caused by internal preemption", containerId);
      TezTaskAttemptID taskAttemptId = entityTracker.getTaskAttemptIdForContainer(containerId);
      if (taskAttemptId != null) {
        sendTaskTerminated(taskAttemptId, true);
      }
    }
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
    int dagId = taskSpec.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId();
    if (currentQueryIdentifierProto == null || (dagId != currentQueryIdentifierProto.getDagIdentifier())) {
      resetCurrentDag(dagId);
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

    LlapNodeId nodeId = LlapNodeId.getInstance(host, port);
    registerKnownNode(nodeId);
    entityTracker.registerTaskAttempt(containerId, taskSpec.getTaskAttemptID(), host, port);
    nodesForQuery.add(nodeId);

    sourceStateTracker.registerTaskForStateUpdates(host, port, taskSpec.getInputs());
    FragmentRuntimeInfo fragmentRuntimeInfo = sourceStateTracker.getFragmentRuntimeInfo(
        taskSpec.getVertexName(), taskSpec.getTaskAttemptID().getTaskID().getId(), priority);
    SubmitWorkRequestProto requestProto;

    try {
      requestProto = constructSubmitWorkRequest(containerId, taskSpec, fragmentRuntimeInfo);
    } catch (IOException e) {
      throw new RuntimeException("Failed to construct request", e);
    }

    // Have to register this up front right now. Otherwise, it's possible for the task to start
    // sending out status/DONE/KILLED/FAILED messages before TAImpl knows how to handle them.
    getContext()
        .taskStartedRemotely(taskSpec.getTaskAttemptID(), containerId);
    communicator.sendSubmitWork(requestProto, host, port,
        new LlapProtocolClientProxy.ExecuteRequestCallback<SubmitWorkResponseProto>() {
          @Override
          public void setResponse(SubmitWorkResponseProto response) {
            if (response.hasSubmissionState()) {
              LlapDaemonProtocolProtos.SubmissionStateProto ss = response.getSubmissionState();
              if (ss.equals(LlapDaemonProtocolProtos.SubmissionStateProto.REJECTED)) {
                LOG.info(
                    "Unable to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " +
                        containerId + ", Service Busy");
                getContext().taskKilled(taskSpec.getTaskAttemptID(),
                    TaskAttemptEndReason.EXECUTOR_BUSY, "Service Busy");
                return;
              }
            } else {
              // TODO: Provide support for reporting errors
              // This should never happen as server always returns a valid status on success
              throw new RuntimeException("SubmissionState in response is expected!");
            }
            LOG.info("Successfully launched task: " + taskSpec.getTaskAttemptID());
          }

          @Override
          public void indicateError(Throwable t) {
            if (t instanceof ServiceException) {
              ServiceException se = (ServiceException) t;
              t = se.getCause();
            }
            if (t instanceof RemoteException) {
              RemoteException re = (RemoteException) t;
              // All others from the remote service cause the task to FAIL.
              LOG.info(
                  "Failed to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " +
                      containerId, t);
              getContext()
                  .taskFailed(taskSpec.getTaskAttemptID(), TaskAttemptEndReason.OTHER,
                      t.toString());
            } else {
              // Exception from the RPC layer - communication failure, consider as KILLED / service down.
              if (t instanceof IOException) {
                LOG.info(
                    "Unable to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " +
                        containerId + ", Communication Error");
                getContext().taskKilled(taskSpec.getTaskAttemptID(),
                    TaskAttemptEndReason.COMMUNICATION_ERROR, "Communication Error");
              } else {
                // Anything else is a FAIL.
                LOG.info(
                    "Failed to run task: " + taskSpec.getTaskAttemptID() + " on containerId: " +
                        containerId, t);
                getContext()
                    .taskFailed(taskSpec.getTaskAttemptID(), TaskAttemptEndReason.OTHER,
                        t.getMessage());
              }
            }
          }
        });
  }

  @Override
  public void unregisterRunningTaskAttempt(final TezTaskAttemptID taskAttemptId,
                                           TaskAttemptEndReason endReason,
                                           String diagnostics) {
    super.unregisterRunningTaskAttempt(taskAttemptId, endReason, diagnostics);

    if (endReason == TaskAttemptEndReason.INTERNAL_PREEMPTION) {
      LOG.info("Processing taskEnd for task {} caused by internal preemption", taskAttemptId);
      sendTaskTerminated(taskAttemptId, false);
    }
    entityTracker.unregisterTaskAttempt(taskAttemptId);
    // This will also be invoked for tasks which have been KILLED / rejected by the daemon.
    // Informing the daemon becomes necessary once the LlapScheduler supports preemption
    // and/or starts attempting to kill tasks which may be running on a node.
  }

  private void sendTaskTerminated(final TezTaskAttemptID taskAttemptId,
                                  boolean invokedByContainerEnd) {
    LOG.info(
        "DBG: Attempting to send terminateRequest for fragment {} due to internal preemption invoked by {}",
        taskAttemptId.toString(), invokedByContainerEnd ? "containerEnd" : "taskEnd");
    LlapNodeId nodeId = entityTracker.getNodeIdForTaskAttempt(taskAttemptId);
    // NodeId can be null if the task gets unregistered due to failure / being killed by the daemon itself
    if (nodeId != null) {
      TerminateFragmentRequestProto request =
          TerminateFragmentRequestProto.newBuilder().setQueryIdentifier(currentQueryIdentifierProto)
              .setFragmentIdentifierString(taskAttemptId.toString()).build();
      communicator.sendTerminateFragment(request, nodeId.getHostname(), nodeId.getPort(),
          new LlapProtocolClientProxy.ExecuteRequestCallback<TerminateFragmentResponseProto>() {
            @Override
            public void setResponse(TerminateFragmentResponseProto response) {
            }

            @Override
            public void indicateError(Throwable t) {
              LOG.warn("Failed to send terminate fragment request for {}",
                  taskAttemptId.toString());
            }
          });
    } else {
      LOG.info(
          "Not sending terminate request for fragment {} since it's node is not known. Already unregistered",
          taskAttemptId.toString());
    }
  }




  @Override
  public void dagComplete(final int dagIdentifier) {
    QueryCompleteRequestProto request = QueryCompleteRequestProto.newBuilder()
        .setQueryIdentifier(constructQueryIdentifierProto(dagIdentifier))
        .setDeleteDelay(deleteDelayOnDagComplete).build();
    for (final LlapNodeId llapNodeId : nodesForQuery) {
      LOG.info("Sending dagComplete message for {}, to {}", dagIdentifier, llapNodeId);
      communicator.sendQueryComplete(request, llapNodeId.getHostname(), llapNodeId.getPort(),
          new LlapProtocolClientProxy.ExecuteRequestCallback<LlapDaemonProtocolProtos.QueryCompleteResponseProto>() {
            @Override
            public void setResponse(LlapDaemonProtocolProtos.QueryCompleteResponseProto response) {
            }

            @Override
            public void indicateError(Throwable t) {
              LOG.warn("Failed to indicate dag complete dagId={} to node {}", dagIdentifier, llapNodeId);
            }
          });
    }

    nodesForQuery.clear();
    // TODO Ideally move some of the other cleanup code from resetCurrentDag over here
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
        new LlapProtocolClientProxy.ExecuteRequestCallback<SourceStateUpdatedResponseProto>() {
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


  private static class PingingNodeInfo {
    final AtomicLong logTimestamp;
    final AtomicInteger pingCount;

    PingingNodeInfo(long currentTs) {
      logTimestamp = new AtomicLong(currentTs);
      pingCount = new AtomicInteger(1);
    }
  }

  public void registerKnownNode(LlapNodeId nodeId) {
    Long old = knownNodeMap.putIfAbsent(nodeId,
        TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS));
    if (old == null) {
      if (isInfoEnabled) {
        LOG.info("Added new known node: {}", nodeId);
      }
    }
  }

  public void registerPingingNode(LlapNodeId nodeId) {
    long currentTs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    PingingNodeInfo ni = new PingingNodeInfo(currentTs);
    PingingNodeInfo old = pingedNodeMap.put(nodeId, ni);
    if (old == null) {
      if (isInfoEnabled) {
        LOG.info("Added new pinging node: [{}]", nodeId);
      }
    } else {
      old.pingCount.incrementAndGet();
    }
    // The node should always be known by this point. Log occasionally if it is not known.
    if (!knownNodeMap.containsKey(nodeId)) {
      if (old == null) {
        // First time this is seen. Log it.
        LOG.warn("Received ping from unknownNode: [{}], count={}", nodeId, ni.pingCount.get());
      } else {
        // Pinged before. Log only occasionally.
        if (currentTs > old.logTimestamp.get() + 5000l) { // 5 seconds elapsed. Log again.
          LOG.warn("Received ping from unknownNode: [{}], count={}", nodeId, old.pingCount.get());
          old.logTimestamp.set(currentTs);
        }
      }

    }
  }


  private final AtomicLong nodeNotFoundLogTime = new AtomicLong(0);

  void nodePinged(String hostname, int port) {
    LlapNodeId nodeId = LlapNodeId.getInstance(hostname, port);
    registerPingingNode(nodeId);
    BiMap<ContainerId, TezTaskAttemptID> biMap =
        entityTracker.getContainerAttemptMapForNode(nodeId);
    if (biMap != null) {
      synchronized (biMap) {
        for (Map.Entry<ContainerId, TezTaskAttemptID> entry : biMap.entrySet()) {
          getContext().taskAlive(entry.getValue());
          getContext().containerAlive(entry.getKey());
        }
      }
    } else {
      long currentTs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
      if (currentTs > nodeNotFoundLogTime.get() + 5000l) {
        LOG.warn("Received ping from node without any registered tasks or containers: " + hostname +
            ":" + port +
            ". Could be caused by pre-emption by the AM," +
            " or a mismatched hostname. Enable debug logging for mismatched host names");
        nodeNotFoundLogTime.set(currentTs);
      }
    }
  }

  private void resetCurrentDag(int newDagId) {
    // Working on the assumption that a single DAG runs at a time per AM.
    currentQueryIdentifierProto = constructQueryIdentifierProto(newDagId);
    sourceStateTracker.resetState(newDagId);
    nodesForQuery.clear();
    LOG.info("CurrentDagId set to: " + newDagId + ", name=" + getContext().getCurrentDagName());
    // TODO Is it possible for heartbeats to come in from lost tasks - those should be told to die, which
    // is likely already happening.
  }

  private SubmitWorkRequestProto constructSubmitWorkRequest(ContainerId containerId,
                                                            TaskSpec taskSpec,
                                                            FragmentRuntimeInfo fragmentRuntimeInfo) throws
      IOException {
    SubmitWorkRequestProto.Builder builder =
        SubmitWorkRequestProto.newBuilder(BASE_SUBMIT_WORK_REQUEST);
    builder.setContainerIdString(containerId.toString());
    builder.setAmHost(getAddress().getHostName());
    builder.setAmPort(getAddress().getPort());
    Credentials taskCredentials = new Credentials();
    // Credentials can change across DAGs. Ideally construct only once per DAG.
    taskCredentials.addAll(getContext().getCredentials());

    Preconditions.checkState(currentQueryIdentifierProto.getDagIdentifier() ==
        taskSpec.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId());
    ByteBuffer credentialsBinary = credentialMap.get(currentQueryIdentifierProto);
    if (credentialsBinary == null) {
      credentialsBinary = serializeCredentials(getContext().getCredentials());
      credentialMap.putIfAbsent(currentQueryIdentifierProto, credentialsBinary.duplicate());
    } else {
      credentialsBinary = credentialsBinary.duplicate();
    }
    builder.setCredentialsBinary(ByteString.copyFrom(credentialsBinary));
    builder.setFragmentSpec(Converters.convertTaskSpecToProto(taskSpec));
    builder.setFragmentRuntimeInfo(fragmentRuntimeInfo);
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
    public void nodeHeartbeat(Text hostname, int port) throws IOException {
      nodePinged(hostname.toString(), port);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received heartbeat from [" + hostname + ":" + port +"]");
      }
    }

    @Override
    public void taskKilled(TezTaskAttemptID taskAttemptId) throws IOException {
      // TODO Unregister the task for state updates, which could in turn unregister the node.
      getContext().taskKilled(taskAttemptId,
          TaskAttemptEndReason.EXTERNAL_PREEMPTION, "Attempt preempted");
      entityTracker.unregisterTaskAttempt(taskAttemptId);
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

  /**
   * Track the association between known containers and taskAttempts, along with the nodes they are assigned to.
   */
  @VisibleForTesting
  static final class EntityTracker {
    @VisibleForTesting
    final ConcurrentMap<TezTaskAttemptID, LlapNodeId> attemptToNodeMap = new ConcurrentHashMap<>();
    @VisibleForTesting
    final ConcurrentMap<ContainerId, LlapNodeId> containerToNodeMap = new ConcurrentHashMap<>();
    @VisibleForTesting
    final ConcurrentMap<LlapNodeId, BiMap<ContainerId, TezTaskAttemptID>> nodeMap = new ConcurrentHashMap<>();

    void registerTaskAttempt(ContainerId containerId, TezTaskAttemptID taskAttemptId, String host, int port) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering " + containerId + ", " + taskAttemptId + " for node: " + host + ":" + port);
      }
      LlapNodeId llapNodeId = LlapNodeId.getInstance(host, port);
      attemptToNodeMap.putIfAbsent(taskAttemptId, llapNodeId);

      registerContainer(containerId, host, port);

      // nodeMap registration.
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
        if (bMap.isEmpty()) {
          nodeMap.remove(llapNodeId);
        }
      }

      // Remove the container mapping
      if (matched != null) {
        containerToNodeMap.remove(matched);
      }

    }

    void registerContainer(ContainerId containerId, String hostname, int port) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Registering " + containerId + " for node: " + hostname + ":" + port);
      }
      containerToNodeMap.putIfAbsent(containerId, LlapNodeId.getInstance(hostname, port));
      // nodeMap registration is not required, since there's no taskId association.
    }

    LlapNodeId getNodeIdForContainer(ContainerId containerId) {
      return containerToNodeMap.get(containerId);
    }

    LlapNodeId getNodeIdForTaskAttempt(TezTaskAttemptID taskAttemptId) {
      return attemptToNodeMap.get(taskAttemptId);
    }

    ContainerId getContainerIdForAttempt(TezTaskAttemptID taskAttemptId) {
      LlapNodeId llapNodeId = getNodeIdForTaskAttempt(taskAttemptId);
      if (llapNodeId != null) {
        BiMap<TezTaskAttemptID, ContainerId> bMap = nodeMap.get(llapNodeId).inverse();
        if (bMap != null) {
          synchronized (bMap) {
            return bMap.get(taskAttemptId);
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    TezTaskAttemptID getTaskAttemptIdForContainer(ContainerId containerId) {
      LlapNodeId llapNodeId = getNodeIdForContainer(containerId);
      if (llapNodeId != null) {
        BiMap<ContainerId, TezTaskAttemptID> bMap = nodeMap.get(llapNodeId);
        if (bMap != null) {
          synchronized (bMap) {
            return bMap.get(containerId);
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
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
        if (bMap.isEmpty()) {
          nodeMap.remove(llapNodeId);
        }
      }

      // Remove the container mapping
      if (matched != null) {
        attemptToNodeMap.remove(matched);
      }
    }

    /**
     * Return a {@link BiMap} containing container->taskAttemptId mapping for the host specified.
     * </p>
     * <p/>
     * This method return the internal structure used by the EntityTracker. Users must synchronize
     * on the structure to ensure correct usage.
     *
     * @param llapNodeId
     * @return
     */
    BiMap<ContainerId, TezTaskAttemptID> getContainerAttemptMapForNode(LlapNodeId llapNodeId) {
      BiMap<ContainerId, TezTaskAttemptID> biMap = nodeMap.get(llapNodeId);
      return biMap;
    }

  }

  private QueryIdentifierProto constructQueryIdentifierProto(int dagIdentifier) {
    return QueryIdentifierProto.newBuilder()
        .setAppIdentifier(getContext().getCurrentAppIdentifier()).setDagIdentifier(dagIdentifier)
        .build();
  }
}
