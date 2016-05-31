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

package org.apache.hadoop.hive.llap.daemon.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.QueryFailedHandler;
import org.apache.hadoop.hive.llap.daemon.impl.LlapTokenChecker.LlapTokenInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GroupInputSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmissionStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexIdentifier;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.security.LlapSignerImpl;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.log4j.NDC;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class ContainerRunnerImpl extends CompositeService implements ContainerRunner, FragmentCompletionHandler, QueryFailedHandler {

  // TODO Setup a set of threads to process incoming requests.
  // Make sure requests for a single dag/query are handled by the same thread

  private static final Logger LOG = LoggerFactory.getLogger(ContainerRunnerImpl.class);
  public static final String THREAD_NAME_FORMAT_PREFIX = "ContainerExecutor ";

  private final AMReporter amReporter;
  private final QueryTracker queryTracker;
  private final Scheduler<TaskRunnerCallable> executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final AtomicReference<Integer> localShufflePort;
  private final Map<String, String> localEnv = new HashMap<>();
  private final long memoryPerExecutor;
  private final LlapDaemonExecutorMetrics metrics;
  private final Configuration conf;
  private final TaskRunnerCallable.ConfParams confParams;
  private final KilledTaskHandler killedTaskHandler = new KilledTaskHandlerImpl();
  private final HadoopShim tezHadoopShim;
  private final LlapSignerImpl signer;
  private final String clusterId;

  public ContainerRunnerImpl(Configuration conf, int numExecutors, int waitQueueSize,
      boolean enablePreemption, String[] localDirsBase, AtomicReference<Integer> localShufflePort,
      AtomicReference<InetSocketAddress> localAddress,
      long totalMemoryAvailableBytes, LlapDaemonExecutorMetrics metrics,
      AMReporter amReporter, ClassLoader classLoader, DaemonId daemonId) {
    super("ContainerRunnerImpl");
    this.conf = conf;
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.localAddress = localAddress;
    this.localShufflePort = localShufflePort;
    this.amReporter = amReporter;
    this.signer = UserGroupInformation.isSecurityEnabled()
        ? new LlapSignerImpl(conf, daemonId) : null;

    this.clusterId = daemonId.getClusterString();
    this.queryTracker = new QueryTracker(conf, localDirsBase, clusterId);
    addIfService(queryTracker);
    String waitQueueSchedulerClassName = HiveConf.getVar(
        conf, ConfVars.LLAP_DAEMON_WAIT_QUEUE_COMPARATOR_CLASS_NAME);
    this.executorService = new TaskExecutorService(numExecutors, waitQueueSize,
        waitQueueSchedulerClassName, enablePreemption, classLoader, metrics);

    addIfService(executorService);

    // 80% of memory considered for accounted buffers. Rest for objects.
    // TODO Tune this based on the available size.
    this.memoryPerExecutor = (long)(totalMemoryAvailableBytes * 0.8 / (float) numExecutors);
    this.metrics = metrics;

    confParams = new TaskRunnerCallable.ConfParams(
        conf.getInt(TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT),
        conf.getLong(
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS,
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT),
        conf.getInt(TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
            TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT)
    );
    tezHadoopShim = new HadoopShimsLoader(conf).getHadoopShim();

    LOG.info("ContainerRunnerImpl config: " +
            "memoryPerExecutorDerviced=" + memoryPerExecutor
    );
  }

  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    LOG.info("Using ShufflePort: " + localShufflePort.get());
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
        ByteBuffer.allocate(4).putInt(localShufflePort.get()), localEnv);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public SubmitWorkResponseProto submitWork(SubmitWorkRequestProto request) throws IOException {
    VertexOrBinary vob = request.getWorkSpec();
    SignableVertexSpec vertex = vob.hasVertex() ? vob.getVertex() : null;
    ByteString vertexBinary = vob.hasVertexBinary() ? vob.getVertexBinary() : null;
    if (vertex != null && vertexBinary != null) {
      throw new IOException(
          "Vertex and vertexBinary in VertexOrBinary cannot be set at the same time");
    }
    if (vertexBinary != null) {
      vertex = SignableVertexSpec.parseFrom(vob.getVertexBinary());
    }

    LlapTokenInfo tokenInfo = LlapTokenChecker.getTokenInfo(clusterId);
    if (tokenInfo.isSigningRequired) {
      checkSignature(vertex, vertexBinary, request, tokenInfo.userName);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Queueing container for execution: " + stringifySubmitRequest(request, vertex));
    }
    VertexIdentifier vId = vertex.getVertexIdentifier();
    TezTaskAttemptID attemptId = Converters.createTaskAttemptId(
        vId, request.getFragmentNumber(), request.getAttemptNumber());
    String fragmentIdString = attemptId.toString();
    HistoryLogger.logFragmentStart(vId.getApplicationIdString(), request.getContainerIdString(),
        localAddress.get().getHostName(), vertex.getDagName(), vId.getDagId(),
        vertex.getVertexName(), request.getFragmentNumber(), request.getAttemptNumber());

    // This is the start of container-annotated logging.
    // TODO Reduce the length of this string. Way too verbose at the moment.
    NDC.push(fragmentIdString);
    Scheduler.SubmissionState submissionState;
    SubmitWorkResponseProto.Builder responseBuilder = SubmitWorkResponseProto.newBuilder();
    try {
      Map<String, String> env = new HashMap<>();
      // TODO What else is required in this environment map.
      env.putAll(localEnv);
      env.put(ApplicationConstants.Environment.USER.name(), vertex.getUser());

      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.fromString(fragmentIdString);
      int dagIdentifier = taskAttemptId.getTaskID().getVertexID().getDAGId().getId();

      QueryIdentifier queryIdentifier = new QueryIdentifier(
          vId.getApplicationIdString(), dagIdentifier);

      Credentials credentials = new Credentials();
      DataInputBuffer dib = new DataInputBuffer();
      byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
      dib.reset(tokenBytes, tokenBytes.length);
      credentials.readTokenStorageStream(dib);

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

      QueryFragmentInfo fragmentInfo = queryTracker.registerFragment(
          queryIdentifier, vId.getApplicationIdString(), vertex.getDagName(), dagIdentifier,
          vertex.getVertexName(), request.getFragmentNumber(), request.getAttemptNumber(),
          vertex.getUser(), vertex, jobToken, fragmentIdString, tokenInfo);

      String[] localDirs = fragmentInfo.getLocalDirs();
      Preconditions.checkNotNull(localDirs);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dirs are: " + Arrays.toString(localDirs));
      }
      // May need to setup localDir for re-localization, which is usually setup as Environment.PWD.
      // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)

      Configuration callableConf = new Configuration(getConfig());
      TaskRunnerCallable callable = new TaskRunnerCallable(request, fragmentInfo, callableConf,
          new LlapExecutionContext(localAddress.get().getHostName(), queryTracker), env,
          credentials, memoryPerExecutor, amReporter, confParams, metrics, killedTaskHandler,
          this, tezHadoopShim, attemptId, vertex);
      submissionState = executorService.schedule(callable);

      if (LOG.isInfoEnabled()) {
        LOG.info("SubmissionState for {} : {} ", fragmentIdString, submissionState);
      }

      if (submissionState.equals(Scheduler.SubmissionState.REJECTED)) {
        // Stop tracking the fragment and re-throw the error.
        fragmentComplete(fragmentInfo);
        return responseBuilder
            .setSubmissionState(SubmissionStateProto.valueOf(submissionState.name()))
            .build();
      }
      if (metrics != null) {
        metrics.incrExecutorTotalRequestsHandled();
      }
    } finally {
      NDC.pop();
    }

    responseBuilder.setSubmissionState(SubmissionStateProto.valueOf(submissionState.name()));
    return responseBuilder.build();
  }

  private void checkSignature(SignableVertexSpec vertex, ByteString vertexBinary,
      SubmitWorkRequestProto request, String tokenUserName) throws SecurityException, IOException {
    if (!request.hasWorkSpecSignature()) {
      throw new SecurityException("Unsigned fragment not allowed");
    }
    if (vertexBinary == null) {
      ByteString.Output os = ByteString.newOutput();
      vertex.writeTo(os);
      vertexBinary = os.toByteString();
    }
    signer.checkSignature(vertexBinary.toByteArray(),
        request.getWorkSpecSignature().toByteArray(), (int)vertex.getSignatureKeyId());
    if (!vertex.hasUser() || !vertex.getUser().equals(tokenUserName)) {
      throw new SecurityException("LLAP token is for " + tokenUserName
          + " but the fragment is for " + (vertex.hasUser() ? vertex.getUser() : null));
    }
  }

  private static class LlapExecutionContext extends ExecutionContextImpl
      implements TezProcessor.Hook {
    private final QueryTracker queryTracker;
    public LlapExecutionContext(String hostname, QueryTracker queryTracker) {
      super(hostname);
      this.queryTracker = queryTracker;
    }

    @Override
    public void initializeHook(TezProcessor source) {
      queryTracker.registerDagQueryId(
          new QueryIdentifier(source.getContext().getApplicationId().toString(),
              source.getContext().getDagIdentifier()),
          HiveConf.getVar(source.getConf(), HiveConf.ConfVars.HIVEQUERYID));
    }
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(
      SourceStateUpdatedRequestProto request) throws IOException {
    LOG.info("Processing state update: " + stringifySourceStateUpdateRequest(request));
    QueryIdentifier queryId = new QueryIdentifier(request.getQueryIdentifier().getAppIdentifier(),
        request.getQueryIdentifier().getDagIdentifier());
    queryTracker.registerSourceStateChange(queryId, request.getSrcName(), request.getState());
    return SourceStateUpdatedResponseProto.getDefaultInstance();
  }

  @Override
  public QueryCompleteResponseProto queryComplete(
      QueryCompleteRequestProto request) throws IOException {
    QueryIdentifier queryIdentifier =
        new QueryIdentifier(request.getQueryIdentifier().getAppIdentifier(),
            request.getQueryIdentifier().getDagIdentifier());
    LOG.info("Processing queryComplete notification for {}", queryIdentifier);
    List<QueryFragmentInfo> knownFragments = queryTracker.queryComplete(
        queryIdentifier, request.getDeleteDelay(), false);
    LOG.info("DBG: Pending fragment count for completed query {} = {}", queryIdentifier,
        knownFragments.size());
    for (QueryFragmentInfo fragmentInfo : knownFragments) {
      LOG.info("Issuing killFragment for completed query {} {}", queryIdentifier,
          fragmentInfo.getFragmentIdentifierString());
      executorService.killFragment(fragmentInfo.getFragmentIdentifierString());
    }
    return QueryCompleteResponseProto.getDefaultInstance();
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      TerminateFragmentRequestProto request) throws IOException {
    String fragmentId = request.getFragmentIdentifierString();
    LOG.info("DBG: Received terminateFragment request for {}", fragmentId);
    // TODO: ideally, QueryTracker should have fragment-to-query mapping.
    QueryIdentifier queryId = executorService.findQueryByFragment(fragmentId);
    // checkPermissions returns false if query is not found, throws on failure.
    if (queryId != null && queryTracker.checkPermissionsForQuery(queryId)) {
      executorService.killFragment(fragmentId);
    }
    return TerminateFragmentResponseProto.getDefaultInstance();
  }

  private String stringifySourceStateUpdateRequest(SourceStateUpdatedRequestProto request) {
    StringBuilder sb = new StringBuilder();
    QueryIdentifier queryIdentifier = new QueryIdentifier(request.getQueryIdentifier().getAppIdentifier(),
        request.getQueryIdentifier().getDagIdentifier());
    sb.append("queryIdentifier=").append(queryIdentifier)
        .append(", ").append("sourceName=").append(request.getSrcName())
        .append(", ").append("state=").append(request.getState());
    return sb.toString();
  }

  public static String stringifySubmitRequest(
      SubmitWorkRequestProto request, SignableVertexSpec vertex) {
    StringBuilder sb = new StringBuilder();
    sb.append("am_details=").append(request.getAmHost()).append(":").append(request.getAmPort());
    sb.append(", taskInfo=").append(vertex.getVertexIdentifier()).append(" fragment ")
      .append(request.getFragmentNumber()).append(" attempt ").append(request.getAttemptNumber());
    sb.append(", user=").append(vertex.getUser());
    sb.append(", appIdString=").append(vertex.getVertexIdentifier().getApplicationIdString());
    sb.append(", appAttemptNum=").append(vertex.getVertexIdentifier().getAppAttemptNumber());
    sb.append(", containerIdString=").append(request.getContainerIdString());
    sb.append(", dagName=").append(vertex.getDagName());
    sb.append(", vertexName=").append(vertex.getVertexName());
    sb.append(", processor=").append(vertex.getProcessorDescriptor().getClassName());
    sb.append(", numInputs=").append(vertex.getInputSpecsCount());
    sb.append(", numOutputs=").append(vertex.getOutputSpecsCount());
    sb.append(", numGroupedInputs=").append(vertex.getGroupedInputSpecsCount());
    sb.append(", Inputs={");
    if (vertex.getInputSpecsCount() > 0) {
      for (IOSpecProto ioSpec : vertex.getInputSpecsList()) {
        sb.append("{").append(ioSpec.getConnectedVertexName()).append(",")
            .append(ioSpec.getIoDescriptor().getClassName()).append(",")
            .append(ioSpec.getPhysicalEdgeCount()).append("}");
      }
    }
    sb.append("}");
    sb.append(", Outputs={");
    if (vertex.getOutputSpecsCount() > 0) {
      for (IOSpecProto ioSpec : vertex.getOutputSpecsList()) {
        sb.append("{").append(ioSpec.getConnectedVertexName()).append(",")
            .append(ioSpec.getIoDescriptor().getClassName()).append(",")
            .append(ioSpec.getPhysicalEdgeCount()).append("}");
      }
    }
    sb.append("}");
    sb.append(", GroupedInputs={");
    if (vertex.getGroupedInputSpecsCount() > 0) {
      for (GroupInputSpecProto group : vertex.getGroupedInputSpecsList()) {
        sb.append("{").append("groupName=").append(group.getGroupName()).append(", elements=")
            .append(group.getGroupVerticesList()).append("}");
        sb.append(group.getGroupVerticesList());
      }
    }
    sb.append("}");

    FragmentRuntimeInfo fragmentRuntimeInfo = request.getFragmentRuntimeInfo();
    sb.append(", FragmentRuntimeInfo={");
    sb.append("taskCount=").append(fragmentRuntimeInfo.getNumSelfAndUpstreamTasks());
    sb.append(", completedTaskCount=").append(fragmentRuntimeInfo.getNumSelfAndUpstreamCompletedTasks());
    sb.append(", dagStartTime=").append(fragmentRuntimeInfo.getDagStartTime());
    sb.append(", firstAttemptStartTime=").append(fragmentRuntimeInfo.getFirstAttemptStartTime());
    sb.append(", currentAttemptStartTime=").append(fragmentRuntimeInfo.getCurrentAttemptStartTime());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void fragmentComplete(QueryFragmentInfo fragmentInfo) {
    queryTracker.fragmentComplete(fragmentInfo);
  }

  @Override
  public void queryFailed(QueryIdentifier queryIdentifier) {
    LOG.info("Processing query failed notification for {}", queryIdentifier);
    List<QueryFragmentInfo> knownFragments;
    try {
      knownFragments = queryTracker.queryComplete(queryIdentifier, -1, true);
    } catch (IOException e) {
      throw new RuntimeException(e); // Should never happen here, no permission check.
    }
    LOG.info("DBG: Pending fragment count for failed query {} = {}", queryIdentifier,
        knownFragments.size());
    for (QueryFragmentInfo fragmentInfo : knownFragments) {
      LOG.info("DBG: Issuing killFragment for failed query {} {}", queryIdentifier,
          fragmentInfo.getFragmentIdentifierString());
      executorService.killFragment(fragmentInfo.getFragmentIdentifierString());
    }
  }

  private class KilledTaskHandlerImpl implements KilledTaskHandler {

    @Override
    public void taskKilled(String amLocation, int port, String user,
                           Token<JobTokenIdentifier> jobToken, QueryIdentifier queryIdentifier,
                           TezTaskAttemptID taskAttemptId) {
      amReporter.taskKilled(amLocation, port, user, jobToken, queryIdentifier, taskAttemptId);
    }
  }

  public Set<String> getExecutorStatus() {
    return executorService.getExecutorsStatus();
  }
}
