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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.UgiFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.NotTezEventHelper;
import org.apache.hadoop.hive.llap.counters.WmFragmentCounters;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.QueryFailedHandler;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.daemon.impl.LlapTokenChecker.LlapTokenInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GroupInputSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.NotTezEvent;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmissionStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RegisterDagRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.RegisterDagResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SetCapacityResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.security.LlapExtClientJwtHelper;
import org.apache.hadoop.hive.llap.security.LlapSignerImpl;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
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
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.net.SocketFactory;

public class ContainerRunnerImpl extends CompositeService implements ContainerRunner, FragmentCompletionHandler, QueryFailedHandler {

  // TODO Setup a set of threads to process incoming requests.
  // Make sure requests for a single dag/query are handled by the same thread

  private static final Logger LOG = LoggerFactory.getLogger(ContainerRunnerImpl.class);
  public static final String THREAD_NAME_FORMAT_PREFIX = "ContainerExecutor ";

  private UgiPool ugiPool;
  private final AMReporter amReporter;
  private final QueryTracker queryTracker;
  private final Scheduler<TaskRunnerCallable> executorService;
  private final SchedulerFragmentCompletingListener completionListener;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final AtomicReference<Integer> localShufflePort;
  private final Map<String, String> localEnv = new HashMap<>();
  private final long memoryPerExecutor;
  private final LlapDaemonExecutorMetrics metrics;
  private final TaskRunnerCallable.ConfParams confParams;
  private final KilledTaskHandler killedTaskHandler = new KilledTaskHandlerImpl();
  private final HadoopShim tezHadoopShim;
  private final LlapSignerImpl signer;
  private final String clusterId;
  private final DaemonId daemonId;
  private final UgiFactory fsUgiFactory;
  private final SocketFactory socketFactory;
  private final boolean execUseFQDN;

  public ContainerRunnerImpl(Configuration conf, int numExecutors, AtomicReference<Integer> localShufflePort,
      AtomicReference<InetSocketAddress> localAddress,
      long totalMemoryAvailableBytes, LlapDaemonExecutorMetrics metrics,
      AMReporter amReporter, QueryTracker queryTracker, Scheduler<TaskRunnerCallable> executorService,
      DaemonId daemonId, UgiFactory fsUgiFactory,
      SocketFactory socketFactory) {
    super("ContainerRunnerImpl");
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.ugiPool = new UgiPool(numExecutors);
    this.localAddress = localAddress;
    this.localShufflePort = localShufflePort;
    this.amReporter = amReporter;
    this.signer = UserGroupInformation.isSecurityEnabled()
        ? new LlapSignerImpl(conf, daemonId.getClusterString()) : null;
    this.fsUgiFactory = fsUgiFactory;
    this.socketFactory = socketFactory;

    this.clusterId = daemonId.getClusterString();
    this.daemonId = daemonId;
    this.queryTracker = queryTracker;
    this.executorService = executorService;
    completionListener = (SchedulerFragmentCompletingListener) executorService;
    this.execUseFQDN = conf.getBoolean(HiveConf.ConfVars.LLAP_DAEMON_EXEC_USE_FQDN.varname, true);

    // Distribute the available memory between the tasks.
    this.memoryPerExecutor = (long)(totalMemoryAvailableBytes / (float) numExecutors);
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
  public RegisterDagResponseProto registerDag(RegisterDagRequestProto request)
      throws IOException {
    QueryIdentifierProto identifier = request.getQueryIdentifier();
    Credentials credentials;
    if (request.hasCredentialsBinary()) {
      credentials = LlapUtil.credentialsFromByteArray(
          request.getCredentialsBinary().toByteArray());
    } else {
      credentials = new Credentials();
    }
    queryTracker.registerDag(identifier.getApplicationIdString(),
        identifier.getDagIndex(), request.getUser(), credentials);
    LOG.info("Application with  id={}, dagId={} registered", identifier.getApplicationIdString(),
        identifier.getDagIndex());
    return RegisterDagResponseProto.newBuilder().build();
  }

  @Override
  public SubmitWorkResponseProto submitWork(SubmitWorkRequestProto request) throws IOException {
    LlapTokenInfo tokenInfo = null;
    try {
      tokenInfo = LlapTokenChecker.getTokenInfo(clusterId);
    } catch (SecurityException ex) {
      logSecurityErrorRarely(null);
      throw ex;
    }
    SignableVertexSpec vertex = extractVertexSpec(request, tokenInfo);
    TezEvent initialEvent = extractInitialEvent(request, tokenInfo);

    TezTaskAttemptID attemptId =
        Converters.createTaskAttemptId(vertex.getQueryIdentifier(), vertex.getVertexIndex(),
            request.getFragmentNumber(), request.getAttemptNumber());
    String fragmentIdString = attemptId.toString();

    QueryIdentifierProto qIdProto = vertex.getQueryIdentifier();

    verifyJwtForExternalClient(request, qIdProto.getApplicationIdString(), fragmentIdString);

    LOG.info("Queueing container for execution: fragemendId={}, {}", fragmentIdString,
        stringifySubmitRequest(request, vertex));

    HistoryLogger.logFragmentStart(qIdProto.getApplicationIdString(), request.getContainerIdString(),
        localAddress.get().getHostName(),
        constructUniqueQueryId(vertex.getHiveQueryId(), qIdProto.getDagIndex()),
        qIdProto.getDagIndex(),
        vertex.getVertexName(), request.getFragmentNumber(), request.getAttemptNumber());

    // This is the start of container-annotated logging.
    final String dagId = attemptId.getDAGID().toString();
    final String queryId = vertex.getHiveQueryId();
    final String fragmentId = LlapTezUtils.stripAttemptPrefix(fragmentIdString);
    MDC.put("dagId", dagId);
    MDC.put("queryId", queryId);
    MDC.put("fragmentId", fragmentId);
    // TODO: Ideally we want tez to use CallableWithMdc that retains the MDC for threads created in
    // thread pool. For now, we will push both dagId and queryId into NDC and the custom thread
    // pool that we use for task execution and llap io (StatsRecordingThreadPool) will pop them
    // using reflection and update the MDC.
    NDC.push(dagId);
    NDC.push(queryId);
    NDC.push(fragmentId);
    Scheduler.SubmissionState submissionState;
    SubmitWorkResponseProto.Builder responseBuilder = SubmitWorkResponseProto.newBuilder();
    try {
      Map<String, String> env = new HashMap<>();
      // TODO What else is required in this environment map.
      env.putAll(localEnv);
      env.put(ApplicationConstants.Environment.USER.name(), vertex.getUser());

      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.fromString(fragmentIdString);
      int dagIdentifier = taskAttemptId.getDAGID().getId();

      QueryIdentifier queryIdentifier = new QueryIdentifier(
          qIdProto.getApplicationIdString(), dagIdentifier);

      Credentials credentials = LlapUtil.credentialsFromByteArray(
          request.getCredentialsBinary().toByteArray());

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

      LlapNodeId amNodeId = LlapNodeId.getInstance(request.getAmHost(), request.getAmPort());
      QueryFragmentInfo fragmentInfo = queryTracker.registerFragment(
          queryIdentifier, qIdProto.getApplicationIdString(), dagId,
          vertex.getDagName(), vertex.getHiveQueryId(), dagIdentifier,
          vertex.getVertexName(), request.getFragmentNumber(), request.getAttemptNumber(),
          vertex.getUser(), vertex, jobToken, fragmentIdString, tokenInfo, amNodeId, ugiPool);

      // May need to setup localDir for re-localization, which is usually setup as Environment.PWD.
      // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)

      // Lazy create conf object, as it gets expensive in this codepath.
      Supplier<Configuration> callableConf = () -> new Configuration(getConfig());
      UserGroupInformation fsTaskUgi = fsUgiFactory == null ? null : fsUgiFactory.createUgi();
      boolean isGuaranteed = request.hasIsGuaranteed() && request.getIsGuaranteed();

      // enable the printing of (per daemon) LLAP task queue/run times via LLAP_TASK_TIME_SUMMARY
      ConfVars tezSummary = ConfVars.TEZ_EXEC_SUMMARY;
      ConfVars llapTasks = ConfVars.LLAP_TASK_TIME_SUMMARY;
      boolean addTaskTimes = getConfig().getBoolean(tezSummary.varname, tezSummary.defaultBoolVal)
                             && getConfig().getBoolean(llapTasks.varname, llapTasks.defaultBoolVal);

      final String llapHost;
      if (UserGroupInformation.isSecurityEnabled()) {
        // when kerberos is enabled always use FQDN
        llapHost = localAddress.get().getHostName();
      } else if (execUseFQDN) {
        // when FQDN is explicitly requested (default)
        llapHost = localAddress.get().getHostName();
      } else {
        // when FQDN is not requested, use ip address
        llapHost = localAddress.get().getAddress().getHostAddress();
      }
      LOG.info("Using llap host: {} for execution context. hostName: {} hostAddress: {}", llapHost,
        localAddress.get().getHostName(), localAddress.get().getAddress().getHostAddress());
      // TODO: ideally we'd register TezCounters here, but it seems impossible before registerTask.
      WmFragmentCounters wmCounters = new WmFragmentCounters(addTaskTimes);
      TaskRunnerCallable callable = new TaskRunnerCallable(request, fragmentInfo, callableConf,
          new ExecutionContextImpl(llapHost), env,
          credentials, memoryPerExecutor, amReporter, confParams, metrics, killedTaskHandler,
          this, tezHadoopShim, attemptId, vertex, initialEvent, fsTaskUgi,
          completionListener, socketFactory, isGuaranteed, wmCounters);
      submissionState = executorService.schedule(callable);

      LOG.info("SubmissionState for {} : {} ", fragmentIdString, submissionState);

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
      MDC.clear();
      NDC.clear();
    }

    return responseBuilder.setUniqueNodeId(daemonId.getUniqueNodeIdInCluster())
        .setSubmissionState(SubmissionStateProto.valueOf(submissionState.name()))
        .build();
  }

  // if request is coming from llap external client, verify the JWT
  // as of now, JWT contains applicationId
  private void verifyJwtForExternalClient(SubmitWorkRequestProto request, String extClientAppIdFromSplit,
      String fragmentIdString) {
    LOG.info("Checking if request[{}] is from llap external client in a cloud based deployment",
        extClientAppIdFromSplit);
    if (request.getIsExternalClientRequest() && LlapUtil.isCloudDeployment(getConfig())) {
      LOG.info("Llap external client request - {}, verifying JWT", extClientAppIdFromSplit);
      Preconditions.checkState(request.hasJwt(), "JWT not found in request, fragmentId: " + fragmentIdString);

      LlapExtClientJwtHelper llapExtClientJwtHelper = new LlapExtClientJwtHelper(getConfig());
      Jws<Claims> claimsJws;
      try {
        claimsJws = llapExtClientJwtHelper.parseClaims(request.getJwt());
      } catch (JwtException e) {
        LOG.error("Cannot verify JWT provided with the request, fragmentId: {}, {}", fragmentIdString, e);
        throw e;
      }

      String extClientAppIdFromJwt = (String) claimsJws.getBody().get(LlapExtClientJwtHelper.LLAP_EXT_CLIENT_APP_ID);

      // this should never happen ideally.
      // extClientAppId is injected in JWT and fragment request by initial get_splits() call.
      // so both of these - extClientAppIdFromJwt and extClientAppIdFromSplit should be equal eventually if the signed JWT is valid for this request.
      // In get_splits, this extClientAppId is obtained via LlapCoordinator#createExtClientAppId which generates a
      // application Id to be used by external clients.
      Preconditions.checkState(extClientAppIdFromJwt.equals(extClientAppIdFromSplit),
          String.format("applicationId[%s] in request does not match to applicationId[%s] in JWT",
              extClientAppIdFromSplit, extClientAppIdFromJwt));
      LOG.info("Llap external client request - {}, JWT verification successful", extClientAppIdFromSplit);
    }
  }

  private SignableVertexSpec extractVertexSpec(SubmitWorkRequestProto request,
      LlapTokenInfo tokenInfo) throws InvalidProtocolBufferException, IOException {
    VertexOrBinary vob = request.getWorkSpec();
    SignableVertexSpec vertex = vob.hasVertex() ? vob.getVertex() : null;
    ByteString vertexBinary = vob.hasVertexBinary() ? vob.getVertexBinary() : null;
    if (vertexBinary != null) {
      if (vertex != null) {
        throw new IOException(
          "Vertex and vertexBinary in VertexOrBinary cannot be set at the same time");
      }
      vertex = SignableVertexSpec.parseFrom(vob.getVertexBinary());
    }

    if (tokenInfo.isSigningRequired) {
      checkSignature(vertex, vertexBinary, request, tokenInfo.userName);
    }
    return vertex;
  }

  private TezEvent extractInitialEvent(SubmitWorkRequestProto request, LlapTokenInfo tokenInfo)
      throws InvalidProtocolBufferException {
    if (!request.hasInitialEventBytes()) return null;
    ByteString initialEventByteString = request.getInitialEventBytes();
    byte[] initialEventBytes = initialEventByteString.toByteArray();
    NotTezEvent initialEvent = NotTezEvent.parseFrom(initialEventBytes);
    if (tokenInfo.isSigningRequired) {
      if (!request.hasInitialEventSignature()) {
        logSecurityErrorRarely(tokenInfo.userName);
        throw new SecurityException("Unsigned initial event is not allowed");
      }
      byte[] signatureBytes = request.getInitialEventSignature().toByteArray();
      try {
        signer.checkSignature(initialEventBytes, signatureBytes, initialEvent.getKeyId());
      } catch (SecurityException ex) {
        logSecurityErrorRarely(tokenInfo.userName);
        throw ex;
      }
    }
    return NotTezEventHelper.toTezEvent(initialEvent);
  }

  private void checkSignature(SignableVertexSpec vertex, ByteString vertexBinary,
      SubmitWorkRequestProto request, String tokenUserName) throws SecurityException, IOException {
    if (!request.hasWorkSpecSignature()) {
      logSecurityErrorRarely(tokenUserName);
      throw new SecurityException("Unsigned fragment not allowed");
    }
    if (vertexBinary == null) {
      ByteString.Output os = ByteString.newOutput();
      vertex.writeTo(os);
      vertexBinary = os.toByteString();
    }
    try {
      signer.checkSignature(vertexBinary.toByteArray(),
          request.getWorkSpecSignature().toByteArray(), (int)vertex.getSignatureKeyId());
    } catch (SecurityException ex) {
      logSecurityErrorRarely(tokenUserName);
      throw ex;
    }
    if (!vertex.hasUser() || !vertex.getUser().equals(tokenUserName)) {
      logSecurityErrorRarely(tokenUserName);
      throw new SecurityException("LLAP token is for " + tokenUserName
          + " but the fragment is for " + (vertex.hasUser() ? vertex.getUser() : null));
    }
  }

  private final AtomicLong lastLoggedError = new AtomicLong(0);
  private void logSecurityErrorRarely(String userName) {
    if (!LOG.isWarnEnabled()) return;
    long time = System.nanoTime();
    long oldTime = lastLoggedError.get();
    if (oldTime != 0 && (time - oldTime) < 1000000000L) return; // 1 second
    if (!lastLoggedError.compareAndSet(oldTime, time)) return;
    String tokens = null;
    try {
      tokens = "" + LlapTokenChecker.getLlapTokens(UserGroupInformation.getCurrentUser(), null);
    } catch (Exception e) {
      tokens = "error: " + e.getMessage();
    }
    LOG.warn("Security error from " + userName + "; cluster " + clusterId + "; tokens " + tokens);
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(
      SourceStateUpdatedRequestProto request) throws IOException {
    LOG.info("Processing state update: " + stringifySourceStateUpdateRequest(request));
    QueryIdentifier queryId =
        new QueryIdentifier(request.getQueryIdentifier().getApplicationIdString(),
            request.getQueryIdentifier().getDagIndex());
    queryTracker.registerSourceStateChange(queryId, request.getSrcName(), request.getState());
    return SourceStateUpdatedResponseProto.getDefaultInstance();
  }

  @Override
  public QueryCompleteResponseProto queryComplete(
      QueryCompleteRequestProto request) throws IOException {
    QueryIdentifier queryIdentifier =
        new QueryIdentifier(request.getQueryIdentifier().getApplicationIdString(),
            request.getQueryIdentifier().getDagIndex());
    LOG.info("Processing queryComplete notification for {}", queryIdentifier);
    QueryInfo queryInfo = queryTracker.queryComplete(queryIdentifier, request.getDeleteDelay(), false);
    if (queryInfo != null) {
      List<QueryFragmentInfo> knownFragments = queryInfo.getRegisteredFragments();
      LOG.info("DBG: Pending fragment count for completed query {} = {}", queryIdentifier,
        knownFragments.size());
      for (QueryFragmentInfo fragmentInfo : knownFragments) {
        LOG.info("Issuing killFragment for completed query {} {}", queryIdentifier,
          fragmentInfo.getFragmentIdentifierString());
        executorService.killFragment(fragmentInfo.getFragmentIdentifierString());
      }
      amReporter.queryComplete(queryIdentifier);
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

  @Override
  public UpdateFragmentResponseProto updateFragment(
      UpdateFragmentRequestProto request) throws IOException {
    String fragmentId = request.getFragmentIdentifierString();
    boolean isGuaranteed = request.hasIsGuaranteed() && request.getIsGuaranteed();
    LOG.info("DBG: Received updateFragment request for {}", fragmentId);
    // TODO: ideally, QueryTracker should have fragment-to-query mapping.
    QueryIdentifier queryId = executorService.findQueryByFragment(fragmentId);
    // checkPermissions returns false if query is not found, throws on failure.
    boolean result = false;
    if (queryId != null && queryTracker.checkPermissionsForQuery(queryId)) {
      result = executorService.updateFragment(fragmentId, isGuaranteed);
    }
    return UpdateFragmentResponseProto.newBuilder()
        .setResult(result).setIsGuaranteed(isGuaranteed).build();
  }

  @Override
  public SetCapacityResponseProto setCapacity(
      SetCapacityRequestProto request) {
    executorService.setCapacity(request.getExecutorNum(), request.getQueueSize());
    return SetCapacityResponseProto.newBuilder().build();
  }

  private String stringifySourceStateUpdateRequest(SourceStateUpdatedRequestProto request) {
    StringBuilder sb = new StringBuilder();
    QueryIdentifier queryIdentifier = new QueryIdentifier(request.getQueryIdentifier().getApplicationIdString(),
        request.getQueryIdentifier().getDagIndex());
    sb.append("queryIdentifier=").append(queryIdentifier)
        .append(", ").append("sourceName=").append(request.getSrcName())
        .append(", ").append("state=").append(request.getState());
    return sb.toString();
  }

  public static String stringifySubmitRequest(
      SubmitWorkRequestProto request, SignableVertexSpec vertex) {
    StringBuilder sb = new StringBuilder();
    sb.append("am_details=").append(request.getAmHost()).append(":").append(request.getAmPort());
    sb.append(", taskInfo=").append(" fragment ")
      .append(request.getFragmentNumber()).append(" attempt ").append(request.getAttemptNumber());
    sb.append(", user=").append(vertex.getUser());
    sb.append(", queryId=").append(vertex.getHiveQueryId());
    sb.append(", appIdString=").append(vertex.getQueryIdentifier().getApplicationIdString());
    sb.append(", appAttemptNum=").append(vertex.getQueryIdentifier().getAppAttemptNumber());
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
    knownFragments = queryTracker.getRegisteredFragments(queryIdentifier);
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
    public void taskKilled(String amLocation, int port, String umbilicalUser,
                           Token<JobTokenIdentifier> jobToken, QueryIdentifier queryIdentifier,
                           TezTaskAttemptID taskAttemptId) {
      amReporter.taskKilled(amLocation, port, umbilicalUser, jobToken, queryIdentifier, taskAttemptId);
    }
  }

  public Set<String> getExecutorStatus() {
    return executorService.getExecutorsStatusForReporting();
  }

  public static String constructUniqueQueryId(String queryId, int dagIndex) {
    // Hive QueryId is not always unique.
    return queryId + "-" + dagIndex;
  }

  public int getNumActive() {
    return executorService.getNumActiveForReporting();
  }

  static class UgiPool {
    // Pool of UGI for a given appTokenIdentifier (AM). Expires after 3 hours of last access
    private final Cache<String, BlockingQueue<UserGroupInformation>> ugiPool =
        CacheBuilder
            .newBuilder().removalListener(new RemovalListener<String, BlockingQueue<UserGroupInformation>>() {
          @Override
          public void onRemoval(
              RemovalNotification<String, BlockingQueue<UserGroupInformation>> notification) {
            LOG.debug("Removing " + notification.getValue()  + " from pool.Pool size: " + ugiPool.size());
          }
        }).expireAfterAccess(60 * 3, TimeUnit.MINUTES).build();

    private final int numExecutors;

    public UgiPool(int numExecutors) {
      this.numExecutors = numExecutors;
    }

    /**
     * Get UGI for a given AM and appToken. It is possible to have more than one
     * UGI per AM.
     *
     * @param appTokenIdentifier
     * @param appToken
     * @return UserGroupInformation
     * @throws ExecutionException
     */
    public UserGroupInformation getUmbilicalUgi(String appTokenIdentifier,
        Token<JobTokenIdentifier> appToken) throws ExecutionException {
      BlockingQueue<UserGroupInformation> queue = ugiPool.get(appTokenIdentifier,
          new Callable<BlockingQueue<UserGroupInformation>>() {
            @Override
            public BlockingQueue<UserGroupInformation> call() throws Exception {
              UserGroupInformation ugi = UserGroupInformation.createRemoteUser(appTokenIdentifier);
              ugi.addToken(appToken);
              BlockingQueue<UserGroupInformation> queue = new LinkedBlockingQueue<>(numExecutors);
              queue.add(ugi);
              LOG.debug("Added new ugi pool for " + appTokenIdentifier + ", Pool Size: ");
              return queue;
            }
          });

      //Get UGI from the queue. Possible to maintain more than one UGI per AM. Ref: HIVE-16634
      UserGroupInformation ugi = queue.poll();
      if (ugi == null) {
        ugi = UserGroupInformation.createRemoteUser(appTokenIdentifier);
        ugi.addToken(appToken);
        queue.offer(ugi);
        LOG.info("Added new ugi for " + appTokenIdentifier + ". Pool size:" + ugiPool.size());
      }
      return ugi;
    }

    /**
     * Return UGI back to pool
     *
     * @param appTokenIdentifier AM identifier
     * @param ugi
     */
    public void returnUmbilicalUgi(String appTokenIdentifier, UserGroupInformation ugi) {
      BlockingQueue<UserGroupInformation> ugiQueue = ugiPool.getIfPresent(appTokenIdentifier);
      // Entry could have been removed due to expiry. Check before returning back to queue
      if (ugiQueue != null) {
        ugiQueue.offer(ugi);
      }
    }
  }

}
