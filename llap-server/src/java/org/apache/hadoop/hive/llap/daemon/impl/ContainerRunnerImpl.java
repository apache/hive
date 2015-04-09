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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CallableWithNdc;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GroupInputSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.hive.llap.tezplugins.Converters;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.tez.runtime.task.TezTaskRunner;

public class ContainerRunnerImpl extends AbstractService implements ContainerRunner {

  public static final String THREAD_NAME_FORMAT_PREFIX = "ContainerExecutor ";
  public static final String THREAD_NAME_FORMAT = THREAD_NAME_FORMAT_PREFIX + "%d";
  private static final Logger LOG = Logger.getLogger(ContainerRunnerImpl.class);

  private volatile AMReporter amReporter;
  private final ListeningExecutorService executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final String[] localDirsBase;
  private final Map<String, String> localEnv = new HashMap<>();
  private final FileSystem localFs;
  private final long memoryPerExecutor;
  private final LlapDaemonExecutorMetrics metrics;
  private final Configuration conf;
  private final ConfParams confParams;

  // Map of dagId to vertices and associated state.
  private final ConcurrentMap<String, ConcurrentMap<String, SourceStateProto>> sourceCompletionMap = new ConcurrentHashMap<>();
  // TODO Support for removing queued containers, interrupting / killing specific containers

  public ContainerRunnerImpl(Configuration conf, int numExecutors, String[] localDirsBase, int localShufflePort,
      AtomicReference<InetSocketAddress> localAddress,
      long totalMemoryAvailableBytes, LlapDaemonExecutorMetrics metrics) {
    super("ContainerRunnerImpl");
    this.conf = conf;
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.localDirsBase = localDirsBase;
    this.localAddress = localAddress;

    ExecutorService raw = Executors.newFixedThreadPool(numExecutors,
        new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_FORMAT).build());
    this.executorService = MoreExecutors.listeningDecorator(raw);
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
        ByteBuffer.allocate(4).putInt(localShufflePort), localEnv);

    // 80% of memory considered for accounted buffers. Rest for objects.
    // TODO Tune this based on the available size.
    this.memoryPerExecutor = (long)(totalMemoryAvailableBytes * 0.8 / (float) numExecutors);
    this.metrics = metrics;

    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
    confParams = new ConfParams(
        conf.getInt(TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT),
        conf.getLong(
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS,
            TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT),
        conf.getInt(TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
            TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT)
    );

    LOG.info("ContainerRunnerImpl config: " +
        "memoryPerExecutorDerviced=" + memoryPerExecutor
    );
  }

  @Override
  public void serviceStart() {
    // The node id will only be available at this point, since the server has been started in LlapDaemon
    LlapNodeId llapNodeId = LlapNodeId.getInstance(localAddress.get().getHostName(),
        localAddress.get().getPort());
    this.amReporter = new AMReporter(llapNodeId, conf);
    amReporter.init(conf);
    amReporter.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (amReporter != null) {
      amReporter.stop();
      amReporter = null;
    }
    super.serviceStop();
  }

  // TODO Move this into a utilities class
  private static String createAppSpecificLocalDir(String baseDir, String applicationIdString,
                                                  String user) {
    // TODO This is broken for secure clusters. The app will not have permission to create these directories.
    // May work via Slider - since the directory would already exist. Otherwise may need a custom shuffle handler.
    // TODO This should be the process user - and not the user on behalf of whom the query is being submitted.
    return baseDir + File.separator + "usercache" + File.separator + user + File.separator +
        "appcache" + File.separator + applicationIdString;
  }

  @Override
  public void submitWork(SubmitWorkRequestProto request) throws IOException {
    HistoryLogger.logFragmentStart(request.getApplicationIdString(), request.getContainerIdString(),
        localAddress.get().getHostName(), request.getFragmentSpec().getDagName(),
        request.getFragmentSpec().getVertexName(), request.getFragmentSpec().getFragmentNumber(),
        request.getFragmentSpec().getAttemptNumber());
    LOG.info("Queueing container for execution: " + stringifySubmitRequest(request));
    // This is the start of container-annotated logging.
    // TODO Reduce the length of this string. Way too verbose at the moment.
    String ndcContextString =
        request.getContainerIdString() + "_" +
            request.getFragmentSpec().getDagName() + "_" +
            request.getFragmentSpec().getVertexName() +
            "_" + request.getFragmentSpec().getFragmentNumber() + "_" +
            request.getFragmentSpec().getAttemptNumber();
    NDC.push(ndcContextString);
    try {
      Map<String, String> env = new HashMap<String, String>();
      // TODO What else is required in this environment map.
      env.putAll(localEnv);
      env.put(ApplicationConstants.Environment.USER.name(), request.getUser());

      String[] localDirs = new String[localDirsBase.length];

      // Setup up local dirs to be application specific, and create them.
      for (int i = 0; i < localDirsBase.length; i++) {
        localDirs[i] = createAppSpecificLocalDir(localDirsBase[i], request.getApplicationIdString(),
            request.getUser());
        localFs.mkdirs(new Path(localDirs[i]));
      }
      // TODO Avoid this directory creation on each work-unit submission.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Dirs are: " + Arrays.toString(localDirs));
      }
      // May need to setup localDir for re-localization, which is usually setup as Environment.PWD.
      // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)

      Credentials credentials = new Credentials();
      DataInputBuffer dib = new DataInputBuffer();
      byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
      dib.reset(tokenBytes, tokenBytes.length);
      credentials.readTokenStorageStream(dib);

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

      // TODO Unregistering does not happen at the moment, since there's no signals on when an app completes.
      LOG.info("DEBUG: Registering request with the ShuffleHandler");
      ShuffleHandler.get().registerApplication(request.getApplicationIdString(), jobToken, request.getUser(), localDirs);

      ConcurrentMap<String, SourceStateProto> sourceCompletionMap = getSourceCompletionMap(request.getFragmentSpec().getDagName());
      TaskRunnerCallable callable = new TaskRunnerCallable(request, new Configuration(getConfig()),
          new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
          credentials, memoryPerExecutor, amReporter, sourceCompletionMap, confParams);
      ListenableFuture<ContainerExecutionResult> future = executorService.submit(callable);
      Futures.addCallback(future, new TaskRunnerCallback(request, callable));
      metrics.incrExecutorTotalRequestsHandled();
      metrics.incrExecutorNumQueuedRequests();
    } finally {
      NDC.pop();
    }
  }

  @Override
  public void sourceStateUpdated(SourceStateUpdatedRequestProto request) {
    LOG.info("Processing state update: " + stringifySourceStateUpdateRequest(request));
    ConcurrentMap<String, SourceStateProto> dagMap = getSourceCompletionMap(request.getDagName());
    dagMap.put(request.getSrcName(), request.getState());
  }

  static class TaskRunnerCallable extends CallableWithNdc<ContainerExecutionResult> {

    private final SubmitWorkRequestProto request;
    private final Configuration conf;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;
    private final Credentials credentials;
    private final long memoryAvailable;
    private final ConfParams confParams;
    private final Token<JobTokenIdentifier> jobToken;
    private final AMReporter amReporter;
    private final ConcurrentMap<String, SourceStateProto> sourceCompletionMap;
    private final TaskSpec taskSpec;
    private volatile TezTaskRunner taskRunner;
    private volatile TaskReporterInterface taskReporter;
    private volatile ListeningExecutorService executor;
    private LlapTaskUmbilicalProtocol umbilical;
    private volatile long startTime;
    private volatile String threadName;
    private volatile boolean cancelled = false;



    TaskRunnerCallable(SubmitWorkRequestProto request, Configuration conf,
                            ExecutionContext executionContext, Map<String, String> envMap,
                            String[] localDirs, Credentials credentials,
                            long memoryAvailable, AMReporter amReporter,
                            ConcurrentMap<String, SourceStateProto> sourceCompletionMap, ConfParams confParams) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
      this.sourceCompletionMap = sourceCompletionMap;
      this.credentials = credentials;
      this.memoryAvailable = memoryAvailable;
      this.confParams = confParams;
      this.jobToken = TokenCache.getSessionToken(credentials);
      this.taskSpec = Converters.getTaskSpecfromProto(request.getFragmentSpec());
      this.amReporter = amReporter;
      // Register with the AMReporter when the callable is setup. Unregister once it starts running.
      this.amReporter.registerTask(request.getAmHost(), request.getAmPort(), request.getUser(), jobToken);
    }

    @Override
    protected ContainerExecutionResult callInternal() throws Exception {
      this.startTime = System.currentTimeMillis();
      this.threadName = Thread.currentThread().getName();
      if (LOG.isDebugEnabled()) {
        LOG.debug("canFinish: " + taskSpec.getTaskAttemptID() + ": " + canFinish());
      }


      // Unregister from the AMReporter, since the task is now running.
      this.amReporter.unregisterTask(request.getAmHost(), request.getAmPort());

      // TODO This executor seems unnecessary. Here and TezChild
      ExecutorService executorReal = Executors.newFixedThreadPool(1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat(
                  "TezTaskRunner_" + request.getFragmentSpec().getTaskAttemptIdString())
              .build());
      executor = MoreExecutors.listeningDecorator(executorReal);

      // TODO Consolidate this code with TezChild.
      Stopwatch sw = new Stopwatch().start();
      UserGroupInformation taskUgi = UserGroupInformation.createRemoteUser(request.getUser());
      taskUgi.addCredentials(credentials);

      Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<>();
      serviceConsumerMetadata.put(TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
          TezCommonUtils.convertJobTokenToBytes(jobToken));
      Multimap<String, String> startedInputsMap = HashMultimap.create();

      UserGroupInformation taskOwner =
          UserGroupInformation.createRemoteUser(request.getTokenIdentifier());
      final InetSocketAddress address =
          NetUtils.createSocketAddrForHost(request.getAmHost(), request.getAmPort());
      SecurityUtil.setTokenService(jobToken, address);
      taskOwner.addToken(jobToken);
      umbilical = taskOwner.doAs(new PrivilegedExceptionAction<LlapTaskUmbilicalProtocol>() {
        @Override
        public LlapTaskUmbilicalProtocol run() throws Exception {
          return RPC.getProxy(LlapTaskUmbilicalProtocol.class,
              LlapTaskUmbilicalProtocol.versionID, address, conf);
        }
      });

      taskReporter = new LlapTaskReporter(
          umbilical,
          confParams.amHeartbeatIntervalMsMax,
          confParams.amCounterHeartbeatInterval,
          confParams.amMaxEventsPerHeartbeat,
          new AtomicLong(0),
          request.getContainerIdString());

      taskRunner = new TezTaskRunner(conf, taskUgi, localDirs,
          taskSpec,
          request.getAppAttemptNumber(),
          serviceConsumerMetadata, envMap, startedInputsMap, taskReporter, executor, objectRegistry,
          pid,
          executionContext, memoryAvailable);

       boolean shouldDie;
       try {
         shouldDie = !taskRunner.run();
         if (shouldDie) {
           LOG.info("Got a shouldDie notification via heartbeats. Shutting down");
           return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
               "Asked to die by the AM");
         }
       } catch (IOException e) {
         return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
             e, "TaskExecutionFailure: " + e.getMessage());
       } catch (TezException e) {
         return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
             e, "TaskExecutionFailure: " + e.getMessage());
       } finally {
        // TODO Fix UGI and FS Handling. Closing UGI here causes some errors right now.
//        FileSystem.closeAllForUGI(taskUgi);
       }
      LOG.info("ExecutionTime for Container: " + request.getContainerIdString() + "=" +
          sw.stop().elapsedMillis());
      if (LOG.isDebugEnabled()) {
        LOG.debug("canFinish post completion: " + taskSpec.getTaskAttemptID() + ": " + canFinish());
      }

      return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
          null);
    }

    /**
     * Check whether a task can run to completion or may end up blocking on it's sources.
     * This currently happens via looking up source state.
     * TODO: Eventually, this should lookup the Hive Processor to figure out whether
     * it's reached a state where it can finish - especially in cases of failures
     * after data has been fetched.
     * @return
     */
    public boolean canFinish() {
      List<InputSpec> inputSpecList =  taskSpec.getInputs();
      boolean canFinish = true;
      if (inputSpecList != null && !inputSpecList.isEmpty()) {
        for (InputSpec inputSpec : inputSpecList) {
          if (isSourceOfInterest(inputSpec)) {
            // Lookup the state in the map.
            SourceStateProto state = sourceCompletionMap.get(inputSpec.getSourceVertexName());
            if (state != null && state == SourceStateProto.S_SUCCEEDED) {
              continue;
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot finish due to source: " + inputSpec.getSourceVertexName());
              }
              canFinish = false;
              break;
            }
          }
        }
      }
      return canFinish;
    }

    private boolean isSourceOfInterest(InputSpec inputSpec) {
      String inputClassName = inputSpec.getInputDescriptor().getClassName();
      // MRInput is not of interest since it'll always be ready.
      return !inputClassName.equals(MRInputLegacy.class.getName());
    }

    public void shutdown() {
      executor.shutdownNow();
      if (taskReporter != null) {
        taskReporter.shutdown();
      }
      if (umbilical != null) {
        RPC.stopProxy(umbilical);
      }
    }
  }

  final class TaskRunnerCallback implements FutureCallback<ContainerExecutionResult> {

    private final SubmitWorkRequestProto request;
    private final TaskRunnerCallable taskRunnerCallable;

    TaskRunnerCallback(SubmitWorkRequestProto request,
                       TaskRunnerCallable taskRunnerCallable) {
      this.request = request;
      this.taskRunnerCallable = taskRunnerCallable;
    }

    // TODO Slightly more useful error handling
    @Override
    public void onSuccess(ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + getTaskIdentifierString(request));
          metrics.incrExecutorTotalSuccess();
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + getTaskIdentifierString(request));
          metrics.incrExecutorTotalExecutionFailed();
          break;
        case INTERRUPTED:
          LOG.info("Interrupted while running: " + getTaskIdentifierString(request));
          metrics.incrExecutorTotalInterrupted();
          break;
        case ASKED_TO_DIE:
          LOG.info("Asked to die while running: " + getTaskIdentifierString(request));
          metrics.incrExecutorTotalAskedToDie();
          break;
      }
      taskRunnerCallable.shutdown();
      HistoryLogger
          .logFragmentEnd(request.getApplicationIdString(), request.getContainerIdString(),
              localAddress.get().getHostName(), request.getFragmentSpec().getDagName(),
              request.getFragmentSpec().getVertexName(),
              request.getFragmentSpec().getFragmentNumber(),
              request.getFragmentSpec().getAttemptNumber(), taskRunnerCallable.threadName,
              taskRunnerCallable.startTime, true);
      metrics.decrExecutorNumQueuedRequests();
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("TezTaskRunner execution failed for : " + getTaskIdentifierString(request), t);
      // TODO HIVE-10236 Report a fatal error over the umbilical
      taskRunnerCallable.shutdown();
      HistoryLogger
          .logFragmentEnd(request.getApplicationIdString(), request.getContainerIdString(),
              localAddress.get().getHostName(), request.getFragmentSpec().getDagName(),
              request.getFragmentSpec().getVertexName(),
              request.getFragmentSpec().getFragmentNumber(),
              request.getFragmentSpec().getAttemptNumber(), taskRunnerCallable.threadName,
              taskRunnerCallable.startTime, false);
      metrics.decrExecutorNumQueuedRequests();
    }

    private String getTaskIdentifierString(SubmitWorkRequestProto request) {
      StringBuilder sb = new StringBuilder();
      sb.append("AppId=").append(request.getApplicationIdString())
          .append(", containerId=").append(request.getContainerIdString())
          .append(", Dag=").append(request.getFragmentSpec().getDagName())
          .append(", Vertex=").append(request.getFragmentSpec().getVertexName())
          .append(", FragmentNum=").append(request.getFragmentSpec().getFragmentNumber())
          .append(", Attempt=").append(request.getFragmentSpec().getAttemptNumber());
      return sb.toString();
    }
  }

  private static class ConfParams {
    final int amHeartbeatIntervalMsMax;
    final long amCounterHeartbeatInterval;
    final int amMaxEventsPerHeartbeat;

    public ConfParams(int amHeartbeatIntervalMsMax, long amCounterHeartbeatInterval,
                      int amMaxEventsPerHeartbeat) {
      this.amHeartbeatIntervalMsMax = amHeartbeatIntervalMsMax;
      this.amCounterHeartbeatInterval = amCounterHeartbeatInterval;
      this.amMaxEventsPerHeartbeat = amMaxEventsPerHeartbeat;
    }
  }

  private String stringifySourceStateUpdateRequest(SourceStateUpdatedRequestProto request) {
    StringBuilder sb = new StringBuilder();
    sb.append("dagName=").append(request.getDagName())
        .append(", ").append("sourceName=").append(request.getSrcName())
        .append(", ").append("state=").append(request.getState());
    return sb.toString();
  }

  private String stringifySubmitRequest(SubmitWorkRequestProto request) {
    StringBuilder sb = new StringBuilder();
    sb.append("am_details=").append(request.getAmHost()).append(":").append(request.getAmPort());
    sb.append(", user=").append(request.getUser());
    sb.append(", appIdString=").append(request.getApplicationIdString());
    sb.append(", appAttemptNum=").append(request.getAppAttemptNumber());
    sb.append(", containerIdString=").append(request.getContainerIdString());
    FragmentSpecProto fragmentSpec = request.getFragmentSpec();
    sb.append(", dagName=").append(fragmentSpec.getDagName());
    sb.append(", vertexName=").append(fragmentSpec.getVertexName());
    sb.append(", taskInfo=").append(fragmentSpec.getTaskAttemptIdString());
    sb.append(", processor=").append(fragmentSpec.getProcessorDescriptor().getClassName());
    sb.append(", numInputs=").append(fragmentSpec.getInputSpecsCount());
    sb.append(", numOutputs=").append(fragmentSpec.getOutputSpecsCount());
    sb.append(", numGroupedInputs=").append(fragmentSpec.getGroupedInputSpecsCount());
    sb.append(", Inputs={");
    if (fragmentSpec.getInputSpecsCount() > 0) {
      for (IOSpecProto ioSpec : fragmentSpec.getInputSpecsList()) {
        sb.append("{").append(ioSpec.getConnectedVertexName()).append(",")
            .append(ioSpec.getIoDescriptor().getClassName()).append(",")
            .append(ioSpec.getPhysicalEdgeCount()).append("}");
      }
    }
    sb.append("}");
    sb.append(", Outputs={");
    if (fragmentSpec.getOutputSpecsCount() > 0) {
      for (IOSpecProto ioSpec : fragmentSpec.getOutputSpecsList()) {
        sb.append("{").append(ioSpec.getConnectedVertexName()).append(",")
            .append(ioSpec.getIoDescriptor().getClassName()).append(",")
            .append(ioSpec.getPhysicalEdgeCount()).append("}");
      }
    }
    sb.append("}");
    sb.append(", GroupedInputs={");
    if (fragmentSpec.getGroupedInputSpecsCount() > 0) {
      for (GroupInputSpecProto group : fragmentSpec.getGroupedInputSpecsList()) {
        sb.append("{").append("groupName=").append(group.getGroupName()).append(", elements=")
            .append(group.getGroupVerticesList()).append("}");
        sb.append(group.getGroupVerticesList());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  private ConcurrentMap<String, SourceStateProto> getSourceCompletionMap(String dagName) {
    ConcurrentMap<String, SourceStateProto> dagMap = sourceCompletionMap.get(dagName);
    if (dagMap == null) {
      dagMap = new ConcurrentHashMap<>();
      ConcurrentMap<String, SourceStateProto> old = sourceCompletionMap.putIfAbsent(dagName, dagMap);
      dagMap = (old != null) ? old : dagMap;
    }
    return dagMap;
  }
}
