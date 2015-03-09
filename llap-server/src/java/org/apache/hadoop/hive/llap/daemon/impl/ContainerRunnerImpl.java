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
import java.util.Map;
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
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
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
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.task.TaskReporter;
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

  private final ListeningExecutorService executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final String[] localDirsBase;
  private final Map<String, String> localEnv = new HashMap<String, String>();
  private volatile FileSystem localFs;
  private final long memoryPerExecutor;
  private final LlapDaemonExecutorMetrics metrics;
  private final ConfParams confParams;
  // TODO Support for removing queued containers, interrupting / killing specific containers

  public ContainerRunnerImpl(int numExecutors, String[] localDirsBase, int localShufflePort,
      AtomicReference<InetSocketAddress> localAddress,
      long totalMemoryAvailableBytes, LlapDaemonExecutorMetrics metrics) {
    super("ContainerRunnerImpl");
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.localDirsBase = localDirsBase;
    this.localAddress = localAddress;
    this.confParams = new ConfParams();
    // Setup to defaults to start with
    confParams.amMaxEventsPerHeartbeat = TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT;
    confParams.amHeartbeatIntervalMsMax =
        TezConfiguration.TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT;
    confParams.amCounterHeartbeatInterval =
        TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT;

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
    LOG.info("ContainerRunnerImpl config: " +
        "memoryPerExecutorDerviced=" + memoryPerExecutor
    );
  }

  @Override
  public void serviceInit(Configuration conf) {
    try {
      localFs = FileSystem.getLocal(conf);
      // TODO Fix visibility of these parameters - which
      confParams.amCounterHeartbeatInterval = conf.getLong(
          TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS,
          TezConfiguration.TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT);
      confParams.amHeartbeatIntervalMsMax =
          conf.getInt(TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS,
              TezConfiguration.TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
      confParams.amMaxEventsPerHeartbeat =
          conf.getInt(TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT,
              TezConfiguration.TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException("Failed to setup local filesystem instance", e);
    }
  }

  @Override
  public void serviceStart() {
  }

  @Override
  protected void serviceStop() throws Exception {
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
    LOG.info("Queuing container for execution: " + request);
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


      // Setup workingDir. This is otherwise setup as Environment.PWD
      // Used for re-localization, to add the user specified configuration (conf_pb_binary_stream)
      String workingDir = localDirs[0];

      Credentials credentials = new Credentials();
      DataInputBuffer dib = new DataInputBuffer();
      byte[] tokenBytes = request.getCredentialsBinary().toByteArray();
      dib.reset(tokenBytes, tokenBytes.length);
      credentials.readTokenStorageStream(dib);

      Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);

      // TODO Unregistering does not happen at the moment, since there's no signals on when an app completes.
      LOG.info("DEBUG: Registering request with the ShuffleHandler");
      ShuffleHandler.get().registerApplication(request.getApplicationIdString(), jobToken, request.getUser());

      TaskRunnerCallable callable = new TaskRunnerCallable(request, new Configuration(getConfig()),
          new ExecutionContextImpl(localAddress.get().getHostName()), env, localDirs,
          workingDir, credentials, memoryPerExecutor, confParams);
      ListenableFuture<ContainerExecutionResult> future = executorService.submit(callable);
      Futures.addCallback(future, new TaskRunnerCallback(request, callable));
      metrics.incrExecutorTotalRequestsHandled();
      metrics.incrExecutorNumQueuedRequests();
    } finally {
      NDC.pop();
    }
  }

  static class TaskRunnerCallable extends CallableWithNdc<ContainerExecutionResult> {

    private final SubmitWorkRequestProto request;
    private final Configuration conf;
    private final String workingDir;
    private final String[] localDirs;
    private final Map<String, String> envMap;
    private final String pid = null;
    private final ObjectRegistryImpl objectRegistry;
    private final ExecutionContext executionContext;
    private final Credentials credentials;
    private final long memoryAvailable;
    private final ListeningExecutorService executor;
    private final ConfParams confParams;
    private volatile TezTaskRunner taskRunner;
    private volatile TaskReporter taskReporter;
    private TezTaskUmbilicalProtocol umbilical;
    private volatile long startTime;
    private volatile String threadName;


    TaskRunnerCallable(SubmitWorkRequestProto request, Configuration conf,
                            ExecutionContext executionContext, Map<String, String> envMap,
                            String[] localDirs, String workingDir, Credentials credentials,
                            long memoryAvailable, ConfParams confParams) {
      this.request = request;
      this.conf = conf;
      this.executionContext = executionContext;
      this.envMap = envMap;
      this.workingDir = workingDir;
      this.localDirs = localDirs;
      this.objectRegistry = new ObjectRegistryImpl();
      this.credentials = credentials;
      this.memoryAvailable = memoryAvailable;
      this.confParams = confParams;
      // TODO This executor seems unnecessary. Here and TezChild
      ExecutorService executorReal = Executors.newFixedThreadPool(1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat(
                  "TezTaskRunner_" + request.getFragmentSpec().getTaskAttemptIdString())
              .build());
      executor = MoreExecutors.listeningDecorator(executorReal);
    }

    @Override
    protected ContainerExecutionResult callInternal() throws Exception {
      this.startTime = System.currentTimeMillis();
      this.threadName = Thread.currentThread().getName();
      // TODO Consolidate this code with TezChild.
      Stopwatch sw = new Stopwatch().start();
       UserGroupInformation taskUgi = UserGroupInformation.createRemoteUser(request.getUser());
       taskUgi.addCredentials(credentials);

       Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
       Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
       serviceConsumerMetadata.put(TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
           TezCommonUtils.convertJobTokenToBytes(jobToken));
       Multimap<String, String> startedInputsMap = HashMultimap.create();

       UserGroupInformation taskOwner =
           UserGroupInformation.createRemoteUser(request.getTokenIdentifier());
       final InetSocketAddress address =
           NetUtils.createSocketAddrForHost(request.getAmHost(), request.getAmPort());
       SecurityUtil.setTokenService(jobToken, address);
       taskOwner.addToken(jobToken);
       umbilical = taskOwner.doAs(new PrivilegedExceptionAction<TezTaskUmbilicalProtocol>() {
         @Override
         public TezTaskUmbilicalProtocol run() throws Exception {
           return RPC.getProxy(TezTaskUmbilicalProtocol.class,
               TezTaskUmbilicalProtocol.versionID, address, conf);
         }
       });

       taskReporter = new TaskReporter(
           umbilical,
           confParams.amHeartbeatIntervalMsMax,
           confParams.amCounterHeartbeatInterval,
           confParams.amMaxEventsPerHeartbeat,
           new AtomicLong(0),
           request.getContainerIdString());

       taskRunner = new TezTaskRunner(conf, taskUgi, localDirs,
           Converters.getTaskSpecfromProto(request.getFragmentSpec()), umbilical,
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
      return new ContainerExecutionResult(ContainerExecutionResult.ExitStatus.SUCCESS, null,
          null);
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
    int amHeartbeatIntervalMsMax;
    long amCounterHeartbeatInterval;
    int amMaxEventsPerHeartbeat;
  }
}
