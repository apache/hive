/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tezplugins.LlapTezUtils;
import org.apache.hadoop.hive.ql.io.IOContextMap;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.HadoopShim;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.apache.tez.runtime.task.TezTaskRunner2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TaskRunnerCallable extends CallableWithNdc<TaskRunner2Result> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskRunnerCallable.class);
  private final SubmitWorkRequestProto request;
  private final Configuration conf;
  private final Map<String, String> envMap;
  private final String pid = null;
  private final ObjectRegistryImpl objectRegistry;
  private final ExecutionContext executionContext;
  private final Credentials credentials;
  private final long memoryAvailable;
  private final ConfParams confParams;
  private final Token<JobTokenIdentifier> jobToken;
  private final AMReporter amReporter;
  private final TaskSpec taskSpec;
  private final QueryFragmentInfo fragmentInfo;
  private final KilledTaskHandler killedTaskHandler;
  private final FragmentCompletionHandler fragmentCompletionHanler;
  private volatile TezTaskRunner2 taskRunner;
  private volatile TaskReporterInterface taskReporter;
  private volatile ExecutorService executor;
  private LlapTaskUmbilicalProtocol umbilical;
  private volatile long startTime;
  private volatile String threadName;
  private final LlapDaemonExecutorMetrics metrics;
  private final String requestId;
  private final String threadNameSuffix;
  private final String queryId;
  private final HadoopShim tezHadoopShim;
  private boolean shouldRunTask = true;
  final Stopwatch runtimeWatch = new Stopwatch();
  final Stopwatch killtimerWatch = new Stopwatch();
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final AtomicBoolean isCompleted = new AtomicBoolean(false);
  private final AtomicBoolean killInvoked = new AtomicBoolean(false);
  private final SignableVertexSpec vertex;
  private final TezEvent initialEvent;
  private final SchedulerFragmentCompletingListener completionListener;
  private UserGroupInformation fsTaskUgi;
  private final SocketFactory socketFactory;

  @VisibleForTesting
  public TaskRunnerCallable(SubmitWorkRequestProto request, QueryFragmentInfo fragmentInfo,
                            Configuration conf, ExecutionContext executionContext, Map<String, String> envMap,
                            Credentials credentials, long memoryAvailable, AMReporter amReporter, ConfParams confParams,
                            LlapDaemonExecutorMetrics metrics, KilledTaskHandler killedTaskHandler,
                            FragmentCompletionHandler fragmentCompleteHandler, HadoopShim tezHadoopShim,
                            TezTaskAttemptID attemptId, SignableVertexSpec vertex, TezEvent initialEvent,
                            UserGroupInformation fsTaskUgi, SchedulerFragmentCompletingListener completionListener,
                            SocketFactory socketFactory) {
    this.request = request;
    this.fragmentInfo = fragmentInfo;
    this.conf = conf;
    this.executionContext = executionContext;
    this.envMap = envMap;
    this.objectRegistry = new ObjectRegistryImpl();
    this.credentials = credentials;
    this.memoryAvailable = memoryAvailable;
    this.confParams = confParams;
    this.jobToken = TokenCache.getSessionToken(credentials);
    this.vertex = vertex;
    this.taskSpec = Converters.getTaskSpecfromProto(
        vertex, request.getFragmentNumber(), request.getAttemptNumber(), attemptId);
    this.amReporter = amReporter;
    // Register with the AMReporter when the callable is setup. Unregister once it starts running.
    if (amReporter != null && jobToken != null) {
      this.amReporter.registerTask(request.getAmHost(), request.getAmPort(),
          vertex.getTokenIdentifier(), jobToken, fragmentInfo.getQueryInfo().getQueryIdentifier(), attemptId);
    }
    this.metrics = metrics;
    this.requestId = taskSpec.getTaskAttemptID().toString();
    threadNameSuffix = constructThreadNameSuffix(taskSpec.getTaskAttemptID());

    this.queryId = ContainerRunnerImpl
        .constructUniqueQueryId(vertex.getHiveQueryId(),
            fragmentInfo.getQueryInfo().getDagIdentifier());
    this.killedTaskHandler = killedTaskHandler;
    this.fragmentCompletionHanler = fragmentCompleteHandler;
    this.tezHadoopShim = tezHadoopShim;
    this.initialEvent = initialEvent;
    this.fsTaskUgi = fsTaskUgi;
    this.completionListener = completionListener;
    this.socketFactory = socketFactory;
  }

  public long getStartTime() {
    return startTime;
  }


  @Override
  protected TaskRunner2Result callInternal() throws Exception {
    setMDCFromNDC();

    try {
      isStarted.set(true);
      this.startTime = System.currentTimeMillis();
      threadName = Thread.currentThread().getName();
      this.threadName = Thread.currentThread().getName();
      if (LOG.isDebugEnabled()) {
        LOG.debug("canFinish: " + taskSpec.getTaskAttemptID() + ": " + canFinish());
      }

      // Unregister from the AMReporter, since the task is now running.
      TezTaskAttemptID ta = taskSpec.getTaskAttemptID();
      this.amReporter.unregisterTask(request.getAmHost(), request.getAmPort(),
          fragmentInfo.getQueryInfo().getQueryIdentifier(), ta);

      synchronized (this) {
        if (!shouldRunTask) {
          LOG.info("Not starting task {} since it was killed earlier", ta);
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, null, false);
        }
      }

      // TODO This executor seems unnecessary. Here and TezChild
      executor = new StatsRecordingThreadPool(1, 1,
          0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(),
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("TezTR-" + threadNameSuffix)
              .build());

      // TODO Consolidate this code with TezChild.
      runtimeWatch.start();
      if (fsTaskUgi == null) {
        fsTaskUgi = UserGroupInformation.createRemoteUser(vertex.getUser());
      }
      fsTaskUgi.addCredentials(credentials);

      Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<>();
      serviceConsumerMetadata.put(TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
          TezCommonUtils.convertJobTokenToBytes(jobToken));
      Multimap<String, String> startedInputsMap = createStartedInputMap(vertex);

      final UserGroupInformation taskOwner = fragmentInfo.getQueryInfo().getUmbilicalUgi();
      if (LOG.isDebugEnabled()) {
        LOG.debug("taskOwner hashCode:" + taskOwner.hashCode());
      }
      final InetSocketAddress address =
          NetUtils.createSocketAddrForHost(request.getAmHost(), request.getAmPort());
      umbilical = taskOwner.doAs(new PrivilegedExceptionAction<LlapTaskUmbilicalProtocol>() {
        @Override
        public LlapTaskUmbilicalProtocol run() throws Exception {
          return RPC.getProxy(LlapTaskUmbilicalProtocol.class,
              LlapTaskUmbilicalProtocol.versionID, address, taskOwner, conf, socketFactory);
        }
      });

      String fragmentId = LlapTezUtils.stripAttemptPrefix(taskSpec.getTaskAttemptID().toString());
      taskReporter = new LlapTaskReporter(
          completionListener,
          umbilical,
          confParams.amHeartbeatIntervalMsMax,
          confParams.amCounterHeartbeatInterval,
          confParams.amMaxEventsPerHeartbeat,
          new AtomicLong(0),
          request.getContainerIdString(),
          fragmentId,
          initialEvent,
          requestId);

      String attemptId = fragmentInfo.getFragmentIdentifierString();
      IOContextMap.setThreadAttemptId(attemptId);
      try {
        synchronized (this) {
          if (shouldRunTask) {
            taskRunner = new TezTaskRunner2(conf, fsTaskUgi, fragmentInfo.getLocalDirs(),
                taskSpec,
                vertex.getQueryIdentifier().getAppAttemptNumber(),
                serviceConsumerMetadata, envMap, startedInputsMap, taskReporter, executor,
                objectRegistry,
                pid,
                executionContext, memoryAvailable, false, tezHadoopShim);
          }
        }
        if (taskRunner == null) {
          LOG.info("Not starting task {} since it was killed earlier", taskSpec.getTaskAttemptID());
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, null, false);
        }

        try {
          TaskRunner2Result result = taskRunner.run();
          if (result.isContainerShutdownRequested()) {
            LOG.warn("Unexpected container shutdown requested while running task. Ignoring");
          }
          isCompleted.set(true);
          return result;
        } finally {
          FileSystem.closeAllForUGI(fsTaskUgi);
          LOG.info("ExecutionTime for Container: " + request.getContainerIdString() + "=" +
                  runtimeWatch.stop().elapsedMillis());
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "canFinish post completion: " + taskSpec.getTaskAttemptID() + ": " + canFinish());
          }
        }
      } finally {
        IOContextMap.clearThreadAttempt(attemptId);
      }
    } finally {
      MDC.clear();
    }
  }

  private void setMDCFromNDC() {
    final Stack<String> clonedNDC = NDC.cloneStack();
    final String fragId = clonedNDC.pop();
    final String queryId = clonedNDC.pop();
    final String dagId = clonedNDC.pop();
    MDC.put("dagId", dagId);
    MDC.put("queryId", queryId);
    MDC.put("fragmentId", fragId);
  }

  private String constructThreadNameSuffix(TezTaskAttemptID taskAttemptId) {
    StringBuilder sb = new StringBuilder();
    TezTaskID taskId = taskAttemptId.getTaskID();
    TezVertexID vertexId = taskId.getVertexID();
    TezDAGID dagId = vertexId.getDAGId();
    ApplicationId appId = dagId.getApplicationId();
    long clusterTs = appId.getClusterTimestamp();
    long clusterTsShort = clusterTs % 1_000_000L;

    sb.append(clusterTsShort).append("_");
    sb.append(appId.getId()).append("_");
    sb.append(dagId.getId()).append("_");
    sb.append(vertexId.getId()).append("_");
    sb.append(taskId.getId()).append("_");
    sb.append(taskAttemptId.getId());
    return sb.toString();
  }

  /**
   * Attempt to kill a running task. If the task has not started running, it will not start.
   * If it's already running, a kill request will be sent to it.
   * <p/>
   * The AM will be informed about the task kill.
   */
  public void killTask() {
    if (!isCompleted.get()) {
      if (!killInvoked.getAndSet(true)) {
        synchronized (this) {
          TezTaskAttemptID ta = taskSpec.getTaskAttemptID();
          LOG.info("Kill task requested for id={}, taskRunnerSetup={}", ta, taskRunner != null);
          shouldRunTask = false;
          if (taskRunner != null) {
            killtimerWatch.start();
            LOG.info("Issuing kill to task {}", taskSpec.getTaskAttemptID());
            boolean killed = taskRunner.killTask();

            if (killed) {
              // Sending a kill message to the AM right here. Don't need to wait for the task to complete.
              LOG.info("Kill request for task {} completed. Informing AM", ta);
              // Inform the scheduler that this fragment has been killed.
              // If the kill failed - that means the task has already hit a final condition,
              // and a notification comes from the LlapTaskReporter
              completionListener.fragmentCompleting(getRequestId(), SchedulerFragmentCompletingListener.State.KILLED);
              reportTaskKilled();
            } else {
              LOG.info("Kill request for task {} did not complete because the task is already complete",
                  ta);
            }
          } else {
            // If the task hasn't started, and it is killed - report back to the AM that the task has been killed.
            LOG.debug("Reporting taskKilled for non-started fragment {}", getRequestId());
            reportTaskKilled();
          }
          if (!isStarted.get()) {
            // If the task hasn't started - inform about fragment completion immediately. It's possible for
            // the callable to never run.
            fragmentCompletionHanler.fragmentComplete(fragmentInfo);
            this.amReporter
                .unregisterTask(request.getAmHost(), request.getAmPort(),
                    fragmentInfo.getQueryInfo().getQueryIdentifier(), ta);
          }
        }
      } else {
        // This should not happen.
        LOG.warn("Ignoring kill request for task {} since a previous kill request was processed",
            taskSpec.getTaskAttemptID());
      }
    } else {
      LOG.info("Ignoring kill request for task {} since it's already complete",
          taskSpec.getTaskAttemptID());
    }
  }

  /**
   * Inform the AM that this task has been killed.
   */
  public void reportTaskKilled() {
    killedTaskHandler
        .taskKilled(request.getAmHost(), request.getAmPort(), vertex.getTokenIdentifier(), jobToken,
            fragmentInfo.getQueryInfo().getQueryIdentifier(), taskSpec.getTaskAttemptID());
  }

  public boolean canFinish() {
    return fragmentInfo.canFinish();
  }

  private static Multimap<String, String> createStartedInputMap(SignableVertexSpec vertex) {
    Multimap<String, String> startedInputMap = HashMultimap.create();
    // Let the Processor control start for Broadcast inputs.

    // TODO For now, this affects non broadcast unsorted cases as well. Make use of the edge
    // property when it's available.
    for (IOSpecProto inputSpec : vertex.getInputSpecsList()) {
      if (inputSpec.getIoDescriptor().getClassName().equals(UnorderedKVInput.class.getName())) {
        startedInputMap.put(vertex.getVertexName(), inputSpec.getConnectedVertexName());
      }
    }
    return startedInputMap;
  }

  public void shutdown() {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (taskReporter != null) {
      taskReporter.shutdown();
    }
    if (umbilical != null) {
      RPC.stopProxy(umbilical);
    }
  }

  @Override
  public String toString() {
    return requestId + " {canFinish: " + canFinish() +
        ", vertexParallelism: " + vertex.getVertexParallelism() +
        ", selfAndUpstreamParallelism: " + request.getFragmentRuntimeInfo().getNumSelfAndUpstreamTasks() +
        ", selfAndUpstreamComplete: " + request.getFragmentRuntimeInfo().getNumSelfAndUpstreamCompletedTasks() +
        ", firstAttemptStartTime: " + getFragmentRuntimeInfo().getFirstAttemptStartTime() +
        ", dagStartTime:" + getFragmentRuntimeInfo().getDagStartTime() +
        ", withinDagPriority: " + getFragmentRuntimeInfo().getWithinDagPriority() +
        "}";
  }

  @Override
  public int hashCode() {
    return requestId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TaskRunnerCallable)) {
      return false;
    }
    return requestId.equals(((TaskRunnerCallable) obj).getRequestId());
  }

  public String getRequestId() {
    return requestId;
  }

  public String getQueryId() {
    return queryId;
  }

  public QueryFragmentInfo getFragmentInfo() {
    return fragmentInfo;
  }

  public TaskRunnerCallback getCallback() {
    return new TaskRunnerCallback(request, vertex, this);
  }

  public SubmitWorkRequestProto getRequest() {
    return request;
  }

  final class TaskRunnerCallback implements FutureCallback<TaskRunner2Result> {

    private final SubmitWorkRequestProto request;
    private final SignableVertexSpec vertex;
    private final TaskRunnerCallable taskRunnerCallable;

    TaskRunnerCallback(SubmitWorkRequestProto request, SignableVertexSpec vertex,
        TaskRunnerCallable taskRunnerCallable) {
      this.request = request;
      this.vertex = vertex;
      this.taskRunnerCallable = taskRunnerCallable;
    }

    // Errors are handled on the way over. FAIL/SUCCESS is informed via regular heartbeats. Killed
    // via a kill message when a task kill is requested by the daemon.
    @Override
    public void onSuccess(TaskRunner2Result result) {
      isCompleted.set(true);

      switch(result.getEndReason()) {
        // Only the KILLED case requires a message to be sent out to the AM.
        case SUCCESS:
          LOG.debug("Successfully finished {}", requestId);
          if (metrics != null) {
            metrics.incrExecutorTotalSuccess();
          }
          break;
        case CONTAINER_STOP_REQUESTED:
          LOG.info("Received container stop request (AM preemption) for {}", requestId);
          if (metrics != null) {
            metrics.incrExecutorTotalKilled();
          }
          break;
        case KILL_REQUESTED:
          LOG.info("Killed task {}", requestId);
          if (killtimerWatch.isRunning()) {
            killtimerWatch.stop();
            long elapsed = killtimerWatch.elapsedMillis();
            LOG.info("Time to die for task {}", elapsed);
            if (metrics != null) {
              metrics.addMetricsPreemptionTimeToKill(elapsed);
            }
          }
          if (metrics != null) {
            metrics.addMetricsPreemptionTimeLost(runtimeWatch.elapsedMillis());
            metrics.incrExecutorTotalKilled();
          }
          break;
        case COMMUNICATION_FAILURE:
          LOG.info("Failed to run {} due to communication failure", requestId);
          if (metrics != null) {
            metrics.incrExecutorTotalExecutionFailed();
          }
          break;
        case TASK_ERROR:
          LOG.info("Failed to run {} due to task error", requestId);
          if (metrics != null) {
            metrics.incrExecutorTotalExecutionFailed();
          }
          break;
      }
      fragmentCompletionHanler.fragmentComplete(fragmentInfo);

      taskRunnerCallable.shutdown();
      logFragmentEnd(true);
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("TezTaskRunner execution failed for : "
          + getTaskIdentifierString(request, vertex, queryId), t);
      isCompleted.set(true);
      fragmentCompletionHanler.fragmentComplete(fragmentInfo);
      // TODO HIVE-10236 Report a fatal error over the umbilical
      taskRunnerCallable.shutdown();
      logFragmentEnd(false);
    }

    protected void logFragmentEnd(boolean success) {
      HistoryLogger.logFragmentEnd(vertex.getQueryIdentifier().getApplicationIdString(),
          request.getContainerIdString(), executionContext.getHostName(), queryId,
          fragmentInfo.getQueryInfo().getDagIdentifier(), vertex.getVertexName(),
          request.getFragmentNumber(), request.getAttemptNumber(), taskRunnerCallable.threadName,
          taskRunnerCallable.startTime, success);
    }
  }

  public static class ConfParams {
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

  public static String getTaskIdentifierString(
      SubmitWorkRequestProto request, SignableVertexSpec vertex, String queryId) {
    StringBuilder sb = new StringBuilder();
    sb.append("AppId=").append(vertex.getQueryIdentifier().getApplicationIdString())
        .append(", containerId=").append(request.getContainerIdString())
        .append(", QueryId=").append(queryId)
        .append(", Vertex=").append(vertex.getVertexName())
        .append(", FragmentNum=").append(request.getFragmentNumber())
        .append(", Attempt=").append(request.getAttemptNumber());
    return sb.toString();
  }

  public FragmentRuntimeInfo getFragmentRuntimeInfo() {
    return request.getFragmentRuntimeInfo();
  }

  public SignableVertexSpec getVertexSpec() {
    // TODO: support for binary spec? presumably we'd parse it somewhere earlier
    return vertex;
  }
}
