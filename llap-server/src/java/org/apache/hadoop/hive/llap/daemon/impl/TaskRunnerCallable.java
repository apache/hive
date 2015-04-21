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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.CallableWithNdc;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.tezplugins.Converters;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.internals.api.TaskReporterInterface;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.task.TezChild;
import org.apache.tez.runtime.task.TezTaskRunner;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
public class TaskRunnerCallable extends CallableWithNdc<TezChild.ContainerExecutionResult> {
  private static final Logger LOG = Logger.getLogger(TaskRunnerCallable.class);
  private final LlapDaemonProtocolProtos.SubmitWorkRequestProto request;
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
  private final ConcurrentMap<String, LlapDaemonProtocolProtos.SourceStateProto> sourceCompletionMap;
  private final TaskSpec taskSpec;
  private volatile TezTaskRunner taskRunner;
  private volatile TaskReporterInterface taskReporter;
  private volatile ListeningExecutorService executor;
  private LlapTaskUmbilicalProtocol umbilical;
  private volatile long startTime;
  private volatile String threadName;
  private LlapDaemonExecutorMetrics metrics;
  protected String requestId;

  TaskRunnerCallable(LlapDaemonProtocolProtos.SubmitWorkRequestProto request, Configuration conf,
      ExecutionContext executionContext, Map<String, String> envMap,
      String[] localDirs, Credentials credentials,
      long memoryAvailable, AMReporter amReporter,
      ConcurrentMap<String, LlapDaemonProtocolProtos.SourceStateProto> sourceCompletionMap,
      ConfParams confParams, LlapDaemonExecutorMetrics metrics) {
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
    if (jobToken != null) {
    this.amReporter.registerTask(request.getAmHost(), request.getAmPort(),
        request.getUser(), jobToken);
    }
    this.metrics = metrics;
    this.requestId = getTaskAttemptId(request);
  }

  @Override
  protected TezChild.ContainerExecutionResult callInternal() throws Exception {
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
    Multimap<String, String> startedInputsMap = createStartedInputMap(request.getFragmentSpec());

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
        return new TezChild.ContainerExecutionResult(
            TezChild.ContainerExecutionResult.ExitStatus.ASKED_TO_DIE, null,
            "Asked to die by the AM");
      }
    } catch (IOException e) {
      return new TezChild.ContainerExecutionResult(
          TezChild.ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
          e, "TaskExecutionFailure: " + e.getMessage());
    } catch (TezException e) {
      return new TezChild.ContainerExecutionResult(
          TezChild.ContainerExecutionResult.ExitStatus.EXECUTION_FAILURE,
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

    return new TezChild.ContainerExecutionResult(
        TezChild.ContainerExecutionResult.ExitStatus.SUCCESS, null,
        null);
  }

  /**
   * Check whether a task can run to completion or may end up blocking on it's sources.
   * This currently happens via looking up source state.
   * TODO: Eventually, this should lookup the Hive Processor to figure out whether
   * it's reached a state where it can finish - especially in cases of failures
   * after data has been fetched.
   *
   * @return
   */
  public boolean canFinish() {
    List<InputSpec> inputSpecList = taskSpec.getInputs();
    boolean canFinish = true;
    if (inputSpecList != null && !inputSpecList.isEmpty()) {
      for (InputSpec inputSpec : inputSpecList) {
        if (isSourceOfInterest(inputSpec)) {
          // Lookup the state in the map.
          LlapDaemonProtocolProtos.SourceStateProto state = sourceCompletionMap
              .get(inputSpec.getSourceVertexName());
          if (state != null && state == LlapDaemonProtocolProtos.SourceStateProto.S_SUCCEEDED) {
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

  private Multimap<String, String> createStartedInputMap(FragmentSpecProto fragmentSpec) {
    Multimap<String, String> startedInputMap = HashMultimap.create();
    // Let the Processor control start for Broadcast inputs.

    // TODO For now, this affects non broadcast unsorted cases as well. Make use of the edge
    // property when it's available.
    for (IOSpecProto inputSpec : fragmentSpec.getInputSpecsList()) {
      if (inputSpec.getIoDescriptor().getClassName().equals(UnorderedKVInput.class.getName())) {
        startedInputMap.put(fragmentSpec.getVertexName(), inputSpec.getConnectedVertexName());
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
    return requestId;
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

  public int getVertexParallelism() {
    return request.getFragmentSpec().getVertexParallelism();
  }

  public String getRequestId() {
    return requestId;
  }

  public TaskRunnerCallback getCallback() {
    return new TaskRunnerCallback(request, this);
  }

  final class TaskRunnerCallback implements FutureCallback<TezChild.ContainerExecutionResult> {

    private final LlapDaemonProtocolProtos.SubmitWorkRequestProto request;
    private final TaskRunnerCallable taskRunnerCallable;
    private final String requestId;

    TaskRunnerCallback(LlapDaemonProtocolProtos.SubmitWorkRequestProto request,
        TaskRunnerCallable taskRunnerCallable) {
      this.request = request;
      this.taskRunnerCallable = taskRunnerCallable;
      this.requestId = getTaskIdentifierString(request);
    }

    public String getRequestId() {
      return requestId;
    }

    // TODO Slightly more useful error handling
    @Override
    public void onSuccess(TezChild.ContainerExecutionResult result) {
      switch (result.getExitStatus()) {
        case SUCCESS:
          LOG.info("Successfully finished: " + requestId);
          metrics.incrExecutorTotalSuccess();
          break;
        case EXECUTION_FAILURE:
          LOG.info("Failed to run: " + requestId);
          metrics.incrExecutorTotalExecutionFailed();
          break;
        case INTERRUPTED:
          LOG.info("Interrupted while running: " + requestId);
          metrics.incrExecutorTotalInterrupted();
          break;
        case ASKED_TO_DIE:
          LOG.info("Asked to die while running: " + requestId);
          metrics.incrExecutorTotalAskedToDie();
          break;
      }
      taskRunnerCallable.shutdown();
      HistoryLogger
          .logFragmentEnd(request.getApplicationIdString(), request.getContainerIdString(),
              executionContext.getHostName(), request.getFragmentSpec().getDagName(),
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
              executionContext.getHostName(), request.getFragmentSpec().getDagName(),
              request.getFragmentSpec().getVertexName(),
              request.getFragmentSpec().getFragmentNumber(),
              request.getFragmentSpec().getAttemptNumber(), taskRunnerCallable.threadName,
              taskRunnerCallable.startTime, false);
      if (metrics != null) {
        metrics.decrExecutorNumQueuedRequests();
      }
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
      LlapDaemonProtocolProtos.SubmitWorkRequestProto request) {
    StringBuilder sb = new StringBuilder();
    sb.append("AppId=").append(request.getApplicationIdString())
        .append(", containerId=").append(request.getContainerIdString())
        .append(", Dag=").append(request.getFragmentSpec().getDagName())
        .append(", Vertex=").append(request.getFragmentSpec().getVertexName())
        .append(", FragmentNum=").append(request.getFragmentSpec().getFragmentNumber())
        .append(", Attempt=").append(request.getFragmentSpec().getAttemptNumber());
    return sb.toString();
  }

  private String getTaskAttemptId(SubmitWorkRequestProto request) {
    return request.getFragmentSpec().getTaskAttemptIdString();
  }
}
