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
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.HistoryLogger;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GroupInputSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.IOSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.shufflehandler.ShuffleHandler;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;

import com.google.common.base.Preconditions;

// TODO Convert this to a CompositeService
public class ContainerRunnerImpl extends AbstractService implements ContainerRunner, FragmentCompletionHandler {

  private static final Logger LOG = Logger.getLogger(ContainerRunnerImpl.class);
  public static final String THREAD_NAME_FORMAT_PREFIX = "ContainerExecutor ";

  private volatile AMReporter amReporter;
  private final QueryTracker queryTracker;
  private final Scheduler<TaskRunnerCallable> executorService;
  private final AtomicReference<InetSocketAddress> localAddress;
  private final Map<String, String> localEnv = new HashMap<>();
  private final long memoryPerExecutor;
  private final LlapDaemonExecutorMetrics metrics;
  private final Configuration conf;
  private final TaskRunnerCallable.ConfParams confParams;
  private final KilledTaskHandler killedTaskHandler = new KilledTaskHandlerImpl();

  public ContainerRunnerImpl(Configuration conf, int numExecutors, int waitQueueSize,
      boolean enablePreemption, String[] localDirsBase, int localShufflePort,
      AtomicReference<InetSocketAddress> localAddress,
      long totalMemoryAvailableBytes, LlapDaemonExecutorMetrics metrics) {
    super("ContainerRunnerImpl");
    this.conf = conf;
    Preconditions.checkState(numExecutors > 0,
        "Invalid number of executors: " + numExecutors + ". Must be > 0");
    this.localAddress = localAddress;

    this.queryTracker = new QueryTracker(conf, localDirsBase);
    this.executorService = new TaskExecutorService(numExecutors, waitQueueSize, enablePreemption);
    AuxiliaryServiceHelper.setServiceDataIntoEnv(
        TezConstants.TEZ_SHUFFLE_HANDLER_SERVICE_ID,
        ByteBuffer.allocate(4).putInt(localShufflePort), localEnv);

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

    LOG.info("ContainerRunnerImpl config: " +
            "memoryPerExecutorDerviced=" + memoryPerExecutor
    );
  }

  public void serviceInit(Configuration conf) {
    queryTracker.init(conf);
  }

  @Override
  public void serviceStart() {
    // The node id will only be available at this point, since the server has been started in LlapDaemon
    queryTracker.start();
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
    queryTracker.stop();
    super.serviceStop();
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
      Map<String, String> env = new HashMap<>();
      // TODO What else is required in this environment map.
      env.putAll(localEnv);
      env.put(ApplicationConstants.Environment.USER.name(), request.getUser());

      FragmentSpecProto fragmentSpec = request.getFragmentSpec();
      TezTaskAttemptID taskAttemptId = TezTaskAttemptID.fromString(
          fragmentSpec.getTaskAttemptIdString());
      int dagIdentifier = taskAttemptId.getTaskID().getVertexID().getDAGId().getId();

      QueryFragmentInfo fragmentInfo = queryTracker
          .registerFragment(null, request.getApplicationIdString(), fragmentSpec.getDagName(),
              dagIdentifier,
              fragmentSpec.getVertexName(), fragmentSpec.getFragmentNumber(),
              fragmentSpec.getAttemptNumber(), request.getUser(), request.getFragmentSpec());

      String []localDirs = fragmentInfo.getLocalDirs();
      Preconditions.checkNotNull(localDirs);

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
      ShuffleHandler.get()
          .registerDag(request.getApplicationIdString(), dagIdentifier, jobToken,
              request.getUser(), localDirs);

      TaskRunnerCallable callable = new TaskRunnerCallable(request, fragmentInfo, new Configuration(getConfig()),
          new ExecutionContextImpl(localAddress.get().getHostName()), env,
          credentials, memoryPerExecutor, amReporter, confParams, metrics, killedTaskHandler,
          this);
      try {
        executorService.schedule(callable);
      } catch (RejectedExecutionException e) {
        // Stop tracking the fragment and re-throw the error.
        fragmentComplete(fragmentInfo);
        throw e;
      }
      metrics.incrExecutorTotalRequestsHandled();
      metrics.incrExecutorNumQueuedRequests();
    } finally {
      NDC.pop();
    }
  }

  @Override
  public void sourceStateUpdated(SourceStateUpdatedRequestProto request) {
    LOG.info("Processing state update: " + stringifySourceStateUpdateRequest(request));
    queryTracker.registerSourceStateChange(request.getDagName(), request.getSrcName(),
        request.getState());
  }

  @Override
  public void queryComplete(QueryCompleteRequestProto request) {
    queryTracker.queryComplete(null, request.getDagName(), request.getDeleteDelay());
  }

  @Override
  public void terminateFragment(TerminateFragmentRequestProto request) {
    // TODO Implement when this gets used.
  }

  private String stringifySourceStateUpdateRequest(SourceStateUpdatedRequestProto request) {
    StringBuilder sb = new StringBuilder();
    sb.append("dagName=").append(request.getDagName())
        .append(", ").append("sourceName=").append(request.getSrcName())
        .append(", ").append("state=").append(request.getState());
    return sb.toString();
  }

  public static String stringifySubmitRequest(SubmitWorkRequestProto request) {
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

    FragmentRuntimeInfo fragmentRuntimeInfo = request.getFragmentRuntimeInfo();
    sb.append(", FragmentRuntimeInfo={");
    sb.append("taskCount=").append(fragmentRuntimeInfo.getNumSelfAndUpstreamTasks());
    sb.append(", completedTaskCount=").append(fragmentRuntimeInfo.getNumSelfAndUpstreamCompletedTasks());
    sb.append(", dagStartTime=").append(fragmentRuntimeInfo.getDagStartTime());
    sb.append(", firstAttemptStartTime=").append(fragmentRuntimeInfo.getFirstAttemptStartTime());
    sb.append("}");
    return sb.toString();
  }

  @Override
  public void fragmentComplete(QueryFragmentInfo fragmentInfo) {
    queryTracker.fragmentComplete(fragmentInfo);
  }

  private class KilledTaskHandlerImpl implements KilledTaskHandler {

    @Override
    public void taskKilled(String amLocation, int port, String user,
                           Token<JobTokenIdentifier> jobToken, TezTaskAttemptID taskAttemptId) {
      amReporter.taskKilled(amLocation, port, user, jobToken, taskAttemptId);
    }
  }
}
