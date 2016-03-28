/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapRecordReader.ReaderEvent;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.ext.LlapTaskUmbilicalExternalClient;
import org.apache.hadoop.hive.llap.ext.LlapTaskUmbilicalExternalClient.LlapTaskUmbilicalExternalResponder;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;


public class LlapInputFormat<V extends WritableComparable> implements InputFormat<NullWritable, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapInputFormat.class);


  public LlapInputFormat() {
  }

  /*
   * This proxy record reader has the duty of establishing a connected socket with LLAP, then fire
   * off the work in the split to LLAP and finally return the connected socket back in an
   * LlapRecordReader. The LlapRecordReader class reads the results from the socket.
   */
  @Override
  public RecordReader<NullWritable, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {

    LlapInputSplit llapSplit = (LlapInputSplit) split;
    SubmitWorkInfo submitWorkInfo = SubmitWorkInfo.fromBytes(llapSplit.getPlanBytes());

    ServiceInstance serviceInstance = getServiceInstance(job, llapSplit);
    String host = serviceInstance.getHost();
    int llapSubmitPort = serviceInstance.getRpcPort();

    LOG.info("Found service instance for host " + host + " with rpc port " + llapSubmitPort
        + " and outputformat port " + serviceInstance.getOutputFormatPort());

    LlapRecordReaderTaskUmbilicalExternalResponder umbilicalResponder =
        new LlapRecordReaderTaskUmbilicalExternalResponder();
    LlapTaskUmbilicalExternalClient llapClient =
      new LlapTaskUmbilicalExternalClient(job, submitWorkInfo.getTokenIdentifier(),
          submitWorkInfo.getToken(), umbilicalResponder);
    llapClient.init(job);
    llapClient.start();

    SubmitWorkRequestProto submitWorkRequestProto =
      constructSubmitWorkRequestProto(submitWorkInfo, llapSplit.getSplitNum(),
          llapClient.getAddress(), submitWorkInfo.getToken());

    TezEvent tezEvent = new TezEvent();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(llapSplit.getFragmentBytes(), 0, llapSplit.getFragmentBytes().length);
    tezEvent.readFields(dib);
    List<TezEvent> tezEventList = Lists.newArrayList();
    tezEventList.add(tezEvent);

    llapClient.submitWork(submitWorkRequestProto, host, llapSubmitPort, tezEventList);

    String id = HiveConf.getVar(job, HiveConf.ConfVars.HIVEQUERYID) + "_" + llapSplit.getSplitNum();

    HiveConf conf = new HiveConf();
    Socket socket = new Socket(host,
        serviceInstance.getOutputFormatPort());

    LOG.debug("Socket connected");

    socket.getOutputStream().write(id.getBytes());
    socket.getOutputStream().write(0);
    socket.getOutputStream().flush();

    LOG.info("Registered id: " + id);

    LlapRecordReader recordReader = new LlapRecordReader(socket.getInputStream(), llapSplit.getSchema(), Text.class);
    umbilicalResponder.setRecordReader(recordReader);
    return recordReader;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    throw new IOException("These are not the splits you are looking for.");
  }

  private ServiceInstance getServiceInstance(JobConf job, LlapInputSplit llapSplit) throws IOException {
    LlapRegistryService registryService = LlapRegistryService.getClient(job);
    String host = llapSplit.getLocations()[0];

    ServiceInstance serviceInstance = getServiceInstanceForHost(registryService, host);
    if (serviceInstance == null) {
      throw new IOException("No service instances found for " + host + " in registry");
    }

    return serviceInstance;
  }

  private ServiceInstance getServiceInstanceForHost(LlapRegistryService registryService, String host) throws IOException {
    InetAddress address = InetAddress.getByName(host);
    ServiceInstanceSet instanceSet = registryService.getInstances();
    ServiceInstance serviceInstance = null;

    // The name used in the service registry may not match the host name we're using.
    // Try hostname/canonical hostname/host address

    String name = address.getHostName();
    LOG.info("Searching service instance by hostname " + name);
    serviceInstance = selectServiceInstance(instanceSet.getByHost(name));
    if (serviceInstance != null) {
      return serviceInstance;
    }

    name = address.getCanonicalHostName();
    LOG.info("Searching service instance by canonical hostname " + name);
    serviceInstance = selectServiceInstance(instanceSet.getByHost(name));
    if (serviceInstance != null) {
      return serviceInstance;
    }

    name = address.getHostAddress();
    LOG.info("Searching service instance by address " + name);
    serviceInstance = selectServiceInstance(instanceSet.getByHost(name));
    if (serviceInstance != null) {
      return serviceInstance;
    }

    return serviceInstance;
  }

  private ServiceInstance selectServiceInstance(Set<ServiceInstance> serviceInstances) {
    if (serviceInstances == null || serviceInstances.isEmpty()) {
      return null;
    }

    // Get the first live service instance
    for (ServiceInstance serviceInstance : serviceInstances) {
      if (serviceInstance.isAlive()) {
        return serviceInstance;
      }
    }

    LOG.info("No live service instances were found");
    return null;
  }

  private SubmitWorkRequestProto constructSubmitWorkRequestProto(SubmitWorkInfo submitWorkInfo,
      int taskNum,
      InetSocketAddress address,
      Token<JobTokenIdentifier> token) throws
        IOException {
    TaskSpec taskSpec = submitWorkInfo.getTaskSpec();
    ApplicationId appId = submitWorkInfo.getFakeAppId();

    SubmitWorkRequestProto.Builder builder = SubmitWorkRequestProto.newBuilder();
    // This works, assuming the executor is running within YARN.
    LOG.info("Setting user in submitWorkRequest to: " +
        System.getenv(ApplicationConstants.Environment.USER.name()));
    builder.setUser(System.getenv(ApplicationConstants.Environment.USER.name()));
    builder.setApplicationIdString(appId.toString());
    builder.setAppAttemptNumber(0);
    builder.setTokenIdentifier(appId.toString());

    ContainerId containerId =
      ContainerId.newInstance(ApplicationAttemptId.newInstance(appId, 0), taskNum);
    builder.setContainerIdString(containerId.toString());

    builder.setAmHost(address.getHostName());
    builder.setAmPort(address.getPort());
    Credentials taskCredentials = new Credentials();
    // Credentials can change across DAGs. Ideally construct only once per DAG.
    // TODO Figure out where credentials will come from. Normally Hive sets up
    // URLs on the tez dag, for which Tez acquires credentials.

    //    taskCredentials.addAll(getContext().getCredentials());

    //    Preconditions.checkState(currentQueryIdentifierProto.getDagIdentifier() ==
    //        taskSpec.getTaskAttemptID().getTaskID().getVertexID().getDAGId().getId());
    //    ByteBuffer credentialsBinary = credentialMap.get(currentQueryIdentifierProto);
    //    if (credentialsBinary == null) {
    //      credentialsBinary = serializeCredentials(getContext().getCredentials());
    //      credentialMap.putIfAbsent(currentQueryIdentifierProto, credentialsBinary.duplicate());
    //    } else {
    //      credentialsBinary = credentialsBinary.duplicate();
    //    }
    //    builder.setCredentialsBinary(ByteString.copyFrom(credentialsBinary));
    Credentials credentials = new Credentials();
    TokenCache.setSessionToken(token, credentials);
    ByteBuffer credentialsBinary = serializeCredentials(credentials);
    builder.setCredentialsBinary(ByteString.copyFrom(credentialsBinary));


    builder.setFragmentSpec(Converters.convertTaskSpecToProto(taskSpec));

    FragmentRuntimeInfo.Builder runtimeInfo = FragmentRuntimeInfo.newBuilder();
    runtimeInfo.setCurrentAttemptStartTime(System.currentTimeMillis());
    runtimeInfo.setWithinDagPriority(0);
    runtimeInfo.setDagStartTime(submitWorkInfo.getCreationTime());
    runtimeInfo.setFirstAttemptStartTime(submitWorkInfo.getCreationTime());
    runtimeInfo.setNumSelfAndUpstreamTasks(taskSpec.getVertexParallelism());
    runtimeInfo.setNumSelfAndUpstreamCompletedTasks(0);


    builder.setUsingTezAm(false);
    builder.setFragmentRuntimeInfo(runtimeInfo.build());
    return builder.build();
  }

  private ByteBuffer serializeCredentials(Credentials credentials) throws IOException {
    Credentials containerCredentials = new Credentials();
    containerCredentials.addAll(credentials);
    DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
    containerCredentials.writeTokenStorageToStream(containerTokens_dob);
    return ByteBuffer.wrap(containerTokens_dob.getData(), 0, containerTokens_dob.getLength());
  }

  private static class LlapRecordReaderTaskUmbilicalExternalResponder implements LlapTaskUmbilicalExternalResponder {
    protected LlapRecordReader recordReader = null;
    protected LinkedBlockingQueue<ReaderEvent> queuedEvents = new LinkedBlockingQueue<ReaderEvent>();

    public LlapRecordReaderTaskUmbilicalExternalResponder() {
    }

    @Override
    public void submissionFailed(String fragmentId, Throwable throwable) {
      try {
        sendOrQueueEvent(LlapRecordReader.ReaderEvent.errorEvent(
            "Received submission failed event for fragment ID " + fragmentId));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    @Override
    public void heartbeat(TezHeartbeatRequest request) {
      TezTaskAttemptID taskAttemptId = request.getCurrentTaskAttemptID();
      List<TezEvent> inEvents = request.getEvents();
      for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
        EventType eventType = tezEvent.getEventType();
        try {
          switch (eventType) {
            case TASK_ATTEMPT_COMPLETED_EVENT:
              sendOrQueueEvent(LlapRecordReader.ReaderEvent.doneEvent());
              break;
            case TASK_ATTEMPT_FAILED_EVENT:
              TaskAttemptFailedEvent taskFailedEvent = (TaskAttemptFailedEvent) tezEvent.getEvent();
              sendOrQueueEvent(LlapRecordReader.ReaderEvent.errorEvent(taskFailedEvent.getDiagnostics()));
              break;
            case TASK_STATUS_UPDATE_EVENT:
              // If we want to handle counters
              break;
            default:
              LOG.warn("Unhandled event type " + eventType);
              break;
          }
        } catch (Exception err) {
          LOG.error("Error during heartbeat responder:", err);
        }
      }
    }

    @Override
    public void taskKilled(TezTaskAttemptID taskAttemptId) {
      try {
        sendOrQueueEvent(LlapRecordReader.ReaderEvent.errorEvent(
            "Received task killed event for task ID " + taskAttemptId));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    @Override
    public void heartbeatTimeout(String taskAttemptId) {
      try {
        sendOrQueueEvent(LlapRecordReader.ReaderEvent.errorEvent(
            "Timed out waiting for heartbeat for task ID " + taskAttemptId));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    public synchronized LlapRecordReader getRecordReader() {
      return recordReader;
    }

    public synchronized void setRecordReader(LlapRecordReader recordReader) {
      this.recordReader = recordReader;

      if (recordReader == null) {
        return;
      }

      // If any events were queued by the responder, give them to the record reader now.
      while (!queuedEvents.isEmpty()) {
        LlapRecordReader.ReaderEvent readerEvent = queuedEvents.poll();
        LOG.debug("Sending queued event to record reader: " + readerEvent.getEventType());
        recordReader.handleEvent(readerEvent);
      }
    }

    /**
     * Send the ReaderEvents to the record reader, if it is registered to this responder.
     * If there is no registered record reader, add them to a list of pending reader events
     * since we don't want to drop these events.
     * @param readerEvent
     */
    protected synchronized void sendOrQueueEvent(LlapRecordReader.ReaderEvent readerEvent) {
      LlapRecordReader recordReader = getRecordReader();
      if (recordReader != null) {
        recordReader.handleEvent(readerEvent);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No registered record reader, queueing event " + readerEvent.getEventType()
              + " with message " + readerEvent.getMessage());
        }

        try {
          queuedEvents.put(readerEvent);
        } catch (Exception err) {
          throw new RuntimeException("Unexpected exception while queueing reader event", err);
        }
      }
    }

    /**
     * Clear the list of queued reader events if we are not interested in sending any pending events to any registering record reader.
     */
    public void clearQueuedEvents() {
      queuedEvents.clear();
    }
  }
}
