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
package org.apache.hadoop.hive.llap.ext;

import org.apache.hadoop.io.Writable;

import java.util.HashSet;

import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol.TezAttemptArray;

import org.apache.hadoop.io.ArrayWritable;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmissionStateProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.helpers.LlapTaskUmbilicalServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LlapTaskUmbilicalExternalClient extends AbstractService implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskUmbilicalExternalClient.class);

  private final LlapProtocolClientProxy communicator;
  private volatile LlapTaskUmbilicalServer llapTaskUmbilicalServer;
  private final Configuration conf;
  private final LlapTaskUmbilicalProtocol umbilical;

  protected final String tokenIdentifier;
  protected final Token<JobTokenIdentifier> sessionToken;

  private final ConcurrentMap<String, PendingEventData> pendingEvents = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, TaskHeartbeatInfo> registeredTasks= new ConcurrentHashMap<String, TaskHeartbeatInfo>();
  private LlapTaskUmbilicalExternalResponder responder = null;
  private final ScheduledThreadPoolExecutor timer;
  private final long connectionTimeout;

  private static class TaskHeartbeatInfo {
    final String taskAttemptId;
    final String hostname;
    String uniqueNodeId;
    final int port;
    final AtomicLong lastHeartbeat = new AtomicLong();

    public TaskHeartbeatInfo(String taskAttemptId, String hostname, int port) {
      this.taskAttemptId = taskAttemptId;
      this.hostname = hostname;
      this.port = port;
      this.lastHeartbeat.set(System.currentTimeMillis());
    }
  }

  private static class PendingEventData {
    final TaskHeartbeatInfo heartbeatInfo;
    final List<TezEvent> tezEvents;

    public PendingEventData(TaskHeartbeatInfo heartbeatInfo, List<TezEvent> tezEvents) {
      this.heartbeatInfo = heartbeatInfo;
      this.tezEvents = tezEvents;
    }
  }

  public LlapTaskUmbilicalExternalClient(Configuration conf, String tokenIdentifier,
      Token<JobTokenIdentifier> sessionToken, LlapTaskUmbilicalExternalResponder responder,
      Token<LlapTokenIdentifier> llapToken) {
    super(LlapTaskUmbilicalExternalClient.class.getName());
    this.conf = conf;
    this.umbilical = new LlapTaskUmbilicalExternalImpl();
    this.tokenIdentifier = tokenIdentifier;
    this.sessionToken = sessionToken;
    this.responder = responder;
    this.timer = new ScheduledThreadPoolExecutor(1);
    this.connectionTimeout = 3 * HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    // Add support for configurable threads, however 1 should always be enough.
    this.communicator = new LlapProtocolClientProxy(1, conf, llapToken);
    this.communicator.init(conf);
  }

  @Override
  public void serviceStart() throws IOException {
    // If we use a single server for multiple external clients, then consider using more than one handler.
    int numHandlers = 1;
    llapTaskUmbilicalServer = new LlapTaskUmbilicalServer(conf, umbilical, numHandlers, tokenIdentifier, sessionToken);
    communicator.start();
  }

  @Override
  public void serviceStop() {
    llapTaskUmbilicalServer.shutdownServer();
    timer.shutdown();
    if (this.communicator != null) {
      this.communicator.stop();
    }
  }

  public InetSocketAddress getAddress() {
    return llapTaskUmbilicalServer.getAddress();
  }


  /**
   * Submit the work for actual execution.
   * @throws InvalidProtocolBufferException 
   */
  public void submitWork(SubmitWorkRequestProto request, String llapHost, int llapPort) {
    // Register the pending events to be sent for this spec.
    VertexOrBinary vob = request.getWorkSpec();
    assert vob.hasVertexBinary() != vob.hasVertex();
    SignableVertexSpec vertex = null;
    try {
      vertex = vob.hasVertex() ? vob.getVertex()
          : SignableVertexSpec.parseFrom(vob.getVertexBinary());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    QueryIdentifierProto queryIdentifierProto = vertex.getQueryIdentifier();
    TezTaskAttemptID attemptId = Converters.createTaskAttemptId(queryIdentifierProto,
        vertex.getVertexIndex(), request.getFragmentNumber(), request.getAttemptNumber());
    final String fragmentId = attemptId.toString();

    final TaskHeartbeatInfo thi = new TaskHeartbeatInfo(fragmentId, llapHost, llapPort);
    pendingEvents.putIfAbsent(
        fragmentId, new PendingEventData(thi, Lists.<TezEvent>newArrayList()));

    // Setup timer task to check for hearbeat timeouts
    timer.scheduleAtFixedRate(new HeartbeatCheckTask(),
        connectionTimeout, connectionTimeout, TimeUnit.MILLISECONDS);

    // Send out the actual SubmitWorkRequest
    communicator.sendSubmitWork(request, llapHost, llapPort,
        new LlapProtocolClientProxy.ExecuteRequestCallback<SubmitWorkResponseProto>() {

          @Override
          public void setResponse(SubmitWorkResponseProto response) {
            if (response.hasSubmissionState()) {
              if (response.getSubmissionState().equals(SubmissionStateProto.REJECTED)) {
                String msg = "Fragment: " + fragmentId + " rejected. Server Busy.";
                LOG.info(msg);
                if (responder != null) {
                  Throwable err = new RuntimeException(msg);
                  responder.submissionFailed(fragmentId, err);
                }
                return;
              }
            }
            if (response.hasUniqueNodeId()) {
              thi.uniqueNodeId = response.getUniqueNodeId();
            }
          }

          @Override
          public void indicateError(Throwable t) {
            String msg = "Failed to submit: " + fragmentId;
            LOG.error(msg, t);
            Throwable err = new RuntimeException(msg, t);
            responder.submissionFailed(fragmentId, err);
          }
        });
  }

  private void updateHeartbeatInfo(String taskAttemptId) {
    int updateCount = 0;

    PendingEventData pendingEventData = pendingEvents.get(taskAttemptId);
    if (pendingEventData != null) {
      pendingEventData.heartbeatInfo.lastHeartbeat.set(System.currentTimeMillis());
      updateCount++;
    }

    TaskHeartbeatInfo heartbeatInfo = registeredTasks.get(taskAttemptId);
    if (heartbeatInfo != null) {
      heartbeatInfo.lastHeartbeat.set(System.currentTimeMillis());
      updateCount++;
    }

    if (updateCount == 0) {
      LOG.warn("No tasks found for heartbeat from taskAttemptId " + taskAttemptId);
    }
  }

  private void updateHeartbeatInfo(
      String hostname, String uniqueId, int port, TezAttemptArray tasks) {
    int updateCount = 0;
    HashSet<TezTaskAttemptID> attempts = new HashSet<>();
    for (Writable w : tasks.get()) {
      attempts.add((TezTaskAttemptID)w);
    }

    String error = "";
    for (String key : pendingEvents.keySet()) {
      PendingEventData pendingEventData = pendingEvents.get(key);
      if (pendingEventData != null) {
        TaskHeartbeatInfo thi = pendingEventData.heartbeatInfo;
        String thiUniqueId = thi.uniqueNodeId;
        if (thi.hostname.equals(hostname) && thi.port == port
            && (thiUniqueId != null && thiUniqueId.equals(uniqueId))) {
          TezTaskAttemptID ta = TezTaskAttemptID.fromString(thi.taskAttemptId);
          if (attempts.contains(ta)) {
            thi.lastHeartbeat.set(System.currentTimeMillis());
            updateCount++;
          } else {
            error += (thi.taskAttemptId + ", ");
          }
        }
      }
    }

    for (String key : registeredTasks.keySet()) {
      TaskHeartbeatInfo thi = registeredTasks.get(key);
      if (thi != null) {
        String thiUniqueId = thi.uniqueNodeId;
        if (thi.hostname.equals(hostname) && thi.port == port
            && (thiUniqueId != null && thiUniqueId.equals(uniqueId))) {
          TezTaskAttemptID ta = TezTaskAttemptID.fromString(thi.taskAttemptId);
          if (attempts.contains(ta)) {
            thi.lastHeartbeat.set(System.currentTimeMillis());
            updateCount++;
          } else {
            error += (thi.taskAttemptId + ", ");
          }
        }
      }
    }
    if (!error.isEmpty()) {
      LOG.info("The tasks we expected to be on the node are not there: " + error);
    }

    if (updateCount == 0) {
      LOG.info("No tasks found for heartbeat from hostname " + hostname + ", port " + port);
    }
  }

  private class HeartbeatCheckTask implements Runnable {
    public void run() {
      long currentTime = System.currentTimeMillis();
      List<String> timedOutTasks = new ArrayList<String>();

      // Check both pending and registered tasks for timeouts
      for (String key : pendingEvents.keySet()) {
        PendingEventData pendingEventData = pendingEvents.get(key);
        if (pendingEventData != null) {
          if (currentTime - pendingEventData.heartbeatInfo.lastHeartbeat.get() >= connectionTimeout) {
            timedOutTasks.add(key);
          }
        }
      }
      for (String timedOutTask : timedOutTasks) {
        LOG.info("Pending taskAttemptId " + timedOutTask + " timed out");
        responder.heartbeatTimeout(timedOutTask);
        pendingEvents.remove(timedOutTask);
      }

      timedOutTasks.clear();
      for (String key : registeredTasks.keySet()) {
        TaskHeartbeatInfo heartbeatInfo = registeredTasks.get(key);
        if (heartbeatInfo != null) {
          if (currentTime - heartbeatInfo.lastHeartbeat.get() >= connectionTimeout) {
            timedOutTasks.add(key);
          }
        }
      }
      for (String timedOutTask : timedOutTasks) {
        LOG.info("Running taskAttemptId " + timedOutTask + " timed out");
        responder.heartbeatTimeout(timedOutTask);
        registeredTasks.remove(timedOutTask);
      }
    }
  }

  public interface LlapTaskUmbilicalExternalResponder {
    void submissionFailed(String fragmentId, Throwable throwable);
    void heartbeat(TezHeartbeatRequest request);
    void taskKilled(TezTaskAttemptID taskAttemptId);
    void heartbeatTimeout(String fragmentId);
  }



  // Ideally, the server should be shared across all client sessions running on the same node.
  private class LlapTaskUmbilicalExternalImpl implements  LlapTaskUmbilicalProtocol {

    @Override
    public boolean canCommit(TezTaskAttemptID taskid) throws IOException {
      // Expecting only a single instance of a task to be running.
      return true;
    }

    @Override
    public TezHeartbeatResponse heartbeat(TezHeartbeatRequest request) throws IOException,
        TezException {
      // Keep-alive information. The client should be informed and will have to take care of re-submitting the work.
      // Some parts of fault tolerance go here.

      // This also provides completion information, and a possible notification when task actually starts running (first heartbeat)

      if (LOG.isDebugEnabled()) {
        LOG.debug("Received heartbeat from container, request=" + request);
      }

      // Incoming events can be ignored until the point when shuffle needs to be handled, instead of just scans.
      TezHeartbeatResponse response = new TezHeartbeatResponse();

      response.setLastRequestId(request.getRequestId());
      // Assuming TaskAttemptId and FragmentIdentifierString are the same. Verify this.
      TezTaskAttemptID taskAttemptId = request.getCurrentTaskAttemptID();
      String taskAttemptIdString = taskAttemptId.toString();

      updateHeartbeatInfo(taskAttemptIdString);

      List<TezEvent> tezEvents = null;
      PendingEventData pendingEventData = pendingEvents.remove(taskAttemptIdString);
      if (pendingEventData == null) {
        tezEvents = Collections.emptyList();

        // If this heartbeat was not from a pending event and it's not in our list of registered tasks,
        if (!registeredTasks.containsKey(taskAttemptIdString)) {
          LOG.info("Unexpected heartbeat from " + taskAttemptIdString);
          response.setShouldDie(); // Do any of the other fields need to be set?
          return response;
        }
      } else {
        tezEvents = pendingEventData.tezEvents;
        // Tasks removed from the pending list should then be added to the registered list.
        registeredTasks.put(taskAttemptIdString, pendingEventData.heartbeatInfo);
      }

      response.setLastRequestId(request.getRequestId());
      // Irrelevant from eventIds. This can be tracked in the AM itself, instead of polluting the task.
      // Also since we have all the MRInput events here - they'll all be sent in together.
      response.setNextFromEventId(0); // Irrelevant. See comment above.
      response.setNextPreRoutedEventId(0); //Irrelevant. See comment above.
      response.setEvents(tezEvents);

      List<TezEvent> inEvents = request.getEvents();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Heartbeat from " + taskAttemptIdString +
            " events: " + (inEvents != null ? inEvents.size() : -1));
      }
      for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
        EventType eventType = tezEvent.getEventType();
        switch (eventType) {
          case TASK_ATTEMPT_COMPLETED_EVENT:
            LOG.debug("Task completed event for " + taskAttemptIdString);
            registeredTasks.remove(taskAttemptIdString);
            break;
          case TASK_ATTEMPT_FAILED_EVENT:
            LOG.debug("Task failed event for " + taskAttemptIdString);
            registeredTasks.remove(taskAttemptIdString);
            break;
          case TASK_STATUS_UPDATE_EVENT:
            // If we want to handle counters
            LOG.debug("Task update event for " + taskAttemptIdString);
            break;
          default:
            LOG.warn("Unhandled event type " + eventType);
            break;
        }
      }

      // Pass the request on to the responder
      try {
        if (responder != null) {
          responder.heartbeat(request);
        }
      } catch (Exception err) {
        LOG.error("Error during responder execution", err);
      }

      return response;
    }

    @Override
    public void nodeHeartbeat(
        Text hostname, Text uniqueId, int port, TezAttemptArray aw) throws IOException {
      updateHeartbeatInfo(hostname.toString(), uniqueId.toString(), port, aw);
      // No need to propagate to this to the responder
    }

    @Override
    public void taskKilled(TezTaskAttemptID taskAttemptId) throws IOException {
      String taskAttemptIdString = taskAttemptId.toString();
      LOG.error("Task killed - " + taskAttemptIdString);
      registeredTasks.remove(taskAttemptIdString);

      try {
        if (responder != null) {
          responder.taskKilled(taskAttemptId);
        }
      } catch (Exception err) {
        LOG.error("Error during responder execution", err);
      }
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                  int clientMethodsHash) throws IOException {
      return ProtocolSignature.getProtocolSignature(this, protocol,
          clientVersion, clientMethodsHash);
    }
  }

}
