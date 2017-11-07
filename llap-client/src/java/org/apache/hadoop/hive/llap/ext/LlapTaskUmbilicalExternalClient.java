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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol.TezAttemptArray;

import org.apache.hadoop.io.ArrayWritable;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.protocol.LlapTaskUmbilicalProtocol;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.llap.tez.LlapProtocolClientProxy;
import org.apache.hadoop.hive.llap.tezplugins.helpers.LlapTaskUmbilicalServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.apache.tez.runtime.api.impl.TezHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LlapTaskUmbilicalExternalClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(LlapTaskUmbilicalExternalClient.class);

  private static ScheduledThreadPoolExecutor retryExecutor = new ScheduledThreadPoolExecutor(1);

  private final Random rand = new Random();
  private final LlapProtocolClientProxy communicator;
  private volatile LlapTaskUmbilicalServer llapTaskUmbilicalServer;
  private final Configuration conf;

  protected final String tokenIdentifier;
  protected final Token<JobTokenIdentifier> sessionToken;
  private LlapTaskUmbilicalExternalResponder responder = null;
  private final long connectionTimeout;
  private long baseDelay;
  private int attemptNum = 0;
  private volatile boolean closed = false;
  private volatile boolean timeoutsDisabled = false;
  private RequestInfo requestInfo;
  List<TezEvent> tezEvents;

  // Using a shared instance of the umbilical server.
  private static class SharedUmbilicalServer {
    LlapTaskUmbilicalExternalImpl umbilicalProtocol;
    LlapTaskUmbilicalServer llapTaskUmbilicalServer;

    private volatile static SharedUmbilicalServer instance;
    private static final Object lock = new Object();

    static SharedUmbilicalServer getInstance(Configuration conf) {
      SharedUmbilicalServer value = instance;
      if (value == null) {
        synchronized (lock) {
          if (instance == null) {
            instance = new SharedUmbilicalServer(conf);
          }
          value = instance;
        }
      }
      return value;
    }

    private SharedUmbilicalServer(Configuration conf) {
      try {
        umbilicalProtocol = new LlapTaskUmbilicalExternalImpl(conf);
        llapTaskUmbilicalServer = new LlapTaskUmbilicalServer(conf, umbilicalProtocol, 1);
      } catch (Exception err) {
        throw new ExceptionInInitializerError(err);
      }
    }
  }

  private enum RequestState {
    PENDING, RUNNING
  };

  private static class RequestInfo {
    RequestState state;
    final SubmitWorkRequestProto request;
    final QueryIdentifierProto queryIdentifierProto;
    final String taskAttemptId;
    final String hostname;
    String uniqueNodeId;
    final int port;
    final AtomicLong lastHeartbeat = new AtomicLong();

    public RequestInfo(SubmitWorkRequestProto request, QueryIdentifierProto queryIdentifierProto,
        String taskAttemptId, String hostname, int port) {
      this.state = RequestState.PENDING;
      this.request = request;
      this.queryIdentifierProto = queryIdentifierProto;
      this.taskAttemptId = taskAttemptId;
      this.hostname = hostname;
      this.port = port;
      this.lastHeartbeat.set(System.currentTimeMillis());
    }
  }

  public LlapTaskUmbilicalExternalClient(Configuration conf, String tokenIdentifier,
      Token<JobTokenIdentifier> sessionToken, LlapTaskUmbilicalExternalResponder responder,
      Token<LlapTokenIdentifier> llapToken) {
    this.conf = conf;
    this.tokenIdentifier = tokenIdentifier;
    this.sessionToken = sessionToken;
    this.responder = responder;
    this.connectionTimeout = 3 * HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    this.baseDelay = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_SLEEP_BETWEEN_RETRIES_MS,
        TimeUnit.MILLISECONDS);
    // Add support for configurable threads, however 1 should always be enough.
    this.communicator = new LlapProtocolClientProxy(1, conf, llapToken);
    this.communicator.init(conf);
  }

  private void terminateRequest() {
    if (closed || requestInfo == null) {
      LOG.warn("No current request to terminate");
      return;
    }

    TerminateFragmentRequestProto.Builder builder = TerminateFragmentRequestProto.newBuilder();
    builder.setQueryIdentifier(requestInfo.queryIdentifierProto);
    builder.setFragmentIdentifierString(requestInfo.taskAttemptId);

    final String taskAttemptId = requestInfo.taskAttemptId;
    communicator.sendTerminateFragment(builder.build(), requestInfo.hostname, requestInfo.port,
        new LlapProtocolClientProxy.ExecuteRequestCallback<TerminateFragmentResponseProto>() {

      @Override
      public void setResponse(TerminateFragmentResponseProto response) {
        LOG.debug("Received terminate response for " + taskAttemptId);
      }

      @Override
      public void indicateError(Throwable t) {
        String msg = "Failed to terminate " + taskAttemptId;
        LOG.error(msg, t);
        // Don't propagate the error - termination was done as part of closing the client.
      }
    });
  }

  public InetSocketAddress getAddress() {
    return SharedUmbilicalServer.getInstance(conf).llapTaskUmbilicalServer.getAddress();
  }


  /**
   * Submit the work for actual execution.
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

    this.requestInfo = new RequestInfo(request, queryIdentifierProto, fragmentId, llapHost, llapPort);

    this.tezEvents = Lists.<TezEvent>newArrayList();
    registerClient();

    // Send out the actual SubmitWorkRequest
    final LlapTaskUmbilicalExternalClient client = this;
    communicator.start();
    submitWork();
  }

  private void submitWork() {
    if (!closed) {
      communicator.sendSubmitWork(requestInfo.request,
          requestInfo.hostname, requestInfo.port, new SubmitWorkCallback(this));
    }
  }

  private void retrySubmission() {
    attemptNum++;

    // Don't retry immediately - use delay with exponential backoff
    long retryDelay = determineRetryDelay();
    LOG.info("Queueing fragment for resubmission {}, attempt {}, delay {}",
        this.requestInfo.taskAttemptId, attemptNum, retryDelay);
    disableTimeouts();  // Don't timeout because of retry delay
    retryExecutor.schedule(
        new Runnable() {
          @Override
          public void run() {
            // Re-enable timeouts
            enableTimeouts();
            submitWork();
          }
        },
        retryDelay,
        TimeUnit.MILLISECONDS);
  }

  // Helper class to submit fragments to LLAP and retry rejected submissions.
  static class SubmitWorkCallback implements LlapProtocolClientProxy.ExecuteRequestCallback<SubmitWorkResponseProto> {
    private LlapTaskUmbilicalExternalClient client;

    public SubmitWorkCallback(LlapTaskUmbilicalExternalClient client) {
      this.client = client;
    }

    @Override
    public void setResponse(SubmitWorkResponseProto response) {
      if (response.hasSubmissionState()) {
        if (response.getSubmissionState().equals(SubmissionStateProto.REJECTED)) {
          String fragmentId = this.client.requestInfo.taskAttemptId;
          String msg = "Fragment: " + fragmentId + " rejected. Server Busy.";
          LOG.info(msg);

          // taskKill() should also be received during a rejected submission,
          // we will let that logic handle retries.

          return;
        }
      }
      if (response.hasUniqueNodeId()) {
        client.requestInfo.uniqueNodeId = response.getUniqueNodeId();
      }
    }

    @Override
    public void indicateError(Throwable t) {
      String fragmentId = this.client.requestInfo.taskAttemptId;
      String msg = "Failed to submit: " + fragmentId;
      LOG.error(msg, t);
      Throwable err = new RuntimeException(msg, t);
      client.unregisterClient();
      client.responder.submissionFailed(fragmentId, err);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      terminateRequest();
      unregisterClient();
    }
  }

  private void registerClient() {
    SharedUmbilicalServer umbilicalServer = SharedUmbilicalServer.getInstance(conf);
    LlapTaskUmbilicalExternalClient prevVal =
        umbilicalServer.umbilicalProtocol.registeredClients.putIfAbsent(requestInfo.taskAttemptId, this);
    if (prevVal != null) {
      LOG.warn("Unexpected - fragment " + requestInfo.taskAttemptId + " is already registered!");
    }
    umbilicalServer.llapTaskUmbilicalServer.addTokenForJob(tokenIdentifier, sessionToken);
  }

  private void unregisterClient() {
    if (!closed && requestInfo != null) {
      communicator.stop();
      SharedUmbilicalServer umbilicalServer = SharedUmbilicalServer.getInstance(conf);
      umbilicalServer.umbilicalProtocol.unregisterClient(requestInfo.taskAttemptId);
      umbilicalServer.llapTaskUmbilicalServer.removeTokenForJob(tokenIdentifier);
      closed = true;
    }
  }

  long getLastHeartbeat() {
    return this.requestInfo.lastHeartbeat.get();
  }

  void setLastHeartbeat(long lastHeartbeat) {
    this.requestInfo.lastHeartbeat.set(lastHeartbeat);
  }

  private boolean isTimedOut(long currentTime) {
    if (timeoutsDisabled) {
      return false;
    }
    return (currentTime - getLastHeartbeat() >= connectionTimeout);
  }

  private void enableTimeouts() {
    setLastHeartbeat(System.currentTimeMillis());
    timeoutsDisabled = false;
  }

  private void disableTimeouts() {
    timeoutsDisabled = true;
  }

  private long determineRetryDelay() {
    // Delay with exponential backoff
    int maxDelay = (int) Math.min(baseDelay * Math.pow(2, attemptNum), 60000);
    long retryDelay = rand.nextInt(maxDelay);
    return retryDelay;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LlapTaskUmbilicalExternalClient");
    if (requestInfo != null) {
      sb.append("(");
      sb.append(requestInfo.taskAttemptId);
      sb.append(")");
    }
    return sb.toString();
  }

  // Periodic task to time out submitted tasks that have not been updated with umbilical heartbeat.
  private static class HeartbeatCheckTask implements Runnable {
    LlapTaskUmbilicalExternalImpl umbilicalImpl;

    public HeartbeatCheckTask(LlapTaskUmbilicalExternalImpl umbilicalImpl) {
      this.umbilicalImpl = umbilicalImpl;
    }

    public void run() {
      long currentTime = System.currentTimeMillis();
      List<LlapTaskUmbilicalExternalClient> timedOutTasks = new ArrayList<LlapTaskUmbilicalExternalClient>();

      for (Map.Entry<String, LlapTaskUmbilicalExternalClient> entry : umbilicalImpl.registeredClients.entrySet()) {
        LlapTaskUmbilicalExternalClient client = entry.getValue();
        if (client.isTimedOut(currentTime)) {
          timedOutTasks.add(client);
        }
      }

      for (LlapTaskUmbilicalExternalClient timedOutTask : timedOutTasks) {
        String taskAttemptId = timedOutTask.requestInfo.taskAttemptId;
        LOG.info("Running taskAttemptId " + taskAttemptId + " timed out");
        timedOutTask.unregisterClient();
        timedOutTask.responder.heartbeatTimeout(taskAttemptId);
      }
    }
  }

  public interface LlapTaskUmbilicalExternalResponder {
    void submissionFailed(String fragmentId, Throwable throwable);
    void heartbeat(TezHeartbeatRequest request);
    void taskKilled(TezTaskAttemptID taskAttemptId);
    void heartbeatTimeout(String fragmentId);
  }

  private static class LlapTaskUmbilicalExternalImpl implements LlapTaskUmbilicalProtocol {

    final ConcurrentMap<String, LlapTaskUmbilicalExternalClient> registeredClients = new ConcurrentHashMap<>();
    private final ScheduledThreadPoolExecutor timer;

    public LlapTaskUmbilicalExternalImpl(Configuration conf) {
      long taskInterval = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.LLAP_DAEMON_AM_LIVENESS_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      // Setup timer task to check for hearbeat timeouts
      this.timer = new ScheduledThreadPoolExecutor(1);
      timer.scheduleAtFixedRate(new HeartbeatCheckTask(this),
          taskInterval, taskInterval, TimeUnit.MILLISECONDS);
    }

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
      LlapTaskUmbilicalExternalClient client = registeredClients.get(taskAttemptIdString);
      if (client == null) {
        // Heartbeat is from a task that we are not currently tracking.
        LOG.info("Unexpected heartbeat from " + taskAttemptIdString);
        response.setShouldDie(); // Do any of the other fields need to be set?
        return response;
      }

      if (client.requestInfo.state == RequestState.PENDING) {
        client.requestInfo.state = RequestState.RUNNING;
        tezEvents = client.tezEvents;
      } else {
        tezEvents = Collections.emptyList();
      }

      boolean shouldUnregisterClient = false;

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
            shouldUnregisterClient = true;
            break;
          case TASK_ATTEMPT_FAILED_EVENT:
            LOG.debug("Task failed event for " + taskAttemptIdString);
            shouldUnregisterClient = true;
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

      if (shouldUnregisterClient) {
        client.unregisterClient();
      }

      // Pass the request on to the responder
      try {
        if (client.responder != null) {
          client.responder.heartbeat(request);
        }
      } catch (Exception err) {
        LOG.error("Error during responder execution", err);
      }

      return response;
    }

    @Override
    public void nodeHeartbeat(
        Text hostname, Text uniqueId, int port, TezAttemptArray aw) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node heartbeat from " + hostname + ":" + port + ", " + uniqueId);
      }
      updateHeartbeatInfo(hostname.toString(), uniqueId.toString(), port, aw);
      // No need to propagate to this to the responder
    }

    @Override
    public void taskKilled(TezTaskAttemptID taskAttemptId) throws IOException {
      String taskAttemptIdString = taskAttemptId.toString();
      LlapTaskUmbilicalExternalClient client = registeredClients.get(taskAttemptIdString);
      if (client != null) {
        if (client.requestInfo.state == RequestState.PENDING) {
          // A task kill while the request is still in PENDING state means the request should be retried.
          LOG.info("Received task kill for {} which is still in pending state. Retry submission.", taskAttemptIdString);
          client.retrySubmission();
        } else {
          try {
            LOG.error("Task killed - " + taskAttemptIdString);
            client.unregisterClient();
            if (client.responder != null) {
              client.responder.taskKilled(taskAttemptId);
            }
          } catch (Exception err) {
            LOG.error("Error during responder execution", err);
          }
        }
      } else {
        LOG.info("Received task killed notification for task which is not currently being tracked: " + taskAttemptId);
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

    private void unregisterClient(String taskAttemptId) {
      registeredClients.remove(taskAttemptId);
    }

    private void updateHeartbeatInfo(String taskAttemptId) {
      int updateCount = 0;

      LlapTaskUmbilicalExternalClient registeredClient = registeredClients.get(taskAttemptId);
      if (registeredClient != null) {
        registeredClient.setLastHeartbeat(System.currentTimeMillis());
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
      for (Map.Entry<String, LlapTaskUmbilicalExternalClient> entry : registeredClients.entrySet()) {
        LlapTaskUmbilicalExternalClient registeredClient = entry.getValue();
        if (doesClientMatchHeartbeat(registeredClient, hostname, uniqueId, port)) {
          TezTaskAttemptID ta = TezTaskAttemptID.fromString(registeredClient.requestInfo.taskAttemptId);
          if (attempts.contains(ta)) {
            registeredClient.setLastHeartbeat(System.currentTimeMillis());
            updateCount++;
          } else {
            error += (registeredClient.requestInfo.taskAttemptId + ", ");
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

    private static boolean doesClientMatchHeartbeat(LlapTaskUmbilicalExternalClient client,
        String hostname, String uniqueId, int port) {
      return (hostname.equals(client.requestInfo.hostname)
          && port == client.requestInfo.port
          && uniqueId.equals(client.requestInfo.uniqueNodeId));
    }
  }
}
