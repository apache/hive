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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapBaseRecordReader.ReaderEvent;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.LlapOutputSocketInitMessage;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.hive.llap.ext.LlapTaskUmbilicalExternalClient;
import org.apache.hadoop.hive.llap.ext.LlapTaskUmbilicalExternalClient.LlapTaskUmbilicalExternalResponder;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezHeartbeatRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;


/**
 * Base LLAP input format to handle requesting of splits and communication with LLAP daemon.
 */
public class LlapBaseInputFormat<V extends WritableComparable<?>>
  implements InputFormat<NullWritable, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapBaseInputFormat.class);

  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final Object lock = new Object();
  private static final Map<String, List<Connection>> connectionMap = new HashMap<String, List<Connection>>();

  private String url;  // "jdbc:hive2://localhost:10000/default"
  private String user; // "hive",
  private String pwd;  // ""
  private String query;
  private final Random rand = new Random();

  public static final String URL_KEY = "llap.if.hs2.connection";
  public static final String QUERY_KEY = "llap.if.query";
  public static final String USER_KEY = "llap.if.user";
  public static final String PWD_KEY = "llap.if.pwd";
  public static final String HANDLE_ID = "llap.if.handleid";
  public static final String DB_KEY = "llap.if.database";

  public final String SPLIT_QUERY = "select get_splits(\"%s\",%d)";
  public static final LlapServiceInstance[] serviceInstanceArray = new LlapServiceInstance[0];

  public LlapBaseInputFormat(String url, String user, String pwd, String query) {
    this.url = url;
    this.user = user;
    this.pwd = pwd;
    this.query = query;
  }

  public LlapBaseInputFormat() {}


  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<NullWritable, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

    LlapInputSplit llapSplit = (LlapInputSplit) split;

    // Set conf to use LLAP user rather than current user for LLAP Zk registry.
    HiveConf.setVar(job, HiveConf.ConfVars.LLAP_ZK_REGISTRY_USER, llapSplit.getLlapUser());
    SubmitWorkInfo submitWorkInfo = SubmitWorkInfo.fromBytes(llapSplit.getPlanBytes());

    LlapServiceInstance serviceInstance = getServiceInstance(job, llapSplit);
    String host = serviceInstance.getHost();
    int llapSubmitPort = serviceInstance.getRpcPort();

    LOG.info("Found service instance for host " + host + " with rpc port " + llapSubmitPort
        + " and outputformat port " + serviceInstance.getOutputFormatPort());

    byte[] llapTokenBytes = llapSplit.getTokenBytes();
    Token<LlapTokenIdentifier> llapToken = null;
    if (llapTokenBytes != null) {
      DataInputBuffer in = new DataInputBuffer();
      in.reset(llapTokenBytes, 0, llapTokenBytes.length);
      llapToken = new Token<LlapTokenIdentifier>();
      llapToken.readFields(in);
    }

    LlapRecordReaderTaskUmbilicalExternalResponder umbilicalResponder =
        new LlapRecordReaderTaskUmbilicalExternalResponder();
    LlapTaskUmbilicalExternalClient llapClient =
      new LlapTaskUmbilicalExternalClient(job, submitWorkInfo.getTokenIdentifier(),
          submitWorkInfo.getToken(), umbilicalResponder, llapToken);

    int attemptNum = 0;
    // Use task attempt number from conf if provided
    TaskAttemptID taskAttemptId = TaskAttemptID.forName(job.get(MRJobConfig.TASK_ATTEMPT_ID));
    if (taskAttemptId != null) {
      attemptNum = taskAttemptId.getId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting attempt number to " + attemptNum + " from task attempt ID in conf: " +
            job.get(MRJobConfig.TASK_ATTEMPT_ID));
      }
    }

    SubmitWorkRequestProto request = constructSubmitWorkRequestProto(
        submitWorkInfo, llapSplit.getSplitNum(), attemptNum, llapClient.getAddress(),
        submitWorkInfo.getToken(), llapSplit.getFragmentBytes(),
        llapSplit.getFragmentBytesSignature(), job);
    llapClient.submitWork(request, host, llapSubmitPort);

    Socket socket = new Socket(host, serviceInstance.getOutputFormatPort());

    LOG.debug("Socket connected");
    SignableVertexSpec vertex = SignableVertexSpec.parseFrom(submitWorkInfo.getVertexBinary());

    String fragmentId =
        Converters.createTaskAttemptId(vertex.getQueryIdentifier(), vertex.getVertexIndex(),
            request.getFragmentNumber(), request.getAttemptNumber()).toString();
    OutputStream socketStream = socket.getOutputStream();
    LlapOutputSocketInitMessage.Builder builder =
        LlapOutputSocketInitMessage.newBuilder().setFragmentId(fragmentId);
    if (llapSplit.getTokenBytes() != null) {
      builder.setToken(ByteString.copyFrom(llapSplit.getTokenBytes()));
    }
    builder.build().writeDelimitedTo(socketStream);
    socketStream.flush();

    LOG.info("Registered id: " + fragmentId);

    @SuppressWarnings("rawtypes")
    LlapBaseRecordReader recordReader = new LlapBaseRecordReader(socket.getInputStream(),
        llapSplit.getSchema(), Text.class, job, llapClient, (java.io.Closeable)socket);
    umbilicalResponder.setRecordReader(recordReader);
    return recordReader;
  }

  /**
   * Calling getSplits() will open a HiveServer2 connection which should be closed by the calling application
   * using LlapBaseInputFormat.close() when the application is done with the splits.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<InputSplit> ins = new ArrayList<InputSplit>();

    if (url == null) url = job.get(URL_KEY);
    if (query == null) query = job.get(QUERY_KEY);
    if (user == null) user = job.get(USER_KEY);
    if (pwd == null) pwd = job.get(PWD_KEY);
    String database = job.get(DB_KEY);

    if (url == null || query == null) {
      throw new IllegalStateException();
    }

    String handleId = job.get(HANDLE_ID);
    if (handleId == null) {
      handleId = UUID.randomUUID().toString();
      LOG.info("Handle ID not specified - generated handle ID {}", handleId);
    }

    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    LOG.info("Handle ID {}: query={}", handleId, query);
    String escapedQuery = StringUtils.escapeString(query, ESCAPE_CHAR, escapedChars);
    String sql = String.format(SPLIT_QUERY, escapedQuery, numSplits);
    try {
      Connection conn = DriverManager.getConnection(url,user,pwd);
      try (
        Statement stmt = conn.createStatement();
      ) {
        if (database != null && !database.isEmpty()) {
          stmt.execute("USE " + database);
        }
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
          // deserialize split
          DataInput in = new DataInputStream(res.getBinaryStream(1));
          InputSplitWithLocationInfo is = new LlapInputSplit();
          is.readFields(in);
          ins.add(is);
        }
        res.close();
      } catch (Exception e) {
        LOG.error("Closing connection due to error", e);
        conn.close();
        throw e;
      }

      // Keep connection open to hang on to associated resources (temp tables, locks).
      // Save to connectionMap so it can be closed at user's convenience.
      addConnection(handleId, conn);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return ins.toArray(new InputSplit[ins.size()]);
  }

  private void addConnection(String handleId, Connection connection) {
    synchronized (lock) {
      List<Connection> handleConnections = connectionMap.get(handleId);
      if (handleConnections == null) {
        handleConnections = new ArrayList<Connection>();
        connectionMap.put(handleId, handleConnections);
      }
      handleConnections.add(connection);
    }
  }

  /**
   * Close the connection associated with the handle ID, if getSplits() was configured with a handle ID.
   * Call when the application is done using the splits generated by getSplits().
   * @param handleId Handle ID used in configuration for getSplits()
   * @throws IOException
   */
  public static void close(String handleId) throws IOException {
    List<Connection> handleConnections;
    synchronized (lock) {
      handleConnections = connectionMap.remove(handleId);
    }
    if (handleConnections != null) {
      LOG.debug("Closing {} connections for handle ID {}", handleConnections.size(), handleId);
      for (Connection conn : handleConnections) {
        try {
          conn.close();
        } catch (Exception err) {
          LOG.error("Error while closing connection for " + handleId, err);
        }
      }
    } else {
      LOG.debug("No connection found for handle ID {}", handleId);
    }
  }

  /**
   * Close all outstanding connections created by getSplits() calls
   */
  public static void closeAll() {
    LOG.debug("Closing all handles");
    for (String handleId : connectionMap.keySet()) {
      try {
        close(handleId);
      } catch (Exception err) {
        LOG.error("Error closing handle ID " + handleId, err);
      }
    }
  }

  static {
    // Shutdown hook to clean up resources at process end.
    ShutdownHookManager.addShutdownHook(new Runnable() {
      @Override
      public void run() {
        closeAll();
      }
    });
  }

  private LlapServiceInstance getServiceInstance(JobConf job, LlapInputSplit llapSplit) throws IOException {
    LlapRegistryService registryService = LlapRegistryService.getClient(job);
    String host = llapSplit.getLocations()[0];

    LlapServiceInstance serviceInstance = getServiceInstanceForHost(registryService, host);
    if (serviceInstance == null) {
      LOG.info("No service instances found for " + host + " in registry.");
      serviceInstance = getServiceInstanceRandom(registryService);
      if (serviceInstance == null) {
        throw new IOException("No service instances found in registry");
      }
    }

    return serviceInstance;
  }

  private LlapServiceInstance getServiceInstanceForHost(LlapRegistryService registryService, String host) throws IOException {
    InetAddress address = InetAddress.getByName(host);
    LlapServiceInstanceSet instanceSet = registryService.getInstances();
    LlapServiceInstance serviceInstance = null;

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


  private LlapServiceInstance getServiceInstanceRandom(LlapRegistryService registryService) throws IOException {
    LlapServiceInstanceSet instanceSet = registryService.getInstances();
    LlapServiceInstance serviceInstance = null;

    LOG.info("Finding random live service instance");
    Collection<LlapServiceInstance> allInstances = instanceSet.getAll();
    if (allInstances.size() > 0) {
      int randIdx = rand.nextInt(allInstances.size());;
      serviceInstance = allInstances.toArray(serviceInstanceArray)[randIdx];
    }
    return serviceInstance;
  }

  private LlapServiceInstance selectServiceInstance(Set<LlapServiceInstance> serviceInstances) {
    if (serviceInstances == null || serviceInstances.isEmpty()) {
      return null;
    }

    // Get the first live service instance
    for (LlapServiceInstance serviceInstance : serviceInstances) {
      return serviceInstance;
    }

    LOG.info("No live service instances were found");
    return null;
  }

  private SubmitWorkRequestProto constructSubmitWorkRequestProto(SubmitWorkInfo submitWorkInfo,
      int taskNum, int attemptNum, InetSocketAddress address, Token<JobTokenIdentifier> token,
      byte[] fragmentBytes, byte[] fragmentBytesSignature, JobConf job) throws IOException {
    ApplicationId appId = submitWorkInfo.getFakeAppId();

    // This works, assuming the executor is running within YARN.
    String user = System.getenv(ApplicationConstants.Environment.USER.name());
    LOG.info("Setting user in submitWorkRequest to: " + user);

    ContainerId containerId =
        ContainerId.newInstance(ApplicationAttemptId.newInstance(appId, attemptNum), taskNum);

    // Credentials can change across DAGs. Ideally construct only once per DAG.
    Credentials credentials = new Credentials();
    TokenCache.setSessionToken(token, credentials);
    ByteBuffer credentialsBinary = serializeCredentials(credentials);

    FragmentRuntimeInfo.Builder runtimeInfo = FragmentRuntimeInfo.newBuilder();
    runtimeInfo.setCurrentAttemptStartTime(System.currentTimeMillis());
    runtimeInfo.setWithinDagPriority(0);
    runtimeInfo.setDagStartTime(submitWorkInfo.getCreationTime());
    runtimeInfo.setFirstAttemptStartTime(submitWorkInfo.getCreationTime());
    runtimeInfo.setNumSelfAndUpstreamTasks(submitWorkInfo.getVertexParallelism());
    runtimeInfo.setNumSelfAndUpstreamCompletedTasks(0);

    SubmitWorkRequestProto.Builder builder = SubmitWorkRequestProto.newBuilder();

    VertexOrBinary.Builder vertexBuilder = VertexOrBinary.newBuilder();
    vertexBuilder.setVertexBinary(ByteString.copyFrom(submitWorkInfo.getVertexBinary()));
    if (submitWorkInfo.getVertexSignature() != null) {
      // Unsecure case?
      builder.setWorkSpecSignature(ByteString.copyFrom(submitWorkInfo.getVertexSignature()));
    }
    builder.setWorkSpec(vertexBuilder.build());
    builder.setFragmentNumber(taskNum);
    builder.setAttemptNumber(attemptNum);
    builder.setContainerIdString(containerId.toString());
    builder.setAmHost(LlapUtil.getAmHostNameFromAddress(address, job));
    builder.setAmPort(address.getPort());
    builder.setCredentialsBinary(ByteString.copyFrom(credentialsBinary));
    builder.setFragmentRuntimeInfo(runtimeInfo.build());
    builder.setInitialEventBytes(ByteString.copyFrom(fragmentBytes));
    if (fragmentBytesSignature != null) {
      builder.setInitialEventSignature(ByteString.copyFrom(fragmentBytesSignature));
    }
    return builder.build();
  }

  private ByteBuffer serializeCredentials(Credentials credentials) throws IOException {
    Credentials containerCredentials = new Credentials();
    containerCredentials.addAll(credentials);
    DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
    containerCredentials.writeTokenStorageToStream(containerTokens_dob);
    return ByteBuffer.wrap(containerTokens_dob.getData(), 0, containerTokens_dob.getLength());
  }

  private static final char ESCAPE_CHAR = '\\';

  private static final char[] escapedChars = {
    '"', ESCAPE_CHAR
  };

  private static class LlapRecordReaderTaskUmbilicalExternalResponder implements LlapTaskUmbilicalExternalResponder {
    protected LlapBaseRecordReader<?> recordReader = null;
    protected LinkedBlockingQueue<ReaderEvent> queuedEvents = new LinkedBlockingQueue<ReaderEvent>();

    public LlapRecordReaderTaskUmbilicalExternalResponder() {
    }

    @Override
    public void submissionFailed(String fragmentId, Throwable throwable) {
      try {
        sendOrQueueEvent(ReaderEvent.errorEvent(
            "Received submission failed event for fragment ID " + fragmentId + ": " + throwable.toString()));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    @Override
    public void heartbeat(TezHeartbeatRequest request) {
      List<TezEvent> inEvents = request.getEvents();
      for (TezEvent tezEvent : ListUtils.emptyIfNull(inEvents)) {
        EventType eventType = tezEvent.getEventType();
        try {
          switch (eventType) {
            case TASK_ATTEMPT_COMPLETED_EVENT:
              sendOrQueueEvent(ReaderEvent.doneEvent());
              break;
            case TASK_ATTEMPT_FAILED_EVENT:
              TaskAttemptFailedEvent taskFailedEvent = (TaskAttemptFailedEvent) tezEvent.getEvent();
              sendOrQueueEvent(ReaderEvent.errorEvent(taskFailedEvent.getDiagnostics()));
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
        sendOrQueueEvent(ReaderEvent.errorEvent(
            "Received task killed event for task ID " + taskAttemptId));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    @Override
    public void heartbeatTimeout(String taskAttemptId) {
      try {
        sendOrQueueEvent(ReaderEvent.errorEvent(
            "Timed out waiting for heartbeat for task ID " + taskAttemptId));
      } catch (Exception err) {
        LOG.error("Error during heartbeat responder:", err);
      }
    }

    public synchronized LlapBaseRecordReader<?> getRecordReader() {
      return recordReader;
    }

    public synchronized void setRecordReader(LlapBaseRecordReader recordReader) {
      this.recordReader = recordReader;

      if (recordReader == null) {
        return;
      }

      // If any events were queued by the responder, give them to the record reader now.
      while (!queuedEvents.isEmpty()) {
        ReaderEvent readerEvent = queuedEvents.poll();
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
    protected synchronized void sendOrQueueEvent(ReaderEvent readerEvent) {
      LlapBaseRecordReader<?> recordReader = getRecordReader();
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
