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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.ext.LlapTaskUmbilicalExternalClient;
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
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    int llapSubmitPort = HiveConf.getIntVar(job, HiveConf.ConfVars.LLAP_DAEMON_RPC_PORT);

    LOG.info("ZZZ: DBG: Starting LlapTaskUmbilicalExternalClient");

    LlapTaskUmbilicalExternalClient llapClient =
        new LlapTaskUmbilicalExternalClient(job, submitWorkInfo.getTokenIdentifier(),
            submitWorkInfo.getToken());
    llapClient.init(job);
    llapClient.start();

    LOG.info("ZZZ: DBG: Crated LlapClient");
    // TODO KKK Shutdown the llap client.

    SubmitWorkRequestProto submitWorkRequestProto =
        constructSubmitWorkRequestProto(submitWorkInfo, llapSplit.getSplitNum(),
            llapClient.getAddress(), submitWorkInfo.getToken());

    LOG.info("ZZZ: DBG: Created submitWorkRequest for: " + submitWorkRequestProto.getFragmentSpec().getFragmentIdentifierString());

    TezEvent tezEvent = new TezEvent();
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(llapSplit.getFragmentBytes(), 0, llapSplit.getFragmentBytes().length);
    tezEvent.readFields(dib);
    List<TezEvent> tezEventList = Lists.newArrayList();
    tezEventList.add(tezEvent);

    // this is just the portion that sets up the io to receive data
    String host = split.getLocations()[0];

    llapClient.submitWork(submitWorkRequestProto, host, llapSubmitPort, tezEventList);

    String id = HiveConf.getVar(job, HiveConf.ConfVars.HIVEQUERYID) + "_" + llapSplit.getSplitNum();

    HiveConf conf = new HiveConf();
    Socket socket = new Socket(host,
        conf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT));

    LOG.debug("Socket connected");

    socket.getOutputStream().write(id.getBytes());
    socket.getOutputStream().write(0);
    socket.getOutputStream().flush();

    LOG.debug("Registered id: " + id);

    return new LlapRecordReader(socket.getInputStream(), llapSplit.getSchema(), Text.class);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    throw new IOException("These are not the splits you are looking for.");
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
    LOG.info("DBG: Setting user in submitWorkRequest to: " +
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
}
