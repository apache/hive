/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;

import org.apache.hive.common.util.Ref;

import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.ql.exec.tez.AmPluginNode.AmPluginInfo;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.AsyncPbRpcProxy;
import org.apache.hadoop.hive.llap.LlapNodeId;
import org.apache.hadoop.hive.llap.impl.LlapPluginProtocolClientImpl;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryResponseProto;
import org.apache.hadoop.hive.llap.protocol.LlapPluginProtocolPB;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;

public class LlapPluginEndpointClientImpl extends
    AsyncPbRpcProxy<LlapPluginProtocolPB, JobTokenIdentifier>
    implements LlapPluginEndpointClient {

  public LlapPluginEndpointClientImpl(
      Configuration conf, Token<JobTokenIdentifier> token, int expectedNodes) {
    // A single concurrent request per node is currently hardcoded. The node includes a port number
    // so different AMs on the same host count as different nodes; we only have one request type,
    // and it is not useful to send more than one in parallel.
    super(LlapPluginEndpointClientImpl.class.getSimpleName(),
        HiveConf.getIntVar(conf, ConfVars.LLAP_PLUGIN_CLIENT_NUM_THREADS), conf, token,
        HiveConf.getTimeVar(conf, ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_TIMEOUT_MS,
            TimeUnit.MILLISECONDS),
        HiveConf.getTimeVar(conf, ConfVars.LLAP_TASK_COMMUNICATOR_CONNECTION_SLEEP_BETWEEN_RETRIES_MS,
            TimeUnit.MILLISECONDS), expectedNodes, 1);
  }

  @Override
  protected LlapPluginProtocolPB createProtocolImpl(Configuration conf,
      String hostname, int port, UserGroupInformation ugi,
      RetryPolicy retryPolicy, SocketFactory socketFactory) {
    return new LlapPluginProtocolClientImpl(conf, hostname, port, retryPolicy, socketFactory, ugi);
  }

  @Override
  protected void shutdownProtocolImpl(LlapPluginProtocolPB proxy) {
    // Nothing to do.
  }

  @Override
  protected String getTokenUser(Token<JobTokenIdentifier> token) {
    try {
      return token.decodeIdentifier().getJobId().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.exec.tez.LlapPluginEndpointClient#sendUpdateQuery(org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto, java.lang.String, int, org.apache.hadoop.security.token.Token, org.apache.hadoop.hive.llap.AsyncPbRpcProxy.ExecuteRequestCallback)
   */
  @Override
  public void sendUpdateQuery(UpdateQueryRequestProto request,
      AmPluginNode node, UpdateRequestContext callback) {
    queueRequest(new SendUpdateQueryCallable(node, request, callback));
  }

  private class SendUpdateQueryCallable
      extends CallableRequest<UpdateQueryRequestProto, UpdateQueryResponseProto> {
    private final AmPluginNode node;
    private AmPluginInfo info;
    private final UpdateRequestContext context;

    protected SendUpdateQueryCallable(
        AmPluginNode node, UpdateQueryRequestProto request, UpdateRequestContext callback) {
      super(request, callback);
      this.node = node;
      this.context = callback;
    }

    @Override
    public LlapNodeId getNodeId() throws InterruptedException, TimeoutException {
      ensureInfo();
      return LlapNodeId.getInstance(info.amHost, info.amPluginPort);
    }

    @Override
    public UpdateQueryResponseProto call() throws Exception {
      ensureInfo();
      LlapNodeId nodeId = LlapNodeId.getInstance(info.amHost, info.amPluginPort);
      return getProxy(nodeId, info.amPluginToken).updateQuery(null, request);
    }

    private void ensureInfo() {
      if (info != null) return;
      Ref<Integer> endpointVersion = new Ref<>(-1);
      info = node.getAmPluginInfo(endpointVersion);
      context.setNodeInfo(info, endpointVersion.value); // Give the caller context for future errors.
      if (info == null) {
        // RequestManager will catch this and handle like any other error.
        throw new RuntimeException("No AM plugin info for " + node);
      }
    }
  }
}
