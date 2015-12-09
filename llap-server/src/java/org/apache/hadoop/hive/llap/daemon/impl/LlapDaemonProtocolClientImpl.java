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

import javax.annotation.Nullable;
import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;
import org.apache.hadoop.security.UserGroupInformation;

// TODO Change all this to be based on a regular interface instead of relying on the Proto service - Exception signatures cannot be controlled without this for the moment.


public class LlapDaemonProtocolClientImpl implements LlapDaemonProtocolBlockingPB {

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;
  LlapDaemonProtocolBlockingPB proxy;


  public LlapDaemonProtocolClientImpl(Configuration conf, String hostname, int port,
                                      @Nullable RetryPolicy retryPolicy,
                                      @Nullable SocketFactory socketFactory) {
    this.conf = conf;
    this.serverAddr = NetUtils.createSocketAddr(hostname, port);
    this.retryPolicy = retryPolicy;
    if (socketFactory == null) {
      this.socketFactory = NetUtils.getDefaultSocketFactory(conf);
    } else {
      this.socketFactory = socketFactory;
    }
  }

  @Override
  public SubmitWorkResponseProto submitWork(RpcController controller,
                                                                     SubmitWorkRequestProto request) throws
      ServiceException {
    try {
      return getProxy().submitWork(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(RpcController controller,
                                                            SourceStateUpdatedRequestProto request) throws
      ServiceException {
    try {
      return getProxy().sourceStateUpdated(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public QueryCompleteResponseProto queryComplete(RpcController controller,
                                                  QueryCompleteRequestProto request) throws
      ServiceException {
    try {
      return getProxy().queryComplete(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      RpcController controller,
      TerminateFragmentRequestProto request) throws ServiceException {
    try {
      return getProxy().terminateFragment(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public LlapDaemonProtocolBlockingPB getProxy() throws IOException {
    if (proxy == null) {
      proxy = createProxy();
    }
    return proxy;
  }

  public LlapDaemonProtocolBlockingPB createProxy() throws IOException {
    RPC.setProtocolEngine(conf, LlapDaemonProtocolBlockingPB.class, ProtobufRpcEngine.class);
    ProtocolProxy<LlapDaemonProtocolBlockingPB> proxy =
        RPC.getProtocolProxy(LlapDaemonProtocolBlockingPB.class, 0, serverAddr,
            UserGroupInformation.getCurrentUser(), conf, NetUtils.getDefaultSocketFactory(conf), 0,
            retryPolicy);
    return proxy.getProxy();
  }
}
