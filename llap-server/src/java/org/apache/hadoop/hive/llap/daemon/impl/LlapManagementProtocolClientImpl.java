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
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hive.llap.daemon.LlapManagementProtocolBlockingPB;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenResponseProto;
import org.apache.hadoop.security.UserGroupInformation;

public class LlapManagementProtocolClientImpl implements LlapManagementProtocolBlockingPB {

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  private final RetryPolicy retryPolicy;
  private final SocketFactory socketFactory;
  LlapManagementProtocolBlockingPB proxy;


  public LlapManagementProtocolClientImpl(Configuration conf, String hostname, int port,
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

  public LlapManagementProtocolBlockingPB getProxy() throws IOException {
    if (proxy == null) {
      proxy = createProxy();
    }
    return proxy;
  }

  public LlapManagementProtocolBlockingPB createProxy() throws IOException {
    RPC.setProtocolEngine(conf, LlapManagementProtocolBlockingPB.class, ProtobufRpcEngine.class);
    ProtocolProxy<LlapManagementProtocolBlockingPB> proxy =
        RPC.getProtocolProxy(LlapManagementProtocolBlockingPB.class, 0, serverAddr,
            UserGroupInformation.getCurrentUser(), conf, NetUtils.getDefaultSocketFactory(conf), 0,
            retryPolicy);
    return proxy.getProxy();
  }

  @Override
  public GetTokenResponseProto getDelegationToken(RpcController controller,
      GetTokenRequestProto request) throws ServiceException {
    try {
      return getProxy().getDelegationToken(null, request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }
}
