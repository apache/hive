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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;

// TODO Change all this to be based on a regular interface instead of relying on the Proto service - Exception signatures cannot be controlled without this for the moment.


public class LlapDaemonProtocolClientImpl implements LlapDaemonProtocolBlockingPB {

  private final Configuration conf;
  private final InetSocketAddress serverAddr;
  LlapDaemonProtocolBlockingPB proxy;


  public LlapDaemonProtocolClientImpl(Configuration conf, String hostname, int port) {
    this.conf = conf;
    this.serverAddr = NetUtils.createSocketAddr(hostname, port);
  }

  @Override
  public LlapDaemonProtocolProtos.SubmitWorkResponseProto submitWork(RpcController controller,
                                                                     LlapDaemonProtocolProtos.SubmitWorkRequestProto request) throws
      ServiceException {
    try {
      return getProxy().submitWork(null, request);
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
    LlapDaemonProtocolBlockingPB p;
    // TODO Fix security
    RPC.setProtocolEngine(conf, LlapDaemonProtocolBlockingPB.class, ProtobufRpcEngine.class);
    p =  RPC.getProxy(LlapDaemonProtocolBlockingPB.class, 0, serverAddr, conf);
    return p;
  }
}
