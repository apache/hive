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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonProtocolBlockingPB;

public class LlapDaemonProtocolServerImpl extends AbstractService
    implements LlapDaemonProtocolBlockingPB {

  private static final Log LOG = LogFactory.getLog(LlapDaemonProtocolServerImpl.class);

  private final int numHandlers;
  private final ContainerRunner containerRunner;
  private final int configuredPort;
  private RPC.Server server;
  private final AtomicReference<InetSocketAddress> bindAddress;


  public LlapDaemonProtocolServerImpl(int numHandlers,
                                      ContainerRunner containerRunner,
                                      AtomicReference<InetSocketAddress> address,
                                      int configuredPort) {
    super("LlapDaemonProtocolServerImpl");
    this.numHandlers = numHandlers;
    this.containerRunner = containerRunner;
    this.bindAddress = address;
    this.configuredPort = configuredPort;
    LOG.info("Creating: " + LlapDaemonProtocolServerImpl.class.getSimpleName() +
        " with port configured to: " + configuredPort);
  }

  @Override
  public SubmitWorkResponseProto submitWork(RpcController controller,
                                            LlapDaemonProtocolProtos.SubmitWorkRequestProto request) throws
      ServiceException {
    try {
      containerRunner.submitWork(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return SubmitWorkResponseProto.getDefaultInstance();
  }

  @Override
  public void serviceStart() {
    Configuration conf = getConfig();

    InetSocketAddress addr = new InetSocketAddress(configuredPort);
    try {
      server = createServer(LlapDaemonProtocolBlockingPB.class, addr, conf, numHandlers,
          LlapDaemonProtocolProtos.LlapDaemonProtocol.newReflectiveBlockingService(this));
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to run RPC Server on port: " + configuredPort, e);
      throw new RuntimeException(e);
    }

    InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
    this.bindAddress.set(NetUtils.createSocketAddrForHost(
        serverBindAddress.getAddress().getCanonicalHostName(),
        serverBindAddress.getPort()));
    LOG.info("Instantiated " + LlapDaemonProtocolBlockingPB.class.getSimpleName() + " at " +
        bindAddress);
  }

  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  InetSocketAddress getBindAddress() {
    return bindAddress.get();
  }

  private RPC.Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf,
                                  int numHandlers, BlockingService blockingService) throws
      IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Server server = new RPC.Builder(conf)
        .setProtocol(pbProtocol)
        .setInstance(blockingService)
        .setBindAddress(addr.getHostName())
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers)
        .build();
    // TODO Add security.
    return server;
  }
}
