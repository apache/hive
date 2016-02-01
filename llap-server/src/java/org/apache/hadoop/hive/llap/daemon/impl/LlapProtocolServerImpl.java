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
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hive.llap.security.LlapSecurityHelper;
import org.apache.hadoop.hive.llap.security.LlapTokenIdentifier;
import org.apache.hadoop.hive.llap.security.SecretManager;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.hive.llap.daemon.ContainerRunner;
import org.apache.hadoop.hive.llap.protocol.LlapProtocolBlockingPB;
import org.apache.hadoop.hive.llap.protocol.LlapManagementProtocolPB;
import org.apache.hadoop.hive.llap.security.LlapDaemonPolicyProvider;

public class LlapProtocolServerImpl extends AbstractService
    implements LlapProtocolBlockingPB, LlapManagementProtocolPB {

  private static final Logger LOG = LoggerFactory.getLogger(LlapProtocolServerImpl.class);

  private final int numHandlers;
  private final ContainerRunner containerRunner;
  private final int srvPort, mngPort;
  private RPC.Server server, mngServer;
  private final AtomicReference<InetSocketAddress> srvAddress, mngAddress;
  private SecretManager zkSecretManager;

  public LlapProtocolServerImpl(int numHandlers,
                                ContainerRunner containerRunner,
                                AtomicReference<InetSocketAddress> srvAddress,
                                AtomicReference<InetSocketAddress> mngAddress,
                                int srvPort,
                                int mngPort) {
    super("LlapDaemonProtocolServerImpl");
    this.numHandlers = numHandlers;
    this.containerRunner = containerRunner;
    this.srvAddress = srvAddress;
    this.srvPort = srvPort;
    this.mngAddress = mngAddress;
    this.mngPort = mngPort;
    LOG.info("Creating: " + LlapProtocolServerImpl.class.getSimpleName() +
        " with port configured to: " + srvPort);
  }

  @Override
  public SubmitWorkResponseProto submitWork(RpcController controller,
                                            SubmitWorkRequestProto request) throws
      ServiceException {
    try {
      return containerRunner.submitWork(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(RpcController controller,
      SourceStateUpdatedRequestProto request) throws ServiceException {
    return containerRunner.sourceStateUpdated(request);
  }

  @Override
  public QueryCompleteResponseProto queryComplete(RpcController controller,
      QueryCompleteRequestProto request) throws ServiceException {
    return containerRunner.queryComplete(request);
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      RpcController controller,
      TerminateFragmentRequestProto request) throws ServiceException {
    return containerRunner.terminateFragment(request);
  }

  @Override
  public void serviceStart() {
    final Configuration conf = getConfig();
    final BlockingService daemonImpl =
        LlapDaemonProtocolProtos.LlapDaemonProtocol.newReflectiveBlockingService(this);
    final BlockingService managementImpl =
        LlapDaemonProtocolProtos.LlapManagementProtocol.newReflectiveBlockingService(this);
    if (!UserGroupInformation.isSecurityEnabled()) {
      startProtocolServers(conf, daemonImpl, managementImpl);
      return;
    }
    String llapPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL),
        llapKeytab = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE);
    zkSecretManager = SecretManager.createSecretManager(conf, llapPrincipal, llapKeytab);

    // Start the protocol server after properly authenticating with daemon keytab.
    UserGroupInformation daemonUgi = null;
    try {
      daemonUgi = LlapSecurityHelper.loginWithKerberos(llapPrincipal, llapKeytab);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    daemonUgi.doAs(new PrivilegedAction<Void>() {
       @Override
      public Void run() {
         startProtocolServers(conf, daemonImpl, managementImpl);
         return null;
      }
    });
  }

  private void startProtocolServers(
      Configuration conf, BlockingService daemonImpl, BlockingService managementImpl) {
    server = startProtocolServer(srvPort, numHandlers, srvAddress, conf, daemonImpl,
        LlapProtocolBlockingPB.class);
    mngServer = startProtocolServer(mngPort, 2, mngAddress, conf, managementImpl,
        LlapManagementProtocolPB.class);
  }

  private RPC.Server startProtocolServer(int srvPort, int numHandlers,
      AtomicReference<InetSocketAddress> bindAddress, Configuration conf,
      BlockingService impl, Class<?> protocolClass) {
    InetSocketAddress addr = new InetSocketAddress(srvPort);
    RPC.Server server;
    try {
      server = createServer(protocolClass, addr, conf, numHandlers, impl);
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to run RPC Server on port: " + srvPort, e);
      throw new RuntimeException(e);
    }

    InetSocketAddress serverBindAddress = NetUtils.getConnectAddress(server);
    bindAddress.set(NetUtils.createSocketAddrForHost(
        serverBindAddress.getAddress().getCanonicalHostName(),
        serverBindAddress.getPort()));
    LOG.info("Instantiated " + protocolClass.getSimpleName() + " at " + bindAddress);
    return server;
  }

  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
    if (mngServer != null) {
      mngServer.stop();
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  InetSocketAddress getBindAddress() {
    return srvAddress.get();
  }

  private RPC.Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf,
                                  int numHandlers, BlockingService blockingService) throws
      IOException {
    RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
    RPC.Builder builder = new RPC.Builder(conf)
        .setProtocol(pbProtocol)
        .setInstance(blockingService)
        .setBindAddress(addr.getHostName())
        .setPort(addr.getPort())
        .setNumHandlers(numHandlers);
    if (zkSecretManager != null) {
      builder = builder.setSecretManager(zkSecretManager);
    }
    RPC.Server server = builder.build();

    if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      server.refreshServiceAcl(conf, new LlapDaemonPolicyProvider());
    }
    return server;
  }


  @Override
  public GetTokenResponseProto getDelegationToken(RpcController controller,
      GetTokenRequestProto request) throws ServiceException {
    if (zkSecretManager == null) {
      throw new ServiceException("Operation not supported on unsecure cluster");
    }
    UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    Text renewer = new Text(ugi.getShortUserName());
    LlapTokenIdentifier llapId = new LlapTokenIdentifier(owner, renewer, realUser);
    // TODO: note that the token is not renewable right now and will last for 2 weeks by default.
    Token<LlapTokenIdentifier> token = new Token<LlapTokenIdentifier>(llapId, zkSecretManager);
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    try {
      token.write(out);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    ByteString bs = ByteString.copyFrom(out.toByteArray());
    GetTokenResponseProto response = GetTokenResponseProto.newBuilder().setToken(bs).build();
    return response;
  }
}
