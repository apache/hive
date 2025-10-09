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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.DaemonId;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.CacheEntryList;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetTokenResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetCacheContentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.GetCacheContentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryCompleteResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SourceStateUpdatedResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.TerminateFragmentResponseProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.UpdateFragmentResponseProto;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
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

  private enum TokenRequiresSigning {
    TRUE, FALSE, EXCEPT_OWNER
  }

  private final int numHandlers;
  private final ContainerRunner containerRunner;
  private final int srvPort, mngPort, externalClientsRpcPort;
  private RPC.Server server, mngServer, externalClientsRpcServer;
  private final AtomicReference<InetSocketAddress> srvAddress, mngAddress;
  private final SecretManager secretManager;
  private String clusterUser = null;
  private boolean isRestrictedToClusterUser = false;
  private final LlapDaemonExecutorMetrics executorMetrics;
  private TokenRequiresSigning isSigningRequiredConfig = TokenRequiresSigning.TRUE;
  private LlapTokenManager llapTokenManager = new DummyTokenManager();

  public LlapProtocolServerImpl(SecretManager secretManager, int numHandlers,
      ContainerRunner containerRunner, AtomicReference<InetSocketAddress> srvAddress,
      AtomicReference<InetSocketAddress> mngAddress, int srvPort, int externalClientsRpcPort,
      int mngPort, DaemonId daemonId,
      LlapDaemonExecutorMetrics executorMetrics) {
    super("LlapDaemonProtocolServerImpl");
    this.numHandlers = numHandlers;
    this.containerRunner = containerRunner;
    this.secretManager = secretManager;
    this.srvAddress = srvAddress;
    this.srvPort = srvPort;
    this.mngAddress = mngAddress;
    this.externalClientsRpcPort = externalClientsRpcPort;
    this.mngPort = mngPort;
    this.executorMetrics = executorMetrics;
    LOG.info("Creating: " + LlapProtocolServerImpl.class.getSimpleName() +
        " with port configured to: " + srvPort);
  }

  @Override
  public LlapDaemonProtocolProtos.RegisterDagResponseProto registerDag(
      RpcController controller,
      LlapDaemonProtocolProtos.RegisterDagRequestProto request)
      throws ServiceException {
    try {
      return containerRunner.registerDag(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SubmitWorkResponseProto submitWork(RpcController controller,
      SubmitWorkRequestProto request) throws ServiceException {
    try {
      return containerRunner.submitWork(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public SourceStateUpdatedResponseProto sourceStateUpdated(RpcController controller,
      SourceStateUpdatedRequestProto request) throws ServiceException {
    try {
      return containerRunner.sourceStateUpdated(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public QueryCompleteResponseProto queryComplete(RpcController controller,
      QueryCompleteRequestProto request) throws ServiceException {
    try {
      return containerRunner.queryComplete(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public TerminateFragmentResponseProto terminateFragment(
      RpcController controller, TerminateFragmentRequestProto request) throws ServiceException {
    try {
      return containerRunner.terminateFragment(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public UpdateFragmentResponseProto updateFragment(
      RpcController controller, UpdateFragmentRequestProto request) throws ServiceException {
    try {
      return containerRunner.updateFragment(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public void serviceStart() {
    final Configuration conf = getConfig();
    isSigningRequiredConfig = getSigningConfig(conf);
    final BlockingService daemonImpl =
        LlapDaemonProtocolProtos.LlapDaemonProtocol.newReflectiveBlockingService(this);
    final BlockingService managementImpl =
        LlapDaemonProtocolProtos.LlapManagementProtocol.newReflectiveBlockingService(this);
    if (!UserGroupInformation.isSecurityEnabled()) {
      startProtocolServers(conf, daemonImpl, managementImpl);
      return;
    }
    try {
      this.clusterUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (isPermissiveManagementAcl(conf)) {
      LOG.warn("Management protocol has a '*' ACL.");
      isRestrictedToClusterUser = true;
    }
    String llapPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL),
        llapKeytab = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE);

    // Start the protocol server after properly authenticating with daemon keytab.
    UserGroupInformation daemonUgi = null;
    try {
      daemonUgi = LlapUtil.loginWithKerberos(llapPrincipal, llapKeytab);
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

  private static TokenRequiresSigning getSigningConfig(final Configuration conf) {
    String signSetting = HiveConf.getVar(
        conf, ConfVars.LLAP_REMOTE_TOKEN_REQUIRES_SIGNING).toLowerCase();
    switch (signSetting) {
    case "true": return TokenRequiresSigning.TRUE;
    case "except_llap_owner": return TokenRequiresSigning.EXCEPT_OWNER;
    case "false": return TokenRequiresSigning.FALSE;
    default: {
      throw new RuntimeException("Invalid value for "
          + ConfVars.LLAP_REMOTE_TOKEN_REQUIRES_SIGNING.varname + ": " + signSetting);
    }
    }
  }

  private static boolean isPermissiveManagementAcl(Configuration conf) {
    return HiveConf.getBoolVar(conf, ConfVars.LLAP_VALIDATE_ACLS)
        && AccessControlList.WILDCARD_ACL_VALUE.equals(
            HiveConf.getVar(conf, ConfVars.LLAP_MANAGEMENT_ACL))
        && "".equals(HiveConf.getVar(conf, ConfVars.LLAP_MANAGEMENT_ACL_DENY));
  }

  private void startProtocolServers(
      Configuration conf, BlockingService daemonImpl, BlockingService managementImpl) {
    LlapDaemonPolicyProvider pp = new LlapDaemonPolicyProvider();
    server = LlapUtil.startProtocolServer(srvPort, numHandlers, srvAddress, conf, daemonImpl,
        LlapProtocolBlockingPB.class, secretManager, pp, ConfVars.LLAP_SECURITY_ACL,
        ConfVars.LLAP_SECURITY_ACL_DENY);
    // for cloud deployments, start a separate RPC server on the port
    // which we can open to accept requests from external clients.
    if (LlapUtil.isCloudDeployment(conf)) {
      externalClientsRpcServer = LlapUtil.startProtocolServer(externalClientsRpcPort, numHandlers, null, conf, daemonImpl,
          LlapProtocolBlockingPB.class, secretManager, pp, ConfVars.LLAP_SECURITY_ACL,
          ConfVars.LLAP_SECURITY_ACL_DENY);

      LOG.info("Started externalClientsRpcServer for cloud based deployments : {}, {}", externalClientsRpcServer.getListenerAddress(), externalClientsRpcServer);
    }
    mngServer = LlapUtil.startProtocolServer(mngPort, 2, mngAddress, conf, managementImpl,
        LlapManagementProtocolPB.class, secretManager, pp, ConfVars.LLAP_MANAGEMENT_ACL,
        ConfVars.LLAP_MANAGEMENT_ACL_DENY);
  }


  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
    if (externalClientsRpcServer != null) {
      externalClientsRpcServer.stop();
    }
    if (mngServer != null) {
      mngServer.stop();
    }
  }

  @InterfaceAudience.Private
  InetSocketAddress getBindAddress() {
    return srvAddress.get();
  }

  @InterfaceAudience.Private
  InetSocketAddress getManagementBindAddress() {
    return mngAddress.get();
  }

  @InterfaceAudience.Private
  InetSocketAddress getExternalClientsRpcServerBindAddress() {
    return externalClientsRpcServer.getListenerAddress();
  }

  @Override
  public GetTokenResponseProto getDelegationToken(RpcController controller,
      GetTokenRequestProto request) throws ServiceException {
    if (secretManager == null) {
      throw new ServiceException("Operation not supported on unsecure cluster");
    }
    UserGroupInformation callingUser = null;
    Token<LlapTokenIdentifier> token = null;
    try {
      callingUser = UserGroupInformation.getCurrentUser();
      // Determine if the user would need to sign fragments.
      boolean isSigningRequired = determineIfSigningIsRequired(callingUser);
      token = llapTokenManager.getToken(request, isSigningRequired);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    if (isRestrictedToClusterUser && !clusterUser.equals(callingUser.getShortUserName())) {
      throw new ServiceException("Management protocol ACL is too permissive. The access has been"
          + " automatically restricted to " + clusterUser + "; " + callingUser.getShortUserName()
          + " is denied access. Please set " + ConfVars.LLAP_VALIDATE_ACLS.varname + " to false,"
          + " or adjust " + ConfVars.LLAP_MANAGEMENT_ACL.varname + " and "
          + ConfVars.LLAP_MANAGEMENT_ACL_DENY.varname + " to a more restrictive ACL.");
    }

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

  @Override
  public LlapDaemonProtocolProtos.PurgeCacheResponseProto purgeCache(final RpcController controller,
    final LlapDaemonProtocolProtos.PurgeCacheRequestProto request) throws ServiceException {
    LlapDaemonProtocolProtos.PurgeCacheResponseProto.Builder responseProtoBuilder = LlapDaemonProtocolProtos
      .PurgeCacheResponseProto.newBuilder();
    LlapIo<?> llapIo = LlapProxy.getIo();
    if (llapIo != null) {
      responseProtoBuilder.setPurgedMemoryBytes(llapIo.purge());
    } else {
      responseProtoBuilder.setPurgedMemoryBytes(0);
    }
    return responseProtoBuilder.build();
  }

  @Override
  public LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto getDaemonMetrics(final RpcController controller,
      final LlapDaemonProtocolProtos.GetDaemonMetricsRequestProto request) throws ServiceException {
    LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto.Builder responseProtoBuilder =
        LlapDaemonProtocolProtos.GetDaemonMetricsResponseProto.newBuilder();
    if (executorMetrics != null) {
      Map<String, Long> data = new HashMap<>();
      DumpingMetricsCollector dmc = new DumpingMetricsCollector(data);
      executorMetrics.getMetrics(dmc, true);
      data.forEach((key, value) -> responseProtoBuilder.addMetrics(
          LlapDaemonProtocolProtos.MapEntry.newBuilder().setKey(key).setValue(value).build()));
    }
    return responseProtoBuilder.build();
  }

  @Override
  public LlapDaemonProtocolProtos.SetCapacityResponseProto setCapacity(final RpcController controller,
      final LlapDaemonProtocolProtos.SetCapacityRequestProto request) throws ServiceException {
    try {
      return containerRunner.setCapacity(request);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public LlapDaemonProtocolProtos.EvictEntityResponseProto evictEntity(
      RpcController controller, LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest)
      throws ServiceException {
    LlapDaemonProtocolProtos.EvictEntityResponseProto.Builder responseProtoBuilder =
        LlapDaemonProtocolProtos.EvictEntityResponseProto.newBuilder();

    LlapIo<?> llapIo = LlapProxy.getIo();
    if (llapIo != null) {
      long evicted = llapIo.evictEntity(protoRequest);
      responseProtoBuilder.setEvictedBytes(evicted);
    } else {
      responseProtoBuilder.setEvictedBytes(-1L);
    }
    return responseProtoBuilder.build();
  }

  @Override
  public GetCacheContentResponseProto getCacheContent(RpcController controller,
      GetCacheContentRequestProto request) {
    GetCacheContentResponseProto.Builder responseProtoBuilder = GetCacheContentResponseProto.newBuilder();
    LlapIo<?> llapIo = LlapProxy.getIo();
    if (llapIo != null) {
      CacheEntryList entries = llapIo.fetchCachedContentInfo();
      responseProtoBuilder.setResult(entries);
    }
    return responseProtoBuilder.build();
  }

  private boolean determineIfSigningIsRequired(UserGroupInformation callingUser) {
    switch (isSigningRequiredConfig) {
    case FALSE: return false;
    case TRUE: return true;
    // Note that this uses short user name without consideration for Kerberos realm.
    // This seems to be the common approach (e.g. for HDFS permissions), but it may be
    // better to consider the realm (although not the host, so not the full name).
    case EXCEPT_OWNER: return !clusterUser.equals(callingUser.getShortUserName());
    default: throw new AssertionError("Unknown value " + isSigningRequiredConfig);
    }
  }

  public LlapProtocolServerImpl withTokenManager(LlapTokenManager llapTokenManager) {
    this.llapTokenManager = llapTokenManager;
    return this;
  }
}
