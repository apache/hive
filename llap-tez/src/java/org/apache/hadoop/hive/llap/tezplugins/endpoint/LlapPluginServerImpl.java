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

package org.apache.hadoop.hive.llap.tezplugins.endpoint;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryRequestProto;
import org.apache.hadoop.hive.llap.plugin.rpc.LlapPluginProtocolProtos.UpdateQueryResponseProto;
import org.apache.hadoop.hive.llap.protocol.LlapPluginProtocolPB;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService;
import org.apache.tez.common.security.JobTokenIdentifier;

public class LlapPluginServerImpl extends AbstractService implements LlapPluginProtocolPB {
  private static final Logger LOG = LoggerFactory.getLogger(LlapPluginServerImpl.class);

  private final int port;
  private RPC.Server server;
  private final SecretManager<JobTokenIdentifier> secretManager;
  private final int numHandlers;
  private final LlapTaskSchedulerService parent;

  public LlapPluginServerImpl(SecretManager<JobTokenIdentifier> secretManager,
      int numHandlers, int port, LlapTaskSchedulerService parent) {
    super("LlapPluginServerImpl");
    this.secretManager = secretManager;
    this.port = port;
    this.numHandlers = numHandlers;
    this.parent = parent;
    LOG.info("Creating the plugin endpoint on port " + port);
  }

  @Override
  public UpdateQueryResponseProto updateQuery(RpcController controller,
      UpdateQueryRequestProto request) throws ServiceException {
    parent.updateQuery(request);
    return UpdateQueryResponseProto.getDefaultInstance();
  }

  @Override
  public void serviceStart() {
    final Configuration conf = getConfig();
    final BlockingService daemonImpl =
        LlapPluginProtocolProtos.LlapPluginProtocol.newReflectiveBlockingService(this);
    server = LlapUtil.startProtocolServer(port, numHandlers, null, conf, daemonImpl,
        LlapPluginProtocolPB.class, secretManager, new LlapPluginPolicyProvider(),
        ConfVars.LLAP_PLUGIN_ACL, ConfVars.LLAP_PLUGIN_ACL_DENY);
  }

  @Override
  public void serviceStop() {
    if (server != null) {
      server.stop();
    }
  }
}
