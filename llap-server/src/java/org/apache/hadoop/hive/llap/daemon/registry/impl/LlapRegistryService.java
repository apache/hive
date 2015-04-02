package org.apache.hadoop.hive.llap.daemon.registry.impl;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.impl.LlapDaemon;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Preconditions;

public class LlapRegistryService extends AbstractService {

  private static final Logger LOG = Logger.getLogger(LlapRegistryService.class);

  public final static String SERVICE_CLASS = "org-apache-hive";

  private RegistryOperationsService client;
  private String instanceName;
  private Configuration conf;
  private ServiceRecordMarshal encoder;

  private static final String hostname;
  
  static {
    String localhost = "localhost";
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      // ignore
    }
    hostname = localhost;
  }

  public LlapRegistryService() {
    super("LlapRegistryService");
  }

  @Override
  public void serviceInit(Configuration conf) {
    String registryId = conf.getTrimmed(LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS);
    if (registryId.startsWith("@")) {
      LOG.info("Llap Registry is enabled with registryid: " + registryId);
      this.conf = new Configuration(conf);
      conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
      // registry reference
      instanceName = registryId.substring(1);
      client = (RegistryOperationsService) RegistryOperationsFactory.createInstance(conf);
      encoder = new RegistryUtils.ServiceRecordMarshal();

    } else {
      LOG.info("Llap Registry is disabled");
    }
  }

  @Override
  public void serviceStart() throws Exception {
    if (client != null) {
      client.start();
    }
  }

  public void serviceStop() throws Exception {
    if (client != null) {
      client.stop();
    }
  }

  public Endpoint getRpcEndpoint() {
    final int rpcPort =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_RPC_PORT,
            LlapConfiguration.LLAP_DAEMON_RPC_PORT_DEFAULT);

    return RegistryTypeUtils.ipcEndpoint("llap", new InetSocketAddress(hostname, rpcPort));
  }

  public Endpoint getShuffleEndpoint() {
    final int shufflePort =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_YARN_SHUFFLE_PORT,
            LlapConfiguration.LLAP_DAEMON_YARN_SHUFFLE_PORT_DEFAULT);
    // HTTP today, but might not be
    return RegistryTypeUtils.inetAddrEndpoint("shuffle", ProtocolTypes.PROTOCOL_TCP, hostname,
        shufflePort);
  }

  private final String getPath() {
    return RegistryPathUtils.join(RegistryUtils.componentPath(RegistryUtils.currentUser(),
        SERVICE_CLASS, instanceName, "workers"), "worker-");
  }

  public void registerWorker() throws IOException {
    if (this.client != null) {
      String path = getPath();
      ServiceRecord srv = new ServiceRecord();
      srv.addInternalEndpoint(getRpcEndpoint());
      srv.addInternalEndpoint(getShuffleEndpoint());

      for (Map.Entry<String, String> kv : this.conf) {
        if (kv.getKey().startsWith(LlapConfiguration.LLAP_DAEMON_PREFIX)) {
          // TODO: read this somewhere useful, like the allocator
          srv.set(kv.getKey(), kv.getValue());
        }
      }

      client.mknode(RegistryPathUtils.parentOf(path), true);

      // FIXME: YARN registry needs to expose Ephemeral_Seq nodes & return the paths
      client.zkCreate(path, CreateMode.EPHEMERAL_SEQUENTIAL, encoder.toBytes(srv),
          client.getClientAcls());
    }
  }

  public void unregisterWorker() {
    if (this.client != null) {
      // with ephemeral nodes, there's nothing to do here
      // because the create didn't return paths
    }
  }

  public Map<String, ServiceRecord> getWorkers() throws IOException {
    if (this.client != null) {
      String path = getPath();
      return RegistryUtils.listServiceRecords(client, RegistryPathUtils.parentOf(path));
    } else {
      Preconditions.checkNotNull(this.client, "Yarn registry client is not intialized");
      return null;
    }
  }
}
