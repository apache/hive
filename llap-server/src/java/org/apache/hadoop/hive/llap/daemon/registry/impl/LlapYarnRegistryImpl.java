/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.registry.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceRegistry;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Preconditions;

public class LlapYarnRegistryImpl implements ServiceRegistry {

  private static final Logger LOG = Logger.getLogger(LlapYarnRegistryImpl.class);

  private RegistryOperationsService client;
  private String instanceName;
  private Configuration conf;
  private ServiceRecordMarshal encoder;

  private final DynamicServiceInstanceSet instances = new DynamicServiceInstanceSet();

  private static final UUID uniq = UUID.randomUUID();
  private static final String hostname;

  private static final String UNIQUE_IDENTIFIER = "llap.unique.id";

  private final static String SERVICE_CLASS = "org-apache-hive";

  final ScheduledExecutorService refresher = Executors.newScheduledThreadPool(1);
  final long refreshDelay;

  static {
    String localhost = "localhost";
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      // ignore
    }
    hostname = localhost;
  }

  public LlapYarnRegistryImpl(String instanceName, Configuration conf) {

    LOG.info("Llap Registry is enabled with registryid: " + instanceName);
    this.conf = new Configuration(conf);
    this.instanceName = instanceName;
    conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    // registry reference
    client = (RegistryOperationsService) RegistryOperationsFactory.createInstance(conf);
    encoder = new RegistryUtils.ServiceRecordMarshal();
    refreshDelay =
        conf.getInt(LlapConfiguration.LLAP_DAEMON_SERVICE_REFRESH_INTERVAL,
            LlapConfiguration.LLAP_DAEMON_SERVICE_REFRESH_INTERVAL_DEFAULT);
    Preconditions.checkArgument(refreshDelay > 0,
        "Refresh delay for registry has to be positive = %d", refreshDelay);
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

  @Override
  public void register() throws IOException {
    String path = getPath();
    ServiceRecord srv = new ServiceRecord();
    srv.addInternalEndpoint(getRpcEndpoint());
    srv.addInternalEndpoint(getShuffleEndpoint());

    for (Map.Entry<String, String> kv : this.conf) {
      if (kv.getKey().startsWith(LlapConfiguration.LLAP_DAEMON_PREFIX)
          || kv.getKey().startsWith("hive.llap.")) {
        // TODO: read this somewhere useful, like the task scheduler
        srv.set(kv.getKey(), kv.getValue());
      }
    }

    // restart sensitive instance id
    srv.set(UNIQUE_IDENTIFIER, uniq.toString());

    client.mknode(RegistryPathUtils.parentOf(path), true);

    // FIXME: YARN registry needs to expose Ephemeral_Seq nodes & return the paths
    client.zkCreate(path, CreateMode.EPHEMERAL_SEQUENTIAL, encoder.toBytes(srv),
        client.getClientAcls());
  }

  @Override
  public void unregister() throws IOException {
   // Nothing for the zkCreate models
  }

  private class DynamicServiceInstance implements ServiceInstance {

    private final ServiceRecord srv;
    private boolean alive = true;
    private final String host;
    private final int rpcPort;
    private final int shufflePort;

    public DynamicServiceInstance(ServiceRecord srv) throws IOException {
      this.srv = srv;

      final Endpoint shuffle = srv.getInternalEndpoint("shuffle");
      final Endpoint rpc = srv.getInternalEndpoint("llap");

      this.host =
          RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_HOSTNAME_FIELD);
      this.rpcPort =
          Integer.valueOf(RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
      this.shufflePort =
          Integer.valueOf(RegistryTypeUtils.getAddressField(shuffle.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
    }

    @Override
    public String getWorkerIdentity() {
      return srv.get(UNIQUE_IDENTIFIER);
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getRpcPort() {
      return rpcPort;
    }

    @Override
    public int getShufflePort() {
      return shufflePort;
    }

    @Override
    public boolean isAlive() {
      return alive ;
    }

    public void kill() {
      LOG.info("Killing " + this);
      this.alive = false;
    }

    @Override
    public Map<String, String> getProperties() {
      return srv.attributes();
    }

    @Override
    public Resource getResource() {
      int memory = Integer.valueOf(srv.get(LlapConfiguration.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB));
      int vCores = Integer.valueOf(srv.get(LlapConfiguration.LLAP_DAEMON_NUM_EXECUTORS));
      return Resource.newInstance(memory, vCores);
    }

    @Override
    public String toString() {
      return "DynamicServiceInstance [alive=" + alive + ", host=" + host + ":" + rpcPort + "]";
    }
  }

  private class DynamicServiceInstanceSet implements ServiceInstanceSet {

    Map<String, ServiceInstance> instances;

    @Override
    public Map<String, ServiceInstance> getAll() {
      return instances;
    }

    @Override
    public ServiceInstance getInstance(String name) {
      return instances.get(name);
    }

    @Override
    public synchronized void refresh() throws IOException {
      /* call this from wherever */
      Map<String, ServiceInstance> freshInstances = new HashMap<String, ServiceInstance>();

      String path = getPath();
      Map<String, ServiceRecord> records =
          RegistryUtils.listServiceRecords(client, RegistryPathUtils.parentOf(path));
      Set<String> latestKeys = new HashSet<String>();
      LOG.info("Starting to refresh ServiceInstanceSet " + System.identityHashCode(this));
      for (ServiceRecord rec : records.values()) {
        ServiceInstance instance = new DynamicServiceInstance(rec);
        if (instance != null) {
          if (instances != null && instances.containsKey(instance.getWorkerIdentity()) == false) {
            // add a new object
            freshInstances.put(instance.getWorkerIdentity(), instance);
            if (LOG.isInfoEnabled()) {
              LOG.info("Adding new worker " + instance.getWorkerIdentity() + " which mapped to "
                  + instance);
            }
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("Retaining running worker " + instance.getWorkerIdentity() + " which mapped to " + instance);
          }
        }
        latestKeys.add(instance.getWorkerIdentity());
      }

      if (instances != null) {
        // deep-copy before modifying
        Set<String> oldKeys = new HashSet(instances.keySet());
        if (oldKeys.removeAll(latestKeys)) {
          for (String k : oldKeys) {
            // this is so that people can hold onto ServiceInstance references as placeholders for tasks
            final DynamicServiceInstance dead = (DynamicServiceInstance) instances.get(k);
            dead.kill();
            if (LOG.isInfoEnabled()) {
              LOG.info("Deleting dead worker " + k + " which mapped to " + dead);
            }
          }
        }
        this.instances.keySet().removeAll(oldKeys);
        this.instances.putAll(freshInstances);
      } else {
        this.instances = freshInstances;
      }
    }

    @Override
    public Set<ServiceInstance> getByHost(String host) {
      Set<ServiceInstance> byHost = new HashSet<ServiceInstance>();
      for (ServiceInstance i : instances.values()) {
        if (host.equals(i.getHost())) {
          // all hosts in instances should be alive in this impl
          byHost.add(i);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Locality comparing " + host + " to " + i.getHost());
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Returning " + byHost.size() + " hosts for locality allocation on " + host);
      }
      return byHost;
    }
  }

  @Override
  public ServiceInstanceSet getInstances(String component) throws IOException {
    Preconditions.checkArgument("LLAP".equals(component)); // right now there is only 1 component 
    if (this.client != null) {
      instances.refresh();
      return instances;
    } else {
      Preconditions.checkNotNull(this.client, "Yarn registry client is not intialized");
      return null;
    }
  }

  @Override
  public void start() {
    if (client != null) {
      client.start();
      refresher.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            instances.refresh();
          } catch (IOException ioe) {
            LOG.warn("Could not refresh hosts during scheduled refresh", ioe);
          }
        }
      }, 0, refreshDelay, TimeUnit.SECONDS);
    }
  }

  @Override
  public void stop() {
    if (client != null) {
      client.stop();
    }
  }
}
