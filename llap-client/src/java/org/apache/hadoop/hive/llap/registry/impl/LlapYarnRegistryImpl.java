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
package org.apache.hadoop.hive.llap.registry.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class LlapYarnRegistryImpl implements ServiceRegistry {

  /** IPC endpoint names. */
  private static final String IPC_SERVICES = "services",
      IPC_MNG = "llapmng", IPC_SHUFFLE = "shuffle", IPC_LLAP = "llap";

  private static final Logger LOG = LoggerFactory.getLogger(LlapYarnRegistryImpl.class);

  private final RegistryOperationsService client;
  private final Configuration conf;
  private final ServiceRecordMarshal encoder;
  private final String path;

  private final DynamicServiceInstanceSet instances = new DynamicServiceInstanceSet();

  private static final UUID uniq = UUID.randomUUID();
  private static final String hostname;

  private static final String UNIQUE_IDENTIFIER = "llap.unique.id";

  private final static String SERVICE_CLASS = "org-apache-hive";

  final ScheduledExecutorService refresher = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LlapYarnRegistryRefresher").build());
  final long refreshDelay;
  private final boolean isDaemon;

  static {
    String localhost = "localhost";
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      // ignore
    }
    hostname = localhost;
  }

  public LlapYarnRegistryImpl(String instanceName, Configuration conf, boolean isDaemon) {

    LOG.info("Llap Registry is enabled with registryid: " + instanceName);
    this.conf = new Configuration(conf);
    conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    // registry reference
    client = (RegistryOperationsService) RegistryOperationsFactory.createInstance(conf);
    encoder = new RegistryUtils.ServiceRecordMarshal();
    this.path = RegistryPathUtils.join(RegistryUtils.componentPath(RegistryUtils.currentUser(),
        SERVICE_CLASS, instanceName, "workers"), "worker-");
    refreshDelay = HiveConf.getTimeVar(
        conf, ConfVars.LLAP_DAEMON_SERVICE_REFRESH_INTERVAL, TimeUnit.SECONDS);
    this.isDaemon = isDaemon;
    Preconditions.checkArgument(refreshDelay > 0,
        "Refresh delay for registry has to be positive = %d", refreshDelay);
  }

  public Endpoint getRpcEndpoint() {
    final int rpcPort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_RPC_PORT);
    return RegistryTypeUtils.ipcEndpoint(IPC_LLAP, new InetSocketAddress(hostname, rpcPort));
  }

  public Endpoint getShuffleEndpoint() {
    final int shufflePort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_YARN_SHUFFLE_PORT);
    // HTTP today, but might not be
    return RegistryTypeUtils.inetAddrEndpoint(IPC_SHUFFLE, ProtocolTypes.PROTOCOL_TCP, hostname,
        shufflePort);
  }

  public Endpoint getServicesEndpoint() {
    final int servicePort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_WEB_PORT);
    final boolean isSSL = HiveConf.getBoolVar(conf, ConfVars.LLAP_DAEMON_WEB_SSL);
    final String scheme = isSSL ? "https" : "http";
    final URL serviceURL;
    try {
      serviceURL = new URL(scheme, hostname, servicePort, "");
      return RegistryTypeUtils.webEndpoint(IPC_SERVICES, serviceURL.toURI());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("llap service URI for " + hostname + " is invalid", e);
    }
  }

  public Endpoint getMngEndpoint() {
    return RegistryTypeUtils.ipcEndpoint(IPC_MNG, new InetSocketAddress(hostname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_MANAGEMENT_RPC_PORT)));
  }

  private final String getPath() {
    return this.path;
  }

  @Override
  public void register() throws IOException {
    String path = getPath();
    ServiceRecord srv = new ServiceRecord();
    srv.addInternalEndpoint(getRpcEndpoint());
    srv.addInternalEndpoint(getMngEndpoint());
    srv.addInternalEndpoint(getShuffleEndpoint());
    srv.addExternalEndpoint(getServicesEndpoint());

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
    private final int mngPort;
    private final int shufflePort;

    public DynamicServiceInstance(ServiceRecord srv) throws IOException {
      this.srv = srv;

      final Endpoint shuffle = srv.getInternalEndpoint(IPC_SHUFFLE);
      final Endpoint rpc = srv.getInternalEndpoint(IPC_LLAP);
      final Endpoint mng = srv.getInternalEndpoint(IPC_MNG);

      this.host =
          RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_HOSTNAME_FIELD);
      this.rpcPort =
          Integer.valueOf(RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
      this.mngPort =
          Integer.valueOf(RegistryTypeUtils.getAddressField(mng.addresses.get(0),
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
      // May be possible to generate a notification back to the scheduler from here.
      LOG.info("Killing service instance: " + this);
      this.alive = false;
    }

    @Override
    public Map<String, String> getProperties() {
      return srv.attributes();
    }

    @Override
    public Resource getResource() {
      int memory = Integer.valueOf(srv.get(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname));
      int vCores = Integer.valueOf(srv.get(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname));
      return Resource.newInstance(memory, vCores);
    }

    @Override
    public String toString() {
      return "DynamicServiceInstance [alive=" + alive + ", host=" + host + ":" + rpcPort + " with resources=" + getResource() +"]";
    }

    @Override
    public int getManagementPort() {
      return mngPort;
    }

    // Relying on the identity hashCode and equality, since refreshing instances retains the old copy
    // of an already known instance.
  }

  private class DynamicServiceInstanceSet implements ServiceInstanceSet {

    // LinkedHashMap to retain iteration order.
    private final Map<String, ServiceInstance> instances = new LinkedHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    @Override
    public Map<String, ServiceInstance> getAll() {
      // Return a copy. Instances may be modified during a refresh.
      readLock.lock();
      try {
        return new LinkedHashMap<>(instances);
      } finally {
        readLock.unlock();
      }
    }

    @Override
    public List<ServiceInstance> getAllInstancesOrdered() {
      List<ServiceInstance> list = new LinkedList<>();
      readLock.lock();
      try {
        list.addAll(instances.values());
      } finally {
        readLock.unlock();
      }
      Collections.sort(list, new Comparator<ServiceInstance>() {
        @Override
        public int compare(ServiceInstance o1, ServiceInstance o2) {
          return o2.getWorkerIdentity().compareTo(o2.getWorkerIdentity());
        }
      });
      return list;
    }

    @Override
    public ServiceInstance getInstance(String name) {
      readLock.lock();
      try {
        return instances.get(name);
      } finally {
        readLock.unlock();
      }
    }

    @Override
    public  void refresh() throws IOException {
      /* call this from wherever */
      Map<String, ServiceInstance> freshInstances = new HashMap<String, ServiceInstance>();

      String path = getPath();
      Map<String, ServiceRecord> records =
          RegistryUtils.listServiceRecords(client, RegistryPathUtils.parentOf(path));
      // Synchronize after reading the service records from the external service (ZK)
      writeLock.lock();
      try {
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
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Retaining running worker " + instance.getWorkerIdentity() +
                    " which mapped to " + instance);
              }
            }
          }
          latestKeys.add(instance.getWorkerIdentity());
        }

        if (instances != null) {
          // deep-copy before modifying
          Set<String> oldKeys = new HashSet<>(instances.keySet());
          if (oldKeys.removeAll(latestKeys)) {
            // This is all the records which have not checked in, and are effectively dead.
            for (String k : oldKeys) {
              // this is so that people can hold onto ServiceInstance references as placeholders for tasks
              final DynamicServiceInstance dead = (DynamicServiceInstance) instances.get(k);
              dead.kill();
              if (LOG.isInfoEnabled()) {
                LOG.info("Deleting dead worker " + k + " which mapped to " + dead);
              }
            }
          }
          // oldKeys contains the set of dead instances at this point.
          this.instances.keySet().removeAll(oldKeys);
          this.instances.putAll(freshInstances);
        } else {
          this.instances.putAll(freshInstances);
        }
      } finally {
        writeLock.unlock();
      }
    }

    @Override
    public Set<ServiceInstance> getByHost(String host) {
      // TODO Maybe store this as a map which is populated during construction, to avoid walking
      // the map on each request.
      readLock.lock();
      Set<ServiceInstance> byHost = new HashSet<ServiceInstance>();
      try {
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
      } finally {
        readLock.unlock();
      }
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
    if (client == null) return;
    client.start();
    if (isDaemon) return;
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

  @Override
  public void stop() {
    if (client != null) {
      client.stop();
    }
  }
}
