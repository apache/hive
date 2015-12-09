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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapFixedRegistryImpl implements ServiceRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(LlapFixedRegistryImpl.class);

  @InterfaceAudience.Private
  // This is primarily for testing to avoid the host lookup
  public static final String FIXED_REGISTRY_RESOLVE_HOST_NAMES = "fixed.registry.resolve.host.names";

  private final int port;
  private final int shuffle;
  private final int mngPort;
  private final String[] hosts;
  private final int memory;
  private final int vcores;
  private final boolean resolveHosts;

  private final Map<String, String> srv = new HashMap<String, String>();

  public LlapFixedRegistryImpl(String hosts, Configuration conf) {
    this.hosts = hosts.split(",");
    this.port = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_RPC_PORT);
    this.shuffle = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_YARN_SHUFFLE_PORT);
    this.resolveHosts = conf.getBoolean(FIXED_REGISTRY_RESOLVE_HOST_NAMES, true);
    this.mngPort = HiveConf.getIntVar(conf, ConfVars.LLAP_MANAGEMENT_RPC_PORT);

    for (Map.Entry<String, String> kv : conf) {
      if (kv.getKey().startsWith(LlapConfiguration.LLAP_DAEMON_PREFIX)
          || kv.getKey().startsWith("hive.llap.")
          || kv.getKey().startsWith(LlapConfiguration.LLAP_PREFIX)) {
        // TODO: read this somewhere useful, like the task scheduler
        srv.put(kv.getKey(), kv.getValue());
      }
    }

    this.memory = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB);
    this.vcores = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
  }

  @Override
  public void start() throws InterruptedException {
    // nothing to start
  }

  @Override
  public void stop() throws InterruptedException {
    // nothing to stop
  }

  @Override
  public void register() throws IOException {
    // nothing to register
  }

  @Override
  public void unregister() throws IOException {
    // nothing to unregister
  }

  public static String getWorkerIdentity(String host) {
    // trigger clean errors for anyone who mixes up identity with hosts
    return "host-" + host;
  }

  private final class FixedServiceInstance implements ServiceInstance {

    private final String host;

    public FixedServiceInstance(String host) {
      if (resolveHosts) {
        try {
          InetAddress inetAddress = InetAddress.getByName(host);
          if (NetUtils.isLocalAddress(inetAddress)) {
            InetSocketAddress socketAddress = new InetSocketAddress(0);
            socketAddress = NetUtils.getConnectAddress(socketAddress);
            LOG.info("Adding host identified as local: " + host + " as "
                + socketAddress.getHostName());
            host = socketAddress.getHostName();
          }
        } catch (UnknownHostException e) {
          LOG.warn("Ignoring resolution issues for host: " + host, e);
        }
      }
      this.host = host;
    }

    @Override
    public String getWorkerIdentity() {
      return LlapFixedRegistryImpl.getWorkerIdentity(host);
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getRpcPort() {
      // TODO: allow >1 port per host?
      return LlapFixedRegistryImpl.this.port;
    }

    @Override
    public int getManagementPort() {
      return LlapFixedRegistryImpl.this.mngPort;
    }

    @Override
    public int getShufflePort() {
      return LlapFixedRegistryImpl.this.shuffle;
    }

    @Override
    public boolean isAlive() {
      return true;
    }

    @Override
    public Map<String, String> getProperties() {
      Map<String, String> properties = new HashMap<>(srv);
      // no worker identity
      return properties;
    }

    @Override
    public Resource getResource() {
      return Resource.newInstance(memory, vcores);
    }

    @Override
    public String toString() {
      return "FixedServiceInstance{" +
          "host=" + host +
          ", memory=" + memory +
          ", vcores=" + vcores +
          '}';
    }
  }

  private final class FixedServiceInstanceSet implements ServiceInstanceSet {

    private final Map<String, ServiceInstance> instances = new HashMap<String, ServiceInstance>();

    public FixedServiceInstanceSet() {
      for (String host : hosts) {
        // trigger bugs in anyone who uses this as a hostname
        instances.put(getWorkerIdentity(host), new FixedServiceInstance(host));
      }
    }

    @Override
    public Map<String, ServiceInstance> getAll() {
      return instances;
    }

    @Override
    public ServiceInstance getInstance(String name) {
      return instances.get(name);
    }

    @Override
    public Set<ServiceInstance> getByHost(String host) {
      Set<ServiceInstance> byHost = new HashSet<ServiceInstance>();
      ServiceInstance inst = getInstance(getWorkerIdentity(host));
      if (inst != null) {
        byHost.add(inst);
      }
      return byHost;
    }

    @Override
    public void refresh() throws IOException {
      // I will do no such thing
    }

  }

  @Override
  public ServiceInstanceSet getInstances(String component) throws IOException {
    return new FixedServiceInstanceSet();
  }

  @Override
  public String toString() {
    return String.format("FixedRegistry hosts=%s", StringUtils.join(",", this.hosts));
  }
}
