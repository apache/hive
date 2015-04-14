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
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceRegistry;
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
  
  private ServiceRegistry registry = null;

  public LlapRegistryService() {
    super("LlapRegistryService");
  }

  @Override
  public void serviceInit(Configuration conf) {
    String hosts = conf.getTrimmed(LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS);
    if (hosts.startsWith("@")) {
      registry = initRegistry(hosts.substring(1), conf);
    } else {
      registry = new LlapFixedRegistryImpl(hosts, conf);
    }
    LOG.info("Using LLAP registry type " + registry);
  }

  private ServiceRegistry initRegistry(String instanceName, Configuration conf) {
    return new LlapYarnRegistryImpl(instanceName, conf);
  }

  @Override
  public void serviceStart() throws Exception {
    if (this.registry != null) {
      this.registry.start();
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (this.registry != null) {
      this.registry.start();
    } else {
      LOG.warn("Stopping non-existent registry service");
    }
  }

  public void registerWorker() throws IOException {
    if (this.registry != null) {
      this.registry.register();
    }
  }

  public void unregisterWorker() throws IOException {
    if (this.registry != null) {
      this.registry.unregister();
    }
  }

  public ServiceInstanceSet getInstances() throws IOException {
    return this.registry.getInstances("LLAP");
  }
}
