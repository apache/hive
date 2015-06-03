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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.daemon.registry.ServiceRegistry;
import org.apache.hadoop.service.AbstractService;
import org.apache.log4j.Logger;

public class LlapRegistryService extends AbstractService {

  private static final Logger LOG = Logger.getLogger(LlapRegistryService.class);

  private ServiceRegistry registry = null;
  private final boolean isDaemon;

  public LlapRegistryService(boolean isDaemon) {
    super("LlapRegistryService");
    this.isDaemon = isDaemon;
  }

  @Override
  public void serviceInit(Configuration conf) {
    String hosts = conf.getTrimmed(LlapConfiguration.LLAP_DAEMON_SERVICE_HOSTS);
    if (hosts.startsWith("@")) {
      registry = new LlapYarnRegistryImpl(hosts.substring(1), conf, isDaemon);
    } else {
      registry = new LlapFixedRegistryImpl(hosts, conf);
    }
    LOG.info("Using LLAP registry type " + registry);
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
