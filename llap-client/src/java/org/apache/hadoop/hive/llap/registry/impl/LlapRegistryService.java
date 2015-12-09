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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapRegistryService extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapRegistryService.class);

  private ServiceRegistry registry = null;
  private final boolean isDaemon;

  public LlapRegistryService(boolean isDaemon) {
    super("LlapRegistryService");
    this.isDaemon = isDaemon;
  }

  @Override
  public void serviceInit(Configuration conf) {
    String hosts = HiveConf.getTrimmedVar(conf, ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
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
    if (isDaemon) {
      registerWorker();
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (isDaemon) {
      unregisterWorker();
    }
    if (this.registry != null) {
      this.registry.stop();
    } else {
      LOG.warn("Stopping non-existent registry service");
    }
  }

  private void registerWorker() throws IOException {
    if (this.registry != null) {
      this.registry.register();
    }
  }

  private void unregisterWorker() throws IOException {
    if (this.registry != null) {
      this.registry.unregister();
    }
  }

  public ServiceInstanceSet getInstances() throws IOException {
    return this.registry.getInstances("LLAP");
  }
}
