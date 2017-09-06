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
package org.apache.hadoop.hive.registry.impl;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hive.registry.ServiceInstance;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceInstanceBase implements ServiceInstance {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceInstanceBase.class);

  protected final ServiceRecord srv;
  protected final String host;
  protected final int rpcPort;

  public ServiceInstanceBase(ServiceRecord srv, String rpcName) throws IOException {
    this.srv = srv;

    if (LOG.isTraceEnabled()) {
      LOG.trace("Working with ServiceRecord: {}", srv);
    }

    final Endpoint rpc = srv.getInternalEndpoint(rpcName);

    this.host =
        RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
            AddressTypes.ADDRESS_HOSTNAME_FIELD);
    this.rpcPort =
        Integer.parseInt(RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
            AddressTypes.ADDRESS_PORT_FIELD));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServiceInstanceBase other = (ServiceInstanceBase) o;
    return this.getWorkerIdentity().equals(other.getWorkerIdentity());
  }

  @Override
  public int hashCode() {
    return getWorkerIdentity().hashCode();
  }

  @Override
  public String getWorkerIdentity() {
    return srv.get(ZkRegistryBase.UNIQUE_IDENTIFIER);
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
  public Map<String, String> getProperties() {
    return srv.attributes();
  }

  @Override
  public String toString() {
    return "DynamicServiceInstance [id=" + getWorkerIdentity() + ", host="
        + host + ":" + rpcPort + "]";
  }
}