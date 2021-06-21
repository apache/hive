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
import java.util.Objects;

import org.apache.hadoop.hive.registry.ServiceInstance;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceInstanceBase implements ServiceInstance {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceInstanceBase.class);
  private String host;
  private int rpcPort;
  private String workerIdentity;
  private Map<String, String> properties;

  // empty c'tor to make jackson happy
  public ServiceInstanceBase() {

  }

  public ServiceInstanceBase(ServiceRecord srv, String rpcName) throws IOException {
    LOG.trace("Working with ServiceRecord: {}", srv);
    final Endpoint rpc = srv.getInternalEndpoint(rpcName);
    this.host = RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
        AddressTypes.ADDRESS_HOSTNAME_FIELD);
    this.rpcPort = Integer.parseInt(RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
        AddressTypes.ADDRESS_PORT_FIELD));
    this.workerIdentity = srv.get(ZkRegistryBase.UNIQUE_IDENTIFIER);
    this.properties = srv.attributes();
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
    return Objects.equals(getWorkerIdentity(), other.getWorkerIdentity())
      && Objects.equals(host, other.host)
      && Objects.equals(rpcPort, other.rpcPort);
  }

  @Override
  public int hashCode() {
    return getWorkerIdentity().hashCode() + (31 * host.hashCode()) + (31 * rpcPort);
  }

  @Override
  public String getWorkerIdentity() {
    return workerIdentity;
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
    return properties;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public void setRpcPort(final int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public void setWorkerIdentity(final String workerIdentity) {
    this.workerIdentity = workerIdentity;
  }

  public void setProperties(final Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "DynamicServiceInstance [id=" + getWorkerIdentity() + ", host="
      + host + ":" + rpcPort + "]";
  }
}