/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.server;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.registry.impl.ServiceInstanceBase;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import com.google.common.base.Preconditions;

public class HiveServer2Instance extends ServiceInstanceBase {
  private boolean isLeader;
  private String transportMode;
  private String httpEndpoint;

  // empty c'tor to make jackson happy
  public HiveServer2Instance() {

  }

  public HiveServer2Instance(final ServiceRecord srv, final String endPointName) throws IOException {
    super(srv, endPointName);

    Endpoint activeEndpoint = srv.getInternalEndpoint(HS2ActivePassiveHARegistry.ACTIVE_ENDPOINT);
    Endpoint passiveEndpoint = srv.getInternalEndpoint(HS2ActivePassiveHARegistry.PASSIVE_ENDPOINT);
    this.isLeader = activeEndpoint != null;
    Preconditions.checkArgument(activeEndpoint == null || passiveEndpoint == null,
      "Incorrect service record. Both active and passive endpoints cannot be non-null!");
    this.transportMode = srv.get(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    if (transportMode.equalsIgnoreCase("http")) {
      this.httpEndpoint = srv.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname);
    } else {
      this.httpEndpoint = "";
    }
  }

  public boolean isLeader() {
    return isLeader;
  }

  public String getTransportMode() {
    return transportMode;
  }

  public String getHttpEndpoint() {
    return httpEndpoint;
  }

  public void setLeader(final boolean leader) {
    isLeader = leader;
  }

  public void setTransportMode(final String transportMode) {
    this.transportMode = transportMode;
  }

  public void setHttpEndpoint(final String httpEndpoint) {
    this.httpEndpoint = httpEndpoint;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HiveServer2Instance other = (HiveServer2Instance) o;
    return super.equals(o) && isLeader == other.isLeader
      && Objects.equals(transportMode, other.transportMode)
      && Objects.equals(httpEndpoint, other.httpEndpoint);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(isLeader) + Objects.hashCode(transportMode) + Objects.hashCode(httpEndpoint);
  }

  @Override
  public String toString() {
    String result = "instanceId: " + getWorkerIdentity() + " isLeader: " + isLeader + " host: " + getHost() +
      " port: " + getRpcPort() + " transportMode: " + transportMode;
    if (httpEndpoint != null) {
      result += " httpEndpoint: " + httpEndpoint;
    }
    return result;
  }
}
