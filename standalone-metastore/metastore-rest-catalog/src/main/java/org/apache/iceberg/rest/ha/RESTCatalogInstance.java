/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest.ha;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.registry.impl.ServiceInstanceBase;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import com.google.common.base.Preconditions;

/**
 * REST Catalog instance representation for HA service discovery.
 * Similar to HiveServer2Instance.
 */
public class RESTCatalogInstance extends ServiceInstanceBase {
  private boolean isLeader;
  private String restEndpoint;

  // Empty constructor for Jackson
  public RESTCatalogInstance() {
  }

  public RESTCatalogInstance(final ServiceRecord srv, final String endPointName) throws IOException {
    super(srv, endPointName);

    Endpoint activeEndpoint = srv.getInternalEndpoint(RESTCatalogHARegistry.ACTIVE_ENDPOINT);
    Endpoint passiveEndpoint = srv.getInternalEndpoint(RESTCatalogHARegistry.PASSIVE_ENDPOINT);
    this.isLeader = activeEndpoint != null;
    Preconditions.checkArgument(activeEndpoint == null || passiveEndpoint == null,
        "Incorrect service record. Both active and passive endpoints cannot be non-null!");
    
    String instanceUri = srv.get(MetastoreConf.ConfVars.REST_CATALOG_INSTANCE_URI.getVarname());
    if (instanceUri != null && !instanceUri.isEmpty()) {
      this.restEndpoint = "http://" + instanceUri + "/iceberg";
    } else {
      // Fallback: construct from host and port
      this.restEndpoint = "http://" + getHost() + ":" + getRpcPort() + "/iceberg";
    }
  }

  public boolean isLeader() {
    return isLeader;
  }

  public String getRestEndpoint() {
    return restEndpoint;
  }

  public void setLeader(final boolean leader) {
    isLeader = leader;
  }

  public void setRestEndpoint(final String restEndpoint) {
    this.restEndpoint = restEndpoint;
  }

  public boolean isActive() {
    return isLeader;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RESTCatalogInstance other = (RESTCatalogInstance) o;
    return super.equals(o) && isLeader == other.isLeader
        && Objects.equals(restEndpoint, other.restEndpoint);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(isLeader) + Objects.hashCode(restEndpoint);
  }

  @Override
  public String toString() {
    return "instanceId: " + getWorkerIdentity() + " isLeader: " + isLeader 
        + " host: " + getHost() + " port: " + getRpcPort() 
        + " restEndpoint: " + restEndpoint;
  }
}
