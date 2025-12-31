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

import java.util.Collection;
import java.util.Set;

/**
 * Interface for REST Catalog HA instance set.
 * Similar to HiveServer2HAInstanceSet.
 */
public interface RESTCatalogHAInstanceSet {
  /**
   * In Active/Passive setup, returns current active leader.
   * @return leader instance
   */
  RESTCatalogInstance getLeader();

  /**
   * Get all REST Catalog instances.
   * @return collection of instances
   */
  Collection<RESTCatalogInstance> getAll();

  /**
   * Get instance by instance ID.
   * @param instanceId instance ID
   * @return instance or null if not found
   */
  RESTCatalogInstance getInstance(String instanceId);

  /**
   * Get instances by host.
   * @param host hostname
   * @return set of instances on that host
   */
  Set<RESTCatalogInstance> getByHost(String host);

  /**
   * Get number of instances.
   * @return size
   */
  int size();
}

