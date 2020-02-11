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
package org.apache.hadoop.hive.llap.registry;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hive.registry.ServiceInstance;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * ServiceRegistry interface for switching between fixed host and dynamic registry implementations.
 */
public interface ServiceRegistry<T extends ServiceInstance> {

  /**
   * Start the service registry
   */
  void start() throws IOException;

  /**
   * Stop the service registry
   */
  void stop() throws IOException;

  /**
   * Register the current instance - the implementation takes care of the endpoints to register.
   * @return self identifying name
   */
  String register() throws IOException;

  /**
   * Remove the current registration cleanly (implementation defined cleanup)
   */
  void unregister() throws IOException;

  /**
   * Update the current registration with the given attributes.
   */
  void updateRegistration(Iterable<Map.Entry<String, String>> attributes) throws IOException;

  /**
   * Client API to get the list of instances registered via the current registry key.
   * @param component
   * @param clusterReadyTimeoutMs The time to wait for the cluster to be ready, if it's not
   *                              started yet. 0 means do not wait.
   */
  ServiceInstanceSet<T> getInstances(String component, long clusterReadyTimeoutMs) throws
    IOException;

  /**
   * Adds state change listeners for service instances.
   * @param listener - state change listener
   */
  void registerStateChangeListener(ServiceInstanceStateChangeListener<T> listener) throws IOException;

  /**
   * @return The application ID of the LLAP cluster.
   */
  ApplicationId getApplicationId() throws IOException;
}