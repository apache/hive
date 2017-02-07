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

import java.util.Collection;
import java.util.Set;

/**
 * Note: For most of the implementations, there's no guarantee that the ServiceInstance returned by
 * one invocation is the same as the instance returned by another invocation. e.g. the ZK registry
 * returns a new ServiceInstance object each time a getInstance call is made.
 */
public interface ServiceInstanceSet {

  /**
   * Get an instance mapping which map worker identity to each instance.
   * 
   * The worker identity does not collide between restarts, so each restart will have a unique id,
   * while having the same host/ip pair.
   * 
   * @return
   */
  Collection<ServiceInstance> getAll();

  /**
   * Gets a list containing all the instances. This list has the same iteration order across
   * different processes, assuming the list of registry entries is the same.
   * @param consistentIndexes if true, also try to maintain the same exact index for each node
   *                          across calls, by inserting inactive instances to replace the
   *                          removed ones.
   */
  Collection<ServiceInstance> getAllInstancesOrdered(boolean consistentIndexes);

  /**
   * Get an instance by worker identity.
   * 
   * @param name
   * @return
   */
  ServiceInstance getInstance(String name);

  /**
   * Get a list of service instances for a given host.
   * 
   * The list could include dead and alive instances.
   * 
   * @param host
   * @return
   */
  Set<ServiceInstance> getByHost(String host);

  /**
   * Get number of instances in the currently availabe.
   *
   * @return - number of instances
   */
  int size();
}
