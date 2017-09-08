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
package org.apache.hadoop.hive.registry;

import java.util.Collection;
import java.util.Set;

/**
 * Note: For most of the implementations, there's no guarantee that the ServiceInstance returned by
 * one invocation is the same as the instance returned by another invocation. e.g. the ZK registry
 * returns a new ServiceInstance object each time a getInstance call is made.
 */
public interface ServiceInstanceSet<InstanceType extends ServiceInstance> {

  /**
   * Get an instance mapping which map worker identity to each instance.
   * 
   * The worker identity does not collide between restarts, so each restart will have a unique id,
   * while having the same host/ip pair.
   * 
   * @return
   */
  Collection<InstanceType> getAll();

  /**
   * Get an instance by worker identity.
   * 
   * @param name
   * @return
   */
  InstanceType getInstance(String name);

  /**
   * Get a list of service instances for a given host.
   * 
   * The list could include dead and alive instances.
   * 
   * @param host
   * @return
   */
  Set<InstanceType> getByHost(String host);

  /**
   * Get number of instances in the currently availabe.
   *
   * @return - number of instances
   */
  int size();

}
