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


import org.apache.hadoop.hive.registry.ServiceInstance;

import org.apache.hadoop.yarn.api.records.Resource;

public interface LlapServiceInstance extends ServiceInstance {

  /**
   * Management endpoint for service instance
   *
   * @return
   */
  public int getManagementPort();

  /**
   * Shuffle Endpoint for service instance
   * 
   * @return
   */
  public int getShufflePort();


  /**
   * Address for services hosted on http
   * @return
   */
  public String getServicesAddress();
  /**
   * OutputFormat endpoint for service instance
   *
   * @return
   */
  public int getOutputFormatPort();


  /**
   * Memory and Executors available for the LLAP tasks
   * 
   * This does not include the size of the cache or the actual vCores allocated via YARN.
   * 
   * @return
   */
  public Resource getResource();
}
