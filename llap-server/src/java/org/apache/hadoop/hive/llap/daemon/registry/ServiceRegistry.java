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
package org.apache.hadoop.hive.llap.daemon.registry;

import java.io.IOException;

/**
 * ServiceRegistry interface for switching between fixed host and dynamic registry implementations.
 */
public interface ServiceRegistry {

  /**
   * Start the service registry
   * 
   * @throws InterruptedException
   */
  public void start() throws InterruptedException;

  /**
   * Stop the service registry
   * 
   * @throws InterruptedException
   */
  public void stop() throws InterruptedException;

  /**
   * Register the current instance - the implementation takes care of the endpoints to register.
   * 
   * @throws IOException
   */
  public void register() throws IOException;

  /**
   * Remove the current registration cleanly (implementation defined cleanup)
   * 
   * @throws IOException
   */
  public void unregister() throws IOException;

  /**
   * Client API to get the list of instances registered via the current registry key.
   * 
   * @param component
   * @return
   * @throws IOException
   */
  public ServiceInstanceSet getInstances(String component) throws IOException;
}
