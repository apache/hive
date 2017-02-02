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

package org.apache.hadoop.hive.llap.registry.impl;

import java.util.Map;

import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.yarn.api.records.Resource;

public class InactiveServiceInstance implements ServiceInstance {
  private final String name;
  public InactiveServiceInstance(String name) {
    this.name = name;
  }

  @Override
  public String getWorkerIdentity() {
    return name;
  }

  @Override
  public String getHost() {
    return null;
  }

  @Override
  public int getRpcPort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getManagementPort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getShufflePort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getServicesAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getOutputFormatPort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> getProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Resource getResource() {
    throw new UnsupportedOperationException();
  }
}
