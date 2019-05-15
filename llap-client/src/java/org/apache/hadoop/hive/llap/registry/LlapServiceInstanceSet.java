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
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface LlapServiceInstanceSet extends ServiceInstanceSet<LlapServiceInstance> {

  /**
   * Gets a list containing all the instances. This list has the same iteration order across
   * different processes, assuming the list of registry entries is the same.
   * @param consistentIndexes if true, also try to maintain the same exact index for each node
   *                          across calls, by inserting inactive instances to replace the
   *                          removed ones.
   */
  Collection<LlapServiceInstance> getAllInstancesOrdered(
      boolean consistentIndexes);

  /** LLAP application ID */
  ApplicationId getApplicationId();
}
