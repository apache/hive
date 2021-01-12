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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import static org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl.COMPUTE_NAME_PROPERTY_KEY;

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

  /**
   * Returns an unordered collection of LLAP instances whose compute group property matches the predicate.
   * @param predicate if it evaluates to true against an instance's compute group name, the instance will be included
   *                  in the result
   * @return collection of instances
   */
  Collection<LlapServiceInstance> getAllForComputeGroup(Predicate<String> predicate);

  /**
   * Helper method for getAllForComputeGroup implementors
   * @param allInstances - all the instances to apply further filtering on
   * @param predicate - the predicate to filter
   * @return
   */
  static Collection<LlapServiceInstance> getAllForComputeGroup(Collection<LlapServiceInstance> allInstances,
      Predicate<String> predicate) {
    List<LlapServiceInstance> result = new LinkedList<>();
    for (LlapServiceInstance instance : allInstances) {
      Map<String, String> properties = instance.getProperties();
      if (properties == null) {
        continue;
      }
      String computeGroup = properties.get(COMPUTE_NAME_PROPERTY_KEY);
      if (computeGroup != null && predicate.test(computeGroup)) {
        result.add(instance);
      }
    }
    return result;
  }

  /** LLAP application ID */
  ApplicationId getApplicationId();
}
