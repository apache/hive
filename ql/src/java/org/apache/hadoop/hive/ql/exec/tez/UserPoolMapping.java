/**
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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager.TmpUserMapping;

class UserPoolMapping {
  private final static class Mapping {
    public Mapping(String poolName, int priority) {
      this.fullPoolName = poolName;
      this.priority = priority;
    }
    int priority;
    String fullPoolName;
    @Override
    public String toString() {
      return "[" + fullPoolName + ", priority=" + priority + "]";
    }
  }

  private final Map<String, Mapping> userMappings = new HashMap<>();
  private final String defaultPoolName;
  // TODO: add other types as needed

  public UserPoolMapping(List<TmpUserMapping> mappings) {
    String defaultPoolName = null;
    for (TmpUserMapping mapping : mappings) {
      switch (mapping.getType()) {
      case USER: {
          Mapping val = new Mapping(mapping.getPoolName(), mapping.getPriority());
          Mapping oldValue = userMappings.put(mapping.getName(), val);
          if (oldValue != null) {
            throw new AssertionError("Duplicate mapping for user " + mapping.getName() + "; "
                + oldValue + " and " + val);
          }
        break;
      }
      case DEFAULT: {
        String poolName = mapping.getPoolName();
        if (defaultPoolName != null) {
          throw new AssertionError("Duplicate default mapping; "
              + defaultPoolName + " and " + poolName);
        }
        defaultPoolName = poolName;
        break;
      }
      default: throw new AssertionError("Unknown type " + mapping.getType());
      }
    }
    this.defaultPoolName = defaultPoolName;
  }

  public String mapSessionToPoolName(String userName) {
    // For now, we only have user rules, so this is very simple.
    // In future we'd also look up groups (each groups the user is in initially; we may do it
    // the opposite way if the user is a member of many groups but there are not many rules),
    // whatever user supplies in connection string to HS2, etc.
    // If multiple rules match, we'd need to get the highest priority one.
    Mapping userMapping = userMappings.get(userName);
    if (userMapping != null) return userMapping.fullPoolName;
    return defaultPoolName;
  }
}