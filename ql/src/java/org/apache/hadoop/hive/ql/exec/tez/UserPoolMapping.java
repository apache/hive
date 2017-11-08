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

import org.apache.hadoop.hive.metastore.api.WMMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class UserPoolMapping {
  public static enum MappingType {
    USER, DEFAULT
  }

  private final static class Mapping {
    public Mapping(String poolName, int priority) {
      this.fullPoolName = poolName;
      this.priority = priority;
    }
    int priority;
    /** The destination pool; null means unmanaged. */
    String fullPoolName;
    @Override
    public String toString() {
      return "[" + (fullPoolName == null ? "unmanaged" : fullPoolName)
          + ", priority=" + priority + "]";
    }
  }

  /** Contains all the information necessary to map a query to a pool. */
  public static final class MappingInput {
    public final String userName;
    // TODO: we may add app name, group name, etc. later

    public MappingInput(String userName) {
      this.userName = userName;
    }

    @Override
    public String toString() {
      return userName;
    }
  }


  private final Map<String, Mapping> userMappings = new HashMap<>();
  private final String defaultPoolPath;

  public UserPoolMapping(List<WMMapping> mappings, String defaultPoolPath) {
    if (mappings != null) {
      for (WMMapping mapping : mappings) {
        MappingType type = MappingType.valueOf(mapping.getEntityType().toUpperCase());
        switch (type) {
        case USER: {
          Mapping val = new Mapping(mapping.getPoolName(), mapping.getOrdering());
          Mapping oldValue = userMappings.put(mapping.getEntityName(), val);
          if (oldValue != null) {
            throw new AssertionError("Duplicate mapping for user " + mapping.getEntityName()
                + "; " + oldValue + " and " + val);
          }
          break;
        }
        default: throw new AssertionError("Unknown type " + type);
        }
      }
    }
    this.defaultPoolPath = defaultPoolPath;
  }

  public String mapSessionToPoolName(MappingInput input) {
    // For now, we only have user rules, so this is very simple.
    // In future we'd also look up groups (each groups the user is in initially; we may do it
    // the opposite way if the user is a member of many groups but there are not many rules),
    // whatever user supplies in connection string to HS2, etc.
    // If multiple rules match, we'd need to get the highest priority one.
    Mapping userMapping = userMappings.get(input.userName);
    if (userMapping != null) return userMapping.fullPoolName;
    return defaultPoolPath;
  }
}