/*
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

import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

class UserPoolMapping {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(UserPoolMapping.class);

  public static enum MappingType {
    USER, GROUP, APPLICATION
  }

  private final Map<String, Mapping> userMappings = new HashMap<>(),
      groupMappings = new HashMap<>(), appMappings = new HashMap<>();
  private final String defaultPoolPath;

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
    private final String userName, wmPool, appName;
    private final List<String> groups;
    // TODO: we may add app name, etc. later

    public MappingInput(String userName, List<String> groups, String wmPool, String appName) {
      this.userName = userName;
      this.groups = groups;
      this.appName = appName;
      this.wmPool = wmPool;
    }

    public List<String> getGroups() {
      return groups == null ? Lists.<String>newArrayList() : groups;
    }

    private String getUserName() {
      return userName;
    }

    @Override
    public String toString() {
      return "{" + userName + "; app " + appName
          + "; pool " + wmPool + "; groups " + groups + "}";
    }

    public String getAppName() {
      return appName;
    }
  }


  public UserPoolMapping(List<WMMapping> mappings, String defaultPoolPath) {
    if (mappings != null) {
      for (WMMapping mapping : mappings) {
        MappingType type = MappingType.valueOf(mapping.getEntityType().toUpperCase());
        switch (type) {
        case USER: {
          addMapping(mapping, userMappings, "user");
          break;
        }
        case GROUP: {
          addMapping(mapping, groupMappings, "group");
          break;
        }
        case APPLICATION: {
          addMapping(mapping, appMappings, "application");
          break;
        }
        default: throw new AssertionError("Unknown type " + type);
        }
      }
    }
    this.defaultPoolPath = defaultPoolPath;
  }

  private static void addMapping(WMMapping mapping, Map<String, Mapping> map, String text) {
    Mapping val = new Mapping(mapping.getPoolPath(), mapping.getOrdering());
    Mapping oldValue = map.put(mapping.getEntityName(), val);
    if (oldValue != null) {
      throw new AssertionError("Duplicate mapping for " + text + " " + mapping.getEntityName()
          + "; " + oldValue + " and " + val);
    }
  }

  public String mapSessionToPoolName(MappingInput input, boolean allowAnyPool, Set<String> pools) {
    if (allowAnyPool && input.wmPool != null) {
      return (pools == null || pools.contains(input.wmPool)) ? input.wmPool : null;
    }
    // For equal-priority rules, user rules come first because they are more specific; then apps,
    // then groups (this is arbitrary).
    Mapping mapping = userMappings.get(input.getUserName());
    boolean isExplicitMatch = false;
    if (mapping != null) {
      isExplicitMatch = isExplicitPoolMatch(input, mapping);
      if (isExplicitMatch) return mapping.fullPoolName;
    }
    // We don't check explicit pool match for apps; both are specified on the jdbc string
    // so it doesn't make sense to have both and make sure one matches the other.
    if (mapping == null && input.getAppName() != null) {
      mapping = appMappings.get(input.getAppName());
    }
    for (String group : input.getGroups()) {
      Mapping candidate = groupMappings.get(group);
      if (candidate == null) continue;
      isExplicitMatch = isExplicitPoolMatch(input, candidate);
      if (isExplicitMatch) return candidate.fullPoolName;
      if (mapping == null || candidate.priority < mapping.priority) {
        mapping = candidate;
      }
    }
    if (input.wmPool != null && !isExplicitMatch) return null;
    if (mapping != null) return mapping.fullPoolName;
    return defaultPoolPath;
  }

  private static boolean isExplicitPoolMatch(MappingInput input, Mapping mapping) {
    return input.wmPool != null && input.wmPool.equals(mapping.fullPoolName);
  }
}
