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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link HiveResourceACLs}.
 */
public class HiveResourceACLsImpl implements HiveResourceACLs {

  Map<String, Map<Privilege, AccessResult>> userPermissions = new HashMap<String, Map<Privilege, AccessResult>>();
  Map<String, Map<Privilege, AccessResult>> groupPermissions = new HashMap<String, Map<Privilege, AccessResult>>();

  @Override
  public Map<String, Map<Privilege, AccessResult>> getUserPermissions() {
    return userPermissions;
  }

  @Override
  public Map<String, Map<Privilege, AccessResult>> getGroupPermissions() {
    return groupPermissions;
  }

  public void addUserEntry(String user, Privilege priv, AccessResult result) {
    if (userPermissions.containsKey(user)) {
      userPermissions.get(user).put(priv, result);
    } else {
      Map<Privilege, AccessResult> entry = new EnumMap<Privilege, AccessResult>(Privilege.class);
      entry.put(priv, result);
      userPermissions.put(user, entry);
    }
  }

  public void addGroupEntry(String group, Privilege priv, AccessResult result) {
    if (groupPermissions.containsKey(group)) {
      groupPermissions.get(group).put(priv, result);
    } else {
      Map<Privilege, AccessResult> entry = new EnumMap<Privilege, AccessResult>(Privilege.class);
      entry.put(priv, result);
      groupPermissions.put(group, entry);
    }
  }
}
