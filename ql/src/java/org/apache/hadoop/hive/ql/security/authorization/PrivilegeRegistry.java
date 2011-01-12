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

package org.apache.hadoop.hive.ql.security.authorization;

import java.util.HashMap;
import java.util.Map;

/**
 * PrivilegeRegistry is used to do privilege lookups. Given a privilege name, it
 * will return the Privilege object.
 */
public class PrivilegeRegistry {

  protected static Map<String, Privilege> Registry = new HashMap<String, Privilege>();

  static {
    Registry.put(Privilege.ALL.getPriv().toLowerCase(), Privilege.ALL);
    Registry.put(Privilege.ALTER_DATA.getPriv().toLowerCase(),
        Privilege.ALTER_DATA);
    Registry.put(Privilege.ALTER_METADATA.getPriv().toLowerCase(),
        Privilege.ALTER_METADATA);
    Registry.put(Privilege.CREATE.getPriv().toLowerCase(), Privilege.CREATE);
    Registry.put(Privilege.DROP.getPriv().toLowerCase(), Privilege.DROP);
    Registry.put(Privilege.INDEX.getPriv().toLowerCase(), Privilege.INDEX);
    Registry.put(Privilege.LOCK.getPriv().toLowerCase(), Privilege.LOCK);
    Registry.put(Privilege.SELECT.getPriv().toLowerCase(), Privilege.SELECT);
    Registry.put(Privilege.SHOW_DATABASE.getPriv().toLowerCase(),
        Privilege.SHOW_DATABASE);
  }

  public static Privilege getPrivilege(String privilegeName) {
    return Registry.get(privilegeName.toLowerCase());
  }

}
