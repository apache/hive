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

import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * PrivilegeRegistry is used to do privilege lookups. Given a privilege name, it
 * will return the Privilege object.
 */
public class PrivilegeRegistry {

  protected static Map<PrivilegeType, Privilege> Registry = null;

  public static Privilege getPrivilege(PrivilegeType privilegeType) {
    initializeRegistry();
    return Registry.get(privilegeType);
  }

  private static void initializeRegistry() {
    if(Registry != null){
      //already initialized, nothing to do
      return;
    }
    //population of registry done in separate synchronized call
    populateRegistry();
  }

  /**
   * Add entries to registry. This needs to be synchronized to avoid Registry being populated
   * multiple times.
   */
  private static synchronized void populateRegistry() {
    //do check again in synchronized block
    if(Registry != null){
      //already initialized, nothing to do
      return;
    }
    Registry = new HashMap<PrivilegeType, Privilege>();

    //add the privileges supported in authorization mode V1
    Registry.put(Privilege.ALL.getPriv(), Privilege.ALL);
    Registry.put(Privilege.ALTER_DATA.getPriv(), Privilege.ALTER_DATA);
    Registry.put(Privilege.ALTER_METADATA.getPriv(), Privilege.ALTER_METADATA);
    Registry.put(Privilege.CREATE.getPriv(), Privilege.CREATE);
    Registry.put(Privilege.DROP.getPriv(), Privilege.DROP);
    Registry.put(Privilege.INDEX.getPriv(), Privilege.INDEX);
    Registry.put(Privilege.LOCK.getPriv(), Privilege.LOCK);
    Registry.put(Privilege.SELECT.getPriv(), Privilege.SELECT);
    Registry.put(Privilege.SHOW_DATABASE.getPriv(),
        Privilege.SHOW_DATABASE);
    if(SessionState.get().isAuthorizationModeV2()){
      //add the privileges not supported in V1
      //The list of privileges supported in V2 is implementation defined,
      //so just pass everything that syntax supports.
      Registry.put(Privilege.INSERT.getPriv(), Privilege.INSERT);
      Registry.put(Privilege.DELETE.getPriv(), Privilege.DELETE);
    }
  }

  public static Privilege getPrivilege(int privilegeToken) {
    initializeRegistry();
    return Registry.get(PrivilegeType.getPrivTypeByToken(privilegeToken));
  }

  public static Privilege getPrivilege(String privilegeName) {
    initializeRegistry();
    return Registry.get(PrivilegeType.getPrivTypeByName(privilegeName));
  }

}
