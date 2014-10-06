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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;

/**
 * Represents a privilege granted for an object to a principal
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public class HivePrivilegeInfo{
  private final HivePrincipal principal;
  private final HivePrivilege privilege;
  private final HivePrivilegeObject object;
  private final HivePrincipal grantorPrincipal;
  private final boolean grantOption;
  private final int grantTime;

  public HivePrivilegeInfo(HivePrincipal principal, HivePrivilege privilege,
      HivePrivilegeObject object, HivePrincipal grantorPrincipal, boolean grantOption,
      int grantTime){
    this.principal = principal;
    this.privilege = privilege;
    this.object = object;
    this.grantorPrincipal = grantorPrincipal;
    this.grantOption = grantOption;
    this.grantTime = grantTime;
  }

  public HivePrincipal getPrincipal() {
    return principal;
  }

  public HivePrivilege getPrivilege() {
    return privilege;
  }

  public HivePrivilegeObject getObject() {
    return object;
  }

  public HivePrincipal getGrantorPrincipal() {
    return grantorPrincipal;
  }

  public boolean isGrantOption() {
    return grantOption;
  }

  public int getGrantTime() {
    return grantTime;
  }


}