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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class GrantRoleEvent extends ListenerEvent {
  private final Role role;
  private final String principalName;
  private final PrincipalType principalType;
  private final String grantor;
  private final PrincipalType grantorType;
  private final boolean grantOption;

  public GrantRoleEvent(boolean status, IHMSHandler handler,
      Role role, String principalName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) {
    super(status, handler);
    this.role = role;
    this.principalName = principalName;
    this.principalType = principalType;
    this.grantor = grantor;
    this.grantorType = grantorType;
    this.grantOption = grantOption;
  }

  public Role getRole() {
    return role;
  }

  public String getPrincipalName() {
    return principalName;
  }

  public PrincipalType getPrincipalType() {
    return principalType;
  }

  public String getGrantor() {
    return grantor;
  }

  public PrincipalType getGrantorType() {
    return grantorType;
  }

  public boolean isGrantOption() {
    return grantOption;
  }
}
