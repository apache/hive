/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;

public abstract class GrantRoleMessage extends EventMessage {
  public GrantRoleMessage() {
    super(EventType.GRANT_ROLE);
  }

  public abstract Role getRole() throws Exception;

  public abstract String getPrincipalName();

  public abstract PrincipalType getPrincipalType();

  public abstract String getGrantor();

  public abstract PrincipalType getGrantorType();

  public abstract boolean isGrantOption();

  @Override
  public EventMessage checkValid() {
    if (getPrincipalName() == null)
      throw new IllegalStateException("Principal name unset.");
    if (getPrincipalType() == null)
      throw new IllegalStateException("Principal type unset.");
    if (getGrantor() == null)
      throw new IllegalStateException("Grantor unset.");
    if (getGrantorType() == null)
      throw new IllegalStateException("Grantor type unset.");
    return this;
  }
}
