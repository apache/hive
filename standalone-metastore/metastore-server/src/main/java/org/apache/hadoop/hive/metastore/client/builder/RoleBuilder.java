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
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Role;

/**
 * A builder for {@link Role}.  The roleName and the ownerName must be provided.
 */
public class RoleBuilder {
  private String roleName, ownerName;
  private int createTime;

  public RoleBuilder() {
    createTime = (int)(System.currentTimeMillis() / 1000);
  }

  public RoleBuilder setRoleName(String roleName) {
    this.roleName = roleName;
    return this;
  }

  public RoleBuilder setOwnerName(String ownerName) {
    this.ownerName = ownerName;
    return this;
  }

  public RoleBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public Role build() throws MetaException {
    if (roleName == null || ownerName == null) {
      throw new MetaException("role name and owner name must be provided.");
    }
    return new Role(roleName, createTime, ownerName);
  }
}
