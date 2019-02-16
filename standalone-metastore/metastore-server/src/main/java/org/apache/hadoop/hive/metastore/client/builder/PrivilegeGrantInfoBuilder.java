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
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;

import java.io.IOException;

/**
 * Builder for {@link PrivilegeGrantInfo}.  The privilege is required.  If not provided the grantor
 * is
 * assumed to be the current user.  This is really intended for use by the
 * {@link HiveObjectPrivilegeBuilder}.
 */
public class PrivilegeGrantInfoBuilder {
  private String privilege, grantor;
  private int createTime;
  private PrincipalType grantorType;
  private boolean grantOption;

  public PrivilegeGrantInfoBuilder() {
    createTime = (int)(System.currentTimeMillis() / 1000);
    grantOption = false;
  }

  public PrivilegeGrantInfoBuilder setPrivilege(String privilege) {
    this.privilege = privilege;
    return this;
  }

  public PrivilegeGrantInfoBuilder setGrantor(String grantor) {
    this.grantor = grantor;
    return this;
  }

  public PrivilegeGrantInfoBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public PrivilegeGrantInfoBuilder setGrantorType(PrincipalType grantorType) {
    this.grantorType = grantorType;
    return this;
  }

  public PrivilegeGrantInfoBuilder setGrantOption(boolean grantOption) {
    this.grantOption = grantOption;
    return this;
  }

  public PrivilegeGrantInfo build() throws MetaException {
    if (privilege == null) {
      throw new MetaException("Privilege must be provided.");
    }
    if (grantor == null) {
      try {
        grantor = SecurityUtils.getUser();
        grantorType = PrincipalType.USER;
      } catch (IOException e) {
        throw MetaStoreUtils.newMetaException(e);
      }
    }
    return new PrivilegeGrantInfo(privilege, createTime, grantor, grantorType, grantOption);
  }
}
