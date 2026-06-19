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

import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;

/**
 * A builder for {@link GrantRevokePrivilegeRequest}.  The revoke of grant option defaults to
 * false.  The request Type and the privileges must be provided.
 */
public class GrantRevokePrivilegeRequestBuilder {
  private GrantRevokeType requestType;
  private PrivilegeBag privileges;
  private boolean revokeGrantOption;

  public GrantRevokePrivilegeRequestBuilder() {
    privileges = new PrivilegeBag();
    revokeGrantOption = false;
  }

  public GrantRevokePrivilegeRequestBuilder setRequestType(GrantRevokeType requestType) {
    this.requestType = requestType;
    return this;
  }

  public GrantRevokePrivilegeRequestBuilder setRevokeGrantOption(boolean revokeGrantOption) {
    this.revokeGrantOption = revokeGrantOption;
    return this;
  }

  public GrantRevokePrivilegeRequestBuilder addPrivilege(HiveObjectPrivilege privilege) {
    privileges.addToPrivileges(privilege);
    return this;
  }

  public GrantRevokePrivilegeRequest build() throws MetaException {
    if (requestType == null || privileges.getPrivilegesSize() == 0) {
      throw new MetaException("The request type and at least one privilege must be provided.");
    }
    GrantRevokePrivilegeRequest rqst = new GrantRevokePrivilegeRequest(requestType, privileges);
    if (revokeGrantOption) rqst.setRevokeGrantOption(revokeGrantOption);
    return rqst;
  }
}
