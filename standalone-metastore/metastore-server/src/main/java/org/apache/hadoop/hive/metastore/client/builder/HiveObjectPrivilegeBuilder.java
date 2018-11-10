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

import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;

/**
 * Builder for {@link HiveObjectPrivilege}.  All values must be set.
 */
public class HiveObjectPrivilegeBuilder {
  private HiveObjectRef hiveObjectRef;
  private String principleName;
  private PrincipalType principalType;
  private PrivilegeGrantInfo grantInfo;
  private String authorizer;

  public HiveObjectPrivilegeBuilder setHiveObjectRef(HiveObjectRef hiveObjectRef) {
    this.hiveObjectRef = hiveObjectRef;
    return this;
  }

  public HiveObjectPrivilegeBuilder setPrincipleName(String principleName) {
    this.principleName = principleName;
    return this;
  }

  public HiveObjectPrivilegeBuilder setPrincipalType(PrincipalType principalType) {
    this.principalType = principalType;
    return this;
  }

  public HiveObjectPrivilegeBuilder setGrantInfo(PrivilegeGrantInfo grantInfo) {
    this.grantInfo = grantInfo;
    return this;
  }

  public HiveObjectPrivilegeBuilder setAuthorizer(String authorizer) {
    this.authorizer = authorizer;
    return this;
  }

  public HiveObjectPrivilege build() throws MetaException {
    if (hiveObjectRef == null || principleName == null || principalType == null ||
        grantInfo == null) {
      throw new MetaException("hive object reference, principle name and type, and grant info " +
          "must all be provided");
    }
    return new HiveObjectPrivilege(hiveObjectRef, principleName, principalType, grantInfo, authorizer);
  }
}
