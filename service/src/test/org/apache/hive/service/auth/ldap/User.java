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
package org.apache.hive.service.auth.ldap;

import com.google.common.base.Preconditions;

public final class User {

  private final String dn;
  private final String id;
  private final String password;

  private User(Builder builder) {
    dn = builder.dn;
    id = builder.id;
    password = builder.password;
  }

  public String getDn() {
    return dn;
  }

  public String getId() {
    return id;
  }

  public String getPassword() {
    return password;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Credentials credentialsWithDn() {
    return Credentials.of(dn, password);
  }

  public Credentials credentialsWithId() {
    return Credentials.of(id, password);
  }

  public static final class Builder {
    private String dn;
    private String id;
    private String password;

    private Builder() {
    }

    public Builder dn(String dn) {
      Preconditions.checkNotNull(dn, "DN should not be NULL");
      Preconditions.checkState(this.dn == null, "DN has been set already");
      this.dn = dn;
      return this;
    }

    public Builder id(String id) {
      Preconditions.checkNotNull(id, "ID should not be NULL");
      Preconditions.checkState(this.id == null, "ID has been set already");
      this.id = id;
      return this;
    }

    public Builder password(String password) {
      Preconditions.checkNotNull(password, "Password should not be NULL");
      Preconditions.checkState(this.password == null, "Password has been set already");
      this.password = password;
      return this;
    }

    public Builder useIdForPassword() {
      Preconditions.checkState(this.id != null, "User ID has not been set");
      return password(id);
    }

    public User build() {
      Preconditions.checkNotNull(this.dn, "DN is required");
      Preconditions.checkNotNull(this.id, "ID is required");
      Preconditions.checkNotNull(this.password, "Password is required");
      return new User(this);
    }
  }
}
