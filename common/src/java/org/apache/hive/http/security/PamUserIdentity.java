/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.http.security;

import com.sun.security.auth.UserPrincipal;
import org.eclipse.jetty.security.UserIdentity;

import javax.security.auth.Subject;
import java.security.Principal;

/**
 * A UserIdentity implementation for PAM-authenticated users.
 * Delegates to the Jetty 12 {@link UserIdentity#from} factory and
 * overrides {@link #isUserInRole} to always return true.
 */
public class PamUserIdentity implements UserIdentity {
  private final UserIdentity delegate;

  public PamUserIdentity(String username) {
    this.delegate = UserIdentity.from(
        new Subject(), new UserPrincipal(username), "pam");
  }

  @Override
  public Subject getSubject() {
    return delegate.getSubject();
  }

  @Override
  public Principal getUserPrincipal() {
    return delegate.getUserPrincipal();
  }

  @Override
  public boolean isUserInRole(String role) {
    return true;
  }

  @Override
  public String toString() {
    return PamUserIdentity.class.getSimpleName();
  }
}
