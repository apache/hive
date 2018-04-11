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

import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import javax.servlet.ServletRequest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PamLoginService extends AbstractLifeCycle implements LoginService {
  private final ConcurrentMap<String, UserIdentity> users = new ConcurrentHashMap<>();

  private IdentityService identityService = new DefaultIdentityService();

  private static final Logger LOG = Log.getLogger(PamLoginService.class);

  @Override
  public String getName() {
    return "pam";
  }

  @Override
  public UserIdentity login(String username, Object credentials, ServletRequest request) {
    UserIdentity user = users.get(username);

    if (user != null) {
      return user;
    }

    user = new PamUserIdentity(username);
    users.put(username, user);
    return user;
  }

  @Override
  public boolean validate(UserIdentity user) {
    return users.containsKey(user.getUserPrincipal().getName());
  }

  @Override
  public IdentityService getIdentityService() {
    return identityService;
  }

  @Override
  public void setIdentityService(IdentityService identityService) {
    if (isRunning())
      throw new IllegalStateException("Running");
    this.identityService = identityService;
  }

  @Override
  public void logout(UserIdentity user) {
    users.remove(user.getUserPrincipal().getName());
    LOG.debug("logout {}", user);
  }
}
