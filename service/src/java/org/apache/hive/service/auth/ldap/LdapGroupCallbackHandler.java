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
package org.apache.hive.service.auth.ldap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hive.service.auth.LdapAuthenticationProviderImpl;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Callback handler that enforces LDAP filters on Kerberos-authenticated users.
 * This handler applies the same LDAP filter resolution used for LDAP authentication
 * to Kerberos users, ensuring consistent authorization policies.
 */
public class LdapGroupCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LdapGroupCallbackHandler.class);

  private final HiveConf conf;
  private final boolean enableLdapGroupCheck;
  private final CallbackHandler delegateHandler;
  private final DirSearchFactory dirSearchFactory;
  private final Filter filter;

  public LdapGroupCallbackHandler(HiveConf conf) {
    this(conf, new LdapSearchFactory(), new SaslRpcServer.SaslGssCallbackHandler());
  }

  public LdapGroupCallbackHandler(HiveConf conf, DirSearchFactory dirSearchFactory, CallbackHandler delegateHandler) {
    this.conf = conf;
    this.delegateHandler = delegateHandler;
    this.dirSearchFactory = dirSearchFactory;
    this.enableLdapGroupCheck = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS);
    this.filter = enableLdapGroupCheck ? LdapAuthenticationProviderImpl.resolveFilter(conf) : null;

    if (enableLdapGroupCheck && filter == null) {
      LOG.warn("LDAP group check enabled but no filters configured");
    }
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    List<Callback> unhandledCallbacks = new ArrayList<>();

    for (Callback callback : callbacks) {
      if (callback instanceof AuthorizeCallback) {
        AuthorizeCallback ac = (AuthorizeCallback) callback;
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();

        if (StringUtils.isBlank(authenticationID) || StringUtils.isBlank(authorizationID)) {
          LOG.debug("Missing authentication or authorization ID; delegating callback");
          unhandledCallbacks.add(callback);
          continue;
        }

        if (!authenticationID.equals(authorizationID)) {
          LOG.debug("Delegating authorization for different auth IDs");
          unhandledCallbacks.add(callback);
          continue;
        }

        // If group check is not enabled or no filter configured, authorize immediately.
        if (!enableLdapGroupCheck || filter == null) {
          ac.setAuthorized(true);
          continue;
        }

        String user = extractUserName(authenticationID);
        boolean authorized = applyLdapFilter(user);
        ac.setAuthorized(authorized);
      } else {
        unhandledCallbacks.add(callback);
      }
    }

    if (!unhandledCallbacks.isEmpty()) {
      delegateHandler.handle(unhandledCallbacks.toArray(new Callback[0]));
    }
  }

  /**
   * Applies configured LDAP filters to authenticate a user.
   *
   * @param user the username to validate
   * @return true if the user passes all configured filters, false otherwise
   */
  private boolean applyLdapFilter(String user) {
    try {
      String bindDN = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER);
      char[] rawPassword = conf.getPassword(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_PASSWORD.varname);
      String bindPassword = (rawPassword == null) ? null : new String(rawPassword);

      if (StringUtils.isBlank(bindDN) || StringUtils.isBlank(bindPassword)) {
        LOG.error("LDAP bind DN or password is not configured");
        return false;
      }

      DirSearch dirSearch = this.dirSearchFactory.getInstance(conf, bindDN, bindPassword);

      filter.apply(dirSearch, user);
      LOG.debug("User {} passed LDAP filter validation", user);
      return true;

    } catch (AuthenticationException e) {
      LOG.warn("User {} failed LDAP filter validation: {}", user, e.getMessage());
      return false;
    } catch (Exception e) {
      LOG.error("Error applying LDAP filter for user {}", user, e);
      return false;
    }
  }

  @VisibleForTesting
  public static String extractUserName(@NotNull String principal) {
    int idx = principal.indexOf('@');
    if (idx > 0) {
      principal = principal.substring(0, idx);
    }
    idx = principal.indexOf('/');
    if (idx > 0) {
      principal = principal.substring(0, idx);
    }
    return principal;
  }
}
