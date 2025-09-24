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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;

/**
 * Helper that encapsulates LDAP filter resolution and enforcement for Kerberos-authenticated users.
 */
public final class KerberosLdapFilterEnforcer {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosLdapFilterEnforcer.class);

  private final HiveConf conf;
  private final DirSearchFactory dirSearchFactory;
  private final boolean enableLdapGroupCheck;
  private final Filter filter;

  public KerberosLdapFilterEnforcer(HiveConf conf, DirSearchFactory dirSearchFactory) {
    this.conf = conf;
    this.dirSearchFactory = dirSearchFactory;
    this.enableLdapGroupCheck = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS);
    this.filter = enableLdapGroupCheck ? LdapAuthenticationProviderImpl.resolveFilter(conf) : null;

    if (enableLdapGroupCheck && filter == null) {
      LOG.warn("LDAP group check enabled but no filters configured");
    }
  }

  /**
   * Applies configured LDAP filters to authenticate a user principal.
   *
   * @param principal Kerberos principal to validate
   * @return {@code true} if the principal passes all configured filters; {@code false} otherwise
   */
  public boolean applyLdapFilter(String principal) {
    if (!enableLdapGroupCheck || filter == null) {
      return true;
    }

    String user = extractUserName(principal);
    try (DirSearch dirSearch = createDirSearch()) {
      filter.apply(dirSearch, user);
      LOG.debug("Principal {} passed LDAP filter validation", principal);
      return true;
    } catch (AuthenticationException e) {
      LOG.warn("Principal {} failed LDAP filter validation: {}", principal, e.getMessage());
      return false;
    } catch (Exception e) {
      LOG.error("Error applying LDAP filter for principal {}", principal, e);
      return false;
    }
  }

  public boolean isFilterConfigured() {
    return enableLdapGroupCheck && filter != null;
  }

  private DirSearch createDirSearch() throws AuthenticationException {
    String bindDN = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER);
    char[] passwordChars;
    try {
      passwordChars = conf.getPassword(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_PASSWORD.varname);
    } catch (IOException e) {
      throw new AuthenticationException("Failed to retrieve LDAP bind password");
    }
    String bindPassword = passwordChars == null ? null : new String(passwordChars);

    if (StringUtils.isBlank(bindDN) || StringUtils.isBlank(bindPassword)) {
      throw new AuthenticationException("LDAP bind credentials not configured");
    }

    return dirSearchFactory.getInstance(conf, bindDN, bindPassword);
  }

  @VisibleForTesting
  public static String extractUserName(String principal) {
    String[] parts = SaslRpcServer.splitKerberosName(principal);
    return parts.length > 0 ? parts[0] : principal;
  }
}
