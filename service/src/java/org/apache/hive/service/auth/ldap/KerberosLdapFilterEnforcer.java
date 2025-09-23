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

import java.io.IOException;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Singleton helper that applies configured LDAP filters for Kerberos-authenticated users.
 */
public final class KerberosLdapFilterEnforcer {

  public static final KerberosLdapFilterEnforcer INSTANCE = new KerberosLdapFilterEnforcer();

  private KerberosLdapFilterEnforcer() {
  }

  /**
   * Apply the configured LDAP {@link Filter} for a Kerberos-authenticated user.
   *
   * @param conf Hive configuration
   * @param factory factory used to obtain directory search clients
   * @param filter resolved LDAP filter
   * @param user short user name extracted from Kerberos principal
   * @param existingDirSearch optional existing directory client to reuse
   * @param failOnMissingFilter whether lack of configured filter should be treated as failure
   * @return the directory client that was used. Callers should retain it if they want to reuse it.
   * @throws AuthenticationException if the filter check fails or configuration is invalid
   */
  public DirSearch enforce(HiveConf conf, DirSearchFactory factory, Filter filter, String user,
      DirSearch existingDirSearch, boolean failOnMissingFilter) throws AuthenticationException {
    if (filter == null) {
      if (failOnMissingFilter) {
        throw new AuthenticationException("LDAP filters not configured");
      }
      return existingDirSearch;
    }

    DirSearch dirSearch = existingDirSearch != null ? existingDirSearch : createDirSearch(conf, factory);
    try {
      filter.apply(dirSearch, user);
      return dirSearch;
    } catch (AuthenticationException e) {
      throw e;
    } catch (Exception e) {
      throw new AuthenticationException("Error applying LDAP filter for user " + user, e);
    }
  }

  private DirSearch createDirSearch(HiveConf conf, DirSearchFactory factory) throws AuthenticationException {
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

    return factory.getInstance(conf, bindDN, bindPassword);
  }
}
