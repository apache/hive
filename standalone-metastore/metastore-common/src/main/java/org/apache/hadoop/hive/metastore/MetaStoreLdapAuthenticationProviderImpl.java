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
package org.apache.hadoop.hive.metastore;

import javax.security.sasl.AuthenticationException;
import javax.naming.NamingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.ldap.ChainFilterFactory;
import org.apache.hadoop.hive.metastore.ldap.CustomQueryFilterFactory;
import org.apache.hadoop.hive.metastore.ldap.LdapSearchFactory;
import org.apache.hadoop.hive.metastore.ldap.Filter;
import org.apache.hadoop.hive.metastore.ldap.DirSearch;
import org.apache.hadoop.hive.metastore.ldap.DirSearchFactory;
import org.apache.hadoop.hive.metastore.ldap.FilterFactory;
import org.apache.hadoop.hive.metastore.ldap.GroupFilterFactory;
import org.apache.hadoop.hive.metastore.ldap.LdapUtils;
import org.apache.hadoop.hive.metastore.ldap.UserFilterFactory;
import org.apache.hadoop.hive.metastore.ldap.UserGroupSearchFilterFactory;
import org.apache.hadoop.hive.metastore.ldap.UserSearchFilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This file is copy of LdapAuthenticationProviderImpl from hive.auth. We should try to
//  deduplicate the code.
public class MetaStoreLdapAuthenticationProviderImpl implements MetaStorePasswdAuthenticationProvider {

  private static final Logger LOG =
          LoggerFactory.getLogger(MetaStoreLdapAuthenticationProviderImpl.class);

  private static final List<FilterFactory> FILTER_FACTORIES = ImmutableList.<FilterFactory>of(
      new UserGroupSearchFilterFactory(),
      new CustomQueryFilterFactory(),
      new ChainFilterFactory(new UserSearchFilterFactory(), new UserFilterFactory(),
          new GroupFilterFactory())
  );

  private final Configuration conf;
  private final Filter filter;
  private final DirSearchFactory searchFactory;

  public MetaStoreLdapAuthenticationProviderImpl(Configuration conf) {
    this(conf, new LdapSearchFactory());
  }

  @VisibleForTesting
  MetaStoreLdapAuthenticationProviderImpl(Configuration conf, DirSearchFactory searchFactory) {
    this.conf = conf;
    this.searchFactory = searchFactory;
    filter = resolveFilter(conf);
  }

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    DirSearch search = null;
    String bindUser = MetastoreConf.getVar(this.conf,
            MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER);
    if (StringUtils.isBlank(bindUser)) {
      bindUser = null;
    }
    String bindPassword;
    try {
      bindPassword = MetastoreConf.getPassword(this.conf,
              MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_PASSWORD);
      if (StringUtils.isBlank(bindPassword)) {
        bindPassword = null;
      }
    } catch (IOException e) {
      bindPassword = null;
    }
    boolean usedBind = bindUser != null && bindPassword != null;
    if (!usedBind) {
      // If no bind user or bind password was specified,
      // we assume the user we are authenticating has the ability to search
      // the LDAP tree, so we use it as the "binding" account.
      // This is the way it worked before bind users were allowed in the LDAP authenticator,
      // so we keep existing systems working.
      bindUser = user;
      bindPassword = password;
    }
    try {
      search = createDirSearch(bindUser, bindPassword);
      applyFilter(search, user);
      if (usedBind) {
        // If we used the bind user, then we need to authenticate again,
        // this time using the full user name we got during the bind process.
        createDirSearch(search.findUserDn(user), password);
      }
    } catch (NamingException e) {
      throw new AuthenticationException("Unable to find the user in the LDAP tree. " + e.getMessage());
    } finally {
      ServiceUtils.cleanup(LOG, search);
    }
  }

  private DirSearch createDirSearch(String user, String password) throws AuthenticationException {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank user name has been provided");
    }
    if (StringUtils.isBlank(password) || password.getBytes(StandardCharsets.UTF_8)[0] == 0) {
      throw new AuthenticationException("Error validating LDAP user:"
          + " a null or blank password has been provided");
    }
    List<String> principals = LdapUtils.createCandidatePrincipals(conf, user);
    for (Iterator<String> iterator = principals.iterator(); iterator.hasNext();) {
      String principal = iterator.next();
      try {
        return searchFactory.getInstance(conf, principal, password);
      } catch (AuthenticationException ex) {
        if (!iterator.hasNext()) {
          throw ex;
        }
      }
    }
    throw new AuthenticationException(
        String.format("No candidate principals for %s was found.", user));
  }

  private static Filter resolveFilter(Configuration conf) {
    for (FilterFactory filterProvider : FILTER_FACTORIES) {
      Filter filter = filterProvider.getInstance(conf);
      if (filter != null) {
        return filter;
      }
    }
    return null;
  }

  private void applyFilter(DirSearch client, String user) throws AuthenticationException {
    if (filter != null) {
      if (LdapUtils.hasDomain(user)) {
        filter.apply(client, LdapUtils.extractUserName(user));
      } else {
        filter.apply(client, user);
      }
    }
  }
}
