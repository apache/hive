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
package org.apache.hadoop.hive.metastore.ldap;

import javax.security.sasl.AuthenticationException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.EnumMap;
import java.util.Map;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreLdapAuthenticationProviderImpl;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Assert;

public final class LdapAuthenticationTestCase {

  private final MetaStoreLdapAuthenticationProviderImpl ldapProvider;

  public static Builder builder() {
    return new Builder();
  }

  private LdapAuthenticationTestCase(Builder builder) {
    this.ldapProvider = new MetaStoreLdapAuthenticationProviderImpl(builder.conf);
  }

  public void assertAuthenticatePasses(Credentials credentials) {
    try {
      ldapProvider.authenticate(credentials.getUser(), credentials.getPassword());
    } catch (AuthenticationException e) {
      String message = String.format("Authentication failed for user '%s' with password '%s'",
          credentials.getUser(), credentials.getPassword());
      throw new AssertionError(message, e);
    }
  }

  public void assertAuthenticateFails(Credentials credentials) {
    assertAuthenticateFails(credentials.getUser(), credentials.getPassword());
  }

  public void assertAuthenticateFailsUsingWrongPassword(Credentials credentials) {
    assertAuthenticateFails(credentials.getUser(), "not" + credentials.getPassword());
  }

  public void assertAuthenticateFails(String user, String password) {
    try {
      ldapProvider.authenticate(user, password);
      Assert.fail(String.format("Expected authentication to fail for %s", user));
    } catch (AuthenticationException expected) {
      Assert.assertNotNull("Expected authentication exception", expected);
    }
  }

  public static final class Builder {

    private final Map<MetastoreConf.ConfVars, String> overrides =
            new EnumMap<>(MetastoreConf.ConfVars.class);
    private Configuration conf;

    public Builder baseDN(String baseDN) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, baseDN);
    }

    public Builder guidKey(String guidKey) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GUIDKEY, guidKey);
    }

    public Builder userDNPatterns(String... userDNPatterns) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERDNPATTERN,
          Joiner.on(':').join(userDNPatterns));
    }

    public Builder userFilters(String... userFilters) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER,
          Joiner.on(',').join(userFilters));
    }

    public Builder groupDNPatterns(String... groupDNPatterns) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPDNPATTERN,
          Joiner.on(':').join(groupDNPatterns));
    }

    public Builder groupFilters(String... groupFilters) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER,
          Joiner.on(',').join(groupFilters));
    }

    public Builder groupClassKey(String groupClassKey) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPCLASS_KEY, groupClassKey);
    }

    public Builder ldapServer(LdapServer ldapServer) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_URL,
          "ldap://localhost:" + ldapServer.getPort());
    }

    public Builder customQuery(String customQuery) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_CUSTOMLDAPQUERY, customQuery);
    }

    public Builder groupMembershipKey(String groupMembershipKey) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPMEMBERSHIP_KEY,
          groupMembershipKey);
    }

    public Builder userMembershipKey(String userMembershipKey) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY,
          userMembershipKey);
    }

    public Builder userSearchFilter(String userSearchFilter) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERSEARCHFILTER,
          userSearchFilter);
    }

    public Builder groupSearchFilter(String groupSearchFilter) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPSEARCHFILTER,
          groupSearchFilter);
    }

    public Builder groupBaseDN(String groupBaseDN) {
      return setVarOnce(MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPBASEDN,
          groupBaseDN);
    }

    private Builder setVarOnce(MetastoreConf.ConfVars confVar, String value) {
      Preconditions.checkState(!overrides.containsKey(confVar),
          "Property %s has been set already", confVar);
      overrides.put(confVar, value);
      return this;
    }

    private void overrideMetaStoreConf() {
      conf.set("hive.root.logger", "DEBUG,console");
      for (Map.Entry<MetastoreConf.ConfVars, String> entry : overrides.entrySet()) {
        MetastoreConf.setVar(conf, entry.getKey(), entry.getValue());
      }
    }

    public LdapAuthenticationTestCase build() {
      Preconditions.checkState(conf == null,
          "Test Case Builder should not be reused. Please create a new instance.");
      conf = MetastoreConf.newMetastoreConf();
      overrideMetaStoreConf();
      return new LdapAuthenticationTestCase(this);
    }
  }
}
