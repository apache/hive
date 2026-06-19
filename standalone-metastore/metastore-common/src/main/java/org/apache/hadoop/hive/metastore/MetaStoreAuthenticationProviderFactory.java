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

import org.apache.hadoop.conf.Configuration;

import javax.security.sasl.AuthenticationException;

// This file is copies from org.apache.hive.service.auth.AuthenticationProviderFactory. Need to
// deduplicate this code.
/**
 * This class helps select a {@link MetaStorePasswdAuthenticationProvider} for a given {@code
 * AuthMethod}.
 */
public final class MetaStoreAuthenticationProviderFactory {

  public enum AuthMethods {
    LDAP("LDAP"),
    PAM("PAM"),
    CUSTOM("CUSTOM"),
    NONE("NONE"),
    CONFIG("CONFIG");

    private final String authMethod;

    AuthMethods(String authMethod) {
      this.authMethod = authMethod;
    }

    public String getAuthMethod() {
      return authMethod;
    }

    public static AuthMethods getValidAuthMethod(String authMethodStr)
      throws AuthenticationException {
      for (AuthMethods auth : AuthMethods.values()) {
        if (authMethodStr.equals(auth.getAuthMethod())) {
          return auth;
        }
      }
      throw new AuthenticationException("Not a valid authentication method");
    }
  }

  private MetaStoreAuthenticationProviderFactory() {
  }

  public static MetaStorePasswdAuthenticationProvider getAuthenticationProvider(AuthMethods authMethod)
    throws AuthenticationException {
    return getAuthenticationProvider(new Configuration(), authMethod);
  }

  public static MetaStorePasswdAuthenticationProvider getAuthenticationProvider(Configuration conf, AuthMethods authMethod)
    throws AuthenticationException {
    if (authMethod == AuthMethods.LDAP) {
      return new MetaStoreLdapAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.CUSTOM) {
      return new MetaStoreCustomAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.CONFIG) {
      return new MetaStoreConfigAuthenticationProviderImpl(conf);
    } else if (authMethod == AuthMethods.NONE) {
      return new MetaStoreAnonymousAuthenticationProviderImpl();
    } else {
      throw new AuthenticationException("Unsupported authentication method");
    }
  }
}
