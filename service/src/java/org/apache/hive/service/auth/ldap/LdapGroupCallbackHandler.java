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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.SaslRpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;

/**
 * Callback handler that enforces LDAP filters on Kerberos-authenticated users.
 * This handler applies the same LDAP filter resolution used for LDAP authentication
 * to Kerberos users, ensuring consistent authorization policies.
 */
public class LdapGroupCallbackHandler implements CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LdapGroupCallbackHandler.class);

  private final CallbackHandler delegateHandler;
  private final KerberosLdapFilterEnforcer filterEnforcer;

  public LdapGroupCallbackHandler(HiveConf conf) {
    this(conf, new LdapSearchFactory(), new SaslRpcServer.SaslGssCallbackHandler());
  }

  @VisibleForTesting
  private LdapGroupCallbackHandler(HiveConf conf, DirSearchFactory dirSearchFactory, CallbackHandler delegateHandler) {
    this.delegateHandler = delegateHandler;
    this.filterEnforcer = new KerberosLdapFilterEnforcer(conf, dirSearchFactory);
  }

  @VisibleForTesting
  public static LdapGroupCallbackHandler createForTesting(HiveConf conf, DirSearchFactory dirSearchFactory,
      CallbackHandler delegateHandler) {
    return new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    delegateHandler.handle(callbacks);

    for (Callback callback : callbacks) {
      if (!(callback instanceof AuthorizeCallback)) {
        continue;
      }

      AuthorizeCallback ac = (AuthorizeCallback) callback;

      if (!ac.isAuthorized()) {
        LOG.debug("Delegate callback rejected {}; skipping LDAP filter", ac.getAuthenticationID());
        continue;
      }

      String authenticationID = ac.getAuthenticationID();

      boolean authorized = filterEnforcer.applyLdapFilter(authenticationID);
      ac.setAuthorized(authorized);
    }
  }
}
