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
package  org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.security.Security;
import java.util.HashMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreAuthenticationProviderFactory.AuthMethods;
import org.apache.hadoop.hive.metastore.MetaStorePlainSaslServer.SaslPlainProvider;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This is a copy of org.apache.hive.service.auth.PlainSaslHelper modified for HMS. See whether
// we can deduplicate the code.
public final class MetaStorePlainSaslHelper {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStorePlainSaslHelper.class);

  static {
    Security.addProvider(new SaslPlainProvider());
  }

  // This is a hack. This method does nothing but is used to load the class and thus
  // invoke the static initializer above which add SaSl service provider. Without that HMS trying
  // to authenticate using password based authentication gives NullPointerException.
  // We don't need this hack in HS2, but we do not know the code path through which this class
  // gets loaded in HS2.
  public static void init() {
    // Do nothing

    LOG.info("init called to add SaslPlainProvider to Sasl providers.");
  }


  public static TTransportFactory getPlainTransportFactory(String authTypeStr, Configuration conf)
    throws LoginException {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    try {
      saslFactory.addServerDefinition("PLAIN", authTypeStr, null, new HashMap<String, String>(),
        new PlainServerCallbackHandler(authTypeStr, conf));
    } catch (AuthenticationException e) {
      throw new LoginException("Error setting callback handler" + e);
    }
    return saslFactory;
  }

  public static TTransport getPlainTransport(String username, String password,
    TTransport underlyingTransport) throws SaslException, TTransportException {
    return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String, String>(),
      new PlainCallbackHandler(username, password), underlyingTransport);
  }

  private MetaStorePlainSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  public static final class PlainServerCallbackHandler implements CallbackHandler {

    private final AuthMethods authMethod;
    private final Configuration conf;

    PlainServerCallbackHandler(String authMethodStr, Configuration conf) throws AuthenticationException {
      authMethod = AuthMethods.getValidAuthMethod(authMethodStr);
      this.conf = conf;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      String username = null;
      String password = null;
      AuthorizeCallback ac = null;

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          username = nc.getName();
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          password = new String(pc.getPassword());
        } else if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
      MetaStorePasswdAuthenticationProvider provider =
        MetaStoreAuthenticationProviderFactory.getAuthenticationProvider(conf, authMethod);
      provider.authenticate(username, password);
      if (ac != null) {
        ac.setAuthorized(true);
      }
    }
  }

  public static class PlainCallbackHandler implements CallbackHandler {

    private final String username;
    private final String password;

    public PlainCallbackHandler(String username, String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback) callback;
          nameCallback.setName(username);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callback;
          passCallback.setPassword(password.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }
}
