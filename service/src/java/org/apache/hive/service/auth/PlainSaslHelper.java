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
package org.apache.hive.service.auth;

import java.io.IOException;
import java.net.InetAddress;
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

import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods;
import org.apache.hive.service.auth.PlainSaslServer.SaslPlainProvider;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PlainSaslHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PlainSaslHelper.class);

  public static TProcessorFactory getPlainProcessorFactory(TCLIService.Iface service) {
    return new SQLPlainProcessorFactory(service);
  }

  // Register Plain SASL server provider
  static {
    Security.addProvider(new SaslPlainProvider());
  }

  public static TTransportFactory getPlainTransportFactory(String authTypeStr)
    throws LoginException {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    try {
      saslFactory.addServerDefinition("PLAIN", authTypeStr, null, new HashMap<String, String>(),
        new PlainServerCallbackHandler(authTypeStr));
    } catch (AuthenticationException e) {
      throw new LoginException("Error setting callback handler" + e);
    }
    return saslFactory;
  }

  static TTransportFactory getDualPlainTransportFactory(TTransportFactory otherTrans,
                                                        String trustedDomain)
          throws LoginException {
    LOG.info("Created additional transport factory for skipping authentication when client " +
            "connection is from the same domain.");
    return new DualSaslTransportFactory(otherTrans, trustedDomain);
  }

  public static TTransport getPlainTransport(String username, String password,
    TTransport underlyingTransport) throws SaslException, TTransportException {
    return new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String, String>(),
      new PlainCallbackHandler(username, password), underlyingTransport);
  }

  // Return true if the remote host is from the trusted domain, i.e. host URL has the same
  // suffix as the trusted domain.
  static public boolean isHostFromTrustedDomain(String remoteHost, String trustedDomain) {
    return remoteHost.endsWith(trustedDomain);
  }

  private PlainSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  static final class DualSaslTransportFactory extends TTransportFactory {
    TTransportFactory otherFactory;
    TTransportFactory noAuthFactory;
    String trustedDomain;

    DualSaslTransportFactory(TTransportFactory otherFactory, String trustedDomain)
            throws LoginException {
      this.noAuthFactory = getPlainTransportFactory(AuthMethods.NONE.toString());
      this.otherFactory = otherFactory;
      this.trustedDomain = trustedDomain;
    }

    @Override
    public TTransport getTransport(final TTransport trans) throws TTransportException {
      TSocket tSocket = null;
      // Attempt to avoid authentication if only we can fetch the client IP address and it
      // happens to be from the same domain as the server.
      if (trans instanceof TSocket) {
        tSocket = (TSocket) trans;
      } else if (trans instanceof TSaslServerTransport) {
        TSaslServerTransport saslTrans = (TSaslServerTransport) trans;
        tSocket = (TSocket)(saslTrans.getUnderlyingTransport());
      }
      String remoteHost = tSocket != null ?
              tSocket.getSocket().getInetAddress().getCanonicalHostName() : null;
      if (remoteHost != null && isHostFromTrustedDomain(remoteHost, trustedDomain)) {
        LOG.info("No authentication performed because the connecting host " + remoteHost + " is " +
                "from the trusted domain " + trustedDomain);
        return noAuthFactory.getTransport(trans);
      }

      return otherFactory.getTransport(trans);
    }
  }

  public static final class PlainServerCallbackHandler implements CallbackHandler {

    private final AuthMethods authMethod;

    PlainServerCallbackHandler(String authMethodStr) throws AuthenticationException {
      authMethod = AuthMethods.getValidAuthMethod(authMethodStr);
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
      PasswdAuthenticationProvider provider =
        AuthenticationProviderFactory.getAuthenticationProvider(authMethod);
      try {
        provider.Authenticate(username, password);
      } catch (Exception e) {
        LOG.error("Login attempt is failed for user : " + username + ". Error Messsage : " + e.getMessage());
        throw e;
      }
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

  private static final class SQLPlainProcessorFactory extends TProcessorFactory {

    private final TCLIService.Iface service;

    SQLPlainProcessorFactory(TCLIService.Iface service) {
      super(null);
      this.service = service;
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      return new TSetIpAddressProcessor<Iface>(service);
    }
  }

}
