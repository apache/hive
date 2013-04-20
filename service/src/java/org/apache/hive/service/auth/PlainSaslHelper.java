/**
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
import java.util.HashMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PlainSaslServer.ExternalAuthenticationCallback;
import org.apache.hive.service.auth.PlainSaslServer.SaslPlainProvider;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

public class PlainSaslHelper {

  private static class PlainServerCallbackHandler implements CallbackHandler {

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      ExternalAuthenticationCallback ac = null;
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof ExternalAuthenticationCallback) {
          ac = (ExternalAuthenticationCallback) callbacks[i];
          break;
        } else {
          throw new UnsupportedCallbackException(callbacks[i]);
        }
      }

      if (ac != null) {
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(ac.getAuthMethod());
        provider.Authenticate(ac.getUserName(), ac.getPasswd());
        ac.setAuthenticated(true);
      }
    }
  }

  public static class PlainClientbackHandler implements CallbackHandler {

    private final String userName;
    private final String passWord;

    public PlainClientbackHandler (String userName, String passWord) {
      this.userName = userName;
      this.passWord = passWord;
    }

    @Override
    public void handle(Callback[] callbacks)
          throws IOException, UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback)callbacks[i];
          nameCallback.setName(userName);
        } else if (callbacks[i] instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callbacks[i];
          passCallback.setPassword(passWord.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callbacks[i]);
        }
      }
    }
  }

  private static class SQLPlainProcessorFactory extends TProcessorFactory {
    private final ThriftCLIService service;
    private final HiveConf conf;
    private final boolean doAsEnabled;

    public SQLPlainProcessorFactory(ThriftCLIService service) {
      super(null);
      this.service = service;
      this.conf = service.getHiveConf();
      this.doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      TProcessor baseProcessor =  new TCLIService.Processor<Iface>(service);
      return doAsEnabled ? new TUGIContainingProcessor(baseProcessor, conf) :
            new TSetIpAddressProcessor<Iface>(service);
    }
  }

  public static TProcessorFactory getPlainProcessorFactory(ThriftCLIService service) {
    return new SQLPlainProcessorFactory(service);
  }

  // Register Plain SASL server provider
  static {
    java.security.Security.addProvider(new SaslPlainProvider());
  }

  public static TTransportFactory getPlainTransportFactory(String authTypeStr) {
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    saslFactory.addServerDefinition("PLAIN",
        authTypeStr, null, new HashMap<String, String>(),
        new PlainServerCallbackHandler());
    return saslFactory;
  }

  public static TTransport getPlainTransport(String userName, String passwd,
      final TTransport underlyingTransport) throws SaslException {
    return new TSaslClientTransport("PLAIN", null,
        null, null, new HashMap<String, String>(),
        new PlainClientbackHandler(userName, passwd), underlyingTransport);
  }

}
