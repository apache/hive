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


import java.io.IOException;
import java.util.HashMap;

import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthenticationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.TUGIContainingTransport;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 *
 * This is copied from HiveAuthFactory and modified to be used for HMS. But we should see if we can
 * use same code for both Hive and HMS.
 */
public class AuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AuthFactory.class);

  private HadoopThriftAuthBridge.Server saslServer;
  private String authTypeStr;
  private final String transportMode;
  private String hadoopAuth;
  private MetastoreDelegationTokenManager delegationTokenManager = null;
  private boolean useFramedTransport;
  private boolean executeSetUGI;
  private Configuration conf;

  public AuthFactory(HadoopThriftAuthBridge bridge, Configuration conf, Object baseHandler)
          throws HiveMetaException, TTransportException {
    // For now metastore only operates in binary mode. It would be good if we could model an HMS
    // as a ThriftCliService, but right now that's too much tied with HiveServer2.
    this.conf = conf;
    transportMode = "binary";
    authTypeStr = MetastoreConf.getVar(conf,
            MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION);
    useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
    executeSetUGI = MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI);

    // Secured mode communication with Hadoop
    // Blend this with THRIFT_METASTORE_AUTHENTICATION, for now they are separate. useSasl
    // should be set to true when authentication is anything other than NONE. Or we should use a
    // separate configuration for that?
    // In case of HS2, this is defined by configuration HADOOP_SECURITY_AUTHENTICATION, which
    // indicates the authentication used by underlying HDFS. In case of metastore SASL and hadoop
    // authentication seem to be tied up. But with password based SASL we might have to break
    // this coupling. Should we provide HADOOP_SECURITY_AUTHENTICATION for hadoop too, or use
    // USE_THRIFT_SASL itself to indicate that the underlying Hadoop is kerberized.
    if (StringUtils.isBlank(authTypeStr)) {
      authTypeStr = AuthConstants.AuthTypes.NOSASL.getAuthName();
    }

    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_SASL)) {
      hadoopAuth = "kerberos";
      // If SASL is enabled but no authentication method is specified, we use only kerberos as an
      // authentication mechanism.
      if (authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.NOSASL.getAuthName())) {
        authTypeStr = AuthConstants.AuthTypes.KERBEROS.getAuthName();
      }
    } else {
      hadoopAuth = "simple";
    }

    LOG.info("Using authentication " + authTypeStr +
            " with kerberos authentication " + (isSASLWithKerberizedHadoop() ? "enabled." : "disabled"));
    if (isSASLWithKerberizedHadoop()) {
      // we are in secure mode.
      if (useFramedTransport) {
        throw new HiveMetaException("Framed transport is not supported with SASL enabled.");
      }
      saslServer =
          bridge.createServer(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE),
                  MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL),
                  MetastoreConf.getVar(conf, ConfVars.CLIENT_KERBEROS_PRINCIPAL));

      // Start delegation token manager
      delegationTokenManager = new MetastoreDelegationTokenManager();
      try {
        delegationTokenManager.startDelegationTokenSecretManager(conf, baseHandler,
            HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
        saslServer.setSecretManager(delegationTokenManager.getSecretManager());
      } catch (IOException e) {
        throw new TTransportException("Failed to start token manager", e);
      }
    }
  }

  TTransportFactory getAuthTransFactory(boolean useSSL, Configuration conf) throws LoginException {
    TTransportFactory transportFactory;
    TSaslServerTransport.Factory serverTransportFactory;

    if (isSASLWithKerberizedHadoop()) {
      try {
        if (useFramedTransport) {
          throw new LoginException("Framed transport is not supported with SASL enabled.");
        }
        serverTransportFactory = saslServer.createSaslServerTransportFactory(
                MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
        transportFactory = saslServer.wrapTransportFactoryInClientUGI(serverTransportFactory);
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
      if (authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.KERBEROS.getAuthName())) {
        // no-op
      } else if (authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.NONE.getAuthName()) ||
          authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.LDAP.getAuthName()) ||
          authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.PAM.getAuthName()) ||
          authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.CUSTOM.getAuthName()) ||
          authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.CONFIG.getAuthName())) {
        try {
          MetaStorePlainSaslHelper.init();
          LOG.debug("Adding server definition for PLAIN SaSL with authentication "+ authTypeStr +
                  " to transport factory " + serverTransportFactory);
          serverTransportFactory.addServerDefinition("PLAIN",
              authTypeStr, null, new HashMap<String, String>(),
              new MetaStorePlainSaslHelper.PlainServerCallbackHandler(authTypeStr, conf));
        } catch (AuthenticationException e) {
          throw new LoginException("Error setting callback handler" + e);
        }
      } else {
        throw new LoginException("Unsupported authentication type " + authTypeStr);
      }
    } else {
      if (authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.NONE.getAuthName()) ||
              authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.LDAP.getAuthName()) ||
              authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.PAM.getAuthName()) ||
              authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.CUSTOM.getAuthName()) ||
              authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.CONFIG.getAuthName())) {
        if (useFramedTransport) {
          throw new LoginException("Framed transport is not supported with password based " +
                  "authentication enabled.");

        }
        if (executeSetUGI) {
          throw new LoginException("Setting " + ConfVars.EXECUTE_SET_UGI + " is not supported " +
                  "with password based authentication enabled.");
        }
        LOG.info("Using plain SASL transport factory with " + authTypeStr + " authentication");
        transportFactory = MetaStorePlainSaslHelper.getPlainTransportFactory(authTypeStr, conf);
      } else if (authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.NOSASL.getAuthName())) {
        if (executeSetUGI) {
          transportFactory = useFramedTransport ?
                  new ChainedTTransportFactory(new TFramedTransport.Factory(),
                          new TUGIContainingTransport.Factory())
                  :new TUGIContainingTransport.Factory();
        } else {
          transportFactory = useFramedTransport ?
                  new TFramedTransport.Factory() : new TTransportFactory();
        }
      } else {
        throw new LoginException("Unsupported authentication type " + authTypeStr);
      }
    }

    return transportFactory;
  }

  public HadoopThriftAuthBridge.Server getSaslServer() throws IllegalStateException {
    if (!isSASLWithKerberizedHadoop() || null == saslServer) {
      throw new IllegalStateException("SASL server is not setup");
    }
    return saslServer;
  }

  public MetastoreDelegationTokenManager getDelegationTokenManager() throws IllegalStateException {
    if (!isSASLWithKerberizedHadoop() || null == saslServer) {
      throw new IllegalStateException("SASL server is not setup");
    }
    return delegationTokenManager;
  }

  public boolean isSASLWithKerberizedHadoop() {
    return "kerberos".equalsIgnoreCase(hadoopAuth)
            && !authTypeStr.equalsIgnoreCase(AuthConstants.AuthTypes.NOSASL.getAuthName());
  }

  private static final class ChainedTTransportFactory extends TTransportFactory {
    private final TTransportFactory parentTransFactory;
    private final TTransportFactory childTransFactory;

    private ChainedTTransportFactory(
            TTransportFactory parentTransFactory,
            TTransportFactory childTransFactory) {
      this.parentTransFactory = parentTransFactory;
      this.childTransFactory = childTransFactory;
    }

    @Override
    public TTransport getTransport(TTransport trans) throws TTransportException {
      return childTransFactory.getTransport(parentTransFactory.getTransport(trans));
    }
  }
}
