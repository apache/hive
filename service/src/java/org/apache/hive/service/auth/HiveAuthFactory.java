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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.metastore.security.DBTokenStore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class HiveAuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthFactory.class);

  private HadoopThriftAuthBridge.Server saslServer;
  private final HiveConf conf;
  private final AuthType authType;
  private String hadoopAuth;
  private MetastoreDelegationTokenManager delegationTokenManager = null;

  public HiveAuthFactory(HiveConf conf, boolean isHttpMode) throws TTransportException {
    this.conf = conf;
    // ShimLoader.getHadoopShims().isSecurityEnabled() will only check that
    // hadoopAuth is not simple, it does not guarantee it is kerberos
    hadoopAuth = conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple");
    authType = AuthType.authTypeFromConf(conf, isHttpMode);
    if (isSASLWithKerberizedHadoop()) {
      saslServer =
          HadoopThriftAuthBridge.getBridge().createServer(
              conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
              conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL),
              conf.getVar(ConfVars.HIVE_SERVER2_CLIENT_KERBEROS_PRINCIPAL));

      // Start delegation token manager
      delegationTokenManager = new MetastoreDelegationTokenManager();
      try {
        Object baseHandler = null;
        String tokenStoreClass = MetaStoreServerUtils.getTokenStoreClassName(conf);

        if (tokenStoreClass.equals(DBTokenStore.class.getName())) {
          // IMetaStoreClient is needed to access token store if DBTokenStore is to be used. It
          // will be got via Hive.get(conf).getMSC in a thread where the DelegationTokenStore
          // is called. To avoid the cyclic reference, we pass the Hive class to DBTokenStore where
          // it is used to get a threadLocal Hive object with a synchronized MetaStoreClient using
          // Java reflection.
          // Note: there will be two HS2 life-long opened MSCs, one is stored in HS2 thread local
          // Hive object, the other is in a daemon thread spawned in DelegationTokenSecretManager
          // to remove expired tokens.
          baseHandler = Hive.class;
        }

        delegationTokenManager.startDelegationTokenSecretManager(conf, baseHandler,
            HadoopThriftAuthBridge.Server.ServerMode.HIVESERVER2);
        saslServer.setSecretManager(delegationTokenManager.getSecretManager());
      }
      catch (IOException e) {
        throw new TTransportException("Failed to start token manager", e);
      }
    }
  }

  public Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP = SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    TTransportFactory transportFactory;
    TSaslServerTransport.Factory serverTransportFactory;

    if (isSASLWithKerberizedHadoop()) {
      try {
        serverTransportFactory = saslServer.createSaslServerTransportFactory(
            getSaslProperties());
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
      if (authType.isPasswordBasedAuthEnabled()) {
        String authTypeStr = authType.getPasswordBasedAuthStr();
        try {
          serverTransportFactory.addServerDefinition("PLAIN",
              authTypeStr, null, new HashMap<String, String>(),
              new PlainSaslHelper.PlainServerCallbackHandler(authTypeStr));
        } catch (AuthenticationException e) {
          throw new LoginException ("Error setting callback handler" + e);
        }
      }
      transportFactory = saslServer.wrapTransportFactory(serverTransportFactory);
    } else if (authType.isPasswordBasedAuthEnabled()) {
      String authTypeStr = authType.getPasswordBasedAuthStr();
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authType.isEnabled(HiveAuthConstants.AuthTypes.NOSASL)) {
      transportFactory = new TTransportFactory();
    } else {
      throw new LoginException("Unsupported authentication type " + authType.getAuthTypes());
    }

    String trustedDomain = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TRUSTED_DOMAIN).trim();
    if (!trustedDomain.isEmpty()) {
      transportFactory = PlainSaslHelper.getDualPlainTransportFactory(transportFactory, trustedDomain);
    }
    return transportFactory;
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   * @param service
   * @return
   * @throws LoginException
   */
  public TProcessorFactory getAuthProcFactory(TCLIService.Iface service) throws LoginException {
    if (isSASLWithKerberizedHadoop()) {
      return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    } else {
      return PlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public String getRemoteUser() {
    return saslServer == null ? null : saslServer.getRemoteUser();
  }

  public String getIpAddress() {
    if (saslServer == null || saslServer.getRemoteAddress() == null) {
      return null;
    } else {
      return saslServer.getRemoteAddress().getHostAddress();
    }
  }

  public String getUserAuthMechanism() {
    return saslServer == null ? null : saslServer.getUserAuthMechanism();
  }

  public boolean isSASLWithKerberizedHadoop() {
    return "kerberos".equalsIgnoreCase(hadoopAuth)
        && !authType.isEnabled(HiveAuthConstants.AuthTypes.NOSASL);
  }

  public boolean isSASLKerberosUser() {
    return AuthMethod.KERBEROS.getMechanismName().equals(getUserAuthMechanism())
            || AuthMethod.TOKEN.getMechanismName().equals(getUserAuthMechanism());
  }

  // Perform kerberos login using the hadoop shim API if the configuration is available
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 Kerberos principal or keytab is not correctly configured");
    } else {
      UserGroupInformation.loginUserFromKeytab(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  // Perform SPNEGO login using the hadoop shim API if the configuration is available
  public static UserGroupInformation loginFromSpnegoKeytabAndReturnUGI(HiveConf hiveConf)
    throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 SPNEGO principal or keytab is not correctly configured");
    } else {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer, String remoteAddr)
      throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }

    try {
      String tokenStr = delegationTokenManager.getDelegationTokenWithService(owner, renewer,
          HiveAuthConstants.HS2_CLIENT_TOKEN, remoteAddr);
      if (tokenStr == null || tokenStr.isEmpty()) {
        throw new HiveSQLException(
            "Received empty retrieving delegation token for user " + owner, "08S01");
      }
      return tokenStr;
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error retrieving delegation token for user " + owner, "08S01", e);
    } catch (InterruptedException e) {
      throw new HiveSQLException("delegation token retrieval interrupted", "08S01", e);
    }
  }

  // cancel given delegation token
  public void cancelDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      delegationTokenManager.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error canceling delegation token " + delegationToken, "08S01", e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      delegationTokenManager.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error renewing delegation token " + delegationToken, "08S01", e);
    }
  }

  public String verifyDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return delegationTokenManager.verifyDelegationToken(delegationToken);
    } catch (IOException e) {
      String msg =  "Error verifying delegation token " + delegationToken;
      LOG.error(msg, e);
      throw new HiveSQLException(msg, "08S01", e);
    }
  }

  public String getUserFromToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return delegationTokenManager.getUserFromToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error extracting user from delegation token " + delegationToken, "08S01", e);
    }
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
    HiveConf hiveConf) throws HiveSQLException {
    try {
      UserGroupInformation sessionUgi;
      if (UserGroupInformation.isSecurityEnabled()) {
        KerberosNameShim kerbName = ShimLoader.getHadoopShims().getKerberosNameShim(realUser);
        sessionUgi = UserGroupInformation.createProxyUser(
            kerbName.getServiceName(), UserGroupInformation.getLoginUser());
      } else {
        sessionUgi = UserGroupInformation.createRemoteUser(realUser);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf);
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
            ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException(
        "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e);
    }
  }
}
