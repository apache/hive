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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthFactory.class);

  public static enum AuthTypes {
    NOSASL("NOSASL"),
    NONE("NONE"),
    LDAP("LDAP"),
    KERBEROS("KERBEROS"),
    CUSTOM("CUSTOM"),
    PAM("PAM");

    private String authType;

    AuthTypes(String authType) {
      this.authType = authType;
    }

    public String getAuthName() {
      return authType;
    }

  };

  private HadoopThriftAuthBridge.Server saslServer = null;
  private String authTypeStr;
  private String transportMode;
  private final HiveConf conf;

  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static final String HS2_CLIENT_TOKEN = "hiveserver2ClientToken";

  public HiveAuthFactory(HiveConf conf) throws TTransportException {
    this.conf = conf;
    transportMode = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    authTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);

    // In http mode we use NOSASL as the default auth type
    if (transportMode.equalsIgnoreCase("http")) {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NOSASL.getAuthName();
      }
    }
    else {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NONE.getAuthName();
      }
      if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())
          && ShimLoader.getHadoopShims().isSecureShimImpl()) {
        saslServer = ShimLoader.getHadoopThriftAuthBridge().createServer(
            conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
            conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
            );
        // start delegation token manager
        try {
          saslServer.startDelegationTokenSecretManager(conf, null);
        } catch (IOException e) {
          throw new TTransportException("Failed to start token manager", e);
        }
      }
    }
  }

  public Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP =
        SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    // hadoop.rpc.protection being set to a higher level than hive.server2.thrift.rpc.protection
    // does not make sense in most situations. Log warning message in such cases.
    Map<String, String> hadoopSaslProps =  ShimLoader.getHadoopThriftAuthBridge().
        getHadoopSaslProperties(conf);
    SaslQOP hadoopSaslQOP = SaslQOP.fromString(hadoopSaslProps.get(Sasl.QOP));
    if(hadoopSaslQOP.ordinal() > saslQOP.ordinal()) {
      LOG.warn(MessageFormat.format("\"hadoop.rpc.protection\" is set to higher security level " +
          "{0} then {1} which is set to {2}", hadoopSaslQOP.toString(),
          ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname, saslQOP.toString()));
    }
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    TTransportFactory transportFactory;
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      try {
        transportFactory = saslServer.createTransportFactory(getSaslProperties());
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NONE.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.LDAP.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.PAM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName())) {
      transportFactory = new TTransportFactory();
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.CUSTOM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr);
    }
    return transportFactory;
  }

  public TProcessorFactory getAuthProcFactory(ThriftCLIService service)
      throws LoginException {
    if (transportMode.equalsIgnoreCase("http")) {
      return HttpAuthUtils.getAuthProcFactory(service);
    }
    else {
      if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
        return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
      } else {
        return PlainSaslHelper.getPlainProcessorFactory(service);
      }
    }
  }

  public String getRemoteUser() {
    if (saslServer != null) {
      return saslServer.getRemoteUser();
    } else {
      return null;
    }
  }

  public String getIpAddress() {
    if(saslServer != null && saslServer.getRemoteAddress() != null) {
      return saslServer.getRemoteAddress().getHostAddress();
    } else {
      return null;
    }
  }

  // Perform kerberos login using the hadoop shim API if the configuration is available
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (!principal.isEmpty() && !keyTabFile.isEmpty()) {
      ShimLoader.getHadoopShims().loginUserFromKeytab(principal, keyTabFile);
    } else {
      throw new IOException ("HiveServer2 kerberos principal or keytab " +
          "is not correctly configured");
    }
  }

  // Perform spnego login using the hadoop shim API if the configuration is available
  public static UserGroupInformation loginFromSpnegoKeytabAndReturnUGI(
      HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
    if (!principal.isEmpty() && !keyTabFile.isEmpty()) {
      return ShimLoader.getHadoopShims().loginUserFromKeytabAndReturnUGI(
          principal, keyTabFile);
    } else {
      throw new IOException ("HiveServer2 SPNego principal or keytab " +
          "is not correctly configured");
    }
  }

  public static TTransport getSocketTransport(String host, int port, int loginTimeout)
      throws TTransportException {
    return new TSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout)
      throws TTransportException {
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout,
      String trustStorePath, String trustStorePassWord) throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    params.setTrustStore(trustStorePath, trustStorePassWord);
    params.requireClientAuth(true);
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout, params);
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
      throws TTransportException {
    InetSocketAddress serverAddress = null;
    if (hiveHost != null && !hiveHost.isEmpty()) {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    } else {
      serverAddress = new  InetSocketAddress(portNum);
    }
    return new TServerSocket(serverAddress );
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum,
      String keyStorePath, String keyStorePassWord) throws TTransportException, UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keyStorePath, keyStorePassWord);

    InetAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      serverAddress = InetAddress.getLocalHost();
    } else {
      serverAddress = InetAddress.getByName(hiveHost);
    }
    return TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress, params);
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication");
    }

    try {
      String tokenStr = saslServer.getDelegationTokenWithService(owner, renewer, HS2_CLIENT_TOKEN);
      if (tokenStr == null || tokenStr.isEmpty()) {
        throw new HiveSQLException("Received empty retrieving delegation token for user " + owner);
      }
      return tokenStr;
    } catch (IOException e) {
      throw new HiveSQLException("Error retrieving delegation token for user " + owner, e);
    } catch (InterruptedException e) {
      throw new HiveSQLException("delegation token retrieval interrupted", e);
    }
  }

  // cancel given delegation token
  public void cancelDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication");
    }
    try {
      saslServer.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException("Error canceling delegation token " + delegationToken, e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication");
    }
    try {
      saslServer.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException("Error renewing delegation token " + delegationToken, e);
    }
  }

  public String getUserFromToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication");
    }
    try {
      return saslServer.getUserFromToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException("Error extracting user from delegation token " + delegationToken, e);
    }
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
      HiveConf hiveConf) throws HiveSQLException {
    UserGroupInformation sessionUgi;

    try {
      if (ShimLoader.getHadoopShims().isSecurityEnabled()) {
        sessionUgi = ShimLoader.getHadoopShims().createProxyUser(realUser);
      } else {
        sessionUgi = ShimLoader.getHadoopShims().createRemoteUser(realUser, null);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ShimLoader.getHadoopShims().
        authorizeProxyAccess(proxyUser, sessionUgi, ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException("Failed to validate proxy privilage of " + realUser +
          " for " + proxyUser, e);
    }
  }

}
