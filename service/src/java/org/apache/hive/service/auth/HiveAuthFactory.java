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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLServerSocket;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
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

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class HiveAuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthFactory.class);

  public enum AuthTypes {
    NOSASL, NONE, LDAP, KERBEROS, CUSTOM, PAM
  }

  public static enum TransTypes {
    HTTP {
      AuthTypes getDefaultAuthType() {
        return AuthTypes.NOSASL;
      }
    },
    BINARY {
      AuthTypes getDefaultAuthType() {
        return AuthTypes.NONE;
      }
    };
    abstract AuthTypes getDefaultAuthType();
  }

  private final HadoopThriftAuthBridge.Server saslServer;
  private final AuthTypes authType;
  private final TransTypes transportType;
  private final int saslMessageLimit;
  private final HiveConf conf;

  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static final String HS2_CLIENT_TOKEN = "hiveserver2ClientToken";

  public HiveAuthFactory(HiveConf conf) throws TTransportException {
    this.conf = conf;
    saslMessageLimit = conf.getIntVar(ConfVars.HIVE_THRIFT_SASL_MESSAGE_LIMIT);
    String transTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    String authTypeStr = conf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
    transportType = TransTypes.valueOf(transTypeStr.toUpperCase());
    authType =
        authTypeStr == null ? transportType.getDefaultAuthType() : AuthTypes.valueOf(authTypeStr
            .toUpperCase());
    if (transportType == TransTypes.BINARY
        && authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.name())
        && ShimLoader.getHadoopShims().isSecureShimImpl()) {
      saslServer =
          ShimLoader.getHadoopThriftAuthBridge().createServer(
              conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
              conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL));
      // start delegation token manager
      try {
        saslServer.startDelegationTokenSecretManager(conf, null, ServerMode.HIVESERVER2);
      } catch (Exception e) {
        throw new TTransportException("Failed to start token manager", e);
      }
    } else {
      saslServer = null;
    }
  }

  public Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP = SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws Exception {
    if (authType == AuthTypes.KERBEROS) {
      return saslServer.createTransportFactory(getSaslProperties(), saslMessageLimit);
    }
    if (authType == AuthTypes.NOSASL) {
      return new TTransportFactory();
    }
    return PlainSaslHelper.getPlainTransportFactory(authType.name(), saslMessageLimit);
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   *
   * @param service
   * @return
   * @throws LoginException
   */
  public TProcessorFactory getAuthProcFactory(ThriftCLIService service) {
    if (authType == AuthTypes.KERBEROS) {
      return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    }
    return PlainSaslHelper.getPlainProcessorFactory(service);
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

  // Perform kerberos login using the hadoop shim API if the configuration is available
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 Kerberos principal or keytab is not correctly configured");
    } else {
      ShimLoader.getHadoopShims().loginUserFromKeytab(principal, keyTabFile);
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
      return ShimLoader.getHadoopShims().loginUserFromKeytabAndReturnUGI(principal, keyTabFile);
    }
  }

  public static TTransport getSocketTransport(String host, int port, int loginTimeout) {
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

  public static TServerSocket getServerSocket(String hiveHost, int portNum, int socketTimeout,
      boolean keepAlive) throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket serverSocket = new TServerSocket(serverAddress, socketTimeout);
    if (keepAlive) {
      serverSocket = new TServerSocketKeepAlive(serverSocket.getServerSocket());
    }
    return serverSocket;
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
      String keyStorePassWord, List<String> sslVersionBlacklist, int socketTimeout,
      boolean keepAlive) throws TTransportException, UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keyStorePath, keyStorePassWord);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket = TSSLTransportFactory.getServerSocket(portNum, socketTimeout,
        serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<String>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase());
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<String>();
      for (String protocol : sslServerSocket.getEnabledProtocols()) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase())) {
          LOG.debug("Disabling SSL Protocol: " + protocol);
        } else {
          enabledProtocols.add(protocol);
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      LOG.info("SSL Server Socket Enabled Protocols: "
          + Arrays.toString(sslServerSocket.getEnabledProtocols()));
    }
    if (keepAlive) {
      thriftServerSocket = new TServerSocketKeepAlive(thriftServerSocket.getServerSocket());
    }
    return thriftServerSocket;
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }

    try {
      String tokenStr = saslServer.getDelegationTokenWithService(owner, renewer, HS2_CLIENT_TOKEN);
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
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error canceling delegation token " + delegationToken, "08S01", e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      saslServer.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error renewing delegation token " + delegationToken, "08S01", e);
    }
  }

  public String getUserFromToken(String delegationToken) throws HiveSQLException {
    if (saslServer == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return saslServer.getUserFromToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error extracting user from delegation token " + delegationToken, "08S01", e);
    }
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
    HiveConf hiveConf) throws HiveSQLException {
    try {
      UserGroupInformation sessionUgi;
      if (ShimLoader.getHadoopShims().isSecurityEnabled()) {
        KerberosNameShim kerbName = ShimLoader.getHadoopShims().getKerberosNameShim(realUser);
        String shortPrincipalName = kerbName.getServiceName();
        sessionUgi = ShimLoader.getHadoopShims().createProxyUser(shortPrincipalName);
      } else {
        sessionUgi = ShimLoader.getHadoopShims().createRemoteUser(realUser, null);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ShimLoader.getHadoopShims().
          authorizeProxyAccess(proxyUser, sessionUgi, ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException(
        "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e);
    }
  }

  /**
   * TServerSocketKeepAlive - like TServerSocket, but will enable keepalive for
   * accepted sockets.
   * 
   */
  static class TServerSocketKeepAlive extends TServerSocket {
    public TServerSocketKeepAlive(ServerSocket serverSocket) throws TTransportException {
      super(serverSocket);
    }

    @Override
    protected TSocket acceptImpl() throws TTransportException {
      TSocket ts = super.acceptImpl();
      try {
        ts.getSocket().setKeepAlive(true);
      } catch (SocketException e) {
        throw new TTransportException(e);
      }
      return ts;
    }
  }
}
