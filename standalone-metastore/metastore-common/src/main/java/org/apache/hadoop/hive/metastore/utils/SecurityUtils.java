/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.base.Preconditions;
import java.io.FileInputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.security.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SecurityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    String doAs = System.getenv("HADOOP_USER_NAME");
    if (doAs != null && doAs.length() > 0) {
     /*
      * this allows doAs (proxy user) to be passed along across process boundary where
      * delegation tokens are not supported.  For example, a DDL stmt via WebHCat with
      * a doAs parameter, forks to 'hcat' which needs to start a Session that
      * proxies the end user
      */
      return UserGroupInformation.createProxyUser(doAs, UserGroupInformation.getLoginUser());
    }
    return UserGroupInformation.getCurrentUser();
  }
  /**
   * Dynamically sets up the JAAS configuration that uses kerberos
   * @param principal
   * @param keyTabFile
   * @throws IOException
   */
  public static void setZookeeperClientKerberosJaasConfig(String principal, String keyTabFile) throws IOException {
    // ZooKeeper property name to pick the correct JAAS conf section
    final String SASL_LOGIN_CONTEXT_NAME = "HiveZooKeeperClient";
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, SASL_LOGIN_CONTEXT_NAME);

    principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    JaasConfiguration jaasConf = new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal, keyTabFile);

    // Install the Configuration in the runtime.
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
  }

  /**
   * A JAAS configuration for ZooKeeper clients intended to use for SASL
   * Kerberos.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    // Current installed Configuration
    private static final boolean IBM_JAVA = System.getProperty("java.vendor")
      .contains("IBM");
    private final javax.security.auth.login.Configuration baseConfig = javax.security.auth.login.Configuration
        .getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String hiveLoginContextName, String principal, String keyTabFile) {
      this.loginContextName = hiveLoginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap<String, String>();
        if (IBM_JAVA) {
          krbOptions.put("credsType", "both");
          krbOptions.put("useKeytab", keyTabFile);
        } else {
          krbOptions.put("doNotPrompt", "true");
          krbOptions.put("storeKey", "true");
          krbOptions.put("useKeyTab", "true");
          krbOptions.put("keyTab", keyTabFile);
        }
  krbOptions.put("principal", principal);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry hiveZooKeeperClientEntry = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(), LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[] { hiveZooKeeperClientEntry };
      }
      // Try the base config
      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }
      return null;
    }
  }
  
  /**
   * Get the string form of the token given a token signature. The signature is used as the value of
   * the "service" field in the token for lookup. Ref: AbstractDelegationTokenSelector in Hadoop. If
   * there exists such a token in the token cache (credential store) of the job, the lookup returns
   * that. This is relevant only when running against a "secure" hadoop release The method gets hold
   * of the tokens if they are set up by hadoop - this should happen on the map/reduce tasks if the
   * client added the tokens into hadoop's credential store in the front end during job submission.
   * The method will select the hive delegation token among the set of tokens and return the string
   * form of it
   * 
   * @param tokenSignature
   * @return the string form of the token found
   * @throws IOException
   */
  public static String getTokenStrForm(String tokenSignature) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    TokenSelector<? extends TokenIdentifier> tokenSelector = new DelegationTokenSelector();

    Token<? extends TokenIdentifier> token = tokenSelector.selectToken(
        tokenSignature == null ? new Text() : new Text(tokenSignature), ugi.getTokens());
    return token != null ? token.encodeToUrlString() : null;
  }
  
  /**
   * Create a delegation token object for the given token string and service. Add the token to given
   * UGI
   * 
   * @param ugi
   * @param tokenStr
   * @param tokenService
   * @throws IOException
   */
  public static void setTokenStr(UserGroupInformation ugi, String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    ugi.addToken(delegationToken);
  }

  /**
   * Create a new token using the given string and service
   * 
   * @param tokenStr
   * @param tokenService
   * @return
   * @throws IOException
   */
  private static Token<DelegationTokenIdentifier> createToken(String tokenStr, String tokenService)
      throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }


  /**
   * @return the user name set in hadoop.job.ugi param or the current user from System
   * @throws IOException if underlying Hadoop call throws LoginException
   */
  public static String getUser() throws IOException {
    try {
      UserGroupInformation ugi = getUGI();
      return ugi.getUserName();
    } catch (LoginException le) {
      throw new IOException(le);
    }
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum) throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
      String keyStorePassWord, String keyStoreType, String keyStoreAlgorithm, List<String> sslVersionBlacklist)
      throws TTransportException, UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    String kStoreType = keyStoreType.isEmpty()? KeyStore.getDefaultType() : keyStoreType;
    String kStoreAlgorithm = keyStoreAlgorithm.isEmpty()?
            KeyManagerFactory.getDefaultAlgorithm() : keyStoreAlgorithm;
    params.setKeyStore(keyStorePath, keyStorePassWord, kStoreAlgorithm, kStoreType);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket =
        TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase());
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<>();
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
    return thriftServerSocket;
  }

  public static TTransport getSSLSocket(String host, int port, int socketTimeout, int connectionTimeout,
      String trustStorePath, String trustStorePassWord, String trustStoreType,
      String trustStoreAlgorithm) throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
        new TSSLTransportFactory.TSSLTransportParameters();
    String tStoreType = trustStoreType.isEmpty()? KeyStore.getDefaultType() : trustStoreType;
    String tStoreAlgorithm = trustStoreAlgorithm.isEmpty()?
        TrustManagerFactory.getDefaultAlgorithm() : trustStoreAlgorithm;
    params.setTrustStore(trustStorePath, trustStorePassWord,
        tStoreAlgorithm, tStoreType);
    params.requireClientAuth(true);
    // The underlying SSLSocket object is bound to host:port with the given SO_TIMEOUT and
    // connection timeout and SSLContext created with the given params
    TSocket tSSLSocket = TSSLTransportFactory.getClientSocket(host, port, socketTimeout, params);
    tSSLSocket.setConnectTimeout(connectionTimeout);
    return getSSLSocketWithHttps(tSSLSocket);
  }

  /*
  Sets the ssl related configs in the underlying http client builder and wrap it up
  in a THttpClient
   */
  public static THttpClient getThriftHttpsClient(String httpsUrl, String trustStorePath,
      String trustStorePasswd, String trustStoreAlgorithm, String trustStoreType,
      HttpClientBuilder underlyingHttpClientBuilder) throws TTransportException, IOException,
      KeyStoreException, NoSuchAlgorithmException, CertificateException,
      KeyManagementException {
    Preconditions.checkNotNull(underlyingHttpClientBuilder, "httpClientBuilder should not be null");
    if (trustStoreType == null || trustStoreType.isEmpty()) {
      trustStoreType = KeyStore.getDefaultType();
    }
    KeyStore sslTrustStore = KeyStore.getInstance(trustStoreType);
    try (FileInputStream fis = new FileInputStream(trustStorePath)) {
      sslTrustStore.load(fis, trustStorePasswd.toCharArray());
    }

    SSLContext sslContext =
        SSLContexts.custom().setTrustManagerFactoryAlgorithm(trustStoreAlgorithm).
            loadTrustMaterial(sslTrustStore, null).build();
    SSLConnectionSocketFactory socketFactory =
        new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier(null));
    final Registry<ConnectionSocketFactory> registry =
        RegistryBuilder.<ConnectionSocketFactory> create().register("https", socketFactory)
            .build();
    underlyingHttpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
    return new THttpClient(httpsUrl, underlyingHttpClientBuilder.build());
  }

  // Using endpoint identification algorithm as HTTPS enables us to do
  // CNAMEs/subjectAltName verification
  private static TSocket getSSLSocketWithHttps(TSocket tSSLSocket) throws TTransportException {
    SSLSocket sslSocket = (SSLSocket) tSSLSocket.getSocket();
    SSLParameters sslParams = sslSocket.getSSLParameters();
    sslParams.setEndpointIdentificationAlgorithm("HTTPS");
    sslSocket.setSSLParameters(sslParams);
    return new TSocket(sslSocket);
  }

  /**
   * Relogin if login user is logged in using keytab
   * Relogin is actually done by ugi code only if sufficient time has passed
   * A no-op if kerberos security is not enabled
   * @throws MetaException
   */
  public static void reloginExpiringKeytabUser() throws MetaException {
    if(!UserGroupInformation.isSecurityEnabled()){
      return;
    }
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      //checkTGT calls ugi.relogin only after checking if it is close to tgt expiry
      //hadoop relogin is actually done only every x minutes (x=10 in hadoop 1.x)
      if(ugi.isFromKeytab()){
        ugi.checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      String msg = "Error doing relogin using keytab " + e.getMessage();
      LOG.error(msg, e);
      throw new MetaException(msg);
    }
  }
}
