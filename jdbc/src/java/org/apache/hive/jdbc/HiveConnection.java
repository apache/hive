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

package org.apache.hive.jdbc;

import org.apache.hive.service.rpc.thrift.TSetClientInfoResp;

import org.apache.hive.service.rpc.thrift.TSetClientInfoReq;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.session.SessionUtils;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HiveConnection.
 *
 */
public class HiveConnection implements java.sql.Connection {
  public static final Logger LOG = LoggerFactory.getLogger(HiveConnection.class.getName());

  private String jdbcUriString;
  private String host;
  private int port;
  private final Map<String, String> sessConfMap;
  private JdbcConnectionParams connParams;
  private final boolean isEmbeddedMode;
  private TTransport transport;
  private boolean assumeSubject;
  // TODO should be replaced by CliServiceClient
  private TCLIService.Iface client;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TSessionHandle sessHandle = null;
  private final List<TProtocolVersion> supportedProtocols = new LinkedList<TProtocolVersion>();
  private int loginTimeout = 0;
  private TProtocolVersion protocol;
  private int fetchSize = HiveStatement.DEFAULT_FETCH_SIZE;
  private String initFile = null;
  private String wmPool = null, wmApp = null;
  private Properties clientInfo;
  private Subject loggedInSubject;

  /**
   * Get all direct HiveServer2 URLs from a ZooKeeper based HiveServer2 URL
   * @param zookeeperBasedHS2Url
   * @return
   * @throws Exception
   */
  public static List<JdbcConnectionParams> getAllUrls(String zookeeperBasedHS2Url) throws Exception {
    JdbcConnectionParams params = Utils.parseURL(zookeeperBasedHS2Url, new Properties());
    // if zk is disabled or if HA service discovery is enabled we return the already populated params.
    // in HA mode, params is already populated with Active server host info.
    if (params.getZooKeeperEnsemble() == null ||
      ZooKeeperHiveClientHelper.isZkHADynamicDiscoveryMode(params.getSessionVars())) {
      return Collections.singletonList(params);
    }
    return ZooKeeperHiveClientHelper.getDirectParamsList(params);
  }

  public HiveConnection(String uri, Properties info) throws SQLException {
    setupLoginTimeout();
    try {
      connParams = Utils.parseURL(uri, info);
    } catch (ZooKeeperHiveClientException e) {
      throw new SQLException(e);
    }
    jdbcUriString = connParams.getJdbcUriString();
    // JDBC URL: jdbc:hive2://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list
    // each list: <key1>=<val1>;<key2>=<val2> and so on
    // sess_var_list -> sessConfMap
    // hive_conf_list -> hiveConfMap
    // hive_var_list -> hiveVarMap
    sessConfMap = connParams.getSessionVars();
    if (isKerberosAuthMode()) {
      host = Utils.getCanonicalHostName(connParams.getHost());
    } else {
      host = connParams.getHost();
    }
    port = connParams.getPort();
    isEmbeddedMode = connParams.isEmbeddedMode();

    if (sessConfMap.containsKey(JdbcConnectionParams.FETCH_SIZE)) {
      fetchSize = Integer.parseInt(sessConfMap.get(JdbcConnectionParams.FETCH_SIZE));
    }
    if (sessConfMap.containsKey(JdbcConnectionParams.INIT_FILE)) {
      initFile = sessConfMap.get(JdbcConnectionParams.INIT_FILE);
    }
    wmPool = sessConfMap.get(JdbcConnectionParams.WM_POOL);
    for (String application : JdbcConnectionParams.APPLICATION) {
      wmApp = sessConfMap.get(application);
      if (wmApp != null) break;
    }

    // add supported protocols
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);

    if (isEmbeddedMode) {
      EmbeddedThriftBinaryCLIService embeddedClient = new EmbeddedThriftBinaryCLIService();
      embeddedClient.init(null, connParams.getHiveConfs());
      client = embeddedClient;
      connParams.getHiveConfs().clear();
      // open client session
      openSession();
      executeInitSql();
    } else {
      int maxRetries = 1;
      try {
        String strRetries = sessConfMap.get(JdbcConnectionParams.RETRIES);
        if (StringUtils.isNotBlank(strRetries)) {
          maxRetries = Integer.parseInt(strRetries);
        }
      } catch(NumberFormatException e) { // Ignore the exception
      }

      for (int numRetries = 0;;) {
        try {
          // open the client transport
          openTransport();
          // set up the client
          client = new TCLIService.Client(new TBinaryProtocol(transport));
          // open client session
          openSession();
          executeInitSql();

          break;
        } catch (Exception e) {
          LOG.warn("Failed to connect to " + connParams.getHost() + ":" + connParams.getPort());
          String errMsg = null;
          String warnMsg = "Could not open client transport with JDBC Uri: " + jdbcUriString + ": ";
          if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(sessConfMap)) {
            errMsg = "Could not open client transport for any of the Server URI's in ZooKeeper: ";
            // Try next available server in zookeeper, or retry all the servers again if retry is enabled
            while(!Utils.updateConnParamsFromZooKeeper(connParams) && ++numRetries < maxRetries) {
              connParams.getRejectedHostZnodePaths().clear();
            }
            // Update with new values
            jdbcUriString = connParams.getJdbcUriString();
            if (isKerberosAuthMode()) {
              host = Utils.getCanonicalHostName(connParams.getHost());
            } else {
              host = connParams.getHost();
            }
            port = connParams.getPort();
          } else {
            errMsg = warnMsg;
            ++numRetries;
          }

          if (numRetries >= maxRetries) {
            throw new SQLException(errMsg + e.getMessage(), " 08S01", e);
          } else {
            LOG.warn(warnMsg + e.getMessage() + " Retrying " + numRetries + " of " + maxRetries);
          }
        }
      }
    }

    // Wrap the client with a thread-safe proxy to serialize the RPC calls
    client = newSynchronizedClient(client);
  }

  private void executeInitSql() throws SQLException {
    if (initFile != null) {
      try {
        List<String> sqlList = parseInitFile(initFile);
        Statement st = createStatement();
        for(String sql : sqlList) {
          boolean hasResult = st.execute(sql);
          if (hasResult) {
            ResultSet rs = st.getResultSet();
            while (rs.next()) {
              System.out.println(rs.getString(1));
            }
          }
        }
      } catch(Exception e) {
        LOG.error("Failed to execute initial SQL");
        throw new SQLException(e.getMessage());
      }
    }
  }

  public static List<String> parseInitFile(String initFile) throws IOException {
    File file = new File(initFile);
    BufferedReader br = null;
    List<String> initSqlList = null;
    try {
      FileInputStream input = new FileInputStream(file);
      br = new BufferedReader(new InputStreamReader(input, "UTF-8"));
      String line;
      StringBuilder sb = new StringBuilder("");
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() != 0) {
          if (line.startsWith("#") || line.startsWith("--")) {
            continue;
          } else {
            line = line.concat(" ");
            sb.append(line);
          }
        }
      }
      initSqlList = getInitSql(sb.toString());
    } catch(IOException e) {
      LOG.error("Failed to read initial SQL file", e);
      throw new IOException(e);
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return initSqlList;
  }

  private static List<String> getInitSql(String sbLine) {
    char[] sqlArray = sbLine.toCharArray();
    List<String> initSqlList = new ArrayList();
    int index = 0;
    int beginIndex = 0;
    for (; index < sqlArray.length; index++) {
      if (sqlArray[index] == ';') {
        String sql = sbLine.substring(beginIndex, index).trim();
        initSqlList.add(sql);
        beginIndex = index + 1;
      }
    }
    return initSqlList;
  }


  private void openTransport() throws Exception {
    assumeSubject =
        JdbcConnectionParams.AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT.equals(sessConfMap
            .get(JdbcConnectionParams.AUTH_KERBEROS_AUTH_TYPE));
    transport = isHttpTransportMode() ? createHttpTransport() : createBinaryTransport();
    if (!transport.isOpen()) {
      transport.open();
    }
    logZkDiscoveryMessage("Connected to " + connParams.getHost() + ":" + connParams.getPort());
  }

  public String getConnectedUrl() {
    return jdbcUriString;
  }

  private String getServerHttpUrl(boolean useSsl) {
    // Create the http/https url
    // JDBC driver will set up an https url if ssl is enabled, otherwise http
    String schemeName = useSsl ? "https" : "http";
    // http path should begin with "/"
    String httpPath;
    httpPath = sessConfMap.get(JdbcConnectionParams.HTTP_PATH);
    if (httpPath == null) {
      httpPath = "/";
    } else if (!httpPath.startsWith("/")) {
      httpPath = "/" + httpPath;
    }
    return schemeName + "://" + host + ":" + port + httpPath;
  }

  private TTransport createHttpTransport() throws SQLException, TTransportException {
    CloseableHttpClient httpClient;
    boolean useSsl = isSslConnection();
    // Create an http client from the configs
    httpClient = getHttpClient(useSsl);
    transport = new THttpClient(getServerHttpUrl(useSsl), httpClient);
    return transport;
  }

  private CloseableHttpClient getHttpClient(Boolean useSsl) throws SQLException {
    boolean isCookieEnabled = sessConfMap.get(JdbcConnectionParams.COOKIE_AUTH) == null ||
      (!JdbcConnectionParams.COOKIE_AUTH_FALSE.equalsIgnoreCase(
      sessConfMap.get(JdbcConnectionParams.COOKIE_AUTH)));
    String cookieName = sessConfMap.get(JdbcConnectionParams.COOKIE_NAME) == null ?
      JdbcConnectionParams.DEFAULT_COOKIE_NAMES_HS2 :
      sessConfMap.get(JdbcConnectionParams.COOKIE_NAME);
    CookieStore cookieStore = isCookieEnabled ? new BasicCookieStore() : null;
    HttpClientBuilder httpClientBuilder;
    // Request interceptor for any request pre-processing logic
    HttpRequestInterceptor requestInterceptor;
    Map<String, String> additionalHttpHeaders = new HashMap<String, String>();
    Map<String, String> customCookies = new HashMap<String, String>();

    // Retrieve the additional HttpHeaders
    for (Map.Entry<String, String> entry : sessConfMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(JdbcConnectionParams.HTTP_HEADER_PREFIX)) {
        additionalHttpHeaders.put(key.substring(JdbcConnectionParams.HTTP_HEADER_PREFIX.length()),
          entry.getValue());
      }
      if (key.startsWith(JdbcConnectionParams.HTTP_COOKIE_PREFIX)) {
        customCookies.put(key.substring(JdbcConnectionParams.HTTP_COOKIE_PREFIX.length()),
          entry.getValue());
      }
    }
    // Configure http client for kerberos/password based authentication
    if (isKerberosAuthMode()) {
      if (assumeSubject) {
        // With this option, we're assuming that the external application,
        // using the JDBC driver has done a JAAS kerberos login already
        AccessControlContext context = AccessController.getContext();
        loggedInSubject = Subject.getSubject(context);
        if (loggedInSubject == null) {
          throw new SQLException("The Subject is not set");
        }
      }
      /**
       * Add an interceptor which sets the appropriate header in the request.
       * It does the kerberos authentication and get the final service ticket,
       * for sending to the server before every request.
       * In https mode, the entire information is encrypted
       */
      requestInterceptor = new HttpKerberosRequestInterceptor(sessConfMap.get(JdbcConnectionParams.AUTH_PRINCIPAL),
          host, getServerHttpUrl(useSsl), loggedInSubject, cookieStore, cookieName, useSsl, additionalHttpHeaders,
          customCookies);
    } else {
      // Check for delegation token, if present add it in the header
      String tokenStr = getClientDelegationToken(sessConfMap);
      if (tokenStr != null) {
        requestInterceptor = new HttpTokenAuthInterceptor(tokenStr, cookieStore, cookieName, useSsl,
            additionalHttpHeaders, customCookies);
      } else {
      /**
       * Add an interceptor to pass username/password in the header.
       * In https mode, the entire information is encrypted
       */
        requestInterceptor = new HttpBasicAuthInterceptor(getUserName(), getPassword(), cookieStore,
            cookieName, useSsl, additionalHttpHeaders, customCookies);
      }
    }
    // Configure http client for cookie based authentication
    if (isCookieEnabled) {
      // Create a http client with a retry mechanism when the server returns a status code of 401.
      httpClientBuilder =
          HttpClients.custom().setServiceUnavailableRetryStrategy(
              new ServiceUnavailableRetryStrategy() {
                @Override
                public boolean retryRequest(final HttpResponse response, final int executionCount,
                    final HttpContext context) {
                  int statusCode = response.getStatusLine().getStatusCode();
                  boolean ret = statusCode == 401 && executionCount <= 1;

                  // Set the context attribute to true which will be interpreted by the request
                  // interceptor
                  if (ret) {
                    context.setAttribute(Utils.HIVE_SERVER2_RETRY_KEY,
                        Utils.HIVE_SERVER2_RETRY_TRUE);
                  }
                  return ret;
                }

                @Override
                public long getRetryInterval() {
                  // Immediate retry
                  return 0;
                }
              });
    } else {
      httpClientBuilder = HttpClientBuilder.create();
    }
    // In case the server's idletimeout is set to a lower value, it might close it's side of
    // connection. However we retry one more time on NoHttpResponseException
    httpClientBuilder.setRetryHandler(new HttpRequestRetryHandler() {
      @Override
      public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        if (executionCount > 1) {
          LOG.info("Retry attempts to connect to server exceeded.");
          return false;
        }
        if (exception instanceof org.apache.http.NoHttpResponseException) {
          LOG.info("Could not connect to the server. Retrying one more time.");
          return true;
        }
        return false;
      }
    });

    // Add the request interceptor to the client builder
    httpClientBuilder.addInterceptorFirst(requestInterceptor);

    // Add an interceptor to add in an XSRF header
    httpClientBuilder.addInterceptorLast(new XsrfHttpRequestInterceptor());

    // Configure http client for SSL
    if (useSsl) {
      String useTwoWaySSL = sessConfMap.get(JdbcConnectionParams.USE_TWO_WAY_SSL);
      String sslTrustStorePath = sessConfMap.get(JdbcConnectionParams.SSL_TRUST_STORE);
      String sslTrustStorePassword = sessConfMap.get(
        JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);
      KeyStore sslTrustStore;
      SSLConnectionSocketFactory socketFactory;
      SSLContext sslContext;
      /**
       * The code within the try block throws: SSLInitializationException, KeyStoreException,
       * IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException &
       * UnrecoverableKeyException. We don't want the client to retry on any of these,
       * hence we catch all and throw a SQLException.
       */
      try {
        if (useTwoWaySSL != null && useTwoWaySSL.equalsIgnoreCase(JdbcConnectionParams.TRUE)) {
          socketFactory = getTwoWaySSLSocketFactory();
        } else if (sslTrustStorePath == null || sslTrustStorePath.isEmpty()) {
          // Create a default socket factory based on standard JSSE trust material
          socketFactory = SSLConnectionSocketFactory.getSocketFactory();
        } else {
          // Pick trust store config from the given path
          sslTrustStore = KeyStore.getInstance(JdbcConnectionParams.SSL_TRUST_STORE_TYPE);
          try (FileInputStream fis = new FileInputStream(sslTrustStorePath)) {
            sslTrustStore.load(fis, sslTrustStorePassword.toCharArray());
          }
          sslContext = SSLContexts.custom().loadTrustMaterial(sslTrustStore, null).build();
          socketFactory =
              new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier(null));
        }
        final Registry<ConnectionSocketFactory> registry =
            RegistryBuilder.<ConnectionSocketFactory> create().register("https", socketFactory)
                .build();
        httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
      } catch (Exception e) {
        String msg =
            "Could not create an https connection to " + jdbcUriString + ". " + e.getMessage();
        throw new SQLException(msg, " 08S01", e);
      }
    }
    return httpClientBuilder.build();
  }

  /**
   * Create underlying SSL or non-SSL transport
   *
   * @return TTransport
   * @throws TTransportException
   */
  private TTransport createUnderlyingTransport() throws TTransportException {
    TTransport transport = null;
    // Note: Thrift returns an SSL socket that is already bound to the specified host:port
    // Therefore an open called on this would be a no-op later
    // Hence, any TTransportException related to connecting with the peer are thrown here.
    // Bubbling them up the call hierarchy so that a retry can happen in openTransport,
    // if dynamic service discovery is configured.
    if (isSslConnection()) {
      // get SSL socket
      String sslTrustStore = sessConfMap.get(JdbcConnectionParams.SSL_TRUST_STORE);
      String sslTrustStorePassword = sessConfMap.get(
        JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);

      if (sslTrustStore == null || sslTrustStore.isEmpty()) {
        transport = HiveAuthUtils.getSSLSocket(host, port, loginTimeout);
      } else {
        transport = HiveAuthUtils.getSSLSocket(host, port, loginTimeout,
            sslTrustStore, sslTrustStorePassword);
      }
    } else {
      // get non-SSL socket transport
      transport = HiveAuthUtils.getSocketTransport(host, port, loginTimeout);
    }
    return transport;
  }

  /**
   * Create transport per the connection options
   * Supported transport options are:
   *   - SASL based transports over
   *      + Kerberos
   *      + Delegation token
   *      + SSL
   *      + non-SSL
   *   - Raw (non-SASL) socket
   *
   *   Kerberos and Delegation token supports SASL QOP configurations
   * @throws SQLException, TTransportException
   */
  private TTransport createBinaryTransport() throws SQLException, TTransportException {
    try {
      TTransport socketTransport = createUnderlyingTransport();
      // handle secure connection if specified
      if (!JdbcConnectionParams.AUTH_SIMPLE.equals(sessConfMap.get(JdbcConnectionParams.AUTH_TYPE))) {
        // If Kerberos
        Map<String, String> saslProps = new HashMap<String, String>();
        SaslQOP saslQOP = SaslQOP.AUTH;
        if (sessConfMap.containsKey(JdbcConnectionParams.AUTH_QOP)) {
          try {
            saslQOP = SaslQOP.fromString(sessConfMap.get(JdbcConnectionParams.AUTH_QOP));
          } catch (IllegalArgumentException e) {
            throw new SQLException("Invalid " + JdbcConnectionParams.AUTH_QOP +
                " parameter. " + e.getMessage(), "42000", e);
          }
          saslProps.put(Sasl.QOP, saslQOP.toString());
        } else {
          // If the client did not specify qop then just negotiate the one supported by server
          saslProps.put(Sasl.QOP, "auth-conf,auth-int,auth");
        }
        saslProps.put(Sasl.SERVER_AUTH, "true");
        if (sessConfMap.containsKey(JdbcConnectionParams.AUTH_PRINCIPAL)) {
          transport = KerberosSaslHelper.getKerberosTransport(
              sessConfMap.get(JdbcConnectionParams.AUTH_PRINCIPAL), host,
              socketTransport, saslProps, assumeSubject);
        } else {
          // If there's a delegation token available then use token based connection
          String tokenStr = getClientDelegationToken(sessConfMap);
          if (tokenStr != null) {
            transport = KerberosSaslHelper.getTokenTransport(tokenStr,
                host, socketTransport, saslProps);
          } else {
            // we are using PLAIN Sasl connection with user/password
            String userName = getUserName();
            String passwd = getPassword();
            // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
            transport = PlainSaslHelper.getPlainTransport(userName, passwd, socketTransport);
          }
        }
      } else {
        // Raw socket connection (non-sasl)
        transport = socketTransport;
      }
    } catch (SaslException e) {
      throw new SQLException("Could not create secure connection to "
          + jdbcUriString + ": " + e.getMessage(), " 08S01", e);
    }
    return transport;
  }

  SSLConnectionSocketFactory getTwoWaySSLSocketFactory() throws SQLException {
    SSLConnectionSocketFactory socketFactory = null;

    try {
      KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
        JdbcConnectionParams.SUNX509_ALGORITHM_STRING,
        JdbcConnectionParams.SUNJSSE_ALGORITHM_STRING);
      String keyStorePath = sessConfMap.get(JdbcConnectionParams.SSL_KEY_STORE);
      String keyStorePassword = sessConfMap.get(JdbcConnectionParams.SSL_KEY_STORE_PASSWORD);
      KeyStore sslKeyStore = KeyStore.getInstance(JdbcConnectionParams.SSL_KEY_STORE_TYPE);

      if (keyStorePath == null || keyStorePath.isEmpty()) {
        throw new IllegalArgumentException(JdbcConnectionParams.SSL_KEY_STORE
        + " Not configured for 2 way SSL connection, keyStorePath param is empty");
      }
      try (FileInputStream fis = new FileInputStream(keyStorePath)) {
        sslKeyStore.load(fis, keyStorePassword.toCharArray());
      }
      keyManagerFactory.init(sslKeyStore, keyStorePassword.toCharArray());

      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
        JdbcConnectionParams.SUNX509_ALGORITHM_STRING);
      String trustStorePath = sessConfMap.get(JdbcConnectionParams.SSL_TRUST_STORE);
      String trustStorePassword = sessConfMap.get(
        JdbcConnectionParams.SSL_TRUST_STORE_PASSWORD);
      KeyStore sslTrustStore = KeyStore.getInstance(JdbcConnectionParams.SSL_TRUST_STORE_TYPE);

      if (trustStorePath == null || trustStorePath.isEmpty()) {
        throw new IllegalArgumentException(JdbcConnectionParams.SSL_TRUST_STORE
        + " Not configured for 2 way SSL connection");
      }
      try (FileInputStream fis = new FileInputStream(trustStorePath)) {
        sslTrustStore.load(fis, trustStorePassword.toCharArray());
      }
      trustManagerFactory.init(sslTrustStore);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagerFactory.getKeyManagers(),
        trustManagerFactory.getTrustManagers(), new SecureRandom());
      socketFactory = new SSLConnectionSocketFactory(context);
    } catch (Exception e) {
      throw new SQLException("Error while initializing 2 way ssl socket factory ", e);
    }
    return socketFactory;
  }

  // Lookup the delegation token. First in the connection URL, then Configuration
  private String getClientDelegationToken(Map<String, String> jdbcConnConf)
      throws SQLException {
    String tokenStr = null;
    if (JdbcConnectionParams.AUTH_TOKEN.equalsIgnoreCase(jdbcConnConf.get(JdbcConnectionParams.AUTH_TYPE))) {
      // check delegation token in job conf if any
      try {
        tokenStr = SessionUtils.getTokenStrForm(HiveAuthConstants.HS2_CLIENT_TOKEN);
      } catch (IOException e) {
        throw new SQLException("Error reading token ", e);
      }
    }
    return tokenStr;
  }

  private void openSession() throws SQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();

    Map<String, String> openConf = new HashMap<String, String>();
    // for remote JDBC client, try to set the conf var using 'set foo=bar'
    for (Entry<String, String> hiveConf : connParams.getHiveConfs().entrySet()) {
      openConf.put("set:hiveconf:" + hiveConf.getKey(), hiveConf.getValue());
    }
    // For remote JDBC client, try to set the hive var using 'set hivevar:key=value'
    for (Entry<String, String> hiveVar : connParams.getHiveVars().entrySet()) {
      openConf.put("set:hivevar:" + hiveVar.getKey(), hiveVar.getValue());
    }
    // switch the database
    openConf.put("use:database", connParams.getDbName());
    // set the fetchSize
    openConf.put("set:hiveconf:hive.server2.thrift.resultset.default.fetch.size",
      Integer.toString(fetchSize));
    if (wmPool != null) {
      openConf.put("set:hivevar:wmpool", wmPool);
    }
    if (wmApp != null) {
      openConf.put("set:hivevar:wmapp", wmApp);
    }

    // set the session configuration
    Map<String, String> sessVars = connParams.getSessionVars();
    if (sessVars.containsKey(HiveAuthConstants.HS2_PROXY_USER)) {
      openConf.put(HiveAuthConstants.HS2_PROXY_USER,
          sessVars.get(HiveAuthConstants.HS2_PROXY_USER));
    }
    openReq.setConfiguration(openConf);

    // Store the user name in the open request in case no non-sasl authentication
    if (JdbcConnectionParams.AUTH_SIMPLE.equals(sessConfMap.get(JdbcConnectionParams.AUTH_TYPE))) {
      openReq.setUsername(sessConfMap.get(JdbcConnectionParams.AUTH_USER));
      openReq.setPassword(sessConfMap.get(JdbcConnectionParams.AUTH_PASSWD));
    }

    try {
      TOpenSessionResp openResp = client.OpenSession(openReq);

      // validate connection
      Utils.verifySuccess(openResp.getStatus());
      if (!supportedProtocols.contains(openResp.getServerProtocolVersion())) {
        throw new TException("Unsupported Hive2 protocol");
      }
      protocol = openResp.getServerProtocolVersion();
      sessHandle = openResp.getSessionHandle();

      // Update fetchSize if modified by server
      String serverFetchSize =
        openResp.getConfiguration().get("hive.server2.thrift.resultset.default.fetch.size");
      if (serverFetchSize != null) {
        fetchSize = Integer.parseInt(serverFetchSize);
      }
    } catch (TException e) {
      LOG.error("Error opening session", e);
      throw new SQLException("Could not establish connection to "
          + jdbcUriString + ": " + e.getMessage(), " 08S01", e);
    }
    isClosed = false;
  }

  /**
   * @return username from sessConfMap
   */
  private String getUserName() {
    return getSessionValue(JdbcConnectionParams.AUTH_USER, JdbcConnectionParams.ANONYMOUS_USER);
  }

  /**
   * @return password from sessConfMap
   */
  private String getPassword() {
    return getSessionValue(JdbcConnectionParams.AUTH_PASSWD, JdbcConnectionParams.ANONYMOUS_PASSWD);
  }

  private boolean isSslConnection() {
    return "true".equalsIgnoreCase(sessConfMap.get(JdbcConnectionParams.USE_SSL));
  }

  private boolean isKerberosAuthMode() {
    return !JdbcConnectionParams.AUTH_SIMPLE.equals(sessConfMap.get(JdbcConnectionParams.AUTH_TYPE))
        && sessConfMap.containsKey(JdbcConnectionParams.AUTH_PRINCIPAL);
  }

  private boolean isHttpTransportMode() {
    String transportMode = sessConfMap.get(JdbcConnectionParams.TRANSPORT_MODE);
    if(transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  private void logZkDiscoveryMessage(String message) {
    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(sessConfMap)) {
      LOG.info(message);
    }
  }

  /**
   * Lookup varName in sessConfMap, if its null or empty return the default
   * value varDefault
   * @param varName
   * @param varDefault
   * @return
   */
  private String getSessionValue(String varName, String varDefault) {
    String varValue = sessConfMap.get(varName);
    if ((varValue == null) || varValue.isEmpty()) {
      varValue = varDefault;
    }
    return varValue;
  }

  // copy loginTimeout from driver manager. Thrift timeout needs to be in millis
  private void setupLoginTimeout() {
    long timeOut = TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout());
    if (timeOut > Integer.MAX_VALUE) {
      loginTimeout = Integer.MAX_VALUE;
    } else {
      loginTimeout = (int) timeOut;
    }
  }

  public void abort(Executor executor) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getDelegationToken(String owner, String renewer) throws SQLException {
    TGetDelegationTokenReq req = new TGetDelegationTokenReq(sessHandle, owner, renewer);
    try {
      TGetDelegationTokenResp tokenResp = client.GetDelegationToken(req);
      Utils.verifySuccess(tokenResp.getStatus());
      return tokenResp.getDelegationToken();
    } catch (TException e) {
      throw new SQLException("Could not retrieve token: " +
          e.getMessage(), " 08S01", e);
    }
  }

  public void cancelDelegationToken(String tokenStr) throws SQLException {
    TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(sessHandle, tokenStr);
    try {
      TCancelDelegationTokenResp cancelResp =
          client.CancelDelegationToken(cancelReq);
      Utils.verifySuccess(cancelResp.getStatus());
      return;
    } catch (TException e) {
      throw new SQLException("Could not cancel token: " +
          e.getMessage(), " 08S01", e);
    }
  }

  public void renewDelegationToken(String tokenStr) throws SQLException {
    TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(sessHandle, tokenStr);
    try {
      TRenewDelegationTokenResp renewResp =
          client.RenewDelegationToken(cancelReq);
      Utils.verifySuccess(renewResp.getStatus());
      return;
    } catch (TException e) {
      throw new SQLException("Could not renew token: " +
          e.getMessage(), " 08S01", e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#clearWarnings()
   */

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#close()
   */

  @Override
  public void close() throws SQLException {
    if (!isClosed) {
      TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
      try {
        client.CloseSession(closeReq);
      } catch (TException e) {
        throw new SQLException("Error while cleaning up the server resources", e);
      } finally {
        isClosed = true;
        if (transport != null) {
          transport.close();
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#commit()
   */

  @Override
  public void commit() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createArrayOf(java.lang.String,
   * java.lang.Object[])
   */

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createBlob()
   */

  @Override
  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createClob()
   */

  @Override
  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createNClob()
   */

  @Override
  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createSQLXML()
   */

  @Override
  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /**
   * Creates a Statement object for sending SQL statements to the database.
   *
   * @throws SQLException
   *           if a database access error occurs.
   * @see java.sql.Connection#createStatement()
   */

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new HiveStatement(this, client, sessHandle, fetchSize);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int)
   */

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException("Statement with resultset concurrency " +
          resultSetConcurrency + " is not supported", "HYC00"); // Optional feature not implemented
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException("Statement with resultset type " + resultSetType +
          " is not supported", "HYC00"); // Optional feature not implemented
    }
    return new HiveStatement(this, client, sessHandle,
        resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE, fetchSize);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStatement(int, int, int)
   */

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
   */

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getAutoCommit()
   */

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getCatalog()
   */

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo()
   */

  @Override
  public Properties getClientInfo() throws SQLException {
    return clientInfo == null ? new Properties() : clientInfo;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo(java.lang.String)
   */

  @Override
  public String getClientInfo(String name) throws SQLException {
    if (clientInfo == null) return null;
    return clientInfo.getProperty(name);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getHoldability()
   */

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getMetaData()
   */

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    return new HiveDatabaseMetaData(this, client, sessHandle);
  }

  public int getNetworkTimeout() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getSchema() throws SQLException {
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    try (Statement stmt = createStatement();
         ResultSet res = stmt.executeQuery("SELECT current_database()")) {
      if (!res.next()) {
        throw new SQLException("Failed to get schema information");
      }
      return res.getString(1);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTransactionIsolation()
   */

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getTypeMap()
   */

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getWarnings()
   */

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isClosed()
   */

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isReadOnly()
   */

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#isValid(int)
   */

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("timeout value was negative");
    }
    boolean rc = false;
    try {
      String productName = new HiveDatabaseMetaData(this, client, sessHandle)
              .getDatabaseProductName();
      rc = true;
    } catch (SQLException e) {
      // IGNORE
    }
    return rc;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#nativeSQL(java.lang.String)
   */

  @Override
  public String nativeSQL(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String)
   */

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
   */

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
   */

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String)
   */

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new HivePreparedStatement(this, client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int)
   */

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new HivePreparedStatement(this, client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
   */

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String,
   * java.lang.String[])
   */

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
   */

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    return new HivePreparedStatement(this, client, sessHandle, sql);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
   */

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback()
   */

  @Override
  public void rollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setAutoCommit(boolean)
   */

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // Per JDBC spec, if the connection is closed a SQLException should be thrown.
    if(isClosed) {
      throw new SQLException("Connection is closed");
    }
    // The auto-commit mode is always enabled for this connection. Per JDBC spec,
    // if setAutoCommit is called and the auto-commit mode is not changed, the call is a no-op.
    if (!autoCommit) {
      LOG.warn("Request to set autoCommit to false; Hive does not support autoCommit=false.");
      SQLWarning warning = new SQLWarning("Hive does not support autoCommit=false");
      if (warningChain == null) warningChain = warning;
      else warningChain.setNextWarning(warning);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setCatalog(java.lang.String)
   */

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // Per JDBC spec, if the driver does not support catalogs,
    // it will silently ignore this request.
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    return;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.util.Properties)
   */

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    clientInfo = properties;
    sendClientInfo();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (clientInfo == null) {
      clientInfo = new Properties();
    }
    clientInfo.put(name, value);
    sendClientInfo();
  }


  private void sendClientInfo() throws SQLClientInfoException {
    TSetClientInfoReq req = new TSetClientInfoReq(sessHandle);
    Map<String, String> map = new HashMap<>();
    if (clientInfo != null) {
      for (Entry<Object, Object> e : clientInfo.entrySet()) {
        if (e.getKey() == null || e.getValue() == null) continue;
        map.put(e.getKey().toString(), e.getValue().toString());
      }
    }
    req.setConfiguration(map);
    try {
      TSetClientInfoResp openResp = client.SetClientInfo(req);
      Utils.verifySuccess(openResp.getStatus());
    } catch (TException | SQLException e) {
      LOG.error("Error sending client info", e);
      throw new SQLClientInfoException("Error sending client info", null, e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setHoldability(int)
   */
  @Override
  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setReadOnly(boolean)
   */

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // Per JDBC spec, if the connection is closed a SQLException should be thrown.
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    // Per JDBC spec, the request defines a hint to the driver to enable database optimizations.
    // The read-only mode for this connection is disabled and cannot be enabled (isReadOnly always returns false).
    // The most correct behavior is to throw only if the request tries to enable the read-only mode.
    if(readOnly) {
      throw new SQLException("Enabling read-only mode not supported");
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint()
   */

  @Override
  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public void setSchema(String schema) throws SQLException {
    // JDK 1.7
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    if (schema == null || schema.isEmpty()) {
      throw new SQLException("Schema name is null or empty");
    }
    Statement stmt = createStatement();
    stmt.execute("use " + schema);
    stmt.close();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTransactionIsolation(int)
   */

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // TODO: throw an exception?
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setTypeMap(java.util.Map)
   */

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public TProtocolVersion getProtocol() {
    return protocol;
  }

  public static TCLIService.Iface newSynchronizedClient(
      TCLIService.Iface client) {
    return (TCLIService.Iface) Proxy.newProxyInstance(
        HiveConnection.class.getClassLoader(),
      new Class [] { TCLIService.Iface.class },
      new SynchronizedHandler(client));
  }

  private static class SynchronizedHandler implements InvocationHandler {
    private final TCLIService.Iface client;
    private final ReentrantLock lock = new ReentrantLock(true);

    SynchronizedHandler(TCLIService.Iface client) {
      this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object [] args)
        throws Throwable {
      try {
        lock.lock();
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        // all IFace APIs throw TException
        if (e.getTargetException() instanceof TException) {
          throw (TException)e.getTargetException();
        } else {
          // should not happen
          throw new TException("Error in calling method " + method.getName(),
              e.getTargetException());
        }
      } catch (Exception e) {
        throw new TException("Error in calling method " + method.getName(), e);
      } finally {
        lock.unlock();
      }
    }
  }
}
