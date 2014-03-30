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

package org.apache.hive.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TCancelDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.cli.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * HiveConnection.
 *
 */
public class HiveConnection implements java.sql.Connection {
  public static final Log LOG = LogFactory.getLog(HiveConnection.class.getName());
  private static final String HIVE_AUTH_TYPE= "auth";
  private static final String HIVE_AUTH_QOP = "sasl.qop";
  private static final String HIVE_AUTH_SIMPLE = "noSasl";
  private static final String HIVE_AUTH_TOKEN = "delegationToken";
  private static final String HIVE_AUTH_USER = "user";
  private static final String HIVE_AUTH_PRINCIPAL = "principal";
  private static final String HIVE_AUTH_PASSWD = "password";
  private static final String HIVE_AUTH_KERBEROS_AUTH_TYPE = "kerberosAuthType";
  private static final String HIVE_AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT = "fromSubject";
  private static final String HIVE_ANONYMOUS_USER = "anonymous";
  private static final String HIVE_ANONYMOUS_PASSWD = "anonymous";
  private static final String HIVE_USE_SSL = "ssl";
  private static final String HIVE_SSL_TRUST_STORE = "sslTrustStore";
  private static final String HIVE_SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
  private static final String HIVE_SERVER2_TRANSPORT_MODE = "hive.server2.transport.mode";
  private static final String HIVE_SERVER2_THRIFT_HTTP_PATH = "hive.server2.thrift.http.path";
  private static final String HIVE_VAR_PREFIX = "hivevar:";
  private static final String HIVE_CONF_PREFIX = "hiveconf:";
  // Currently supports JKS keystore format
  // See HIVE-6286 (Add support for PKCS12 keystore format)
  private static final String HIVE_SSL_TRUST_STORE_TYPE = "JKS";

  private final String jdbcURI;
  private final String host;
  private final int port;
  private final Map<String, String> sessConfMap;
  private final Map<String, String> hiveConfMap;
  private final Map<String, String> hiveVarMap;
  private final boolean isEmbeddedMode;
  private TTransport transport;
  private TCLIService.Iface client;   // todo should be replaced by CliServiceClient
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TSessionHandle sessHandle = null;
  private final List<TProtocolVersion> supportedProtocols = new LinkedList<TProtocolVersion>();
  private int loginTimeout = 0;
  private TProtocolVersion protocol;

  public HiveConnection(String uri, Properties info) throws SQLException {
    setupLoginTimeout();
    jdbcURI = uri;
    // parse the connection uri
    Utils.JdbcConnectionParams connParams;
    try {
      connParams = Utils.parseURL(uri);
    } catch (IllegalArgumentException e) {
      throw new SQLException(e);
    }
    // extract parsed connection parameters:
    // JDBC URL: jdbc:hive2://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list
    // each list: <key1>=<val1>;<key2>=<val2> and so on
    // sess_var_list -> sessConfMap
    // hive_conf_list -> hiveConfMap
    // hive_var_list -> hiveVarMap
    host = connParams.getHost();
    port = connParams.getPort();
    sessConfMap = connParams.getSessionVars();
    hiveConfMap = connParams.getHiveConfs();

    hiveVarMap = connParams.getHiveVars();
    for (Map.Entry<Object, Object> kv : info.entrySet()) {
      if ((kv.getKey() instanceof String)) {
        String key = (String) kv.getKey();
        if (key.startsWith(HIVE_VAR_PREFIX)) {
          hiveVarMap.put(key.substring(HIVE_VAR_PREFIX.length()), info.getProperty(key));
        } else if (key.startsWith(HIVE_CONF_PREFIX)) {
          hiveConfMap.put(key.substring(HIVE_CONF_PREFIX.length()), info.getProperty(key));
        }
      }
    }

    isEmbeddedMode = connParams.isEmbeddedMode();

    if (isEmbeddedMode) {
      client = new EmbeddedThriftBinaryCLIService();
    } else {
      // extract user/password from JDBC connection properties if its not supplied in the
      // connection URL
      if (info.containsKey(HIVE_AUTH_USER)) {
        sessConfMap.put(HIVE_AUTH_USER, info.getProperty(HIVE_AUTH_USER));
        if (info.containsKey(HIVE_AUTH_PASSWD)) {
          sessConfMap.put(HIVE_AUTH_PASSWD, info.getProperty(HIVE_AUTH_PASSWD));
        }
      }
      if (info.containsKey(HIVE_AUTH_TYPE)) {
        sessConfMap.put(HIVE_AUTH_TYPE, info.getProperty(HIVE_AUTH_TYPE));
      }
      // open the client transport
      openTransport();
      // set up the client
      client = new TCLIService.Client(new TBinaryProtocol(transport));
    }

    // add supported protocols
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
    supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7);

    // open client session
    openSession(connParams.getSessionVars());

    configureConnection(connParams.getDbName());
  }

  private void openTransport() throws SQLException {
    // TODO: Refactor transport creation to a factory, it's getting uber messy here
    transport = isHttpTransportMode() ? createHttpTransport() : createBinaryTransport();
    try {
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new SQLException("Could not open connection to "
          + jdbcURI + ": " + e.getMessage(), " 08S01", e);
    }
  }

  private String getServerHttpUrl(boolean useSsl) {
    // Create the http/https url
    // JDBC driver will set up an https url if ssl is enabled, otherwise http
    String schemeName = useSsl ? "https" : "http";
    // http path should begin with "/"
    String httpPath;
    httpPath = hiveConfMap.get(HIVE_SERVER2_THRIFT_HTTP_PATH);
    if(httpPath == null) {
      httpPath = "/";
    }
    else if(!httpPath.startsWith("/")) {
      httpPath = "/" + httpPath;
    }
    return schemeName +  "://" + host + ":" + port + httpPath;
  }

  private TTransport createHttpTransport() throws SQLException {
    DefaultHttpClient httpClient;

    boolean useSsl = isSslConnection();

    // Create an http client from the configs
    try {
      httpClient = getHttpClient(useSsl);
    } catch (Exception e) {
      String msg =  "Could not create http connection to " +
          jdbcURI + ". " + e.getMessage();
      throw new SQLException(msg, " 08S01", e);
    }

    try {
      transport = new THttpClient(getServerHttpUrl(useSsl), httpClient);
    }
    catch (TTransportException e) {
      String msg =  "Could not create http connection to " +
          jdbcURI + ". " + e.getMessage();
      throw new SQLException(msg, " 08S01", e);
    }
    return transport;
  }

  private DefaultHttpClient getHttpClient(Boolean useSsl) throws SQLException {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    // Request interceptor for any request pre-processing logic
    HttpRequestInterceptor requestInterceptor;
    // If Kerberos
    if (isKerberosAuthMode()) {
      if (useSsl) {
        String msg = "SSL encryption is currently not supported with " +
            "kerberos authentication";
        throw new SQLException(msg, " 08S01");
      }
      /**
       * Add an interceptor which sets the appropriate header in the request.
       * It does the kerberos authentication and get the final service ticket,
       * for sending to the server before every request.
       */
      requestInterceptor = new HttpKerberosRequestInterceptor(
          sessConfMap.get(HIVE_AUTH_PRINCIPAL), host, getServerHttpUrl(false));
    }
    else {
      /**
       * Add an interceptor to pass username/password in the header.
       * In https mode, the entire information is encrypted
       */
      requestInterceptor = new HttpBasicAuthInterceptor(getUserName(), getPassword());
      // Configure httpClient for SSL
      if (useSsl) {
        String sslTrustStorePath = sessConfMap.get(HIVE_SSL_TRUST_STORE);
        String sslTrustStorePassword = sessConfMap.get(
            HIVE_SSL_TRUST_STORE_PASSWORD);
        KeyStore sslTrustStore;
        SSLSocketFactory socketFactory;
        try {
          if (sslTrustStorePath == null || sslTrustStorePath.isEmpty()) {
            // Create a default socket factory based on standard JSSE trust material
            socketFactory = SSLSocketFactory.getSocketFactory();
          }
          else {
            // Pick trust store config from the given path
            sslTrustStore = KeyStore.getInstance(HIVE_SSL_TRUST_STORE_TYPE);
            sslTrustStore.load(new FileInputStream(sslTrustStorePath),
                sslTrustStorePassword.toCharArray());
            socketFactory = new SSLSocketFactory(sslTrustStore);
          }
          socketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
          Scheme sslScheme = new Scheme("https", 443, socketFactory);
          httpClient.getConnectionManager().getSchemeRegistry().register(sslScheme);
        }
        catch (Exception e) {
          String msg =  "Could not create an https connection to " +
              jdbcURI + ". " + e.getMessage();
          throw new SQLException(msg, " 08S01", e);
        }
      }
    }
    httpClient.addRequestInterceptor(requestInterceptor);
    return httpClient;
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
   */
  private TTransport createBinaryTransport() throws SQLException {
    try {
      // handle secure connection if specified
      if (!HIVE_AUTH_SIMPLE.equals(sessConfMap.get(HIVE_AUTH_TYPE))) {
        // If Kerberos
        Map<String, String> saslProps = new HashMap<String, String>();
        SaslQOP saslQOP = SaslQOP.AUTH;
        if (sessConfMap.containsKey(HIVE_AUTH_PRINCIPAL)) {
          if (sessConfMap.containsKey(HIVE_AUTH_QOP)) {
            try {
              saslQOP = SaslQOP.fromString(sessConfMap.get(HIVE_AUTH_QOP));
            } catch (IllegalArgumentException e) {
              throw new SQLException("Invalid " + HIVE_AUTH_QOP +
                  " parameter. " + e.getMessage(), "42000", e);
            }
          }
          saslProps.put(Sasl.QOP, saslQOP.toString());
          saslProps.put(Sasl.SERVER_AUTH, "true");
          boolean assumeSubject = HIVE_AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT.equals(sessConfMap.get(HIVE_AUTH_KERBEROS_AUTH_TYPE));
          transport = KerberosSaslHelper.getKerberosTransport(
              sessConfMap.get(HIVE_AUTH_PRINCIPAL), host,
              HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps, assumeSubject);
        } else {
          // If there's a delegation token available then use token based connection
          String tokenStr = getClientDelegationToken(sessConfMap);
          if (tokenStr != null) {
            transport = KerberosSaslHelper.getTokenTransport(tokenStr,
                host, HiveAuthFactory.getSocketTransport(host, port, loginTimeout), saslProps);
          } else {
            // we are using PLAIN Sasl connection with user/password
            String userName = getUserName();
            String passwd = getPassword();
            if (isSslConnection()) {
              // get SSL socket
              String sslTrustStore = sessConfMap.get(HIVE_SSL_TRUST_STORE);
              String sslTrustStorePassword = sessConfMap.get(HIVE_SSL_TRUST_STORE_PASSWORD);
              if (sslTrustStore == null || sslTrustStore.isEmpty()) {
                transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout);
              } else {
                transport = HiveAuthFactory.getSSLSocket(host, port, loginTimeout,
                    sslTrustStore, sslTrustStorePassword);
              }
            } else {
              // get non-SSL socket transport
              transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout);
            }
            // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
            transport = PlainSaslHelper.getPlainTransport(userName, passwd, transport);
          }
        }
      } else {
        // Raw socket connection (non-sasl)
        transport = HiveAuthFactory.getSocketTransport(host, port, loginTimeout);
      }
    } catch (SaslException e) {
      throw new SQLException("Could not create secure connection to "
          + jdbcURI + ": " + e.getMessage(), " 08S01", e);
    } catch (TTransportException e) {
      throw new SQLException("Could not create connection to "
          + jdbcURI + ": " + e.getMessage(), " 08S01", e);
    }
    return transport;
  }

  // Lookup the delegation token. First in the connection URL, then Configuration
  private String getClientDelegationToken(Map<String, String> jdbcConnConf)
      throws SQLException {
    String tokenStr = null;
    if (HIVE_AUTH_TOKEN.equalsIgnoreCase(jdbcConnConf.get(HIVE_AUTH_TYPE))) {
      // check delegation token in job conf if any
      try {
        tokenStr = ShimLoader.getHadoopShims().
            getTokenStrForm(HiveAuthFactory.HS2_CLIENT_TOKEN);
      } catch (IOException e) {
        throw new SQLException("Error reading token ", e);
      }
    }
    return tokenStr;
  }

  private void openSession(Map<String, String> sessVars) throws SQLException {
    TOpenSessionReq openReq = new TOpenSessionReq();

    // set the session configuration
    if (sessVars.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      Map<String, String> openConf = new HashMap<String, String>();
      openConf.put(HiveAuthFactory.HS2_PROXY_USER,
          sessVars.get(HiveAuthFactory.HS2_PROXY_USER));
      openReq.setConfiguration(openConf);
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
    } catch (TException e) {
      LOG.error("Error opening session", e);
      throw new SQLException("Could not establish connection to "
          + jdbcURI + ": " + e.getMessage(), " 08S01", e);
    }
    isClosed = false;
  }

  private void configureConnection(String dbName) throws SQLException {
    // set the hive variable in session state for local mode
    if (isEmbeddedMode) {
      if (!hiveVarMap.isEmpty()) {
        SessionState.get().setHiveVariables(hiveVarMap);
      }
    } else {
      // for remote JDBC client, try to set the conf var using 'set foo=bar'
      Statement stmt = createStatement();
      for (Entry<String, String> hiveConf : hiveConfMap.entrySet()) {
        stmt.execute("set " + hiveConf.getKey() + "=" + hiveConf.getValue());
      }

      // For remote JDBC client, try to set the hive var using 'set hivevar:key=value'
      for (Entry<String, String> hiveVar : hiveVarMap.entrySet()) {
        stmt.execute("set hivevar:" + hiveVar.getKey() + "=" + hiveVar.getValue());
      }
      // if the client is setting a non-default db, then switch the database
      if (!Utils.DEFAULT_DATABASE.equalsIgnoreCase(dbName)) {
        stmt.execute("use " + dbName);
      }
      stmt.close();
    }
  }

  /**
   * @return username from sessConfMap
   */
  private String getUserName() {
    return getSessionValue(HIVE_AUTH_USER, HIVE_ANONYMOUS_USER);
  }

  /**
   * @return password from sessConfMap
   */
  private String getPassword() {
    return getSessionValue(HIVE_AUTH_PASSWD, HIVE_ANONYMOUS_PASSWD);
  }

  private boolean isSslConnection() {
    return "true".equalsIgnoreCase(sessConfMap.get(HIVE_USE_SSL));
  }

  private boolean isKerberosAuthMode() {
    return !HIVE_AUTH_SIMPLE.equals(sessConfMap.get(HIVE_AUTH_TYPE))
        && sessConfMap.containsKey(HIVE_AUTH_PRINCIPAL);
  }

  private boolean isHttpTransportMode() {
    String transportMode = hiveConfMap.get(HIVE_SERVER2_TRANSPORT_MODE);
    if(transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createBlob()
   */

  @Override
  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createClob()
   */

  @Override
  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createNClob()
   */

  @Override
  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#createSQLXML()
   */

  @Override
  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
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
    return new HiveStatement(this, client, sessHandle);
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
        resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE);
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getClientInfo(java.lang.String)
   */

  @Override
  public String getClientInfo(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#getHoldability()
   */

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
  }

  public String getSchema() throws SQLException {
    if (isClosed) {
      throw new SQLException("Connection is closed");
    }
    Statement stmt = createStatement();
    ResultSet res = stmt.executeQuery("SELECT current_database()");
    if (!res.next()) {
      throw new SQLException("Failed to get schema information");
    }
    String schemaName = res.getString(1);
    res.close();
    stmt.close();
    return schemaName;
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
    throw new SQLException("Method not supported");
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
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#nativeSQL(java.lang.String)
   */

  @Override
  public String nativeSQL(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#prepareCall(java.lang.String)
   */

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
   */

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback()
   */

  @Override
  public void rollback() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#rollback(java.sql.Savepoint)
   */

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setAutoCommit(boolean)
   */

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLException("enabling autocommit is not supported");
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
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
   */

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    // TODO Auto-generated method stub
    throw new SQLClientInfoException("Method not supported", null);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setHoldability(int)
   */

  @Override
  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setReadOnly(boolean)
   */

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint()
   */

  @Override
  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Connection#setSavepoint(java.lang.String)
   */

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
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
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  public TProtocolVersion getProtocol() {
    return protocol;
  }
}
