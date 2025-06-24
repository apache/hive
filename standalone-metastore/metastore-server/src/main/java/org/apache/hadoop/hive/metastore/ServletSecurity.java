/* * Licensed to the Apache Software Foundation (ASF) under one
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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.apache.hadoop.hive.metastore.auth.jwt.JWTValidator;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Optional;

/**
 * Secures servlet processing.
 * <p>This is to be used by servlets that require impersonation through {@link UserGroupInformation#doAs(PrivilegedAction)}
 * method when providing service. The servlet request header provides user identification
 * that Hadoop{@literal '}s security uses to perform actions, the
 * {@link ServletSecurity#execute(HttpServletRequest, HttpServletResponse, ServletSecurity.MethodExecutor)}
 * method invokes the executor through a {@link PrivilegedAction} in the expected {@link UserGroupInformation} context.
 * </p>
 * A typical usage in a servlet is the following:
 * <pre><code>
 * ServletSecurity security; // ...
 * {@literal @}Override protected void doPost(HttpServletRequest request, HttpServletResponse response)
 *    throws ServletException, IOException {
 *  security.execute(request, response, this::runPost);
 * }
 * private void runPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
 * ...
 * }
 * </code></pre>
 * <p>
 * As a convenience, instead of embedding the security instance, one can wrap an existing servlet in a proxy that
 * will ensure all its service methods are called with the expected {@link UserGroupInformation} .
 * </p>
 * <pre><code>
 *  HttpServlet myServlet = ...;
 *  ServletSecurity security = ...: ;
 *  Servlet ugiServlet = security.proxy(mySerlvet);
 *  }
 *  </code></pre>
 * <p>
 * This implementation performs user extraction and eventual JWT validation to
 * execute (servlet service) methods within the context of the retrieved UserGroupInformation.
 * </p>
 */
public class ServletSecurity {
  private static final Logger LOG = LoggerFactory.getLogger(ServletSecurity.class);
  static final String X_USER = MetaStoreUtils.USER_NAME_HTTP_HEADER;
  private final boolean isSecurityEnabled;
  private final boolean jwtAuthEnabled;
  private final Configuration conf;
  private JWTValidator jwtValidator = null;

  public ServletSecurity(String authType, Configuration conf) {
    this(conf, isAuthJwt(authType));
  }

  public ServletSecurity(Configuration conf, boolean jwt) {
    this.conf = conf;
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    this.jwtAuthEnabled = jwt;
  }

  private static boolean isAuthJwt(String authType) {
    return "jwt".equalsIgnoreCase(authType);
  }

  /**
   * Should be called in Servlet.init()
   * @throws ServletException if the jwt validator creation throws an exception
   */
  public void init() throws ServletException {
    if (jwtAuthEnabled && jwtValidator == null) {
      try {
        jwtValidator = new JWTValidator(this.conf);
      } catch (Exception e) {
        throw new ServletException("Failed to initialize ServletSecurity.", e);
      }
    }
  }

  /**
   * Proxy a servlet instance service through this security executor.
   */
  public class ProxyServlet extends HttpServlet {
    private final HttpServlet delegate;

    ProxyServlet(HttpServlet delegate) {
      this.delegate = delegate;
    }

    @Override
    public void init() throws ServletException {
      ServletSecurity.this.init();
      delegate.init();
    }

    @Override
    public void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
      execute(request, response, delegate::service);
    }

    @Override
    public String getServletName() {
      try {
        return delegate.getServletName();
      } catch (IllegalStateException ill) {
        return delegate.toString();
      }
    }

    @Override
    public String getServletInfo() {
      return delegate.getServletInfo();
    }
  }

  /**
   * Creates a proxy servlet.
   * @param servlet the servlet to serve within this security context
   * @return a servlet instance or null if security initialization fails
   */
  public HttpServlet proxy(HttpServlet servlet) {
    try {
      init();
    } catch (ServletException e) {
      LOG.error("Unable to proxy security for servlet {}", servlet.toString(), e);
      return null;
    }
    return new ProxyServlet(servlet);
  }

  /**
   * Any http method executor.
   * <p>A method whose signature is similar to
   * {@link HttpServlet#doPost(HttpServletRequest, HttpServletResponse)},
   * {@link HttpServlet#doGet(HttpServletRequest, HttpServletResponse)},
   * etc.</p>
   */
  @FunctionalInterface
  public interface MethodExecutor {
    /**
     * The method to call to secure the execution of a (http) method.
     * @param request the request
     * @param response the response
     * @throws ServletException if the method executor fails
     * @throws IOException if the Json in/out fail
     */
    void execute(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException;
  }

  /**
   * The method to call to secure the execution of a (http) method.
   * @param request the request
   * @param response the response
   * @param executor the method executor
   * @throws IOException if the Json in/out fail
   */
  public void execute(HttpServletRequest request, HttpServletResponse response, MethodExecutor executor)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Logging headers in {} request", request.getMethod());
      Enumeration<String> headerNames = request.getHeaderNames();
      while (headerNames.hasMoreElements()) {
        String headerName = headerNames.nextElement();
        LOG.debug("Header: [{}], Value: [{}]", headerName,
            request.getHeader(headerName));
      }
    }
    final UserGroupInformation clientUgi;
    try {
      String userFromHeader = extractUserName(request, response);
      // Temporary, and useless for now. Here only to allow this to work on an otherwise kerberized
      // server.
      if (isSecurityEnabled || jwtAuthEnabled) {
        LOG.info("Creating proxy user for: {}", userFromHeader);
        clientUgi = UserGroupInformation.createProxyUser(userFromHeader, UserGroupInformation.getLoginUser());
      } else {
        LOG.info("Creating remote user for: {}", userFromHeader);
        clientUgi = UserGroupInformation.createRemoteUser(userFromHeader);
      }
    } catch (HttpAuthenticationException e) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.getWriter().printf("Authentication error: %s", e.getMessage());
      // Also log the error message on server side
      LOG.error("Authentication error: ", e);
      // no need to go further
      return;
    }
    final PrivilegedExceptionAction<Void> action = () -> {
      executor.execute(request, response);
      return null;
    };
    try {
      clientUgi.doAs(action);
    } catch (InterruptedException e) {
      LOG.info("Interrupted when executing http request as user: {}", clientUgi.getUserName(), e);
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      throw new IOException("Exception when executing http request as user: "+ clientUgi.getUserName(), e);
    }
  }

  private String extractUserName(HttpServletRequest request, HttpServletResponse response)
      throws HttpAuthenticationException {
    if (!jwtAuthEnabled) {
      String userFromHeader = request.getHeader(X_USER);
      if (userFromHeader == null || userFromHeader.isEmpty()) {
        throw new HttpAuthenticationException("User header " + X_USER + " missing in request");
      }
      return userFromHeader;
    }
    String signedJwt = extractBearerToken(request, response);
    if (signedJwt == null) {
      throw new HttpAuthenticationException("Couldn't find bearer token in the auth header in the request");
    }
    String user;
    try {
      user = jwtValidator.validateJWTAndExtractUser(signedJwt);
      Preconditions.checkNotNull(user, "JWT needs to contain the user name as subject");
      Preconditions.checkState(!user.isEmpty(), "User name should not be empty in JWT");
      LOG.info("Successfully validated and extracted user name {} from JWT in Auth "
          + "header in the request", user);
    } catch (Exception e) {
      throw new HttpAuthenticationException("Failed to validate JWT from Bearer token in "
          + "Authentication header", e);
    }
    return user;
  }

  /**
   * Extracts the bearer authorization header from the request. If there is no bearer
   * authorization token, returns null.
   */
  private String extractBearerToken(HttpServletRequest request,
                                    HttpServletResponse response) {
    BearerAuthExtractor extractor = new BearerAuthExtractor();
    Optional<TokenCredentials> tokenCredentials = extractor.extract(new JEEContext(
        request, response));
    return tokenCredentials.map(TokenCredentials::getToken).orElse(null);
  }

  /**
   * Login the server principal using KRB Keytab file if security is enabled.
   * @param conf the configuration
   * @throws IOException if getting the server principal fails
   */
  static void loginServerPrincipal(Configuration conf) throws IOException {
    // This check is likely pointless, especially with the current state of the http
    // servlet which respects whatever comes in. Putting this in place for the moment
    // only to enable testing on an otherwise secure cluster.
    LOG.info(" Checking if security is enabled");
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Logging in via keytab while starting HTTP metastore");
      // Handle renewal
      String kerberosName = SecurityUtil.getServerPrincipal(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
      String keyTabFile = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);
      UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
    } else {
      LOG.info("Security is not enabled. Not logging in via keytab");
    }
  }

  /**
   * Creates an SSL context factory if configuration states so.
   * @param conf the configuration
   * @return null if no ssl in config, an instance otherwise
   * @throws IOException if getting password fails
   */
  static SslContextFactory createSslContextFactory(Configuration conf) throws IOException {
    final boolean useSsl  = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_SSL);
    if (!useSsl) {
      return null;
    }
    final String keyStorePath = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PATH).trim();
    if (keyStorePath.isEmpty()) {
      throw new IllegalArgumentException(MetastoreConf.ConfVars.SSL_KEYSTORE_PATH
          + " Not configured for SSL connection");
    }
    final String keyStorePassword =
        MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD);
    final String keyStoreType =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_TYPE).trim();
    final String keyStoreAlgorithm =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYMANAGERFACTORY_ALGORITHM).trim();
    final String[] excludedProtocols =
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_PROTOCOL_BLACKLIST).split(",");
    if (LOG.isInfoEnabled()) {
      LOG.info("HTTP Server SSL: adding excluded protocols: {}", Arrays.toString(excludedProtocols));
    }
    SslContextFactory factory = new SslContextFactory.Server();
    factory.addExcludeProtocols(excludedProtocols);
    if (LOG.isInfoEnabled()) {
      LOG.info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = {}",
        Arrays.toString(factory.getExcludeProtocols()));
    }
    factory.setKeyStorePath(keyStorePath);
    factory.setKeyStorePassword(keyStorePassword);
    factory.setKeyStoreType(keyStoreType);
    factory.setKeyManagerFactoryAlgorithm(keyStoreAlgorithm);
    return factory;
  }
}
