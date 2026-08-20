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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import static javax.ws.rs.core.HttpHeaders.WWW_AUTHENTICATE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.apache.hadoop.hive.metastore.auth.jwt.SimpleJWTAuthenticator;
import org.apache.hadoop.hive.metastore.auth.oauth2.OAuth2Authenticator;
import org.apache.hadoop.hive.metastore.auth.oauth2.OAuth2AuthenticatorFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.pac4j.core.context.JEEContext;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.extractor.BearerAuthExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
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
  public enum AuthType {
    NONE, SIMPLE, JWT, OAUTH2;

    public static AuthType fromString(String type) {
      return AuthType.valueOf(type.toUpperCase());
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ServletSecurity.class);
  static final String X_USER = MetaStoreUtils.USER_NAME_HTTP_HEADER;
  private final boolean isSecurityEnabled;
  private final AuthType authType;
  private final Configuration conf;
  private final Function<HttpServletRequest, List<String>> scopeProvider;
  private SimpleJWTAuthenticator jwtAuthenticator = null;
  private OAuth2Authenticator oAuth2Authenticator = null;
  private final Cache<UgiKey, UserGroupInformation> proxyUserCache;

  /**
   * Cache key for a proxy {@link UserGroupInformation}. A proxy UGI is bound to both the effective user it
   * impersonates and the server login user acting as its real user, so both participate in identity.
   */
  record UgiKey(String effectiveUser, String loginUser) {}

  public ServletSecurity(AuthType authType, Configuration conf) {
    this(authType, conf, null);
  }

  public ServletSecurity(AuthType authType, Configuration conf,
      Function<HttpServletRequest, List<String>> scopeProvider) {
    this(authType, conf, scopeProvider, ForkJoinPool.commonPool());
  }

  @VisibleForTesting
  ServletSecurity(AuthType authType, Configuration conf,
      Function<HttpServletRequest, List<String>> scopeProvider, Executor cacheCleanupExecutor) {
    this.conf = conf;
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    this.authType = authType;
    this.scopeProvider = scopeProvider;
    this.proxyUserCache = createCacheWithConfig(
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_UGI_CACHE_EXPIRY, TimeUnit.MILLISECONDS),
        MetastoreConf.getLongVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_UGI_CACHE_SIZE),
        cacheCleanupExecutor);
  }

  /**
   * Creates a UGI cache with the specified expiration time and maximum size.
   *
   * @param expirationMs Time in milliseconds after which entries expire due to inactivity
   * @param maxSize      Maximum number of entries the cache can hold
   * @param cacheCleanupExecutor executor on which removal-listener cleanup ({@link FileSystem#closeAllForUGI})
   *                             runs; production uses {@link ForkJoinPool#commonPool()} so cleanup stays off the
   *                             request thread
   * @return A configured Caffeine cache for UGI objects
   */
  private Cache<UgiKey, UserGroupInformation> createCacheWithConfig(long expirationMs, long maxSize,
      Executor cacheCleanupExecutor) {
    // Note: eviction closes the UGI's FileSystems. If an entry is evicted while a request is still inside doAs,
    // that in-flight operation could see a "FileSystem closed" error. We don't reference-count to prevent this;
    // instead we rely on generous margins: expiry is idle-based (expireAfterAccess), and both the expiry window
    // and maximumSize should be kept well above the longest operation / peak concurrent distinct users.
    RemovalListener<UgiKey, UserGroupInformation> cleanupListener =
        (key, ugi, cause) -> {
          if (ugi != null) {
            try {
              FileSystem.closeAllForUGI(ugi);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Cleaned up FileSystem handles for evicted UGI: {} (cause: {})",
                    ugi.getUserName(), cause);
              }
            } catch (IOException cleanupException) {
              LOG.error("Failed to clean up FileSystem handles for evicted UGI: {} (cause: {})",
                  ugi, cause, cleanupException);
            }
          }
        };

    Caffeine<UgiKey, UserGroupInformation> builder = Caffeine.<UgiKey, UserGroupInformation>newBuilder()
        .maximumSize(maxSize)
        .executor(cacheCleanupExecutor)
        .removalListener(cleanupListener);

    if (expirationMs > 0) {
      builder.expireAfterAccess(Duration.ofMillis(expirationMs))
          .ticker(Ticker.systemTicker());
    }

    return builder.build();
  }

  /**
   * Returns the (cached) proxy {@link UserGroupInformation} for the given user, creating it on a cache miss.
   * <p>Caching the proxy UGI prevents Hadoop{@literal '}s {@code FileSystem.CACHE} from accumulating a distinct
   * entry (and its RPC/IPC resources) for every request; evicted entries are cleaned up through
   * {@link FileSystem#closeAllForUGI(UserGroupInformation)}.</p>
   * @param userName the effective user name extracted from the request
   * @param loginUser the server login user that acts as the real user of the proxy
   * @return the cached or freshly created proxy user
   */
  @VisibleForTesting
  UserGroupInformation getProxyUser(String userName, UserGroupInformation loginUser) {
    return proxyUserCache.get(new UgiKey(userName, loginUser.getUserName()), key -> {
      LOG.debug("Creating proxy user for: {}", key.effectiveUser());
      return UserGroupInformation.createProxyUser(key.effectiveUser(), loginUser);
    });
  }

  /**
   * Forces the proxy user cache to run any pending maintenance (eviction and cleanup).
   */
  @VisibleForTesting
  void cleanUpProxyUserCache() {
    proxyUserCache.cleanUp();
  }

  /**
   * @return the approximate number of entries currently held in the proxy user cache
   */
  @VisibleForTesting
  long proxyUserCacheSize() {
    proxyUserCache.cleanUp();
    return proxyUserCache.estimatedSize();
  }

  /**
   * Should be called in Servlet.init()
   * @throws ServletException if the jwt validator creation throws an exception
   */
  public void init() throws ServletException {
    if (authType == AuthType.JWT && jwtAuthenticator == null) {
      try {
        jwtAuthenticator = SimpleJWTAuthenticator.create(this.conf);
      } catch (Exception e) {
        throw new ServletException("Failed to initialize ServletSecurity.", e);
      }
    } else if (authType == AuthType.OAUTH2 && oAuth2Authenticator == null) {
      try {
        oAuth2Authenticator = OAuth2AuthenticatorFactory.createAuthenticator(this.conf);
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
    if (authType == AuthType.NONE) {
      LOG.warn("Servlet security is disabled for {}", servlet);
      return servlet;
    }
    try {
      init();
    } catch (ServletException e) {
      LOG.error("Unable to proxy security for servlet {}", servlet, e);
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
      if (isSecurityEnabled || authType == AuthType.JWT || authType == AuthType.OAUTH2) {
        clientUgi = getProxyUser(userFromHeader, UserGroupInformation.getLoginUser());
      } else {
        // Unreachable in the case of NONE
        Preconditions.checkState(authType == AuthType.SIMPLE);
        LOG.info("Creating remote user for: {}", userFromHeader);
        clientUgi = UserGroupInformation.createRemoteUser(userFromHeader);
      }
    } catch (HttpAuthenticationException e) {
      response.setStatus(e.getStatusCode());
      e.getWwwAuthenticateHeader().ifPresent(value -> response.setHeader(WWW_AUTHENTICATE, value));
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
    switch (authType) {
    case NONE:
      throw new IllegalArgumentException("This method should not be called when auth type is NONE");
    case SIMPLE:
      String userFromHeader = request.getHeader(X_USER);
      if (userFromHeader == null || userFromHeader.isEmpty()) {
        throw new HttpAuthenticationException("User header " + X_USER + " missing in request");
      }
      return userFromHeader;
    case JWT:
      String signedJwt = extractBearerToken(request, response);
      if (signedJwt == null) {
        throw new HttpAuthenticationException("Couldn't find bearer token in the auth header in the request");
      }
      String user;
      try {
        user = jwtAuthenticator.resolveUserName(signedJwt);
        Preconditions.checkNotNull(user, "JWT needs to contain the user name as subject");
        Preconditions.checkState(!user.isEmpty(), "User name should not be empty in JWT");
        LOG.info("Successfully validated and extracted user name {} from JWT in Auth "
            + "header in the request", user);
      } catch (Exception e) {
        throw new HttpAuthenticationException("Failed to validate JWT from Bearer token in "
            + "Authentication header", e);
      }
      return user;
    case OAUTH2:
      String accessToken = extractBearerToken(request, response);
      return oAuth2Authenticator.resolveUserName(accessToken, scopeProvider.apply(request));
    default:
      throw new IllegalArgumentException("Unknown auth type: " + authType);
    }
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
   * Creates an SSL context factory if the configuration states so.
   * @param conf the configuration
   * @return null if no ssl in config, an instance otherwise
   * @throws IOException if getting password fails
   */
  static SslContextFactory createSslContextFactory(Configuration conf) throws IOException {
    final boolean useSsl = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_SSL);
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
    String[] includeProtocols = SecurityUtils.parseIncludeProtocols(
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_INCLUDE_PROTOCOLS));
    String[] includeCipherSuites = SecurityUtils.parseIncludeCipherSuites(
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_INCLUDE_CIPHERSUITES));
    SslContextFactory factory = new SslContextFactory.Server();
    if (includeProtocols.length > 0) {
      factory.setIncludeProtocols(includeProtocols);
    }
    factory.addExcludeProtocols(excludedProtocols);
    if (LOG.isInfoEnabled()) {
      LOG.info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = {}",
        Arrays.toString(factory.getExcludeProtocols()));
    }
    factory.setKeyStorePath(keyStorePath);
    factory.setKeyStorePassword(keyStorePassword);
    factory.setKeyStoreType(keyStoreType.isEmpty() ? KeyStore.getDefaultType() : keyStoreType);
    factory.setKeyManagerFactoryAlgorithm(keyStoreAlgorithm.isEmpty() ?
        KeyManagerFactory.getDefaultAlgorithm() : keyStoreAlgorithm);
    if (includeCipherSuites.length > 0) {
      factory.setIncludeCipherSuites(includeCipherSuites);
    }
    return factory;
  }
}
