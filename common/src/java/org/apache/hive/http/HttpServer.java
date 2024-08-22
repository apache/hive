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

package org.apache.hive.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.KeyManagerFactory;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.http.security.PamConstraint;
import org.apache.hive.http.security.PamConstraintMapping;
import org.apache.hive.http.security.PamLoginService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.FileManager;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler.Context;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

/**
 * A simple embedded Jetty server to serve as HS2/HMS web UI.
 */
public class HttpServer {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HttpServer.class);

  public static final String CONF_CONTEXT_ATTRIBUTE = "hive.conf";
  public static final String ADMINS_ACL = "admins.acl";
  private XFrameOption xFrameOption;
  private boolean xFrameOptionIsEnabled;
  private boolean isSSLEnabled;
  public static final String HTTP_HEADER_PREFIX = "hadoop.http.header.";
  private static final String X_FRAME_OPTIONS = "X-FRAME-OPTIONS";
  static final String X_XSS_PROTECTION  =
          "X-XSS-Protection:1; mode=block";
  static final String X_CONTENT_TYPE_OPTIONS =
          "X-Content-Type-Options:nosniff";
  static final String STRICT_TRANSPORT_SECURITY =
          "Strict-Transport-Security:max-age=31536000; includeSubDomains";
  private static final String HTTP_HEADER_REGEX =
          "hadoop\\.http\\.header\\.([a-zA-Z\\-_]+)";
  private static final Pattern PATTERN_HTTP_HEADER_REGEX =
          Pattern.compile(HTTP_HEADER_REGEX);



  private final String name;
  private WebAppContext rootWebAppContext;
  private Server webServer;
  private QueuedThreadPool threadPool;
  private PortHandlerWrapper portHandlerWrapper;

  /**
   * Create a status server on the given port.
   */
  private HttpServer(final Builder b) throws IOException {
    this.name = b.name;
    this.xFrameOptionIsEnabled = b.xFrameEnabled;
    this.isSSLEnabled = b.useSSL;
    this.xFrameOption = b.xFrameOption;
    createWebServer(b);
  }

  public static class Builder {
    private final String name;
    private String host;
    private int port;
    private int maxThreads;
    private HiveConf conf;
    private final Map<String, Object> contextAttrs = new HashMap<String, Object>();
    private String keyStorePassword;
    private String keyStorePath;
    private String keyStoreType;
    private String keyManagerFactoryAlgorithm;
    private String excludeCiphersuites;
    private String spnegoPrincipal;
    private String spnegoKeytab;
    private boolean useSPNEGO;
    private boolean useSSL;
    private boolean usePAM;
    private boolean enableCORS;
    private String allowedOrigins;
    private String allowedMethods;
    private String allowedHeaders;
    private PamAuthenticator pamAuthenticator;
    private String contextRootRewriteTarget = "/index.html";
    private boolean xFrameEnabled;
    private XFrameOption xFrameOption = XFrameOption.SAMEORIGIN;
    private final List<Pair<String, Class<? extends HttpServlet>>> servlets =
        new LinkedList<Pair<String, Class<? extends HttpServlet>>>();
    private boolean disableDirListing = false;
    private final Map<String, Pair<String, Filter>> globalFilters = new LinkedHashMap<>();
    private String contextPath = "/";

    public Builder(String name) {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "Name must be specified");
      this.name = name;
    }

    public HttpServer build() throws IOException {
      return new HttpServer(this);
    }

    public void setContextPath(String contextPath) {
      this.contextPath = contextPath;
    }

    public Builder setConf(HiveConf origConf) {
      this.conf = new HiveConf(origConf);
      origConf.stripHiddenConfigurations(conf);
      setContextAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
      return this;
    }


    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setMaxThreads(int maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }

    public Builder setAdmins(String admins) {
      if (admins != null) {
        setContextAttribute(ADMINS_ACL, new AccessControlList(admins));
      }
      return this;
    }

    public Builder setKeyStorePassword(String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    public Builder setKeyStorePath(String keyStorePath) {
      this.keyStorePath = keyStorePath;
      return this;
    }

    public Builder setKeyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return this;
    }

    public Builder setKeyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm) {
      this.keyManagerFactoryAlgorithm = keyManagerFactoryAlgorithm;
      return this;
    }

    public Builder setExcludeCiphersuites(String excludeCiphersuites) {
      this.excludeCiphersuites = excludeCiphersuites;
      return this;
    }

    public Builder setUseSSL(boolean useSSL) {
      this.useSSL = useSSL;
      return this;
    }

    public Builder setUsePAM(boolean usePAM) {
      this.usePAM = usePAM;
      return this;
    }

    public Builder setPAMAuthenticator(PamAuthenticator pamAuthenticator){
      this.pamAuthenticator = pamAuthenticator;
      return this;
    }

    public Builder setUseSPNEGO(boolean useSPNEGO) {
      this.useSPNEGO = useSPNEGO;
      return this;
    }

    public Builder setEnableCORS(boolean enableCORS) {
      this.enableCORS = enableCORS;
      return this;
    }

    public Builder setAllowedOrigins(String allowedOrigins) {
      this.allowedOrigins = allowedOrigins;
      return this;
    }

    public Builder setAllowedMethods(String allowedMethods) {
      this.allowedMethods = allowedMethods;
      return this;
    }

    public Builder setAllowedHeaders(String allowedHeaders) {
      this.allowedHeaders = allowedHeaders;
      return this;
    }

    public Builder setSPNEGOPrincipal(String principal) {
      this.spnegoPrincipal = principal;
      return this;
    }

    public Builder setSPNEGOKeytab(String keytab) {
      this.spnegoKeytab = keytab;
      return this;
    }

    public Builder setContextAttribute(String name, Object value) {
      contextAttrs.put(name, value);
      return this;
    }

    public Builder setContextRootRewriteTarget(String contextRootRewriteTarget) {
      this.contextRootRewriteTarget = contextRootRewriteTarget;
      return this;
    }

    public Builder addServlet(String endpoint, Class<? extends HttpServlet> servlet) {
      servlets.add(new Pair<String, Class<? extends HttpServlet>>(endpoint, servlet));
      return this;
    }

    public Builder addGlobalFilter(String name, String pathSpec, Filter filter) {
      globalFilters.put(name, Pair.create(pathSpec, filter));
      return this;
    }

    /**
     * Adds the ability to control X_FRAME_OPTIONS on HttpServer2.
     * @param xFrameEnabled - True enables X_FRAME_OPTIONS false disables it.
     * @return Builder.
     */
    public Builder configureXFrame(boolean xFrameEnabled) {
      this.xFrameEnabled = xFrameEnabled;
      return this;
    }

    /**
     * Sets a valid X-Frame-option that can be used by HttpServer2.
     * @param option - String DENY, SAMEORIGIN or ALLOW-FROM are the only valid
     *               options. Any other value will throw IllegalArgument
     *               Exception.
     * @return  Builder.
     */
    public Builder setXFrameOption(String option) {
      this.xFrameOption = XFrameOption.getEnum(option);
      return this;
    }

    public void setDisableDirListing(boolean disableDirListing) {
      this.disableDirListing = disableDirListing;
    }
  }

  public void start() throws Exception {
    webServer.start();
    LOG.info("Started HttpServer[{}] on port {}", name, getPort());
  }

  public void stop() throws Exception {
    webServer.stop();
  }

  public int getPort() {
    return ((ServerConnector)(webServer.getConnectors()[0])).getLocalPort();
  }

  /**
   * Checks the user has privileges to access to instrumentation servlets.
   * <p>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
   * (default value) it always returns TRUE.
   * </p>
   * <p>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
   * it will check if the current user is in the admin ACLS. If the user is
   * in the admin ACLs it returns TRUE, otherwise it returns FALSE.
   * </p>
   *
   * @param servletContext the servlet context.
   * @param request the servlet request.
   * @param response the servlet response.
   * @return TRUE/FALSE based on the logic described above.
   */
  @InterfaceAudience.LimitedPrivate("hive")
  public static boolean isInstrumentationAccessAllowed(
          ServletContext servletContext, HttpServletRequest request,
          HttpServletResponse response) throws IOException {
    Configuration conf =
      (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    boolean access = true;
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
      false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    }
    return access;
  }

  /**
   * Same as {@link HttpServer#isInstrumentationAccessAllowed(ServletContext, HttpServletRequest, HttpServletResponse)}
   * except that it returns true only if <code>hadoop.security.instrumentation.requires.admin</code> is set to true.
   */
  @InterfaceAudience.LimitedPrivate("hive")
  public static boolean isInstrumentationAccessAllowedStrict(
    ServletContext servletContext, HttpServletRequest request,
    HttpServletResponse response) throws IOException {
    Configuration conf =
      (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    boolean access;
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    } else {
      return false;
    }
    return access;
  }

  /**
   * Check if the remote user has access to an object (e.g. query history) that belongs to a user
   *
   * @param ctx the context containing the admin ACL.
   * @param request the HTTP request.
   * @param remoteUser the user that sent out the request.
   * @param user the user of the object being checked against.
   * @return true if the remote user is the same as the user or has the admin access
   * @throws IOException
   */
  public static boolean hasAccess(String remoteUser, String user,
      ServletContext ctx, HttpServletRequest request) throws IOException {
    return StringUtils.equalsIgnoreCase(remoteUser, user) ||
        HttpServer.hasAdministratorAccess(ctx, request, null);
  }

  /**
   * Does the user sending the HttpServletRequest have the administrator ACLs? If
   * it isn't the case, response will be modified to send an error to the user.
   *
   * @param servletContext
   * @param request
   * @param response used to send the error response if user does not have admin access (no error if null)
   * @return true if admin-authorized, false otherwise
   * @throws IOException
   */
  static boolean hasAdministratorAccess(
      ServletContext servletContext, HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Configuration conf =
        (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    // If there is no authorization, anybody has administrator access.
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      return true;
    }
    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      if (response != null) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                           """
                           Unauthenticated users are not \
                           authorized to access this page.\
                           """);
      }
      return false;
    }

    if (servletContext.getAttribute(ADMINS_ACL) != null &&
        !userHasAdministratorAccess(servletContext, remoteUser)) {
      if (response != null) {
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "User "
            + remoteUser + " is unauthorized to access this page.");
      }
      return false;
    }

    return true;
  }

  /**
   * Get the admin ACLs from the given ServletContext and check if the given
   * user is in the ACL.
   *
   * @param servletContext the context containing the admin ACL.
   * @param remoteUser the remote user to check for.
   * @return true if the user is present in the ACL, false if no ACL is set or
   *         the user is not present
   */
  static boolean userHasAdministratorAccess(ServletContext servletContext,
      String remoteUser) {
    AccessControlList adminsAcl = (AccessControlList) servletContext
        .getAttribute(ADMINS_ACL);
    UserGroupInformation remoteUserUGI =
        UserGroupInformation.createRemoteUser(remoteUser);
    return adminsAcl != null && adminsAcl.isUserAllowed(remoteUserUGI);
  }

  /**
   * Create the web context for the application of specified name
   */
  WebAppContext createWebAppContext(Builder b) throws FileNotFoundException {
    WebAppContext ctx = new WebAppContext();
    setContextAttributes(ctx.getServletContext(), b.contextAttrs);
    ctx.getServletContext().getSessionCookieConfig().setHttpOnly(true);
    ctx.setDisplayName(b.name);
    ctx.setContextPath(b.contextPath);
    ctx.setWar(getWebAppsPath(b.name) + "/" + b.name);
    return ctx;
  }

  /**
   * Secure the web server with kerberos (AuthenticationFilter).
   */
  void setupSpnegoFilter(Builder b, ServletContextHandler ctx) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("kerberos.principal",
      SecurityUtil.getServerPrincipal(b.spnegoPrincipal, b.host));
    params.put("kerberos.keytab", b.spnegoKeytab);
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
    FilterHolder holder = new FilterHolder();
    holder.setClassName(AuthenticationFilter.class.getName());
    holder.setInitParameters(params);
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilterWithMapping(
      holder, "/*", FilterMapping.ALL);
  }

  /**
   * Setup cross-origin requests (CORS) filter.
   * @param b - builder
   * @param webAppContext - webAppContext
   */
  private void setupCORSFilter(Builder b, WebAppContext webAppContext) {
    FilterHolder holder = new FilterHolder();
    holder.setClassName(CrossOriginFilter.class.getName());
    Map<String, String> params = new HashMap<>();
    params.put(CrossOriginFilter.ALLOWED_ORIGINS, b.allowedOrigins);
    params.put(CrossOriginFilter.ALLOWED_METHODS, b.allowedMethods);
    params.put(CrossOriginFilter.ALLOWED_HEADERS, b.allowedHeaders);
    holder.setInitParameters(params);

    ServletHandler handler = webAppContext.getServletHandler();
    handler.addFilterWithMapping(holder, "/*", FilterMapping.ALL);
  }

  /**
   * Creates a port connector and initializes a web application that processes requests through the newly 
   * created port connector.
   * 
   * @param builder - The builder object used to configure and create the port connector and web application.
   * @return ContextHandlerCollection - A collection of request handlers associated with the new port connector,
   *         which includes the newly initialized web application.
   */
  public ContextHandlerCollection createAndAddWebApp(Builder builder) throws IOException {
    WebAppContext webAppContext = createWebAppContext(builder);
    initWebAppContext(builder, webAppContext);
    RewriteHandler rwHandler = createRewriteHandler(builder, webAppContext);

    ContextHandlerCollection portHandler = new ContextHandlerCollection();
    portHandler.addHandler(rwHandler);

    for (Pair<String, Class<? extends HttpServlet>> p : builder.servlets) {
      addServlet(p.getKey(), "/" + p.getKey(), p.getValue(), webAppContext);
    }

    builder.globalFilters.forEach((k, v) -> 
        addFilter(k, v.getKey(), v.getValue(), webAppContext.getServletHandler()));

    // Associate the port handler with the a new connector and add it to the server
    ServerConnector connector = createAndAddChannelConnector(threadPool.getQueueSize(), builder);
    portHandlerWrapper.addHandler(connector, portHandler);

    if (builder.contextPath.equals("/")) {
      rootWebAppContext = webAppContext;
    }

    return portHandler;
  }

  /**
   * Initializes the {@link WebAppContext} based on the provided configuration in the {@link Builder}.
   * The method sets up various filters and configurations for the web application context, including
   * security and cross-origin resource sharing (CORS) settings, as well as header management.
   *
   * <p>The method performs the following actions based on the {@code builder} configuration:</p>
   * <ul>
   *   <li>If {@code builder.useSPNEGO} is {@code true}, sets up the SPNEGO filter for Kerberos authentication.</li>
   *   <li>If {@code builder.enableCORS} is {@code true}, sets up the CORS filter.</li>
   *   <li>If {@code builder.xFrameEnabled} is {@code true}, configures the X-Frame-Options header filter.</li>
   *   <li>If {@code builder.disableDirListing} is {@code true}, disables directory listing on the servlet.</li>
   * </ul>
   *
   * @param builder The {@link Builder} object containing configuration options to customize the web application context.
   * @param webAppContext The {@link WebAppContext} to which the request will be forwarded 
   *                      after the URI has been rewritten.
   * @throws IOException If an I/O error occurs while initializing the web application context.
   */
  private void initWebAppContext(Builder builder, WebAppContext webAppContext) throws IOException {
    if (builder.useSPNEGO) {
      // Secure the web server with kerberos
      setupSpnegoFilter(builder, webAppContext);
    }

    if (builder.enableCORS) {
      setupCORSFilter(builder, webAppContext);
    }

    Map<String, String> xFrameParams = setHeaders();
    if (builder.xFrameEnabled) {
      setupXframeFilter(xFrameParams, webAppContext);
    }

    if (builder.disableDirListing) {
      disableDirectoryListingOnServlet(webAppContext);
    }
  }

  /**
   * Creates and configures a {@link RewriteHandler} that rewrites incoming request URIs 
   * based on predefined rules, and sets the specified {@link WebAppContext} as the 
   * handler for the rewritten requests.
   *
   * <p>This method creates a {@link RewriteHandler} that rewrites requests to the root path
   * ("/") to a new target URI specified by the {@code builder.contextRootRewriteTarget}. 
   * The URI rewrite is applied before forwarding the request to the given {@link WebAppContext}.</p>
   *
   * @param builder The builder object containing configuration values, such as the 
   *                target for URI rewrite.
   * @param webAppContext The {@link WebAppContext} to which the request will be forwarded 
   *                      after the URI has been rewritten.
   * @return A {@link RewriteHandler} configured with the rewrite rule and the web application context.
   */
  private RewriteHandler createRewriteHandler(Builder builder, WebAppContext webAppContext) {
    RewriteHandler rwHandler = new RewriteHandler();
    rwHandler.setRewriteRequestURI(true);
    rwHandler.setRewritePathInfo(false);

    RewriteRegexRule rootRule = new RewriteRegexRule();
    rootRule.setRegex("^/$");
    rootRule.setReplacement(builder.contextRootRewriteTarget);
    rootRule.setTerminating(true);

    rwHandler.addRule(rootRule);
    rwHandler.setHandler(webAppContext);
    
    return rwHandler;
  }
  
  /**
   * Create a channel connector for "http/https" requests and add it to the server
   */
  ServerConnector createAndAddChannelConnector(int queueSize, Builder b) {
    ServerConnector connector;

    final HttpConfiguration conf = new HttpConfiguration();
    conf.setRequestHeaderSize(1024*64);
    conf.setSendServerVersion(false);
    conf.setSendXPoweredBy(false);
    final HttpConnectionFactory http = new HttpConnectionFactory(conf);

    if (!b.useSSL) {
      connector = new ServerConnector(webServer, http);
    } else {
      SslContextFactory sslContextFactory = new SslContextFactory.Server();
      sslContextFactory.setKeyStorePath(b.keyStorePath);
      sslContextFactory.setKeyStoreType(b.keyStoreType == null || b.keyStoreType.isEmpty() ?
          KeyStore.getDefaultType(): b.keyStoreType);
      sslContextFactory.setKeyManagerFactoryAlgorithm(
          b.keyManagerFactoryAlgorithm == null || b.keyManagerFactoryAlgorithm.isEmpty()?
          KeyManagerFactory.getDefaultAlgorithm() : b.keyManagerFactoryAlgorithm);
      if (b.excludeCiphersuites != null && !b.excludeCiphersuites.trim().isEmpty()) {
        Set<String> excludeCS = Sets.newHashSet(
            Splitter.on(",").trimResults().omitEmptyStrings().split(b.excludeCiphersuites.trim()));
        int eSize = excludeCS.size();
        if (eSize > 0) {
          sslContextFactory.setExcludeCipherSuites(excludeCS.toArray(new String[eSize]));
        }
      }
      Set<String> excludedSSLProtocols = Sets.newHashSet(
        Splitter.on(",").trimResults().omitEmptyStrings().split(
          Strings.nullToEmpty(b.conf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST))));
      sslContextFactory.addExcludeProtocols(excludedSSLProtocols.toArray(
          new String[excludedSSLProtocols.size()]));
      sslContextFactory.setKeyStorePassword(b.keyStorePassword);
      connector = new ServerConnector(webServer, sslContextFactory, http);
    }

    connector.setAcceptQueueSize(queueSize);
    connector.setReuseAddress(true);
    connector.setHost(b.host);
    connector.setPort(b.port);

    webServer.addConnector(connector);
    return connector;
  }

  /**
   * Secure the web server with PAM.
   */
  void setupPam(Builder b, Handler handler) {
    LoginService loginService = new PamLoginService();
    webServer.addBean(loginService);
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();
    Constraint constraint = new PamConstraint();
    ConstraintMapping mapping = new PamConstraintMapping(constraint);
    security.setConstraintMappings(Collections.singletonList(mapping));
    security.setAuthenticator(b.pamAuthenticator);
    security.setLoginService(loginService);
    security.setHandler(handler);
    webServer.setHandler(security);
  }

  /**
   * Set servlet context attributes that can be used in jsp.
   */
  void setContextAttributes(Context ctx, Map<String, Object> contextAttrs) {
    for (Map.Entry<String, Object> e: contextAttrs.entrySet()) {
      ctx.setAttribute(e.getKey(), e.getValue());
    }
  }

  private void createWebServer(final Builder b) throws IOException {
    // Create the thread pool for the web server to handle HTTP requests
    threadPool = new QueuedThreadPool();
    if (b.maxThreads > 0) {
      threadPool.setMaxThreads(b.maxThreads);
    }
    threadPool.setDaemon(true);
    threadPool.setName(b.name + "-web");

    this.webServer = new Server(threadPool);

    initializeWebServer(b);
  }

  private void initializeWebServer(final Builder b) throws IOException {
    // Set handling for low resource conditions.
    final LowResourceMonitor low = new LowResourceMonitor(webServer);
    low.setLowResourcesIdleTimeout(10000);
    webServer.addBean(low);

    // Configure the global context handler
    portHandlerWrapper = new PortHandlerWrapper();
    webServer.setHandler(portHandlerWrapper);

    if (b.usePAM) {
      setupPam(b, portHandlerWrapper);
    }

    // Configures the web server connector and port handler to listen on
    // Also creates and adds the web application context to the server to which the servlets will be added
    ContextHandlerCollection portHandler = createAndAddWebApp(b);

    addServlet("jmx", "/jmx", JMXJsonServlet.class);
    addServlet("conf", "/conf", ConfServlet.class);
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("conflog", "/conflog", Log4j2ConfiguratorServlet.class);

    final String asyncProfilerHome = ProfileServlet.getAsyncProfilerHome();
    if (asyncProfilerHome != null && !asyncProfilerHome.trim().isEmpty()) {
      addServlet("prof", "/prof", ProfileServlet.class);
      Path tmpDir = Paths.get(ProfileServlet.OUTPUT_DIR);
      if (Files.notExists(tmpDir)) {
        Files.createDirectories(tmpDir);
      }
      ServletContextHandler genCtx = new ServletContextHandler(portHandler, "/prof-output");
      setContextAttributes(genCtx.getServletContext(), b.contextAttrs);
      genCtx.addServlet(ProfileOutputServlet.class, "/*");
      genCtx.setResourceBase(tmpDir.toAbsolutePath().toString());
      genCtx.setDisplayName("prof-output");
    } else {
      LOG.info("ASYNC_PROFILER_HOME env or -Dasync.profiler.home not specified. Disabling /prof endpoint..");
    }
    ServletContextHandler staticCtx = new ServletContextHandler(portHandler, "/static");
    staticCtx.setResourceBase(getWebAppsPath(b.name) + "/static");
    staticCtx.addServlet(DefaultServlet.class, "/*");
    staticCtx.setDisplayName("static");
    disableDirectoryListingOnServlet(staticCtx);

    String logDir = getLogDir(b.conf);
    if (logDir != null) {
      ServletContextHandler logCtx = new ServletContextHandler(portHandler, "/logs");
      setContextAttributes(logCtx.getServletContext(), b.contextAttrs);
      if(b.useSPNEGO) {
        setupSpnegoFilter(b,logCtx);
      }
      logCtx.addServlet(AdminAuthorizedServlet.class, "/*");
      logCtx.setResourceBase(logDir);
      logCtx.setDisplayName("logs");
    }

    // Define the global filers for each servlet context except the staticCtx(css style).
    Optional<Handler[]> handlers = Optional.ofNullable(portHandler.getHandlers());
    handlers.ifPresent(hs -> Arrays.stream(hs)
        .filter(h -> h instanceof ServletContextHandler && !"static".equals(((ServletContextHandler) h).getDisplayName()))
        .forEach(h -> b.globalFilters.forEach((k, v) ->
            addFilter(k, v.getKey(), v.getValue(), ((ServletContextHandler) h).getServletHandler()))));
  }

  private Map<String, String> setHeaders() {
    Map<String, String> xFrameParams = new HashMap<>();
    xFrameParams.putAll(getDefaultHeaders());
    if(this.xFrameOptionIsEnabled) {
      xFrameParams.put(HTTP_HEADER_PREFIX+X_FRAME_OPTIONS,
              this.xFrameOption.toString());
    }
    return xFrameParams;
  }

  private Map<String, String> getDefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    String[] splitVal = X_CONTENT_TYPE_OPTIONS.split(":");
    headers.put(HTTP_HEADER_PREFIX + splitVal[0],
            splitVal[1]);
    splitVal = X_XSS_PROTECTION.split(":");
    headers.put(HTTP_HEADER_PREFIX + splitVal[0],
            splitVal[1]);
    if(this.isSSLEnabled){
      splitVal = STRICT_TRANSPORT_SECURITY.split(":");
      headers.put(HTTP_HEADER_PREFIX + splitVal[0],splitVal[1]);
    }
    return headers;
  }

  private void setupXframeFilter(Map<String, String> params, WebAppContext webAppContext) {
    FilterHolder holder = new FilterHolder();
    holder.setClassName(QuotingInputFilter.class.getName());
    holder.setInitParameters(params);

    ServletHandler handler = webAppContext.getServletHandler();
    handler.addFilterWithMapping(holder, "/*", FilterMapping.ALL);

  }

  String getLogDir(Configuration conf) {
    String logDir = conf.get("hive.log.dir");
    if (logDir == null) {
      logDir = System.getProperty("hive.log.dir");
    }
    if (logDir != null) {
      return logDir;
    }

    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    for (Logger logger: context.getLoggers()) {
      for (Appender appender: logger.getAppenders().values()) {
        if (appender instanceof AbstractOutputStreamAppender streamAppender) {
          OutputStreamManager manager =
            streamAppender.getManager();
          if (manager instanceof FileManager fileManager) {
            String fileName = fileManager.getFileName();
            if (fileName != null) {
              return fileName.substring(0, fileName.lastIndexOf('/'));
            }
          }
        }
      }
    }
    return null;
  }

  String getWebAppsPath(String appName) throws FileNotFoundException {
    String relativePath = "hive-webapps/" + appName;
    URL url = getClass().getClassLoader().getResource(relativePath);
    if (url == null) {
      throw new FileNotFoundException(relativePath
          + " not found in CLASSPATH");
    }
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Add a servlet to the rootWebAppContext that is added to the webserver during its initialization.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec, Class<? extends HttpServlet> clazz) {
    addServlet(name, pathSpec, clazz, rootWebAppContext);
  }

  /**
   * Add a servlet to the provided webAppContext
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @param webAppContext The webAppContext to which the servlet will be added
   */
  private void addServlet(String name, String pathSpec, Class<? extends HttpServlet> clazz,
      WebAppContext webAppContext) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
  }

  /**
   * Add a servlet holder to the rootWebAppContext that is added to the webserver during its initialization.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param holder The servlet holder to be added to the webAppContext
   */
  public void addServlet(String name, String pathSpec, ServletHolder holder) {
    addServlet(name, pathSpec, holder, rootWebAppContext);
  }

  /**
   * Add a servlet holder to the provided webAppContext
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param holder The servlet holder to be added to the webAppContext
   * @param webAppContext The webAppContext to which the servlet will be added
   */
  private void addServlet(String name, String pathSpec, ServletHolder holder, WebAppContext webAppContext) {
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
  }

  public void addFilter(String name, String pathSpec, Filter filter, ServletHandler handler) {
    FilterHolder holder = new FilterHolder(filter);
    if (name != null) {
      holder.setName(name);
    }
    handler.addFilterWithMapping(holder, pathSpec, FilterMapping.ALL);
  }

  private static void disableDirectoryListingOnServlet(ServletContextHandler contextHandler) {
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
  }

  /**
   * The X-FRAME-OPTIONS header in HTTP response to mitigate clickjacking
   * attack.
   */
  public enum XFrameOption {
    DENY("DENY"), SAMEORIGIN("SAMEORIGIN"), ALLOWFROM("ALLOW-FROM");

    XFrameOption(String name) {
      this.name = name;
    }

    private final String name;

    @Override
    public String toString() {
      return this.name;
    }

    /**
     * We cannot use valueOf since the AllowFrom enum differs from its value
     * Allow-From. This is a helper method that does exactly what valueof does,
     * but allows us to handle the AllowFrom issue gracefully.
     *
     * @param value - String must be DENY, SAMEORIGIN or ALLOW-FROM.
     * @return XFrameOption or throws IllegalException.
     */
    private static XFrameOption getEnum(String value) {
      Preconditions.checkState(value != null && !value.isEmpty());
      for (XFrameOption xoption : values()) {
        if (value.equals(xoption.toString())) {
          return xoption;
        }
      }
      throw new IllegalArgumentException("Unexpected value in xFrameOption.");
    }
  }
  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks. It also
   * sets X-FRAME-OPTIONS in the header to mitigate clickjacking attacks.
   */
  public static class QuotingInputFilter implements Filter {

    private FilterConfig config;
    private Map<String, String> headerMap;

    public static class RequestQuoter extends HttpServletRequestWrapper {
      private final HttpServletRequest rawRequest;

      public RequestQuoter(HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }

      /**
       * Return the set of parameter names, quoting each name.
       */
      @SuppressWarnings("unchecked")
      @Override
      public Enumeration<String> getParameterNames() {
        return new Enumeration<String>() {
          private Enumeration<String> rawIterator =
                  rawRequest.getParameterNames();
          @Override
          public boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }

      /**
       * Unquote the name and quote the value.
       */
      @Override
      public String getParameter(String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter
                (HtmlQuoting.unquoteHtmlChars(name)));
      }

      @Override
      public String[] getParameterValues(String name) {
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        String[] unquoteValue = rawRequest.getParameterValues(unquoteName);
        if (unquoteValue == null) {
          return null;
        }
        String[] result = new String[unquoteValue.length];
        for(int i=0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<String, String[]> getParameterMap() {
        Map<String, String[]> result = new HashMap<>();
        Map<String, String[]> raw = rawRequest.getParameterMap();
        for (Map.Entry<String,String[]> item: raw.entrySet()) {
          String[] rawValue = item.getValue();
          String[] cookedValue = new String[rawValue.length];
          for(int i=0; i< rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }

      /**
       * Quote the url so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public StringBuffer getRequestURL(){
        String url = rawRequest.getRequestURL().toString();
        return new StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }

      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public String getServerName() {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
      this.config = config;
      initHttpHeaderMap();
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain
    ) throws IOException, ServletException {
      HttpServletRequestWrapper quoted =
              new RequestQuoter((HttpServletRequest) request);
      HttpServletResponse httpResponse = (HttpServletResponse) response;

      String mime = inferMimeType(request);
      if (mime == null) {
        httpResponse.setContentType("text/plain; charset=utf-8");
      } else if (mime.startsWith("text/html")) {
        // HTML with unspecified encoding, we want to
        // force HTML with utf-8 encoding
        // This is to avoid the following security issue:
        // http://openmya.hacker.jp/hasegawa/security/utf7cs.html
        httpResponse.setContentType("text/html; charset=utf-8");
      } else if (mime.startsWith("application/xml")) {
        httpResponse.setContentType("text/xml; charset=utf-8");
      }
      headerMap.forEach((k, v) -> httpResponse.addHeader(k, v));
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private String inferMimeType(ServletRequest request) {
      String path = ((HttpServletRequest)request).getRequestURI();
      ServletContextHandler.Context sContext =
              (ServletContextHandler.Context)config.getServletContext();
      String mime = sContext.getMimeType(path);
      return (mime == null) ? null : mime;
    }

    private void initHttpHeaderMap() {
      Enumeration<String> params = this.config.getInitParameterNames();
      headerMap = new HashMap<>();
      while (params.hasMoreElements()) {
        String key = params.nextElement();
        Matcher m = PATTERN_HTTP_HEADER_REGEX.matcher(key);
        if (m.matches()) {
          String headerKey = m.group(1);
          headerMap.put(headerKey, config.getInitParameter(key));
        }
      }
    }
  }

  /**
   * A custom {@link ContextHandlerCollection} that maps server connectors (ports) to specific handler collections.
   * This class allows for the association of different handlers with different ports, and ensures that requests
   * are routed to the appropriate handler based on the port they came through.
   *
   * <p>The {@link PortHandlerWrapper} class overrides the {@link ContextHandlerCollection#handle} method to
   * select the appropriate handler based on the request's port and delegate the request to that handler.
   * </p>
   *
   * <p>This class uses a map to associate each {@link ServerConnector} (which represents a port) to a 
   * {@link HandlerCollection}. The {@link #addHandler(ServerConnector, HandlerCollection)} method allows handlers
   * to be added for specific ports.</p>
   */
  static class PortHandlerWrapper extends ContextHandlerCollection {

    /** Map of server connectors (ports) to their corresponding handler collections. */
    private final Map<ServerConnector, HandlerCollection> connectorToHandlerMap = new HashMap<>();

    /**
     * Adds a handler collection to the {@link PortHandlerWrapper} for a specific server connector (port).
     *
     * @param connector the {@link ServerConnector} representing the port to which the handler should be associated
     * @param handler the {@link HandlerCollection} that will handle requests on the specified port
     */
    public void addHandler(ServerConnector connector, HandlerCollection handler) {
      connectorToHandlerMap.put(connector, handler);
      addHandler(handler);
    }

    /**
     * Handles the HTTP request by determining which port the request came through and routing it to the appropriate handler.
     *
     * @param target the target of the request
     * @param baseRequest the base request object
     * @param request the {@link HttpServletRequest} object containing the request details
     * @param response the {@link HttpServletResponse} object to send the response
     * @throws IOException if an input or output exception occurs during the handling of the request
     * @throws ServletException if a servlet-specific exception occurs during the handling of the request
     */
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
      // Determine the connector (port) the request came through
      int port = request.getServerPort();

      // Find the handler for the corresponding port
      Handler handler = connectorToHandlerMap.entrySet().stream()
          .filter(entry -> entry.getKey().getPort() == port)
          .map(Map.Entry::getValue)
          .findFirst()
          .orElseThrow(() -> new IllegalStateException("No handler found for port " + port));

      // Delegate the request to the handler
      handler.handle(target, baseRequest, request, response);
    }
  }
}
