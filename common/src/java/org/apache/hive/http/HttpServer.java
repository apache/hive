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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import org.apache.commons.lang.StringUtils;
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
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler.Context;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.ServerConnector;
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
  private String appDir;
  private WebAppContext webAppContext;
  private Server webServer;

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

    public Builder(String name) {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "Name must be specified");
      this.name = name;
    }

    public HttpServer build() throws IOException {
      return new HttpServer(this);
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
                           "Unauthenticated users are not " +
                           "authorized to access this page.");
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
  WebAppContext createWebAppContext(Builder b) {
    WebAppContext ctx = new WebAppContext();
    setContextAttributes(ctx.getServletContext(), b.contextAttrs);
    ctx.getServletContext().getSessionCookieConfig().setHttpOnly(true);
    ctx.setDisplayName(b.name);
    ctx.setContextPath("/");
    ctx.setWar(appDir + "/" + b.name);
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
   */
  private void setupCORSFilter(Builder b) {
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
   * Create a channel connector for "http/https" requests
   */
  Connector createChannelConnector(int queueSize, Builder b) {
    ServerConnector connector;

    final HttpConfiguration conf = new HttpConfiguration();
    conf.setRequestHeaderSize(1024*64);
    final HttpConnectionFactory http = new HttpConnectionFactory(conf);

    if (!b.useSSL) {
      connector = new ServerConnector(webServer, http);
    } else {
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(b.keyStorePath);
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
    QueuedThreadPool threadPool = new QueuedThreadPool();
    if (b.maxThreads > 0) {
      threadPool.setMaxThreads(b.maxThreads);
    }
    threadPool.setDaemon(true);
    threadPool.setName(b.name + "-web");

    this.webServer = new Server(threadPool);
    this.appDir = getWebAppsPath(b.name);
    this.webAppContext = createWebAppContext(b);

    if (b.useSPNEGO) {
      // Secure the web server with kerberos
      setupSpnegoFilter(b, webAppContext);
    }

    if (b.enableCORS) {
      setupCORSFilter(b);
    }

    Map<String, String> xFrameParams = setHeaders();
    if(b.xFrameEnabled){
      setupXframeFilter(b,xFrameParams);
    }

    initializeWebServer(b, threadPool.getMaxThreads());
  }

  private void initializeWebServer(final Builder b, int queueSize) throws IOException {
    // Set handling for low resource conditions.
    final LowResourceMonitor low = new LowResourceMonitor(webServer);
    low.setLowResourcesIdleTimeout(10000);
    webServer.addBean(low);

    Connector connector = createChannelConnector(queueSize, b);
    webServer.addConnector(connector);

    RewriteHandler rwHandler = new RewriteHandler();
    rwHandler.setRewriteRequestURI(true);
    rwHandler.setRewritePathInfo(false);

    RewriteRegexRule rootRule = new RewriteRegexRule();
    rootRule.setRegex("^/$");
    rootRule.setReplacement(b.contextRootRewriteTarget);
    rootRule.setTerminating(true);

    rwHandler.addRule(rootRule);
    rwHandler.setHandler(webAppContext);

    // Configure web application contexts for the web server
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.addHandler(rwHandler);
    webServer.setHandler(contexts);


    if(b.usePAM){
      setupPam(b, contexts);
    }


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
      ServletContextHandler genCtx =
        new ServletContextHandler(contexts, "/prof-output");
      setContextAttributes(genCtx.getServletContext(), b.contextAttrs);
      genCtx.addServlet(ProfileOutputServlet.class, "/*");
      genCtx.setResourceBase(tmpDir.toAbsolutePath().toString());
      genCtx.setDisplayName("prof-output");
    } else {
      LOG.info("ASYNC_PROFILER_HOME env or -Dasync.profiler.home not specified. Disabling /prof endpoint..");
    }

    for (Pair<String, Class<? extends HttpServlet>> p : b.servlets) {
      addServlet(p.getFirst(), "/" + p.getFirst(), p.getSecond());
    }

    ServletContextHandler staticCtx =
      new ServletContextHandler(contexts, "/static");
    staticCtx.setResourceBase(appDir + "/static");
    staticCtx.addServlet(DefaultServlet.class, "/*");
    staticCtx.setDisplayName("static");

    String logDir = getLogDir(b.conf);
    if (logDir != null) {
      ServletContextHandler logCtx =
        new ServletContextHandler(contexts, "/logs");
      setContextAttributes(logCtx.getServletContext(), b.contextAttrs);
      if(b.useSPNEGO) {
        setupSpnegoFilter(b,logCtx);
      }
      logCtx.addServlet(AdminAuthorizedServlet.class, "/*");
      logCtx.setResourceBase(logDir);
      logCtx.setDisplayName("logs");
    }
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

  private void setupXframeFilter(Builder b, Map<String, String> params) {
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
        if (appender instanceof AbstractOutputStreamAppender) {
          OutputStreamManager manager =
            ((AbstractOutputStreamAppender<?>)appender).getManager();
          if (manager instanceof FileManager) {
            String fileName = ((FileManager)manager).getFileName();
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
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
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

}
