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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
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

  private final String name;
  private String appDir;
  private WebAppContext webAppContext;
  private Server webServer;

  /**
   * Create a status server on the given port.
   */
  private HttpServer(final Builder b) throws IOException {
    this.name = b.name;

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
    private PamAuthenticator pamAuthenticator;
    private String contextRootRewriteTarget = "/index.html";
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
    ctx.setDisplayName(b.name);
    ctx.setContextPath("/");
    ctx.setWar(appDir + "/" + b.name);
    return ctx;
  }

  /**
   * Secure the web server with kerberos (AuthenticationFilter).
   */
  void setupSpnegoFilter(Builder b) throws IOException {
    Map<String, String> params = new HashMap<String, String>();
    params.put("kerberos.principal",
      SecurityUtil.getServerPrincipal(b.spnegoPrincipal, b.host));
    params.put("kerberos.keytab", b.spnegoKeytab);
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
    FilterHolder holder = new FilterHolder();
    holder.setClassName(AuthenticationFilter.class.getName());
    holder.setInitParameters(params);

    ServletHandler handler = webAppContext.getServletHandler();
    handler.addFilterWithMapping(
      holder, "/*", FilterMapping.ALL);
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
      setupSpnegoFilter(b);
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
      logCtx.addServlet(AdminAuthorizedServlet.class, "/*");
      logCtx.setResourceBase(logDir);
      logCtx.setDisplayName("logs");
    }
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
}
