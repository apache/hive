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
package org.apache.hive.hcatalog.templeton;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.LowResourceMonitor;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The main executable that starts up and runs the Server.
 */
@InterfaceAudience.LimitedPrivate("Integration Tests")
@InterfaceStability.Unstable
public class Main {
  public static final String SERVLET_PATH = "templeton";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static final int DEFAULT_PORT = 8080;
  public static final String DEFAULT_HOST = "0.0.0.0";
  public static final String DEFAULT_KEY_STORE_PATH = "";
  public static final String DEFAULT_KEY_STORE_PASSWORD = "";
  public static final String DEFAULT_SSL_PROTOCOL_BLACKLIST = "SSLv2,SSLv3";
  private Server server;

  private static volatile AppConfig conf;

  /**
   * Retrieve the config singleton.
   */
  public static synchronized AppConfig getAppConfigInstance() {
    if (conf == null)
      LOG.error("Bug: configuration not yet loaded");
    return conf;
  }

  Main(String[] args) {
    init(args);
  }

  public void init(String[] args) {
    initLogger();
    conf = loadConfig(args);
    conf.startCleanup();
    LOG.debug("Loaded conf " + conf);
  }

  // Jersey uses java.util.logging - bridge to slf4
  private void initLogger() {
    java.util.logging.Logger rootLogger
      = java.util.logging.LogManager.getLogManager().getLogger("");
    for (java.util.logging.Handler h : rootLogger.getHandlers())
      rootLogger.removeHandler(h);

    SLF4JBridgeHandler.install();
  }

  public AppConfig loadConfig(String[] args) {
    AppConfig cf = new AppConfig();
    try {
      GenericOptionsParser parser = new GenericOptionsParser(cf, args);
      if (parser.getRemainingArgs().length > 0)
        usage();
    } catch (IOException e) {
      LOG.error("Unable to parse options: " + e);
      usage();
    }

    return cf;
  }

  public void usage() {
    System.err.println("usage: templeton [-Dtempleton.port=N] [-D...]");
    System.exit(1);
  }

  public void run() {
    int port = conf.getInt(AppConfig.PORT, DEFAULT_PORT);
    try {
      checkEnv();
      runServer(port);
      // Currently only print the first port to be consistent with old behavior
      port =  ArrayUtils.isEmpty(server.getConnectors()) ? -1 : ((ServerConnector)(server.getConnectors()[0])).getLocalPort();

      System.out.println("templeton: listening on port " + port);
      LOG.info("Templeton listening on port " + port);
    } catch (Exception e) {
      System.err.println("templeton: Server failed to start: " + e.getMessage());
      LOG.error("Server failed to start: " , e);
      System.exit(1);
    }
  }
  void stop() {
    if(server != null) {
      try {
        server.stop();
      }
      catch(Exception ex) {
        LOG.warn("Failed to stop jetty.Server", ex);
      }
    }
  }


  private void checkEnv() {
    checkCurrentDirPermissions();

  }

  private void checkCurrentDirPermissions() {
    //org.apache.commons.exec.DefaultExecutor requires
    // that current directory exists
    File pwd = new File(".");
    if (!pwd.exists()) {
      String msg = "Server failed to start: templeton: Current working directory '.' does not exist!";
      System.err.println(msg);
      LOG.error(msg);
      System.exit(1);
    }
  }

  public Server runServer(int port)
    throws Exception {

    //Authenticate using keytab
    if (UserGroupInformation.isSecurityEnabled()) {
      String serverPrincipal = SecurityUtil.getServerPrincipal(conf.kerberosPrincipal(), "0.0.0.0");
      UserGroupInformation.loginUserFromKeytab(serverPrincipal,
        conf.kerberosKeytab());
    }

    // Create the Jetty server. If jetty conf file exists, use that to create server
    // to have more control.
    Server server = null;
    if (StringUtils.isEmpty(conf.jettyConfiguration())) {
      server = new Server(port);
    } else {
        FileInputStream jettyConf = new FileInputStream(conf.jettyConfiguration());
        XmlConfiguration configuration = new XmlConfiguration(jettyConf);
        server = (Server)configuration.configure();
    }

    ServletContextHandler root = new ServletContextHandler(server, "/");

    // Add the Auth filter
    FilterHolder fHolder = makeAuthFilter();
    EnumSet<DispatcherType> dispatches = EnumSet.of(DispatcherType.REQUEST);

    /* 
     * We add filters for each of the URIs supported by templeton.
     * If we added the entire sub-structure using '/*', the mapreduce 
     * notification cannot give the callback to templeton in secure mode.
     * This is because mapreduce does not use secure credentials for 
     * callbacks. So jetty would fail the request as unauthorized.
     */ 
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/ddl/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/pig/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/hive/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/sqoop/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/queue/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/jobs/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/mapreduce/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/status/*", dispatches);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/version/*", dispatches);

    if (conf.getBoolean(AppConfig.XSRF_FILTER_ENABLED, false)){
      root.addFilter(makeXSRFFilter(), "/" + SERVLET_PATH + "/*", dispatches);
      LOG.debug("XSRF filter enabled");
    } else {
      LOG.warn("XSRF filter disabled");
    }

    root.addFilter(makeFrameOptionFilter(), "/" + SERVLET_PATH + "/*", dispatches);

    // Connect Jersey
    ServletHolder h = new ServletHolder(new ServletContainer(makeJerseyConfig()));
    root.addServlet(h, "/" + SERVLET_PATH + "/*");
    // Add any redirects
    addRedirects(server);

    // Set handling for low resource conditions.
    final LowResourceMonitor low = new LowResourceMonitor(server);
    low.setLowResourcesIdleTimeout(10000);
    server.addBean(low);

    server.setConnectors(new Connector[]{ createChannelConnector(server) });

    // Start the server
    server.start();
    this.server = server;
    return server;
  }

  public FilterHolder makeXSRFFilter() {
    String customHeader = null; // The header to look for. We use "X-XSRF-HEADER" if this is null.
    String methodsToIgnore = null; // Methods to not filter. By default: "GET,OPTIONS,HEAD,TRACE" if null.
    FilterHolder fHolder = new FilterHolder(Utils.getXSRFFilter());
    if (customHeader != null){
      fHolder.setInitParameter(Utils.XSRF_CUSTOM_HEADER_PARAM, customHeader);
    }
    if (methodsToIgnore != null){
      fHolder.setInitParameter(Utils.XSRF_CUSTOM_METHODS_TO_IGNORE_PARAM, methodsToIgnore);
    }
    FilterHolder xsrfFilter = fHolder;

    return xsrfFilter;
  }

  /**
   Create a channel connector for "http/https" requests.
   */

  private Connector createChannelConnector(Server server) {
    ServerConnector connector;
    final HttpConfiguration httpConf = new HttpConfiguration();
    httpConf.setRequestHeaderSize(1024 * 64);
    final HttpConnectionFactory http = new HttpConnectionFactory(httpConf);

    if (conf.getBoolean(AppConfig.USE_SSL, false)) {
      LOG.info("Using SSL for templeton.");
      SslContextFactory sslContextFactory = new SslContextFactory.Server();
      sslContextFactory.setKeyStorePath(conf.get(AppConfig.KEY_STORE_PATH, DEFAULT_KEY_STORE_PATH));
      sslContextFactory.setKeyStorePassword(conf.get(AppConfig.KEY_STORE_PASSWORD, DEFAULT_KEY_STORE_PASSWORD));
      Set<String> excludedSSLProtocols = Sets.newHashSet(Splitter.on(",").trimResults().omitEmptyStrings()
          .split(Objects.toString(conf.get(AppConfig.SSL_PROTOCOL_BLACKLIST, DEFAULT_SSL_PROTOCOL_BLACKLIST), "")));
      sslContextFactory.addExcludeProtocols(excludedSSLProtocols.toArray(new String[excludedSSLProtocols.size()]));
      connector = new ServerConnector(server, sslContextFactory, http);
    } else {
      connector = new ServerConnector(server, http);
    }

    connector.setReuseAddress(true);
    connector.setHost(conf.get(AppConfig.HOST, DEFAULT_HOST));
    connector.setPort(conf.getInt(AppConfig.PORT, DEFAULT_PORT));
    return connector;
  }

  // Configure the AuthFilter with the Kerberos params iff security
  // is enabled.
  public FilterHolder makeAuthFilter() throws IOException {
    FilterHolder authFilter = new FilterHolder(AuthFilter.class);
    UserNameHandler.allowAnonymous(authFilter);
  
    String confPrefix = "dfs.web.authentication";
    String prefix = confPrefix + ".";
    authFilter.setInitParameter(AuthenticationFilter.CONFIG_PREFIX, confPrefix);
    authFilter.setInitParameter(prefix + AuthenticationFilter.COOKIE_PATH, "/");
    
    if (UserGroupInformation.isSecurityEnabled()) {
      authFilter.setInitParameter(prefix + AuthenticationFilter.AUTH_TYPE, KerberosAuthenticationHandler.TYPE);
      
      String serverPrincipal = SecurityUtil.getServerPrincipal(conf.kerberosPrincipal(), "0.0.0.0");
      authFilter.setInitParameter(prefix + KerberosAuthenticationHandler.PRINCIPAL, serverPrincipal);
      authFilter.setInitParameter(prefix + KerberosAuthenticationHandler.KEYTAB, conf.kerberosKeytab());
      authFilter.setInitParameter(prefix + AuthenticationFilter.SIGNATURE_SECRET, conf.kerberosSecret());
    } else {
      authFilter.setInitParameter(prefix + AuthenticationFilter.AUTH_TYPE, PseudoAuthenticationHandler.TYPE);
    }
    
    return authFilter;
  }

  public FilterHolder makeFrameOptionFilter() {
    FilterHolder frameOptionFilter = new FilterHolder(XFrameOptionsFilter.class);
    frameOptionFilter.setInitParameter(AppConfig.FRAME_OPTIONS_FILETER, conf.get(AppConfig.FRAME_OPTIONS_FILETER));
    return frameOptionFilter;
  }

  public static class XFrameOptionsFilter implements Filter {
    private final static String defaultMode = "DENY";

    private String mode = null;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
      mode = filterConfig.getInitParameter(AppConfig.FRAME_OPTIONS_FILETER);
      if (mode == null) {
        mode = defaultMode;
      }
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
        throws IOException, ServletException {
      final HttpServletResponse res = (HttpServletResponse) response;
      res.setHeader("X-FRAME-OPTIONS", mode);
      chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
      // do nothing
    }
  }

  public PackagesResourceConfig makeJerseyConfig() {
    PackagesResourceConfig rc
      = new PackagesResourceConfig("org.apache.hive.hcatalog.templeton");
    HashMap<String, Object> props = new HashMap<String, Object>();
    props.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
    props.put("com.sun.jersey.config.property.WadlGeneratorConfig",
      "org.apache.hive.hcatalog.templeton.WadlConfig");
    rc.setPropertiesAndFeatures(props);

    return rc;
  }

  public void addRedirects(Server server) {
    RewriteHandler rewrite = new RewriteHandler();

    RedirectPatternRule redirect = new RedirectPatternRule();
    redirect.setPattern("/templeton/v1/application.wadl");
    redirect.setLocation("/templeton/application.wadl");
    rewrite.addRule(redirect);

    HandlerList handlerlist = new HandlerList();
    ArrayList<Handler> handlers = new ArrayList<Handler>();

    // Any redirect handlers need to be added first
    handlers.add(rewrite);

    // Now add all the default handlers
    for (Handler handler : server.getHandlers()) {
      handlers.add(handler);
    }
    Handler[] newlist = new Handler[handlers.size()];
    handlerlist.setHandlers(handlers.toArray(newlist));
    server.setHandler(handlerlist);
  }

  public static void main(String[] args) {
    Main templeton = new Main(args);
    templeton.run();
  }

  /**
   * as of 3/6/2014 all WebHCat gives examples of POST requests that send user.name as a form 
   * parameter (in simple security mode).  That is no longer supported by PseudoAuthenticationHandler.
   * This class compensates for it.  
   * Alternatively, WebHCat could have implemented it's own version of PseudoAuthenticationHandler
   * and make sure that it's picked up by AuthenticationFilter.init(); (HADOOP-10193 has some context)
   * @deprecated since 0.13; callers should submit user.name as a query parameter.  user.name as a 
   * form param will be de-supported in 0.15
   */
  static final class UserNameHandler {
    static void allowAnonymous(FilterHolder authFilter) {
      /*note that will throw if Anonymous mode is not allowed & user.name is not in query string of the request;
      * this ensures that in the context of WebHCat, PseudoAuthenticationHandler allows Anonymous even though
      * WebHCat itself will throw if it can't figure out user.name*/
      authFilter.setInitParameter("dfs.web.authentication." + PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
    }
    static String getUserName(HttpServletRequest request) {
      if(!UserGroupInformation.isSecurityEnabled() && "POST".equalsIgnoreCase(request.getMethod())) {
      /*as of hadoop 2.3.0, PseudoAuthenticationHandler only expects user.name as a query param
      * (not as a form param in a POST request.  For backwards compatibility, we this logic
      * to get user.name when it's sent as a form parameter.
      * This is added in Hive 0.13 and should be de-supported in 0.15*/
        String userName = request.getParameter(PseudoAuthenticator.USER_NAME);
        if(userName != null) {
          LOG.warn(PseudoAuthenticator.USER_NAME + 
            " is sent as form parameter which is deprecated as of Hive 0.13.  Should send it in the query string.");
        }
        return userName;
      }
      return null;
    }
  }
}
