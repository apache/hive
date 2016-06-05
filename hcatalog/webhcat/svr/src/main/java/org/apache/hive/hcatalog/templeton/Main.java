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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.eclipse.jetty.rewrite.handler.RedirectPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.servlet.http.HttpServletRequest;

/**
 * The main executable that starts up and runs the Server.
 */
@InterfaceAudience.LimitedPrivate("Integration Tests")
@InterfaceStability.Unstable
public class Main {
  public static final String SERVLET_PATH = "templeton";
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static final int DEFAULT_PORT = 8080;
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
      port =  ArrayUtils.isEmpty(server.getConnectors()) ? -1 : server.getConnectors()[0].getPort();

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
      UserGroupInformation.loginUserFromKeytab(conf.kerberosPrincipal(),
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

    /* 
     * We add filters for each of the URIs supported by templeton.
     * If we added the entire sub-structure using '/*', the mapreduce 
     * notification cannot give the callback to templeton in secure mode.
     * This is because mapreduce does not use secure credentials for 
     * callbacks. So jetty would fail the request as unauthorized.
     */ 
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/ddl/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/pig/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/hive/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/sqoop/*",
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/queue/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/jobs/*",
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/mapreduce/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/status/*", 
             FilterMapping.REQUEST);
    root.addFilter(fHolder, "/" + SERVLET_PATH + "/v1/version/*", 
             FilterMapping.REQUEST);

    if (conf.getBoolean(AppConfig.XSRF_FILTER_ENABLED, false)){
      root.addFilter(makeXSRFFilter(), "/" + SERVLET_PATH + "/*",
             FilterMapping.REQUEST);
      LOG.debug("XSRF filter enabled");
    } else {
      LOG.warn("XSRF filter disabled");
    }

    // Connect Jersey
    ServletHolder h = new ServletHolder(new ServletContainer(makeJerseyConfig()));
    root.addServlet(h, "/" + SERVLET_PATH + "/*");
    // Add any redirects
    addRedirects(server);

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

  // Configure the AuthFilter with the Kerberos params iff security
  // is enabled.
  public FilterHolder makeAuthFilter() {
    FilterHolder authFilter = new FilterHolder(AuthFilter.class);
    UserNameHandler.allowAnonymous(authFilter);
    if (UserGroupInformation.isSecurityEnabled()) {
      //http://hadoop.apache.org/docs/r1.1.1/api/org/apache/hadoop/security/authentication/server/AuthenticationFilter.html
      authFilter.setInitParameter("dfs.web.authentication.signature.secret",
        conf.kerberosSecret());
      //https://svn.apache.org/repos/asf/hadoop/common/branches/branch-1.2/src/packages/templates/conf/hdfs-site.xml
      authFilter.setInitParameter("dfs.web.authentication.kerberos.principal",
        conf.kerberosPrincipal());
      //http://https://svn.apache.org/repos/asf/hadoop/common/branches/branch-1.2/src/packages/templates/conf/hdfs-site.xml
      authFilter.setInitParameter("dfs.web.authentication.kerberos.keytab",
        conf.kerberosKeytab());
    }
    return authFilter;
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
