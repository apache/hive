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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.security.UserGroupInformation;
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
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * The main executable that starts up and runs the Server.
 */
@InterfaceAudience.LimitedPrivate("Integration Tests")
@InterfaceStability.Unstable
public class Main {
  public static final String SERVLET_PATH = "templeton";
  private static final Log LOG = LogFactory.getLog(Main.class);

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
      System.out.println("templeton: listening on port " + port);
      LOG.info("Templeton listening on port " + port);
    } catch (Exception e) {
      System.err.println("templeton: Server failed to start: " + e.getMessage());
      LOG.fatal("Server failed to start: " + e);
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
      LOG.fatal(msg);
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

    // Create the Jetty server
    Server server = new Server(port);
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

  // Configure the AuthFilter with the Kerberos params iff security
  // is enabled.
  public FilterHolder makeAuthFilter() {
    FilterHolder authFilter = new FilterHolder(AuthFilter.class);
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
}
