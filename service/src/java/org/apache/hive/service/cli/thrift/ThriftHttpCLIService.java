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

package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.cli.CLIService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class ThriftHttpCLIService extends ThriftCLIService {

  public ThriftHttpCLIService(CLIService cliService) {
    super(cliService, "ThriftHttpCLIService");
  }

  @Override
  public void run() {
    try {
      // Configure Jetty to serve http requests
      // Example of a client connection URL: http://localhost:10000/servlets/thrifths2/
      // a gateway may cause actual target URL to differ, e.g. http://gateway:port/hive2/servlets/thrifths2/

      verifyHttpConfiguration(hiveConf);

      String portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }

      minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_MIN_WORKER_THREADS);
      maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_WORKER_THREADS);

      String httpPath =  getHttpPath(hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));

      httpServer = new org.eclipse.jetty.server.Server();
      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMinThreads(minWorkerThreads);
      threadPool.setMaxThreads(maxWorkerThreads);
      httpServer.setThreadPool(threadPool);

      SelectChannelConnector connector = new SelectChannelConnector();;
      boolean useSsl = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL);
      String schemeName = useSsl ? "https" : "http";
      String authType = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
      // Set during the init phase of HiveServer2 if auth mode is kerberos
      // UGI for the hive/_HOST (kerberos) principal
      UserGroupInformation serviceUGI = cliService.getServiceUGI();
      // UGI for the http/_HOST (SPNego) principal
      UserGroupInformation httpUGI = cliService.getHttpUGI();

      if (useSsl) {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        String keyStorePassword = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD);
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname +
              " Not configured for SSL connection");
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        connector = new SslSelectChannelConnector(sslContextFactory);
      }

      connector.setPort(portNum);
      // Linux:yes, Windows:no
      connector.setReuseAddress(!Shell.WINDOWS);
      httpServer.addConnector(connector);

      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);
      TProcessor processor = processorFactory.getProcessor(null);

      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

      TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory,
          authType, serviceUGI, httpUGI);

      final ServletContextHandler context = new ServletContextHandler(
          ServletContextHandler.SESSIONS);
      context.setContextPath("/");

      httpServer.setHandler(context);
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

      // TODO: check defaults: maxTimeout, keepalive, maxBodySize, bodyRecieveDuration, etc.
      httpServer.start();
      String msg = "Started ThriftHttpCLIService in " + schemeName + " mode on port " + portNum +
          " path=" + httpPath +
          " with " + minWorkerThreads + ".." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      httpServer.join();
    } catch (Throwable t) {
      LOG.error("Error: ", t);
    }
  }

  /**
   * The config parameter can be like "path", "/path", "/path/", "path/*", "/path1/path2/*" and so on.
   * httpPath should end up as "/*", "/path/*" or "/path1/../pathN/*"
   * @param httpPath
   * @return
   */
  private String getHttpPath(String httpPath) {
    if(httpPath == null || httpPath.equals("")) {
      httpPath = "/*";
    }
    else {
      if(!httpPath.startsWith("/")) {
        httpPath = "/" + httpPath;
      }
      if(httpPath.endsWith("/")) {
        httpPath = httpPath + "*";
      }
      if(!httpPath.endsWith("/*")) {
        httpPath = httpPath + "/*";
      }
    }
    return httpPath;
  }

  /**
   * Verify that this configuration is supported by transportMode of HTTP
   * @param hiveConf
   */
  private static void verifyHttpConfiguration(HiveConf hiveConf) {
    String authType = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);

    // Error out if KERBEROS auth mode is being used and use SSL is also set to true
    if(authType.equalsIgnoreCase(AuthTypes.KERBEROS.toString()) &&
        hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
      String msg = ConfVars.HIVE_SERVER2_AUTHENTICATION + " setting of " +
          authType + " is not supported with " +
          ConfVars.HIVE_SERVER2_USE_SSL + " set to true";
      LOG.fatal(msg);
      throw new RuntimeException(msg);
    }

    // Warn that SASL is not used in http mode
    if(authType.equalsIgnoreCase(AuthTypes.NONE.toString())) {
      // NONE in case of thrift mode uses SASL
      LOG.warn(ConfVars.HIVE_SERVER2_AUTHENTICATION + " setting to " +
          authType + ". SASL is not supported with http transport mode," +
          " so using equivalent of " + AuthTypes.NOSASL);
    }
  }

}
