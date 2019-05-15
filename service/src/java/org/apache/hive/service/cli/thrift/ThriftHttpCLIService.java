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

package org.apache.hive.service.cli.thrift;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.HttpMethod;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;


public class ThriftHttpCLIService extends ThriftCLIService {
  private static final String APPLICATION_THRIFT = "application/x-thrift";
  protected org.eclipse.jetty.server.Server server;

  private final Runnable oomHook;
  public ThriftHttpCLIService(CLIService cliService, Runnable oomHook) {
    super(cliService, ThriftHttpCLIService.class.getSimpleName());
    this.oomHook = oomHook;
  }

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target
   * URL to differ, e.g. http://gateway:port/hive2/servlets/thrifths2/
   */
  @Override
  protected void initServer() {
    try {
      // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject
      // subsequent requests
      String threadPoolName = "HiveServer2-HttpHandler-Pool";
      ExecutorService executorService = new ThreadPoolExecutorWithOomHook(minWorkerThreads,
          maxWorkerThreads,workerKeepAliveTime, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new ThreadFactoryWithGarbageCleanup(threadPoolName), oomHook);
      ExecutorThreadPool threadPool = new ExecutorThreadPool(executorService);

      // HTTP Server
      server = new Server(threadPool);

      ServerConnector connector;

      final HttpConfiguration conf = new HttpConfiguration();
      // Configure header size
      int requestHeaderSize =
          hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_REQUEST_HEADER_SIZE);
      int responseHeaderSize =
          hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_RESPONSE_HEADER_SIZE);
      conf.setRequestHeaderSize(requestHeaderSize);
      conf.setResponseHeaderSize(responseHeaderSize);
      final HttpConnectionFactory http = new HttpConnectionFactory(conf) {
        public Connection newConnection(Connector connector, EndPoint endPoint) {
          Connection connection = super.newConnection(connector, endPoint);
          connection.addListener(new Connection.Listener() {
            public void onOpened(Connection connection) {
              openConnection();
            }

            public void onClosed(Connection connection) {
              closeConnection();
            }
          });
          return connection;
        }
      };

      boolean useSsl = hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL);
      String schemeName = useSsl ? "https" : "http";

      // Change connector if SSL is used
      if (useSsl) {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(
              ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname 
              + " Not configured for SSL connection");
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        String[] excludedProtocols = hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",");
        LOG.info("HTTP Server SSL: adding excluded protocols: " + Arrays.toString(excludedProtocols));
        sslContextFactory.addExcludeProtocols(excludedProtocols);
        LOG.info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = "
            + Arrays.toString(sslContextFactory.getExcludeProtocols()));
        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        connector = new ServerConnector(server, sslContextFactory, http);
      } else {
        connector = new ServerConnector(server, http);
      }

      connector.setPort(portNum);
      // Linux:yes, Windows:no
      connector.setReuseAddress(true);
      int maxIdleTime = (int) hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME,
          TimeUnit.MILLISECONDS);
      connector.setIdleTimeout(maxIdleTime);
      connector.setAcceptQueueSize(maxWorkerThreads);

      server.addConnector(connector);

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      TProcessor processor = new TCLIService.Processor<Iface>(this);
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      // Set during the init phase of HiveServer2 if auth mode is kerberos
      // UGI for the hive/_HOST (kerberos) principal
      UserGroupInformation serviceUGI = cliService.getServiceUGI();
      // UGI for the http/_HOST (SPNego) principal
      UserGroupInformation httpUGI = cliService.getHttpUGI();
      String authType = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);
      TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory, authType, serviceUGI, httpUGI,
          hiveAuthFactory);

      // Context handler
      final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath("/");
      if (hiveConf.getBoolean(ConfVars.HIVE_SERVER2_XSRF_FILTER_ENABLED.varname, false)) {
        // context.addFilter(Utils.getXSRFFilterHolder(null, null), "/" ,
        // FilterMapping.REQUEST);
        // Filtering does not work here currently, doing filter in ThriftHttpServlet
        LOG.debug("XSRF filter enabled");
      } else {
        LOG.warn("XSRF filter disabled");
      }

      final String httpPath = getHttpPath(hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH));

      if (HiveConf.getBoolVar(hiveConf, ConfVars.HIVE_SERVER2_THRIFT_HTTP_COMPRESSION_ENABLED)) {
        final GzipHandler gzipHandler = new GzipHandler();
        gzipHandler.setHandler(context);
        gzipHandler.addIncludedMethods(HttpMethod.POST);
        gzipHandler.addIncludedMimeTypes(APPLICATION_THRIFT);
        server.setHandler(gzipHandler);
      } else {
        server.setHandler(context);
      }
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

      // TODO: check defaults: maxTimeout, keepalive, maxBodySize,
      // bodyRecieveDuration, etc.
      // Finally, start the server
      server.start();
      String msg = "Started " + ThriftHttpCLIService.class.getSimpleName() + " in " + schemeName
          + " mode on port " + portNum + " path=" + httpPath + " with " + minWorkerThreads + "..."
          + maxWorkerThreads + " worker threads";
      LOG.info(msg);
    } catch (Exception e) {
      throw new RuntimeException("Failed to init HttpServer", e);
    }
  }

  private void openConnection() {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      try {
        metrics.incrementCounter(MetricsConstant.OPEN_CONNECTIONS);
        metrics.incrementCounter(MetricsConstant.CUMULATIVE_CONNECTION_COUNT);
      } catch (Exception e) {
        LOG.warn("Error reporting HS2 open connection operation to Metrics system", e);
      }
    }
  }

  private void closeConnection() {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      try {
        metrics.decrementCounter(MetricsConstant.OPEN_CONNECTIONS);
      } catch (Exception e) {
        LOG.warn("Error reporting HS2 close connection operation to Metrics system", e);
      }
    }
  }

  @Override
  public void run() {
    try {
      server.join();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        // This is likely a shutdown
        LOG.info("Caught " + t.getClass().getSimpleName() + ". Shutting down thrift server.");
      } else {
        LOG.error("Exception caught by " + ThriftHttpCLIService.class.getSimpleName() +
            ". Exiting.", t);
        System.exit(-1);
      }
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

  @Override
  protected void stopServer() {
    if((server != null) && server.isStarted()) {
      try {
        server.stop();
        server = null;
        LOG.info("Thrift HTTP server has been stopped");
      } catch (Exception e) {
        LOG.error("Error stopping HTTP server: ", e);
      }
    }
  }

}
