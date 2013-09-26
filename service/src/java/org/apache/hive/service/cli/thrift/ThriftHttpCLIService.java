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
import org.apache.hadoop.util.Shell;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.cli.CLIService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;


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

      String httpPath =  hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH);
      // The config parameter can be like "path", "/path", "/path/", "path/*", "/path1/path2/*" and so on.
      // httpPath should end up as "/*", "/path/*" or "/path1/../pathN/*"
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

      httpServer = new org.mortbay.jetty.Server();

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMinThreads(minWorkerThreads);
      threadPool.setMaxThreads(maxWorkerThreads);
      httpServer.setThreadPool(threadPool);
      SelectChannelConnector connector = new SelectChannelConnector();
      connector.setPort(portNum);

      // Linux:yes, Windows:no
      connector.setReuseAddress(!Shell.WINDOWS);
      httpServer.addConnector(connector);

      TCLIService.Processor<ThriftCLIService> processor =
          new TCLIService.Processor<ThriftCLIService>(new EmbeddedThriftBinaryCLIService());

      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory);
      final Context context = new Context(httpServer, "/", Context.SESSIONS);
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

      // TODO: check defaults: maxTimeout, keepalive, maxBodySize, bodyRecieveDuration, etc.
      httpServer.start();
      String msg = "Starting CLIService in Http mode on port " + portNum +
          " path=" + httpPath +
          " with " + minWorkerThreads + ".." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      httpServer.join();
    } catch (Throwable t) {
      LOG.error("Error: ", t);
    }
  }

  /**
   * Verify that this configuration is supported by transportMode of HTTP
   * @param hiveConf
   */
  private static void verifyHttpConfiguration(HiveConf hiveConf) {
    String authType = hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION);

    // error out if KERBEROS or LDAP mode is being used, it is not supported
    if(authType.equalsIgnoreCase(AuthTypes.KERBEROS.toString()) ||
        authType.equalsIgnoreCase(AuthTypes.LDAP.toString()) ||
        authType.equalsIgnoreCase(AuthTypes.CUSTOM.toString())) {
      String msg = ConfVars.HIVE_SERVER2_AUTHENTICATION + " setting of " +
          authType + " is currently not supported with " +
          ConfVars.HIVE_SERVER2_TRANSPORT_MODE + " setting of http";
      LOG.fatal(msg);
      throw new RuntimeException(msg);
    }

    // Throw exception here
    if(authType.equalsIgnoreCase(AuthTypes.NONE.toString())) {
      // NONE in case of thrift mode uses SASL
      LOG.warn(ConfVars.HIVE_SERVER2_AUTHENTICATION + " setting to " +
          authType + ". SASL is not supported with http transportMode," +
          " so using equivalent of " + AuthTypes.NOSASL);
    }

    // doAs is currently not supported with http
    if(hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      String msg = ConfVars.HIVE_SERVER2_ENABLE_DOAS + " setting of " +
          "true is currently not supported with " +
          ConfVars.HIVE_SERVER2_TRANSPORT_MODE + " setting of http";
      LOG.fatal(msg);
      throw new RuntimeException(msg);
    }
  }

}