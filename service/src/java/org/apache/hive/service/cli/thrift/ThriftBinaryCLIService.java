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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;


public class ThriftBinaryCLIService extends ThriftCLIService {
  protected TServer server;

  public ThriftBinaryCLIService(CLIService cliService) {
    super(cliService, ThriftBinaryCLIService.class.getSimpleName());
  }

  @Override
  protected HiveServer2TransportMode getTransportMode() {
    return HiveServer2TransportMode.binary;
  }

  @Override
  protected void initServer() {
    try {
      // Server thread pool
      String threadPoolName = "HiveServer2-Handler-Pool";
      ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<>(),
          new ThreadFactoryWithGarbageCleanup(threadPoolName));

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf, false);
      TTransportFactory transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      for (String sslVersion : hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthUtils.getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(
              ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname + " Not configured for SSL connection");
        }
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        String keyStoreType = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_TYPE).trim();
        String keyStoreAlgorithm = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYMANAGERFACTORY_ALGORITHM).trim();
        String includeCiphersuites = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_BINARY_INCLUDE_CIPHERSUITES).trim();
        serverSocket = HiveAuthUtils.getServerSSLSocket(hiveHost, portNum, keyStorePath, keyStorePassword,
            keyStoreType, keyStoreAlgorithm, sslVersionBlacklist, includeCiphersuites);
      }

      // Server args
      int maxMessageSize = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket).processorFactory(processorFactory)
          .transportFactory(transportFactory).protocolFactory(new TBinaryProtocol.Factory())
          .inputProtocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
          .executorService(executorService);

      // TCP Server
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(new TServerEventHandler() {

        @Override
        public ServerContext createContext(TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            metrics.incrementCounter(MetricsConstant.OPEN_CONNECTIONS);
            metrics.incrementCounter(MetricsConstant.CUMULATIVE_CONNECTION_COUNT);
          }
          return new ThriftCLIServerContext();
        }

        /**
         * This is called by the Thrift server when the underlying client
         * connection is cleaned up by the server because the connection has
         * been closed.
         */
        @Override
        public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            metrics.decrementCounter(MetricsConstant.OPEN_CONNECTIONS);
          }

          final ThriftCLIServerContext context = (ThriftCLIServerContext) serverContext;
          final Optional<SessionHandle> sessionHandle = context.getSessionHandle();

          if (sessionHandle.isPresent()) {
            // Normally, the client should politely inform the server it is
            // closing its session with Hive before closing its network
            // connection. However, if the client connection dies for any reason
            // (load-balancer round-robin configuration, firewall kills
            // long-running sessions, bad client, failed client, timed-out
            // client, etc.) then the server will close the connection without
            // having properly cleaned up the Hive session (resources,
            // configuration, logging etc.). That needs to be cleaned up now.
            LOG.warn(
                "Client connection bound to {} unexpectedly closed: closing this Hive session to release its resources. "
                    + "The connection processed {} total messages during its lifetime of {}ms. Inspect the client connection "
                    + "for time-out, firewall killing the connection, invalid load balancer configuration, etc.",
                sessionHandle, context.getMessagesProcessedCount(), context.getDuration().toMillis());
            try {
              final boolean close = cliService.getSessionManager().getSession(sessionHandle.get()).getHiveConf()
                  .getBoolVar(ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT);
              if (close) {
                cliService.closeSession(sessionHandle.get());
              } else {
                LOG.warn("Session not actually closed because configuration {} is set to false",
                    ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT.varname);
              }
            } catch (HiveSQLException e) {
              LOG.warn("Failed to close session", e);
            }
          } else {
            // There is no session handle because the client gracefully closed
            // the session *or* because the client had some issue and never was
            // able to create one in the first place
            if (context.getSessionCount() == 0) {
              LOG.info("A client connection was closed before creating a Hive session. "
                  + "Most likely it is a client that is connecting to this server then "
                  + "immediately closing the socket (i.e., TCP health check or port scanner)");
            }
          }
        }

        @Override
        public void preServe() {
        }

        @Override
        public void processContext(ServerContext serverContext, TTransport input, TTransport output) {
          ThriftCLIServerContext context = (ThriftCLIServerContext) serverContext;
          currentServerContext.set(context);
          context.incMessagesProcessedCount();
        }
      });
      String msg = "Starting " + ThriftBinaryCLIService.class.getSimpleName() + " on port " + portNum + " with "
          + minWorkerThreads + "..." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
    } catch (Exception e) {
      throw new RuntimeException("Failed to init thrift server", e);
    }
  }

  @Override
  public void run() {
    try {
      server.serve();
    } catch (Throwable t) {
      if (t instanceof InterruptedException) {
        // This is likely a shutdown
        LOG.info("Caught " + t.getClass().getSimpleName() + ". Shutting down.");
      } else {
        LOG.error("Exception caught by " + this.getClass().getSimpleName() +
            ". Exiting.", t);
      }
      System.exit(-1);
    }
  }

  @Override
  protected void stopServer() {
    server.stop();
    server = null;
    LOG.info("Thrift server has stopped");
  }

}
