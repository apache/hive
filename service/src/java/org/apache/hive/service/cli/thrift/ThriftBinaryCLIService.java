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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;


public class ThriftBinaryCLIService extends ThriftCLIService {
  private final Runnable oomHook;
  protected TServer server;

  public ThriftBinaryCLIService(CLIService cliService, Runnable oomHook) {
    super(cliService, ThriftBinaryCLIService.class.getSimpleName());
    this.oomHook = oomHook;
  }

  @Override
  protected void initServer() {
    try {
      // Server thread pool
      String threadPoolName = "HiveServer2-Handler-Pool";
      ExecutorService executorService = new ThreadPoolExecutorWithOomHook(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new ThreadFactoryWithGarbageCleanup(threadPoolName), oomHook);

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
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
        serverSocket = HiveAuthUtils.getServerSSLSocket(hiveHost, portNum, keyStorePath, keyStorePassword,
            sslVersionBlacklist);
      }

      // Server args
      int maxMessageSize = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE);
      int requestTimeout = (int) hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT,
          TimeUnit.SECONDS);
      int beBackoffSlotLength = (int) hiveConf
          .getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, TimeUnit.MILLISECONDS);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket).processorFactory(processorFactory)
          .transportFactory(transportFactory).protocolFactory(new TBinaryProtocol.Factory())
          .inputProtocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
          .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS).beBackoffSlotLength(beBackoffSlotLength)
          .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS).executorService(executorService);

      // TCP Server
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(new TServerEventHandler() {
        @Override
        public ServerContext createContext(TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.incrementCounter(MetricsConstant.OPEN_CONNECTIONS);
              metrics.incrementCounter(MetricsConstant.CUMULATIVE_CONNECTION_COUNT);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
          return new ThriftCLIServerContext();
        }

        @Override
        public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.decrementCounter(MetricsConstant.OPEN_CONNECTIONS);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
          ThriftCLIServerContext context = (ThriftCLIServerContext) serverContext;
          SessionHandle sessionHandle = context.getSessionHandle();
          if (sessionHandle != null) {
            LOG.info("Session disconnected without closing properly. ");
            try {
              boolean close = cliService.getSessionManager().getSession(sessionHandle).getHiveConf()
                  .getBoolVar(ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT);
              LOG.info((close ? "" : "Not ") + "Closing the session: " + sessionHandle);
              if (close) {
                cliService.closeSession(sessionHandle);
              }
            } catch (HiveSQLException e) {
              LOG.warn("Failed to close session: " + e, e);
            }
          }
        }

        @Override
        public void preServe() {
        }

        @Override
        public void processContext(ServerContext serverContext, TTransport input, TTransport output) {
          currentServerContext.set(serverContext);
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
