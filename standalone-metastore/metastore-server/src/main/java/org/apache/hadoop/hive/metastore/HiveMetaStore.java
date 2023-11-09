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

package org.apache.hadoop.hive.metastore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZKDeRegisterWatcher;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.leader.HouseKeepingTasks;
import org.apache.hadoop.hive.metastore.leader.CMClearer;
import org.apache.hadoop.hive.metastore.leader.CompactorPMF;
import org.apache.hadoop.hive.metastore.leader.LeaderElectionContext;
import org.apache.hadoop.hive.metastore.leader.CompactorTasks;
import org.apache.hadoop.hive.metastore.leader.StatsUpdaterTask;
import org.apache.hadoop.hive.metastore.metrics.JvmPauseMonitor;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;
import org.apache.hadoop.hive.metastore.utils.LogUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TServlet;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
/**
 * TODO:pc remove application logic to a separate interface.
 */
public class HiveMetaStore extends ThriftHiveMetastore {
  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStore.class);

  // boolean that tells if the HiveMetaStore (remote) server is being used.
  // Can be used to determine if the calls to metastore api (HMSHandler) are being made with
  // embedded metastore or a remote one
  private static boolean isMetaStoreRemote = false;

  private static ShutdownHookManager shutdownHookMgr;

  /** MM write states. */
  public static final char MM_WRITE_OPEN = 'o', MM_WRITE_COMMITTED = 'c', MM_WRITE_ABORTED = 'a';

  static HadoopThriftAuthBridge.Server saslServer;
  private static MetastoreDelegationTokenManager delegationTokenManager;
  static boolean useSasl;

  private static ZooKeeperHiveHelper zooKeeperHelper = null;
  private static String msHost = null;
  private static ThriftServer thriftServer;
  private static Server propertyServer = null;


  public static Server getPropertyServer() {
    return propertyServer;
  }

  public static boolean isRenameAllowed(Database srcDB, Database destDB) {
    if (!srcDB.getName().equalsIgnoreCase(destDB.getName())) {
      if (ReplChangeManager.isSourceOfReplication(srcDB) || ReplChangeManager.isSourceOfReplication(destDB)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create HMS handler for embedded metastore.
   *
   * <h1>IMPORTANT</h1>
   *
   * This method is called indirectly by HiveMetastoreClient and HiveMetaStoreClientPreCatalog
   * using reflection. It can not be removed and its arguments can't be changed without matching
   * change in HiveMetastoreClient and HiveMetaStoreClientPreCatalog.
   *
   * @param conf configuration to use
   * @throws MetaException
   */
  static Iface newHMSHandler(Configuration conf)
      throws MetaException {
    HMSHandler baseHandler = new HMSHandler("hive client", conf);
    return HMSHandlerProxyFactory.getProxy(conf, baseHandler, true);
  }

  /**
   * Discard a current delegation token.
   *
   * @param tokenStrForm
   *          the token in string form
   */
  public static void cancelDelegationToken(String tokenStrForm) throws IOException {
    delegationTokenManager.cancelDelegationToken(tokenStrForm);
  }

  /**
   * Get a new delegation token.
   *
   * @param renewer
   *          the designated renewer
   */
  public static String getDelegationToken(String owner, String renewer, String remoteAddr)
      throws IOException, InterruptedException {
    return delegationTokenManager.getDelegationToken(owner, renewer, remoteAddr);
  }

  /**
   * @return true if remote metastore has been created
   */
  public static boolean isMetaStoreRemote() {
    return isMetaStoreRemote;
  }

  /**
   * Renew a delegation token to extend its lifetime.
   *
   * @param tokenStrForm
   *          the token in string form
   */
  public static long renewDelegationToken(String tokenStrForm) throws IOException {
    return delegationTokenManager.renewDelegationToken(tokenStrForm);
  }

  /**
   * HiveMetaStore specific CLI
   *
   */
  public static class HiveMetastoreCli extends CommonCliOptions {
    private int port;

    @SuppressWarnings("static-access")
    HiveMetastoreCli(Configuration configuration) {
      super("hivemetastore", true);
      this.port = MetastoreConf.getIntVar(configuration, ConfVars.SERVER_PORT);

      // -p port
      OPTIONS.addOption(OptionBuilder
          .hasArg()
          .withArgName("port")
          .withDescription("Hive Metastore port number, default:"
              + this.port)
          .create('p'));

    }
    @Override
    public void parse(String[] args) {
      super.parse(args);

      // support the old syntax "hivemetastore [port]" but complain
      args = commandLine.getArgs();
      if (args.length > 0) {
        // complain about the deprecated syntax -- but still run
        System.err.println(
            "This usage has been deprecated, consider using the new command "
                + "line syntax (run with -h to see usage information)");

        this.port = Integer.parseInt(args[0]);
      }

      // notice that command line options take precedence over the
      // deprecated (old style) naked args...

      if (commandLine.hasOption('p')) {
        this.port = Integer.parseInt(commandLine.getOptionValue('p'));
      } else {
        // legacy handling
        String metastorePort = System.getenv("METASTORE_PORT");
        if (metastorePort != null) {
          this.port = Integer.parseInt(metastorePort);
        }
      }
    }

    public int getPort() {
      return this.port;
    }
  }

  /*
  Interface to encapsulate Http and binary thrift server for
  HiveMetastore
   */
  private interface ThriftServer {
    public void start() throws Throwable;
    public boolean isRunning();
    public IHMSHandler getHandler();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Throwable {
    final Configuration conf = MetastoreConf.newMetastoreConf();
    shutdownHookMgr = ShutdownHookManager.get();

    HiveMetastoreCli cli = new HiveMetastoreCli(conf);
    cli.parse(args);
    final boolean isCliVerbose = cli.isVerbose();
    // NOTE: It is critical to do this prior to initializing log4j, otherwise
    // any log specific settings via hiveconf will be ignored
    Properties hiveconf = cli.addHiveconfToSystemProperties();

    // NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    try {
      // If the log4j.configuration property hasn't already been explicitly set,
      // use Hive's default log4j configuration
      if (System.getProperty("log4j.configurationFile") == null) {
        LogUtils.initHiveLog4j(conf);
      } else {
        //reconfigure log4j after settings via hiveconf are write into System Properties
        LoggerContext context =  (LoggerContext)LogManager.getContext(false);
        context.reconfigure();
      }
    } catch (LogUtils.LogInitializationException e) {
      LOG.warn(e.getMessage());
    }
    startupShutdownMessage(HiveMetaStore.class, args, LOG);

    try {
      String msg = "Starting hive metastore on port " + cli.port;
      LOG.info(msg);
      if (cli.isVerbose()) {
        System.err.println(msg);
      }

      // set all properties specified on the command line
      for (Map.Entry<Object, Object> item : hiveconf.entrySet()) {
        conf.set((String) item.getKey(), (String) item.getValue());
      }

      //for metastore process, all metastore call should be embedded metastore call.
      conf.set(ConfVars.THRIFT_URIS.getHiveName(), "");

      // Add shutdown hook.
      shutdownHookMgr.addShutdownHook(() -> {
        String shutdownMsg = "Shutting down hive metastore at " + getHostname();
        LOG.info(shutdownMsg);
        if (isCliVerbose) {
          System.err.println(shutdownMsg);
        }
        if (MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
          try {
            Metrics.shutdown();
          } catch (Exception e) {
            LOG.error("error in Metrics deinit: " + e.getClass().getName() + " "
                + e.getMessage(), e);
          }
        }
        // Remove from zookeeper if it's configured
        try {
          if (MetastoreConf.getVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE)
              .equalsIgnoreCase("zookeeper")) {
            zooKeeperHelper.removeServerInstanceFromZooKeeper();
          }
        } catch (Exception e) {
          LOG.error("Error removing znode for this metastore instance from ZooKeeper.", e);
        }
        ThreadPool.shutdown();
      }, 10);

      //Start Metrics for Standalone (Remote) Mode
      if (MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
        try {
          Metrics.initialize(conf);
        } catch (Exception e) {
          // log exception, but ignore inability to start
          LOG.error("error in Metrics init: " + e.getClass().getName() + " "
              + e.getMessage(), e);
        }
      }

      startMetaStore(cli.getPort(), HadoopThriftAuthBridge.getBridge(), conf, true, null);
    } catch (Throwable t) {
      // Catch the exception, log it and rethrow it.
      LOG.error("Metastore Thrift Server threw an exception...", t);
      throw t;
    }
  }

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}.
   *
   * @param port
   * @param bridge
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge)
      throws Throwable {
    startMetaStore(port, bridge, MetastoreConf.newMetastoreConf(), false, null);
  }

  /**
   * Start the metastore store.
   * @param port
   * @param bridge
   * @param conf
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge,
                                    Configuration conf) throws Throwable {
    startMetaStore(port, bridge, conf, false, null);
  }

  // TODO: Is it worth trying to use a server that supports HTTP/2?
  //  Does the Thrift http client support this?

  private static ThriftServer startHttpMetastore(int port, Configuration conf)
      throws Exception {
    LOG.info("Attempting to start http metastore server on port: {}", port);
    // login principal if security is enabled
    ServletSecurity.loginServerPincipal(conf);

    long maxMessageSize = MetastoreConf.getLongVar(conf, ConfVars.SERVER_MAX_MESSAGE_SIZE);
    int minWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MIN_THREADS);
    int maxWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MAX_THREADS);
    // Server thread pool
    // Start with minWorkerThreads, expand till maxWorkerThreads and reject
    // subsequent requests
    // TODO: Add a config for keepAlive time of threads ?
    ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads, 60L,
        TimeUnit.SECONDS, new SynchronousQueue<>(), r -> {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("Metastore-HttpHandler-Pool: Thread-" + thread.getId());
      return thread;
    }) {
      @Override
      public void setThreadFactory(ThreadFactory threadFactory) {
        // Avoid ExecutorThreadPool overriding the ThreadFactory
        LOG.warn("Ignore setting the thread factory as the pool has already provided his own: {}", getThreadFactory());
      }
    };
    ExecutorThreadPool threadPool = new ExecutorThreadPool((ThreadPoolExecutor) executorService);
    // HTTP Server
    org.eclipse.jetty.server.Server server = new Server(threadPool);
    server.setStopAtShutdown(true);

    ServerConnector connector;
    final HttpConfiguration httpServerConf = new HttpConfiguration();
    httpServerConf.setRequestHeaderSize(
        MetastoreConf.getIntVar(conf, ConfVars.METASTORE_THRIFT_HTTP_REQUEST_HEADER_SIZE));
    httpServerConf.setResponseHeaderSize(
        MetastoreConf.getIntVar(conf, ConfVars.METASTORE_THRIFT_HTTP_RESPONSE_HEADER_SIZE));

    final HttpConnectionFactory http = new HttpConnectionFactory(httpServerConf);

    final SslContextFactory sslContextFactory = ServletSecurity.createSslContextFactory(conf);
    if (sslContextFactory != null) {
      connector = new ServerConnector(server, sslContextFactory, http);
    } else {
      connector = new ServerConnector(server, http);
    }
    connector.setPort(port);
    connector.setReuseAddress(true);
    // TODO: What should the idle timeout be for the metastore? Currently it is 30 minutes
    long maxIdleTimeout = MetastoreConf.getTimeVar(conf, ConfVars.METASTORE_THRIFT_HTTP_MAX_IDLE_TIME,
        TimeUnit.MILLISECONDS);
    connector.setIdleTimeout(maxIdleTimeout);
    // TODO: AcceptQueueSize needs to be higher for HMS
    connector.setAcceptQueueSize(maxWorkerThreads);
    // TODO: Connection keepalive configuration?

    server.addConnector(connector);
    TProcessor processor;
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    final TProtocolFactory protocolFactory;
    if (useCompactProtocol) {
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      protocolFactory = new TBinaryProtocol.Factory();
    }

    HMSHandler baseHandler = new HMSHandler("new db based metaserver", conf);
    IHMSHandler handler = HMSHandlerProxyFactory.getProxy(conf, baseHandler, false);
    processor = new ThriftHiveMetastore.Processor<>(handler);
    LOG.info("Starting DB backed MetaStore Server with generic processor");
    TServlet thriftHttpServlet = new HmsThriftHttpServlet(processor, protocolFactory, conf);

    boolean directSqlEnabled = MetastoreConf.getBoolVar(conf, ConfVars.TRY_DIRECT_SQL);
    HMSHandler.LOG.info("Direct SQL optimization = {}",  directSqlEnabled);

    String httpPath =
        MetaStoreUtils.getHttpPath(
            MetastoreConf.getVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_HTTP_PATH));

    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SECURITY | ServletContextHandler.NO_SESSIONS);

    // Tons of stuff skipped as compared the HS2.
    // Sesions, XSRF, Compression, path configuration, etc.
    constraintHttpMethods(context, false);

    context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);
    // adding a listener on servlet request so as to clean up
    // rawStore when http request is completed
    context.addEventListener(new ServletRequestListener() {
      @Override
      public void requestDestroyed(ServletRequestEvent servletRequestEvent) {
        Request baseRequest = Request.getBaseRequest(servletRequestEvent.getServletRequest());
        HttpChannel channel = baseRequest.getHttpChannel();
        if (LOG.isDebugEnabled()) {
          LOG.debug("request: " + baseRequest + " destroyed " + ", http channel: " + channel);
        }
        HMSHandler.cleanupHandlerContext();
      }
      @Override
      public void requestInitialized(ServletRequestEvent servletRequestEvent) {
        Request baseRequest = Request.getBaseRequest(servletRequestEvent.getServletRequest());
        HttpChannel channel = baseRequest.getHttpChannel();
        if (LOG.isDebugEnabled()) {
          LOG.debug("request: " + baseRequest + " initialized " + ", http channel: " + channel);
        }
      }
    });
    server.setHandler(context);
    return new ThriftServer() {
      @Override
      public void start() throws Throwable {
        HMSHandler.LOG.debug("Starting HTTPServer for HMS");
        server.setStopAtShutdown(true);
        server.start();
        HMSHandler.LOG.info("Started the new HTTPServer for metastore on port [{}]...", port);
        HMSHandler.LOG.info("Options.minWorkerThreads = {}", minWorkerThreads);
        HMSHandler.LOG.info("Options.maxWorkerThreads = {}", maxWorkerThreads);
        HMSHandler.LOG.info("Enable SSL = {}", (sslContextFactory != null));
        server.join();
      }

      @Override
      public boolean isRunning() {
        return server != null && server.isRunning();
      }

      @Override
      public IHMSHandler getHandler() {
        return handler;
      }
    };
  }

  private static ThriftServer startBinaryMetastore(int port, HadoopThriftAuthBridge bridge,
      Configuration conf) throws Throwable {
    // Server will create new threads up to max as necessary. After an idle
    // period, it will destroy threads to keep the number of threads in the
    // pool to min.
    long maxMessageSize = MetastoreConf.getLongVar(conf, ConfVars.SERVER_MAX_MESSAGE_SIZE);
    int minWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MIN_THREADS);
    int maxWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MAX_THREADS);
    boolean tcpKeepAlive = MetastoreConf.getBoolVar(conf, ConfVars.TCP_KEEP_ALIVE);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    HMSHandler baseHandler = new HMSHandler("new db based metaserver", conf);
    AuthFactory authFactory = new AuthFactory(bridge, conf, baseHandler);
    useSasl = authFactory.isSASLWithKerberizedHadoop();

    if (useSasl) {
      // we are in secure mode. Login using keytab
      String kerberosName = SecurityUtil
          .getServerPrincipal(MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
      String keyTabFile = MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE);
      UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
      saslServer = authFactory.getSaslServer();
      delegationTokenManager = authFactory.getDelegationTokenManager();
    }

    TProcessor processor;
    TTransportFactory transFactory = authFactory.getAuthTransFactory(useSSL, conf);
    final TProtocolFactory protocolFactory;
    final TProtocolFactory inputProtoFactory;
    if (useCompactProtocol) {
      protocolFactory = new TCompactProtocol.Factory();
      inputProtoFactory = new TCompactProtocol.Factory(maxMessageSize, maxMessageSize);
    } else {
      protocolFactory = new TBinaryProtocol.Factory();
      inputProtoFactory = new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);
    }
    
    msHost = MetastoreConf.getVar(conf, ConfVars.THRIFT_BIND_HOST);
    if (msHost != null && !msHost.trim().isEmpty()) {
      LOG.info("Binding host " + msHost + " for metastore server");
    }
    
    IHMSHandler handler = HMSHandlerProxyFactory.getProxy(conf, baseHandler, false);
    TServerSocket serverSocket;
    if (useSasl) {
      processor = saslServer.wrapProcessor(
          new ThriftHiveMetastore.Processor<>(handler));
      LOG.info("Starting DB backed MetaStore Server in Secure Mode");
    } else {
      // we are in unsecure mode.
      if (MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
        processor = new TUGIBasedProcessor<>(handler);
        LOG.info("Starting DB backed MetaStore Server with SetUGI enabled");
      } else {
        processor = new TSetIpAddressProcessor<>(handler);
        LOG.info("Starting DB backed MetaStore Server");
      }
    }
    
    if (!useSSL) {
      serverSocket = SecurityUtils.getServerSocket(msHost, port);
    } else {
      String keyStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_KEYSTORE_PATH).trim();
      if (keyStorePath.isEmpty()) {
        throw new IllegalArgumentException(ConfVars.SSL_KEYSTORE_PATH.toString()
            + " Not configured for SSL connection");
      }
      String keyStorePassword =
          MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD);
      String keyStoreType =
          MetastoreConf.getVar(conf, ConfVars.SSL_KEYSTORE_TYPE).trim();
      String keyStoreAlgorithm =
          MetastoreConf.getVar(conf, ConfVars.SSL_KEYMANAGERFACTORY_ALGORITHM).trim();
      // enable SSL support for HMS
      List<String> sslVersionBlacklist = new ArrayList<>();
      for (String sslVersion : MetastoreConf.getVar(conf, ConfVars.SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }

      serverSocket = SecurityUtils.getServerSSLSocket(msHost, port, keyStorePath,
          keyStorePassword, keyStoreType, keyStoreAlgorithm, sslVersionBlacklist);
    }

    if (tcpKeepAlive) {
      serverSocket = new TServerSocketKeepAlive(serverSocket);
    }

    ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
        60L, TimeUnit.SECONDS, new SynchronousQueue<>(), r -> {
          Thread thread = new Thread(r);
          thread.setDaemon(true);
          thread.setName("Metastore-Handler-Pool: Thread-" + thread.getId());
          return thread;
    });

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
        .processor(processor)
        .transportFactory(transFactory)
        .protocolFactory(protocolFactory)
        .inputProtocolFactory(inputProtoFactory)
        .executorService(executorService);

    TServer tServer = new TThreadPoolServer(args);
    TServerEventHandler tServerEventHandler = new TServerEventHandler() {
      @Override
      public void preServe() {
      }

      @Override
      public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
        Metrics.getOpenConnectionsCounter().inc();
        return null;
      }

      @Override
      public void deleteContext(ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
        Metrics.getOpenConnectionsCounter().dec();
        // If the IMetaStoreClient#close was called, HMSHandler#shutdown would have already
        // cleaned up thread local RawStore. Otherwise, do it now.
        HMSHandler.cleanupHandlerContext();
      }

      @Override
      public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
      }
    };

    tServer.setServerEventHandler(tServerEventHandler);
    return new ThriftServer() {
      @Override
      public void start() throws Throwable {
        HMSHandler.LOG.info("Started the new metaserver on port [{}]...", port);
        HMSHandler.LOG.info("Options.minWorkerThreads = {}", minWorkerThreads);
        HMSHandler.LOG.info("Options.maxWorkerThreads = {}", maxWorkerThreads);
        HMSHandler.LOG.info("TCP keepalive = {}", tcpKeepAlive);
        HMSHandler.LOG.info("Enable SSL = {}", useSSL);
        tServer.serve();
      }

      @Override
      public boolean isRunning() {
        return tServer != null && tServer.isServing();
      }

      @Override
      public IHMSHandler getHandler() {
        return handler;
      }
    };
  }

  private static void constraintHttpMethods(ServletContextHandler ctxHandler, boolean allowOptionsMethod) {
    Constraint c = new Constraint();
    c.setAuthenticate(true);

    ConstraintMapping cmt = new ConstraintMapping();
    cmt.setConstraint(c);
    cmt.setMethod("TRACE");
    cmt.setPathSpec("/*");

    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
    if (!allowOptionsMethod) {
      ConstraintMapping cmo = new ConstraintMapping();
      cmo.setConstraint(c);
      cmo.setMethod("OPTIONS");
      cmo.setPathSpec("/*");
      securityHandler.setConstraintMappings(new ConstraintMapping[] {cmt, cmo});
    } else {
      securityHandler.setConstraintMappings(new ConstraintMapping[] {cmt});
    }
    ctxHandler.setSecurityHandler(securityHandler);
  }
  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}.
   *
   * @param port The port on which the Thrift server will start to serve
   * @param bridge
   * @param conf Configuration overrides
   * @param startMetaStoreThreads Start the background threads (initiator, cleaner, statsupdater, etc.)
   * @param startedBackgroundThreads If startMetaStoreThreads is true, this AtomicBoolean will be switched to true,
   *  when all of the background threads are scheduled. Useful for testing purposes to wait
   *  until the MetaStore is fully initialized.
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge,
      Configuration conf, boolean startMetaStoreThreads, AtomicBoolean startedBackgroundThreads) throws Throwable {
    isMetaStoreRemote = true;
    String transportMode = MetastoreConf.getVar(conf, ConfVars.THRIFT_TRANSPORT_MODE, "binary");
    boolean isHttpTransport = transportMode.equalsIgnoreCase("http");
    if (isHttpTransport) {
      thriftServer = startHttpMetastore(port, conf);
    } else {
      thriftServer = startBinaryMetastore(port, bridge, conf);
    }

    boolean directSqlEnabled = MetastoreConf.getBoolVar(conf, ConfVars.TRY_DIRECT_SQL);
    LOG.info("Direct SQL optimization = {}",  directSqlEnabled);

    if (startMetaStoreThreads) {
      Lock metaStoreThreadsLock = new ReentrantLock();
      Condition startCondition = metaStoreThreadsLock.newCondition();
      AtomicBoolean startedServing = new AtomicBoolean();
      startMetaStoreThreads(conf, metaStoreThreadsLock, startCondition, startedServing, startedBackgroundThreads);
      signalOtherThreadsToStart(thriftServer, metaStoreThreadsLock, startCondition, startedServing);
    }

    // If dynamic service discovery through ZooKeeper is enabled, add this server to the ZooKeeper.
    if (MetastoreConf.getVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE)
            .equalsIgnoreCase("zookeeper")) {
      try {
        zooKeeperHelper = MetastoreConf.getZKConfig(conf);
        String serverInstanceURI = getServerInstanceURI(port);
        zooKeeperHelper.addServerInstanceToZooKeeper(serverInstanceURI, serverInstanceURI, null,
            new ZKDeRegisterWatcher(zooKeeperHelper));
        LOG.info("Metastore server instance with URL " + serverInstanceURI + " added to " +
            "the zookeeper");
      } catch (Exception e) {
        LOG.error("Error adding this metastore instance to ZooKeeper: ", e);
        throw e;
      }
    }
    // optionally create and start the property server and servlet
    propertyServer = PropertyServlet.startServer(conf);

    thriftServer.start();
  }

  /**
   * @param port where metastore server is running
   * @return metastore server instance URL. If the metastore server was bound to a configured
   * host, return that appended by port. Otherwise return the externally visible URL of the local
   * host with the given port
   * @throws Exception
   */
  private static String getServerInstanceURI(int port) throws Exception {
    return getServerHostName() + ":" + port;
  }

  static String getServerHostName() throws Exception {
    if (msHost != null && !msHost.trim().isEmpty()) {
      return msHost.trim();
    } else {
      return InetAddress.getLocalHost().getCanonicalHostName();
    }
  }

  private static void signalOtherThreadsToStart(final ThriftServer thriftServer, final Lock startLock,
                                                final Condition startCondition,
                                                final AtomicBoolean startedServing) {
    // A simple thread to wait until the server has started and then signal the other threads to
    // begin
    Thread t = new Thread() {
      @Override
      public void run() {
        do {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn("Signalling thread was interrupted: " + e.getMessage());
          }
        } while (!thriftServer.isRunning());
        startLock.lock();
        try {
          startedServing.set(true);
          startCondition.signalAll();
        } finally {
          startLock.unlock();
        }
      }
    };
    t.setDaemon(true);
    t.start();
  }

  /**
   * Start threads outside of the thrift service, such as the compactor threads.
   * @param conf Hive configuration object
   */
  private static void startMetaStoreThreads(final Configuration conf, final Lock startLock,
      final Condition startCondition, final AtomicBoolean startedServing,
      final AtomicBoolean startedBackGroundThreads) {
    // A thread is spun up to start these other threads.  That's because we can't start them
    // until after the TServer has started, but once TServer.serve is called we aren't given back
    // control.
    Thread t = new Thread() {
      @Override
      public void run() {
        // This is a massive hack.  The compactor threads have to access packages in ql (such as
        // AcidInputFormat).  ql depends on metastore so we can't directly access those.  To deal
        // with this the compactor thread classes have been put in ql and they are instantiated here
        // dyanmically.  This is not ideal but it avoids a massive refactoring of Hive packages.
        //
        // Wrap the start of the threads in a catch Throwable loop so that any failures
        // don't doom the rest of the metastore.
        startLock.lock();
        try {
          JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(conf);
          pauseMonitor.start();
        } catch (Throwable t) {
          LOG.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
              "warned upon.", t);
        }

        try {
          // Per the javadocs on Condition, do not depend on the condition alone as a start gate
          // since spurious wake ups are possible.
          while (!startedServing.get()) {
            startCondition.await();
          }

         LeaderElectionContext context = new LeaderElectionContext.ContextBuilder(conf)
             .setHMSHandler(thriftServer.getHandler()).servHost(getServerHostName())
             .setTType(LeaderElectionContext.TTYPE.ALWAYS_TASKS) // always tasks
             .addListener(new HouseKeepingTasks(conf, false))
             .setTType(LeaderElectionContext.TTYPE.HOUSEKEEPING) // housekeeping tasks
             .addListener(new CMClearer(conf))
             .addListener(new StatsUpdaterTask(conf))
             .addListener(new CompactorTasks(conf, false))
             .addListener(new CompactorPMF())
             .addListener(new HouseKeepingTasks(conf, true))
             .setTType(LeaderElectionContext.TTYPE.WORKER) // compactor worker
             .addListener(new CompactorTasks(conf, true),
                 MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("metastore"))
             .build();
          if (shutdownHookMgr != null) {
            shutdownHookMgr.addShutdownHook(() -> context.close(), 0);
          }
          context.start();
        } catch (Throwable e) {
          LOG.error("Failure when starting the leader tasks, Compaction or Housekeeping tasks may not happen", e);
        } finally {
          startLock.unlock();
        }

        if (startedBackGroundThreads != null) {
          startedBackGroundThreads.set(true);
        }
      }
    };
    t.setDaemon(true);
    t.setName("Metastore threads starter thread");
    t.start();
  }

  /**
   * Print a log message for starting up and shutting down.
   * @param clazz the class of the server
   * @param args arguments
   * @param LOG the target log object
   */
  private static void startupShutdownMessage(Class<?> clazz, String[] args,
                                             final org.slf4j.Logger LOG) {
    final String hostname = getHostname();
    final String classname = clazz.getSimpleName();
    LOG.info(
        toStartupShutdownString("STARTUP_MSG: ", new String[] {
            "Starting " + classname,
            "  host = " + hostname,
            "  args = " + Arrays.asList(args),
            "  version = " + MetastoreVersionInfo.getVersion(),
            "  classpath = " + System.getProperty("java.class.path"),
            "  build = " + MetastoreVersionInfo.getUrl() + " -r "
                + MetastoreVersionInfo.getRevision()
                + "; compiled by '" + MetastoreVersionInfo.getUser()
                + "' on " + MetastoreVersionInfo.getDate()}
        )
    );
  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  private static String toStartupShutdownString(String prefix, String[] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(String s : msg) {
      b.append("\n")
          .append(prefix)
          .append(s);
    }
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Return hostname without throwing exception.
   * @return hostname
   */
  private static String getHostname() {
    try {
      return "" + InetAddress.getLocalHost();
    } catch(UnknownHostException uhe) {
      return "" + uhe;
    }
  }
}
