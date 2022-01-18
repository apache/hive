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

import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZKDeRegisterWatcher;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.StatsUpdateMode;
import org.apache.hadoop.hive.metastore.metrics.JvmPauseMonitor;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LogUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
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
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

  public static boolean isRenameAllowed(Database srcDB, Database destDB) {
    if (!srcDB.getName().equalsIgnoreCase(destDB.getName())) {
      if (ReplChangeManager.isSourceOfReplication(srcDB) || ReplChangeManager.isSourceOfReplication(destDB)) {
        return false;
      }
    }
    return true;
  }

  private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf)
      throws MetaException {
    return newRetryingHMSHandler(baseHandler, conf, false);
  }

  private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf,
      boolean local) throws MetaException {
    return RetryingHMSHandler.getProxy(conf, baseHandler, local);
  }

  /**
   * Create retrying HMS handler for embedded metastore.
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
  static Iface newRetryingHMSHandler(Configuration conf)
      throws MetaException {
    HMSHandler baseHandler = new HMSHandler("hive client", conf, false);
    return RetryingHMSHandler.getProxy(conf, baseHandler, true);
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
        String shutdownMsg = "Shutting down hive metastore.";
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
    // Server will create new threads up to max as necessary. After an idle
    // period, it will destroy threads to keep the number of threads in the
    // pool to min.
    long maxMessageSize = MetastoreConf.getLongVar(conf, ConfVars.SERVER_MAX_MESSAGE_SIZE);
    int minWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MIN_THREADS);
    int maxWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MAX_THREADS);
    boolean tcpKeepAlive = MetastoreConf.getBoolVar(conf, ConfVars.TCP_KEEP_ALIVE);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
    HMSHandler baseHandler = new HMSHandler("new db based metaserver", conf, false);
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
    IHMSHandler handler = newRetryingHMSHandler(baseHandler, conf);

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

    msHost = MetastoreConf.getVar(conf, ConfVars.THRIFT_BIND_HOST);
    if (msHost != null && !msHost.trim().isEmpty()) {
      LOG.info("Binding host " + msHost + " for metastore server");
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

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
        .processor(processor)
        .transportFactory(transFactory)
        .protocolFactory(protocolFactory)
        .inputProtocolFactory(inputProtoFactory)
        .minWorkerThreads(minWorkerThreads)
        .maxWorkerThreads(maxWorkerThreads);

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
        HMSHandler.cleanupRawStore();
      }

      @Override
      public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
      }
    };

    tServer.setServerEventHandler(tServerEventHandler);
    LOG.info("Started the new metaserver on port [" + port
        + "]...");
    LOG.info("Options.minWorkerThreads = "
        + minWorkerThreads);
    LOG.info("Options.maxWorkerThreads = "
        + maxWorkerThreads);
    LOG.info("TCP keepalive = " + tcpKeepAlive);
    LOG.info("Enable SSL = " + useSSL);
    logCompactionParameters(conf);

    boolean directSqlEnabled = MetastoreConf.getBoolVar(conf, ConfVars.TRY_DIRECT_SQL);
    LOG.info("Direct SQL optimization = {}",  directSqlEnabled);

    if (startMetaStoreThreads) {
      Lock metaStoreThreadsLock = new ReentrantLock();
      Condition startCondition = metaStoreThreadsLock.newCondition();
      AtomicBoolean startedServing = new AtomicBoolean();
      startMetaStoreThreads(conf, metaStoreThreadsLock, startCondition, startedServing,
                isMetastoreHousekeepingLeader(conf, getServerHostName()), startedBackgroundThreads);
      signalOtherThreadsToStart(tServer, metaStoreThreadsLock, startCondition, startedServing);
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

    tServer.serve();
  }

  private static void logCompactionParameters(Configuration conf) {
    LOG.info("Compaction HMS parameters:");
    LOG.info("metastore.compactor.initiator.on = {}", MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON));
    LOG.info("metastore.compactor.worker.threads = {}",
        MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_WORKER_THREADS));
    LOG.info("hive.metastore.runworker.in = {}", MetastoreConf.getVar(conf, ConfVars.HIVE_METASTORE_RUNWORKER_IN));
    LOG.info("metastore.compactor.history.retention.attempted = {}",
        MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE));
    LOG.info("metastore.compactor.history.retention.failed = {}",
        MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED));
    LOG.info("metastore.compactor.history.retention.succeeded = {}",
        MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED));
    LOG.info("metastore.compactor.initiator.failed.compacts.threshold = {}",
        MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD));
    LOG.info("metastore.compactor.enable.stats.compression",
        MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_MINOR_STATS_COMPRESSION));

    if (!MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
      LOG.warn("Compactor Initiator is turned Off. Automatic compaction will not be triggered.");
    }

    if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("metastore")) {
      int numThreads = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_WORKER_THREADS);
      if (numThreads < 1) {
        LOG.warn("Invalid number of Compactor Worker threads({}) on HMS", numThreads);
      }
    }
  }

  private static boolean isMetastoreHousekeepingLeader(Configuration conf, String serverHost) {
    String leaderHost =
            MetastoreConf.getVar(conf,
                    MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME);

    // For the sake of backward compatibility, when the current HMS becomes the leader when no
    // leader is specified.
    if (leaderHost == null || leaderHost.isEmpty()) {
      LOG.info(ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is empty. Start all the " +
              "housekeeping threads.");
      return true;
    }

    LOG.info(ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is set to " + leaderHost);
    return leaderHost.trim().equals(serverHost);
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

  private static String getServerHostName() throws Exception {
    if (msHost != null && !msHost.trim().isEmpty()) {
      return msHost.trim();
    } else {
      return InetAddress.getLocalHost().getHostName();
    }
  }

  private static void signalOtherThreadsToStart(final TServer server, final Lock startLock,
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
        } while (!server.isServing());
        startLock.lock();
        try {
          startedServing.set(true);
          startCondition.signalAll();
        } finally {
          startLock.unlock();
        }
      }
    };
    t.start();
  }

  /**
   * Start threads outside of the thrift service, such as the compactor threads.
   * @param conf Hive configuration object
   * @param isLeader true if this metastore is a leader. Most of the housekeeping threads are
   *                 started only in a leader HMS.
   */
  private static void startMetaStoreThreads(final Configuration conf, final Lock startLock,
      final Condition startCondition, final AtomicBoolean startedServing, boolean isLeader,
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

          if (isLeader) {
            startCompactorInitiator(conf);
            startCompactorCleaner(conf);
            startRemoteOnlyTasks(conf);
            startStatsUpdater(conf);
            HMSHandler.startAlwaysTaskThreads(conf);
          }

          // The leader HMS may not necessarily have sufficient compute capacity required to run
          // actual compaction work. So it can run on a non-leader HMS with sufficient capacity
          // or a configured HS2 instance.
          if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("metastore")) {
            startCompactorWorkers(conf);
          }
        } catch (Throwable e) {
          LOG.error("Failure when starting the compactor, compactions may not happen, " +
              StringUtils.stringifyException(e));
        } finally {
          startLock.unlock();
        }

        if (isLeader) {
          ReplChangeManager.scheduleCMClearer(conf);
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

  protected static void startStatsUpdater(Configuration conf) throws Exception {
    StatsUpdateMode mode = StatsUpdateMode.valueOf(
        MetastoreConf.getVar(conf, ConfVars.STATS_AUTO_UPDATE).toUpperCase());
    if (mode == StatsUpdateMode.NONE) {
      return;
    }
    MetaStoreThread t = instantiateThread("org.apache.hadoop.hive.ql.stats.StatsUpdaterThread");
    initializeAndStartThread(t, conf);
  }

  private static void startCompactorInitiator(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread initiator =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Initiator");
      initializeAndStartThread(initiator, conf);
      LOG.info("This HMS instance will act as a Compactor Initiator.");
    }
  }

  private static void startCompactorWorkers(Configuration conf) throws Exception {
    int numWorkers = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_WORKER_THREADS);
    for (int i = 0; i < numWorkers; i++) {
      MetaStoreThread worker =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Worker");
      initializeAndStartThread(worker, conf);
    }
    LOG.info("This HMS instance will act as a Compactor Worker with {} threads", numWorkers);
  }

  private static void startCompactorCleaner(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread cleaner =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Cleaner");
      initializeAndStartThread(cleaner, conf);
      LOG.info("This HMS instance will act as a Compactor Cleaner.");
    }
  }

  private static MetaStoreThread instantiateThread(String classname) throws Exception {
    Class<?> c = Class.forName(classname);
    Object o = c.newInstance();
    if (MetaStoreThread.class.isAssignableFrom(o.getClass())) {
      return (MetaStoreThread)o;
    } else {
      String s = classname + " is not an instance of MetaStoreThread.";
      LOG.error(s);
      throw new IOException(s);
    }
  }

  private static int nextThreadId = 1000000;

  private static void initializeAndStartThread(MetaStoreThread thread, Configuration conf) throws
      Exception {
    LOG.info("Starting metastore thread of type " + thread.getClass().getName());
    thread.setConf(conf);
    thread.setThreadId(nextThreadId++);
    thread.init(new AtomicBoolean());
    thread.start();
  }

  private static void startRemoteOnlyTasks(Configuration conf) throws Exception {
    if(!MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON)) {
      return;
    }

    ThreadPool.initialize(conf);
    Collection<String> taskNames =
        MetastoreConf.getStringCollection(conf, ConfVars.TASK_THREADS_REMOTE_ONLY);
    for (String taskName : taskNames) {
      MetastoreTaskThread task =
          JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
      task.setConf(conf);
      long freq = task.runFrequency(TimeUnit.MILLISECONDS);
      LOG.info("Scheduling for " + task.getClass().getCanonicalName() + " service.");
      ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
    }
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

    shutdownHookMgr.addShutdownHook(
        () -> LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
            "Shutting down " + classname + " at " + hostname})), 0);

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
