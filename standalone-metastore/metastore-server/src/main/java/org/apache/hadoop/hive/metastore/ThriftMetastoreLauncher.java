package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZKDeRegisterWatcher;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.JvmPauseMonitor;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class ThriftMetastoreLauncher implements MetastoreLauncher{

  public static final Logger LOG = LoggerFactory.getLogger(ThriftMetastoreLauncher.class);
  private Configuration conf;
  private HiveMetaStore.HiveMetastoreCli cli;

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}
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
    HiveMetaStore.isMetaStoreRemote = true;
    // Server will create new threads up to max as necessary. After an idle
    // period, it will destroy threads to keep the number of threads in the
    // pool to min.
    long maxMessageSize = MetastoreConf.getLongVar(conf, MetastoreConf.ConfVars.SERVER_MAX_MESSAGE_SIZE);
    int minWorkerThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.SERVER_MIN_THREADS);
    int maxWorkerThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.SERVER_MAX_THREADS);
    boolean tcpKeepAlive = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TCP_KEEP_ALIVE);
    boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
    boolean useSSL = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_SSL);
    HMSHandler baseHandler = new HMSHandler("new db based metaserver",
            conf,false);
    AuthFactory authFactory = new AuthFactory(bridge, conf, baseHandler);
    HiveMetaStore.useSasl = authFactory.isSASLWithKerberizedHadoop();

    if (HiveMetaStore.useSasl) {
      // we are in secure mode. Login using keytab
      String kerberosName = SecurityUtil
          .getServerPrincipal(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
      String keyTabFile = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.KERBEROS_KEYTAB_FILE);
      UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
      HiveMetaStore.saslServer = authFactory.getSaslServer();
      HiveMetaStore.delegationTokenManager = authFactory.getDelegationTokenManager();
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
    IHMSHandler handler = HiveMetaStore.newRetryingHMSHandler(baseHandler, conf);

    TServerSocket serverSocket;

    if (HiveMetaStore.useSasl) {
      processor = HiveMetaStore.saslServer.wrapProcessor(
        new ThriftHiveMetastore.Processor<>(handler));
      HiveMetaStore.LOG.info("Starting DB backed MetaStore Server in Secure Mode");
    } else {
      // we are in unsecure mode.
      if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI)) {
        processor = new TUGIBasedProcessor<>(handler);
        HiveMetaStore.LOG.info("Starting DB backed MetaStore Server with SetUGI enabled");
      } else {
        processor = new TSetIpAddressProcessor<>(handler);
        HiveMetaStore.LOG.info("Starting DB backed MetaStore Server");
      }
    }

    HiveMetaStore.msHost = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_BIND_HOST);
    if (HiveMetaStore.msHost != null && !HiveMetaStore.msHost.trim().isEmpty()) {
      HiveMetaStore.LOG.info("Binding host " + HiveMetaStore.msHost + " for metastore server");
    }

    if (!useSSL) {
      serverSocket = SecurityUtils.getServerSocket(HiveMetaStore.msHost, port);
    } else {
      String keyStorePath = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PATH).trim();
      if (keyStorePath.isEmpty()) {
        throw new IllegalArgumentException(MetastoreConf.ConfVars.SSL_KEYSTORE_PATH.toString()
            + " Not configured for SSL connection");
      }
      String keyStorePassword =
          MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD);
      String keyStoreType =
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_TYPE).trim();
      String keyStoreAlgorithm =
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_KEYMANAGERFACTORY_ALGORITHM).trim();
      // enable SSL support for HMS
      List<String> sslVersionBlacklist = new ArrayList<>();
      for (String sslVersion : MetastoreConf.getVar(conf, MetastoreConf.ConfVars.SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }

      serverSocket = SecurityUtils.getServerSSLSocket(HiveMetaStore.msHost, port, keyStorePath,
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
        cleanupRawStore();
      }

      @Override
      public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport1) {
      }
    };

    tServer.setServerEventHandler(tServerEventHandler);
    HMSHandler.LOG.info("Started the new metaserver on port [" + port
        + "]...");
    HMSHandler.LOG.info("Options.minWorkerThreads = "
        + minWorkerThreads);
    HMSHandler.LOG.info("Options.maxWorkerThreads = "
        + maxWorkerThreads);
    HMSHandler.LOG.info("TCP keepalive = " + tcpKeepAlive);
    HMSHandler.LOG.info("Enable SSL = " + useSSL);
    logCompactionParameters(conf);

    boolean directSqlEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.TRY_DIRECT_SQL);
    HMSHandler.LOG.info("Direct SQL optimization = {}",  directSqlEnabled);

    if (startMetaStoreThreads) {
      Lock metaStoreThreadsLock = new ReentrantLock();
      Condition startCondition = metaStoreThreadsLock.newCondition();
      AtomicBoolean startedServing = new AtomicBoolean();
      startMetaStoreThreads(conf, metaStoreThreadsLock, startCondition, startedServing,
                isMetastoreHousekeepingLeader(conf, getServerHostName()), startedBackgroundThreads);
      signalOtherThreadsToStart(tServer, metaStoreThreadsLock, startCondition, startedServing);
    }

    // If dynamic service discovery through ZooKeeper is enabled, add this server to the ZooKeeper.
    if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_SERVICE_DISCOVERY_MODE)
            .equalsIgnoreCase("zookeeper")) {
      try {
        HiveMetaStore.zooKeeperHelper = MetastoreConf.getZKConfig(conf);
        String serverInstanceURI = getServerInstanceURI(port);
        HiveMetaStore.zooKeeperHelper.addServerInstanceToZooKeeper(serverInstanceURI, serverInstanceURI, null,
            new ZKDeRegisterWatcher(HiveMetaStore.zooKeeperHelper));
        HMSHandler.LOG.info("Metastore server instance with URL " + serverInstanceURI + " added to " +
            "the zookeeper");
      } catch (Exception e) {
        HiveMetaStore.LOG.error("Error adding this metastore instance to ZooKeeper: ", e);
        throw e;
      }
    }

    tServer.serve();
  }

  private static void logCompactionParameters(Configuration conf) {
    HMSHandler.LOG.info("Compaction HMS parameters:");
    HMSHandler.LOG
        .info("metastore.compactor.initiator.on = {}", MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON));
    HMSHandler.LOG.info("metastore.compactor.worker.threads = {}",
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS));
    HMSHandler.LOG
        .info("hive.metastore.runworker.in = {}", MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN));
    HMSHandler.LOG.info("metastore.compactor.history.retention.attempted = {}",
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_ATTEMPTED));
    HMSHandler.LOG.info("metastore.compactor.history.retention.failed = {}",
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED));
    HMSHandler.LOG.info("metastore.compactor.history.retention.succeeded = {}",
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED));
    HMSHandler.LOG.info("metastore.compactor.initiator.failed.compacts.threshold = {}",
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD));
    HMSHandler.LOG.info("metastore.compactor.enable.stats.compression",
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_MINOR_STATS_COMPRESSION));

    if (!MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)) {
      HiveMetaStore.LOG.warn("Compactor Initiator is turned Off. Automatic compaction will not be triggered.");
    }

    if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("metastore")) {
      int numThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
      if (numThreads < 1) {
        HiveMetaStore.LOG.warn("Invalid number of Compactor Worker threads({}) on HMS", numThreads);
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
      HiveMetaStore.LOG.info(MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is empty. Start all the " +
              "housekeeping threads.");
      return true;
    }

    HiveMetaStore.LOG.info(MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME + " is set to " + leaderHost);
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
    if (HiveMetaStore.msHost != null && !HiveMetaStore.msHost.trim().isEmpty()) {
      return HiveMetaStore.msHost.trim();
    } else {
      return InetAddress.getLocalHost().getHostName();
    }
  }

  static void cleanupRawStore() {
    try {
      RawStore rs = HMSHandler.getRawStore();
      if (rs != null) {
        HMSHandler.logAndAudit("Cleaning up thread local RawStore...");
        rs.shutdown();
      }
    } finally {
      HMSHandler handler = HMSHandler.threadLocalHMSHandler.get();
      if (handler != null) {
        handler.notifyMetaListenersOnShutDown();
      }
      HMSHandler.threadLocalHMSHandler.remove();
      HMSHandler.threadLocalConf.remove();
      HMSHandler.threadLocalModifiedConfig.remove();
      HMSHandler.removeRawStore();
      HMSHandler.logAndAudit("Done cleaning up thread local RawStore");
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
            HiveMetaStore.LOG.warn("Signalling thread was interrupted: " + e.getMessage());
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
          HiveMetaStore.LOG.warn("Could not initiate the JvmPauseMonitor thread." + " GCs and Pauses may not be " +
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
          HiveMetaStore.LOG.error("Failure when starting the compactor, compactions may not happen, " +
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
    MetastoreConf.StatsUpdateMode mode = MetastoreConf.StatsUpdateMode.valueOf(
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.STATS_AUTO_UPDATE).toUpperCase());
    if (mode == MetastoreConf.StatsUpdateMode.NONE) return;
    MetaStoreThread t = instantiateThread("org.apache.hadoop.hive.ql.stats.StatsUpdaterThread");
    initializeAndStartThread(t, conf);
  }

  private static void startCompactorInitiator(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread initiator =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Initiator");
      initializeAndStartThread(initiator, conf);
      HiveMetaStore.LOG.info("This HMS instance will act as a Compactor Initiator.");
    }
  }

  private static void startCompactorWorkers(Configuration conf) throws Exception {
    int numWorkers = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
    for (int i = 0; i < numWorkers; i++) {
      MetaStoreThread worker =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Worker");
      initializeAndStartThread(worker, conf);
    }
    HiveMetaStore.LOG.info("This HMS instance will act as a Compactor Worker with {} threads", numWorkers);
  }

  private static void startCompactorCleaner(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread cleaner =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Cleaner");
      initializeAndStartThread(cleaner, conf);
      HiveMetaStore.LOG.info("This HMS instance will act as a Compactor Cleaner.");
    }
  }

  private static MetaStoreThread instantiateThread(String classname) throws Exception {
    Class<?> c = Class.forName(classname);
    Object o = c.newInstance();
    if (MetaStoreThread.class.isAssignableFrom(o.getClass())) {
      return (MetaStoreThread)o;
    } else {
      String s = classname + " is not an instance of MetaStoreThread.";
      HiveMetaStore.LOG.error(s);
      throw new IOException(s);
    }
  }

  private static void initializeAndStartThread(MetaStoreThread thread, Configuration conf) throws
      Exception {
    HiveMetaStore.LOG.info("Starting metastore thread of type " + thread.getClass().getName());
    thread.setConf(conf);
    thread.setThreadId(HiveMetaStore.nextThreadId++);
    thread.init(new AtomicBoolean());
    thread.start();
  }

  private static void startRemoteOnlyTasks(Configuration conf) throws Exception {
    if(!MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON)) {
      return;
    }

    ThreadPool.initialize(conf);
    Collection<String> taskNames =
        MetastoreConf.getStringCollection(conf, MetastoreConf.ConfVars.TASK_THREADS_REMOTE_ONLY);
    for (String taskName : taskNames) {
      MetastoreTaskThread task =
          JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
      task.setConf(conf);
      long freq = task.runFrequency(TimeUnit.MILLISECONDS);
      HiveMetaStore.LOG.info("Scheduling for " + task.getClass().getCanonicalName() + " service.");
      ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void run() {
    ZooKeeperHiveHelper zooKeeperHelper = MetastoreConf.getZKConfig(conf);
    ShutdownHookManager shutdownHookMgr = ShutdownHookManager.get();
    try {
      String msg = "Starting hive metastore on port " + cli.getPort();
      HMSHandler.LOG.info(msg);
      if (cli.isVerbose()) {
        System.err.println(msg);
      }

      // set all properties specified on the command line
      for (Map.Entry<Object, Object> item : cli.addHiveconfToSystemProperties().entrySet()) {
        conf.set((String) item.getKey(), (String) item.getValue());
      }

      //for metastore process, all metastore call should be embedded metastore call.
      conf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), "");

      // Add shutdown hook.
      shutdownHookMgr.addShutdownHook(() -> {
        String shutdownMsg = "Shutting down hive metastore.";
        HMSHandler.LOG.info(shutdownMsg);
        if (cli.isVerbose()) {
          System.err.println(shutdownMsg);
        }
        if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED)) {
          try {
            Metrics.shutdown();
          } catch (Exception e) {
            LOG.error("error in Metrics deinit: " + e.getClass().getName() + " "
                + e.getMessage(), e);
          }
        }
        // Remove from zookeeper if it's configured
        try {
          if (MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_SERVICE_DISCOVERY_MODE)
              .equalsIgnoreCase("zookeeper")) {
            zooKeeperHelper.removeServerInstanceFromZooKeeper();
          }
        } catch (Exception e) {
          LOG.error("Error removing znode for this metastore instance from ZooKeeper.", e);
        }
        ThreadPool.shutdown();
      }, 10);

      //Start Metrics for Standalone (Remote) Mode
      if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED)) {
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
      HMSHandler.LOG
          .error("Metastore Thrift Server threw an exception...", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public MetastoreLauncher configure(Configuration conf, HiveMetaStore.HiveMetastoreCli cli) {
    this.conf = conf;
    this.cli = cli;
    return this;
  }
}
