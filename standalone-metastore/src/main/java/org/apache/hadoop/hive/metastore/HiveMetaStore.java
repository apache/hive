/* * Licensed to the Apache Software Foundation (ASF) under one
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

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependNotNullCatToDbName;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import javax.jdo.JDOException;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropConstraintEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAuthorizationCallEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreAddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.PreReadCatalogEvent;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadISchemaEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.events.PreReadhSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.metrics.JvmPauseMonitor;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.security.TUGIContainingTransport;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.hive.metastore.utils.CommonCliOptions;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LogUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * TODO:pc remove application logic to a separate interface.
 */
public class HiveMetaStore extends ThriftHiveMetastore {
  public static final Logger LOG = LoggerFactory.getLogger(HiveMetaStore.class);
  public static final String PARTITION_NUMBER_EXCEED_LIMIT_MSG =
      "Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.";

  // boolean that tells if the HiveMetaStore (remote) server is being used.
  // Can be used to determine if the calls to metastore api (HMSHandler) are being made with
  // embedded metastore or a remote one
  private static boolean isMetaStoreRemote = false;

  // Used for testing to simulate method timeout.
  @VisibleForTesting
  static boolean TEST_TIMEOUT_ENABLED = false;
  @VisibleForTesting
  static long TEST_TIMEOUT_VALUE = -1;

  private static ShutdownHookManager shutdownHookMgr;

  public static final String ADMIN = "admin";
  public static final String PUBLIC = "public";
  /** MM write states. */
  public static final char MM_WRITE_OPEN = 'o', MM_WRITE_COMMITTED = 'c', MM_WRITE_ABORTED = 'a';

  private static HadoopThriftAuthBridge.Server saslServer;
  private static MetastoreDelegationTokenManager delegationTokenManager;
  private static boolean useSasl;

  static final String NO_FILTER_STRING = "";
  static final int UNLIMITED_MAX_PARTITIONS = -1;

  private static final class ChainedTTransportFactory extends TTransportFactory {
    private final TTransportFactory parentTransFactory;
    private final TTransportFactory childTransFactory;

    private ChainedTTransportFactory(
        TTransportFactory parentTransFactory,
        TTransportFactory childTransFactory) {
      this.parentTransFactory = parentTransFactory;
      this.childTransFactory = childTransFactory;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return childTransFactory.getTransport(parentTransFactory.getTransport(trans));
    }
  }

  public static class HMSHandler extends FacebookBase implements IHMSHandler {
    public static final Logger LOG = HiveMetaStore.LOG;
    private final Configuration conf; // stores datastore (jpox) properties,
                                     // right now they come from jpox.properties

    private static String currentUrl;
    private FileMetadataManager fileMetadataManager;
    private PartitionExpressionProxy expressionProxy;
    private StorageSchemaReader storageSchemaReader;

    // Variables for metrics
    // Package visible so that HMSMetricsListener can see them.
    static AtomicInteger databaseCount, tableCount, partCount;

    private Warehouse wh; // hdfs warehouse
    private static final ThreadLocal<RawStore> threadLocalMS =
        new ThreadLocal<RawStore>() {
          @Override
          protected RawStore initialValue() {
            return null;
          }
        };

    private static final ThreadLocal<TxnStore> threadLocalTxn = new ThreadLocal<TxnStore>() {
      @Override
      protected TxnStore initialValue() {
        return null;
      }
    };

    private static final ThreadLocal<Map<String, com.codahale.metrics.Timer.Context>> timerContexts =
        new ThreadLocal<Map<String, com.codahale.metrics.Timer.Context>>() {
      @Override
      protected Map<String, com.codahale.metrics.Timer.Context> initialValue() {
        return new HashMap<>();
      }
    };

    public static RawStore getRawStore() {
      return threadLocalMS.get();
    }

    static void removeRawStore() {
      threadLocalMS.remove();
    }

    // Thread local configuration is needed as many threads could make changes
    // to the conf using the connection hook
    private static final ThreadLocal<Configuration> threadLocalConf =
        new ThreadLocal<Configuration>() {
          @Override
          protected Configuration initialValue() {
            return null;
          }
        };

    /**
     * Thread local HMSHandler used during shutdown to notify meta listeners
     */
    private static final ThreadLocal<HMSHandler> threadLocalHMSHandler = new ThreadLocal<>();

    /**
     * Thread local Map to keep track of modified meta conf keys
     */
    private static final ThreadLocal<Map<String, String>> threadLocalModifiedConfig =
        new ThreadLocal<>();

    private static ExecutorService threadPool;

    static final Logger auditLog = LoggerFactory.getLogger(
        HiveMetaStore.class.getName() + ".audit");

    private static void logAuditEvent(String cmd) {
      if (cmd == null) {
        return;
      }

      UserGroupInformation ugi;
      try {
        ugi = SecurityUtils.getUGI();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      String address = getIPAddress();
      if (address == null) {
        address = "unknown-ip-addr";
      }

      auditLog.info("ugi={}	ip={}	cmd={}	", ugi.getUserName(), address, cmd);
    }

    private static String getIPAddress() {
      if (useSasl) {
        if (saslServer != null && saslServer.getRemoteAddress() != null) {
          return saslServer.getRemoteAddress().getHostAddress();
        }
      } else {
        // if kerberos is not enabled
        return getThreadLocalIpAddress();
      }
      return null;
    }

    private static int nextSerialNum = 0;
    private static ThreadLocal<Integer> threadLocalId = new ThreadLocal<Integer>() {
      @Override
      protected Integer initialValue() {
        return nextSerialNum++;
      }
    };

    // This will only be set if the metastore is being accessed from a metastore Thrift server,
    // not if it is from the CLI. Also, only if the TTransport being used to connect is an
    // instance of TSocket. This is also not set when kerberos is used.
    private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
      @Override
      protected String initialValue() {
        return null;
      }
    };

    /**
     * Internal function to notify listeners for meta config change events
     */
    private void notifyMetaListeners(String key, String oldValue, String newValue) throws MetaException {
      for (MetaStoreEventListener listener : listeners) {
        listener.onConfigChange(new ConfigChangeEvent(this, key, oldValue, newValue));
      }

      if (transactionalListeners.size() > 0) {
        // All the fields of this event are final, so no reason to create a new one for each
        // listener
        ConfigChangeEvent cce = new ConfigChangeEvent(this, key, oldValue, newValue);
        for (MetaStoreEventListener transactionalListener : transactionalListeners) {
          transactionalListener.onConfigChange(cce);
        }
      }
    }

    /**
     * Internal function to notify listeners to revert back to old values of keys
     * that were modified during setMetaConf. This would get called from HiveMetaStore#cleanupRawStore
     */
    private void notifyMetaListenersOnShutDown() {
      Map<String, String> modifiedConf = threadLocalModifiedConfig.get();
      if (modifiedConf == null) {
        // Nothing got modified
        return;
      }
      try {
        Configuration conf = threadLocalConf.get();
        if (conf == null) {
          throw new MetaException("Unexpected: modifiedConf is non-null but conf is null");
        }
        // Notify listeners of the changed value
        for (Entry<String, String> entry : modifiedConf.entrySet()) {
          String key = entry.getKey();
          // curr value becomes old and vice-versa
          String currVal = entry.getValue();
          String oldVal = conf.get(key);
          if (!Objects.equals(oldVal, currVal)) {
            notifyMetaListeners(key, oldVal, currVal);
          }
        }
        logInfo("Meta listeners shutdown notification completed.");
      } catch (MetaException e) {
        LOG.error("Failed to notify meta listeners on shutdown: ", e);
      }
    }

    static void setThreadLocalIpAddress(String ipAddress) {
      threadLocalIpAddress.set(ipAddress);
    }

    // This will return null if the metastore is not being accessed from a metastore Thrift server,
    // or if the TTransport being used to connect is not an instance of TSocket, or if kereberos
    // is used
    static String getThreadLocalIpAddress() {
      return threadLocalIpAddress.get();
    }

    // Make it possible for tests to check that the right type of PartitionExpressionProxy was
    // instantiated.
    @VisibleForTesting
    PartitionExpressionProxy getExpressionProxy() {
      return expressionProxy;
    }

    /**
     * Use {@link #getThreadId()} instead.
     * @return thread id
     */
    @Deprecated
    public static Integer get() {
      return threadLocalId.get();
    }

    @Override
    public int getThreadId() {
      return threadLocalId.get();
    }

    public HMSHandler(String name) throws MetaException {
      this(name, MetastoreConf.newMetastoreConf(), true);
    }

    public HMSHandler(String name, Configuration conf) throws MetaException {
      this(name, conf, true);
    }

    public HMSHandler(String name, Configuration conf, boolean init) throws MetaException {
      super(name);
      this.conf = conf;
      isInTest = MetastoreConf.getBoolVar(this.conf, ConfVars.HIVE_IN_TEST);
      if (threadPool == null) {
        synchronized (HMSHandler.class) {
          int numThreads = MetastoreConf.getIntVar(conf, ConfVars.FS_HANDLER_THREADS_COUNT);
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true)
                  .setNameFormat("HMSHandler #%d").build());
        }
      }
      if (init) {
        init();
      }
    }

    /**
     * Use {@link #getConf()} instead.
     * @return Configuration object
     */
    @Deprecated
    public Configuration getHiveConf() {
      return conf;
    }

    private ClassLoader classLoader;
    private AlterHandler alterHandler;
    private List<MetaStorePreEventListener> preListeners;
    private List<MetaStoreEventListener> listeners;
    private List<TransactionalMetaStoreEventListener> transactionalListeners;
    private List<MetaStoreEndFunctionListener> endFunctionListeners;
    private List<MetaStoreInitListener> initListeners;
    private Pattern partitionValidationPattern;
    private final boolean isInTest;

    {
      classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = Configuration.class.getClassLoader();
      }
    }

    @Override
    public List<TransactionalMetaStoreEventListener> getTransactionalListeners() {
      return transactionalListeners;
    }

    @Override
    public List<MetaStoreEventListener> getListeners() {
      return listeners;
    }

    @Override
    public void init() throws MetaException {
      initListeners = MetaStoreUtils.getMetaStoreListeners(
          MetaStoreInitListener.class, conf, MetastoreConf.getVar(conf, ConfVars.INIT_HOOKS));
      for (MetaStoreInitListener singleInitListener: initListeners) {
          MetaStoreInitContext context = new MetaStoreInitContext();
          singleInitListener.onInit(context);
      }

      String alterHandlerName = MetastoreConf.getVar(conf, ConfVars.ALTER_HANDLER);
      alterHandler = ReflectionUtils.newInstance(JavaUtils.getClass(
          alterHandlerName, AlterHandler.class), conf);
      wh = new Warehouse(conf);

      synchronized (HMSHandler.class) {
        if (currentUrl == null || !currentUrl.equals(MetaStoreInit.getConnectionURL(conf))) {
          createDefaultDB();
          createDefaultRoles();
          addAdminUsers();
          currentUrl = MetaStoreInit.getConnectionURL(conf);
        }
      }

      //Start Metrics
      if (MetastoreConf.getBoolVar(conf, ConfVars.METRICS_ENABLED)) {
        LOG.info("Begin calculating metadata count metrics.");
        Metrics.initialize(conf);
        databaseCount = Metrics.getOrCreateGauge(MetricsConstants.TOTAL_DATABASES);
        tableCount = Metrics.getOrCreateGauge(MetricsConstants.TOTAL_TABLES);
        partCount = Metrics.getOrCreateGauge(MetricsConstants.TOTAL_PARTITIONS);
        updateMetrics();

      }

      preListeners = MetaStoreUtils.getMetaStoreListeners(MetaStorePreEventListener.class,
          conf, MetastoreConf.getVar(conf, ConfVars.PRE_EVENT_LISTENERS));
      preListeners.add(0, new TransactionalValidationListener(conf));
      listeners = MetaStoreUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf,
          MetastoreConf.getVar(conf, ConfVars.EVENT_LISTENERS));
      listeners.add(new SessionPropertiesListener(conf));
      listeners.add(new AcidEventListener(conf));
      transactionalListeners = MetaStoreUtils.getMetaStoreListeners(TransactionalMetaStoreEventListener.class,
          conf, MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
      if (Metrics.getRegistry() != null) {
        listeners.add(new HMSMetricsListener(conf));
      }

      endFunctionListeners = MetaStoreUtils.getMetaStoreListeners(
          MetaStoreEndFunctionListener.class, conf, MetastoreConf.getVar(conf, ConfVars.END_FUNCTION_LISTENERS));

      String partitionValidationRegex =
          MetastoreConf.getVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
      if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
        partitionValidationPattern = Pattern.compile(partitionValidationRegex);
      } else {
        partitionValidationPattern = null;
      }

      ThreadPool.initialize(conf);
      Collection<String> taskNames =
          MetastoreConf.getStringCollection(conf, ConfVars.TASK_THREADS_ALWAYS);
      for (String taskName : taskNames) {
        MetastoreTaskThread task =
            JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
        task.setConf(conf);
        long freq = task.runFrequency(TimeUnit.MILLISECONDS);
        // For backwards compatibility, since some threads used to be hard coded but only run if
        // frequency was > 0
        if (freq > 0) {
          ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);

        }
      }
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
      fileMetadataManager = new FileMetadataManager(this.getMS(), conf);
    }

    private static String addPrefix(String s) {
      return threadLocalId.get() + ": " + s;
    }

    /**
     * Set copy of invoking HMSHandler on thread local
     */
    private static void setHMSHandler(HMSHandler handler) {
      if (threadLocalHMSHandler.get() == null) {
        threadLocalHMSHandler.set(handler);
      }
    }
    @Override
    public void setConf(Configuration conf) {
      threadLocalConf.set(conf);
      RawStore ms = threadLocalMS.get();
      if (ms != null) {
        ms.setConf(conf); // reload if DS related configuration is changed
      }
    }

    @Override
    public Configuration getConf() {
      Configuration conf = threadLocalConf.get();
      if (conf == null) {
        conf = new Configuration(this.conf);
        threadLocalConf.set(conf);
      }
      return conf;
    }

    private Map<String, String> getModifiedConf() {
      Map<String, String> modifiedConf = threadLocalModifiedConfig.get();
      if (modifiedConf == null) {
        modifiedConf = new HashMap<>();
        threadLocalModifiedConfig.set(modifiedConf);
      }
      return modifiedConf;
    }

    @Override
    public Warehouse getWh() {
      return wh;
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException {
      ConfVars confVar = MetastoreConf.getMetaConf(key);
      if (confVar == null) {
        throw new MetaException("Invalid configuration key " + key);
      }
      try {
        confVar.validate(value);
      } catch (IllegalArgumentException e) {
        throw new MetaException("Invalid configuration value " + value + " for key " + key +
            " by " + e.getMessage());
      }
      Configuration configuration = getConf();
      String oldValue = MetastoreConf.get(configuration, key);
      // Save prev val of the key on threadLocal
      Map<String, String> modifiedConf = getModifiedConf();
      if (!modifiedConf.containsKey(key)) {
        modifiedConf.put(key, oldValue);
      }
      // Set invoking HMSHandler on threadLocal, this will be used later to notify
      // metaListeners in HiveMetaStore#cleanupRawStore
      setHMSHandler(this);
      configuration.set(key, value);
      notifyMetaListeners(key, oldValue, value);
    }

    @Override
    public String getMetaConf(String key) throws MetaException {
      ConfVars confVar = MetastoreConf.getMetaConf(key);
      if (confVar == null) {
        throw new MetaException("Invalid configuration key " + key);
      }
      return getConf().get(key, confVar.getDefaultVal().toString());
    }

    /**
     * Get a cached RawStore.
     *
     * @return the cached RawStore
     * @throws MetaException
     */
    @Override
    public RawStore getMS() throws MetaException {
      Configuration conf = getConf();
      return getMSForConf(conf);
    }

    public static RawStore getMSForConf(Configuration conf) throws MetaException {
      RawStore ms = threadLocalMS.get();
      if (ms == null) {
        ms = newRawStoreForConf(conf);
        ms.verifySchema();
        threadLocalMS.set(ms);
        ms = threadLocalMS.get();
      }
      return ms;
    }

    @Override
    public TxnStore getTxnHandler() {
      TxnStore txn = threadLocalTxn.get();
      if (txn == null) {
        txn = TxnUtils.getTxnStore(conf);
        threadLocalTxn.set(txn);
      }
      return txn;
    }

    static RawStore newRawStoreForConf(Configuration conf) throws MetaException {
      Configuration newConf = new Configuration(conf);
      String rawStoreClassName = MetastoreConf.getVar(newConf, ConfVars.RAW_STORE_IMPL);
      LOG.info(addPrefix("Opening raw store with implementation class:" + rawStoreClassName));
      return RawStoreProxy.getProxy(newConf, conf, rawStoreClassName, threadLocalId.get());
    }

    @VisibleForTesting
    public static void createDefaultCatalog(RawStore ms, Warehouse wh) throws MetaException,
        InvalidOperationException {
      try {
        Catalog defaultCat = ms.getCatalog(DEFAULT_CATALOG_NAME);
        // Null check because in some test cases we get a null from ms.getCatalog.
        if (defaultCat !=null && defaultCat.getLocationUri().equals("TBD")) {
          // One time update issue.  When the new 'hive' catalog is created in an upgrade the
          // script does not know the location of the warehouse.  So we need to update it.
          LOG.info("Setting location of default catalog, as it hasn't been done after upgrade");
          defaultCat.setLocationUri(wh.getWhRoot().toString());
          ms.alterCatalog(defaultCat.getName(), defaultCat);
        }

      } catch (NoSuchObjectException e) {
        Catalog cat = new Catalog(DEFAULT_CATALOG_NAME, wh.getWhRoot().toString());
        cat.setDescription(Warehouse.DEFAULT_CATALOG_COMMENT);
        ms.createCatalog(cat);
      }
    }

    private void createDefaultDB_core(RawStore ms) throws MetaException, InvalidObjectException {
      try {
        ms.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
      } catch (NoSuchObjectException e) {
        Database db = new Database(DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_COMMENT,
          wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString(), null);
        db.setOwnerName(PUBLIC);
        db.setOwnerType(PrincipalType.ROLE);
        db.setCatalogName(DEFAULT_CATALOG_NAME);
        ms.createDatabase(db);
      }
    }

    /**
     * create default database if it doesn't exist.
     *
     * This is a potential contention when HiveServer2 using embedded metastore and Metastore
     * Server try to concurrently invoke createDefaultDB. If one failed, JDOException was caught
     * for one more time try, if failed again, simply ignored by warning, which meant another
     * succeeds.
     *
     * @throws MetaException
     */
    private void createDefaultDB() throws MetaException {
      try {
        RawStore ms = getMS();
        createDefaultCatalog(ms, wh);
        createDefaultDB_core(ms);
      } catch (JDOException e) {
        LOG.warn("Retrying creating default database after error: " + e.getMessage(), e);
        try {
          createDefaultDB_core(getMS());
        } catch (InvalidObjectException e1) {
          throw new MetaException(e1.getMessage());
        }
      } catch (InvalidObjectException|InvalidOperationException e) {
        throw new MetaException(e.getMessage());
      }
    }

    /**
     * create default roles if they don't exist.
     *
     * This is a potential contention when HiveServer2 using embedded metastore and Metastore
     * Server try to concurrently invoke createDefaultRoles. If one failed, JDOException was caught
     * for one more time try, if failed again, simply ignored by warning, which meant another
     * succeeds.
     *
     * @throws MetaException
     */
    private void createDefaultRoles() throws MetaException {
      try {
        createDefaultRoles_core();
      } catch (JDOException e) {
        LOG.warn("Retrying creating default roles after error: " + e.getMessage(), e);
        createDefaultRoles_core();
      }
    }

    private void createDefaultRoles_core() throws MetaException {

      RawStore ms = getMS();
      try {
        ms.addRole(ADMIN, ADMIN);
      } catch (InvalidObjectException e) {
        LOG.debug(ADMIN +" role already exists",e);
      } catch (NoSuchObjectException e) {
        // This should never be thrown.
        LOG.warn("Unexpected exception while adding " +ADMIN+" roles" , e);
      }
      LOG.info("Added "+ ADMIN+ " role in metastore");
      try {
        ms.addRole(PUBLIC, PUBLIC);
      } catch (InvalidObjectException e) {
        LOG.debug(PUBLIC + " role already exists",e);
      } catch (NoSuchObjectException e) {
        // This should never be thrown.
        LOG.warn("Unexpected exception while adding "+PUBLIC +" roles" , e);
      }
      LOG.info("Added "+PUBLIC+ " role in metastore");
      // now grant all privs to admin
      PrivilegeBag privs = new PrivilegeBag();
      privs.addToPrivileges(new HiveObjectPrivilege( new HiveObjectRef(HiveObjectType.GLOBAL, null,
        null, null, null), ADMIN, PrincipalType.ROLE, new PrivilegeGrantInfo("All", 0, ADMIN,
        PrincipalType.ROLE, true)));
      try {
        ms.grantPrivileges(privs);
      } catch (InvalidObjectException e) {
        // Surprisingly these privs are already granted.
        LOG.debug("Failed while granting global privs to admin", e);
      } catch (NoSuchObjectException e) {
        // Unlikely to be thrown.
        LOG.warn("Failed while granting global privs to admin", e);
      }
    }

    /**
     * add admin users if they don't exist.
     *
     * This is a potential contention when HiveServer2 using embedded metastore and Metastore
     * Server try to concurrently invoke addAdminUsers. If one failed, JDOException was caught for
     * one more time try, if failed again, simply ignored by warning, which meant another succeeds.
     *
     * @throws MetaException
     */
    private void addAdminUsers() throws MetaException {
      try {
        addAdminUsers_core();
      } catch (JDOException e) {
        LOG.warn("Retrying adding admin users after error: " + e.getMessage(), e);
        addAdminUsers_core();
      }
    }

    private void addAdminUsers_core() throws MetaException {

      // now add pre-configured users to admin role
      String userStr = MetastoreConf.getVar(conf,ConfVars.USERS_IN_ADMIN_ROLE,"").trim();
      if (userStr.isEmpty()) {
        LOG.info("No user is added in admin role, since config is empty");
        return;
      }
      // Since user names need to be valid unix user names, per IEEE Std 1003.1-2001 they cannot
      // contain comma, so we can safely split above string on comma.

     Iterator<String> users = Splitter.on(",").trimResults().omitEmptyStrings().split(userStr).iterator();
      if (!users.hasNext()) {
        LOG.info("No user is added in admin role, since config value "+ userStr +
          " is in incorrect format. We accept comma separated list of users.");
        return;
      }
      Role adminRole;
      RawStore ms = getMS();
      try {
        adminRole = ms.getRole(ADMIN);
      } catch (NoSuchObjectException e) {
        LOG.error("Failed to retrieve just added admin role",e);
        return;
      }
      while (users.hasNext()) {
        String userName = users.next();
        try {
          ms.grantRole(adminRole, userName, PrincipalType.USER, ADMIN, PrincipalType.ROLE, true);
          LOG.info("Added " + userName + " to admin role");
        } catch (NoSuchObjectException e) {
          LOG.error("Failed to add "+ userName + " in admin role",e);
        } catch (InvalidObjectException e) {
          LOG.debug(userName + " already in admin role", e);
        }
      }
    }

    private static void logInfo(String m) {
      LOG.info(threadLocalId.get().toString() + ": " + m);
      logAuditEvent(m);
    }

    private String startFunction(String function, String extraLogInfo) {
      incrementCounter(function);
      logInfo((getThreadLocalIpAddress() == null ? "" : "source:" + getThreadLocalIpAddress() + " ") +
          function + extraLogInfo);
      com.codahale.metrics.Timer timer =
          Metrics.getOrCreateTimer(MetricsConstants.API_PREFIX + function);
      if (timer != null) {
        // Timer will be null we aren't using the metrics
        timerContexts.get().put(function, timer.time());
      }
      Counter counter = Metrics.getOrCreateCounter(MetricsConstants.ACTIVE_CALLS + function);
      if (counter != null) {
        counter.inc();
      }
      return function;
    }

    private String startFunction(String function) {
      return startFunction(function, "");
    }

    private void startTableFunction(String function, String catName, String db, String tbl) {
      startFunction(function, " : tbl=" +
          getCatalogQualifiedTableName(catName, db, tbl));
    }

    private void startMultiTableFunction(String function, String db, List<String> tbls) {
      String tableNames = join(tbls, ",");
      startFunction(function, " : db=" + db + " tbls=" + tableNames);
    }

    private void startPartitionFunction(String function, String cat, String db, String tbl,
                                        List<String> partVals) {
      startFunction(function, " : tbl=" +
          getCatalogQualifiedTableName(cat, db, tbl) + "[" + join(partVals, ",") + "]");
    }

    private void startPartitionFunction(String function, String catName, String db, String tbl,
                                        Map<String, String> partName) {
      startFunction(function, " : tbl=" +
          getCatalogQualifiedTableName(catName, db, tbl) + "partition=" + partName);
    }

    private void endFunction(String function, boolean successful, Exception e) {
      endFunction(function, successful, e, null);
    }
    private void endFunction(String function, boolean successful, Exception e,
                            String inputTableName) {
      endFunction(function, new MetaStoreEndFunctionContext(successful, e, inputTableName));
    }

    private void endFunction(String function, MetaStoreEndFunctionContext context) {
      com.codahale.metrics.Timer.Context timerContext = timerContexts.get().remove(function);
      if (timerContext != null) {
        timerContext.close();
      }
      Counter counter = Metrics.getOrCreateCounter(MetricsConstants.ACTIVE_CALLS + function);
      if (counter != null) {
        counter.dec();
      }

      for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
        listener.onEndFunction(function, context);
      }
    }

    @Override
    public fb_status getStatus() {
      return fb_status.ALIVE;
    }

    @Override
    public void shutdown() {
      cleanupRawStore();
      PerfLogger.getPerfLogger(false).cleanupPerfLogMetrics();
    }

    @Override
    public AbstractMap<String, Long> getCounters() {
      AbstractMap<String, Long> counters = super.getCounters();

      // Allow endFunctionListeners to add any counters they have collected
      if (endFunctionListeners != null) {
        for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
          listener.exportCounters(counters);
        }
      }

      return counters;
    }

    @Override
    public void create_catalog(CreateCatalogRequest rqst)
        throws AlreadyExistsException, InvalidObjectException, MetaException {
      Catalog catalog = rqst.getCatalog();
      startFunction("create_catalog", ": " + catalog.toString());
      boolean success = false;
      Exception ex = null;
      try {
        try {
          getMS().getCatalog(catalog.getName());
          throw new AlreadyExistsException("Catalog " + catalog.getName() + " already exists");
        } catch (NoSuchObjectException e) {
          // expected
        }

        if (!MetaStoreUtils.validateName(catalog.getName(), null)) {
          throw new InvalidObjectException(catalog.getName() + " is not a valid catalog name");
        }

        if (catalog.getLocationUri() == null) {
          throw new InvalidObjectException("You must specify a path for the catalog");
        }

        RawStore ms = getMS();
        Path catPath = new Path(catalog.getLocationUri());
        boolean madeDir = false;
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        try {
          firePreEvent(new PreCreateCatalogEvent(this, catalog));
          if (!wh.isDir(catPath)) {
            if (!wh.mkdirs(catPath)) {
              throw new MetaException("Unable to create catalog path " + catPath +
                  ", failed to create catalog " + catalog.getName());
            }
            madeDir = true;
          }

          ms.openTransaction();
          ms.createCatalog(catalog);

          // Create a default database inside the catalog
          Database db = new Database(DEFAULT_DATABASE_NAME, "Default database for catalog " +
              catalog.getName(), catalog.getLocationUri(), Collections.emptyMap());
          db.setCatalogName(catalog.getName());
          create_database_core(ms, db);

          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.CREATE_CATALOG,
                    new CreateCatalogEvent(true, this, catalog));
          }

          success = ms.commitTransaction();
        } finally {
          if (!success) {
            ms.rollbackTransaction();
            if (madeDir) {
              wh.deleteDir(catPath, true);
            }
          }

          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                EventType.CREATE_CATALOG,
                new CreateCatalogEvent(success, this, catalog),
                null,
                transactionalListenersResponses, ms);
          }
        }
        success = true;
      } catch (AlreadyExistsException|InvalidObjectException|MetaException e) {
        ex = e;
        throw e;
      } finally {
        endFunction("create_catalog", success, ex);
      }
    }

    @Override
    public GetCatalogResponse get_catalog(GetCatalogRequest rqst)
        throws NoSuchObjectException, TException {
      String catName = rqst.getName();
      startFunction("get_catalog", ": " + catName);
      Catalog cat = null;
      Exception ex = null;
      try {
        cat = getMS().getCatalog(catName);
        firePreEvent(new PreReadCatalogEvent(this, cat));
        return new GetCatalogResponse(cat);
      } catch (MetaException|NoSuchObjectException e) {
        ex = e;
        throw e;
      } finally {
        endFunction("get_database", cat != null, ex);
      }
    }

    @Override
    public GetCatalogsResponse get_catalogs() throws MetaException {
      startFunction("get_catalogs");

      List<String> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getCatalogs();
      } catch (MetaException e) {
        ex = e;
        throw e;
      } finally {
        endFunction("get_catalog", ret != null, ex);
      }
      return new GetCatalogsResponse(ret == null ? Collections.emptyList() : ret);

    }

    @Override
    public void drop_catalog(DropCatalogRequest rqst)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
      String catName = rqst.getName();
      startFunction("drop_catalog", ": " + catName);
      if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(catName)) {
        endFunction("drop_catalog", false, null);
        throw new MetaException("Can not drop " + DEFAULT_CATALOG_NAME + " catalog");
      }

      boolean success = false;
      Exception ex = null;
      try {
        dropCatalogCore(catName);
        success = true;
      } catch (NoSuchObjectException|InvalidOperationException|MetaException e) {
        ex = e;
        throw e;
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("drop_catalog", success, ex);
      }

    }

    private void dropCatalogCore(String catName)
        throws MetaException, NoSuchObjectException, InvalidOperationException {
      boolean success = false;
      Catalog cat = null;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        cat = ms.getCatalog(catName);

        firePreEvent(new PreDropCatalogEvent(this, cat));

        List<String> allDbs = get_databases(prependNotNullCatToDbName(catName, null));
        if (allDbs != null && !allDbs.isEmpty()) {
          // It might just be the default, in which case we can drop that one if it's empty
          if (allDbs.size() == 1 && allDbs.get(0).equals(DEFAULT_DATABASE_NAME)) {
            try {
              drop_database_core(ms, catName, DEFAULT_DATABASE_NAME, true, false);
            } catch (InvalidOperationException e) {
              // This means there are tables of something in the database
              throw new InvalidOperationException("There are still objects in the default " +
                  "database for catalog " + catName);
            } catch (InvalidObjectException|IOException|InvalidInputException e) {
              MetaException me = new MetaException("Error attempt to drop default database for " +
                  "catalog " + catName);
              me.initCause(e);
              throw me;
            }
          } else {
            throw new InvalidOperationException("There are non-default databases in the catalog " +
                catName + " so it cannot be dropped.");
          }
        }

        ms.dropCatalog(catName) ;
        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_CATALOG,
                  new DropCatalogEvent(true, this, cat));
        }

        success = ms.commitTransaction();
      } finally {
        if (success) {
          wh.deleteDir(wh.getDnsPath(new Path(cat.getLocationUri())), false);
        } else {
          ms.rollbackTransaction();
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DROP_CATALOG,
              new DropCatalogEvent(success, this, cat),
              null,
              transactionalListenerResponses, ms);
        }
      }
    }


    // Assumes that the catalog has already been set.
    private void create_database_core(RawStore ms, final Database db)
        throws AlreadyExistsException, InvalidObjectException, MetaException {
      if (!MetaStoreUtils.validateName(db.getName(), null)) {
        throw new InvalidObjectException(db.getName() + " is not a valid database name");
      }

      Catalog cat = null;
      try {
        cat = getMS().getCatalog(db.getCatalogName());
      } catch (NoSuchObjectException e) {
        LOG.error("No such catalog " + db.getCatalogName());
        throw new InvalidObjectException("No such catalog " + db.getCatalogName());
      }
      Path dbPath = wh.determineDatabasePath(cat, db);
      db.setLocationUri(dbPath.toString());

      boolean success = false;
      boolean madeDir = false;
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();
      try {
        firePreEvent(new PreCreateDatabaseEvent(db, this));
        if (!wh.isDir(dbPath)) {
          LOG.debug("Creating database path " + dbPath);
          if (!wh.mkdirs(dbPath)) {
            throw new MetaException("Unable to create database path " + dbPath +
                ", failed to create database " + db.getName());
          }
          madeDir = true;
        }

        ms.openTransaction();
        ms.createDatabase(db);

        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.CREATE_DATABASE,
                                                    new CreateDatabaseEvent(db, true, this));
        }

        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(dbPath, true);
          }
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.CREATE_DATABASE,
                                                new CreateDatabaseEvent(db, success, this),
                                                null,
                                                transactionalListenersResponses, ms);
        }
      }
    }

    @Override
    public void create_database(final Database db)
        throws AlreadyExistsException, InvalidObjectException, MetaException {
      startFunction("create_database", ": " + db.toString());
      boolean success = false;
      Exception ex = null;
      if (!db.isSetCatalogName()) db.setCatalogName(getDefaultCatalog(conf));
      try {
        try {
          if (null != get_database_core(db.getCatalogName(), db.getName())) {
            throw new AlreadyExistsException("Database " + db.getName() + " already exists");
          }
        } catch (NoSuchObjectException e) {
          // expected
        }

        if (TEST_TIMEOUT_ENABLED) {
          try {
            Thread.sleep(TEST_TIMEOUT_VALUE);
          } catch (InterruptedException e) {
            // do nothing
          }
          Deadline.checkTimeout();
        }
        create_database_core(getMS(), db);
        success = true;
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("create_database", success, ex);
      }
    }

    @Override
    public Database get_database(final String name) throws NoSuchObjectException, MetaException {
      startFunction("get_database", ": " + name);
      Database db = null;
      Exception ex = null;
      try {
        String[] parsedDbName = parseDbName(name, conf);
        db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        firePreEvent(new PreReadDatabaseEvent(db, this));
      } catch (MetaException|NoSuchObjectException e) {
        ex = e;
        throw e;
      } finally {
        endFunction("get_database", db != null, ex);
      }
      return db;
    }

    @Override
    public Database get_database_core(String catName, final String name) throws NoSuchObjectException, MetaException {
      Database db = null;
      if (name == null) {
        throw new MetaException("Database name cannot be null.");
      }
      try {
        db = getMS().getDatabase(catName, name);
      } catch (MetaException | NoSuchObjectException e) {
        throw e;
      } catch (Exception e) {
        assert (e instanceof RuntimeException);
        throw (RuntimeException) e;
      }
      return db;
    }

    @Override
    public void alter_database(final String dbName, final Database newDB) throws TException {
      startFunction("alter_database " + dbName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      Database oldDB = null;
      Map<String, String> transactionalListenersResponses = Collections.emptyMap();

      // Perform the same URI normalization as create_database_core.
      if (newDB.getLocationUri() != null) {
        newDB.setLocationUri(wh.getDnsPath(new Path(newDB.getLocationUri())).toString());
      }

      String[] parsedDbName = parseDbName(dbName, conf);

      try {
        oldDB = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        if (oldDB == null) {
          throw new MetaException("Could not alter database \"" + parsedDbName[DB_NAME] +
              "\". Could not retrieve old definition.");
        }
        firePreEvent(new PreAlterDatabaseEvent(oldDB, newDB, this));

        ms.openTransaction();
        ms.alterDatabase(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], newDB);

        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_DATABASE,
                  new AlterDatabaseEvent(oldDB, newDB, true, this));
        }

        success = ms.commitTransaction();
      } catch (MetaException|NoSuchObjectException e) {
        ex = e;
        throw e;
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }

        if ((null != oldDB) && (!listeners.isEmpty())) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_DATABASE,
              new AlterDatabaseEvent(oldDB, newDB, success, this),
              null,
              transactionalListenersResponses, ms);
        }
        endFunction("alter_database", success, ex);
      }
    }

    private void drop_database_core(RawStore ms, String catName,
        final String name, final boolean deleteData, final boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException,
        IOException, InvalidObjectException, InvalidInputException {
      boolean success = false;
      Database db = null;
      List<Path> tablePaths = new ArrayList<>();
      List<Path> partitionPaths = new ArrayList<>();
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      if (name == null) {
        throw new MetaException("Database name cannot be null.");
      }
      try {
        ms.openTransaction();
        db = ms.getDatabase(catName, name);

        firePreEvent(new PreDropDatabaseEvent(db, this));
        String catPrependedName = MetaStoreUtils.prependCatalogToDbName(catName, name, conf);

        List<String> allTables = get_all_tables(catPrependedName);
        List<String> allFunctions = get_functions(catPrependedName, "*");

        if (!cascade) {
          if (!allTables.isEmpty()) {
            throw new InvalidOperationException(
                "Database " + db.getName() + " is not empty. One or more tables exist.");
          }
          if (!allFunctions.isEmpty()) {
            throw new InvalidOperationException(
                "Database " + db.getName() + " is not empty. One or more functions exist.");
          }
        }
        Path path = new Path(db.getLocationUri()).getParent();
        if (!wh.isWritable(path)) {
          throw new MetaException("Database not dropped since " +
              path + " is not writable by " +
              SecurityUtils.getUser());
        }

        Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));

        // drop any functions before dropping db
        for (String funcName : allFunctions) {
          drop_function(catPrependedName, funcName);
        }

        // drop tables before dropping db
        int tableBatchSize = MetastoreConf.getIntVar(conf,
            ConfVars.BATCH_RETRIEVE_MAX);

        int startIndex = 0;
        // retrieve the tables from the metastore in batches to alleviate memory constraints
        while (startIndex < allTables.size()) {
          int endIndex = Math.min(startIndex + tableBatchSize, allTables.size());

          List<Table> tables;
          try {
            tables = ms.getTableObjectsByName(catName, name, allTables.subList(startIndex, endIndex));
          } catch (UnknownDBException e) {
            throw new MetaException(e.getMessage());
          }

          if (tables != null && !tables.isEmpty()) {
            for (Table table : tables) {

              // If the table is not external and it might not be in a subdirectory of the database
              // add it's locations to the list of paths to delete
              Path tablePath = null;
              if (table.getSd().getLocation() != null && !isExternal(table)) {
                tablePath = wh.getDnsPath(new Path(table.getSd().getLocation()));
                if (!wh.isWritable(tablePath.getParent())) {
                  throw new MetaException("Database metadata not deleted since table: " +
                      table.getTableName() + " has a parent location " + tablePath.getParent() +
                      " which is not writable by " + SecurityUtils.getUser());
                }

                if (!isSubdirectory(databasePath, tablePath)) {
                  tablePaths.add(tablePath);
                }
              }

              // For each partition in each table, drop the partitions and get a list of
              // partitions' locations which might need to be deleted
              partitionPaths = dropPartitionsAndGetLocations(ms, catName, name, table.getTableName(),
                  tablePath, table.getPartitionKeys(), deleteData && !isExternal(table));

              // Drop the table but not its data
              drop_table(MetaStoreUtils.prependCatalogToDbName(table.getCatName(), table.getDbName(), conf),
                  table.getTableName(), false);
            }

            startIndex = endIndex;
          }
        }

        if (ms.dropDatabase(catName, name)) {
          if (!transactionalListeners.isEmpty()) {
            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.DROP_DATABASE,
                                                      new DropDatabaseEvent(db, true, this));
          }

          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData) {
          // Delete the data in the partitions which have other locations
          deletePartitionData(partitionPaths);
          // Delete the data in the tables which have other locations
          for (Path tablePath : tablePaths) {
            deleteTableData(tablePath);
          }
          // Delete the data in the database
          try {
            wh.deleteDir(new Path(db.getLocationUri()), true);
          } catch (Exception e) {
            LOG.error("Failed to delete database directory: " + db.getLocationUri() +
                " " + e.getMessage());
          }
          // it is not a terrible thing even if the data is not deleted
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.DROP_DATABASE,
                                                new DropDatabaseEvent(db, success, this),
                                                null,
                                                transactionalListenerResponses, ms);
        }
      }
    }

    /**
     * Returns a BEST GUESS as to whether or not other is a subdirectory of parent. It does not
     * take into account any intricacies of the underlying file system, which is assumed to be
     * HDFS. This should not return any false positives, but may return false negatives.
     *
     * @param parent
     * @param other
     * @return
     */
    private boolean isSubdirectory(Path parent, Path other) {
      return other.toString().startsWith(parent.toString().endsWith(Path.SEPARATOR) ?
          parent.toString() : parent.toString() + Path.SEPARATOR);
    }

    @Override
    public void drop_database(final String dbName, final boolean deleteData, final boolean cascade)
        throws NoSuchObjectException, InvalidOperationException, MetaException {
      startFunction("drop_database", ": " + dbName);
      String[] parsedDbName = parseDbName(dbName, conf);
      if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(parsedDbName[CAT_NAME]) &&
          DEFAULT_DATABASE_NAME.equalsIgnoreCase(parsedDbName[DB_NAME])) {
        endFunction("drop_database", false, null);
        throw new MetaException("Can not drop " + DEFAULT_DATABASE_NAME + " database in catalog "
            + DEFAULT_CATALOG_NAME);
      }

      boolean success = false;
      Exception ex = null;
      try {
        drop_database_core(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], deleteData,
            cascade);
        success = true;
      } catch (NoSuchObjectException|InvalidOperationException|MetaException e) {
        ex = e;
        throw e;
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("drop_database", success, ex);
      }
    }


    @Override
    public List<String> get_databases(final String pattern) throws MetaException {
      startFunction("get_databases", ": " + pattern);

      String[] parsedDbNamed = parseDbName(pattern, conf);
      List<String> ret = null;
      Exception ex = null;
      try {
        if (parsedDbNamed[DB_NAME] == null) {
          ret = getMS().getAllDatabases(parsedDbNamed[CAT_NAME]);
        } else {
          ret = getMS().getDatabases(parsedDbNamed[CAT_NAME], parsedDbNamed[DB_NAME]);
        }
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_databases", ret != null, ex);
      }
      return ret;
    }

    @Override
    public List<String> get_all_databases() throws MetaException {
      return get_databases(MetaStoreUtils.prependCatalogToDbName(null, null, conf));
    }

    private void create_type_core(final RawStore ms, final Type type)
        throws AlreadyExistsException, MetaException, InvalidObjectException {
      if (!MetaStoreUtils.validateName(type.getName(), null)) {
        throw new InvalidObjectException("Invalid type name");
      }

      boolean success = false;
      try {
        ms.openTransaction();
        if (is_type_exists(ms, type.getName())) {
          throw new AlreadyExistsException("Type " + type.getName() + " already exists");
        }
        ms.createType(type);
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
      }
    }

    @Override
    public boolean create_type(final Type type) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      startFunction("create_type", ": " + type.toString());
      boolean success = false;
      Exception ex = null;
      try {
        create_type_core(getMS(), type);
        success = true;
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("create_type", success, ex);
      }

      return success;
    }

    @Override
    public Type get_type(final String name) throws MetaException, NoSuchObjectException {
      startFunction("get_type", ": " + name);

      Type ret = null;
      Exception ex = null;
      try {
        ret = getMS().getType(name);
        if (null == ret) {
          throw new NoSuchObjectException("Type \"" + name + "\" not found.");
        }
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("get_type", ret != null, ex);
      }
      return ret;
    }

    private boolean is_type_exists(RawStore ms, String typeName)
        throws MetaException {
      return (ms.getType(typeName) != null);
    }

    @Override
    public boolean drop_type(final String name) throws MetaException, NoSuchObjectException {
      startFunction("drop_type", ": " + name);

      boolean success = false;
      Exception ex = null;
      try {
        // TODO:pc validate that there are no types that refer to this
        success = getMS().dropType(name);
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("drop_type", success, ex);
      }
      return success;
    }

    @Override
    public Map<String, Type> get_type_all(String name) throws MetaException {
      // TODO Auto-generated method stub
      startFunction("get_type_all", ": " + name);
      endFunction("get_type_all", false, null);
      throw new MetaException("Not yet implemented");
    }

    private void create_table_core(final RawStore ms, final Table tbl,
        final EnvironmentContext envContext)
            throws AlreadyExistsException, MetaException,
            InvalidObjectException, NoSuchObjectException {
      create_table_core(ms, tbl, envContext, null, null, null, null, null, null);
    }

    private void create_table_core(final RawStore ms, final Table tbl,
        final EnvironmentContext envContext, List<SQLPrimaryKey> primaryKeys,
        List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
        List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
                                   List<SQLCheckConstraint> checkConstraints)
        throws AlreadyExistsException, MetaException,
        InvalidObjectException, NoSuchObjectException {
      if (!MetaStoreUtils.validateName(tbl.getTableName(), conf)) {
        throw new InvalidObjectException(tbl.getTableName()
            + " is not a valid object name");
      }
      String validate = MetaStoreUtils.validateTblColumns(tbl.getSd().getCols());
      if (validate != null) {
        throw new InvalidObjectException("Invalid column " + validate);
      }
      if (tbl.getPartitionKeys() != null) {
        validate = MetaStoreUtils.validateTblColumns(tbl.getPartitionKeys());
        if (validate != null) {
          throw new InvalidObjectException("Invalid partition column " + validate);
        }
      }
      SkewedInfo skew = tbl.getSd().getSkewedInfo();
      if (skew != null) {
        validate = MetaStoreUtils.validateSkewedColNames(skew.getSkewedColNames());
        if (validate != null) {
          throw new InvalidObjectException("Invalid skew column " + validate);
        }
        validate = MetaStoreUtils.validateSkewedColNamesSubsetCol(
            skew.getSkewedColNames(), tbl.getSd().getCols());
        if (validate != null) {
          throw new InvalidObjectException("Invalid skew column " + validate);
        }
      }

      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      Path tblPath = null;
      boolean success = false, madeDir = false;
      try {
        if (!tbl.isSetCatName()) tbl.setCatName(getDefaultCatalog(conf));
        firePreEvent(new PreCreateTableEvent(tbl, this));

        ms.openTransaction();

        Database db = ms.getDatabase(tbl.getCatName(), tbl.getDbName());
        if (db == null) {
          throw new NoSuchObjectException("The database " +
              Warehouse.getCatalogQualifiedDbName(tbl.getCatName(), tbl.getDbName()) + " does not exist");
        }

        // get_table checks whether database exists, it should be moved here
        if (is_table_exists(ms, tbl.getCatName(), tbl.getDbName(), tbl.getTableName())) {
          throw new AlreadyExistsException("Table " + getCatalogQualifiedTableName(tbl)
              + " already exists");
        }

        if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
          if (tbl.getSd().getLocation() == null
              || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(
                ms.getDatabase(tbl.getCatName(), tbl.getDbName()), tbl.getTableName());
          } else {
            if (!isExternal(tbl) && !MetaStoreUtils.isNonNativeTable(tbl)) {
              LOG.warn("Location: " + tbl.getSd().getLocation()
                  + " specified for non-external table:" + tbl.getTableName());
            }
            tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          }
          tbl.getSd().setLocation(tblPath.toString());
        }

        if (tblPath != null) {
          if (!wh.isDir(tblPath)) {
            if (!wh.mkdirs(tblPath)) {
              throw new MetaException(tblPath
                  + " is not a directory or unable to create one");
            }
            madeDir = true;
          }
        }
        if (MetastoreConf.getBoolVar(conf, ConfVars.STATS_AUTO_GATHER) &&
            !MetaStoreUtils.isView(tbl)) {
          MetaStoreUtils.updateTableStatsFast(db, tbl, wh, madeDir, false, envContext, true);
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        tbl.setCreateTime((int) time);
        if (tbl.getParameters() == null ||
            tbl.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
          tbl.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
        }

        if (primaryKeys == null && foreignKeys == null
                && uniqueConstraints == null && notNullConstraints == null && defaultConstraints == null
            && checkConstraints == null) {
          ms.createTable(tbl);
        } else {
          // Set constraint name if null before sending to listener
          List<String> constraintNames = ms.createTableWithConstraints(tbl, primaryKeys, foreignKeys,
              uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
          int primaryKeySize = 0;
          if (primaryKeys != null) {
            primaryKeySize = primaryKeys.size();
            for (int i = 0; i < primaryKeys.size(); i++) {
              if (primaryKeys.get(i).getPk_name() == null) {
                primaryKeys.get(i).setPk_name(constraintNames.get(i));
              }
            }
          }
          int foreignKeySize = 0;
          if (foreignKeys != null) {
            foreignKeySize = foreignKeys.size();
            for (int i = 0; i < foreignKeySize; i++) {
              if (foreignKeys.get(i).getFk_name() == null) {
                foreignKeys.get(i).setFk_name(constraintNames.get(primaryKeySize + i));
              }
            }
          }
          int uniqueConstraintSize = 0;
          if (uniqueConstraints != null) {
            uniqueConstraintSize = uniqueConstraints.size();
            for (int i = 0; i < uniqueConstraintSize; i++) {
              if (uniqueConstraints.get(i).getUk_name() == null) {
                uniqueConstraints.get(i).setUk_name(constraintNames.get(primaryKeySize + foreignKeySize + i));
              }
            }
          }
          int notNullConstraintSize =  0;
          if (notNullConstraints != null) {
            for (int i = 0; i < notNullConstraints.size(); i++) {
              if (notNullConstraints.get(i).getNn_name() == null) {
                notNullConstraints.get(i).setNn_name(constraintNames.get(primaryKeySize + foreignKeySize + uniqueConstraintSize + i));
              }
            }
          }
          int defaultConstraintSize =  0;
          if (defaultConstraints!= null) {
            for (int i = 0; i < defaultConstraints.size(); i++) {
              if (defaultConstraints.get(i).getDc_name() == null) {
                defaultConstraints.get(i).setDc_name(constraintNames.get(primaryKeySize + foreignKeySize
                    + uniqueConstraintSize + notNullConstraintSize + i));
              }
            }
          }
          if (checkConstraints!= null) {
            for (int i = 0; i < checkConstraints.size(); i++) {
              if (checkConstraints.get(i).getDc_name() == null) {
                checkConstraints.get(i).setDc_name(constraintNames.get(primaryKeySize + foreignKeySize
                                                                             + uniqueConstraintSize
                                                                             + defaultConstraintSize
                                                                           + notNullConstraintSize + i));
              }
            }
          }
        }

        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
              EventType.CREATE_TABLE, new CreateTableEvent(tbl, true, this), envContext);
          if (primaryKeys != null && !primaryKeys.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_PRIMARYKEY,
                new AddPrimaryKeyEvent(primaryKeys, true, this), envContext);
          }
          if (foreignKeys != null && !foreignKeys.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_FOREIGNKEY,
                new AddForeignKeyEvent(foreignKeys, true, this), envContext);
          }
          if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_UNIQUECONSTRAINT,
                new AddUniqueConstraintEvent(uniqueConstraints, true, this), envContext);
          }
          if (notNullConstraints != null && !notNullConstraints.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_NOTNULLCONSTRAINT,
                new AddNotNullConstraintEvent(notNullConstraints, true, this), envContext);
          }
        }

        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(tblPath, true);
          }
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.CREATE_TABLE,
              new CreateTableEvent(tbl, success, this), envContext, transactionalListenerResponses, ms);
          if (primaryKeys != null && !primaryKeys.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_PRIMARYKEY,
                new AddPrimaryKeyEvent(primaryKeys, success, this), envContext);
          }
          if (foreignKeys != null && !foreignKeys.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_FOREIGNKEY,
                new AddForeignKeyEvent(foreignKeys, success, this), envContext);
          }
          if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_UNIQUECONSTRAINT,
                new AddUniqueConstraintEvent(uniqueConstraints, success, this), envContext);
          }
          if (notNullConstraints != null && !notNullConstraints.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_NOTNULLCONSTRAINT,
                new AddNotNullConstraintEvent(notNullConstraints, success, this), envContext);
          }
        }
      }
    }

    @Override
    public void create_table(final Table tbl) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      create_table_with_environment_context(tbl, null);
    }

    @Override
    public void create_table_with_environment_context(final Table tbl,
        final EnvironmentContext envContext)
        throws AlreadyExistsException, MetaException, InvalidObjectException {
      startFunction("create_table", ": " + tbl.toString());
      boolean success = false;
      Exception ex = null;
      try {
        create_table_core(getMS(), tbl, envContext);
        success = true;
      } catch (NoSuchObjectException e) {
        LOG.warn("create_table_with_environment_context got ", e);
        ex = e;
        throw new InvalidObjectException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("create_table", success, ex, tbl.getTableName());
      }
    }

    @Override
    public void create_table_with_constraints(final Table tbl,
        final List<SQLPrimaryKey> primaryKeys, final List<SQLForeignKey> foreignKeys,
        List<SQLUniqueConstraint> uniqueConstraints,
        List<SQLNotNullConstraint> notNullConstraints,
        List<SQLDefaultConstraint> defaultConstraints,
        List<SQLCheckConstraint> checkConstraints)
        throws AlreadyExistsException, MetaException, InvalidObjectException {
      startFunction("create_table", ": " + tbl.toString());
      boolean success = false;
      Exception ex = null;
      try {
        create_table_core(getMS(), tbl, null, primaryKeys, foreignKeys,
            uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
        success = true;
      } catch (NoSuchObjectException e) {
        ex = e;
        throw new InvalidObjectException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("create_table", success, ex, tbl.getTableName());
      }
    }

    @Override
    public void drop_constraint(DropConstraintRequest req)
        throws MetaException, InvalidObjectException {
      String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
      String dbName = req.getDbname();
      String tableName = req.getTablename();
      String constraintName = req.getConstraintname();
      startFunction("drop_constraint", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        ms.dropConstraint(catName, dbName, tableName, constraintName);
        if (transactionalListeners.size() > 0) {
          DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName,
              tableName, constraintName, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onDropConstraint(dropConstraintEvent);
          }
        }
        success = ms.commitTransaction();
      } catch (NoSuchObjectException e) {
        ex = e;
        throw new InvalidObjectException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else {
          for (MetaStoreEventListener listener : listeners) {
            DropConstraintEvent dropConstraintEvent = new DropConstraintEvent(catName, dbName,
                tableName, constraintName, true, this);
            listener.onDropConstraint(dropConstraintEvent);
          }
        }
        endFunction("drop_constraint", success, ex, constraintName);
      }
    }

    @Override
    public void add_primary_key(AddPrimaryKeyRequest req)
      throws MetaException, InvalidObjectException {
      List<SQLPrimaryKey> primaryKeyCols = req.getPrimaryKeyCols();
      String constraintName = (primaryKeyCols != null && primaryKeyCols.size() > 0) ?
        primaryKeyCols.get(0).getPk_name() : "null";
      startFunction("add_primary_key", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addPrimaryKeys(primaryKeyCols);
        // Set primary key name if null before sending to listener
        if (primaryKeyCols != null) {
          for (int i = 0; i < primaryKeyCols.size(); i++) {
            if (primaryKeyCols.get(i).getPk_name() == null) {
              primaryKeyCols.get(i).setPk_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (primaryKeyCols != null && primaryKeyCols.size() > 0) {
            AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
            for (MetaStoreEventListener transactionalListener : transactionalListeners) {
              transactionalListener.onAddPrimaryKey(addPrimaryKeyEvent);
            }
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (primaryKeyCols != null && primaryKeyCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeyCols, true, this);
            listener.onAddPrimaryKey(addPrimaryKeyEvent);
          }
        }
        endFunction("add_primary_key", success, ex, constraintName);
      }
    }

    @Override
    public void add_foreign_key(AddForeignKeyRequest req)
      throws MetaException, InvalidObjectException {
      List<SQLForeignKey> foreignKeyCols = req.getForeignKeyCols();
      String constraintName = (foreignKeyCols != null && foreignKeyCols.size() > 0) ?
        foreignKeyCols.get(0).getFk_name() : "null";
      startFunction("add_foreign_key", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addForeignKeys(foreignKeyCols);
        // Set foreign key name if null before sending to listener
        if (foreignKeyCols != null) {
          for (int i = 0; i < foreignKeyCols.size(); i++) {
            if (foreignKeyCols.get(i).getFk_name() == null) {
              foreignKeyCols.get(i).setFk_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (foreignKeyCols != null && foreignKeyCols.size() > 0) {
            AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeyCols, true, this);
            for (MetaStoreEventListener transactionalListener : transactionalListeners) {
              transactionalListener.onAddForeignKey(addForeignKeyEvent);
            }
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (foreignKeyCols != null && foreignKeyCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeyCols, true, this);
            listener.onAddForeignKey(addForeignKeyEvent);
          }
        }
        endFunction("add_foreign_key", success, ex, constraintName);
      }
    }

    @Override
    public void add_unique_constraint(AddUniqueConstraintRequest req)
      throws MetaException, InvalidObjectException {
      List<SQLUniqueConstraint> uniqueConstraintCols = req.getUniqueConstraintCols();
      String constraintName = (uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) ?
              uniqueConstraintCols.get(0).getUk_name() : "null";
      startFunction("add_unique_constraint", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addUniqueConstraints(uniqueConstraintCols);
        // Set unique constraint name if null before sending to listener
        if (uniqueConstraintCols != null) {
          for (int i = 0; i < uniqueConstraintCols.size(); i++) {
            if (uniqueConstraintCols.get(i).getUk_name() == null) {
              uniqueConstraintCols.get(i).setUk_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) {
            AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraintCols, true, this);
            for (MetaStoreEventListener transactionalListener : transactionalListeners) {
              transactionalListener.onAddUniqueConstraint(addUniqueConstraintEvent);
            }
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (uniqueConstraintCols != null && uniqueConstraintCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraintCols, true, this);
            listener.onAddUniqueConstraint(addUniqueConstraintEvent);
          }
        }
        endFunction("add_unique_constraint", success, ex, constraintName);
      }
    }

    @Override
    public void add_not_null_constraint(AddNotNullConstraintRequest req)
      throws MetaException, InvalidObjectException {
      List<SQLNotNullConstraint> notNullConstraintCols = req.getNotNullConstraintCols();
      String constraintName = (notNullConstraintCols != null && notNullConstraintCols.size() > 0) ?
              notNullConstraintCols.get(0).getNn_name() : "null";
      startFunction("add_not_null_constraint", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addNotNullConstraints(notNullConstraintCols);
        // Set not null constraint name if null before sending to listener
        if (notNullConstraintCols != null) {
          for (int i = 0; i < notNullConstraintCols.size(); i++) {
            if (notNullConstraintCols.get(i).getNn_name() == null) {
              notNullConstraintCols.get(i).setNn_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (notNullConstraintCols != null && notNullConstraintCols.size() > 0) {
            AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraintCols, true, this);
            for (MetaStoreEventListener transactionalListener : transactionalListeners) {
              transactionalListener.onAddNotNullConstraint(addNotNullConstraintEvent);
            }
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (notNullConstraintCols != null && notNullConstraintCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraintCols, true, this);
            listener.onAddNotNullConstraint(addNotNullConstraintEvent);
          }
        }
        endFunction("add_not_null_constraint", success, ex, constraintName);
      }
    }

    @Override
    public void add_default_constraint(AddDefaultConstraintRequest req)
        throws MetaException, InvalidObjectException {
      List<SQLDefaultConstraint> defaultConstraintCols= req.getDefaultConstraintCols();
      String constraintName = (defaultConstraintCols != null && defaultConstraintCols.size() > 0) ?
          defaultConstraintCols.get(0).getDc_name() : "null";
      startFunction("add_default_constraint", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addDefaultConstraints(defaultConstraintCols);
        // Set not null constraint name if null before sending to listener
        if (defaultConstraintCols != null) {
          for (int i = 0; i < defaultConstraintCols.size(); i++) {
            if (defaultConstraintCols.get(i).getDc_name() == null) {
              defaultConstraintCols.get(i).setDc_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (defaultConstraintCols != null && defaultConstraintCols.size() > 0) {
            //TODO: Even listener for default
            //AddDefaultConstraintEvent addDefaultConstraintEvent = new AddDefaultConstraintEvent(defaultConstraintCols, true, this);
            //for (MetaStoreEventListener transactionalListener : transactionalListeners) {
             // transactionalListener.onAddNotNullConstraint(addDefaultConstraintEvent);
            //}
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (defaultConstraintCols != null && defaultConstraintCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            //AddNotNullConstraintEvent addDefaultConstraintEvent = new AddNotNullConstraintEvent(defaultConstraintCols, true, this);
            //listener.onAddDefaultConstraint(addDefaultConstraintEvent);
          }
        }
        endFunction("add_default_constraint", success, ex, constraintName);
      }
    }

    @Override
    public void add_check_constraint(AddCheckConstraintRequest req)
        throws MetaException, InvalidObjectException {
      List<SQLCheckConstraint> checkConstraintCols= req.getCheckConstraintCols();
      String constraintName = (checkConstraintCols != null && checkConstraintCols.size() > 0) ?
          checkConstraintCols.get(0).getDc_name() : "null";
      startFunction("add_check_constraint", ": " + constraintName);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        List<String> constraintNames = ms.addCheckConstraints(checkConstraintCols);
        if (checkConstraintCols != null) {
          for (int i = 0; i < checkConstraintCols.size(); i++) {
            if (checkConstraintCols.get(i).getDc_name() == null) {
              checkConstraintCols.get(i).setDc_name(constraintNames.get(i));
            }
          }
        }
        if (transactionalListeners.size() > 0) {
          if (checkConstraintCols != null && checkConstraintCols.size() > 0) {
            //TODO: Even listener for check
            //AddcheckConstraintEvent addcheckConstraintEvent = new AddcheckConstraintEvent(checkConstraintCols, true, this);
            //for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            // transactionalListener.onAddNotNullConstraint(addcheckConstraintEvent);
            //}
          }
        }
        success = ms.commitTransaction();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (checkConstraintCols != null && checkConstraintCols.size() > 0) {
          for (MetaStoreEventListener listener : listeners) {
            //AddNotNullConstraintEvent addCheckConstraintEvent = new AddNotNullConstraintEvent(checkConstraintCols, true, this);
            //listener.onAddCheckConstraint(addCheckConstraintEvent);
          }
        }
        endFunction("add_check_constraint", success, ex, constraintName);
      }
    }

    private boolean is_table_exists(RawStore ms, String catName, String dbname, String name)
        throws MetaException {
      return (ms.getTable(catName, dbname, name) != null);
    }

    private boolean drop_table_core(final RawStore ms, final String catName, final String dbname,
                                    final String name, final boolean deleteData,
                                    final EnvironmentContext envContext, final String indexName)
        throws NoSuchObjectException, MetaException, IOException, InvalidObjectException,
        InvalidInputException {
      boolean success = false;
      boolean isExternal = false;
      Path tblPath = null;
      List<Path> partPaths = null;
      Table tbl = null;
      boolean ifPurge = false;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      try {
        ms.openTransaction();
        // drop any partitions
        tbl = get_table_core(catName, dbname, name);
        if (tbl == null) {
          throw new NoSuchObjectException(name + " doesn't exist");
        }
        if (tbl.getSd() == null) {
          throw new MetaException("Table metadata is corrupted");
        }
        ifPurge = isMustPurge(envContext, tbl);

        firePreEvent(new PreDropTableEvent(tbl, deleteData, this));

        isExternal = isExternal(tbl);
        if (tbl.getSd().getLocation() != null) {
          tblPath = new Path(tbl.getSd().getLocation());
          if (!wh.isWritable(tblPath.getParent())) {
            String target = indexName == null ? "Table" : "Index table";
            throw new MetaException(target + " metadata not deleted since " +
                tblPath.getParent() + " is not writable by " +
                SecurityUtils.getUser());
          }
        }

        // Drop the partitions and get a list of locations which need to be deleted
        partPaths = dropPartitionsAndGetLocations(ms, catName, dbname, name, tblPath,
            tbl.getPartitionKeys(), deleteData && !isExternal);

        // Drop any constraints on the table
        ms.dropConstraint(catName, dbname, name, null, true);

        if (!ms.dropTable(catName, dbname, name)) {
          String tableName = getCatalogQualifiedTableName(catName, dbname, name);
          throw new MetaException(indexName == null ? "Unable to drop table " + tableName:
              "Unable to drop index table " + tableName + " for index " + indexName);
        } else {
          if (!transactionalListeners.isEmpty()) {
            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.DROP_TABLE,
                                                      new DropTableEvent(tbl, true, deleteData, this),
                                                      envContext);
          }
          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData && !isExternal) {
          // Data needs deletion. Check if trash may be skipped.
          // Delete the data in the partitions which have other locations
          deletePartitionData(partPaths, ifPurge);
          // Delete the data in the table
          deleteTableData(tblPath, ifPurge);
          // ok even if the data is not deleted
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.DROP_TABLE,
                                                new DropTableEvent(tbl, success, deleteData, this),
                                                envContext,
                                                transactionalListenerResponses, ms);
        }
      }
      return success;
    }

    /**
     * Deletes the data in a table's location, if it fails logs an error
     *
     * @param tablePath
     */
    private void deleteTableData(Path tablePath) {
      deleteTableData(tablePath, false);
    }

    /**
     * Deletes the data in a table's location, if it fails logs an error
     *
     * @param tablePath
     * @param ifPurge completely purge the table (skipping trash) while removing
     *                data from warehouse
     */
    private void deleteTableData(Path tablePath, boolean ifPurge) {

      if (tablePath != null) {
        try {
          wh.deleteDir(tablePath, true, ifPurge);
        } catch (Exception e) {
          LOG.error("Failed to delete table directory: " + tablePath +
              " " + e.getMessage());
        }
      }
    }

    /**
     * Give a list of partitions' locations, tries to delete each one
     * and for each that fails logs an error.
     *
     * @param partPaths
     */
    private void deletePartitionData(List<Path> partPaths) {
      deletePartitionData(partPaths, false);
    }

    /**
    * Give a list of partitions' locations, tries to delete each one
    * and for each that fails logs an error.
    *
    * @param partPaths
    * @param ifPurge completely purge the partition (skipping trash) while
    *                removing data from warehouse
    */
    private void deletePartitionData(List<Path> partPaths, boolean ifPurge) {
      if (partPaths != null && !partPaths.isEmpty()) {
        for (Path partPath : partPaths) {
          try {
            wh.deleteDir(partPath, true, ifPurge);
          } catch (Exception e) {
            LOG.error("Failed to delete partition directory: " + partPath +
                " " + e.getMessage());
          }
        }
      }
    }

    /**
     * Retrieves the partitions specified by partitionKeys. If checkLocation, for locations of
     * partitions which may not be subdirectories of tablePath checks to make the locations are
     * writable.
     *
     * Drops the metadata for each partition.
     *
     * Provides a list of locations of partitions which may not be subdirectories of tablePath.
     *
     * @param ms
     * @param dbName
     * @param tableName
     * @param tablePath
     * @param partitionKeys
     * @param checkLocation
     * @return
     * @throws MetaException
     * @throws IOException
     * @throws InvalidInputException
     * @throws InvalidObjectException
     * @throws NoSuchObjectException
     */
    private List<Path> dropPartitionsAndGetLocations(RawStore ms, String catName, String dbName,
      String tableName, Path tablePath, List<FieldSchema> partitionKeys, boolean checkLocation)
      throws MetaException, IOException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
      int partitionBatchSize = MetastoreConf.getIntVar(conf,
          ConfVars.BATCH_RETRIEVE_MAX);
      Path tableDnsPath = null;
      if (tablePath != null) {
        tableDnsPath = wh.getDnsPath(tablePath);
      }
      List<Path> partPaths = new ArrayList<>();
      Table tbl = ms.getTable(catName, dbName, tableName);

      // call dropPartition on each of the table's partitions to follow the
      // procedure for cleanly dropping partitions.
      while (true) {
        List<Partition> partsToDelete = ms.getPartitions(catName, dbName, tableName, partitionBatchSize);
        if (partsToDelete == null || partsToDelete.isEmpty()) {
          break;
        }
        List<String> partNames = new ArrayList<>();
        for (Partition part : partsToDelete) {
          if (checkLocation && part.getSd() != null &&
              part.getSd().getLocation() != null) {

            Path partPath = wh.getDnsPath(new Path(part.getSd().getLocation()));
            if (tableDnsPath == null ||
                (partPath != null && !isSubdirectory(tableDnsPath, partPath))) {
              if (!wh.isWritable(partPath.getParent())) {
                throw new MetaException("Table metadata not deleted since the partition " +
                    Warehouse.makePartName(partitionKeys, part.getValues()) +
                    " has parent location " + partPath.getParent() + " which is not writable " +
                    "by " + SecurityUtils.getUser());
              }
              partPaths.add(partPath);
            }
          }
          partNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
        }
        for (MetaStoreEventListener listener : listeners) {
          //No drop part listener events fired for public listeners historically, for drop table case.
          //Limiting to internal listeners for now, to avoid unexpected calls for public listeners.
          if (listener instanceof HMSMetricsListener) {
            for (@SuppressWarnings("unused") Partition part : partsToDelete) {
              listener.onDropPartition(null);
            }
          }
        }
        ms.dropPartitions(catName, dbName, tableName, partNames);
      }

      return partPaths;
    }

    @Override
    public void drop_table(final String dbname, final String name, final boolean deleteData)
        throws NoSuchObjectException, MetaException {
      drop_table_with_environment_context(dbname, name, deleteData, null);
    }

    @Override
    public void drop_table_with_environment_context(final String dbname, final String name,
        final boolean deleteData, final EnvironmentContext envContext)
        throws NoSuchObjectException, MetaException {
      String[] parsedDbName = parseDbName(dbname, conf);
      startTableFunction("drop_table", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name);

      boolean success = false;
      Exception ex = null;
      try {
        success = drop_table_core(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name,
            deleteData, envContext, null);
      } catch (IOException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("drop_table", success, ex, name);
      }

    }

    private void updateStatsForTruncate(Map<String,String> props, EnvironmentContext environmentContext) {
      if (null == props) {
        return;
      }
      for (String stat : StatsSetupConst.supportedStats) {
        String statVal = props.get(stat);
        if (statVal != null) {
          //In the case of truncate table, we set the stats to be 0.
          props.put(stat, "0");
        }
      }
      //first set basic stats to true
      StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
      environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
      //then invalidate column stats
      StatsSetupConst.clearColumnStatsState(props);
      return;
    }

    private void alterPartitionForTruncate(final RawStore ms,
                                           final String catName,
                                           final String dbName,
                                           final String tableName,
                                           final Table table,
                                           final Partition partition) throws Exception {
      EnvironmentContext environmentContext = new EnvironmentContext();
      updateStatsForTruncate(partition.getParameters(), environmentContext);

      if (!transactionalListeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.ALTER_PARTITION,
                new AlterPartitionEvent(partition, partition, table, true, true, this));
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
                EventType.ALTER_PARTITION,
                new AlterPartitionEvent(partition, partition, table, true, true, this));
      }

      alterHandler.alterPartition(ms, wh, catName, dbName, tableName, null, partition,
          environmentContext, this);
    }

    private void alterTableStatsForTruncate(final RawStore ms,
                                            final String catName,
                                            final String dbName,
                                            final String tableName,
                                            final Table table,
                                            final List<String> partNames) throws Exception {
      if (partNames == null) {
        if (0 != table.getPartitionKeysSize()) {
          for (Partition partition : ms.getPartitions(catName, dbName, tableName, Integer.MAX_VALUE)) {
            alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition);
          }
        } else {
          EnvironmentContext environmentContext = new EnvironmentContext();
          updateStatsForTruncate(table.getParameters(), environmentContext);

          if (!transactionalListeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.ALTER_TABLE,
                    new AlterTableEvent(table, table, true, true, this));
          }

          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                    EventType.ALTER_TABLE,
                    new AlterTableEvent(table, table, true, true, this));
          }

          alterHandler.alterTable(ms, wh, catName, dbName, tableName, table, environmentContext, this);
        }
      } else {
        for (Partition partition : ms.getPartitionsByNames(catName, dbName, tableName, partNames)) {
          alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition);
        }
      }
      return;
    }

    private List<Path> getLocationsForTruncate(final RawStore ms,
                                               final String catName,
                                               final String dbName,
                                               final String tableName,
                                               final Table table,
                                               final List<String> partNames) throws Exception {
      List<Path> locations = new ArrayList<>();
      if (partNames == null) {
        if (0 != table.getPartitionKeysSize()) {
          for (Partition partition : ms.getPartitions(catName, dbName, tableName, Integer.MAX_VALUE)) {
            locations.add(new Path(partition.getSd().getLocation()));
          }
        } else {
          locations.add(new Path(table.getSd().getLocation()));
        }
      } else {
        for (Partition partition : ms.getPartitionsByNames(catName, dbName, tableName, partNames)) {
          locations.add(new Path(partition.getSd().getLocation()));
        }
      }
      return locations;
    }

    @Override
    public CmRecycleResponse cm_recycle(final CmRecycleRequest request) throws MetaException {
      wh.recycleDirToCmPath(new Path(request.getDataPath()), request.isPurge());
      return new CmRecycleResponse();
    }

    @Override
    public void truncate_table(final String dbName, final String tableName, List<String> partNames)
      throws NoSuchObjectException, MetaException {
      try {
        String[] parsedDbName = parseDbName(dbName, conf);
        Table tbl = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);
        boolean isAutopurge = (tbl.isSetParameters() && "true".equalsIgnoreCase(tbl.getParameters().get("auto.purge")));

        // This is not transactional
        for (Path location : getLocationsForTruncate(getMS(), parsedDbName[CAT_NAME],
            parsedDbName[DB_NAME], tableName, tbl, partNames)) {
          FileSystem fs = location.getFileSystem(getConf());
          if (!org.apache.hadoop.hive.metastore.utils.HdfsUtils.isPathEncrypted(getConf(), fs.getUri(), location) &&
              !FileUtils.pathHasSnapshotSubDir(location, fs)) {
            HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(getConf(), fs, location);
            FileStatus targetStatus = fs.getFileStatus(location);
            String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
            wh.deleteDir(location, true, isAutopurge);
            fs.mkdirs(location);
            HdfsUtils.setFullFileStatus(getConf(), status, targetGroup, fs, location, false);
          } else {
            FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
            if (statuses == null || statuses.length == 0) {
              continue;
            }
            for (final FileStatus status : statuses) {
              wh.deleteDir(status.getPath(), true, isAutopurge);
            }
          }
        }

        // Alter the table/partition stats and also notify truncate table event
        alterTableStatsForTruncate(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName, tbl, partNames);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof NoSuchObjectException) {
          throw (NoSuchObjectException) e;
        } else {
          throw newMetaException(e);
        }
      }
    }

    /**
     * Is this an external table?
     *
     * @param table
     *          Check if this table is external.
     * @return True if the table is external, otherwise false.
     */
    private boolean isExternal(Table table) {
      return MetaStoreUtils.isExternalTable(table);
    }

    @Override
    @Deprecated
    public Table get_table(final String dbname, final String name) throws MetaException,
        NoSuchObjectException {
      String[] parsedDbName = parseDbName(dbname, conf);
      return getTableInternal(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, null);
    }

    @Override
    public GetTableResult get_table_req(GetTableRequest req) throws MetaException,
        NoSuchObjectException {
      String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
      return new GetTableResult(getTableInternal(catName, req.getDbName(), req.getTblName(),
          req.getCapabilities()));
    }

    private Table getTableInternal(String catName, String dbname, String name,
        ClientCapabilities capabilities) throws MetaException, NoSuchObjectException {
      if (isInTest) {
        assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY,
            "Hive tests", "get_table_req");
      }

      Table t = null;
      startTableFunction("get_table", catName, dbname, name);
      Exception ex = null;
      try {
        t = get_table_core(catName, dbname, name);
        if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
          assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES,
              "insert-only tables", "get_table_req");
        }
        firePreEvent(new PreReadTableEvent(t, this));
      } catch (MetaException | NoSuchObjectException e) {
        ex = e;
        throw e;
      } finally {
        endFunction("get_table", t != null, ex, name);
      }
      return t;
    }


    @Override
    public List<TableMeta> get_table_meta(String dbnames, String tblNames, List<String> tblTypes)
        throws MetaException, NoSuchObjectException {
      List<TableMeta> t = null;
      String[] parsedDbName = parseDbName(dbnames, conf);
      startTableFunction("get_table_metas", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblNames);
      Exception ex = null;
      try {
        t = getMS().getTableMeta(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblNames, tblTypes);
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("get_table_metas", t != null, ex);
      }
      return t;
    }

    @Override
    public Table get_table_core(final String catName, final String dbname, final String name)
        throws MetaException, NoSuchObjectException {
      Table t = null;
      try {
        t = getMS().getTable(catName, dbname, name);
        if (t == null) {
          throw new NoSuchObjectException(getCatalogQualifiedTableName(catName, dbname, name) +
            " table not found");
        }
      } catch (Exception e) {
        throwMetaException(e);
      }
      return t;
    }

    /**
     * Gets multiple tables from the hive metastore.
     *
     * @param dbName
     *          The name of the database in which the tables reside
     * @param tableNames
     *          The names of the tables to get.
     *
     * @return A list of tables whose names are in the the list "names" and
     *         are retrievable from the database specified by "dbnames."
     *         There is no guarantee of the order of the returned tables.
     *         If there are duplicate names, only one instance of the table will be returned.
     * @throws MetaException
     * @throws InvalidOperationException
     * @throws UnknownDBException
     */
    @Override
    @Deprecated
    public List<Table> get_table_objects_by_name(final String dbName, final List<String> tableNames)
        throws MetaException, InvalidOperationException, UnknownDBException {
      String[] parsedDbName = parseDbName(dbName, conf);
      return getTableObjectsInternal(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableNames, null);
    }

    @Override
    public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req) throws TException {
      String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
      return new GetTablesResult(getTableObjectsInternal(catName,
          req.getDbName(), req.getTblNames(), req.getCapabilities()));
    }

    private List<Table> getTableObjectsInternal(String catName, String dbName,
                                                List<String> tableNames,
                                                ClientCapabilities capabilities)
            throws MetaException, InvalidOperationException, UnknownDBException {
      if (isInTest) {
        assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY,
            "Hive tests", "get_table_objects_by_name_req");
      }
      List<Table> tables = new ArrayList<>();
      startMultiTableFunction("get_multi_table", dbName, tableNames);
      Exception ex = null;
      int tableBatchSize = MetastoreConf.getIntVar(conf,
          ConfVars.BATCH_RETRIEVE_MAX);

      try {
        if (dbName == null || dbName.isEmpty()) {
          throw new UnknownDBException("DB name is null or empty");
        }
        if (tableNames == null) {
          throw new InvalidOperationException(dbName + " cannot find null tables");
        }

        // The list of table names could contain duplicates. RawStore.getTableObjectsByName()
        // only guarantees returning no duplicate table objects in one batch. If we need
        // to break into multiple batches, remove duplicates first.
        List<String> distinctTableNames = tableNames;
        if (distinctTableNames.size() > tableBatchSize) {
          List<String> lowercaseTableNames = new ArrayList<>();
          for (String tableName : tableNames) {
            lowercaseTableNames.add(org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier(tableName));
          }
          distinctTableNames = new ArrayList<>(new HashSet<>(lowercaseTableNames));
        }

        RawStore ms = getMS();
        int startIndex = 0;
        // Retrieve the tables from the metastore in batches. Some databases like
        // Oracle cannot have over 1000 expressions in a in-list
        while (startIndex < distinctTableNames.size()) {
          int endIndex = Math.min(startIndex + tableBatchSize, distinctTableNames.size());
          tables.addAll(ms.getTableObjectsByName(catName, dbName, distinctTableNames.subList(
              startIndex, endIndex)));
          startIndex = endIndex;
        }
        for (Table t : tables) {
          if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
            assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES,
                "insert-only tables", "get_table_req");
          }
        }
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidOperationException) {
          throw (InvalidOperationException) e;
        } else if (e instanceof UnknownDBException) {
          throw (UnknownDBException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_multi_table", tables != null, ex, join(tableNames, ","));
      }
      return tables;
    }

    @Override
    public Map<String, Materialization> get_materialization_invalidation_info(final String dbName, final List<String> tableNames) {
      return MaterializationsInvalidationCache.get().getMaterializationInvalidationInfo(dbName, tableNames);
    }

    @Override
    public void update_creation_metadata(String catName, final String dbName, final String tableName, CreationMetadata cm) throws MetaException {
      getMS().updateCreationMetadata(catName, dbName, tableName, cm);
    }

    private void assertClientHasCapability(ClientCapabilities client,
        ClientCapability value, String what, String call) throws MetaException {
      if (!doesClientHaveCapability(client, value)) {
        throw new MetaException("Your client does not appear to support " + what + ". To skip"
            + " capability checks, please set " + ConfVars.CAPABILITY_CHECK.toString()
            + " to false. This setting can be set globally, or on the client for the current"
            + " metastore session. Note that this may lead to incorrect results, data loss,"
            + " undefined behavior, etc. if your client is actually incompatible. You can also"
            + " specify custom client capabilities via " + call + " API.");
      }
    }

    private boolean doesClientHaveCapability(ClientCapabilities client, ClientCapability value) {
      if (!MetastoreConf.getBoolVar(getConf(), ConfVars.CAPABILITY_CHECK)) {
        return true;
      }
      return (client != null && client.isSetValues() && client.getValues().contains(value));
    }

    @Override
    public List<String> get_table_names_by_filter(
        final String dbName, final String filter, final short maxTables)
        throws MetaException, InvalidOperationException, UnknownDBException {
      List<String> tables = null;
      startFunction("get_table_names_by_filter", ": db = " + dbName + ", filter = " + filter);
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbName, conf);
      try {
        if (parsedDbName[CAT_NAME] == null || parsedDbName[CAT_NAME].isEmpty() ||
            parsedDbName[DB_NAME] == null || parsedDbName[DB_NAME].isEmpty()) {
          throw new UnknownDBException("DB name is null or empty");
        }
        if (filter == null) {
          throw new InvalidOperationException(filter + " cannot apply null filter");
        }
        tables = getMS().listTableNamesByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], filter, maxTables);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidOperationException) {
          throw (InvalidOperationException) e;
        } else if (e instanceof UnknownDBException) {
          throw (UnknownDBException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_table_names_by_filter", tables != null, ex, join(tables, ","));
      }
      return tables;
    }

    private Partition append_partition_common(RawStore ms, String catName, String dbName,
                                              String tableName, List<String> part_vals,
                                              EnvironmentContext envContext)
        throws InvalidObjectException, AlreadyExistsException, MetaException {

      Partition part = new Partition();
      boolean success = false, madeDir = false;
      Path partLocation = null;
      Table tbl = null;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      try {
        ms.openTransaction();
        part.setCatName(catName);
        part.setDbName(dbName);
        part.setTableName(tableName);
        part.setValues(part_vals);

        MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);

        tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName());
        if (tbl == null) {
          throw new InvalidObjectException(
              "Unable to add partition because table or database do not exist");
        }
        if (tbl.getSd().getLocation() == null) {
          throw new MetaException(
              "Cannot append a partition to a view");
        }

        firePreEvent(new PreAddPartitionEvent(tbl, part, this));

        part.setSd(tbl.getSd().deepCopy());
        partLocation = new Path(tbl.getSd().getLocation(), Warehouse
            .makePartName(tbl.getPartitionKeys(), part_vals));
        part.getSd().setLocation(partLocation.toString());

        Partition old_part;
        try {
          old_part = ms.getPartition(part.getCatName(), part.getDbName(), part
              .getTableName(), part.getValues());
        } catch (NoSuchObjectException e) {
          // this means there is no existing partition
          old_part = null;
        }
        if (old_part != null) {
          throw new AlreadyExistsException("Partition already exists:" + part);
        }

        if (!wh.isDir(partLocation)) {
          if (!wh.mkdirs(partLocation)) {
            throw new MetaException(partLocation
                + " is not a directory or unable to create one");
          }
          madeDir = true;
        }

        // set create time
        long time = System.currentTimeMillis() / 1000;
        part.setCreateTime((int) time);
        part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));

        if (MetastoreConf.getBoolVar(conf, ConfVars.STATS_AUTO_GATHER) &&
            !MetaStoreUtils.isView(tbl)) {
          MetaStoreUtils.updatePartitionStatsFast(part, tbl, wh, madeDir, false, envContext, true);
        }

        if (ms.addPartition(part)) {
          if (!transactionalListeners.isEmpty()) {
            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.ADD_PARTITION,
                                                      new AddPartitionEvent(tbl, part, true, this),
                                                      envContext);
          }

          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(partLocation, true);
          }
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.ADD_PARTITION,
                                                new AddPartitionEvent(tbl, part, success, this),
                                                envContext,
                                                transactionalListenerResponses, ms);
        }
      }
      return part;
    }

    private void firePreEvent(PreEventContext event) throws MetaException {
      for (MetaStorePreEventListener listener : preListeners) {
        try {
          listener.onEvent(event);
        } catch (NoSuchObjectException e) {
          throw new MetaException(e.getMessage());
        } catch (InvalidOperationException e) {
          throw new MetaException(e.getMessage());
        }
      }
    }

    @Override
    public Partition append_partition(final String dbName, final String tableName,
        final List<String> part_vals) throws InvalidObjectException,
        AlreadyExistsException, MetaException {
      return append_partition_with_environment_context(dbName, tableName, part_vals, null);
    }

    @Override
    public Partition append_partition_with_environment_context(final String dbName,
        final String tableName, final List<String> part_vals, final EnvironmentContext envContext)
        throws InvalidObjectException, AlreadyExistsException, MetaException {
      if (part_vals == null || part_vals.isEmpty()) {
        throw new MetaException("The partition values must not be null.");
      }
      String[] parsedDbName = parseDbName(dbName, conf);
      startPartitionFunction("append_partition", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, part_vals);
      if (LOG.isDebugEnabled()) {
        for (String part : part_vals) {
          LOG.debug(part);
        }
      }

      Partition ret = null;
      Exception ex = null;
      try {
        ret = append_partition_common(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, part_vals, envContext);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("append_partition", ret != null, ex, tableName);
      }
      return ret;
    }

    private static class PartValEqWrapper {
      Partition partition;

      PartValEqWrapper(Partition partition) {
        this.partition = partition;
      }

      @Override
      public int hashCode() {
        return partition.isSetValues() ? partition.getValues().hashCode() : 0;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null || !(obj instanceof PartValEqWrapper)) {
          return false;
        }
        Partition p1 = this.partition, p2 = ((PartValEqWrapper)obj).partition;
        if (!p1.isSetValues() || !p2.isSetValues()) {
          return p1.isSetValues() == p2.isSetValues();
        }
        if (p1.getValues().size() != p2.getValues().size()) {
          return false;
        }
        for (int i = 0; i < p1.getValues().size(); ++i) {
          String v1 = p1.getValues().get(i);
          String v2 = p2.getValues().get(i);
          if (v1 == null && v2 == null) {
            continue;
          }
          if (v1 == null || !v1.equals(v2)) {
            return false;
          }
        }
        return true;
      }
    }

    private static class PartValEqWrapperLite {
      List<String> values;
      String location;

      PartValEqWrapperLite(Partition partition) {
        this.values = partition.isSetValues()? partition.getValues() : null;
        this.location = partition.getSd().getLocation();
      }

      @Override
      public int hashCode() {
        return values == null ? 0 : values.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null || !(obj instanceof PartValEqWrapperLite)) {
          return false;
        }

        List<String> lhsValues = this.values;
        List<String> rhsValues = ((PartValEqWrapperLite)obj).values;

        if (lhsValues == null || rhsValues == null) {
          return lhsValues == rhsValues;
        }

        if (lhsValues.size() != rhsValues.size()) {
          return false;
        }

        for (int i=0; i<lhsValues.size(); ++i) {
          String lhsValue = lhsValues.get(i);
          String rhsValue = rhsValues.get(i);

          if ((lhsValue == null && rhsValue != null)
              || (lhsValue != null && !lhsValue.equals(rhsValue))) {
            return false;
          }
        }

        return true;
      }
    }

    private List<Partition> add_partitions_core(final RawStore ms, String catName,
        String dbName, String tblName, List<Partition> parts, final boolean ifNotExists)
        throws TException {
      logInfo("add_partitions");
      boolean success = false;
      // Ensures that the list doesn't have dups, and keeps track of directories we have created.
      final Map<PartValEqWrapper, Boolean> addedPartitions = new ConcurrentHashMap<>();
      final List<Partition> newParts = new ArrayList<>();
      final List<Partition> existingParts = new ArrayList<>();
      Table tbl = null;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();

      try {
        ms.openTransaction();
        tbl = ms.getTable(catName, dbName, tblName);
        if (tbl == null) {
          throw new InvalidObjectException("Unable to add partitions because "
              + getCatalogQualifiedTableName(catName, dbName, tblName) +
              " does not exist");
        }

        if (!parts.isEmpty()) {
          firePreEvent(new PreAddPartitionEvent(tbl, parts, this));
        }

        List<Future<Partition>> partFutures = Lists.newArrayList();
        final Table table = tbl;
        for (final Partition part : parts) {
          if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
            throw new MetaException("Partition does not belong to target table " +
                getCatalogQualifiedTableName(catName, dbName, tblName) + ": " +
                    part);
          }

          boolean shouldAdd = startAddPartition(ms, part, ifNotExists);
          if (!shouldAdd) {
            existingParts.add(part);
            LOG.info("Not adding partition " + part + " as it already exists");
            continue;
          }

          final UserGroupInformation ugi;
          try {
            ugi = UserGroupInformation.getCurrentUser();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          partFutures.add(threadPool.submit(new Callable<Partition>() {
            @Override
            public Partition call() throws Exception {
              ugi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                  try {
                    boolean madeDir = createLocationForAddedPartition(table, part);
                    if (addedPartitions.put(new PartValEqWrapper(part), madeDir) != null) {
                      // Technically, for ifNotExists case, we could insert one and discard the other
                      // because the first one now "exists", but it seems better to report the problem
                      // upstream as such a command doesn't make sense.
                      throw new MetaException("Duplicate partitions in the list: " + part);
                    }
                    initializeAddedPartition(table, part, madeDir);
                  } catch (MetaException e) {
                    throw new IOException(e.getMessage(), e);
                  }
                  return null;
                }
              });
              return part;
            }
          }));
        }

        try {
          for (Future<Partition> partFuture : partFutures) {
            Partition part = partFuture.get();
            if (part != null) {
              newParts.add(part);
            }
          }
        } catch (InterruptedException | ExecutionException e) {
          // cancel other tasks
          for (Future<Partition> partFuture : partFutures) {
            partFuture.cancel(true);
          }
          throw new MetaException(e.getMessage());
        }

        if (!newParts.isEmpty()) {
          success = ms.addPartitions(catName, dbName, tblName, newParts);
        } else {
          success = true;
        }

        // Setting success to false to make sure that if the listener fails, rollback happens.
        success = false;
        // Notification is generated for newly created partitions only. The subset of partitions
        // that already exist (existingParts), will not generate notifications.
        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.ADD_PARTITION,
                                                    new AddPartitionEvent(tbl, newParts, true, this));
        }

        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          for (Map.Entry<PartValEqWrapper, Boolean> e : addedPartitions.entrySet()) {
            if (e.getValue()) {
              // we just created this directory - it's not a case of pre-creation, so we nuke.
              wh.deleteDir(new Path(e.getKey().partition.getSd().getLocation()), true);
            }
          }

          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                                                  EventType.ADD_PARTITION,
                                                  new AddPartitionEvent(tbl, parts, false, this),
                                                  null, null, ms);
          }
        } else {
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                                                  EventType.ADD_PARTITION,
                                                  new AddPartitionEvent(tbl, newParts, true, this),
                                                  null,
                                                  transactionalListenerResponses, ms);

            if (!existingParts.isEmpty()) {
              // The request has succeeded but we failed to add these partitions.
              MetaStoreListenerNotifier.notifyEvent(listeners,
                                                    EventType.ADD_PARTITION,
                                                    new AddPartitionEvent(tbl, existingParts, false, this),
                                                    null, null, ms);
            }
          }
        }
      }
      return newParts;
    }

    @Override
    public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
        throws TException {
      AddPartitionsResult result = new AddPartitionsResult();
      if (request.getParts().isEmpty()) {
        return result;
      }
      try {
        if (!request.isSetCatName()) request.setCatName(getDefaultCatalog(conf));
        // Make sure all of the partitions have the catalog set as well
        request.getParts().forEach(p -> {
          if (!p.isSetCatName()) p.setCatName(getDefaultCatalog(conf));
        });
        List<Partition> parts = add_partitions_core(getMS(), request.getCatName(), request.getDbName(),
            request.getTblName(), request.getParts(), request.isIfNotExists());
        if (request.isNeedResult()) {
          result.setPartitions(parts);
        }
      } catch (TException te) {
        throw te;
      } catch (Exception e) {
        throw newMetaException(e);
      }
      return result;
    }

    @Override
    public int add_partitions(final List<Partition> parts) throws MetaException,
        InvalidObjectException, AlreadyExistsException {
      startFunction("add_partition");
      if (parts.size() == 0) {
        return 0;
      }

      Integer ret = null;
      Exception ex = null;
      try {
        // Old API assumed all partitions belong to the same table; keep the same assumption
        if (!parts.get(0).isSetCatName()) {
          String defaultCat = getDefaultCatalog(conf);
          for (Partition p : parts) p.setCatName(defaultCat);
        }
        ret = add_partitions_core(getMS(), parts.get(0).getCatName(), parts.get(0).getDbName(),
            parts.get(0).getTableName(), parts, false).size();
        assert ret == parts.size();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        String tableName = parts.get(0).getTableName();
        endFunction("add_partition", ret != null, ex, tableName);
      }
      return ret;
    }

    @Override
    public int add_partitions_pspec(final List<PartitionSpec> partSpecs)
        throws TException {
      logInfo("add_partitions_pspec");

      if (partSpecs.isEmpty()) {
        return 0;
      }

      String dbName = partSpecs.get(0).getDbName();
      String tableName = partSpecs.get(0).getTableName();
      // If the catalog name isn't set, we need to go through and set it.
      String catName;
      if (!partSpecs.get(0).isSetCatName()) {
        catName = getDefaultCatalog(conf);
        partSpecs.forEach(ps -> ps.setCatName(catName));
      } else {
        catName = partSpecs.get(0).getCatName();
      }

      return add_partitions_pspec_core(getMS(), catName, dbName, tableName, partSpecs, false);
    }

    private int add_partitions_pspec_core(RawStore ms, String catName, String dbName,
                                          String tblName, List<PartitionSpec> partSpecs,
                                          boolean ifNotExists)
        throws TException {
      boolean success = false;
      // Ensures that the list doesn't have dups, and keeps track of directories we have created.
      final Map<PartValEqWrapperLite, Boolean> addedPartitions = new ConcurrentHashMap<>();
      PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(partSpecs);
      final PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpecProxy
          .getPartitionIterator();
      Table tbl = null;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      try {
        ms.openTransaction();
        tbl = ms.getTable(catName, dbName, tblName);
        if (tbl == null) {
          throw new InvalidObjectException("Unable to add partitions because "
              + "database or table " + dbName + "." + tblName + " does not exist");
        }

        firePreEvent(new PreAddPartitionEvent(tbl, partitionSpecProxy, this));
        List<Future<Partition>> partFutures = Lists.newArrayList();
        final Table table = tbl;
        while(partitionIterator.hasNext()) {
          final Partition part = partitionIterator.getCurrent();

          if (!part.getTableName().equalsIgnoreCase(tblName) || !part.getDbName().equalsIgnoreCase(dbName)) {
            throw new MetaException("Partition does not belong to target table "
                + dbName + "." + tblName + ": " + part);
          }

          boolean shouldAdd = startAddPartition(ms, part, ifNotExists);
          if (!shouldAdd) {
            LOG.info("Not adding partition " + part + " as it already exists");
            continue;
          }

          final UserGroupInformation ugi;
          try {
            ugi = UserGroupInformation.getCurrentUser();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          partFutures.add(threadPool.submit(new Callable<Partition>() {
            @Override public Partition call() throws Exception {
              ugi.doAs(new PrivilegedExceptionAction<Partition>() {
                @Override
                public Partition run() throws Exception {
                  try {
                    boolean madeDir = createLocationForAddedPartition(table, part);
                    if (addedPartitions.put(new PartValEqWrapperLite(part), madeDir) != null) {
                      // Technically, for ifNotExists case, we could insert one and discard the other
                      // because the first one now "exists", but it seems better to report the problem
                      // upstream as such a command doesn't make sense.
                      throw new MetaException("Duplicate partitions in the list: " + part);
                    }
                    initializeAddedPartition(table, part, madeDir);
                  } catch (MetaException e) {
                    throw new IOException(e.getMessage(), e);
                  }
                  return null;
                }
              });
              return part;
            }
          }));
          partitionIterator.next();
        }

        try {
          for (Future<Partition> partFuture : partFutures) {
            partFuture.get();
          }
        } catch (InterruptedException | ExecutionException e) {
          // cancel other tasks
          for (Future<Partition> partFuture : partFutures) {
            partFuture.cancel(true);
          }
          throw new MetaException(e.getMessage());
        }

        success = ms.addPartitions(catName, dbName, tblName, partitionSpecProxy, ifNotExists);
        //setting success to false to make sure that if the listener fails, rollback happens.
        success = false;

        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.ADD_PARTITION,
                                                    new AddPartitionEvent(tbl, partitionSpecProxy, true, this));
        }

        success = ms.commitTransaction();
        return addedPartitions.size();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
          for (Map.Entry<PartValEqWrapperLite, Boolean> e : addedPartitions.entrySet()) {
            if (e.getValue()) {
              // we just created this directory - it's not a case of pre-creation, so we nuke.
              wh.deleteDir(new Path(e.getKey().location), true);
            }
          }
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.ADD_PARTITION,
                                                new AddPartitionEvent(tbl, partitionSpecProxy, true, this),
                                                null,
                                                transactionalListenerResponses, ms);
        }
      }
    }

    private boolean startAddPartition(
        RawStore ms, Partition part, boolean ifNotExists) throws TException {
      MetaStoreUtils.validatePartitionNameCharacters(part.getValues(),
          partitionValidationPattern);
      boolean doesExist = ms.doesPartitionExist(part.getCatName(),
          part.getDbName(), part.getTableName(), part.getValues());
      if (doesExist && !ifNotExists) {
        throw new AlreadyExistsException("Partition already exists: " + part);
      }
      return !doesExist;
    }

    /**
     * Handles the location for a partition being created.
     * @param tbl Table.
     * @param part Partition.
     * @return Whether the partition SD location is set to a newly created directory.
     */
    private boolean createLocationForAddedPartition(
        final Table tbl, final Partition part) throws MetaException {
      Path partLocation = null;
      String partLocationStr = null;
      if (part.getSd() != null) {
        partLocationStr = part.getSd().getLocation();
      }

      if (partLocationStr == null || partLocationStr.isEmpty()) {
        // set default location if not specified and this is
        // a physical table partition (not a view)
        if (tbl.getSd().getLocation() != null) {
          partLocation = new Path(tbl.getSd().getLocation(), Warehouse
              .makePartName(tbl.getPartitionKeys(), part.getValues()));
        }
      } else {
        if (tbl.getSd().getLocation() == null) {
          throw new MetaException("Cannot specify location for a view partition");
        }
        partLocation = wh.getDnsPath(new Path(partLocationStr));
      }

      boolean result = false;
      if (partLocation != null) {
        part.getSd().setLocation(partLocation.toString());

        // Check to see if the directory already exists before calling
        // mkdirs() because if the file system is read-only, mkdirs will
        // throw an exception even if the directory already exists.
        if (!wh.isDir(partLocation)) {
          if (!wh.mkdirs(partLocation)) {
            throw new MetaException(partLocation
                + " is not a directory or unable to create one");
          }
          result = true;
        }
      }
      return result;
    }

    private void initializeAddedPartition(
        final Table tbl, final Partition part, boolean madeDir) throws MetaException {
      initializeAddedPartition(tbl, new PartitionSpecProxy.SimplePartitionWrapperIterator(part), madeDir);
    }

    private void initializeAddedPartition(
        final Table tbl, final PartitionSpecProxy.PartitionIterator part, boolean madeDir) throws MetaException {
      if (MetastoreConf.getBoolVar(conf, ConfVars.STATS_AUTO_GATHER) &&
          !MetaStoreUtils.isView(tbl)) {
        MetaStoreUtils.updatePartitionStatsFast(part, tbl, wh, madeDir, false, null, true);
      }

      // set create time
      long time = System.currentTimeMillis() / 1000;
      part.setCreateTime((int) time);
      if (part.getParameters() == null ||
          part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
        part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
      }

      // Inherit table properties into partition properties.
      Map<String, String> tblParams = tbl.getParameters();
      String inheritProps = MetastoreConf.getVar(conf, ConfVars.PART_INHERIT_TBL_PROPS).trim();
      // Default value is empty string in which case no properties will be inherited.
      // * implies all properties needs to be inherited
      Set<String> inheritKeys = new HashSet<>(Arrays.asList(inheritProps.split(",")));
      if (inheritKeys.contains("*")) {
        inheritKeys = tblParams.keySet();
      }

      for (String key : inheritKeys) {
        String paramVal = tblParams.get(key);
        if (null != paramVal) { // add the property only if it exists in table properties
          part.putToParameters(key, paramVal);
        }
      }
    }

    private Partition add_partition_core(final RawStore ms,
        final Partition part, final EnvironmentContext envContext)
        throws TException {
      boolean success = false;
      Table tbl = null;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      if (!part.isSetCatName()) part.setCatName(getDefaultCatalog(conf));
      try {
        ms.openTransaction();
        tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName());
        if (tbl == null) {
          throw new InvalidObjectException(
              "Unable to add partition because table or database do not exist");
        }

        firePreEvent(new PreAddPartitionEvent(tbl, part, this));

        boolean shouldAdd = startAddPartition(ms, part, false);
        assert shouldAdd; // start would throw if it already existed here
        boolean madeDir = createLocationForAddedPartition(tbl, part);
        try {
          initializeAddedPartition(tbl, part, madeDir);
          success = ms.addPartition(part);
        } finally {
          if (!success && madeDir) {
            wh.deleteDir(new Path(part.getSd().getLocation()), true);
          }
        }

        // Setting success to false to make sure that if the listener fails, rollback happens.
        success = false;

        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.ADD_PARTITION,
                                                    new AddPartitionEvent(tbl, Arrays.asList(part), true, this),
                                                    envContext);

        }

        // we proceed only if we'd actually succeeded anyway, otherwise,
        // we'd have thrown an exception
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.ADD_PARTITION,
                                                new AddPartitionEvent(tbl, Arrays.asList(part), success, this),
                                                envContext,
                                                transactionalListenerResponses, ms);

        }
      }
      return part;
    }

    @Override
    public Partition add_partition(final Partition part)
        throws InvalidObjectException, AlreadyExistsException, MetaException {
      return add_partition_with_environment_context(part, null);
    }

    @Override
    public Partition add_partition_with_environment_context(
        final Partition part, EnvironmentContext envContext)
        throws InvalidObjectException, AlreadyExistsException,
        MetaException {
      startTableFunction("add_partition",
          part.getCatName(), part.getDbName(), part.getTableName());
      Partition ret = null;
      Exception ex = null;
      try {
        ret = add_partition_core(getMS(), part, envContext);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("add_partition", ret != null, ex, part != null ?  part.getTableName(): null);
      }
      return ret;
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs,
        String sourceDbName, String sourceTableName, String destDbName,
        String destTableName) throws TException {
      exchange_partitions(partitionSpecs, sourceDbName, sourceTableName, destDbName, destTableName);
      // Wouldn't it make more sense to return the first element of the list returned by the
      // previous call?
      return new Partition();
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
        String sourceDbName, String sourceTableName, String destDbName,
        String destTableName) throws TException {
      String[] parsedDestDbName = parseDbName(destDbName, conf);
      String[] parsedSourceDbName = parseDbName(sourceDbName, conf);
      // No need to check catalog for null as parseDbName() will never return null for the catalog.
      if (partitionSpecs == null || parsedSourceDbName[DB_NAME] == null || sourceTableName == null
          || parsedDestDbName[DB_NAME] == null || destTableName == null) {
        throw new MetaException("The DB and table name for the source and destination tables,"
            + " and the partition specs must not be null.");
      }
      if (!parsedDestDbName[CAT_NAME].equals(parsedSourceDbName[CAT_NAME])) {
        throw new MetaException("You cannot move a partition across catalogs");
      }

      boolean success = false;
      boolean pathCreated = false;
      RawStore ms = getMS();
      ms.openTransaction();

      Table destinationTable =
          ms.getTable(parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME], destTableName);
      if (destinationTable == null) {
        throw new MetaException( "The destination table " +
            getCatalogQualifiedTableName(parsedDestDbName[CAT_NAME],
                parsedDestDbName[DB_NAME], destTableName) + " not found");
      }
      Table sourceTable =
          ms.getTable(parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME], sourceTableName);
      if (sourceTable == null) {
        throw new MetaException("The source table " +
            getCatalogQualifiedTableName(parsedSourceDbName[CAT_NAME],
                parsedSourceDbName[DB_NAME], sourceTableName) + " not found");
      }
      List<String> partVals = MetaStoreUtils.getPvals(sourceTable.getPartitionKeys(),
          partitionSpecs);
      List<String> partValsPresent = new ArrayList<> ();
      List<FieldSchema> partitionKeysPresent = new ArrayList<> ();
      int i = 0;
      for (FieldSchema fs: sourceTable.getPartitionKeys()) {
        String partVal = partVals.get(i);
        if (partVal != null && !partVal.equals("")) {
          partValsPresent.add(partVal);
          partitionKeysPresent.add(fs);
        }
        i++;
      }
      // Passed the unparsed DB name here, as get_partitions_ps expects to parse it
      List<Partition> partitionsToExchange = get_partitions_ps(sourceDbName, sourceTableName,
          partVals, (short)-1);
      if (partitionsToExchange == null || partitionsToExchange.isEmpty()) {
        throw new MetaException("No partition is found with the values " + partitionSpecs
            + " for the table " + sourceTableName);
      }
      boolean sameColumns = MetaStoreUtils.compareFieldColumns(
          sourceTable.getSd().getCols(), destinationTable.getSd().getCols());
      boolean samePartitions = MetaStoreUtils.compareFieldColumns(
          sourceTable.getPartitionKeys(), destinationTable.getPartitionKeys());
      if (!sameColumns || !samePartitions) {
        throw new MetaException("The tables have different schemas." +
            " Their partitions cannot be exchanged.");
      }
      Path sourcePath = new Path(sourceTable.getSd().getLocation(),
          Warehouse.makePartName(partitionKeysPresent, partValsPresent));
      Path destPath = new Path(destinationTable.getSd().getLocation(),
          Warehouse.makePartName(partitionKeysPresent, partValsPresent));
      List<Partition> destPartitions = new ArrayList<>();

      Map<String, String> transactionalListenerResponsesForAddPartition = Collections.emptyMap();
      List<Map<String, String>> transactionalListenerResponsesForDropPartition =
          Lists.newArrayListWithCapacity(partitionsToExchange.size());

      // Check if any of the partitions already exists in destTable.
      List<String> destPartitionNames = ms.listPartitionNames(parsedDestDbName[CAT_NAME],
          parsedDestDbName[DB_NAME], destTableName, (short) -1);
      if (destPartitionNames != null && !destPartitionNames.isEmpty()) {
        for (Partition partition : partitionsToExchange) {
          String partToExchangeName =
              Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues());
          if (destPartitionNames.contains(partToExchangeName)) {
            throw new MetaException("The partition " + partToExchangeName
                + " already exists in the table " + destTableName);
          }
        }
      }

      try {
        for (Partition partition: partitionsToExchange) {
          Partition destPartition = new Partition(partition);
          destPartition.setDbName(parsedDestDbName[DB_NAME]);
          destPartition.setTableName(destinationTable.getTableName());
          Path destPartitionPath = new Path(destinationTable.getSd().getLocation(),
              Warehouse.makePartName(destinationTable.getPartitionKeys(), partition.getValues()));
          destPartition.getSd().setLocation(destPartitionPath.toString());
          ms.addPartition(destPartition);
          destPartitions.add(destPartition);
          ms.dropPartition(parsedSourceDbName[CAT_NAME], partition.getDbName(), sourceTable.getTableName(),
            partition.getValues());
        }
        Path destParentPath = destPath.getParent();
        if (!wh.isDir(destParentPath)) {
          if (!wh.mkdirs(destParentPath)) {
              throw new MetaException("Unable to create path " + destParentPath);
          }
        }
        /*
         * TODO: Use the hard link feature of hdfs
         * once https://issues.apache.org/jira/browse/HDFS-3370 is done
         */
        pathCreated = wh.renameDir(sourcePath, destPath, false);

        // Setting success to false to make sure that if the listener fails, rollback happens.
        success = false;

        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponsesForAddPartition =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.ADD_PARTITION,
                                                    new AddPartitionEvent(destinationTable, destPartitions, true, this));

          for (Partition partition : partitionsToExchange) {
            DropPartitionEvent dropPartitionEvent =
                new DropPartitionEvent(sourceTable, partition, true, true, this);
            transactionalListenerResponsesForDropPartition.add(
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.DROP_PARTITION,
                                                      dropPartitionEvent));
          }
        }

        success = ms.commitTransaction();
        return destPartitions;
      } finally {
        if (!success || !pathCreated) {
          ms.rollbackTransaction();
          if (pathCreated) {
            wh.renameDir(destPath, sourcePath, false);
          }
        }

        if (!listeners.isEmpty()) {
          AddPartitionEvent addPartitionEvent = new AddPartitionEvent(destinationTable, destPartitions, success, this);
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.ADD_PARTITION,
                                                addPartitionEvent,
                                                null,
                                                transactionalListenerResponsesForAddPartition, ms);

          i = 0;
          for (Partition partition : partitionsToExchange) {
            DropPartitionEvent dropPartitionEvent =
                new DropPartitionEvent(sourceTable, partition, success, true, this);
            Map<String, String> parameters =
                (transactionalListenerResponsesForDropPartition.size() > i)
                    ? transactionalListenerResponsesForDropPartition.get(i)
                    : null;

            MetaStoreListenerNotifier.notifyEvent(listeners,
                                                  EventType.DROP_PARTITION,
                                                  dropPartitionEvent,
                                                  null,
                                                  parameters, ms);
            i++;
          }
        }
      }
    }

    private boolean drop_partition_common(RawStore ms, String catName, String db_name,
                                          String tbl_name, List<String> part_vals,
                                          final boolean deleteData, final EnvironmentContext envContext)
        throws MetaException, NoSuchObjectException, IOException, InvalidObjectException,
      InvalidInputException {
      boolean success = false;
      Path partPath = null;
      Table tbl = null;
      Partition part = null;
      boolean isArchived = false;
      Path archiveParentDir = null;
      boolean mustPurge = false;
      boolean isExternalTbl = false;
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();

      if (db_name == null) {
        throw new MetaException("The DB name cannot be null.");
      }
      if (tbl_name == null) {
        throw new MetaException("The table name cannot be null.");
      }
      if (part_vals == null) {
        throw new MetaException("The partition values cannot be null.");
      }

      try {
        ms.openTransaction();
        part = ms.getPartition(catName, db_name, tbl_name, part_vals);
        tbl = get_table_core(catName, db_name, tbl_name);
        isExternalTbl = isExternal(tbl);
        firePreEvent(new PreDropPartitionEvent(tbl, part, deleteData, this));
        mustPurge = isMustPurge(envContext, tbl);

        if (part == null) {
          throw new NoSuchObjectException("Partition doesn't exist. "
              + part_vals);
        }

        isArchived = MetaStoreUtils.isArchived(part);
        if (isArchived) {
          archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
          verifyIsWritablePath(archiveParentDir);
        }

        if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
          partPath = new Path(part.getSd().getLocation());
          verifyIsWritablePath(partPath);
        }

        if (!ms.dropPartition(catName, db_name, tbl_name, part_vals)) {
          throw new MetaException("Unable to drop partition");
        } else {
          if (!transactionalListeners.isEmpty()) {

            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.DROP_PARTITION,
                                                      new DropPartitionEvent(tbl, part, true, deleteData, this),
                                                      envContext);
          }
          success = ms.commitTransaction();
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData && ((partPath != null) || (archiveParentDir != null))) {
          if (!isExternalTbl) {
            if (mustPurge) {
              LOG.info("dropPartition() will purge " + partPath + " directly, skipping trash.");
            }
            else {
              LOG.info("dropPartition() will move " + partPath + " to trash-directory.");
            }
            // Archived partitions have har:/to_har_file as their location.
            // The original directory was saved in params
            if (isArchived) {
              assert (archiveParentDir != null);
              wh.deleteDir(archiveParentDir, true, mustPurge);
            } else {
              assert (partPath != null);
              wh.deleteDir(partPath, true, mustPurge);
              deleteParentRecursive(partPath.getParent(), part_vals.size() - 1, mustPurge);
            }
            // ok even if the data is not deleted
          }
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.DROP_PARTITION,
                                                new DropPartitionEvent(tbl, part, success, deleteData, this),
                                                envContext,
                                                transactionalListenerResponses, ms);
        }
      }
      return true;
    }

    private static boolean isMustPurge(EnvironmentContext envContext, Table tbl) {
      // Data needs deletion. Check if trash may be skipped.
      // Trash may be skipped iff:
      //  1. deleteData == true, obviously.
      //  2. tbl is external.
      //  3. Either
      //    3.1. User has specified PURGE from the commandline, and if not,
      //    3.2. User has set the table to auto-purge.
      return ((envContext != null) && Boolean.parseBoolean(envContext.getProperties().get("ifPurge")))
        || (tbl.isSetParameters() && "true".equalsIgnoreCase(tbl.getParameters().get("auto.purge")));

    }
    private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
      if (depth > 0 && parent != null && wh.isWritable(parent)) {
        if (wh.isDir(parent) && wh.isEmpty(parent)) {
          wh.deleteDir(parent, true, mustPurge);
        }
        deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
      }
    }

    @Override
    public boolean drop_partition(final String db_name, final String tbl_name,
        final List<String> part_vals, final boolean deleteData)
        throws TException {
      return drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
          null);
    }

    private static class PathAndPartValSize {
      PathAndPartValSize(Path path, int partValSize) {
        this.path = path;
        this.partValSize = partValSize;
      }
      public Path path;
      int partValSize;
    }

    @Override
    public DropPartitionsResult drop_partitions_req(
        DropPartitionsRequest request) throws TException {
      RawStore ms = getMS();
      String dbName = request.getDbName(), tblName = request.getTblName();
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      boolean ifExists = request.isSetIfExists() && request.isIfExists();
      boolean deleteData = request.isSetDeleteData() && request.isDeleteData();
      boolean ignoreProtection = request.isSetIgnoreProtection() && request.isIgnoreProtection();
      boolean needResult = !request.isSetNeedResult() || request.isNeedResult();
      List<PathAndPartValSize> dirsToDelete = new ArrayList<>();
      List<Path> archToDelete = new ArrayList<>();
      EnvironmentContext envContext = request.isSetEnvironmentContext()
          ? request.getEnvironmentContext() : null;

      boolean success = false;
      ms.openTransaction();
      Table tbl = null;
      List<Partition> parts = null;
      boolean mustPurge = false;
      List<Map<String, String>> transactionalListenerResponses = Lists.newArrayList();

      try {
        // We need Partition-s for firing events and for result; DN needs MPartition-s to drop.
        // Great... Maybe we could bypass fetching MPartitions by issuing direct SQL deletes.
        tbl = get_table_core(catName, dbName, tblName);
        isExternal(tbl);
        mustPurge = isMustPurge(envContext, tbl);
        int minCount = 0;
        RequestPartsSpec spec = request.getParts();
        List<String> partNames = null;
        if (spec.isSetExprs()) {
          // Dropping by expressions.
          parts = new ArrayList<>(spec.getExprs().size());
          for (DropPartitionsExpr expr : spec.getExprs()) {
            ++minCount; // At least one partition per expression, if not ifExists
            List<Partition> result = new ArrayList<>();
            boolean hasUnknown = ms.getPartitionsByExpr(
                catName, dbName, tblName, expr.getExpr(), null, (short)-1, result);
            if (hasUnknown) {
              // Expr is built by DDLSA, it should only contain part cols and simple ops
              throw new MetaException("Unexpected unknown partitions to drop");
            }
            // this is to prevent dropping archived partition which is archived in a
            // different level the drop command specified.
            if (!ignoreProtection && expr.isSetPartArchiveLevel()) {
              for (Partition part : parts) {
                if (MetaStoreUtils.isArchived(part)
                    && MetaStoreUtils.getArchivingLevel(part) < expr.getPartArchiveLevel()) {
                  throw new MetaException("Cannot drop a subset of partitions "
                      + " in an archive, partition " + part);
                }
              }
            }
            parts.addAll(result);
          }
        } else if (spec.isSetNames()) {
          partNames = spec.getNames();
          minCount = partNames.size();
          parts = ms.getPartitionsByNames(catName, dbName, tblName, partNames);
        } else {
          throw new MetaException("Partition spec is not set");
        }

        if ((parts.size() < minCount) && !ifExists) {
          throw new NoSuchObjectException("Some partitions to drop are missing");
        }

        List<String> colNames = null;
        if (partNames == null) {
          partNames = new ArrayList<>(parts.size());
          colNames = new ArrayList<>(tbl.getPartitionKeys().size());
          for (FieldSchema col : tbl.getPartitionKeys()) {
            colNames.add(col.getName());
          }
        }

        for (Partition part : parts) {

          // TODO - we need to speed this up for the normal path where all partitions are under
          // the table and we don't have to stat every partition

          firePreEvent(new PreDropPartitionEvent(tbl, part, deleteData, this));
          if (colNames != null) {
            partNames.add(FileUtils.makePartName(colNames, part.getValues()));
          }
          // Preserve the old behavior of failing when we cannot write, even w/o deleteData,
          // and even if the table is external. That might not make any sense.
          if (MetaStoreUtils.isArchived(part)) {
            Path archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
            verifyIsWritablePath(archiveParentDir);
            archToDelete.add(archiveParentDir);
          }
          if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
            Path partPath = new Path(part.getSd().getLocation());
            verifyIsWritablePath(partPath);
            dirsToDelete.add(new PathAndPartValSize(partPath, part.getValues().size()));
          }
        }

        ms.dropPartitions(catName, dbName, tblName, partNames);
        if (parts != null && !transactionalListeners.isEmpty()) {
          for (Partition part : parts) {
            transactionalListenerResponses.add(
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                      EventType.DROP_PARTITION,
                                                      new DropPartitionEvent(tbl, part, true, deleteData, this),
                                                      envContext));
          }
        }

        success = ms.commitTransaction();
        DropPartitionsResult result = new DropPartitionsResult();
        if (needResult) {
          result.setPartitions(parts);
        }

        return result;
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        } else if (deleteData && !isExternal(tbl)) {
          LOG.info( mustPurge?
                      "dropPartition() will purge partition-directories directly, skipping trash."
                    :  "dropPartition() will move partition-directories to trash-directory.");
          // Archived partitions have har:/to_har_file as their location.
          // The original directory was saved in params
          for (Path path : archToDelete) {
            wh.deleteDir(path, true, mustPurge);
          }
          for (PathAndPartValSize p : dirsToDelete) {
            wh.deleteDir(p.path, true, mustPurge);
            try {
              deleteParentRecursive(p.path.getParent(), p.partValSize - 1, mustPurge);
            } catch (IOException ex) {
              LOG.warn("Error from deleteParentRecursive", ex);
              throw new MetaException("Failed to delete parent: " + ex.getMessage());
            }
          }
        }
        if (parts != null) {
          int i = 0;
          if (parts != null && !listeners.isEmpty()) {
            for (Partition part : parts) {
              Map<String, String> parameters =
                  (!transactionalListenerResponses.isEmpty()) ? transactionalListenerResponses.get(i) : null;

              MetaStoreListenerNotifier.notifyEvent(listeners,
                                                    EventType.DROP_PARTITION,
                                                    new DropPartitionEvent(tbl, part, success, deleteData, this),
                                                    envContext,
                                                    parameters, ms);

              i++;
            }
          }
        }
      }
    }

    private void verifyIsWritablePath(Path dir) throws MetaException {
      try {
        if (!wh.isWritable(dir.getParent())) {
          throw new MetaException("Table partition not deleted since " + dir.getParent()
              + " is not writable by " + SecurityUtils.getUser());
        }
      } catch (IOException ex) {
        LOG.warn("Error from isWritable", ex);
        throw new MetaException("Table partition not deleted since " + dir.getParent()
            + " access cannot be checked: " + ex.getMessage());
      }
    }

    @Override
    public boolean drop_partition_with_environment_context(final String db_name,
        final String tbl_name, final List<String> part_vals, final boolean deleteData,
        final EnvironmentContext envContext)
        throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("drop_partition", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, part_vals);
      LOG.info("Partition values:" + part_vals);

      boolean ret = false;
      Exception ex = null;
      try {
        ret = drop_partition_common(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, part_vals, deleteData, envContext);
      } catch (IOException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("drop_partition", ret, ex, tbl_name);
      }
      return ret;

    }

    @Override
    public Partition get_partition(final String db_name, final String tbl_name,
        final List<String> part_vals) throws MetaException, NoSuchObjectException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("get_partition", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, part_vals);

      Partition ret = null;
      Exception ex = null;
      try {
        fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
        ret = getMS().getPartition(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals);
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("get_partition", ret != null, ex, tbl_name);
      }
      return ret;
    }

    /**
     * Fire a pre-event for read table operation, if there are any
     * pre-event listeners registered
     */
    private void fireReadTablePreEvent(String catName, String dbName, String tblName)
        throws MetaException, NoSuchObjectException {
      if(preListeners.size() > 0) {
        // do this only if there is a pre event listener registered (avoid unnecessary
        // metastore api call)
        Table t = getMS().getTable(catName, dbName, tblName);
        if (t == null) {
          throw new NoSuchObjectException(getCatalogQualifiedTableName(catName, dbName, tblName)
              + " table not found");
        }
        firePreEvent(new PreReadTableEvent(t, this));
      }
    }

    @Override
    public Partition get_partition_with_auth(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final String user_name, final List<String> group_names)
        throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("get_partition_with_auth", parsedDbName[CAT_NAME],
          parsedDbName[DB_NAME], tbl_name, part_vals);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      Partition ret = null;
      Exception ex = null;
      try {
        ret = getMS().getPartitionWithAuth(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, part_vals, user_name, group_names);
      } catch (InvalidObjectException e) {
        ex = e;
        throw new NoSuchObjectException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partition_with_auth", ret != null, ex, tbl_name);
      }
      return ret;
    }

    @Override
    public List<Partition> get_partitions(final String db_name, final String tbl_name,
        final short max_parts) throws NoSuchObjectException, MetaException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startTableFunction("get_partitions", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      List<Partition> ret = null;
      Exception ex = null;
      try {
        checkLimitNumberOfPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, NO_FILTER_STRING, max_parts);
        ret = getMS().getPartitions(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
            max_parts);
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("get_partitions", ret != null, ex, tbl_name);
      }
      return ret;

    }

    @Override
    public List<Partition> get_partitions_with_auth(final String dbName,
        final String tblName, final short maxParts, final String userName,
        final List<String> groupNames) throws TException {
      String[] parsedDbName = parseDbName(dbName, conf);
      startTableFunction("get_partitions_with_auth", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);

      List<Partition> ret = null;
      Exception ex = null;
      try {
        checkLimitNumberOfPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tblName, NO_FILTER_STRING, maxParts);
        ret = getMS().getPartitionsWithAuth(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
            maxParts, userName, groupNames);
      } catch (InvalidObjectException e) {
        ex = e;
        throw new NoSuchObjectException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_with_auth", ret != null, ex, tblName);
      }
      return ret;

    }

    private void checkLimitNumberOfPartitionsByFilter(String catName, String dbName,
                                                      String tblName, String filterString,
                                                      int maxParts) throws TException {
      if (isPartitionLimitEnabled()) {
        checkLimitNumberOfPartitions(tblName, get_num_partitions_by_filter(prependCatalogToDbName(
            catName, dbName, conf), tblName, filterString), maxParts);
      }
    }

    private void checkLimitNumberOfPartitionsByExpr(String catName, String dbName, String tblName,
                                                    byte[] filterExpr, int maxParts)
        throws TException {
      if (isPartitionLimitEnabled()) {
        checkLimitNumberOfPartitions(tblName, get_num_partitions_by_expr(catName, dbName, tblName,
            filterExpr), maxParts);
      }
    }

    private boolean isPartitionLimitEnabled() {
      int partitionLimit = MetastoreConf.getIntVar(conf, ConfVars.LIMIT_PARTITION_REQUEST);
      return partitionLimit > -1;
    }

    private void checkLimitNumberOfPartitions(String tblName, int numPartitions, int maxToFetch) throws MetaException {
      if (isPartitionLimitEnabled()) {
        int partitionLimit = MetastoreConf.getIntVar(conf, ConfVars.LIMIT_PARTITION_REQUEST);
        int partitionRequest = (maxToFetch < 0) ? numPartitions : maxToFetch;
        if (partitionRequest > partitionLimit) {
          String configName = ConfVars.LIMIT_PARTITION_REQUEST.toString();
          throw new MetaException(String.format(PARTITION_NUMBER_EXCEED_LIMIT_MSG, partitionRequest,
              tblName, partitionLimit, configName));
        }
      }
    }

    @Override
    public List<PartitionSpec> get_partitions_pspec(final String db_name, final String tbl_name, final int max_parts)
      throws NoSuchObjectException, MetaException  {

      String[] parsedDbName = parseDbName(db_name, conf);
      String tableName = tbl_name.toLowerCase();

      startTableFunction("get_partitions_pspec", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);

      List<PartitionSpec> partitionSpecs = null;
      try {
        Table table = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);
        // get_partitions will parse out the catalog and db names itself
        List<Partition> partitions = get_partitions(db_name, tableName, (short) max_parts);

        if (is_partition_spec_grouping_enabled(table)) {
          partitionSpecs = get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
        }
        else {
          PartitionSpec pSpec = new PartitionSpec();
          pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
          pSpec.setCatName(parsedDbName[CAT_NAME]);
          pSpec.setDbName(parsedDbName[DB_NAME]);
          pSpec.setTableName(tableName);
          pSpec.setRootPath(table.getSd().getLocation());
          partitionSpecs = Arrays.asList(pSpec);
        }

        return partitionSpecs;
      }
      finally {
        endFunction("get_partitions_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tbl_name);
      }
    }

    private static class StorageDescriptorKey {

      private final StorageDescriptor sd;

      StorageDescriptorKey(StorageDescriptor sd) { this.sd = sd; }

      StorageDescriptor getSd() {
        return sd;
      }

      private String hashCodeKey() {
        return sd.getInputFormat() + "\t"
            + sd.getOutputFormat() +  "\t"
            + sd.getSerdeInfo().getSerializationLib() + "\t"
            + sd.getCols();
      }

      @Override
      public int hashCode() {
        return hashCodeKey().hashCode();
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs == this) {
          return true;
        }

        if (!(rhs instanceof StorageDescriptorKey)) {
          return false;
        }

        return (hashCodeKey().equals(((StorageDescriptorKey) rhs).hashCodeKey()));
      }
    }

    private List<PartitionSpec> get_partitionspecs_grouped_by_storage_descriptor(Table table, List<Partition> partitions)
      throws NoSuchObjectException, MetaException {

      assert is_partition_spec_grouping_enabled(table);

      final String tablePath = table.getSd().getLocation();

      ImmutableListMultimap<Boolean, Partition> partitionsWithinTableDirectory
          = Multimaps.index(partitions, new com.google.common.base.Function<Partition, Boolean>() {

        @Override
        public Boolean apply(Partition input) {
          return input.getSd().getLocation().startsWith(tablePath);
        }
      });

      List<PartitionSpec> partSpecs = new ArrayList<>();

      // Classify partitions within the table directory into groups,
      // based on shared SD properties.

      Map<StorageDescriptorKey, List<PartitionWithoutSD>> sdToPartList
          = new HashMap<>();

      if (partitionsWithinTableDirectory.containsKey(true)) {

        ImmutableList<Partition> partsWithinTableDir = partitionsWithinTableDirectory.get(true);
        for (Partition partition : partsWithinTableDir) {

          PartitionWithoutSD partitionWithoutSD
              = new PartitionWithoutSD( partition.getValues(),
              partition.getCreateTime(),
              partition.getLastAccessTime(),
              partition.getSd().getLocation().substring(tablePath.length()), partition.getParameters());

          StorageDescriptorKey sdKey = new StorageDescriptorKey(partition.getSd());
          if (!sdToPartList.containsKey(sdKey)) {
            sdToPartList.put(sdKey, new ArrayList<>());
          }

          sdToPartList.get(sdKey).add(partitionWithoutSD);

        } // for (partitionsWithinTableDirectory);

        for (Map.Entry<StorageDescriptorKey, List<PartitionWithoutSD>> entry : sdToPartList.entrySet()) {
          partSpecs.add(getSharedSDPartSpec(table, entry.getKey(), entry.getValue()));
        }

      } // Done grouping partitions within table-dir.

      // Lump all partitions outside the tablePath into one PartSpec.
      if (partitionsWithinTableDirectory.containsKey(false)) {
        List<Partition> partitionsOutsideTableDir = partitionsWithinTableDirectory.get(false);
        if (!partitionsOutsideTableDir.isEmpty()) {
          PartitionSpec partListSpec = new PartitionSpec();
          partListSpec.setDbName(table.getDbName());
          partListSpec.setTableName(table.getTableName());
          partListSpec.setPartitionList(new PartitionListComposingSpec(partitionsOutsideTableDir));
          partSpecs.add(partListSpec);
        }

      }
      return partSpecs;
    }

    private PartitionSpec getSharedSDPartSpec(Table table, StorageDescriptorKey sdKey, List<PartitionWithoutSD> partitions) {

      StorageDescriptor sd = new StorageDescriptor(sdKey.getSd());
      sd.setLocation(table.getSd().getLocation()); // Use table-dir as root-dir.
      PartitionSpecWithSharedSD sharedSDPartSpec =
          new PartitionSpecWithSharedSD(partitions, sd);

      PartitionSpec ret = new PartitionSpec();
      ret.setRootPath(sd.getLocation());
      ret.setSharedSDPartitionSpec(sharedSDPartSpec);
      ret.setDbName(table.getDbName());
      ret.setTableName(table.getTableName());

      return ret;
    }

    private static boolean is_partition_spec_grouping_enabled(Table table) {

      Map<String, String> parameters = table.getParameters();
      return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled")
          && parameters.get("hive.hcatalog.partition.spec.grouping.enabled").equalsIgnoreCase("true");
    }

    @Override
    public List<String> get_partition_names(final String db_name, final String tbl_name,
        final short max_parts) throws NoSuchObjectException, MetaException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startTableFunction("get_partition_names", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      List<String> ret = null;
      Exception ex = null;
      try {
        ret = getMS().listPartitionNames(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
            max_parts);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_partition_names", ret != null, ex, tbl_name);
      }
      return ret;
    }

    @Override
    public PartitionValuesResponse get_partition_values(PartitionValuesRequest request) throws MetaException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String dbName = request.getDbName();
      String tblName = request.getTblName();
      // This is serious black magic, as the following 2 lines do nothing AFAICT but without them
      // the subsequent call to listPartitionValues fails.
      List<FieldSchema> partCols = new ArrayList<FieldSchema>();
      partCols.add(request.getPartitionKeys().get(0));
      return getMS().listPartitionValues(catName, dbName, tblName, request.getPartitionKeys(),
          request.isApplyDistinct(), request.getFilter(), request.isAscending(),
          request.getPartitionOrder(), request.getMaxParts());
    }

    @Override
    public void alter_partition(final String db_name, final String tbl_name,
        final Partition new_part)
        throws TException {
      rename_partition(db_name, tbl_name, null, new_part);
    }

    @Override
    public void alter_partition_with_environment_context(final String dbName,
        final String tableName, final Partition newPartition,
        final EnvironmentContext envContext)
        throws TException {
      String[] parsedDbName = parseDbName(dbName, conf);
      rename_partition(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, null, newPartition,
          envContext);
    }

    @Override
    public void rename_partition(final String db_name, final String tbl_name,
        final List<String> part_vals, final Partition new_part)
        throws TException {
      // Call rename_partition without an environment context.
      String[] parsedDbName = parseDbName(db_name, conf);
      rename_partition(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals, new_part,
          null);
    }

    private void rename_partition(final String catName, final String db_name, final String tbl_name,
        final List<String> part_vals, final Partition new_part,
        final EnvironmentContext envContext)
        throws TException {
      startTableFunction("alter_partition", catName, db_name, tbl_name);

      if (LOG.isInfoEnabled()) {
        LOG.info("New partition values:" + new_part.getValues());
        if (part_vals != null && part_vals.size() > 0) {
          LOG.info("Old Partition values:" + part_vals);
        }
      }

      // Adds the missing scheme/authority for the new partition location
      if (new_part.getSd() != null) {
        String newLocation = new_part.getSd().getLocation();
        if (org.apache.commons.lang.StringUtils.isNotEmpty(newLocation)) {
          Path tblPath = wh.getDnsPath(new Path(newLocation));
          new_part.getSd().setLocation(tblPath.toString());
        }
      }

      // Make sure the new partition has the catalog value set
      if (!new_part.isSetCatName()) new_part.setCatName(catName);

      Partition oldPart = null;
      Exception ex = null;
      try {
        firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, part_vals, new_part, this));
        if (part_vals != null && !part_vals.isEmpty()) {
          MetaStoreUtils.validatePartitionNameCharacters(new_part.getValues(),
              partitionValidationPattern);
        }

        oldPart = alterHandler.alterPartition(getMS(), wh, catName, db_name, tbl_name,
            part_vals, new_part, envContext, this);

        // Only fetch the table if we actually have a listener
        Table table = null;
        if (!listeners.isEmpty()) {
          if (table == null) {
            table = getMS().getTable(catName, db_name, tbl_name);
          }

          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.ALTER_PARTITION,
                                                new AlterPartitionEvent(oldPart, new_part, table, false, true, this),
                                                envContext);
        }
      } catch (InvalidObjectException e) {
        ex = e;
        throw new InvalidOperationException(e.getMessage());
      } catch (AlreadyExistsException e) {
        ex = e;
        throw new InvalidOperationException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidOperationException) {
          throw (InvalidOperationException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("alter_partition", oldPart != null, ex, tbl_name);
      }
    }

    @Override
    public void alter_partitions(final String db_name, final String tbl_name,
        final List<Partition> new_parts)
        throws TException {
      alter_partitions_with_environment_context(db_name, tbl_name, new_parts, null);
    }

    @Override
    public void alter_partitions_with_environment_context(final String db_name, final String tbl_name,
        final List<Partition> new_parts, EnvironmentContext environmentContext)
        throws TException {

      String[] parsedDbName = parseDbName(db_name, conf);
      startTableFunction("alter_partitions", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);

      if (LOG.isInfoEnabled()) {
        for (Partition tmpPart : new_parts) {
          LOG.info("New partition values:" + tmpPart.getValues());
        }
      }
      // all partitions are altered atomically
      // all prehooks are fired together followed by all post hooks
      List<Partition> oldParts = null;
      Exception ex = null;
      try {
        for (Partition tmpPart : new_parts) {
          // Make sure the catalog name is set in the new partition
          if (!tmpPart.isSetCatName()) tmpPart.setCatName(getDefaultCatalog(conf));
          firePreEvent(new PreAlterPartitionEvent(parsedDbName[DB_NAME], tbl_name, null, tmpPart, this));
        }
        oldParts = alterHandler.alterPartitions(getMS(), wh, parsedDbName[CAT_NAME],
            parsedDbName[DB_NAME], tbl_name, new_parts, environmentContext, this);
        Iterator<Partition> olditr = oldParts.iterator();
        // Only fetch the table if we have a listener that needs it.
        Table table = null;
        for (Partition tmpPart : new_parts) {
          Partition oldTmpPart;
          if (olditr.hasNext()) {
            oldTmpPart = olditr.next();
          }
          else {
            throw new InvalidOperationException("failed to alterpartitions");
          }

          if (table == null) {
            table = getMS().getTable(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
          }

          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                                                  EventType.ALTER_PARTITION,
                                                  new AlterPartitionEvent(oldTmpPart, tmpPart, table, false, true, this));
          }
        }
      } catch (InvalidObjectException e) {
        ex = e;
        throw new InvalidOperationException(e.getMessage());
      } catch (AlreadyExistsException e) {
        ex = e;
        throw new InvalidOperationException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidOperationException) {
          throw (InvalidOperationException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("alter_partition", oldParts != null, ex, tbl_name);
      }
    }

    @Override
    public String getVersion() throws TException {
      endFunction(startFunction("getVersion"), true, null);
      return "3.0";
    }

    @Override
    public void alter_table(final String dbname, final String name,
        final Table newTable)
        throws InvalidOperationException, MetaException {
      // Do not set an environment context.
      String[] parsedDbName = parseDbName(dbname, conf);
      alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, newTable, null);
    }

    @Override
    public void alter_table_with_cascade(final String dbname, final String name,
        final Table newTable, final boolean cascade)
        throws InvalidOperationException, MetaException {
      EnvironmentContext envContext = null;
      if (cascade) {
        envContext = new EnvironmentContext();
        envContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
      }
      String[] parsedDbName = parseDbName(dbname, conf);
      alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, newTable, envContext);
    }

    @Override
    public void alter_table_with_environment_context(final String dbname,
        final String name, final Table newTable,
        final EnvironmentContext envContext)
        throws InvalidOperationException, MetaException {
      String[] parsedDbName = parseDbName(dbname, conf);
      alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, newTable, envContext);
    }

    private void alter_table_core(final String catName, final String dbname, final String name,
                                  final Table newTable, final EnvironmentContext envContext)
        throws InvalidOperationException, MetaException {
      startFunction("alter_table", ": " + getCatalogQualifiedTableName(catName, dbname, name)
          + " newtbl=" + newTable.getTableName());
      // Update the time if it hasn't been specified.
      if (newTable.getParameters() == null ||
          newTable.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
        newTable.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
            .currentTimeMillis() / 1000));
      }

      // Adds the missing scheme/authority for the new table location
      if (newTable.getSd() != null) {
        String newLocation = newTable.getSd().getLocation();
        if (org.apache.commons.lang.StringUtils.isNotEmpty(newLocation)) {
          Path tblPath = wh.getDnsPath(new Path(newLocation));
          newTable.getSd().setLocation(tblPath.toString());
        }
      }
      // Set the catalog name if it hasn't been set in the new table
      if (!newTable.isSetCatName()) newTable.setCatName(catName);

      boolean success = false;
      Exception ex = null;
      try {
        Table oldt = get_table_core(catName, dbname, name);
        firePreEvent(new PreAlterTableEvent(oldt, newTable, this));
        alterHandler.alterTable(getMS(), wh, catName, dbname, name, newTable,
                envContext, this);
        success = true;
      } catch (NoSuchObjectException e) {
        // thrown when the table to be altered does not exist
        ex = e;
        throw new InvalidOperationException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else if (e instanceof InvalidOperationException) {
          throw (InvalidOperationException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("alter_table", success, ex, name);
      }
    }

    @Override
    public List<String> get_tables(final String dbname, final String pattern)
        throws MetaException {
      startFunction("get_tables", ": db=" + dbname + " pat=" + pattern);

      List<String> ret = null;
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbname, conf);
      try {
        ret = getMS().getTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_tables", ret != null, ex);
      }
      return ret;
    }

    @Override
    public List<String> get_tables_by_type(final String dbname, final String pattern, final String tableType)
        throws MetaException {
      startFunction("get_tables_by_type", ": db=" + dbname + " pat=" + pattern + ",type=" + tableType);

      List<String> ret = null;
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbname, conf);
      try {
        ret = getMS().getTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern, TableType.valueOf(tableType));
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_tables_by_type", ret != null, ex);
      }
      return ret;
    }

    @Override
    public List<String> get_materialized_views_for_rewriting(final String dbname)
        throws MetaException {
      startFunction("get_materialized_views_for_rewriting", ": db=" + dbname);

      List<String> ret = null;
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbname, conf);
      try {
        ret = getMS().getMaterializedViewsForRewriting(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_materialized_views_for_rewriting", ret != null, ex);
      }
      return ret;
    }

    @Override
    public List<String> get_all_tables(final String dbname) throws MetaException {
      startFunction("get_all_tables", ": db=" + dbname);

      List<String> ret = null;
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbname, conf);
      try {
        ret = getMS().getAllTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_all_tables", ret != null, ex);
      }
      return ret;
    }

    @Override
    public List<FieldSchema> get_fields(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      return get_fields_with_environment_context(db, tableName, null);
    }

    @Override
    public List<FieldSchema> get_fields_with_environment_context(String db, String tableName,
        final EnvironmentContext envContext)
        throws MetaException, UnknownTableException, UnknownDBException {
      startFunction("get_fields_with_environment_context", ": db=" + db + "tbl=" + tableName);
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];
      String[] parsedDbName = parseDbName(db, conf);

      Table tbl;
      List<FieldSchema> ret = null;
      Exception ex = null;
      ClassLoader orgHiveLoader = null;
      try {
        try {
          tbl = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        if (null == tbl.getSd().getSerdeInfo().getSerializationLib() ||
          MetastoreConf.getStringCollection(conf,
              ConfVars.SERDES_USING_METASTORE_FOR_SCHEMA).contains(
                  tbl.getSd().getSerdeInfo().getSerializationLib())) {
          ret = tbl.getSd().getCols();
        } else {
          StorageSchemaReader schemaReader = getStorageSchemaReader();
          ret = schemaReader.readSchema(tbl, envContext, getConf());
        }
      } catch (Exception e) {
        ex = e;
        if (e instanceof UnknownTableException) {
          throw (UnknownTableException) e;
        } else if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        if (orgHiveLoader != null) {
          conf.setClassLoader(orgHiveLoader);
        }
        endFunction("get_fields_with_environment_context", ret != null, ex, tableName);
      }

      return ret;
    }

    private StorageSchemaReader getStorageSchemaReader() throws MetaException {
      if (storageSchemaReader == null) {
        String className =
            MetastoreConf.getVar(conf, MetastoreConf.ConfVars.STORAGE_SCHEMA_READER_IMPL);
        Class<? extends StorageSchemaReader> readerClass =
            JavaUtils.getClass(className, StorageSchemaReader.class);
        try {
          storageSchemaReader = readerClass.newInstance();
        } catch (InstantiationException|IllegalAccessException e) {
          LOG.error("Unable to instantiate class " + className, e);
          throw new MetaException(e.getMessage());
        }
      }
      return storageSchemaReader;
    }

    /**
     * Return the schema of the table. This function includes partition columns
     * in addition to the regular columns.
     *
     * @param db
     *          Name of the database
     * @param tableName
     *          Name of the table
     * @return List of columns, each column is a FieldSchema structure
     * @throws MetaException
     * @throws UnknownTableException
     * @throws UnknownDBException
     */
    @Override
    public List<FieldSchema> get_schema(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      return get_schema_with_environment_context(db,tableName, null);
    }


    /**
     * Return the schema of the table. This function includes partition columns
     * in addition to the regular columns.
     *
     * @param db
     *          Name of the database
     * @param tableName
     *          Name of the table
     * @param envContext
     *          Store session based properties
     * @return List of columns, each column is a FieldSchema structure
     * @throws MetaException
     * @throws UnknownTableException
     * @throws UnknownDBException
     */
    @Override
    public List<FieldSchema> get_schema_with_environment_context(String db, String tableName,
          final EnvironmentContext envContext)
        throws MetaException, UnknownTableException, UnknownDBException {
      startFunction("get_schema_with_environment_context", ": db=" + db + "tbl=" + tableName);
      boolean success = false;
      Exception ex = null;
      try {
        String[] names = tableName.split("\\.");
        String base_table_name = names[0];
        String[] parsedDbName = parseDbName(db, conf);

        Table tbl;
        try {
          tbl = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], base_table_name);
        } catch (NoSuchObjectException e) {
          throw new UnknownTableException(e.getMessage());
        }
        // Pass unparsed db name here
        List<FieldSchema> fieldSchemas = get_fields_with_environment_context(db, base_table_name,envContext);

        if (tbl == null || fieldSchemas == null) {
          throw new UnknownTableException(tableName + " doesn't exist");
        }

        if (tbl.getPartitionKeys() != null) {
          // Combine the column field schemas and the partition keys to create the
          // whole schema
          fieldSchemas.addAll(tbl.getPartitionKeys());
        }
        success = true;
        return fieldSchemas;
      } catch (Exception e) {
        ex = e;
        if (e instanceof UnknownDBException) {
          throw (UnknownDBException) e;
        } else if (e instanceof UnknownTableException) {
          throw (UnknownTableException) e;
        } else if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          MetaException me = new MetaException(e.toString());
          me.initCause(e);
          throw me;
        }
      } finally {
        endFunction("get_schema_with_environment_context", success, ex, tableName);
      }
    }

    @Override
    public String getCpuProfile(int profileDurationInSec) throws TException {
      return "";
    }

    /**
     * Returns the value of the given configuration variable name. If the
     * configuration variable with the given name doesn't exist, or if there
     * were an exception thrown while retrieving the variable, or if name is
     * null, defaultValue is returned.
     */
    @Override
    public String get_config_value(String name, String defaultValue)
        throws TException {
      startFunction("get_config_value", ": name=" + name + " defaultValue="
          + defaultValue);
      boolean success = false;
      Exception ex = null;
      try {
        if (name == null) {
          success = true;
          return defaultValue;
        }
        // Allow only keys that start with hive.*, hdfs.*, mapred.* for security
        // i.e. don't allow access to db password
        if (!Pattern.matches("(hive|hdfs|mapred|metastore).*", name)) {
          throw new ConfigValSecurityException("For security reasons, the "
              + "config key " + name + " cannot be accessed");
        }

        String toReturn = defaultValue;
        try {
          toReturn = MetastoreConf.get(conf, name);
          if (toReturn == null) {
            toReturn = defaultValue;
          }
        } catch (RuntimeException e) {
          LOG.error(threadLocalId.get().toString() + ": "
              + "RuntimeException thrown in get_config_value - msg: "
              + e.getMessage() + " cause: " + e.getCause());
        }
        success = true;
        return toReturn;
      } catch (Exception e) {
        ex = e;
        if (e instanceof ConfigValSecurityException) {
          throw (ConfigValSecurityException) e;
        } else {
          throw new TException(e);
        }
      } finally {
        endFunction("get_config_value", success, ex);
      }
    }

    private List<String> getPartValsFromName(Table t, String partName)
        throws MetaException, InvalidObjectException {
      Preconditions.checkArgument(t != null, "Table can not be null");
      // Unescape the partition name
      LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

      List<String> partVals = new ArrayList<>();
      for (FieldSchema field : t.getPartitionKeys()) {
        String key = field.getName();
        String val = hm.get(key);
        if (val == null) {
          throw new InvalidObjectException("incomplete partition name - missing " + key);
        }
        partVals.add(val);
      }
      return partVals;
    }

    private List<String> getPartValsFromName(RawStore ms, String catName, String dbName,
                                             String tblName, String partName)
        throws MetaException, InvalidObjectException {
      Table t = ms.getTable(catName, dbName, tblName);
      if (t == null) {
        throw new InvalidObjectException(dbName + "." + tblName
            + " table not found");
      }
      return getPartValsFromName(t, partName);
    }

    private Partition get_partition_by_name_core(final RawStore ms, final String catName,
                                                 final String db_name, final String tbl_name,
                                                 final String part_name) throws TException {
      fireReadTablePreEvent(catName, db_name, tbl_name);
      List<String> partVals;
      try {
        partVals = getPartValsFromName(ms, catName, db_name, tbl_name, part_name);
      } catch (InvalidObjectException e) {
        throw new NoSuchObjectException(e.getMessage());
      }
      Partition p = ms.getPartition(catName, db_name, tbl_name, partVals);

      if (p == null) {
        throw new NoSuchObjectException(getCatalogQualifiedTableName(catName, db_name, tbl_name)
            + " partition (" + part_name + ") not found");
      }
      return p;
    }

    @Override
    public Partition get_partition_by_name(final String db_name, final String tbl_name,
        final String part_name) throws TException {

      String[] parsedDbName = parseDbName(db_name, conf);
      startFunction("get_partition_by_name", ": tbl=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name)
          + " part=" + part_name);
      Partition ret = null;
      Exception ex = null;
      try {
        ret = get_partition_by_name_core(getMS(), parsedDbName[CAT_NAME],
            parsedDbName[DB_NAME], tbl_name, part_name); } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partition_by_name", ret != null, ex, tbl_name);
      }
      return ret;
    }

    @Override
    public Partition append_partition_by_name(final String db_name, final String tbl_name,
        final String part_name) throws TException {
      return append_partition_by_name_with_environment_context(db_name, tbl_name, part_name, null);
    }

    @Override
    public Partition append_partition_by_name_with_environment_context(final String db_name,
        final String tbl_name, final String part_name, final EnvironmentContext env_context)
        throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startFunction("append_partition_by_name", ": tbl="
          + getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name) + " part=" + part_name);

      Partition ret = null;
      Exception ex = null;
      try {
        RawStore ms = getMS();
        List<String> partVals = getPartValsFromName(ms, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_name);
        ret = append_partition_common(ms, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, partVals, env_context);
      } catch (Exception e) {
        ex = e;
        if (e instanceof InvalidObjectException) {
          throw (InvalidObjectException) e;
        } else if (e instanceof AlreadyExistsException) {
          throw (AlreadyExistsException) e;
        } else if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("append_partition_by_name", ret != null, ex, tbl_name);
      }
      return ret;
    }

    private boolean drop_partition_by_name_core(final RawStore ms, final String catName,
                                                final String db_name, final String tbl_name,
                                                final String part_name, final boolean deleteData,
                                                final EnvironmentContext envContext)
        throws TException, IOException {

      List<String> partVals;
      try {
        partVals = getPartValsFromName(ms, catName, db_name, tbl_name, part_name);
      } catch (InvalidObjectException e) {
        throw new NoSuchObjectException(e.getMessage());
      }

      return drop_partition_common(ms, catName, db_name, tbl_name, partVals, deleteData, envContext);
    }

    @Override
    public boolean drop_partition_by_name(final String db_name, final String tbl_name,
        final String part_name, final boolean deleteData) throws TException {
      return drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name,
          deleteData, null);
    }

    @Override
    public boolean drop_partition_by_name_with_environment_context(final String db_name,
        final String tbl_name, final String part_name, final boolean deleteData,
        final EnvironmentContext envContext) throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startFunction("drop_partition_by_name", ": tbl=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name)
          + " part=" + part_name);

      boolean ret = false;
      Exception ex = null;
      try {
        ret = drop_partition_by_name_core(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, part_name, deleteData, envContext);
      } catch (IOException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("drop_partition_by_name", ret, ex, tbl_name);
      }

      return ret;
    }

    @Override
    public List<Partition> get_partitions_ps(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final short max_parts) throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("get_partitions_ps", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, part_vals);

      List<Partition> ret = null;
      Exception ex = null;
      try {
        // Don't send the parsedDbName, as this method will parse itself.
        ret = get_partitions_ps_with_auth(db_name, tbl_name, part_vals,
            max_parts, null, null);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_ps", ret != null, ex, tbl_name);
      }

      return ret;
    }

    @Override
    public List<Partition> get_partitions_ps_with_auth(final String db_name,
        final String tbl_name, final List<String> part_vals,
        final short max_parts, final String userName,
        final List<String> groupNames) throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("get_partitions_ps_with_auth", parsedDbName[CAT_NAME],
          parsedDbName[DB_NAME], tbl_name, part_vals);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      List<Partition> ret = null;
      Exception ex = null;
      try {
        ret = getMS().listPartitionsPsWithAuth(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, part_vals, max_parts, userName, groupNames);
      } catch (InvalidObjectException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_ps_with_auth", ret != null, ex, tbl_name);
      }
      return ret;
    }

    @Override
    public List<String> get_partition_names_ps(final String db_name,
        final String tbl_name, final List<String> part_vals, final short max_parts)
        throws TException {
      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("get_partitions_names_ps", parsedDbName[CAT_NAME],
          parsedDbName[DB_NAME], tbl_name, part_vals);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      List<String> ret = null;
      Exception ex = null;
      try {
        ret = getMS().listPartitionNamesPs(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
            part_vals, max_parts);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_names_ps", ret != null, ex, tbl_name);
      }
      return ret;
    }

    @Override
    public List<String> partition_name_to_vals(String part_name) throws TException {
      if (part_name.length() == 0) {
        return new ArrayList<>();
      }
      LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
      List<String> part_vals = new ArrayList<>();
      part_vals.addAll(map.values());
      return part_vals;
    }

    @Override
    public Map<String, String> partition_name_to_spec(String part_name) throws TException {
      if (part_name.length() == 0) {
        return new HashMap<>();
      }
      return Warehouse.makeSpecFromName(part_name);
    }

    private String lowerCaseConvertPartName(String partName) throws MetaException {
      boolean isFirst = true;
      Map<String, String> partSpec = Warehouse.makeEscSpecFromName(partName);
      String convertedPartName = new String();

      for (Map.Entry<String, String> entry : partSpec.entrySet()) {
        String partColName = entry.getKey();
        String partColVal = entry.getValue();

        if (!isFirst) {
          convertedPartName += "/";
        } else {
          isFirst = false;
        }
        convertedPartName += partColName.toLowerCase() + "=" + partColVal;
      }
      return convertedPartName;
    }

    @Override
    public ColumnStatistics get_table_column_statistics(String dbName, String tableName,
      String colName) throws TException {
      String[] parsedDbName = parseDbName(dbName, conf);
      parsedDbName[CAT_NAME] = parsedDbName[CAT_NAME].toLowerCase();
      parsedDbName[DB_NAME] = parsedDbName[DB_NAME].toLowerCase();
      tableName = tableName.toLowerCase();
      colName = colName.toLowerCase();
      startFunction("get_column_statistics_by_table", ": table=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
              tableName) + " column=" + colName);
      ColumnStatistics statsObj = null;
      try {
        statsObj = getMS().getTableColumnStatistics(
            parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, Lists.newArrayList(colName));
        if (statsObj != null) {
          assert statsObj.getStatsObjSize() <= 1;
        }
        return statsObj;
      } finally {
        endFunction("get_column_statistics_by_table", statsObj != null, null, tableName);
      }
    }

    @Override
    public TableStatsResult get_table_statistics_req(TableStatsRequest request) throws TException {
      String catName = request.isSetCatName() ? request.getCatName().toLowerCase() :
          getDefaultCatalog(conf);
      String dbName = request.getDbName().toLowerCase();
      String tblName = request.getTblName().toLowerCase();
      startFunction("get_table_statistics_req", ": table=" +
          getCatalogQualifiedTableName(catName, dbName, tblName));
      TableStatsResult result = null;
      List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
      for (String colName : request.getColNames()) {
        lowerCaseColNames.add(colName.toLowerCase());
      }
      try {
        ColumnStatistics cs = getMS().getTableColumnStatistics(catName, dbName, tblName, lowerCaseColNames);
        result = new TableStatsResult((cs == null || cs.getStatsObj() == null)
            ? Lists.newArrayList() : cs.getStatsObj());
      } finally {
        endFunction("get_table_statistics_req", result == null, null, tblName);
      }
      return result;
    }

    @Override
    public ColumnStatistics get_partition_column_statistics(String dbName, String tableName,
      String partName, String colName) throws TException {
      dbName = dbName.toLowerCase();
      String[] parsedDbName = parseDbName(dbName, conf);
      tableName = tableName.toLowerCase();
      colName = colName.toLowerCase();
      String convertedPartName = lowerCaseConvertPartName(partName);
      startFunction("get_column_statistics_by_partition", ": table=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tableName) + " partition=" + convertedPartName + " column=" + colName);
      ColumnStatistics statsObj = null;

      try {
        List<ColumnStatistics> list = getMS().getPartitionColumnStatistics(
            parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
            Lists.newArrayList(convertedPartName), Lists.newArrayList(colName));
        if (list.isEmpty()) {
          return null;
        }
        if (list.size() != 1) {
          throw new MetaException(list.size() + " statistics for single column and partition");
        }
        statsObj = list.get(0);
      } finally {
        endFunction("get_column_statistics_by_partition", statsObj != null, null, tableName);
      }
      return statsObj;
    }

    @Override
    public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
        throws TException {
      String catName = request.isSetCatName() ? request.getCatName().toLowerCase() : getDefaultCatalog(conf);
      String dbName = request.getDbName().toLowerCase();
      String tblName = request.getTblName().toLowerCase();
      startFunction("get_partitions_statistics_req", ": table=" +
          getCatalogQualifiedTableName(catName, dbName, tblName));

      PartitionsStatsResult result = null;
      List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
      for (String colName : request.getColNames()) {
        lowerCaseColNames.add(colName.toLowerCase());
      }
      List<String> lowerCasePartNames = new ArrayList<>(request.getPartNames().size());
      for (String partName : request.getPartNames()) {
        lowerCasePartNames.add(lowerCaseConvertPartName(partName));
      }
      try {
        List<ColumnStatistics> stats = getMS().getPartitionColumnStatistics(
            catName, dbName, tblName, lowerCasePartNames, lowerCaseColNames);
        Map<String, List<ColumnStatisticsObj>> map = new HashMap<>();
        for (ColumnStatistics stat : stats) {
          map.put(stat.getStatsDesc().getPartName(), stat.getStatsObj());
        }
        result = new PartitionsStatsResult(map);
      } finally {
        endFunction("get_partitions_statistics_req", result == null, null, tblName);
      }
      return result;
    }

    @Override
    public boolean update_table_column_statistics(ColumnStatistics colStats) throws TException {
      String catName;
      String dbName;
      String tableName;
      String colName;
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
      catName = statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() : getDefaultCatalog(conf);
      dbName = statsDesc.getDbName().toLowerCase();
      tableName = statsDesc.getTableName().toLowerCase();

      statsDesc.setCatName(catName);
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tableName);
      long time = System.currentTimeMillis() / 1000;
      statsDesc.setLastAnalyzed(time);

      List<ColumnStatisticsObj> statsObjs =  colStats.getStatsObj();

      startFunction("write_column_statistics", ":  table=" +
          Warehouse.getCatalogQualifiedTableName(catName, dbName, tableName));
      for (ColumnStatisticsObj statsObj:statsObjs) {
        colName = statsObj.getColName().toLowerCase();
        statsObj.setColName(colName);
        statsObj.setColType(statsObj.getColType().toLowerCase());
      }

     colStats.setStatsDesc(statsDesc);
     colStats.setStatsObj(statsObjs);

     boolean ret = false;

      try {
        ret = getMS().updateTableColumnStatistics(colStats);
        return ret;
      } finally {
        endFunction("write_column_statistics", ret != false, null, tableName);
      }
    }

    private boolean updatePartitonColStats(Table tbl, ColumnStatistics colStats)
        throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
      String catName;
      String dbName;
      String tableName;
      String partName;
      String colName;

      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
      catName = statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() : getDefaultCatalog(conf);
      dbName = statsDesc.getDbName().toLowerCase();
      tableName = statsDesc.getTableName().toLowerCase();
      partName = lowerCaseConvertPartName(statsDesc.getPartName());

      statsDesc.setCatName(catName);
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tableName);
      statsDesc.setPartName(partName);

      long time = System.currentTimeMillis() / 1000;
      statsDesc.setLastAnalyzed(time);

      List<ColumnStatisticsObj> statsObjs =  colStats.getStatsObj();

      startFunction("write_partition_column_statistics",
          ":  db=" + dbName + " table=" + tableName
              + " part=" + partName);
      for (ColumnStatisticsObj statsObj:statsObjs) {
        colName = statsObj.getColName().toLowerCase();
        statsObj.setColName(colName);
        statsObj.setColType(statsObj.getColType().toLowerCase());
      }

      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);

      boolean ret = false;

      try {
        if (tbl == null) {
          tbl = getTable(catName, dbName, tableName);
        }
        List<String> partVals = getPartValsFromName(tbl, partName);
        ret = getMS().updatePartitionColumnStatistics(colStats, partVals);
        return ret;
      } finally {
        endFunction("write_partition_column_statistics", ret != false, null, tableName);
      }
    }

    @Override
    public boolean update_partition_column_statistics(ColumnStatistics colStats) throws TException {
      return updatePartitonColStats(null, colStats);
    }

    @Override
    public boolean delete_partition_column_statistics(String dbName, String tableName,
                                                      String partName, String colName) throws TException {
      dbName = dbName.toLowerCase();
      String[] parsedDbName = parseDbName(dbName, conf);
      tableName = tableName.toLowerCase();
      if (colName != null) {
        colName = colName.toLowerCase();
      }
      String convertedPartName = lowerCaseConvertPartName(partName);
      startFunction("delete_column_statistics_by_partition",": table=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName) +
          " partition=" + convertedPartName + " column=" + colName);
      boolean ret = false;

      try {
        List<String> partVals = getPartValsFromName(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, convertedPartName);
        ret = getMS().deletePartitionColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                                                      convertedPartName, partVals, colName);
      } finally {
        endFunction("delete_column_statistics_by_partition", ret != false, null, tableName);
      }
      return ret;
    }

    @Override
    public boolean delete_table_column_statistics(String dbName, String tableName, String colName)
        throws TException {
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();

      String[] parsedDbName = parseDbName(dbName, conf);

      if (colName != null) {
        colName = colName.toLowerCase();
      }
      startFunction("delete_column_statistics_by_table", ": table=" +
          getCatalogQualifiedTableName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName) + " column=" +
          colName);

      boolean ret = false;
      try {
        ret = getMS().deleteTableColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, colName);
      } finally {
        endFunction("delete_column_statistics_by_table", ret != false, null, tableName);
      }
      return ret;
    }

    @Override
    public List<Partition> get_partitions_by_filter(final String dbName, final String tblName,
                                                    final String filter, final short maxParts)
        throws TException {
      String[] parsedDbName = parseDbName(dbName, conf);
      startTableFunction("get_partitions_by_filter", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tblName);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
      List<Partition> ret = null;
      Exception ex = null;
      try {
        checkLimitNumberOfPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tblName, filter, maxParts);
        ret = getMS().getPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
            filter, maxParts);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_by_filter", ret != null, ex, tblName);
      }
      return ret;
    }

    @Override
    public List<PartitionSpec> get_part_specs_by_filter(final String dbName, final String tblName,
                                                        final String filter, final int maxParts)
        throws TException {

      String[] parsedDbName = parseDbName(dbName, conf);
      startTableFunction("get_partitions_by_filter_pspec", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);

      List<PartitionSpec> partitionSpecs = null;
      try {
        Table table = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
        // Don't pass the parsed db name, as get_partitions_by_filter will parse it itself
        List<Partition> partitions = get_partitions_by_filter(dbName, tblName, filter, (short) maxParts);

        if (is_partition_spec_grouping_enabled(table)) {
          partitionSpecs = get_partitionspecs_grouped_by_storage_descriptor(table, partitions);
        }
        else {
          PartitionSpec pSpec = new PartitionSpec();
          pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
          pSpec.setRootPath(table.getSd().getLocation());
          pSpec.setCatName(parsedDbName[CAT_NAME]);
          pSpec.setDbName(parsedDbName[DB_NAME]);
          pSpec.setTableName(tblName);
          partitionSpecs = Arrays.asList(pSpec);
        }

        return partitionSpecs;
      }
      finally {
        endFunction("get_partitions_by_filter_pspec", partitionSpecs != null && !partitionSpecs.isEmpty(), null, tblName);
      }
    }

    @Override
    public PartitionsByExprResult get_partitions_by_expr(
        PartitionsByExprRequest req) throws TException {
      String dbName = req.getDbName(), tblName = req.getTblName();
      String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
      startTableFunction("get_partitions_by_expr", catName, dbName, tblName);
      fireReadTablePreEvent(catName, dbName, tblName);
      PartitionsByExprResult ret = null;
      Exception ex = null;
      try {
        checkLimitNumberOfPartitionsByExpr(catName, dbName, tblName, req.getExpr(), UNLIMITED_MAX_PARTITIONS);
        List<Partition> partitions = new LinkedList<>();
        boolean hasUnknownPartitions = getMS().getPartitionsByExpr(catName, dbName, tblName,
            req.getExpr(), req.getDefaultPartitionName(), req.getMaxParts(), partitions);
        ret = new PartitionsByExprResult(partitions, hasUnknownPartitions);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_by_expr", ret != null, ex, tblName);
      }
      return ret;
    }

    private void rethrowException(Exception e) throws TException {
      // TODO: Both of these are TException, why do we need these separate clauses?
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        throw newMetaException(e);
      }
    }

    @Override
    public int get_num_partitions_by_filter(final String dbName,
                                            final String tblName, final String filter)
            throws TException {
      String[] parsedDbName = parseDbName(dbName, conf);
      startTableFunction("get_num_partitions_by_filter", parsedDbName[CAT_NAME],
          parsedDbName[DB_NAME], tblName);

      int ret = -1;
      Exception ex = null;
      try {
        ret = getMS().getNumPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tblName, filter);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_num_partitions_by_filter", ret != -1, ex, tblName);
      }
      return ret;
    }

    private int get_num_partitions_by_expr(final String catName, final String dbName,
                                           final String tblName, final byte[] expr)
        throws TException {
      int ret = -1;
      Exception ex = null;
      try {
        ret = getMS().getNumPartitionsByExpr(catName, dbName, tblName, expr);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_num_partitions_by_expr", ret != -1, ex, tblName);
      }
      return ret;
    }

    @Override
    public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
                                                   final List<String> partNames) throws TException {

      String[] parsedDbName = parseDbName(dbName, conf);
      startTableFunction("get_partitions_by_names", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tblName);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
      List<Partition> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getPartitionsByNames(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
            partNames);
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_partitions_by_names", ret != null, ex, tblName);
      }
      return ret;
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName,
                                                   List<String> groupNames) throws TException {
      firePreEvent(new PreAuthorizationCallEvent(this));
      String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : getDefaultCatalog(conf);
      if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
        String partName = getPartName(hiveObject);
        return this.get_column_privilege_set(catName, hiveObject.getDbName(), hiveObject
            .getObjectName(), partName, hiveObject.getColumnName(), userName,
            groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
        String partName = getPartName(hiveObject);
        return this.get_partition_privilege_set(catName, hiveObject.getDbName(),
            hiveObject.getObjectName(), partName, userName, groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
        return this.get_db_privilege_set(catName, hiveObject.getDbName(), userName,
            groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
        return this.get_table_privilege_set(catName, hiveObject.getDbName(), hiveObject
            .getObjectName(), userName, groupNames);
      } else if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
        return this.get_user_privilege_set(userName, groupNames);
      }
      return null;
    }

    private String getPartName(HiveObjectRef hiveObject) throws MetaException {
      String partName = null;
      List<String> partValue = hiveObject.getPartValues();
      if (partValue != null && partValue.size() > 0) {
        try {
          String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
              getDefaultCatalog(conf);
          Table table = get_table_core(catName, hiveObject.getDbName(), hiveObject
              .getObjectName());
          partName = Warehouse
              .makePartName(table.getPartitionKeys(), partValue);
        } catch (NoSuchObjectException e) {
          throw new MetaException(e.getMessage());
        }
      }
      return partName;
    }

    private PrincipalPrivilegeSet get_column_privilege_set(String catName, final String dbName,
        final String tableName, final String partName, final String columnName,
        final String userName, final List<String> groupNames) throws TException {
      incrementCounter("get_column_privilege_set");

      PrincipalPrivilegeSet ret;
      try {
        ret = getMS().getColumnPrivilegeSet(
            catName, dbName, tableName, partName, columnName, userName, groupNames);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    private PrincipalPrivilegeSet get_db_privilege_set(String catName, final String dbName,
        final String userName, final List<String> groupNames) throws TException {
      incrementCounter("get_db_privilege_set");

      PrincipalPrivilegeSet ret;
      try {
        ret = getMS().getDBPrivilegeSet(catName, dbName, userName, groupNames);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    private PrincipalPrivilegeSet get_partition_privilege_set(
        String catName, final String dbName, final String tableName, final String partName,
        final String userName, final List<String> groupNames)
        throws TException {
      incrementCounter("get_partition_privilege_set");

      PrincipalPrivilegeSet ret;
      try {
        ret = getMS().getPartitionPrivilegeSet(catName, dbName, tableName, partName,
            userName, groupNames);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    private PrincipalPrivilegeSet get_table_privilege_set(String catName, final String dbName,
        final String tableName, final String userName,
        final List<String> groupNames) throws TException {
      incrementCounter("get_table_privilege_set");

      PrincipalPrivilegeSet ret;
      try {
        ret = getMS().getTablePrivilegeSet(catName, dbName, tableName, userName,
            groupNames);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean grant_role(final String roleName,
        final String principalName, final PrincipalType principalType,
        final String grantor, final PrincipalType grantorType, final boolean grantOption)
        throws TException {
      incrementCounter("add_role_member");
      firePreEvent(new PreAuthorizationCallEvent(this));
      if (PUBLIC.equals(roleName)) {
        throw new MetaException("No user can be added to " + PUBLIC +". Since all users implicitly"
        + " belong to " + PUBLIC + " role.");
      }
      Boolean ret;
      try {
        RawStore ms = getMS();
        Role role = ms.getRole(roleName);
        if(principalType == PrincipalType.ROLE){
          //check if this grant statement will end up creating a cycle
          if(isNewRoleAParent(principalName, roleName)){
            throw new MetaException("Cannot grant role " + principalName + " to " + roleName +
                " as " + roleName + " already belongs to the role " + principalName +
                ". (no cycles allowed)");
          }
        }
        ret = ms.grantRole(role, principalName, principalType, grantor, grantorType, grantOption);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }



    /**
     * Check if newRole is in parent hierarchy of curRole
     * @param newRole
     * @param curRole
     * @return true if newRole is curRole or present in its hierarchy
     * @throws MetaException
     */
    private boolean isNewRoleAParent(String newRole, String curRole) throws MetaException {
      if(newRole.equals(curRole)){
        return true;
      }
      //do this check recursively on all the parent roles of curRole
      List<Role> parentRoleMaps = getMS().listRoles(curRole, PrincipalType.ROLE);
      for(Role parentRole : parentRoleMaps){
        if(isNewRoleAParent(newRole, parentRole.getRoleName())){
          return true;
        }
      }
      return false;
    }

    @Override
    public List<Role> list_roles(final String principalName,
        final PrincipalType principalType) throws TException {
      incrementCounter("list_roles");
      firePreEvent(new PreAuthorizationCallEvent(this));
      return getMS().listRoles(principalName, principalType);
    }

    @Override
    public boolean create_role(final Role role) throws TException {
      incrementCounter("create_role");
      firePreEvent(new PreAuthorizationCallEvent(this));
      if (PUBLIC.equals(role.getRoleName())) {
         throw new MetaException(PUBLIC + " role implicitly exists. It can't be created.");
      }
      Boolean ret;
      try {
        ret = getMS().addRole(role.getRoleName(), role.getOwnerName());
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean drop_role(final String roleName) throws TException {
      incrementCounter("drop_role");
      firePreEvent(new PreAuthorizationCallEvent(this));
      if (ADMIN.equals(roleName) || PUBLIC.equals(roleName)) {
        throw new MetaException(PUBLIC + "," + ADMIN + " roles can't be dropped.");
      }
      Boolean ret;
      try {
        ret = getMS().removeRole(roleName);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public List<String> get_role_names() throws TException {
      incrementCounter("get_role_names");
      firePreEvent(new PreAuthorizationCallEvent(this));
      List<String> ret;
      try {
        ret = getMS().listRoleNames();
        return ret;
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean grant_privileges(final PrivilegeBag privileges) throws TException {
      incrementCounter("grant_privileges");
      firePreEvent(new PreAuthorizationCallEvent(this));
      Boolean ret;
      try {
        ret = getMS().grantPrivileges(privileges);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public boolean revoke_role(final String roleName, final String userName,
        final PrincipalType principalType) throws TException {
      return revoke_role(roleName, userName, principalType, false);
    }

    private boolean revoke_role(final String roleName, final String userName,
        final PrincipalType principalType, boolean grantOption) throws TException {
      incrementCounter("remove_role_member");
      firePreEvent(new PreAuthorizationCallEvent(this));
      if (PUBLIC.equals(roleName)) {
        throw new MetaException(PUBLIC + " role can't be revoked.");
      }
      Boolean ret;
      try {
        RawStore ms = getMS();
        Role mRole = ms.getRole(roleName);
        ret = ms.revokeRole(mRole, userName, principalType, grantOption);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request)
        throws TException {
      GrantRevokeRoleResponse response = new GrantRevokeRoleResponse();
      boolean grantOption = false;
      if (request.isSetGrantOption()) {
        grantOption = request.isGrantOption();
      }
      switch (request.getRequestType()) {
        case GRANT: {
          boolean result = grant_role(request.getRoleName(),
              request.getPrincipalName(), request.getPrincipalType(),
              request.getGrantor(), request.getGrantorType(), grantOption);
          response.setSuccess(result);
          break;
        }
        case REVOKE: {
          boolean result = revoke_role(request.getRoleName(), request.getPrincipalName(),
              request.getPrincipalType(), grantOption);
          response.setSuccess(result);
          break;
        }
        default:
          throw new MetaException("Unknown request type " + request.getRequestType());
      }

      return response;
    }

    @Override
    public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
        throws TException {
      GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
      switch (request.getRequestType()) {
        case GRANT: {
          boolean result = grant_privileges(request.getPrivileges());
          response.setSuccess(result);
          break;
        }
        case REVOKE: {
          boolean revokeGrantOption = false;
          if (request.isSetRevokeGrantOption()) {
            revokeGrantOption = request.isRevokeGrantOption();
          }
          boolean result = revoke_privileges(request.getPrivileges(), revokeGrantOption);
          response.setSuccess(result);
          break;
        }
        default:
          throw new MetaException("Unknown request type " + request.getRequestType());
      }

      return response;
    }

    @Override
    public boolean revoke_privileges(final PrivilegeBag privileges) throws TException {
      return revoke_privileges(privileges, false);
    }

    public boolean revoke_privileges(final PrivilegeBag privileges, boolean grantOption)
        throws TException {
      incrementCounter("revoke_privileges");
      firePreEvent(new PreAuthorizationCallEvent(this));
      Boolean ret;
      try {
        ret = getMS().revokePrivileges(privileges, grantOption);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    private PrincipalPrivilegeSet get_user_privilege_set(final String userName,
        final List<String> groupNames) throws TException {
      incrementCounter("get_user_privilege_set");
      PrincipalPrivilegeSet ret;
      try {
        ret = getMS().getUserPrivilegeSet(userName, groupNames);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principalName,
        PrincipalType principalType, HiveObjectRef hiveObject)
        throws TException {
      firePreEvent(new PreAuthorizationCallEvent(this));
      String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : getDefaultCatalog(conf);
      if (hiveObject.getObjectType() == null) {
        return getAllPrivileges(principalName, principalType, catName);
      }
      if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
        return list_global_privileges(principalName, principalType);
      }
      if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
        return list_db_privileges(principalName, principalType, catName, hiveObject
            .getDbName());
      }
      if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
        return list_table_privileges(principalName, principalType,
            catName, hiveObject.getDbName(), hiveObject.getObjectName());
      }
      if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
        return list_partition_privileges(principalName, principalType,
            catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
            .getPartValues());
      }
      if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
        if (hiveObject.getPartValues() == null || hiveObject.getPartValues().isEmpty()) {
          return list_table_column_privileges(principalName, principalType,
              catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject.getColumnName());
        }
        return list_partition_column_privileges(principalName, principalType,
            catName, hiveObject.getDbName(), hiveObject.getObjectName(), hiveObject
            .getPartValues(), hiveObject.getColumnName());
      }
      return null;
    }

    private List<HiveObjectPrivilege> getAllPrivileges(String principalName,
        PrincipalType principalType, String catName) throws TException {
      List<HiveObjectPrivilege> privs = new ArrayList<>();
      privs.addAll(list_global_privileges(principalName, principalType));
      privs.addAll(list_db_privileges(principalName, principalType, catName, null));
      privs.addAll(list_table_privileges(principalName, principalType, catName, null, null));
      privs.addAll(list_partition_privileges(principalName, principalType, catName, null, null, null));
      privs.addAll(list_table_column_privileges(principalName, principalType, catName, null, null, null));
      privs.addAll(list_partition_column_privileges(principalName, principalType,
          catName, null, null, null, null));
      return privs;
    }

    private List<HiveObjectPrivilege> list_table_column_privileges(
        final String principalName, final PrincipalType principalType, String catName,
        final String dbName, final String tableName, final String columnName) throws TException {
      incrementCounter("list_table_column_privileges");

      try {
        if (dbName == null) {
          return getMS().listPrincipalTableColumnGrantsAll(principalName, principalType);
        }
        if (principalName == null) {
          return getMS().listTableColumnGrantsAll(catName, dbName, tableName, columnName);
        }
        return getMS().listPrincipalTableColumnGrants(principalName, principalType,
                catName, dbName, tableName, columnName);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private List<HiveObjectPrivilege> list_partition_column_privileges(
        final String principalName, final PrincipalType principalType,
        String catName, final String dbName, final String tableName, final List<String> partValues,
        final String columnName) throws TException {
      incrementCounter("list_partition_column_privileges");

      try {
        if (dbName == null) {
          return getMS().listPrincipalPartitionColumnGrantsAll(principalName, principalType);
        }
        Table tbl = get_table_core(catName, dbName, tableName);
        String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
        if (principalName == null) {
          return getMS().listPartitionColumnGrantsAll(catName, dbName, tableName, partName, columnName);
        }

        return getMS().listPrincipalPartitionColumnGrants(principalName, principalType, catName, dbName,
                tableName, partValues, partName, columnName);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private List<HiveObjectPrivilege> list_db_privileges(final String principalName,
        final PrincipalType principalType, String catName, final String dbName) throws TException {
      incrementCounter("list_security_db_grant");

      try {
        if (dbName == null) {
          return getMS().listPrincipalDBGrantsAll(principalName, principalType);
        }
        if (principalName == null) {
          return getMS().listDBGrantsAll(catName, dbName);
        } else {
          return getMS().listPrincipalDBGrants(principalName, principalType, catName, dbName);
        }
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private List<HiveObjectPrivilege> list_partition_privileges(
        final String principalName, final PrincipalType principalType,
        String catName, final String dbName, final String tableName, final List<String> partValues)
        throws TException {
      incrementCounter("list_security_partition_grant");

      try {
        if (dbName == null) {
          return getMS().listPrincipalPartitionGrantsAll(principalName, principalType);
        }
        Table tbl = get_table_core(catName, dbName, tableName);
        String partName = Warehouse.makePartName(tbl.getPartitionKeys(), partValues);
        if (principalName == null) {
          return getMS().listPartitionGrantsAll(catName, dbName, tableName, partName);
        }
        return getMS().listPrincipalPartitionGrants(
            principalName, principalType, catName, dbName, tableName, partValues, partName);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private List<HiveObjectPrivilege> list_table_privileges(
        final String principalName, final PrincipalType principalType,
        String catName, final String dbName, final String tableName) throws TException {
      incrementCounter("list_security_table_grant");

      try {
        if (dbName == null) {
          return getMS().listPrincipalTableGrantsAll(principalName, principalType);
        }
        if (principalName == null) {
          return getMS().listTableGrantsAll(catName, dbName, tableName);
        }
        return getMS().listAllTableGrants(principalName, principalType, catName, dbName, tableName);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private List<HiveObjectPrivilege> list_global_privileges(
        final String principalName, final PrincipalType principalType) throws TException {
      incrementCounter("list_security_user_grant");

      try {
        if (principalName == null) {
          return getMS().listGlobalGrantsAll();
        }
        return getMS().listPrincipalGlobalGrants(principalName, principalType);
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void cancel_delegation_token(String token_str_form) throws TException {
      startFunction("cancel_delegation_token");
      boolean success = false;
      Exception ex = null;
      try {
        HiveMetaStore.cancelDelegationToken(token_str_form);
        success = true;
      } catch (IOException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("cancel_delegation_token", success, ex);
      }
    }

    @Override
    public long renew_delegation_token(String token_str_form) throws TException {
      startFunction("renew_delegation_token");
      Long ret = null;
      Exception ex = null;
      try {
        ret = HiveMetaStore.renewDelegationToken(token_str_form);
      } catch (IOException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("renew_delegation_token", ret != null, ex);
      }
      return ret;
    }

    @Override
    public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
        throws TException {
      startFunction("get_delegation_token");
      String ret = null;
      Exception ex = null;
      try {
        ret =
            HiveMetaStore.getDelegationToken(token_owner,
                renewer_kerberos_principal_name, getIPAddress());
      } catch (IOException | InterruptedException e) {
        ex = e;
        throw new MetaException(e.getMessage());
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("get_delegation_token", ret != null, ex);
      }
      return ret;
    }

    @Override
    public boolean add_token(String token_identifier, String delegation_token) throws TException {
      startFunction("add_token", ": " + token_identifier);
      boolean ret = false;
      Exception ex = null;
      try {
        ret = getMS().addToken(token_identifier, delegation_token);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("add_token", ret == true, ex);
      }
      return ret;
    }

    @Override
    public boolean remove_token(String token_identifier) throws TException {
      startFunction("remove_token", ": " + token_identifier);
      boolean ret = false;
      Exception ex = null;
      try {
        ret = getMS().removeToken(token_identifier);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("remove_token", ret == true, ex);
      }
      return ret;
    }

    @Override
    public String get_token(String token_identifier) throws TException {
      startFunction("get_token for", ": " + token_identifier);
      String ret = null;
      Exception ex = null;
      try {
        ret = getMS().getToken(token_identifier);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_token", ret != null, ex);
      }
      //Thrift cannot return null result
      return ret == null ? "" : ret;
    }

    @Override
    public List<String> get_all_token_identifiers() throws TException {
      startFunction("get_all_token_identifiers.");
      List<String> ret;
      Exception ex = null;
      try {
        ret = getMS().getAllTokenIdentifiers();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_all_token_identifiers.", ex == null, ex);
      }
      return ret;
    }

    @Override
    public int add_master_key(String key) throws TException {
      startFunction("add_master_key.");
      int ret;
      Exception ex = null;
      try {
        ret = getMS().addMasterKey(key);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("add_master_key.", ex == null, ex);
      }
      return ret;
    }

    @Override
    public void update_master_key(int seq_number, String key) throws TException {
      startFunction("update_master_key.");
      Exception ex = null;
      try {
        getMS().updateMasterKey(seq_number, key);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("update_master_key.", ex == null, ex);
      }
    }

    @Override
    public boolean remove_master_key(int key_seq) throws TException {
      startFunction("remove_master_key.");
      Exception ex = null;
      boolean ret;
      try {
        ret = getMS().removeMasterKey(key_seq);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("remove_master_key.", ex == null, ex);
      }
      return ret;
    }

    @Override
    public List<String> get_master_keys() throws TException {
      startFunction("get_master_keys.");
      Exception ex = null;
      String [] ret = null;
      try {
        ret = getMS().getMasterKeys();
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_master_keys.", ret != null, ex);
      }
      return Arrays.asList(ret);
    }

    @Override
    public void markPartitionForEvent(final String db_name, final String tbl_name,
        final Map<String, String> partName, final PartitionEventType evtType) throws TException {

      Table tbl = null;
      Exception ex = null;
      RawStore ms  = getMS();
      boolean success = false;
      try {
        String[] parsedDbName = parseDbName(db_name, conf);
        ms.openTransaction();
        startPartitionFunction("markPartitionForEvent", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, partName);
        firePreEvent(new PreLoadPartitionDoneEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, partName, this));
        tbl = ms.markPartitionForEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
            partName, evtType);
        if (null == tbl) {
          throw new UnknownTableException("Table: " + tbl_name + " not found.");
        }

        if (transactionalListeners.size() > 0) {
          LoadPartitionDoneEvent lpde = new LoadPartitionDoneEvent(true, tbl, partName, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onLoadPartitionDone(lpde);
          }
        }

        success = ms.commitTransaction();
        for (MetaStoreEventListener listener : listeners) {
          listener.onLoadPartitionDone(new LoadPartitionDoneEvent(true, tbl, partName, this));
        }
      } catch (Exception original) {
        ex = original;
        LOG.error("Exception caught in mark partition event ", original);
        if (original instanceof UnknownTableException) {
          throw (UnknownTableException) original;
        } else if (original instanceof UnknownPartitionException) {
          throw (UnknownPartitionException) original;
        } else if (original instanceof InvalidPartitionException) {
          throw (InvalidPartitionException) original;
        } else if (original instanceof MetaException) {
          throw (MetaException) original;
        } else {
          throw newMetaException(original);
        }
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }

        endFunction("markPartitionForEvent", tbl != null, ex, tbl_name);
      }
    }

    @Override
    public boolean isPartitionMarkedForEvent(final String db_name, final String tbl_name,
        final Map<String, String> partName, final PartitionEventType evtType) throws TException {

      String[] parsedDbName = parseDbName(db_name, conf);
      startPartitionFunction("isPartitionMarkedForEvent", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, partName);
      Boolean ret = null;
      Exception ex = null;
      try {
        ret = getMS().isPartitionMarkedForEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, partName, evtType);
      } catch (Exception original) {
        LOG.error("Exception caught for isPartitionMarkedForEvent ",original);
        ex = original;
        if (original instanceof UnknownTableException) {
          throw (UnknownTableException) original;
        } else if (original instanceof UnknownPartitionException) {
          throw (UnknownPartitionException) original;
        } else if (original instanceof InvalidPartitionException) {
          throw (InvalidPartitionException) original;
        } else if (original instanceof MetaException) {
          throw (MetaException) original;
        } else {
          throw newMetaException(original);
        }
      } finally {
                endFunction("isPartitionMarkedForEvent", ret != null, ex, tbl_name);
      }

      return ret;
    }

    @Override
    public List<String> set_ugi(String username, List<String> groupNames) throws TException {
      Collections.addAll(groupNames, username);
      return groupNames;
    }

    @Override
    public boolean partition_name_has_valid_characters(List<String> part_vals,
        boolean throw_exception) throws TException {
      startFunction("partition_name_has_valid_characters");
      boolean ret;
      Exception ex = null;
      try {
        if (throw_exception) {
          MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
          ret = true;
        } else {
          ret = MetaStoreUtils.partitionNameHasValidCharacters(part_vals,
              partitionValidationPattern);
        }
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException)e;
        } else {
          throw newMetaException(e);
        }
      }
      endFunction("partition_name_has_valid_characters", true, ex);
      return ret;
    }

    private static MetaException newMetaException(Exception e) {
      if (e instanceof MetaException) {
        return (MetaException)e;
      }
      MetaException me = new MetaException(e.toString());
      me.initCause(e);
      return me;
    }

    private void validateFunctionInfo(Function func) throws InvalidObjectException, MetaException {
      if (!MetaStoreUtils.validateName(func.getFunctionName(), null)) {
        throw new InvalidObjectException(func.getFunctionName() + " is not a valid object name");
      }
      String className = func.getClassName();
      if (className == null) {
        throw new InvalidObjectException("Function class name cannot be null");
      }
    }

    @Override
    public void create_function(Function func) throws TException {
      validateFunctionInfo(func);
      boolean success = false;
      RawStore ms = getMS();
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      try {
        String catName = func.isSetCatName() ? func.getCatName() : getDefaultCatalog(conf);
        ms.openTransaction();
        Database db = ms.getDatabase(catName, func.getDbName());
        if (db == null) {
          throw new NoSuchObjectException("The database " + func.getDbName() + " does not exist");
        }

        Function existingFunc = ms.getFunction(catName, func.getDbName(), func.getFunctionName());
        if (existingFunc != null) {
          throw new AlreadyExistsException(
              "Function " + func.getFunctionName() + " already exists");
        }

        long time = System.currentTimeMillis() / 1000;
        func.setCreateTime((int) time);
        ms.createFunction(func);
        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.CREATE_FUNCTION,
                                                    new CreateFunctionEvent(func, true, this));
        }

        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.CREATE_FUNCTION,
                                                new CreateFunctionEvent(func, success, this),
                                                null,
                                                transactionalListenerResponses, ms);
        }
      }
    }

    @Override
    public void drop_function(String dbName, String funcName)
        throws NoSuchObjectException, MetaException,
        InvalidObjectException, InvalidInputException {
      boolean success = false;
      Function func = null;
      RawStore ms = getMS();
      Map<String, String> transactionalListenerResponses = Collections.emptyMap();
      String[] parsedDbName = parseDbName(dbName, conf);
      try {
        ms.openTransaction();
        func = ms.getFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
        if (func == null) {
          throw new NoSuchObjectException("Function " + funcName + " does not exist");
        }
        // if copy of jar to change management fails we fail the metastore transaction, since the
        // user might delete the jars on HDFS externally after dropping the function, hence having
        // a copy is required to allow incremental replication to work correctly.
        if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
          for (ResourceUri uri : func.getResourceUris()) {
            if (uri.getUri().toLowerCase().startsWith("hdfs:")) {
              wh.addToChangeManagement(new Path(uri.getUri()));
            }
          }
        }

        // if the operation on metastore fails, we don't do anything in change management, but fail
        // the metastore transaction, as having a copy of the jar in change management is not going
        // to cause any problem, the cleaner thread will remove this when this jar expires.
        ms.dropFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
        if (transactionalListeners.size() > 0) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                                                    EventType.DROP_FUNCTION,
                                                    new DropFunctionEvent(func, true, this));
        }
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }

        if (listeners.size() > 0) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                                                EventType.DROP_FUNCTION,
                                                new DropFunctionEvent(func, success, this),
                                                null,
                                                transactionalListenerResponses, ms);
        }
      }
    }

    @Override
    public void alter_function(String dbName, String funcName, Function newFunc) throws TException {
      validateFunctionInfo(newFunc);
      boolean success = false;
      RawStore ms = getMS();
      String[] parsedDbName = parseDbName(dbName, conf);
      try {
        ms.openTransaction();
        ms.alterFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName, newFunc);
        success = ms.commitTransaction();
      } finally {
        if (!success) {
          ms.rollbackTransaction();
        }
      }
    }

    @Override
    public List<String> get_functions(String dbName, String pattern)
        throws MetaException {
      startFunction("get_functions", ": db=" + dbName + " pat=" + pattern);

      RawStore ms = getMS();
      Exception ex = null;
      List<String> funcNames = null;
      String[] parsedDbName = parseDbName(dbName, conf);

      try {
        funcNames = ms.getFunctions(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern);
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("get_functions", funcNames != null, ex);
      }

      return funcNames;
    }

    @Override
    public GetAllFunctionsResponse get_all_functions()
            throws MetaException {
      GetAllFunctionsResponse response = new GetAllFunctionsResponse();
      startFunction("get_all_functions");
      RawStore ms = getMS();
      List<Function> allFunctions = null;
      Exception ex = null;
      try {
        // Leaving this as the 'hive' catalog (rather than choosing the default from the
        // configuration) because all the default UDFs are in that catalog, and I think that's
        // would people really want here.
        allFunctions = ms.getAllFunctions(DEFAULT_CATALOG_NAME);
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("get_all_functions", allFunctions != null, ex);
      }
      response.setFunctions(allFunctions);
      return response;
    }

    @Override
    public Function get_function(String dbName, String funcName) throws TException {
      startFunction("get_function", ": " + dbName + "." + funcName);

      RawStore ms = getMS();
      Function func = null;
      Exception ex = null;
      String[] parsedDbName = parseDbName(dbName, conf);

      try {
        func = ms.getFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
        if (func == null) {
          throw new NoSuchObjectException(
              "Function " + dbName + "." + funcName + " does not exist");
        }
      } catch (NoSuchObjectException e) {
        ex = e;
        rethrowException(e);
      } catch (Exception e) {
        ex = e;
        throw newMetaException(e);
      } finally {
        endFunction("get_function", func != null, ex);
      }

      return func;
    }

    // Transaction and locking methods
    @Override
    public GetOpenTxnsResponse get_open_txns() throws TException {
      return getTxnHandler().getOpenTxns();
    }

    // Transaction and locking methods
    @Override
    public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
      return getTxnHandler().getOpenTxnsInfo();
    }

    @Override
    public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
      OpenTxnsResponse response = getTxnHandler().openTxns(rqst);
      List<Long> txnIds = response.getTxn_ids();
      if (txnIds != null && listeners != null && !listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.OPEN_TXN,
            new OpenTxnEvent(txnIds, this));
      }
      return response;
    }

    @Override
    public void abort_txn(AbortTxnRequest rqst) throws TException {
      getTxnHandler().abortTxn(rqst);
      if (listeners != null && !listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ABORT_TXN,
                new AbortTxnEvent(rqst.getTxnid(), this));
      }
    }

    @Override
    public void abort_txns(AbortTxnsRequest rqst) throws TException {
      getTxnHandler().abortTxns(rqst);
      if (listeners != null && !listeners.isEmpty()) {
        for (Long txnId : rqst.getTxn_ids()) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ABORT_TXN,
                  new AbortTxnEvent(txnId, this));
        }
      }
    }

    @Override
    public void commit_txn(CommitTxnRequest rqst) throws TException {
      getTxnHandler().commitTxn(rqst);
      if (listeners != null && !listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.COMMIT_TXN,
                new CommitTxnEvent(rqst.getTxnid(), this));
      }
    }

    @Override
    public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest rqst) throws TException {
      return getTxnHandler().getValidWriteIds(rqst);
    }

    @Override
    public AllocateTableWriteIdsResponse allocate_table_write_ids(
            AllocateTableWriteIdsRequest rqst) throws TException {
      return getTxnHandler().allocateTableWriteIds(rqst);
    }

    @Override
    public LockResponse lock(LockRequest rqst) throws TException {
      return getTxnHandler().lock(rqst);
    }

    @Override
    public LockResponse check_lock(CheckLockRequest rqst) throws TException {
      return getTxnHandler().checkLock(rqst);
    }

    @Override
    public void unlock(UnlockRequest rqst) throws TException {
      getTxnHandler().unlock(rqst);
    }

    @Override
    public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
      return getTxnHandler().showLocks(rqst);
    }

    @Override
    public void heartbeat(HeartbeatRequest ids) throws TException {
      getTxnHandler().heartbeat(ids);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest rqst)
      throws TException {
      return getTxnHandler().heartbeatTxnRange(rqst);
    }
    @Deprecated
    @Override
    public void compact(CompactionRequest rqst) throws TException {
      compact2(rqst);
    }
    @Override
    public CompactionResponse compact2(CompactionRequest rqst) throws TException {
      return getTxnHandler().compact(rqst);
    }

    @Override
    public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
      return getTxnHandler().showCompact(rqst);
    }

    @Override
    public void flushCache() throws TException {
      getMS().flushCache();
    }

    @Override
    public void add_dynamic_partitions(AddDynamicPartitions rqst) throws TException {
      getTxnHandler().addDynamicPartitions(rqst);
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
        throws TException {

      incrementCounter("get_principals_in_role");
      firePreEvent(new PreAuthorizationCallEvent(this));
      Exception ex = null;
      GetPrincipalsInRoleResponse response = null;
      try {
        response = new GetPrincipalsInRoleResponse(getMS().listRoleMembers(request.getRoleName()));
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_principals_in_role", ex == null, ex);
      }
      return response;
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
        GetRoleGrantsForPrincipalRequest request) throws TException {

      incrementCounter("get_role_grants_for_principal");
      firePreEvent(new PreAuthorizationCallEvent(this));
      Exception ex = null;
      List<RolePrincipalGrant> roleMaps = null;
      try {
        roleMaps = getMS().listRolesWithGrants(request.getPrincipal_name(), request.getPrincipal_type());
      } catch (MetaException e) {
        throw e;
      } catch (Exception e) {
        ex = e;
        rethrowException(e);
      } finally {
        endFunction("get_role_grants_for_principal", ex == null, ex);
      }

      //List<RolePrincipalGrant> roleGrantsList = getRolePrincipalGrants(roleMaps);
      return new GetRoleGrantsForPrincipalResponse(roleMaps);
    }

    @Override
    public AggrStats get_aggr_stats_for(PartitionsStatsRequest request) throws TException {
      String catName = request.isSetCatName() ? request.getCatName().toLowerCase() :
          getDefaultCatalog(conf);
      String dbName = request.getDbName().toLowerCase();
      String tblName = request.getTblName().toLowerCase();
      startFunction("get_aggr_stats_for", ": table=" +
          getCatalogQualifiedTableName(catName, dbName, tblName));

      List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
      for (String colName : request.getColNames()) {
        lowerCaseColNames.add(colName.toLowerCase());
      }
      List<String> lowerCasePartNames = new ArrayList<>(request.getPartNames().size());
      for (String partName : request.getPartNames()) {
        lowerCasePartNames.add(lowerCaseConvertPartName(partName));
      }
      AggrStats aggrStats = null;

      try {
        aggrStats = new AggrStats(getMS().get_aggr_stats_for(catName, dbName, tblName,
            lowerCasePartNames, lowerCaseColNames));
        return aggrStats;
      } finally {
          endFunction("get_aggr_stats_for", aggrStats == null, null, request.getTblName());
      }

    }

    @Override
    public boolean set_aggr_stats_for(SetPartitionsStatsRequest request) throws TException {
      boolean ret = true;
      List<ColumnStatistics> csNews = request.getColStats();
      if (csNews == null || csNews.isEmpty()) {
        return ret;
      }
      // figure out if it is table level or partition level
      ColumnStatistics firstColStats = csNews.get(0);
      ColumnStatisticsDesc statsDesc = firstColStats.getStatsDesc();
      String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
      String dbName = statsDesc.getDbName();
      String tableName = statsDesc.getTableName();
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj obj : firstColStats.getStatsObj()) {
        colNames.add(obj.getColName());
      }
      if (statsDesc.isIsTblLevel()) {
        // there should be only one ColumnStatistics
        if (request.getColStatsSize() != 1) {
          throw new MetaException(
              "Expecting only 1 ColumnStatistics for table's column stats, but find "
                  + request.getColStatsSize());
        } else {
          if (request.isSetNeedMerge() && request.isNeedMerge()) {
            // one single call to get all column stats
            ColumnStatistics csOld = getMS().getTableColumnStatistics(catName, dbName, tableName, colNames);
            Table t = getTable(catName, dbName, tableName);
            // we first use t.getParameters() to prune the stats
            MetaStoreUtils.getMergableCols(firstColStats, t.getParameters());
            // we merge those that can be merged
            if (csOld != null && csOld.getStatsObjSize() != 0
                && !firstColStats.getStatsObj().isEmpty()) {
              MetaStoreUtils.mergeColStats(firstColStats, csOld);
            }
            if (!firstColStats.getStatsObj().isEmpty()) {
              return update_table_column_statistics(firstColStats);
            } else {
              LOG.debug("All the column stats are not accurate to merge.");
              return true;
            }
          } else {
            // This is the overwrite case, we do not care about the accuracy.
            return update_table_column_statistics(firstColStats);
          }
        }
      } else {
        // partition level column stats merging
        List<Partition> partitions = new ArrayList<>();
        // note that we may have two or more duplicate partition names.
        // see autoColumnStats_2.q under TestMiniLlapLocalCliDriver
        Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
        for (ColumnStatistics csNew : csNews) {
          String partName = csNew.getStatsDesc().getPartName();
          if (newStatsMap.containsKey(partName)) {
            MetaStoreUtils.mergeColStats(csNew, newStatsMap.get(partName));
          }
          newStatsMap.put(partName, csNew);
        }

        Map<String, ColumnStatistics> oldStatsMap = new HashMap<>();
        Map<String, Partition> mapToPart = new HashMap<>();
        if (request.isSetNeedMerge() && request.isNeedMerge()) {
          // a single call to get all column stats for all partitions
          List<String> partitionNames = new ArrayList<>();
          partitionNames.addAll(newStatsMap.keySet());
          List<ColumnStatistics> csOlds = getMS().getPartitionColumnStatistics(catName, dbName,
              tableName, partitionNames, colNames);
          if (newStatsMap.values().size() != csOlds.size()) {
            // some of the partitions miss stats.
            LOG.debug("Some of the partitions miss stats.");
          }
          for (ColumnStatistics csOld : csOlds) {
            oldStatsMap.put(csOld.getStatsDesc().getPartName(), csOld);
          }
          // another single call to get all the partition objects
          partitions = getMS().getPartitionsByNames(catName, dbName, tableName, partitionNames);
          for (int index = 0; index < partitionNames.size(); index++) {
            mapToPart.put(partitionNames.get(index), partitions.get(index));
          }
        }
        Table t = getTable(catName, dbName, tableName);
        for (Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
          ColumnStatistics csNew = entry.getValue();
          ColumnStatistics csOld = oldStatsMap.get(entry.getKey());
          if (request.isSetNeedMerge() && request.isNeedMerge()) {
            // we first use getParameters() to prune the stats
            MetaStoreUtils.getMergableCols(csNew, mapToPart.get(entry.getKey()).getParameters());
            // we merge those that can be merged
            if (csOld != null && csOld.getStatsObjSize() != 0 && !csNew.getStatsObj().isEmpty()) {
              MetaStoreUtils.mergeColStats(csNew, csOld);
            }
            if (!csNew.getStatsObj().isEmpty()) {
              ret = ret && updatePartitonColStats(t, csNew);
            } else {
              LOG.debug("All the column stats " + csNew.getStatsDesc().getPartName()
                  + " are not accurate to merge.");
            }
          } else {
            ret = ret && updatePartitonColStats(t, csNew);
          }
        }
      }
      return ret;
    }

    private Table getTable(String catName, String dbName, String tableName)
        throws MetaException, InvalidObjectException {
      Table t = getMS().getTable(catName, dbName, tableName);
      if (t == null) {
        throw new InvalidObjectException(getCatalogQualifiedTableName(catName, dbName, tableName)
            + " table not found");
      }
      return t;
    }

    @Override
    public NotificationEventResponse get_next_notification(NotificationEventRequest rqst)
        throws TException {
      try {
        authorizeProxyPrivilege();
      } catch (Exception ex) {
        LOG.error("Not authorized to make the get_next_notification call. You can try to disable " +
            ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), ex);
        throw new TException(ex);
      }

      RawStore ms = getMS();
      return ms.getNextNotification(rqst);
    }

    @Override
    public CurrentNotificationEventId get_current_notificationEventId() throws TException {
      try {
        authorizeProxyPrivilege();
      } catch (Exception ex) {
        LOG.error("Not authorized to make the get_current_notificationEventId call. You can try to disable " +
            ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), ex);
        throw new TException(ex);
      }

      RawStore ms = getMS();
      return ms.getCurrentNotificationEventId();
    }

    @Override
    public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest rqst)
            throws TException {
      try {
        authorizeProxyPrivilege();
      } catch (Exception ex) {
        LOG.error("Not authorized to make the get_notification_events_count call. You can try to disable " +
            ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString(), ex);
        throw new TException(ex);
      }

      RawStore ms = getMS();
      return ms.getNotificationEventsCount(rqst);
    }

    private void authorizeProxyPrivilege() throws Exception {
      // Skip the auth in embedded mode or if the auth is disabled
      if (!isMetaStoreRemote() ||
          !MetastoreConf.getBoolVar(conf, ConfVars.EVENT_DB_NOTIFICATION_API_AUTH)) {
        return;
      }
      String user = null;
      try {
        user = SecurityUtils.getUGI().getShortUserName();
      } catch (Exception ex) {
        LOG.error("Cannot obtain username", ex);
        throw ex;
      }
      if (!MetaStoreUtils.checkUserHasHostProxyPrivileges(user, conf, getIPAddress())) {
        throw new MetaException("User " + user + " is not allowed to perform this API call");
      }
    }

    @Override
    public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
      switch (rqst.getData().getSetField()) {
      case INSERT_DATA:
        String catName = rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
        InsertEvent event =
            new InsertEvent(catName, rqst.getDbName(), rqst.getTableName(), rqst.getPartitionVals(),
                rqst.getData().getInsertData(), rqst.isSuccessful(), this);

        /*
         * The transactional listener response will be set already on the event, so there is not need
         * to pass the response to the non-transactional listener.
         */
        MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.INSERT, event);
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.INSERT, event);

        return new FireEventResponse();

      default:
        throw new TException("Event type " + rqst.getData().getSetField().toString()
            + " not currently supported.");
      }

    }

    @Override
    public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req)
        throws TException {
      GetFileMetadataByExprResult result = new GetFileMetadataByExprResult();
      RawStore ms = getMS();
      if (!ms.isFileMetadataSupported()) {
        result.setIsSupported(false);
        result.setMetadata(EMPTY_MAP_FM2); // Set the required field.
        return result;
      }
      result.setIsSupported(true);

      List<Long> fileIds = req.getFileIds();
      boolean needMetadata = !req.isSetDoGetFooters() || req.isDoGetFooters();
      FileMetadataExprType type = req.isSetType() ? req.getType() : FileMetadataExprType.ORC_SARG;

      ByteBuffer[] metadatas = needMetadata ? new ByteBuffer[fileIds.size()] : null;
      ByteBuffer[] ppdResults = new ByteBuffer[fileIds.size()];
      boolean[] eliminated = new boolean[fileIds.size()];

      getMS().getFileMetadataByExpr(fileIds, type, req.getExpr(), metadatas, ppdResults, eliminated);
      for (int i = 0; i < fileIds.size(); ++i) {
        if (!eliminated[i] && ppdResults[i] == null)
         {
          continue; // No metadata => no ppd.
        }
        MetadataPpdResult mpr = new MetadataPpdResult();
        ByteBuffer ppdResult = eliminated[i] ? null : handleReadOnlyBufferForThrift(ppdResults[i]);
        mpr.setIncludeBitset(ppdResult);
        if (needMetadata) {
          ByteBuffer metadata = eliminated[i] ? null : handleReadOnlyBufferForThrift(metadatas[i]);
          mpr.setMetadata(metadata);
        }
        result.putToMetadata(fileIds.get(i), mpr);
      }
      if (!result.isSetMetadata()) {
        result.setMetadata(EMPTY_MAP_FM2); // Set the required field.
      }
      return result;
    }

    private final static Map<Long, ByteBuffer> EMPTY_MAP_FM1 = new HashMap<>(1);
    private final static Map<Long, MetadataPpdResult> EMPTY_MAP_FM2 = new HashMap<>(1);

    @Override
    public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
      GetFileMetadataResult result = new GetFileMetadataResult();
      RawStore ms = getMS();
      if (!ms.isFileMetadataSupported()) {
        result.setIsSupported(false);
        result.setMetadata(EMPTY_MAP_FM1); // Set the required field.
        return result;
      }
      result.setIsSupported(true);
      List<Long> fileIds = req.getFileIds();
      ByteBuffer[] metadatas = ms.getFileMetadata(fileIds);
      assert metadatas.length == fileIds.size();
      for (int i = 0; i < metadatas.length; ++i) {
        ByteBuffer bb = metadatas[i];
        if (bb == null) {
          continue;
        }
        bb = handleReadOnlyBufferForThrift(bb);
        result.putToMetadata(fileIds.get(i), bb);
      }
      if (!result.isSetMetadata()) {
        result.setMetadata(EMPTY_MAP_FM1); // Set the required field.
      }
      return result;
    }

    private ByteBuffer handleReadOnlyBufferForThrift(ByteBuffer bb) {
      if (!bb.isReadOnly()) {
        return bb;
      }
      // Thrift cannot write read-only buffers... oh well.
      // TODO: actually thrift never writes to the buffer, so we could use reflection to
      //       unset the unnecessary read-only flag if allocation/copy perf becomes a problem.
      ByteBuffer copy = ByteBuffer.allocate(bb.capacity());
      copy.put(bb);
      copy.flip();
      return copy;
    }

    @Override
    public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
      RawStore ms = getMS();
      if (ms.isFileMetadataSupported()) {
        ms.putFileMetadata(req.getFileIds(), req.getMetadata(), req.getType());
      }
      return new PutFileMetadataResult();
    }

    @Override
    public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req)
        throws TException {
      getMS().putFileMetadata(req.getFileIds(), null, null);
      return new ClearFileMetadataResult();
    }

    @Override
    public CacheFileMetadataResult cache_file_metadata(
        CacheFileMetadataRequest req) throws TException {
      RawStore ms = getMS();
      if (!ms.isFileMetadataSupported()) {
        return new CacheFileMetadataResult(false);
      }
      String dbName = req.getDbName(), tblName = req.getTblName(),
          partName = req.isSetPartName() ? req.getPartName() : null;
      boolean isAllPart = req.isSetIsAllParts() && req.isIsAllParts();
      ms.openTransaction();
      boolean success = false;
      try {
        Table tbl = ms.getTable(DEFAULT_CATALOG_NAME, dbName, tblName);
        if (tbl == null) {
          throw new NoSuchObjectException(dbName + "." + tblName + " not found");
        }
        boolean isPartitioned = tbl.isSetPartitionKeys() && tbl.getPartitionKeysSize() > 0;
        String tableInputFormat = tbl.isSetSd() ? tbl.getSd().getInputFormat() : null;
        if (!isPartitioned) {
          if (partName != null || isAllPart) {
            throw new MetaException("Table is not partitioned");
          }
          if (!tbl.isSetSd() || !tbl.getSd().isSetLocation()) {
            throw new MetaException(
                "Table does not have storage location; this operation is not supported on views");
          }
          FileMetadataExprType type = expressionProxy.getMetadataType(tableInputFormat);
          if (type == null) {
            throw new MetaException("The operation is not supported for " + tableInputFormat);
          }
          fileMetadataManager.queueCacheMetadata(tbl.getSd().getLocation(), type);
          success = true;
        } else {
          List<String> partNames;
          if (partName != null) {
            partNames = Lists.newArrayList(partName);
          } else if (isAllPart) {
            partNames = ms.listPartitionNames(DEFAULT_CATALOG_NAME, dbName, tblName, (short)-1);
          } else {
            throw new MetaException("Table is partitioned");
          }
          int batchSize = MetastoreConf.getIntVar(
              conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
          int index = 0;
          int successCount = 0, failCount = 0;
          HashSet<String> failFormats = null;
          while (index < partNames.size()) {
            int currentBatchSize = Math.min(batchSize, partNames.size() - index);
            List<String> nameBatch = partNames.subList(index, index + currentBatchSize);
            index += currentBatchSize;
            List<Partition> parts = ms.getPartitionsByNames(DEFAULT_CATALOG_NAME, dbName, tblName, nameBatch);
            for (Partition part : parts) {
              if (!part.isSetSd() || !part.getSd().isSetLocation()) {
                throw new MetaException("Partition does not have storage location;" +
                    " this operation is not supported on views");
              }
              String inputFormat = part.getSd().isSetInputFormat()
                  ? part.getSd().getInputFormat() : tableInputFormat;
              FileMetadataExprType type = expressionProxy.getMetadataType(inputFormat);
              if (type == null) {
                ++failCount;
                if (failFormats == null) {
                  failFormats = new HashSet<>();
                }
                failFormats.add(inputFormat);
              } else {
                ++successCount;
                fileMetadataManager.queueCacheMetadata(part.getSd().getLocation(), type);
              }
            }
          }
          success = true; // Regardless of the following exception
          if (failCount > 0) {
            String errorMsg = "The operation failed for " + failCount + " partitions and "
                + "succeeded for " + successCount + " partitions; unsupported formats: ";
            boolean isFirst = true;
            for (String s : failFormats) {
              if (!isFirst) {
                errorMsg += ", ";
              }
              isFirst = false;
              errorMsg += s;
            }
            throw new MetaException(errorMsg);
          }
        }
      } finally {
        if (success) {
          if (!ms.commitTransaction()) {
            throw new MetaException("Failed to commit");
          }
        } else {
          ms.rollbackTransaction();
        }
      }
      return new CacheFileMetadataResult(true);
    }

    @VisibleForTesting
    void updateMetrics() throws MetaException {
      if (databaseCount != null) {
        tableCount.set(getMS().getTableCount());
        partCount.set(getMS().getPartitionCount());
        databaseCount.set(getMS().getDatabaseCount());
      }
    }

    @Override
    public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request) throws TException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String db_name = request.getDb_name();
      String tbl_name = request.getTbl_name();
      startTableFunction("get_primary_keys", catName, db_name, tbl_name);
      List<SQLPrimaryKey> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getPrimaryKeys(catName, db_name, tbl_name);
      } catch (Exception e) {
       ex = e;
       throwMetaException(e);
     } finally {
       endFunction("get_primary_keys", ret != null, ex, tbl_name);
     }
     return new PrimaryKeysResponse(ret);
    }

    @Override
    public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request) throws TException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String parent_db_name = request.getParent_db_name();
      String parent_tbl_name = request.getParent_tbl_name();
      String foreign_db_name = request.getForeign_db_name();
      String foreign_tbl_name = request.getForeign_tbl_name();
      startFunction("get_foreign_keys", " : parentdb=" + parent_db_name +
        " parenttbl=" + parent_tbl_name + " foreigndb=" + foreign_db_name +
        " foreigntbl=" + foreign_tbl_name);
      List<SQLForeignKey> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getForeignKeys(catName, parent_db_name, parent_tbl_name,
              foreign_db_name, foreign_tbl_name);
      } catch (Exception e) {
        ex = e;
        throwMetaException(e);
      } finally {
        endFunction("get_foreign_keys", ret != null, ex, foreign_tbl_name);
      }
      return new ForeignKeysResponse(ret);
    }

    private void throwMetaException(Exception e) throws MetaException,
        NoSuchObjectException {
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else {
        throw newMetaException(e);
      }
    }

    @Override
    public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest request)
        throws TException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String db_name = request.getDb_name();
      String tbl_name = request.getTbl_name();
      startTableFunction("get_unique_constraints", catName, db_name, tbl_name);
      List<SQLUniqueConstraint> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getUniqueConstraints(catName, db_name, tbl_name);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_unique_constraints", ret != null, ex, tbl_name);
      }
      return new UniqueConstraintsResponse(ret);
    }

    @Override
    public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request)
        throws TException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String db_name = request.getDb_name();
      String tbl_name = request.getTbl_name();
      startTableFunction("get_not_null_constraints", catName, db_name, tbl_name);
      List<SQLNotNullConstraint> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getNotNullConstraints(catName, db_name, tbl_name);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_not_null_constraints", ret != null, ex, tbl_name);
      }
      return new NotNullConstraintsResponse(ret);
    }

    @Override
    public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest request)
        throws TException {
      String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
      String db_name = request.getDb_name();
      String tbl_name = request.getTbl_name();
      startTableFunction("get_default_constraints", catName, db_name, tbl_name);
      List<SQLDefaultConstraint> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getDefaultConstraints(catName, db_name, tbl_name);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_default_constraints", ret != null, ex, tbl_name);
      }
      return new DefaultConstraintsResponse(ret);
    }

    @Override
    public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request)
        throws TException {
      String catName = request.getCatName();
      String db_name = request.getDb_name();
      String tbl_name = request.getTbl_name();
      startTableFunction("get_check_constraints", catName, db_name, tbl_name);
      List<SQLCheckConstraint> ret = null;
      Exception ex = null;
      try {
        ret = getMS().getCheckConstraints(catName, db_name, tbl_name);
      } catch (Exception e) {
        ex = e;
        if (e instanceof MetaException) {
          throw (MetaException) e;
        } else {
          throw newMetaException(e);
        }
      } finally {
        endFunction("get_check_constraints", ret != null, ex, tbl_name);
      }
      return new CheckConstraintsResponse(ret);
    }

    @Override
    public String get_metastore_db_uuid() throws TException {
      try {
        return getMS().getMetastoreDbUuid();
      } catch (MetaException e) {
        LOG.error("Exception thrown while querying metastore db uuid", e);
        throw e;
      }
    }

    @Override
    public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest request)
        throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
      int defaultPoolSize = MetastoreConf.getIntVar(
          conf, MetastoreConf.ConfVars.WM_DEFAULT_POOL_SIZE);
      WMResourcePlan plan = request.getResourcePlan();
      if (defaultPoolSize > 0 && plan.isSetQueryParallelism()) {
        // If the default pool is not disabled, override the size with the specified parallelism.
        defaultPoolSize = plan.getQueryParallelism();
      }
      try {
        getMS().createResourcePlan(plan, request.getCopyFrom(), defaultPoolSize);
        return new WMCreateResourcePlanResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to persist resource plan", e);
        throw e;
      }
    }

    @Override
    public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest request)
        throws NoSuchObjectException, MetaException, TException {
      try {
        WMFullResourcePlan rp = getMS().getResourcePlan(request.getResourcePlanName());
        WMGetResourcePlanResponse resp = new WMGetResourcePlanResponse();
        resp.setResourcePlan(rp);
        return resp;
      } catch (MetaException e) {
        LOG.error("Exception while trying to retrieve resource plan", e);
        throw e;
      }
    }

    @Override
    public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest request)
        throws MetaException, TException {
      try {
        WMGetAllResourcePlanResponse resp = new WMGetAllResourcePlanResponse();
        resp.setResourcePlans(getMS().getAllResourcePlans());
        return resp;
      } catch (MetaException e) {
        LOG.error("Exception while trying to retrieve resource plans", e);
        throw e;
      }
    }

    @Override
    public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest request)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
      try {
        if (((request.isIsEnableAndActivate() ? 1 : 0) + (request.isIsReplace() ? 1 : 0)
           + (request.isIsForceDeactivate() ? 1 : 0)) > 1) {
          throw new MetaException("Invalid request; multiple flags are set");
        }
        WMAlterResourcePlanResponse response = new WMAlterResourcePlanResponse();
        // This method will only return full resource plan when activating one,
        // to give the caller the result atomically with the activation.
        WMFullResourcePlan fullPlanAfterAlter = getMS().alterResourcePlan(
            request.getResourcePlanName(), request.getResourcePlan(),
            request.isIsEnableAndActivate(), request.isIsForceDeactivate(), request.isIsReplace());
        if (fullPlanAfterAlter != null) {
          response.setFullResourcePlan(fullPlanAfterAlter);
        }
        return response;
      } catch (MetaException e) {
        LOG.error("Exception while trying to alter resource plan", e);
        throw e;
      }
    }

    @Override
    public WMGetActiveResourcePlanResponse get_active_resource_plan(
        WMGetActiveResourcePlanRequest request) throws MetaException, TException {
      try {
        WMGetActiveResourcePlanResponse response = new WMGetActiveResourcePlanResponse();
        response.setResourcePlan(getMS().getActiveResourcePlan());
        return response;
      } catch (MetaException e) {
        LOG.error("Exception while trying to get active resource plan", e);
        throw e;
      }
    }

    @Override
    public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest request)
        throws NoSuchObjectException, MetaException, TException {
      try {
        return getMS().validateResourcePlan(request.getResourcePlanName());
      } catch (MetaException e) {
        LOG.error("Exception while trying to validate resource plan", e);
        throw e;
      }
    }

    @Override
    public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest request)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
      try {
        getMS().dropResourcePlan(request.getResourcePlanName());
        return new WMDropResourcePlanResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to drop resource plan", e);
        throw e;
      }
    }

    @Override
    public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest request)
        throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
      try {
        getMS().createWMTrigger(request.getTrigger());
        return new WMCreateTriggerResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to create trigger", e);
        throw e;
      }
    }

    @Override
    public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest request)
        throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
      try {
        getMS().alterWMTrigger(request.getTrigger());
        return new WMAlterTriggerResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to alter trigger", e);
        throw e;
      }
    }

    @Override
    public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest request)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
      try {
        getMS().dropWMTrigger(request.getResourcePlanName(), request.getTriggerName());
        return new WMDropTriggerResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to drop trigger.", e);
        throw e;
      }
    }

    @Override
    public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
        WMGetTriggersForResourePlanRequest request)
        throws NoSuchObjectException, MetaException, TException {
      try {
        List<WMTrigger> triggers =
            getMS().getTriggersForResourcePlan(request.getResourcePlanName());
        WMGetTriggersForResourePlanResponse response = new WMGetTriggersForResourePlanResponse();
        response.setTriggers(triggers);
        return response;
      } catch (MetaException e) {
        LOG.error("Exception while trying to retrieve triggers plans", e);
        throw e;
      }
    }

    public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest request)
        throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException,
        TException {
      try {
        getMS().alterPool(request.getPool(), request.getPoolPath());
        return new WMAlterPoolResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to alter WMPool", e);
        throw e;
      }
    }

    @Override
    public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest request)
        throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException,
        TException {
      try {
        getMS().createPool(request.getPool());
        return new WMCreatePoolResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to create WMPool", e);
        throw e;
      }
    }

    public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest request)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
      try {
        getMS().dropWMPool(request.getResourcePlanName(), request.getPoolPath());
        return new WMDropPoolResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to drop WMPool", e);
        throw e;
      }
    }

    public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(
        WMCreateOrUpdateMappingRequest request) throws AlreadyExistsException,
        NoSuchObjectException, InvalidObjectException, MetaException, TException {
      try {
        getMS().createOrUpdateWMMapping(request.getMapping(), request.isUpdate());
        return new WMCreateOrUpdateMappingResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to create or update WMMapping", e);
        throw e;
      }
    }

    public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest request)
        throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
      try {
        getMS().dropWMMapping(request.getMapping());
        return new WMDropMappingResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to drop WMMapping", e);
        throw e;
      }
    }

    public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
        WMCreateOrDropTriggerToPoolMappingRequest request) throws AlreadyExistsException,
        NoSuchObjectException, InvalidObjectException, MetaException, TException {
      try {
        if (request.isDrop()) {
          getMS().dropWMTriggerToPoolMapping(
              request.getResourcePlanName(), request.getTriggerName(), request.getPoolPath());
        } else {
          getMS().createWMTriggerToPoolMapping(
              request.getResourcePlanName(), request.getTriggerName(), request.getPoolPath());
        }
        return new WMCreateOrDropTriggerToPoolMappingResponse();
      } catch (MetaException e) {
        LOG.error("Exception while trying to create or drop pool mappings", e);
        throw e;
      }
    }

    public void create_ischema(ISchema schema) throws TException {
      startFunction("create_ischema", ": " + schema.getName());
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        firePreEvent(new PreCreateISchemaEvent(this, schema));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.createISchema(schema);

          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.CREATE_ISCHEMA, new CreateISchemaEvent(true, this, schema));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.CREATE_ISCHEMA,
                new CreateISchemaEvent(success, this, schema), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|AlreadyExistsException e) {
        LOG.error("Caught exception creating schema", e);
        ex = e;
        throw e;
      } finally {
        endFunction("create_ischema", success, ex);
      }
    }

    @Override
    public void alter_ischema(AlterISchemaRequest rqst) throws TException {
      startFunction("alter_ischema", ": " + rqst);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        ISchema oldSchema = ms.getISchema(rqst.getName());
        if (oldSchema == null) {
          throw new NoSuchObjectException("Could not find schema " + rqst.getName());
        }
        firePreEvent(new PreAlterISchemaEvent(this, oldSchema, rqst.getNewSchema()));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.alterISchema(rqst.getName(), rqst.getNewSchema());
          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.ALTER_ISCHEMA, new AlterISchemaEvent(true, this, oldSchema, rqst.getNewSchema()));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_ISCHEMA,
                new AlterISchemaEvent(success, this, oldSchema, rqst.getNewSchema()), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|NoSuchObjectException e) {
        LOG.error("Caught exception altering schema", e);
        ex = e;
        throw e;
      } finally {
        endFunction("alter_ischema", success, ex);
      }
    }

    @Override
    public ISchema get_ischema(ISchemaName schemaName) throws TException {
      startFunction("get_ischema", ": " + schemaName);
      Exception ex = null;
      ISchema schema = null;
      try {
        schema = getMS().getISchema(schemaName);
        if (schema == null) {
          throw new NoSuchObjectException("No schema named " + schemaName + " exists");
        }
        firePreEvent(new PreReadISchemaEvent(this, schema));
        return schema;
      } catch (MetaException e) {
        LOG.error("Caught exception getting schema", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_ischema", schema != null, ex);
      }
    }

    @Override
    public void drop_ischema(ISchemaName schemaName) throws TException {
      startFunction("drop_ischema", ": " + schemaName);
      Exception ex = null;
      boolean success = false;
      RawStore ms = getMS();
      try {
        // look for any valid versions.  This will also throw NoSuchObjectException if the schema
        // itself doesn't exist, which is what we want.
        SchemaVersion latest = ms.getLatestSchemaVersion(schemaName);
        if (latest != null) {
          ex = new InvalidOperationException("Schema " + schemaName + " cannot be dropped, it has" +
              " at least one valid version");
          throw (InvalidObjectException)ex;
        }
        ISchema schema = ms.getISchema(schemaName);
        firePreEvent(new PreDropISchemaEvent(this, schema));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.dropISchema(schemaName);
          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.DROP_ISCHEMA, new DropISchemaEvent(true, this, schema));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.DROP_ISCHEMA,
                new DropISchemaEvent(success, this, schema), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|NoSuchObjectException e) {
        LOG.error("Caught exception dropping schema", e);
        ex = e;
        throw e;
      } finally {
        endFunction("drop_ischema", success, ex);
      }
    }

    @Override
    public void add_schema_version(SchemaVersion schemaVersion) throws TException {
      startFunction("add_schema_version", ": " + schemaVersion);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        // Make sure the referenced schema exists
        if (ms.getISchema(schemaVersion.getSchema()) == null) {
          throw new NoSuchObjectException("No schema named " + schemaVersion.getSchema());
        }
        firePreEvent(new PreAddSchemaVersionEvent(this, schemaVersion));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.addSchemaVersion(schemaVersion);

          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.ADD_SCHEMA_VERSION, new AddSchemaVersionEvent(true, this, schemaVersion));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_SCHEMA_VERSION,
                new AddSchemaVersionEvent(success, this, schemaVersion), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|AlreadyExistsException e) {
        LOG.error("Caught exception adding schema version", e);
        ex = e;
        throw e;
      } finally {
        endFunction("add_schema_version", success, ex);
      }
    }

    @Override
    public SchemaVersion get_schema_version(SchemaVersionDescriptor version) throws TException {
      startFunction("get_schema_version", ": " + version);
      Exception ex = null;
      SchemaVersion schemaVersion = null;
      try {
        schemaVersion = getMS().getSchemaVersion(version);
        if (schemaVersion == null) {
          throw new NoSuchObjectException("No schema version " + version + "exists");
        }
        firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
        return schemaVersion;
      } catch (MetaException e) {
        LOG.error("Caught exception getting schema version", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_schema_version", schemaVersion != null, ex);
      }
    }

    @Override
    public SchemaVersion get_schema_latest_version(ISchemaName schemaName) throws TException {
      startFunction("get_latest_schema_version", ": " + schemaName);
      Exception ex = null;
      SchemaVersion schemaVersion = null;
      try {
        schemaVersion = getMS().getLatestSchemaVersion(schemaName);
        if (schemaVersion == null) {
          throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
        }
        firePreEvent(new PreReadhSchemaVersionEvent(this, Collections.singletonList(schemaVersion)));
        return schemaVersion;
      } catch (MetaException e) {
        LOG.error("Caught exception getting latest schema version", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_latest_schema_version", schemaVersion != null, ex);
      }
    }

    @Override
    public List<SchemaVersion> get_schema_all_versions(ISchemaName schemaName) throws TException {
      startFunction("get_all_schema_versions", ": " + schemaName);
      Exception ex = null;
      List<SchemaVersion> schemaVersions = null;
      try {
        schemaVersions = getMS().getAllSchemaVersion(schemaName);
        if (schemaVersions == null) {
          throw new NoSuchObjectException("No versions of schema " + schemaName + "exist");
        }
        firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
        return schemaVersions;
      } catch (MetaException e) {
        LOG.error("Caught exception getting all schema versions", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_all_schema_versions", schemaVersions != null, ex);
      }
    }

    @Override
    public void drop_schema_version(SchemaVersionDescriptor version) throws TException {
      startFunction("drop_schema_version", ": " + version);
      Exception ex = null;
      boolean success = false;
      RawStore ms = getMS();
      try {
        SchemaVersion schemaVersion = ms.getSchemaVersion(version);
        if (schemaVersion == null) {
          throw new NoSuchObjectException("No schema version " + version);
        }
        firePreEvent(new PreDropSchemaVersionEvent(this, schemaVersion));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.dropSchemaVersion(version);
          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.DROP_SCHEMA_VERSION, new DropSchemaVersionEvent(true, this, schemaVersion));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.DROP_SCHEMA_VERSION,
                new DropSchemaVersionEvent(success, this, schemaVersion), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|NoSuchObjectException e) {
        LOG.error("Caught exception dropping schema version", e);
        ex = e;
        throw e;
      } finally {
        endFunction("drop_schema_version", success, ex);
      }
    }

    @Override
    public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst rqst) throws TException {
      startFunction("get_schemas_by_cols");
      Exception ex = null;
      List<SchemaVersion> schemaVersions = Collections.emptyList();
      try {
        schemaVersions = getMS().getSchemaVersionsByColumns(rqst.getColName(),
            rqst.getColNamespace(), rqst.getType());
        firePreEvent(new PreReadhSchemaVersionEvent(this, schemaVersions));
        final List<SchemaVersionDescriptor> entries = new ArrayList<>(schemaVersions.size());
        schemaVersions.forEach(schemaVersion -> entries.add(
            new SchemaVersionDescriptor(schemaVersion.getSchema(), schemaVersion.getVersion())));
        return new FindSchemasByColsResp(entries);
      } catch (MetaException e) {
        LOG.error("Caught exception doing schema version query", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_schemas_by_cols", !schemaVersions.isEmpty(), ex);
      }
    }

    @Override
    public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest rqst)
        throws TException {
      startFunction("map_schema_version_to_serde, :" + rqst);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
        if (oldSchemaVersion == null) {
          throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
        }
        SerDeInfo serde = ms.getSerDeInfo(rqst.getSerdeName());
        if (serde == null) {
          throw new NoSuchObjectException("No SerDe named " + rqst.getSerdeName());
        }
        SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
        newSchemaVersion.setSerDe(serde);
        firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this,
                        oldSchemaVersion, newSchemaVersion));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_SCHEMA_VERSION,
                new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|NoSuchObjectException e) {
        LOG.error("Caught exception mapping schema version to serde", e);
        ex = e;
        throw e;
      } finally {
        endFunction("map_schema_version_to_serde", success, ex);
      }
    }

    @Override
    public void set_schema_version_state(SetSchemaVersionStateRequest rqst) throws TException {
      startFunction("set_schema_version_state, :" + rqst);
      boolean success = false;
      Exception ex = null;
      RawStore ms = getMS();
      try {
        SchemaVersion oldSchemaVersion = ms.getSchemaVersion(rqst.getSchemaVersion());
        if (oldSchemaVersion == null) {
          throw new NoSuchObjectException("No schema version " + rqst.getSchemaVersion());
        }
        SchemaVersion newSchemaVersion = new SchemaVersion(oldSchemaVersion);
        newSchemaVersion.setState(rqst.getState());
        firePreEvent(new PreAlterSchemaVersionEvent(this, oldSchemaVersion, newSchemaVersion));
        Map<String, String> transactionalListenersResponses = Collections.emptyMap();
        ms.openTransaction();
        try {
          ms.alterSchemaVersion(rqst.getSchemaVersion(), newSchemaVersion);
          if (!transactionalListeners.isEmpty()) {
            transactionalListenersResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.ALTER_SCHEMA_VERSION, new AlterSchemaVersionEvent(true, this,
                        oldSchemaVersion, newSchemaVersion));
          }
          success = ms.commitTransaction();
        } finally {
          if (!success) ms.rollbackTransaction();
          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALTER_SCHEMA_VERSION,
                new AlterSchemaVersionEvent(success, this, oldSchemaVersion, newSchemaVersion), null,
                transactionalListenersResponses, ms);
          }
        }
      } catch (MetaException|NoSuchObjectException e) {
        LOG.error("Caught exception changing schema version state", e);
        ex = e;
        throw e;
      } finally {
        endFunction("set_schema_version_state", success, ex);
      }
    }

    @Override
    public void add_serde(SerDeInfo serde) throws TException {
      startFunction("create_serde", ": " + serde.getName());
      Exception ex = null;
      boolean success = false;
      RawStore ms = getMS();
      try {
        ms.openTransaction();
        ms.addSerde(serde);
        success = ms.commitTransaction();
      } catch (MetaException|AlreadyExistsException e) {
        LOG.error("Caught exception creating serde", e);
        ex = e;
        throw e;
      } finally {
        if (!success) ms.rollbackTransaction();
        endFunction("create_serde", success, ex);
      }
    }

    @Override
    public SerDeInfo get_serde(GetSerdeRequest rqst) throws TException {
      startFunction("get_serde", ": " + rqst);
      Exception ex = null;
      SerDeInfo serde = null;
      try {
        serde = getMS().getSerDeInfo(rqst.getSerdeName());
        if (serde == null) {
          throw new NoSuchObjectException("No serde named " + rqst.getSerdeName() + " exists");
        }
        return serde;
      } catch (MetaException e) {
        LOG.error("Caught exception getting serde", e);
        ex = e;
        throw e;
      } finally {
        endFunction("get_serde", serde != null, ex);
      }
    }
  }

  private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf)
      throws MetaException {
    return newRetryingHMSHandler(baseHandler, conf, false);
  }

  private static IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, Configuration conf,
      boolean local) throws MetaException {
    return RetryingHMSHandler.getProxy(conf, baseHandler, local);
  }

  static Iface newRetryingHMSHandler(String name, Configuration conf, boolean local)
      throws MetaException {
    HMSHandler baseHandler = new HiveMetaStore.HMSHandler(name, conf, false);
    return RetryingHMSHandler.getProxy(conf, baseHandler, local);
  }

  /**
   * Discard a current delegation token.
   *
   * @param tokenStrForm
   *          the token in string form
   */
  public static void cancelDelegationToken(String tokenStrForm
      ) throws IOException {
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
  public static long renewDelegationToken(String tokenStrForm
      ) throws IOException {
    return delegationTokenManager.renewDelegationToken(tokenStrForm);
  }

  /**
   * HiveMetaStore specific CLI
   *
   */
  static public class HiveMetastoreCli extends CommonCliOptions {
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

        this.port = new Integer(args[0]);
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
      HMSHandler.LOG.warn(e.getMessage());
    }
    startupShutdownMessage(HiveMetaStore.class, args, LOG);

    try {
      String msg = "Starting hive metastore on port " + cli.port;
      HMSHandler.LOG.info(msg);
      if (cli.isVerbose()) {
        System.err.println(msg);
      }


      // set all properties specified on the command line
      for (Map.Entry<Object, Object> item : hiveconf.entrySet()) {
        conf.set((String) item.getKey(), (String) item.getValue());
      }

      // Add shutdown hook.
      shutdownHookMgr.addShutdownHook(() -> {
        String shutdownMsg = "Shutting down hive metastore.";
        HMSHandler.LOG.info(shutdownMsg);
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

      Lock startLock = new ReentrantLock();
      Condition startCondition = startLock.newCondition();
      AtomicBoolean startedServing = new AtomicBoolean();
      startMetaStoreThreads(conf, startLock, startCondition, startedServing);
      startMetaStore(cli.getPort(), HadoopThriftAuthBridge.getBridge(), conf, startLock,
          startCondition, startedServing);
    } catch (Throwable t) {
      // Catch the exception, log it and rethrow it.
      HMSHandler.LOG
          .error("Metastore Thrift Server threw an exception...", t);
      throw t;
    }
  }

  private static AtomicInteger openConnections;

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}
   *
   * @param port
   * @param bridge
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge)
      throws Throwable {
    startMetaStore(port, bridge, MetastoreConf.newMetastoreConf(), null, null, null);
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
    startMetaStore(port, bridge, conf, null, null, null);
  }

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}
   *
   * @param port
   * @param bridge
   * @param conf
   *          configuration overrides
   * @throws Throwable
   */
  public static void startMetaStore(int port, HadoopThriftAuthBridge bridge,
      Configuration conf, Lock startLock, Condition startCondition,
      AtomicBoolean startedServing) throws Throwable {
    try {
      isMetaStoreRemote = true;
      // Server will create new threads up to max as necessary. After an idle
      // period, it will destroy threads to keep the number of threads in the
      // pool to min.
      long maxMessageSize = MetastoreConf.getLongVar(conf, ConfVars.SERVER_MAX_MESSAGE_SIZE);
      int minWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MIN_THREADS);
      int maxWorkerThreads = MetastoreConf.getIntVar(conf, ConfVars.SERVER_MAX_THREADS);
      boolean tcpKeepAlive = MetastoreConf.getBoolVar(conf, ConfVars.TCP_KEEP_ALIVE);
      boolean useFramedTransport = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
      boolean useCompactProtocol = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
      boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
      useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);

      if (useSasl) {
        // we are in secure mode. Login using keytab
        String kerberosName = SecurityUtil
            .getServerPrincipal(MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL), "0.0.0.0");
        String keyTabFile = MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE);
        UserGroupInformation.loginUserFromKeytab(kerberosName, keyTabFile);
      }

      TProcessor processor;
      TTransportFactory transFactory;
      final TProtocolFactory protocolFactory;
      final TProtocolFactory inputProtoFactory;
      if (useCompactProtocol) {
        protocolFactory = new TCompactProtocol.Factory();
        inputProtoFactory = new TCompactProtocol.Factory(maxMessageSize, maxMessageSize);
      } else {
        protocolFactory = new TBinaryProtocol.Factory();
        inputProtoFactory = new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize);
      }
      HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf,
          false);
      IHMSHandler handler = newRetryingHMSHandler(baseHandler, conf);

      // Initialize materializations invalidation cache
      MaterializationsInvalidationCache.get().init(conf, handler);
      TServerSocket serverSocket;

      if (useSasl) {
        // we are in secure mode.
        if (useFramedTransport) {
          throw new HiveMetaException("Framed transport is not supported with SASL enabled.");
        }
        saslServer = bridge.createServer(
            MetastoreConf.getVar(conf, ConfVars.KERBEROS_KEYTAB_FILE),
            MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL),
            MetastoreConf.getVar(conf, ConfVars.CLIENT_KERBEROS_PRINCIPAL));
        // Start delegation token manager
        delegationTokenManager = new MetastoreDelegationTokenManager();
        delegationTokenManager.startDelegationTokenSecretManager(conf, baseHandler, HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
        saslServer.setSecretManager(delegationTokenManager.getSecretManager());
        transFactory = saslServer.createTransportFactory(
                MetaStoreUtils.getMetaStoreSaslProperties(conf, useSSL));
        processor = saslServer.wrapProcessor(
          new ThriftHiveMetastore.Processor<>(handler));

        LOG.info("Starting DB backed MetaStore Server in Secure Mode");
      } else {
        // we are in unsecure mode.
        if (MetastoreConf.getBoolVar(conf, ConfVars.EXECUTE_SET_UGI)) {
          transFactory = useFramedTransport ?
              new ChainedTTransportFactory(new TFramedTransport.Factory(),
                  new TUGIContainingTransport.Factory())
              : new TUGIContainingTransport.Factory();

          processor = new TUGIBasedProcessor<>(handler);
          LOG.info("Starting DB backed MetaStore Server with SetUGI enabled");
        } else {
          transFactory = useFramedTransport ?
              new TFramedTransport.Factory() : new TTransportFactory();
          processor = new TSetIpAddressProcessor<>(handler);
          LOG.info("Starting DB backed MetaStore Server");
        }
      }

      if (!useSSL) {
        serverSocket = SecurityUtils.getServerSocket(null, port);
      } else {
        String keyStorePath = MetastoreConf.getVar(conf, ConfVars.SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.SSL_KEYSTORE_PATH.toString()
              + " Not configured for SSL connection");
        }
        String keyStorePassword =
            MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.SSL_KEYSTORE_PASSWORD);

        // enable SSL support for HMS
        List<String> sslVersionBlacklist = new ArrayList<>();
        for (String sslVersion : MetastoreConf.getVar(conf, ConfVars.SSL_PROTOCOL_BLACKLIST).split(",")) {
          sslVersionBlacklist.add(sslVersion);
        }

        serverSocket = SecurityUtils.getServerSSLSocket(null, port, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      if (tcpKeepAlive) {
        serverSocket = new TServerSocketKeepAlive(serverSocket);
      }

      // Metrics will have already been initialized if we're using them since HMSHandler
      // initializes them.
      openConnections = Metrics.getOrCreateGauge(MetricsConstants.OPEN_CONNECTIONS);

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
          openConnections.incrementAndGet();
          return null;
        }

        @Override
        public void deleteContext(ServerContext serverContext, TProtocol tProtocol, TProtocol tProtocol1) {
          openConnections.decrementAndGet();
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

      if (startLock != null) {
        signalOtherThreadsToStart(tServer, startLock, startCondition, startedServing);
      }
      tServer.serve();
    } catch (Throwable x) {
      x.printStackTrace();
      HMSHandler.LOG.error(StringUtils.stringifyException(x));
      throw x;
    }
  }

  private static void cleanupRawStore() {
    try {
      RawStore rs = HMSHandler.getRawStore();
      if (rs != null) {
        HMSHandler.logInfo("Cleaning up thread local RawStore...");
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
      HMSHandler.logInfo("Done cleaning up thread local RawStore");
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
   */
  private static void startMetaStoreThreads(final Configuration conf, final Lock startLock,
                                            final Condition startCondition, final
                                            AtomicBoolean startedServing) {
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
          startCompactorInitiator(conf);
          startCompactorWorkers(conf);
          startCompactorCleaner(conf);
          startRemoteOnlyTasks(conf);
        } catch (Throwable e) {
          LOG.error("Failure when starting the compactor, compactions may not happen, " +
              StringUtils.stringifyException(e));
        } finally {
          startLock.unlock();
        }

        ReplChangeManager.scheduleCMClearer(conf);
      }
    };
    t.setDaemon(true);
    t.setName("Metastore threads starter thread");
    t.start();
  }

  private static void startCompactorInitiator(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread initiator =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Initiator");
      initializeAndStartThread(initiator, conf);
    }
  }

  private static void startCompactorWorkers(Configuration conf) throws Exception {
    int numWorkers = MetastoreConf.getIntVar(conf, ConfVars.COMPACTOR_WORKER_THREADS);
    for (int i = 0; i < numWorkers; i++) {
      MetaStoreThread worker =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Worker");
      initializeAndStartThread(worker, conf);
    }
  }

  private static void startCompactorCleaner(Configuration conf) throws Exception {
    if (MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
      MetaStoreThread cleaner =
          instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Cleaner");
      initializeAndStartThread(cleaner, conf);
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
      MetaException {
    LOG.info("Starting metastore thread of type " + thread.getClass().getName());
    thread.setConf(conf);
    thread.setThreadId(nextThreadId++);
    thread.init(new AtomicBoolean(), new AtomicBoolean());
    thread.start();
  }

  private static void startRemoteOnlyTasks(Configuration conf) throws Exception {
    if(!MetastoreConf.getBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON)) {
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
      ThreadPool.getPool().scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Print a log message for starting up and shutting down
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
  private static String toStartupShutdownString(String prefix, String [] msg) {
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
    try {return "" + InetAddress.getLocalHost();}
    catch(UnknownHostException uhe) {return "" + uhe;}
  }
}
