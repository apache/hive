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

import com.codahale.metrics.Counter;
import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.AcidConstants;
import org.apache.hadoop.hive.common.AcidMetaDataFile;
import org.apache.hadoop.hive.common.AcidMetaDataFile.DataFormat;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.dataconnector.DataConnectorProviderFactory;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.metrics.PerfLogger;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.properties.PropertyException;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyMap;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.apache.hadoop.hive.metastore.txn.*;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_PATH_SUFFIX;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE_PATTERN;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.common.AcidConstants.DELTA_DIGITS;

import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_RESUME_STARTED_AFTER_FAILOVER;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DATABASE_PROPERTY;
import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.RENAME_PARTITION_MAKE_COPY;
import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.TRUNCATE_SKIP_DATA_DELETION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.CTAS_LEGACY_CONFIG;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.rethrowException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.throwMetaException;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTLT;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependNotNullCatToDbName;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * Default handler for all Hive Metastore methods. Implements methods defined in hive_metastore.thrift.
 */
public class HMSHandler extends FacebookBase implements IHMSHandler {
  public static final Logger LOG = LoggerFactory.getLogger(HMSHandler.class);
  private final Configuration conf; // stores datastore (jpox) properties,
                                   // right now they come from jpox.properties

  private static String currentUrl;
  private FileMetadataManager fileMetadataManager;
  private PartitionExpressionProxy expressionProxy;
  private StorageSchemaReader storageSchemaReader;
  private IMetaStoreMetadataTransformer transformer;
  private static DataConnectorProviderFactory dataconnectorFactory = null;

  public static final String PARTITION_NUMBER_EXCEED_LIMIT_MSG =
      "Number of partitions scanned (=%d) on table '%s' exceeds limit (=%d). This is controlled on the metastore server by %s.";

  public static final String ADMIN = "admin";
  public static final String PUBLIC = "public";

  static final String NO_FILTER_STRING = "";
  static final int UNLIMITED_MAX_PARTITIONS = -1;

  static final int LOG_SAMPLE_PARTITIONS_MAX_SIZE = 4;

  static final int LOG_SAMPLE_PARTITIONS_HALF_SIZE = 2;

  static final String LOG_SAMPLE_PARTITIONS_SEPARATOR = ",";

  private Warehouse wh; // hdfs warehouse
  private static Striped<Lock> tablelocks;

  public static RawStore getRawStore() {
    return HMSHandlerContext.getRawStore().orElse(null);
  }

  static void cleanupHandlerContext() {
    AtomicBoolean cleanedRawStore = new AtomicBoolean(false);
    try {
      HMSHandlerContext.getRawStore().ifPresent(rs -> {
        logAndAudit("Cleaning up thread local RawStore...");
        rs.shutdown();
        cleanedRawStore.set(true);
      });
    } finally {
      HMSHandlerContext.getHMSHandler().ifPresent(HMSHandler::notifyMetaListenersOnShutDown);
      if (cleanedRawStore.get()) {
        logAndAudit("Done cleaning up thread local RawStore");
      }
      HMSHandlerContext.clear();
    }
  }

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

  public static String getIPAddress() {
    if (HiveMetaStore.useSasl) {
      if (HiveMetaStore.saslServer != null && HiveMetaStore.saslServer.getRemoteAddress() != null) {
        return HiveMetaStore.saslServer.getRemoteAddress().getHostAddress();
      }
    } else {
      // if kerberos is not enabled
      return getThreadLocalIpAddress();
    }
    return null;
  }

  /**
   * Internal function to notify listeners to revert back to old values of keys
   * that were modified during setMetaConf. This would get called from HiveMetaStore#cleanupHandlerContext
   */
  private void notifyMetaListenersOnShutDown() {
    try {
      Map<String, String> modifiedConf = HMSHandlerContext.getModifiedConfig();
      Configuration conf = HMSHandlerContext.getConfiguration()
          .orElseThrow(() -> new MetaException("Unexpected: modifiedConf is non-null but conf is null"));
      // Notify listeners of the changed value
      for (Map.Entry<String, String> entry : modifiedConf.entrySet()) {
        String key = entry.getKey();
        // curr value becomes old and vice-versa
        String currVal = entry.getValue();
        String oldVal = conf.get(key);
        if (!Objects.equals(oldVal, currVal)) {
          for (List<MetaStoreEventListener> eventListeners :
              new List[] { listeners, transactionalListeners }) {
            MetaStoreListenerNotifier.notifyEvent(eventListeners, EventType.CONFIG_CHANGE,
                new ConfigChangeEvent(this, key, oldVal, currVal));
          }
        }
      }
      logAndAudit("Meta listeners shutdown notification completed.");
    } catch (MetaException e) {
      LOG.error("Failed to notify meta listeners on shutdown: ", e);
    }
  }

  static void setThreadLocalIpAddress(String ipAddress) {
    HMSHandlerContext.setIpAddress(ipAddress);
  }

  // This will return null if the metastore is not being accessed from a metastore Thrift server,
  // or if the TTransport being used to connect is not an instance of TSocket, or if kereberos
  // is used
  static String getThreadLocalIpAddress() {
    return HMSHandlerContext.getIpAddress().orElse(null);
  }

  // Make it possible for tests to check that the right type of PartitionExpressionProxy was
  // instantiated.
  @VisibleForTesting
  PartitionExpressionProxy getExpressionProxy() {
    return expressionProxy;
  }

  public HMSHandler(String name, Configuration conf) {
    super(name);
    this.conf = conf;
    isInTest = MetastoreConf.getBoolVar(this.conf, HIVE_IN_TEST);
    if (threadPool == null) {
      synchronized (HMSHandler.class) {
        if (threadPool == null) {
          int numThreads = MetastoreConf.getIntVar(conf, ConfVars.FS_HANDLER_THREADS_COUNT);
          threadPool = Executors.newFixedThreadPool(numThreads,
              new ThreadFactoryBuilder().setDaemon(true).setNameFormat("HMSHandler #%d").build());
          int numTableLocks = MetastoreConf.getIntVar(conf, ConfVars.METASTORE_NUM_STRIPED_TABLE_LOCKS);
          tablelocks = Striped.lock(numTableLocks);
        }
      }
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

  private AlterHandler alterHandler;
  private List<MetaStorePreEventListener> preListeners;
  private List<MetaStoreEventListener> listeners;
  private List<TransactionalMetaStoreEventListener> transactionalListeners;
  private List<MetaStoreEndFunctionListener> endFunctionListeners;
  private List<MetaStoreInitListener> initListeners;
  private MetaStoreFilterHook filterHook;
  private boolean isServerFilterEnabled = false;

  private Pattern partitionValidationPattern;
  private final boolean isInTest;

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
    Metrics.initialize(conf);
    initListeners = MetaStoreServerUtils.getMetaStoreListeners(
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
        updateMetrics();
      }
    }

    preListeners = MetaStoreServerUtils.getMetaStoreListeners(MetaStorePreEventListener.class,
        conf, MetastoreConf.getVar(conf, ConfVars.PRE_EVENT_LISTENERS));
    preListeners.add(0, new TransactionalValidationListener(conf));
    listeners = MetaStoreServerUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf,
        MetastoreConf.getVar(conf, ConfVars.EVENT_LISTENERS));
    listeners.add(new SessionPropertiesListener(conf));
    transactionalListeners = new ArrayList() {{
        add(new AcidEventListener(conf));
    }};
    transactionalListeners.addAll(MetaStoreServerUtils.getMetaStoreListeners(
            TransactionalMetaStoreEventListener.class, conf,
            MetastoreConf.getVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS)));
    if (Metrics.getRegistry() != null) {
      listeners.add(new HMSMetricsListener(conf));
    }

    boolean canCachedStoreCanUseEvent = false;
    for (MetaStoreEventListener listener : transactionalListeners) {
      if (listener.doesAddEventsToNotificationLogTable()) {
        canCachedStoreCanUseEvent = true;
        break;
      }
    }
    if (conf.getBoolean(ConfVars.METASTORE_CACHE_CAN_USE_EVENT.getVarname(), false) &&
        !canCachedStoreCanUseEvent) {
      throw new MetaException("CahcedStore can not use events for invalidation as there is no " +
          " TransactionalMetaStoreEventListener to add events to notification table");
    }

    endFunctionListeners = MetaStoreServerUtils.getMetaStoreListeners(
        MetaStoreEndFunctionListener.class, conf, MetastoreConf.getVar(conf, ConfVars.END_FUNCTION_LISTENERS));

    String partitionValidationRegex =
        MetastoreConf.getVar(conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
    if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
      partitionValidationPattern = Pattern.compile(partitionValidationRegex);
    }

    expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    fileMetadataManager = new FileMetadataManager(this.getMS(), conf);

    isServerFilterEnabled = getIfServerFilterenabled();
    filterHook = isServerFilterEnabled ? loadFilterHooks() : null;

    String className = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS);
    if (className != null && !className.trim().isEmpty()) {
      try {
        transformer = JavaUtils.newInstance(JavaUtils.getClass(className.trim(), IMetaStoreMetadataTransformer.class),
            new Class[] {IHMSHandler.class}, new Object[] {this});
      } catch (Exception e) {
        LOG.error("Unable to create instance of class " + className, e);
        throw new IllegalArgumentException(e);
      }
    }
    dataconnectorFactory = DataConnectorProviderFactory.getInstance(this);
  }

  /**
   *
   * Filter is actually enabled only when the configured filter hook is configured, not default, and
   * enabled in configuration
   * @return
   */
  private boolean getIfServerFilterenabled() throws MetaException{
    boolean isEnabled = MetastoreConf.getBoolVar(conf, ConfVars.METASTORE_SERVER_FILTER_ENABLED);

    if (!isEnabled) {
      LOG.info("HMS server filtering is disabled by configuration");
      return false;
    }

    String filterHookClassName = MetastoreConf.getVar(conf, ConfVars.FILTER_HOOK);

    if (isBlank(filterHookClassName)) {
      throw new MetaException("HMS server filtering is enabled but no filter hook is configured");
    }

    if (filterHookClassName.trim().equalsIgnoreCase(DefaultMetaStoreFilterHookImpl.class.getName())) {
      throw new MetaException("HMS server filtering is enabled but the filter hook is DefaultMetaStoreFilterHookImpl, which does no filtering");
    }

    LOG.info("HMS server filtering is enabled. The filter class is " + filterHookClassName);
    return true;
  }

  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException  {
    String errorMsg = "Unable to load filter hook at HMS server. ";

    String filterHookClassName = MetastoreConf.getVar(conf, ConfVars.FILTER_HOOK);
    Preconditions.checkState(!isBlank(filterHookClassName));

    try {
      return (MetaStoreFilterHook)Class.forName(
          filterHookClassName.trim(), true, JavaUtils.getClassLoader()).getConstructor(
          Configuration.class).newInstance(conf);
    } catch (Exception e) {
      LOG.error(errorMsg, e);
      throw new IllegalStateException(errorMsg + e.getMessage(), e);
    }
  }

  /**
   * Check if user can access the table associated with the partition. If not, then throw exception
   * so user cannot access partitions associated with this table
   * We are not calling Pre event listener for authorization because it requires getting the
   * table object from DB, more overhead. Instead ,we call filter hook to filter out table if user
   * has no access. Filter hook only requires table name, not table object. That saves DB access for
   * table object, and still achieve the same purpose: checking if user can access the specified
   * table
   *
   * @param catName catalog name of the table
   * @param dbName database name of the table
   * @param tblName table name
   * @throws NoSuchObjectException
   * @throws MetaException
   */
  private void authorizeTableForPartitionMetadata(
      final String catName, final String dbName, final String tblName)
      throws NoSuchObjectException, MetaException {

    FilterUtils.checkDbAndTableFilters(
        isServerFilterEnabled, filterHook, catName, dbName, tblName);
  }

  @Override
  public void setConf(Configuration conf) {
    HMSHandlerContext.setConfiguration(conf);
    // reload if DS related configuration is changed
    HMSHandlerContext.getRawStore().ifPresent(ms -> ms.setConf(conf));
  }

  @Override
  public Configuration getConf() {
    Configuration conf = HMSHandlerContext.getConfiguration()
        .orElseGet(() -> {
          Configuration configuration = new Configuration(this.conf);
          HMSHandlerContext.setConfiguration(configuration);
          return configuration;
        });
    return conf;
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
    Map<String, String> modifiedConf = HMSHandlerContext.getModifiedConfig();
    if (!modifiedConf.containsKey(key)) {
      modifiedConf.put(key, oldValue);
    }
    // Set invoking HMSHandler on threadLocal, this will be used later to notify
    // metaListeners in HiveMetaStore#cleanupHandlerContext
    if (!HMSHandlerContext.getHMSHandler().isPresent()) {
      HMSHandlerContext.setHMSHandler(this);
    }
    configuration.set(key, value);
    for (List<MetaStoreEventListener> eventListeners :
        new List[] { listeners, transactionalListeners }) {
      MetaStoreListenerNotifier.notifyEvent(eventListeners, EventType.CONFIG_CHANGE,
          new ConfigChangeEvent(this, key, oldValue, value));
    }
    if (ConfVars.TRY_DIRECT_SQL == confVar) {
      HMSHandler.LOG.info("Direct SQL optimization = {}",  value);
    }
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

  public static synchronized RawStore getMSForConf(Configuration conf) throws MetaException {
    RawStore ms = getRawStore();
    if (ms == null) {
      ms = newRawStoreForConf(conf);
      try {
        ms.verifySchema();
      } catch (MetaException e) {
        ms.shutdown();
        throw e;
      }
      HMSHandlerContext.setRawStore(ms);
      LOG.info("Created RawStore: {}", ms);
    }
    return ms;
  }

  @Override
  public TxnStore getTxnHandler() {
    return HMSHandlerContext.getTxnStore(conf);
  }

  static RawStore newRawStoreForConf(Configuration conf) throws MetaException {
    Configuration newConf = new Configuration(conf);
    String rawStoreClassName = MetastoreConf.getVar(newConf, ConfVars.RAW_STORE_IMPL);
    LOG.info("Opening raw store with implementation class: {}", rawStoreClassName);
    return RawStoreProxy.getProxy(newConf, conf, rawStoreClassName);
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
      long time = System.currentTimeMillis() / 1000;
      cat.setCreateTime((int) time);
      cat.setDescription(Warehouse.DEFAULT_CATALOG_COMMENT);
      ms.createCatalog(cat);
    }
  }

  private void createDefaultDB_core(RawStore ms) throws MetaException, InvalidObjectException {
    try {
      ms.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
    } catch (NoSuchObjectException e) {
      LOG.info("Started creating a default database with name: "+DEFAULT_DATABASE_NAME);
      Database db = new Database(DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_COMMENT,
          wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME, true).toString(), null);
      db.setOwnerName(PUBLIC);
      db.setOwnerType(PrincipalType.ROLE);
      db.setCatalogName(DEFAULT_CATALOG_NAME);
      long time = System.currentTimeMillis() / 1000;
      db.setCreateTime((int) time);
      db.setType(DatabaseType.NATIVE);
      ms.createDatabase(db);
      LOG.info("Successfully created a default database with name: "+DEFAULT_DATABASE_NAME);
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
        RawStore ms = getMS();
        createDefaultCatalog(ms, wh);
        createDefaultDB_core(ms);
      } catch (InvalidObjectException | InvalidOperationException e1) {
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
        PrincipalType.ROLE, true), "SQL"));
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

  private static void logAndAudit(final String m) {
    LOG.debug(m);
    logAuditEvent(m);
  }

  private String startFunction(String function, String extraLogInfo) {
    incrementCounter(function);
    logAndAudit((getThreadLocalIpAddress() == null ? "" : "source:" + getThreadLocalIpAddress() + " ") +
        function + extraLogInfo);
    com.codahale.metrics.Timer timer =
        Metrics.getOrCreateTimer(MetricsConstants.API_PREFIX + function);
    if (timer != null) {
      // Timer will be null we aren't using the metrics
      HMSHandlerContext.getTimerContexts().put(function, timer.time());
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
        TableName.getQualified(catName, db, tbl));
  }

  private void startMultiTableFunction(String function, String db, List<String> tbls) {
    String tableNames = join(tbls, ",");
    startFunction(function, " : db=" + db + " tbls=" + tableNames);
  }

  private void startPartitionFunction(String function, String cat, String db, String tbl,
                                      List<String> partVals) {
    startFunction(function, " : tbl=" +
        TableName.getQualified(cat, db, tbl) + samplePartitionValues(partVals));
  }

  private void startPartitionFunction(String function, String catName, String db, String tbl,
                                      Map<String, String> partName) {
    startFunction(function, " : tbl=" +
        TableName.getQualified(catName, db, tbl) + " partition=" + partName);
  }

  private void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Max partitions =" + maxParts);
  }

  private void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
                                      List<String> partVals) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Max partitions =" + maxParts
            + samplePartitionValues(partVals));
  }

  private void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
                                      String filter) {
    startFunction(function,
            " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Filter=" + filter + ": Max partitions ="
                    + maxParts);
  }

  private void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
                                      String expression, String defaultPartitionName) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Expression=" + expression
            + ": Default partition name=" + defaultPartitionName + ": Max partitions=" + maxParts);
  }

  private String getGroupsCountAndUsername(final String user_name, final List<String> group_names) {
    return ". Number of groups= " + (group_names == null ? 0 : group_names.size()) + ", user name= " + user_name;
  }

  private String samplePartitionValues(List<String> partVals) {
    if (CollectionUtils.isEmpty(partVals)) {
      return ": Partitions = []";
    }
    StringBuilder sb = new StringBuilder(": Number of Partitions = " + partVals.size());
    sb.append(": Partitions = [");
    if (partVals.size() > LOG_SAMPLE_PARTITIONS_MAX_SIZE) {
      // extracting starting 2 values, and ending 2 values
      sb.append(join(partVals.subList(0, LOG_SAMPLE_PARTITIONS_HALF_SIZE), LOG_SAMPLE_PARTITIONS_SEPARATOR));
      sb.append(" .... ");
      sb.append(join(partVals.subList(partVals.size() - LOG_SAMPLE_PARTITIONS_HALF_SIZE, partVals.size()),
              LOG_SAMPLE_PARTITIONS_SEPARATOR));
    } else {
      sb.append(join(partVals, LOG_SAMPLE_PARTITIONS_SEPARATOR));
    }
    sb.append("]");
    return sb.toString();
  }

  private void endFunction(String function, boolean successful, Exception e) {
    endFunction(function, successful, e, null);
  }
  private void endFunction(String function, boolean successful, Exception e,
                           String inputTableName) {
    endFunction(function, new MetaStoreEndFunctionContext(successful, e, inputTableName));
  }

  private void endFunction(String function, MetaStoreEndFunctionContext context) {
    com.codahale.metrics.Timer.Context timerContext = HMSHandlerContext.getTimerContexts().remove(function);
    if (timerContext != null) {
      long timeTaken = timerContext.stop();
      LOG.debug((getThreadLocalIpAddress() == null ? "" : "source:" + getThreadLocalIpAddress() + " ") +
          function + "time taken(ns): " + timeTaken);
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
    cleanupHandlerContext();
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
        // set the create time of catalog
        long time = System.currentTimeMillis() / 1000;
        catalog.setCreateTime((int) time);
        ms.openTransaction();
        ms.createCatalog(catalog);

        // Create a default database inside the catalog
        Database db = new Database(DEFAULT_DATABASE_NAME,
            "Default database for catalog " + catalog.getName(), catalog.getLocationUri(),
            Collections.emptyMap());
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
            wh.deleteDir(catPath, true, false, false);
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
  public void alter_catalog(AlterCatalogRequest rqst) throws TException {
    startFunction("alter_catalog " + rqst.getName());
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    GetCatalogResponse oldCat = null;

    try {
      oldCat = get_catalog(new GetCatalogRequest(rqst.getName()));
      // Above should have thrown NoSuchObjectException if there is no such catalog
      assert oldCat != null && oldCat.getCatalog() != null;
      firePreEvent(new PreAlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), this));

      ms.openTransaction();
      ms.alterCatalog(rqst.getName(), rqst.getNewCat());

      if (!transactionalListeners.isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.ALTER_CATALOG,
                new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), true, this));
      }

      success = ms.commitTransaction();
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if ((null != oldCat) && (!listeners.isEmpty())) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.ALTER_CATALOG,
            new AlterCatalogEvent(oldCat.getCatalog(), rqst.getNewCat(), success, this),
            null, transactionalListenersResponses, ms);
      }
      endFunction("alter_catalog", success, ex);
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
      endFunction("get_catalog", cat != null, ex);
    }
  }

  @Override
  public GetCatalogsResponse get_catalogs() throws MetaException {
    startFunction("get_catalogs");

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getCatalogs();
    } catch (Exception e) {
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(NoSuchObjectException.class, InvalidOperationException.class, MetaException.class)
          .defaultMetaException();
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
            DropDatabaseRequest req = new DropDatabaseRequest();
            req.setName(DEFAULT_DATABASE_NAME);
            req.setCatalogName(catName);
            req.setDeleteData(true);
            req.setCascade(false);
            
            drop_database_core(ms, req);
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
        wh.deleteDir(wh.getDnsPath(new Path(cat.getLocationUri())), false, false, false);
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

  static boolean isDbReplicationTarget(Database db) {
    String dbCkptStatus = (db.getParameters() == null) ? null : db.getParameters().get(ReplConst.REPL_TARGET_DB_PROPERTY);
    return dbCkptStatus != null && !dbCkptStatus.trim().isEmpty();
  }

  // Assumes that the catalog has already been set.
  private void create_database_core(RawStore ms, final Database db)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    if (!MetaStoreUtils.validateName(db.getName(), conf)) {
      throw new InvalidObjectException(db.getName() + " is not a valid database name");
    }

    Catalog cat = null;
    try {
      cat = getMS().getCatalog(db.getCatalogName());
    } catch (NoSuchObjectException e) {
      LOG.error("No such catalog " + db.getCatalogName());
      throw new InvalidObjectException("No such catalog " + db.getCatalogName());
    }
    boolean skipAuthorization = false;
    String passedInURI = db.getLocationUri();
    String passedInManagedURI = db.getManagedLocationUri();
    if (passedInURI == null && passedInManagedURI == null) {
      skipAuthorization = true;
    }
    final Path defaultDbExtPath = wh.getDefaultDatabasePath(db.getName(), true);
    final Path defaultDbMgdPath = wh.getDefaultDatabasePath(db.getName(), false);
    final Path dbExtPath = (passedInURI != null) ? wh.getDnsPath(new Path(passedInURI)) : wh.determineDatabasePath(cat, db);
    final Path dbMgdPath = (passedInManagedURI != null) ? wh.getDnsPath(new Path(passedInManagedURI)) : null;

    if ((defaultDbExtPath.equals(dbExtPath) && defaultDbMgdPath.equals(dbMgdPath)) &&
        ((dbMgdPath == null) || dbMgdPath.equals(defaultDbMgdPath))) {
      skipAuthorization = true;
    }

    if ( skipAuthorization ) {
      //null out to skip authorizer URI check
      db.setLocationUri(null);
      db.setManagedLocationUri(null);
    }else{
      db.setLocationUri(dbExtPath.toString());
      if (dbMgdPath != null) {
        db.setManagedLocationUri(dbMgdPath.toString());
      }
    }
    if (db.getOwnerName() == null){
      try {
        db.setOwnerName(SecurityUtils.getUGI().getShortUserName());
      }catch (Exception e){
        LOG.warn("Failed to get owner name for create database operation.", e);
      }
    }
    long time = System.currentTimeMillis()/1000;
    db.setCreateTime((int) time);
    boolean success = false;
    boolean madeManagedDir = false;
    boolean madeExternalDir = false;
    boolean isReplicated = isDbReplicationTarget(db);
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    try {
      firePreEvent(new PreCreateDatabaseEvent(db, this));
      //reinstate location uri for metastore db.
      if (skipAuthorization == true){
        db.setLocationUri(dbExtPath.toString());
        if (dbMgdPath != null) {
          db.setManagedLocationUri(dbMgdPath.toString());
        }
      }
      if (db.getCatalogName() != null && !db.getCatalogName().
          equals(Warehouse.DEFAULT_CATALOG_NAME)) {
        if (!wh.isDir(dbExtPath)) {
          LOG.debug("Creating database path " + dbExtPath);
          if (!wh.mkdirs(dbExtPath)) {
            throw new MetaException("Unable to create database path " + dbExtPath +
                ", failed to create database " + db.getName());
          }
          madeExternalDir = true;
        }
      } else {
        if (dbMgdPath != null) {
          try {
            // Since this may be done as random user (if doAs=true) he may not have access
            // to the managed directory. We run this as an admin user
            madeManagedDir = UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Boolean>() {
              @Override public Boolean run() throws MetaException {
                if (!wh.isDir(dbMgdPath)) {
                  LOG.info("Creating database path in managed directory " + dbMgdPath);
                  if (!wh.mkdirs(dbMgdPath)) {
                    throw new MetaException("Unable to create database managed path " + dbMgdPath + ", failed to create database " + db.getName());
                  }
                  return true;
                }
                return false;
              }
            });
            if (madeManagedDir) {
              LOG.info("Created database path in managed directory " + dbMgdPath);
            } else if (!isInTest || !isDbReplicationTarget(db)) { // Hive replication tests doesn't drop the db after each test
              throw new MetaException(
                  "Unable to create database managed directory " + dbMgdPath + ", failed to create database " + db.getName());
            }
          } catch (IOException | InterruptedException e) {
            throw new MetaException(
                "Unable to create database managed directory " + dbMgdPath + ", failed to create database " + db.getName() + ":" + e.getMessage());
          }
        }
        if (dbExtPath != null) {
          try {
            madeExternalDir = UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Boolean>() {
              @Override public Boolean run() throws MetaException {
                if (!wh.isDir(dbExtPath)) {
                  LOG.info("Creating database path in external directory " + dbExtPath);
                  return wh.mkdirs(dbExtPath);
                }
                return false;
              }
            });
            if (madeExternalDir) {
              LOG.info("Created database path in external directory " + dbExtPath);
            } else {
              LOG.warn("Failed to create external path " + dbExtPath + " for database " + db.getName() + ". This may result in access not being allowed if the "
                  + "StorageBasedAuthorizationProvider is enabled");
            }
          } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
            throw new MetaException("Failed to create external path " + dbExtPath + " for database " + db.getName() + ". This may result in access not being allowed if the "
                + "StorageBasedAuthorizationProvider is enabled: " + e.getMessage());
          }
        } else {
          LOG.info("Database external path won't be created since the external warehouse directory is not defined");
        }
      }

      ms.openTransaction();
      ms.createDatabase(db);

      if (!transactionalListeners.isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.CREATE_DATABASE,
                new CreateDatabaseEvent(db, true, this, isReplicated));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();

        if (db.getCatalogName() != null && !db.getCatalogName().
            equals(Warehouse.DEFAULT_CATALOG_NAME)) {
          if (madeManagedDir && dbMgdPath != null) {
            wh.deleteDir(dbMgdPath, true, db);
          }
        } else {
          if (madeManagedDir && dbMgdPath != null) {
            try {
              UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {
                @Override public Void run() throws Exception {
                  wh.deleteDir(dbMgdPath, true, db);
                  return null;
                }
              });
            } catch (IOException | InterruptedException e) {
              LOG.error(
                  "Couldn't delete managed directory " + dbMgdPath + " after " + "it was created for database " + db.getName() + " " + e.getMessage());
            }
          }

          if (madeExternalDir && dbExtPath != null) {
            try {
              UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Void>() {
                @Override public Void run() throws Exception {
                  wh.deleteDir(dbExtPath, true, db);
                  return null;
                }
              });
            } catch (IOException | InterruptedException e) {
              LOG.error("Couldn't delete external directory " + dbExtPath + " after " + "it was created for database "
                  + db.getName() + " " + e.getMessage());
            }
          }
        }
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.CREATE_DATABASE,
            new CreateDatabaseEvent(db, success, this, isReplicated),
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
    if (!db.isSetCatalogName()) {
      db.setCatalogName(getDefaultCatalog(conf));
    }
    try {
      try {
        if (null != get_database_core(db.getCatalogName(), db.getName())) {
          throw new AlreadyExistsException("Database " + db.getName() + " already exists");
        }
      } catch (NoSuchObjectException e) {
        // expected
      }

      create_database_core(getMS(), db);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_database", success, ex);
    }
  }

  @Override
  public Database get_database(final String name)
      throws NoSuchObjectException, MetaException {
    GetDatabaseRequest request = new GetDatabaseRequest();
    String[] parsedDbName = parseDbName(name, conf);
    request.setName(parsedDbName[DB_NAME]);
    if (parsedDbName[CAT_NAME] != null) {
      request.setCatalogName(parsedDbName[CAT_NAME]);
    }
    return get_database_req(request);
  }

  @Override
  public Database get_database_core(String catName, final String name) throws NoSuchObjectException, MetaException {
    Database db = null;
    if (name == null) {
      throw new MetaException("Database name cannot be null.");
    }
    try {
      db = getMS().getDatabase(catName, name);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    }
    return db;
  }

  @Override
  public Database get_database_req(GetDatabaseRequest request) throws NoSuchObjectException, MetaException {
    startFunction("get_database", ": " + request.getName());
    Database db = null;
    Exception ex = null;
    if (request.getName() == null) {
      throw new MetaException("Database name cannot be null.");
    }
    List<String> processorCapabilities = request.getProcessorCapabilities();
    String processorId = request.getProcessorIdentifier();
    try {
      db = getMS().getDatabase(request.getCatalogName(), request.getName());
      firePreEvent(new PreReadDatabaseEvent(db, this));
      if (transformer != null) {
        db = transformer.transformDatabase(db, processorCapabilities, processorId);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    } finally {
      endFunction("get_database", db != null, ex);
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

    // We can replicate into an empty database, in which case newDB will have indication that
    // it's target of replication but not oldDB. But replication flow will never alter a
    // database so that oldDB indicates that it's target or replication but not the newDB. So,
    // relying solely on newDB to check whether the database is target of replication works.
    boolean isReplicated = isDbReplicationTarget(newDB);
    try {
      oldDB = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      if (oldDB == null) {
        throw new MetaException("Could not alter database \"" + parsedDbName[DB_NAME] +
            "\". Could not retrieve old definition.");
      }

      // Add replication target event id.
      if (isReplicationEventIdUpdate(oldDB, newDB)) {
        Map<String, String> oldParams = new LinkedHashMap<>(newDB.getParameters());
        String currentNotificationLogID = Long.toString(ms.getCurrentNotificationEventId().getEventId());
        oldParams.put(REPL_TARGET_DATABASE_PROPERTY, currentNotificationLogID);
        LOG.debug("Adding the {} property for database {} with event id {}", REPL_TARGET_DATABASE_PROPERTY,
            newDB.getName(), currentNotificationLogID);
        newDB.setParameters(oldParams);
      }
      firePreEvent(new PreAlterDatabaseEvent(oldDB, newDB, this));

      ms.openTransaction();
      ms.alterDatabase(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], newDB);

      if (!transactionalListeners.isEmpty()) {
        AlterDatabaseEvent event = new AlterDatabaseEvent(oldDB, newDB, true, this, isReplicated);
        if (!event.shouldSkipCapturing()) {
          transactionalListenersResponses =
                  MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ALTER_DATABASE, event);
        }
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
        AlterDatabaseEvent event = new AlterDatabaseEvent(oldDB, newDB, success, this, isReplicated);
        if (!event.shouldSkipCapturing()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                  EventType.ALTER_DATABASE, event, null, transactionalListenersResponses, ms);
        }
      }
      endFunction("alter_database", success, ex);
    }
  }

  /**
   * Checks whether the repl.last.id is being updated.
   * @param oldDb the old db object
   * @param newDb the new db object
   * @return true if repl.last.id is being changed.
   */
  private boolean isReplicationEventIdUpdate(Database oldDb, Database newDb) {
    Map<String, String> oldDbProp = oldDb.getParameters();
    Map<String, String> newDbProp = newDb.getParameters();
    if (newDbProp == null || newDbProp.isEmpty() ||
      Boolean.parseBoolean(newDbProp.get(REPL_RESUME_STARTED_AFTER_FAILOVER))) {
      return false;
    }
    String newReplId = newDbProp.get(ReplConst.REPL_TARGET_TABLE_PROPERTY);
    String oldReplId = oldDbProp != null ? oldDbProp.get(ReplConst.REPL_TARGET_TABLE_PROPERTY) : null;
    return newReplId != null && !newReplId.equalsIgnoreCase(oldReplId);
  }

  private void drop_database_core(RawStore ms, DropDatabaseRequest req) throws NoSuchObjectException, 
      InvalidOperationException, MetaException, IOException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    Database db = null;
    List<Path> tablePaths = new ArrayList<>();
    List<Path> partitionPaths = new ArrayList<>();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    if (req.getName() == null) {
      throw new MetaException("Database name cannot be null.");
    }
    boolean isReplicated = false;
    try {
      ms.openTransaction();
      db = ms.getDatabase(req.getCatalogName(), req.getName());
      if (db.getType() == DatabaseType.REMOTE) {
        success = drop_remote_database_core(ms, db);
        return;
      }
      isReplicated = isDbReplicationTarget(db);

      if (!isInTest && ReplChangeManager.isSourceOfReplication(db)) {
        throw new InvalidOperationException("can not drop a database which is a source of replication");
      }

      firePreEvent(new PreDropDatabaseEvent(db, this));
      String catPrependedName = MetaStoreUtils.prependCatalogToDbName(req.getCatalogName(), req.getName(), conf);

      Set<String> uniqueTableNames = new HashSet<>(get_all_tables(catPrependedName));
      List<String> allFunctions = get_functions(catPrependedName, "*");
      ListStoredProcedureRequest request = new ListStoredProcedureRequest(req.getCatalogName());
      request.setDbName(req.getName());
      List<String> allProcedures = get_all_stored_procedures(request);
      ListPackageRequest pkgRequest = new ListPackageRequest(req.getCatalogName());
      pkgRequest.setDbName(req.getName());
      List<String> allPackages = get_all_packages(pkgRequest);

      if (!req.isCascade()) {
        if (!uniqueTableNames.isEmpty()) {
          throw new InvalidOperationException(
              "Database " + db.getName() + " is not empty. One or more tables exist.");
        }
        if (!allFunctions.isEmpty()) {
          throw new InvalidOperationException(
              "Database " + db.getName() + " is not empty. One or more functions exist.");
        }
        if (!allProcedures.isEmpty()) {
          throw new InvalidOperationException(
              "Database " + db.getName() + " is not empty. One or more stored procedures exist.");
        }
        if (!allPackages.isEmpty()) {
          throw new InvalidOperationException(
                  "Database " + db.getName() + " is not empty. One or more packages exist.");
        }
      }
      Path path = new Path(db.getLocationUri()).getParent();
      if (!wh.isWritable(path)) {
        throw new MetaException("Database not dropped since its external warehouse location " + path + " is not writable by " +
            SecurityUtils.getUser());
      }
      path = wh.getDatabaseManagedPath(db).getParent();
      if (!wh.isWritable(path)) {
        throw new MetaException("Database not dropped since its managed warehouse location " + path + " is not writable by " +
            SecurityUtils.getUser());
      }

      Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));

      // drop any functions before dropping db
      for (String funcName : allFunctions) {
        drop_function(catPrependedName, funcName);
      }

      for (String procName : allProcedures) {
        drop_stored_procedure(new StoredProcedureRequest(req.getCatalogName(), req.getName(), procName));
      }
      for (String pkgName : allPackages) {
        drop_package(new DropPackageRequest(req.getCatalogName(), req.getName(), pkgName));
      }

      final int tableBatchSize = MetastoreConf.getIntVar(conf,
          ConfVars.BATCH_RETRIEVE_MAX);

      // First pass will drop the materialized views
      List<String> materializedViewNames = getTablesByTypeCore(req.getCatalogName(), req.getName(), ".*",
          TableType.MATERIALIZED_VIEW.toString());
      int startIndex = 0;
      // retrieve the tables from the metastore in batches to alleviate memory constraints
      while (startIndex < materializedViewNames.size()) {
        int endIndex = Math.min(startIndex + tableBatchSize, materializedViewNames.size());

        List<Table> materializedViews;
        try {
          materializedViews = ms.getTableObjectsByName(req.getCatalogName(), req.getName(), materializedViewNames.subList(startIndex, endIndex));
        } catch (UnknownDBException e) {
          throw new MetaException(e.getMessage());
        }

        if (materializedViews != null && !materializedViews.isEmpty()) {
          for (Table materializedView : materializedViews) {
            boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(materializedView, req.isSoftDelete());
            
            if (materializedView.getSd().getLocation() != null && !isSoftDelete) {
              Path materializedViewPath = wh.getDnsPath(new Path(materializedView.getSd().getLocation()));
              
              if (!FileUtils.isSubdirectory(databasePath.toString(), materializedViewPath.toString()) || req.isSoftDelete()) {
                if (!wh.isWritable(materializedViewPath.getParent())) {
                  throw new MetaException("Database metadata not deleted since table: " +
                      materializedView.getTableName() + " has a parent location " + materializedViewPath.getParent() +
                      " which is not writable by " + SecurityUtils.getUser());
                }
                tablePaths.add(materializedViewPath);
              }
            }
            EnvironmentContext context = null;
            if (isSoftDelete) {
              context = new EnvironmentContext();
              context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
            }
            // Drop the materialized view but not its data
            drop_table_with_environment_context(
                req.getName(), materializedView.getTableName(), false, context);
            
            // Remove from all tables
            uniqueTableNames.remove(materializedView.getTableName());
          }
        }
        startIndex = endIndex;
      }

      // drop tables before dropping db
      List<String> allTables = new ArrayList<>(uniqueTableNames);
      startIndex = 0;
      
      // retrieve the tables from the metastore in batches to alleviate memory constraints
      while (startIndex < allTables.size()) {
        int endIndex = Math.min(startIndex + tableBatchSize, allTables.size());

        List<Table> tables;
        try {
          tables = ms.getTableObjectsByName(req.getCatalogName(), req.getName(), allTables.subList(startIndex, endIndex));
        } catch (UnknownDBException e) {
          throw new MetaException(e.getMessage());
        }

        if (tables != null && !tables.isEmpty()) {
          for (Table table : tables) {
            // If the table is not external and it might not be in a subdirectory of the database
            // add it's locations to the list of paths to delete
            boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(table, req.isSoftDelete());
            Path tablePath = null;
            
            boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table, req.isDeleteData()) 
              && !isSoftDelete;
            
            boolean isManagedTable = table.getTableType().equals(TableType.MANAGED_TABLE.toString());
            if (table.getSd().getLocation() != null && tableDataShouldBeDeleted) {
              tablePath = wh.getDnsPath(new Path(table.getSd().getLocation()));
              if (!isManagedTable || req.isSoftDelete()) {
                if (!wh.isWritable(tablePath.getParent())) {
                  throw new MetaException(
                      "Database metadata not deleted since table: " + table.getTableName() + " has a parent location "
                          + tablePath.getParent() + " which is not writable by " + SecurityUtils.getUser());
                }
                tablePaths.add(tablePath);
              }
            }
            // For each partition in each table, drop the partitions and get a list of
            // partitions' locations which might need to be deleted
            partitionPaths = dropPartitionsAndGetLocations(ms, req.getCatalogName(), req.getName(), table.getTableName(),
                tablePath, tableDataShouldBeDeleted);
            
            EnvironmentContext context = null;
            if (isSoftDelete) {
              context = new EnvironmentContext();
              context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
              req.setDeleteManagedDir(false);
            }
            // Drop the table but not its data
            drop_table_with_environment_context(
                MetaStoreUtils.prependCatalogToDbName(table.getCatName(), table.getDbName(), conf),
                table.getTableName(), false, context, false);
          }
        }
        startIndex = endIndex;
      }

      if (ms.dropDatabase(req.getCatalogName(), req.getName())) {
        if (!transactionalListeners.isEmpty()) {
          DropDatabaseEvent dropEvent = new DropDatabaseEvent(db, true, this, isReplicated);
          EnvironmentContext context = null;
          if (!req.isDeleteManagedDir()) {
            context = new EnvironmentContext();
            context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
          }
          dropEvent.setEnvironmentContext(context);
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_DATABASE, dropEvent);
        }
        success = ms.commitTransaction();
      }
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (req.isDeleteData()) {
        // Delete the data in the partitions which have other locations
        deletePartitionData(partitionPaths, false, db);
        // Delete the data in the tables which have other locations or soft-delete is enabled
        for (Path tablePath : tablePaths) {
          deleteTableData(tablePath, false, db);
        }
        final Database dbFinal = db;
        final Path path = (dbFinal.getManagedLocationUri() != null) ?
            new Path(dbFinal.getManagedLocationUri()) : wh.getDatabaseManagedPath(dbFinal);
        if (req.isDeleteManagedDir()) {
          try {
            Boolean deleted = UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Boolean>)
              () -> wh.deleteDir(path, true, dbFinal));
            if (!deleted) {
              LOG.error("Failed to delete database's managed warehouse directory: " + path);
            }
          } catch (Exception e) {
            LOG.error("Failed to delete database's managed warehouse directory: " + path + " " + e.getMessage());
          }
        }
        try {
          Boolean deleted = UserGroupInformation.getCurrentUser().doAs((PrivilegedExceptionAction<Boolean>) 
              () -> wh.deleteDir(new Path(dbFinal.getLocationUri()), true, dbFinal));
          if (!deleted) {
            LOG.error("Failed to delete database external warehouse directory " + db.getLocationUri());
          }
        } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
          LOG.error("Failed to delete the database external warehouse directory: " + db.getLocationUri() + " " + e
            .getMessage());
        }
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.DROP_DATABASE,
            new DropDatabaseEvent(db, success, this, isReplicated),
            null,
            transactionalListenerResponses, ms);
      }
    }
  }

  private boolean drop_remote_database_core(RawStore ms, final Database db) throws MetaException, NoSuchObjectException {
    boolean success = false;
    firePreEvent(new PreDropDatabaseEvent(db, this));

    if (ms.dropDatabase(db.getCatalogName(), db.getName())) {
      success = ms.commitTransaction();
    }
    return success;
  }

  @Override
  public void drop_database(final String dbName, final boolean deleteData, final boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String[] parsedDbName = parseDbName(dbName, conf);
    
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(parsedDbName[DB_NAME]);
    req.setCatalogName(parsedDbName[CAT_NAME]);
    req.setDeleteData(deleteData);
    req.setCascade(cascade);
    drop_database_req(req);  
  }

  @Override
  public void drop_database_req(final DropDatabaseRequest req)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    startFunction("drop_database", ": " + req.getName());
    
    if (DEFAULT_CATALOG_NAME.equalsIgnoreCase(req.getCatalogName()) 
          && DEFAULT_DATABASE_NAME.equalsIgnoreCase(req.getName())) {
      endFunction("drop_database", false, null);
      throw new MetaException("Can not drop " + DEFAULT_DATABASE_NAME + " database in catalog " 
        + DEFAULT_CATALOG_NAME);
    }
    boolean success = false;
    Exception ex = null;
    try {
      drop_database_core(getMS(), req);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(NoSuchObjectException.class, InvalidOperationException.class, MetaException.class)
          .defaultMetaException();
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
        ret = FilterUtils.filterDbNamesIfEnabled(isServerFilterEnabled, filterHook, ret);
      } else {
        ret = getMS().getDatabases(parsedDbNamed[CAT_NAME], parsedDbNamed[DB_NAME]);
        ret = FilterUtils.filterDbNamesIfEnabled(isServerFilterEnabled, filterHook, ret);
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_databases", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_all_databases() throws MetaException {
    // get_databases filters results already. No need to filter here
    return get_databases(MetaStoreUtils.prependCatalogToDbName(null, null, conf));
  }

  private void create_dataconnector_core(RawStore ms, final DataConnector connector)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    if (!MetaStoreUtils.validateName(connector.getName(), conf)) {
      throw new InvalidObjectException(connector.getName() + " is not a valid dataconnector name");
    }

    if (connector.getOwnerName() == null){
      try {
        connector.setOwnerName(SecurityUtils.getUGI().getShortUserName());
      }catch (Exception e){
        LOG.warn("Failed to get owner name for create dataconnector operation.", e);
      }
    }
    long time = System.currentTimeMillis()/1000;
    connector.setCreateTime((int) time);
    boolean success = false;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();
    try {
      firePreEvent(new PreCreateDataConnectorEvent(connector, this));

      ms.openTransaction();
      ms.createDataConnector(connector);

      if (!transactionalListeners.isEmpty()) {
        transactionalListenersResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.CREATE_DATACONNECTOR,
                new CreateDataConnectorEvent(connector, true, this));
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.CREATE_DATACONNECTOR,
            new CreateDataConnectorEvent(connector, success, this),
            null,
            transactionalListenersResponses, ms);
      }
    }
  }

  @Override
  public void create_dataconnector(final DataConnector connector)
      throws AlreadyExistsException, InvalidObjectException, MetaException {
    startFunction("create_dataconnector", ": " + connector.toString());
    boolean success = false;
    Exception ex = null;
    try {
      try {
        if (null != get_dataconnector_core(connector.getName())) {
          throw new AlreadyExistsException("DataConnector " + connector.getName() + " already exists");
        }
      } catch (NoSuchObjectException e) {
        // expected
      }
      create_dataconnector_core(getMS(), connector);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_connector", success, ex);
    }
  }

  @Override
  public DataConnector get_dataconnector_core(final String name) throws NoSuchObjectException, MetaException {
    DataConnector connector = null;
    if (name == null) {
      throw new MetaException("Data connector name cannot be null.");
    }
    try {
      connector = getMS().getDataConnector(name);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    }
    return connector;
  }

  @Override
  public DataConnector get_dataconnector_req(GetDataConnectorRequest request) throws NoSuchObjectException, MetaException {
    startFunction("get_dataconnector", ": " + request.getConnectorName());
    DataConnector connector = null;
    Exception ex = null;
    try {
      connector = get_dataconnector_core(request.getConnectorName());
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .defaultRuntimeException();
    } finally {
      endFunction("get_dataconnector", connector != null, ex);
    }
    return connector;
  }

  @Override
  public void alter_dataconnector(final String dcName, final DataConnector newDC) throws TException {
    startFunction("alter_dataconnector " + dcName);
    boolean success = false;
    Exception ex = null;
    RawStore ms = getMS();
    DataConnector oldDC = null;
    Map<String, String> transactionalListenersResponses = Collections.emptyMap();

    try {
      oldDC = get_dataconnector_core(dcName);
      if (oldDC == null) {
        throw new MetaException("Could not alter dataconnector \"" + dcName +
            "\". Could not retrieve old definition.");
      }
      firePreEvent(new PreAlterDataConnectorEvent(oldDC, newDC, this));

      ms.openTransaction();
      ms.alterDataConnector(dcName, newDC);
      DataConnectorProviderFactory.invalidateDataConnectorFromCache(dcName);

        /*
        if (!transactionalListeners.isEmpty()) {
          transactionalListenersResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.ALTER_DATACONNECTOR,
                  new AlterDataConnectorEvent(oldDC, newDC, true, this));
        }
         */

      success = ms.commitTransaction();
    } catch (MetaException|NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
/*
        if ((null != oldDC) && (!listeners.isEmpty())) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_DATACONNECTOR,
              new AlterDataConnectorEvent(oldDC, newDC, success, this),
              null,
              transactionalListenersResponses, ms);
        }
 */
      endFunction("alter_database", success, ex);
    }
  }

  @Override
  public List<String> get_dataconnectors() throws MetaException {
    startFunction("get_dataconnectors");

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllDataConnectorNames();
      ret = FilterUtils.filterDataConnectorsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_dataconnectors", ret != null, ex);
    }
    return ret;
  }

  @Override
  public void drop_dataconnector(final String dcName, boolean ifNotExists, boolean checkReferences) throws NoSuchObjectException, InvalidOperationException, MetaException {
    startFunction("drop_dataconnector", ": " + dcName);
    boolean success = false;
    DataConnector connector = null;
    Exception ex = null;
    RawStore ms = getMS();
    try {
      connector = getMS().getDataConnector(dcName);
      DataConnectorProviderFactory.invalidateDataConnectorFromCache(dcName);
    } catch (NoSuchObjectException e) {
      if (!ifNotExists) {
        throw new NoSuchObjectException("DataConnector " + dcName + " doesn't exist");
      } else {
        return;
      }
    }
    try {
      ms.openTransaction();
      // TODO find DBs with references to this connector
      // if any existing references and checkReferences=true, do not drop

      firePreEvent(new PreDropDataConnectorEvent(connector, this));

      if (!ms.dropDataConnector(dcName)) {
        throw new MetaException("Unable to drop dataconnector " + dcName);
      } else {
/*
          // TODO
          if (!transactionalListeners.isEmpty()) {
            transactionalListenerResponses =
                MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                    EventType.DROP_TABLE,
                    new DropTableEvent(tbl, true, deleteData,
                        this, isReplicated),
                    envContext);
          }
 */
        success = ms.commitTransaction();
      }
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
/*
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DROP_TABLE,
              new DropTableEvent(tbl, success, deleteData, this, isReplicated),
              envContext,
              transactionalListenerResponses, ms);
        }
 */
      endFunction("drop_dataconnector", success, ex);
    }
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
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
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

  @Override
  public Table translate_table_dryrun(final CreateTableRequest req) throws AlreadyExistsException,
          MetaException, InvalidObjectException, InvalidInputException {
    Table transformedTbl = null;
    Table tbl = req.getTable();
    List<String> processorCapabilities = req.getProcessorCapabilities();
    String processorId = req.getProcessorIdentifier();
    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(conf));
    }
    if (transformer != null) {
      transformedTbl = transformer.transformCreateTable(tbl, processorCapabilities, processorId);
    }
    return transformedTbl != null ? transformedTbl : tbl;
  }

  private void create_table_core(final RawStore ms, final Table tbl,
                                 final EnvironmentContext envContext)
      throws AlreadyExistsException, MetaException,
      InvalidObjectException, NoSuchObjectException, InvalidInputException {
    CreateTableRequest req = new CreateTableRequest(tbl);
    req.setEnvContext(envContext);
    create_table_core(ms, req);
  }

  private void create_table_core(final RawStore ms, final Table tbl,
                                 final EnvironmentContext envContext, List<SQLPrimaryKey> primaryKeys,
                                 List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
                                 List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
                                 List<SQLCheckConstraint> checkConstraints,
                                 List<String> processorCapabilities, String processorIdentifier)
      throws AlreadyExistsException, MetaException,
      InvalidObjectException, NoSuchObjectException, InvalidInputException {
    CreateTableRequest req = new CreateTableRequest(tbl);
    if (envContext != null) {
      req.setEnvContext(envContext);
    }
    if (primaryKeys != null) {
      req.setPrimaryKeys(primaryKeys);
    }
    if (foreignKeys != null) {
      req.setForeignKeys(foreignKeys);
    }
    if (uniqueConstraints != null) {
      req.setUniqueConstraints(uniqueConstraints);
    }
    if (notNullConstraints != null) {
      req.setNotNullConstraints(notNullConstraints);
    }
    if (defaultConstraints != null) {
      req.setDefaultConstraints(defaultConstraints);
    }
    if (checkConstraints != null) {
      req.setCheckConstraints(checkConstraints);
    }
    if (processorCapabilities != null) {
      req.setProcessorCapabilities(processorCapabilities);
      req.setProcessorIdentifier(processorIdentifier);
    }
    create_table_core(ms, req);
  }

  private void create_table_core(final RawStore ms, final CreateTableRequest req)
      throws AlreadyExistsException, MetaException,
      InvalidObjectException, NoSuchObjectException, InvalidInputException {
    ColumnStatistics colStats = null;
    Table tbl = req.getTable();
    EnvironmentContext envContext = req.getEnvContext();
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(req.getPrimaryKeys());
    constraints.setForeignKeys(req.getForeignKeys());
    constraints.setUniqueConstraints(req.getUniqueConstraints());
    constraints.setDefaultConstraints(req.getDefaultConstraints());
    constraints.setCheckConstraints(req.getCheckConstraints());
    constraints.setNotNullConstraints(req.getNotNullConstraints());
    List<String> processorCapabilities = req.getProcessorCapabilities();
    String processorId = req.getProcessorIdentifier();

    // To preserve backward compatibility throw MetaException in case of null database
    if (tbl.getDbName() == null) {
      throw new MetaException("Null database name is not allowed");
    }

    if (!MetaStoreUtils.validateName(tbl.getTableName(), conf)) {
      throw new InvalidObjectException(tbl.getTableName()
          + " is not a valid object name");
    }

    if (!MetaStoreUtils.validateTblStorage(tbl.getSd())) {
      throw new InvalidObjectException(tbl.getTableName()
              + " location must not be root path");
    }

    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(conf));
    }

    Database db = get_database_core(tbl.getCatName(), tbl.getDbName());
    if (db != null && db.getType().equals(DatabaseType.REMOTE)) {
      // HIVE-24425: Create table in REMOTE db should fail
      throw new MetaException("Create table in REMOTE database " + db.getName() + " is not allowed");
    }

    if (is_table_exists(ms, tbl.getCatName(), tbl.getDbName(), tbl.getTableName())) {
      throw new AlreadyExistsException("Table " + getCatalogQualifiedTableName(tbl)
          + " already exists");
    }

    tbl.setDbName(normalizeIdentifier(tbl.getDbName()));
    tbl.setTableName(normalizeIdentifier(tbl.getTableName()));

    if (transformer != null) {
      tbl = transformer.transformCreateTable(tbl, processorCapabilities, processorId);
    }

    Map<String, String> params = tbl.getParameters();
    if (params != null) {
      params.remove(TABLE_IS_CTAS);
      params.remove(TABLE_IS_CTLT);
      if (MetaStoreServerUtils.getBooleanEnvProp(envContext, CTAS_LEGACY_CONFIG) &&
          TableType.MANAGED_TABLE.toString().equals(tbl.getTableType())) {
        params.put("EXTERNAL", "TRUE");
        tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      }
    }

    // If the given table has column statistics, save it here. We will update it later.
    // We don't want it to be part of the Table object being created, lest the create table
    // event will also have the col stats which we don't want.
    if (tbl.isSetColStats()) {
      colStats = tbl.getColStats();
      tbl.unsetColStats();
    }

    String validate = MetaStoreServerUtils.validateTblColumns(tbl.getSd().getCols());
    if (validate != null) {
      throw new InvalidObjectException("Invalid column " + validate);
    }
    if (tbl.getPartitionKeys() != null) {
      validate = MetaStoreServerUtils.validateTblColumns(tbl.getPartitionKeys());
      if (validate != null) {
        throw new InvalidObjectException("Invalid partition column " + validate);
      }
    }
    if (tbl.isSetId()) {
      LOG.debug("Id shouldn't be set but table {}.{} has the Id set to {}. Id is ignored.", tbl.getDbName(),
          tbl.getTableName(), tbl.getId());
      tbl.unsetId();
    }
    SkewedInfo skew = tbl.getSd().getSkewedInfo();
    if (skew != null) {
      validate = MetaStoreServerUtils.validateSkewedColNames(skew.getSkewedColNames());
      if (validate != null) {
        throw new InvalidObjectException("Invalid skew column " + validate);
      }
      validate = MetaStoreServerUtils.validateSkewedColNamesSubsetCol(
          skew.getSkewedColNames(), tbl.getSd().getCols());
      if (validate != null) {
        throw new InvalidObjectException("Invalid skew column " + validate);
      }
    }

    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Path tblPath = null;
    boolean success = false, madeDir = false;
    boolean isReplicated = false;
    try {

      ms.openTransaction();

      db = ms.getDatabase(tbl.getCatName(), tbl.getDbName());
      isReplicated = isDbReplicationTarget(db);

      firePreEvent(new PreCreateTableEvent(tbl, db, this));

      if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
        if (tbl.getSd().getLocation() == null
            || tbl.getSd().getLocation().isEmpty()) {
          tblPath = wh.getDefaultTablePath(db, tbl.getTableName() + getTableSuffix(tbl),
              MetaStoreUtils.isExternalTable(tbl));
        } else {
          if (!MetaStoreUtils.isExternalTable(tbl) && !MetaStoreUtils.isNonNativeTable(tbl)) {
            LOG.warn("Location: " + tbl.getSd().getLocation()
                + " specified for non-external table:" + tbl.getTableName());
          }
          tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          // ignore suffix if it's already there (direct-write CTAS)
          if (!tblPath.getName().matches("(.*)" + SOFT_DELETE_TABLE_PATTERN)) {
            tblPath = new Path(tblPath + getTableSuffix(tbl));
          }
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

      MetaStoreServerUtils.updateTableStatsForCreateTable(wh, db, tbl, envContext, conf, tblPath, madeDir);

      // set create time
      long time = System.currentTimeMillis() / 1000;
      tbl.setCreateTime((int) time);
      if (tbl.getParameters() == null ||
          tbl.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
        tbl.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
      }

      if (CollectionUtils.isEmpty(constraints.getPrimaryKeys()) && CollectionUtils.isEmpty(constraints.getForeignKeys())
          && CollectionUtils.isEmpty(constraints.getUniqueConstraints())&& CollectionUtils.isEmpty(constraints.getNotNullConstraints())&& CollectionUtils.isEmpty(constraints.getDefaultConstraints())
          && CollectionUtils.isEmpty(constraints.getCheckConstraints())) {
        ms.createTable(tbl);
      } else {
        final String catName = tbl.getCatName();
        // Check that constraints have catalog name properly set first
        if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys()) && !constraints.getPrimaryKeys().get(0).isSetCatName()) {
          constraints.getPrimaryKeys().forEach(constraint -> constraint.setCatName(catName));
        }
        if (CollectionUtils.isNotEmpty(constraints.getForeignKeys()) && !constraints.getForeignKeys().get(0).isSetCatName()) {
          constraints.getForeignKeys().forEach(constraint -> constraint.setCatName(catName));
        }
        if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints()) && !constraints.getUniqueConstraints().get(0).isSetCatName()) {
          constraints.getUniqueConstraints().forEach(constraint -> constraint.setCatName(catName));
        }
        if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints()) && !constraints.getNotNullConstraints().get(0).isSetCatName()) {
          constraints.getNotNullConstraints().forEach(constraint -> constraint.setCatName(catName));
        }
        if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints()) && !constraints.getDefaultConstraints().get(0).isSetCatName()) {
          constraints.getDefaultConstraints().forEach(constraint -> constraint.setCatName(catName));
        }
        if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints()) && !constraints.getCheckConstraints().get(0).isSetCatName()) {
          constraints.getCheckConstraints().forEach(constraint -> constraint.setCatName(catName));
        }
        // Set constraint name if null before sending to listener
        constraints = ms.createTableWithConstraints(tbl, constraints);

      }

      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
            EventType.CREATE_TABLE, new CreateTableEvent(tbl, true, this, isReplicated), envContext);
        if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_PRIMARYKEY,
              new AddPrimaryKeyEvent(constraints.getPrimaryKeys(), true, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_FOREIGNKEY,
              new AddForeignKeyEvent(constraints.getForeignKeys(), true, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_UNIQUECONSTRAINT,
              new AddUniqueConstraintEvent(constraints.getUniqueConstraints(), true, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_NOTNULLCONSTRAINT,
              new AddNotNullConstraintEvent(constraints.getNotNullConstraints(), true, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_CHECKCONSTRAINT,
              new AddCheckConstraintEvent(constraints.getCheckConstraints(), true, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventType.ADD_DEFAULTCONSTRAINT,
              new AddDefaultConstraintEvent(constraints.getDefaultConstraints(), true, this), envContext);
        }
      }

      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
        if (madeDir) {
          wh.deleteDir(tblPath, true, false, ReplChangeManager.shouldEnableCm(db, tbl));
        }
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.CREATE_TABLE,
            new CreateTableEvent(tbl, success, this, isReplicated), envContext,
            transactionalListenerResponses, ms);
        if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_PRIMARYKEY,
              new AddPrimaryKeyEvent(constraints.getPrimaryKeys(), success, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_FOREIGNKEY,
              new AddForeignKeyEvent(constraints.getForeignKeys(), success, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_UNIQUECONSTRAINT,
              new AddUniqueConstraintEvent(constraints.getUniqueConstraints(), success, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_NOTNULLCONSTRAINT,
              new AddNotNullConstraintEvent(constraints.getNotNullConstraints(), success, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_DEFAULTCONSTRAINT,
              new AddDefaultConstraintEvent(constraints.getDefaultConstraints(), success, this), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ADD_CHECKCONSTRAINT,
              new AddCheckConstraintEvent(constraints.getCheckConstraints(), success, this), envContext);
        }
      }
    }

    // If the table has column statistics, update it into the metastore. We need a valid
    // writeId list to update column statistics for a transactional table. But during bootstrap
    // replication, where we use this feature, we do not have a valid writeId list which was
    // used to update the stats. But we know for sure that the writeId associated with the
    // stats was valid then (otherwise stats update would have failed on the source). So, craft
    // a valid transaction list with only that writeId and use it to update the stats.
    if (colStats != null) {
      long writeId = tbl.getWriteId();
      String validWriteIds = null;
      if (writeId > 0) {
        ValidWriteIdList validWriteIdList =
            new ValidReaderWriteIdList(TableName.getDbTable(tbl.getDbName(),
                tbl.getTableName()),
                new long[0], new BitSet(), writeId);
        validWriteIds = validWriteIdList.toString();
      }
      updateTableColumnStatsInternal(colStats, validWriteIds, tbl.getWriteId());
    }
  }

  private String getTableSuffix(Table tbl) {
    return tbl.isSetTxnId() && tbl.getParameters() != null 
        && Boolean.parseBoolean(tbl.getParameters().get(SOFT_DELETE_TABLE)) ?
      SOFT_DELETE_PATH_SUFFIX + String.format(DELTA_DIGITS, tbl.getTxnId()) : "";
  }

  @Override
  public void create_table(final Table tbl) throws AlreadyExistsException,
      MetaException, InvalidObjectException, InvalidInputException {
    create_table_with_environment_context(tbl, null);
  }

  @Override
  public void create_table_with_environment_context(final Table tbl,
                                                    final EnvironmentContext envContext)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      create_table_core(getMS(), tbl, envContext);
      success = true;
    } catch (Exception e) {
      LOG.warn("create_table_with_environment_context got ", e);
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table", success, ex, tbl.getTableName());
    }
  }

  @Override
  public void create_table_req(final CreateTableRequest req)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    Table tbl = req.getTable();
    startFunction("create_table_req", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      create_table_core(getMS(), req);
      success = true;
    } catch (Exception e) {
      LOG.warn("create_table_req got ", e);
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_req", success, ex, tbl.getTableName());
    }
  }

  @Override
  public void create_table_with_constraints(final Table tbl,
                                            final List<SQLPrimaryKey> primaryKeys, final List<SQLForeignKey> foreignKeys,
                                            List<SQLUniqueConstraint> uniqueConstraints,
                                            List<SQLNotNullConstraint> notNullConstraints,
                                            List<SQLDefaultConstraint> defaultConstraints,
                                            List<SQLCheckConstraint> checkConstraints)
      throws AlreadyExistsException, MetaException, InvalidObjectException,
      InvalidInputException {
    startFunction("create_table", ": " + tbl.toString());
    boolean success = false;
    Exception ex = null;
    try {
      CreateTableRequest req = new CreateTableRequest(tbl);
      req.setPrimaryKeys(primaryKeys);
      req.setForeignKeys(foreignKeys);
      req.setUniqueConstraints(uniqueConstraints);
      req.setNotNullConstraints(notNullConstraints);
      req.setDefaultConstraints(defaultConstraints);
      req.setCheckConstraints(checkConstraints);
      create_table_req(req);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .throwIfInstance(AlreadyExistsException.class, InvalidInputException.class)
          .defaultMetaException();
    } finally {
      endFunction("create_table_with_constraints", success, ex, tbl.getTableName());
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
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
    String constraintName = (CollectionUtils.isNotEmpty(primaryKeyCols)) ?
        primaryKeyCols.get(0).getPk_name() : "null";
    startFunction("add_primary_key", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (CollectionUtils.isNotEmpty(primaryKeyCols) && !primaryKeyCols.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      primaryKeyCols.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      List<SQLPrimaryKey> primaryKeys = ms.addPrimaryKeys(primaryKeyCols);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
          AddPrimaryKeyEvent addPrimaryKeyEvent = new AddPrimaryKeyEvent(primaryKeys, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddPrimaryKey(addPrimaryKeyEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
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
    List<SQLForeignKey> foreignKeys = req.getForeignKeyCols();
    String constraintName = CollectionUtils.isNotEmpty(foreignKeys) ?
        foreignKeys.get(0).getFk_name() : "null";
    startFunction("add_foreign_key", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (CollectionUtils.isNotEmpty(foreignKeys) && !foreignKeys.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      foreignKeys.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      foreignKeys = ms.addForeignKeys(foreignKeys);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(foreignKeys)) {
          AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeys, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddForeignKey(addForeignKeyEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(foreignKeys)) {
        for (MetaStoreEventListener listener : listeners) {
          AddForeignKeyEvent addForeignKeyEvent = new AddForeignKeyEvent(foreignKeys, true, this);
          listener.onAddForeignKey(addForeignKeyEvent);
        }
      }
      endFunction("add_foreign_key", success, ex, constraintName);
    }
  }

  @Override
  public void add_unique_constraint(AddUniqueConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLUniqueConstraint> uniqueConstraints = req.getUniqueConstraintCols();
    String constraintName = (uniqueConstraints != null && uniqueConstraints.size() > 0) ?
        uniqueConstraints.get(0).getUk_name() : "null";
    startFunction("add_unique_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!uniqueConstraints.isEmpty() && !uniqueConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      uniqueConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      uniqueConstraints = ms.addUniqueConstraints(uniqueConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(uniqueConstraints)) {
          AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddUniqueConstraint(addUniqueConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(uniqueConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddUniqueConstraintEvent addUniqueConstraintEvent = new AddUniqueConstraintEvent(uniqueConstraints, true, this);
          listener.onAddUniqueConstraint(addUniqueConstraintEvent);
        }
      }
      endFunction("add_unique_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_not_null_constraint(AddNotNullConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLNotNullConstraint> notNullConstraints = req.getNotNullConstraintCols();
    String constraintName = (notNullConstraints != null && notNullConstraints.size() > 0) ?
        notNullConstraints.get(0).getNn_name() : "null";
    startFunction("add_not_null_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!notNullConstraints.isEmpty() && !notNullConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      notNullConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      notNullConstraints = ms.addNotNullConstraints(notNullConstraints);

      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(notNullConstraints)) {
          AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddNotNullConstraint(addNotNullConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(notNullConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddNotNullConstraintEvent addNotNullConstraintEvent = new AddNotNullConstraintEvent(notNullConstraints, true, this);
          listener.onAddNotNullConstraint(addNotNullConstraintEvent);
        }
      }
      endFunction("add_not_null_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_default_constraint(AddDefaultConstraintRequest req) throws MetaException, InvalidObjectException {
    List<SQLDefaultConstraint> defaultConstraints = req.getDefaultConstraintCols();
    String constraintName =
        CollectionUtils.isNotEmpty(defaultConstraints) ? defaultConstraints.get(0).getDc_name() : "null";
    startFunction("add_default_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!defaultConstraints.isEmpty() && !defaultConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      defaultConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      defaultConstraints = ms.addDefaultConstraints(defaultConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(defaultConstraints)) {
          AddDefaultConstraintEvent addDefaultConstraintEvent =
              new AddDefaultConstraintEvent(defaultConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddDefaultConstraint(addDefaultConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(defaultConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddDefaultConstraintEvent addDefaultConstraintEvent =
              new AddDefaultConstraintEvent(defaultConstraints, true, this);
          listener.onAddDefaultConstraint(addDefaultConstraintEvent);
        }
      }
      endFunction("add_default_constraint", success, ex, constraintName);
    }
  }

  @Override
  public void add_check_constraint(AddCheckConstraintRequest req)
      throws MetaException, InvalidObjectException {
    List<SQLCheckConstraint> checkConstraints= req.getCheckConstraintCols();
    String constraintName = CollectionUtils.isNotEmpty(checkConstraints) ?
        checkConstraints.get(0).getDc_name() : "null";
    startFunction("add_check_constraint", ": " + constraintName);
    boolean success = false;
    Exception ex = null;
    if (!checkConstraints.isEmpty() && !checkConstraints.get(0).isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      checkConstraints.forEach(pk -> pk.setCatName(defaultCat));
    }
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      checkConstraints = ms.addCheckConstraints(checkConstraints);
      if (transactionalListeners.size() > 0) {
        if (CollectionUtils.isNotEmpty(checkConstraints)) {
          AddCheckConstraintEvent addcheckConstraintEvent = new AddCheckConstraintEvent(checkConstraints, true, this);
          for (MetaStoreEventListener transactionalListener : transactionalListeners) {
            transactionalListener.onAddCheckConstraint(addcheckConstraintEvent);
          }
        }
      }
      success = ms.commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidObjectException.class).defaultMetaException();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (CollectionUtils.isNotEmpty(checkConstraints)) {
        for (MetaStoreEventListener listener : listeners) {
          AddCheckConstraintEvent addCheckConstraintEvent = new AddCheckConstraintEvent(checkConstraints, true, this);
          listener.onAddCheckConstraint(addCheckConstraintEvent);
        }
      }
      endFunction("add_check_constraint", success, ex, constraintName);
    }
  }

  private boolean is_table_exists(RawStore ms, String catName, String dbname, String name)
      throws MetaException {
    return (ms.getTable(catName, dbname, name, null) != null);
  }

  private boolean drop_table_core(final RawStore ms, final String catName, final String dbname,
                                  final String name, final boolean deleteData,
                                  final EnvironmentContext envContext, final String indexName, boolean dropPartitions)
    throws TException, IOException {
    boolean success = false;
    boolean tableDataShouldBeDeleted = false;
    Path tblPath = null;
    List<Path> partPaths = null;
    Table tbl = null;
    boolean ifPurge = false;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;
    boolean isReplicated = false;
    try {
      ms.openTransaction();

      // HIVE-25282: Drop/Alter table in REMOTE db should fail
      db = ms.getDatabase(catName, dbname);
      if (db.getType() == DatabaseType.REMOTE) {
        throw new MetaException("Drop table in REMOTE database " + db.getName() + " is not allowed");
      }
      isReplicated = isDbReplicationTarget(db);

      // drop any partitions
      GetTableRequest req = new GetTableRequest(dbname,name);
      req.setCatName(catName);
      tbl = get_table_core(req);
      if (tbl == null) {
        throw new NoSuchObjectException(name + " doesn't exist");
      }

      // Check if table is part of a materialized view.
      // If it is, it cannot be dropped.
      List<String> isPartOfMV = ms.isPartOfMaterializedView(catName, dbname, name);
      if (!isPartOfMV.isEmpty()) {
        throw new MetaException(String.format("Cannot drop table as it is used in the following materialized" +
            " views %s%n", isPartOfMV));
      }

      if (tbl.getSd() == null) {
        throw new MetaException("Table metadata is corrupted");
      }
      ifPurge = isMustPurge(envContext, tbl);

      firePreEvent(new PreDropTableEvent(tbl, deleteData, this));

      tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(tbl, deleteData);
      if (tableDataShouldBeDeleted && tbl.getSd().getLocation() != null) {
        tblPath = new Path(tbl.getSd().getLocation());
        if (!wh.isWritable(tblPath.getParent())) {
          String target = indexName == null ? "Table" : "Index table";
          throw new MetaException(target + " metadata not deleted since " +
              tblPath.getParent() + " is not writable by " +
              SecurityUtils.getUser());
        }
      }

      // Drop the partitions and get a list of locations which need to be deleted
      // In case of drop database cascade we need not to drop the partitions, they are already dropped.
      if (dropPartitions) {
        partPaths = dropPartitionsAndGetLocations(ms, catName, dbname, name, tblPath, tableDataShouldBeDeleted);
      }
      // Drop any constraints on the table
      ms.dropConstraint(catName, dbname, name, null, true);

      if (!ms.dropTable(catName, dbname, name)) {
        String tableName = TableName.getQualified(catName, dbname, name);
        throw new MetaException(indexName == null ? "Unable to drop table " + tableName:
            "Unable to drop index table " + tableName + " for index " + indexName);
      } else {
        if (!transactionalListeners.isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.DROP_TABLE,
                  new DropTableEvent(tbl, true, deleteData,
                      this, isReplicated),
                  envContext);
        }
        success = ms.commitTransaction();
      }
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (tableDataShouldBeDeleted) {
        // Data needs deletion. Check if trash may be skipped.
        // Delete the data in the partitions which have other locations
        deletePartitionData(partPaths, ifPurge, ReplChangeManager.shouldEnableCm(db, tbl));
        // Delete the data in the table
        deleteTableData(tblPath, ifPurge, ReplChangeManager.shouldEnableCm(db, tbl));
      }

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.DROP_TABLE,
            new DropTableEvent(tbl, success, deleteData, this, isReplicated),
            envContext,
            transactionalListenerResponses, ms);
      }
    }
    return success;
  }

  private boolean checkTableDataShouldBeDeleted(Table tbl, boolean deleteData) {
    if (deleteData && MetaStoreUtils.isExternalTable(tbl)) {
      // External table data can be deleted if EXTERNAL_TABLE_PURGE is true
      return MetaStoreUtils.isExternalTablePurge(tbl);
    }
    return deleteData;
  }

  /**
   * Deletes the data in a table's location, if it fails logs an error
   *
   * @param tablePath
   * @param ifPurge completely purge the table (skipping trash) while removing
   *                data from warehouse
   * @param shouldEnableCm If cm should be enabled
   */
  private void deleteTableData(Path tablePath, boolean ifPurge, boolean shouldEnableCm) {
    if (tablePath != null) {
      deleteDataExcludeCmroot(tablePath, ifPurge, shouldEnableCm);
    }
  }

  /**
   * Deletes the data in a table's location, if it fails logs an error.
   *
   * @param tablePath
   * @param ifPurge completely purge the table (skipping trash) while removing
   *                data from warehouse
   * @param db Database
   */
  private void deleteTableData(Path tablePath, boolean ifPurge, Database db) {
    if (tablePath != null) {
      try {
        wh.deleteDir(tablePath, true, ifPurge, db);
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
   * @param ifPurge completely purge the partition (skipping trash) while
   *                removing data from warehouse
   * @param shouldEnableCm If cm should be enabled
   */
  private void deletePartitionData(List<Path> partPaths, boolean ifPurge, boolean shouldEnableCm) {
    if (partPaths != null && !partPaths.isEmpty()) {
      for (Path partPath : partPaths) {
        deleteDataExcludeCmroot(partPath, ifPurge, shouldEnableCm);
      }
    }
  }

  /**
   * Give a list of partitions' locations, tries to delete each one
   * and for each that fails logs an error.
   *
   * @param partPaths
   * @param ifPurge completely purge the partition (skipping trash) while
   *                removing data from warehouse
   * @param db Database
   */
  private void deletePartitionData(List<Path> partPaths, boolean ifPurge, Database db) {
    if (partPaths != null && !partPaths.isEmpty()) {
      for (Path partPath : partPaths) {
        try {
          wh.deleteDir(partPath, true, ifPurge, db);
        } catch (Exception e) {
          LOG.error("Failed to delete partition directory: " + partPath +
              " " + e.getMessage());
        }
      }
    }
  }

  /**
   * Delete data from path excluding cmdir
   * and for each that fails logs an error.
   *
   * @param path
   * @param ifPurge completely purge the partition (skipping trash) while
   *                removing data from warehouse
   * @param shouldEnableCm If cm should be enabled
   */
  private void deleteDataExcludeCmroot(Path path, boolean ifPurge, boolean shouldEnableCm) {
    try {
      if (shouldEnableCm) {
        //Don't delete cmdir if its inside the partition path
        FileStatus[] statuses = path.getFileSystem(conf).listStatus(path,
            ReplChangeManager.CMROOT_PATH_FILTER);
        for (final FileStatus status : statuses) {
          wh.deleteDir(status.getPath(), true, ifPurge, shouldEnableCm);
        }
        //Check if table directory is empty, delete it
        FileStatus[] statusWithoutFilter = path.getFileSystem(conf).listStatus(path);
        if (statusWithoutFilter.length == 0) {
          wh.deleteDir(path, true, ifPurge, shouldEnableCm);
        }
      } else {
        //If no cm delete the complete table directory
        wh.deleteDir(path, true, ifPurge, shouldEnableCm);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete directory: " + path +
          " " + e.getMessage());
    }
  }

  /**
   * Deletes the partitions specified by catName, dbName, tableName. If checkLocation is true, for
   * locations of partitions which may not be subdirectories of tablePath checks to make sure the
   * locations are writable.
   *
   * Drops the metadata for each partition.
   *
   * Provides a list of locations of partitions which may not be subdirectories of tablePath.
   *
   * @param ms RawStore to use for metadata retrieval and delete
   * @param catName The catName
   * @param dbName The dbName
   * @param tableName The tableName
   * @param tablePath The tablePath of which subdirectories does not have to be checked
   * @param checkLocation Should we check the locations at all
   * @return The list of the Path objects to delete (only in case checkLocation is true)
   * @throws MetaException
   * @throws IOException
   * @throws NoSuchObjectException
   */
  private List<Path> dropPartitionsAndGetLocations(RawStore ms, String catName, String dbName,
                                                   String tableName, Path tablePath, boolean checkLocation)
      throws MetaException, IOException, NoSuchObjectException {
    int batchSize = MetastoreConf.getIntVar(conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
    String tableDnsPath = null;
    if (tablePath != null) {
      tableDnsPath = wh.getDnsPath(tablePath).toString();
    }

    List<Path> partPaths = new ArrayList<>();
    while (true) {
      List<String> partNames;
      if (checkLocation) {
        Map<String, String> partitionLocations = ms.getPartitionLocations(catName, dbName, tableName,
                tableDnsPath, batchSize);
        partNames = new ArrayList<>(partitionLocations.keySet());
        for (String partName : partNames) {
          String pathString = partitionLocations.get(partName);
          if (pathString != null) {
            Path partPath = wh.getDnsPath(new Path(pathString));
            // Double check here. Maybe Warehouse.getDnsPath revealed relationship between the
            // path objects
            if (tableDnsPath == null ||
                !FileUtils.isSubdirectory(tableDnsPath, partPath.toString())) {
              if (!wh.isWritable(partPath.getParent())) {
                throw new MetaException("Table metadata not deleted since the partition "
                    + partName + " has parent location " + partPath.getParent()
                    + " which is not writable by " + SecurityUtils.getUser());
              }
              partPaths.add(partPath);
            }
          }
        }
      } else {
        partNames = ms.listPartitionNames(catName, dbName, tableName, (short) batchSize);
      }

      if (partNames == null || partNames.isEmpty()) {
        // No more partitions left to drop. Return with the collected path list to delete.
        return partPaths;
      }

      for (MetaStoreEventListener listener : listeners) {
        //No drop part listener events fired for public listeners historically, for drop table case.
        //Limiting to internal listeners for now, to avoid unexpected calls for public listeners.
        if (listener instanceof HMSMetricsListener) {
          for (@SuppressWarnings("unused") String partName : partNames) {
            listener.onDropPartition(null);
          }
        }
      }

      ms.dropPartitions(catName, dbName, tableName, partNames);
    }
  }

  @Override
  public void drop_table(final String dbname, final String name, final boolean deleteData)
      throws NoSuchObjectException, MetaException {
    drop_table_with_environment_context(dbname, name, deleteData, null);
  }

  @Override
  public void drop_table_with_environment_context(final String dbname, final String name, final boolean deleteData,
      final EnvironmentContext envContext) throws NoSuchObjectException, MetaException {
    drop_table_with_environment_context(dbname, name, deleteData, envContext, true);
  }

  private void drop_table_with_environment_context(final String dbname, final String name, final boolean deleteData,
      final EnvironmentContext envContext, boolean dropPartitions) throws MetaException, NoSuchObjectException {
    String[] parsedDbName = parseDbName(dbname, conf);
    startTableFunction("drop_table", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name);

    boolean success = false;
    Exception ex = null;
    try {
      success =
          drop_table_core(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, deleteData, envContext, null, dropPartitions);
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
    } finally {
      endFunction("drop_table", success, ex, name);
    }
  }

  private void updateStatsForTruncate(Map<String,String> props, EnvironmentContext environmentContext) {
    if (null == props) {
      return;
    }
    for (String stat : StatsSetupConst.SUPPORTED_STATS) {
      String statVal = props.get(stat);
      if (statVal != null) {
        //In the case of truncate table, we set the stats to be 0.
        props.put(stat, "0");
      }
    }
    //first set basic stats to true
    StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
    environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_POPULATE_QUICK_STATS, StatsSetupConst.TRUE);
    //then invalidate column stats
    StatsSetupConst.clearColumnStatsState(props);
    return;
  }

  private void alterPartitionForTruncate(RawStore ms, String catName, String dbName, String tableName,
                                         Table table, Partition partition, String validWriteIds, long writeId) throws Exception {
    EnvironmentContext environmentContext = new EnvironmentContext();
    updateStatsForTruncate(partition.getParameters(), environmentContext);

    if (!transactionalListeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
          EventType.ALTER_PARTITION,
          new AlterPartitionEvent(partition, partition, table, true, true,
              writeId, this));
    }

    if (!listeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(listeners,
          EventType.ALTER_PARTITION,
          new AlterPartitionEvent(partition, partition, table, true, true,
              writeId, this));
    }

    if (writeId > 0) {
      partition.setWriteId(writeId);
    }
    alterHandler.alterPartition(ms, wh, catName, dbName, tableName, null, partition,
        environmentContext, this, validWriteIds);
  }

  private void alterTableStatsForTruncate(RawStore ms, String catName, String dbName,
                                          String tableName, Table table, List<String> partNames,
                                          String validWriteIds, long writeId) throws Exception {
    if (partNames == null) {
      if (0 != table.getPartitionKeysSize()) {
        for (Partition partition : ms.getPartitions(catName, dbName, tableName, -1)) {
          alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition,
              validWriteIds, writeId);
        }
      } else {
        EnvironmentContext environmentContext = new EnvironmentContext();
        updateStatsForTruncate(table.getParameters(), environmentContext);

        boolean isReplicated = isDbReplicationTarget(ms.getDatabase(catName, dbName));
        if (!transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
              EventType.ALTER_TABLE,
              new AlterTableEvent(table, table, true, true,
                  writeId, this, isReplicated));
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_TABLE,
              new AlterTableEvent(table, table, true, true,
                  writeId, this, isReplicated));
        }

        // TODO: this should actually pass thru and set writeId for txn stats.
        if (writeId > 0) {
          table.setWriteId(writeId);
        }
        alterHandler.alterTable(ms, wh, catName, dbName, tableName, table,
            environmentContext, this, validWriteIds);
      }
    } else {
      for (Partition partition : ms.getPartitionsByNames(catName, dbName, tableName, partNames)) {
        alterPartitionForTruncate(ms, catName, dbName, tableName, table, partition,
            validWriteIds, writeId);
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
        for (Partition partition : ms.getPartitions(catName, dbName, tableName, -1)) {
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
    // Deprecated path, won't work for txn tables.
    truncateTableInternal(dbName, tableName, partNames, null, -1, null);
  }

  @Override
  public TruncateTableResponse truncate_table_req(TruncateTableRequest req)
      throws MetaException, TException {
    truncateTableInternal(req.getDbName(), req.getTableName(), req.getPartNames(),
        req.getValidWriteIdList(), req.getWriteId(), req.getEnvironmentContext());
    return new TruncateTableResponse();
  }

  private void truncateTableInternal(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId, EnvironmentContext context) throws MetaException, NoSuchObjectException {
    boolean isSkipTrash = false, needCmRecycle = false;
    try {
      String[] parsedDbName = parseDbName(dbName, conf);
      Table tbl = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);

      boolean skipDataDeletion = Optional.ofNullable(context)
          .map(EnvironmentContext::getProperties)
          .map(prop -> prop.get(TRUNCATE_SKIP_DATA_DELETION))
          .map(Boolean::parseBoolean)
          .orElse(false);

      if (TxnUtils.isTransactionalTable(tbl) || !skipDataDeletion) {
        if (!skipDataDeletion) {
          isSkipTrash = MetaStoreUtils.isSkipTrash(tbl.getParameters());
          
          Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
          needCmRecycle = ReplChangeManager.shouldEnableCm(db, tbl);
        }
        // This is not transactional
        for (Path location : getLocationsForTruncate(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
            tbl, partNames)) {
          if (!skipDataDeletion) {
            truncateDataFiles(location, isSkipTrash, needCmRecycle);
          } else {
            // For Acid tables we don't need to delete the old files, only write an empty baseDir.
            // Compaction and cleaner will take care of the rest
            addTruncateBaseFile(location, writeId, conf, DataFormat.TRUNCATED);
          }
        }
      }

      // Alter the table/partition stats and also notify truncate table event
      alterTableStatsForTruncate(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tableName, tbl, partNames, validWriteIds, writeId);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class)
          .convertIfInstance(IOException.class, MetaException.class)
          .defaultMetaException();
    }
  }

  /**
   * Add an empty baseDir with a truncate metadatafile
   * @param location partition or table directory
   * @param writeId allocated writeId
   * @throws MetaException
   */
  static void addTruncateBaseFile(Path location, long writeId, Configuration conf, DataFormat dataFormat) 
      throws MetaException {
    if (location == null) 
      return;
    
    Path basePath = new Path(location, AcidConstants.baseDir(writeId));
    try {
      FileSystem fs = location.getFileSystem(conf);
      fs.mkdirs(basePath);
      // We can not leave the folder empty, otherwise it will be skipped at some file listing in AcidUtils
      // No need for a data file, a simple metadata is enough
      AcidMetaDataFile.writeToFile(fs, basePath, dataFormat);
    } catch (Exception e) {
      throw newMetaException(e);
    }
  }

  private void truncateDataFiles(Path location, boolean isSkipTrash, boolean needCmRecycle)
      throws IOException, MetaException {
    FileSystem fs = location.getFileSystem(getConf());
    
    if (!HdfsUtils.isPathEncrypted(getConf(), fs.getUri(), location) &&
        !FileUtils.pathHasSnapshotSubDir(location, fs)) {
      HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(getConf(), fs, location);
      FileStatus targetStatus = fs.getFileStatus(location);
      String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
      
      wh.deleteDir(location, true, isSkipTrash, needCmRecycle);
      fs.mkdirs(location);
      HdfsUtils.setFullFileStatus(getConf(), status, targetGroup, fs, location, false);
    } else {
      FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
      if (statuses == null || statuses.length == 0) {
        return;
      }
      for (final FileStatus status : statuses) {
        wh.deleteDir(status.getPath(), true, isSkipTrash, needCmRecycle);
      }
    }
  }

  @Override
  @Deprecated
  public Table get_table(final String dbname, final String name) throws MetaException,
      NoSuchObjectException {
    String[] parsedDbName = parseDbName(dbname, conf);
    GetTableRequest getTableRequest = new GetTableRequest(parsedDbName[DB_NAME],name);
    getTableRequest.setCatName(parsedDbName[CAT_NAME]);
    return getTableInternal(getTableRequest);
  }

  @Override
  public List<ExtendedTableInfo> get_tables_ext(final GetTablesExtRequest req) throws MetaException {
    List<String> tables = new ArrayList<String>();
    List<ExtendedTableInfo> ret = new ArrayList<ExtendedTableInfo>();
    String catalog  = req.getCatalog();
    String database = req.getDatabase();
    String pattern  = req.getTableNamePattern();
    List<String> processorCapabilities = req.getProcessorCapabilities();
    int limit = req.getLimit();
    String processorId  = req.getProcessorIdentifier();
    List<Table> tObjects = new ArrayList<>();

    startTableFunction("get_tables_ext", catalog, database, pattern);
    Exception ex = null;
    try {
      tables = getMS().getTables(catalog, database, pattern, null, limit);
      LOG.debug("get_tables_ext:getTables() returned " + tables.size());
      tables = FilterUtils.filterTableNamesIfEnabled(isServerFilterEnabled, filterHook,
          catalog, database, tables);

      if (tables.size() > 0) {
        tObjects = getMS().getTableObjectsByName(catalog, database, tables);
        LOG.debug("get_tables_ext:getTableObjectsByName() returned " + tObjects.size());
        if (processorCapabilities == null || processorCapabilities.size() == 0 ||
            processorCapabilities.contains("MANAGERAWMETADATA")) {
          LOG.info("Skipping translation for processor with " + processorId);
        } else {
          if (transformer != null) {
            Map<Table, List<String>> retMap = transformer.transform(tObjects, processorCapabilities, processorId);

            for (Map.Entry<Table, List<String>> entry : retMap.entrySet())  {
              LOG.debug("Table " + entry.getKey().getTableName() + " requires " + Arrays.toString((entry.getValue()).toArray()));
              ret.add(convertTableToExtendedTable(entry.getKey(), entry.getValue(), req.getRequestedFields()));
            }
          } else {
            for (Table table : tObjects) {
              ret.add(convertTableToExtendedTable(table, processorCapabilities, req.getRequestedFields()));
            }
          }
        }
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_tables_ext", ret != null, ex);
    }
    return ret;
  }

  private ExtendedTableInfo convertTableToExtendedTable (Table table,
                                                         List<String> processorCapabilities, int mask) {
    ExtendedTableInfo extTable = new ExtendedTableInfo(table.getTableName());
    if ((mask & GetTablesExtRequestFields.ACCESS_TYPE.getValue()) == GetTablesExtRequestFields.ACCESS_TYPE.getValue()) {
      extTable.setAccessType(table.getAccessType());
    }

    if ((mask & GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue())
        == GetTablesExtRequestFields.PROCESSOR_CAPABILITIES.getValue()) {
      extTable.setRequiredReadCapabilities(table.getRequiredReadCapabilities());
      extTable.setRequiredWriteCapabilities(table.getRequiredWriteCapabilities());
    }

    return extTable;
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException,
      NoSuchObjectException {
    req.setCatName(req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf));
    return new GetTableResult(getTableInternal(req));
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  private Table getTableInternal(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {

    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");

    if (isInTest) {
      assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.TEST_CAPABILITY, "Hive tests",
          "get_table_req");
    }

    Table t = null;
    startTableFunction("get_table", getTableRequest.getCatName(), getTableRequest.getDbName(),
        getTableRequest.getTblName());
    Exception ex = null;
    try {
      t = get_table_core(getTableRequest);
      if (MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
        assertClientHasCapability(getTableRequest.getCapabilities(), ClientCapability.INSERT_ONLY_TABLES,
            "insert-only tables", "get_table_req");
      }

      if (CollectionUtils.isEmpty(getTableRequest.getProcessorCapabilities()) || getTableRequest
          .getProcessorCapabilities().contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with " + getTableRequest.getProcessorIdentifier());
      } else {
        if (transformer != null) {
          List<Table> tList = new ArrayList<>();
          tList.add(t);
          Map<Table, List<String>> ret = transformer
              .transform(tList, getTableRequest.getProcessorCapabilities(), getTableRequest.getProcessorIdentifier());
          if (ret.size() > 1) {
            LOG.warn("Unexpected resultset size:" + ret.size());
            throw new MetaException("Unexpected result from metadata transformer:return list size is " + ret.size());
          }
          t = ret.keySet().iterator().next();
        }
      }

      firePreEvent(new PreReadTableEvent(t, this));
    } catch (MetaException | NoSuchObjectException e) {
      ex = e;
      throw e;
    } finally {
      endFunction("get_table", t != null, ex, getTableRequest.getTblName());
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
      t = FilterUtils.filterTableMetasIfEnabled(isServerFilterEnabled, filterHook, t);
      t = filterReadableTables(parsedDbName[CAT_NAME], t);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_table_metas", t != null, ex);
    }
    return t;
  }

  @Override
  @Deprecated
  public Table get_table_core(
      final String catName,
      final String dbname,
      final String name)
      throws MetaException, NoSuchObjectException {
    GetTableRequest getTableRequest = new GetTableRequest(dbname,name);
    getTableRequest.setCatName(catName);
    return get_table_core(getTableRequest);
  }

  @Override
  @Deprecated
  public Table get_table_core(
      final String catName,
      final String dbname,
      final String name,
      final String writeIdList)
      throws MetaException, NoSuchObjectException {
    GetTableRequest getTableRequest = new GetTableRequest(dbname,name);
    getTableRequest.setCatName(catName);
    getTableRequest.setValidWriteIdList(writeIdList);
    return get_table_core(getTableRequest);
  }

  /**
   * This function retrieves table from metastore. If getColumnStats flag is true,
   * then engine should be specified so the table is retrieve with the column stats
   * for that engine.
   */
  @Override
  public Table get_table_core(GetTableRequest getTableRequest) throws MetaException, NoSuchObjectException {
    Preconditions.checkArgument(!getTableRequest.isGetColumnStats() || getTableRequest.getEngine() != null,
        "To retrieve column statistics with a table, engine parameter cannot be null");
    String catName = getTableRequest.getCatName();
    String dbName = getTableRequest.getDbName();
    String tblName = getTableRequest.getTblName();
    Database db = null;
    Table t = null;
    try {
      db = get_database_core(catName, dbName);
    } catch (Exception e) { /* appears exception is not thrown currently if db doesnt exist */ }

    if (db != null) {
      if (db.getType().equals(DatabaseType.REMOTE)) {
        t = DataConnectorProviderFactory.getDataConnectorProvider(db).getTable(tblName);
        if (t == null) {
          throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
        }
        t.setDbName(dbName);
        return t;
      }
    }

    try {
      t = getMS().getTable(catName, dbName, tblName, getTableRequest.getValidWriteIdList(), getTableRequest.getId());
      if (t == null) {
        throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName) + " table not found");
      }

      // If column statistics was requested and is valid fetch it.
      if (getTableRequest.isGetColumnStats()) {
        ColumnStatistics colStats = getMS().getTableColumnStatistics(catName, dbName, tblName,
            StatsSetupConst.getColumnsHavingStats(t.getParameters()), getTableRequest.getEngine(),
            getTableRequest.getValidWriteIdList());
        if (colStats != null) {
          t.setColStats(colStats);
        }
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
    return getTableObjectsInternal(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableNames, null, null, null);
  }

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req) throws TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    if (isDatabaseRemote(req.getDbName())) {
      return new GetTablesResult(getRemoteTableObjectsInternal(req.getDbName(), req.getTblNames(), req.getTablesPattern()));
    }
    return new GetTablesResult(getTableObjectsInternal(catName, req.getDbName(),
        req.getTblNames(), req.getCapabilities(), req.getProjectionSpec(), req.getTablesPattern()));
  }

  private List<Table> filterTablesByName(List<Table> tables, List<String> tableNames) {
    List<Table> filteredTables = new ArrayList<>();
    for (Table table : tables) {
      if (tableNames.contains(table.getTableName())) {
        filteredTables.add(table);
      }
    }
    return filteredTables;
  }

  private List<Table> getRemoteTableObjectsInternal(String dbname, List<String> tableNames, String pattern) throws MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    try {
      // retrieve tables from remote database
      Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      List<Table> tables = DataConnectorProviderFactory.getDataConnectorProvider(db).getTables(null);

      // filtered out undesired tables
      if (tableNames != null) {
        tables = filterTablesByName(tables, tableNames);
      }

      // set remote tables' local hive database reference
      for (Table table : tables) {
        table.setDbName(dbname);
      }

      return FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tables);
    } catch (Exception e) {
      LOG.warn("Unexpected exception while getting table(s) in remote database " + dbname , e);
      return new ArrayList<Table>();
    }
  }

  private List<Table> getTableObjectsInternal(String catName, String dbName,
                                              List<String> tableNames,
                                              ClientCapabilities capabilities,
                                              GetProjectionsSpec projectionsSpec, String tablePattern)
      throws MetaException, InvalidOperationException, UnknownDBException {
    if (isInTest) {
      assertClientHasCapability(capabilities, ClientCapability.TEST_CAPABILITY,
          "Hive tests", "get_table_objects_by_name_req");
    }

    if (projectionsSpec != null) {
      if (!projectionsSpec.isSetFieldList() && (projectionsSpec.isSetIncludeParamKeyPattern() ||
          projectionsSpec.isSetExcludeParamKeyPattern())) {
        throw new InvalidOperationException("Include and Exclude Param key are not supported.");
      }
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
      RawStore ms = getMS();
      if(tablePattern != null){
        tables = ms.getTableObjectsByName(catName, dbName, tableNames, projectionsSpec, tablePattern);
      }else {
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
            lowercaseTableNames.add(normalizeIdentifier(tableName));
          }
          distinctTableNames = new ArrayList<>(new HashSet<>(lowercaseTableNames));
        }

        int startIndex = 0;
        // Retrieve the tables from the metastore in batches. Some databases like
        // Oracle cannot have over 1000 expressions in a in-list
        while (startIndex < distinctTableNames.size()) {
          int endIndex = Math.min(startIndex + tableBatchSize, distinctTableNames.size());
          tables.addAll(ms.getTableObjectsByName(catName, dbName, distinctTableNames.subList(
                  startIndex, endIndex), projectionsSpec, tablePattern));
          startIndex = endIndex;
        }
      }
      for (Table t : tables) {
        if (t.getParameters() != null && MetaStoreUtils.isInsertOnlyTableParam(t.getParameters())) {
          assertClientHasCapability(capabilities, ClientCapability.INSERT_ONLY_TABLES,
              "insert-only tables", "get_table_req");
        }
      }

      tables = FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tables);
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidOperationException.class, UnknownDBException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_multi_table", tables != null, ex, join(tableNames, ","));
    }
    return tables;
  }

  @Override
  public Materialization get_materialization_invalidation_info(final CreationMetadata cm, final String validTxnList) throws MetaException {
    return getTxnHandler().getMaterializationInvalidationInfo(cm, validTxnList);
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
      tables = FilterUtils.filterTableNamesIfEnabled(
          isServerFilterEnabled, filterHook, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tables);

    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidOperationException.class, UnknownDBException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_table_names_by_filter", tables != null, ex, join(tables, ","));
    }
    return tables;
  }

  private Partition append_partition_common(RawStore ms, String catName, String dbName,
                                            String tableName, List<String> part_vals,
                                            EnvironmentContext envContext)
      throws InvalidObjectException, AlreadyExistsException, MetaException, NoSuchObjectException {

    Partition part = new Partition();
    boolean success = false, madeDir = false;
    Path partLocation = null;
    Table tbl = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;
    try {
      ms.openTransaction();
      part.setCatName(catName);
      part.setDbName(dbName);
      part.setTableName(tableName);
      part.setValues(part_vals);

      MetaStoreServerUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);

      tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName(), null);
      if (tbl == null) {
        throw new InvalidObjectException(
            "Unable to add partition because table or database do not exist");
      }
      if (tbl.getSd().getLocation() == null) {
        throw new MetaException(
            "Cannot append a partition to a view");
      }

      db = get_database_core(catName, dbName);

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

      if (canUpdateStats(tbl)) {
        MetaStoreServerUtils.updatePartitionStatsFast(part, tbl, wh, madeDir, false, envContext, true);
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
          wh.deleteDir(partLocation, true, false, ReplChangeManager.shouldEnableCm(db, tbl));
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
      throw new MetaException("The partition values must not be null or empty.");
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
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("append_partition", ret != null, ex, tableName);
    }
    return ret;
  }

  private static class PartValEqWrapperLite {
    List<String> values;
    String location;

    PartValEqWrapperLite(Partition partition) {
      this.values = partition.isSetValues()? partition.getValues() : null;
      if (partition.getSd() != null) {
        this.location = partition.getSd().getLocation();
      }
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
      String dbName, String tblName, List<Partition> parts, final boolean ifNotExists,
      boolean isSkipColSchemaForPartition) throws TException {
    if (dbName == null || tblName == null) {
      throw new MetaException("The database and table name cannot be null.");
    }

    boolean success = false;
    // Ensures that the list doesn't have dups, and keeps track of directories we have created.
    final Map<PartValEqWrapperLite, Boolean> addedPartitions = new ConcurrentHashMap<>();
    final List<Partition> newParts = new ArrayList<>();
    final List<Partition> existingParts = new ArrayList<>();
    Table tbl = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;

    List<ColumnStatistics> partsColStats = new ArrayList<>(parts.size());
    List<Long> partsWriteIds = new ArrayList<>(parts.size());

    throwUnsupportedExceptionIfRemoteDB(dbName, "add_partitions");

    Lock tableLock = getTableLockFor(dbName, tblName);
    tableLock.lock();
    try {
      ms.openTransaction();
      tbl = ms.getTable(catName, dbName, tblName, null);
      if (tbl == null) {
        throw new InvalidObjectException("Unable to add partitions because "
            + TableName.getQualified(catName, dbName, tblName) +
            " does not exist");
      }
      MTable mTable = getMS().ensureGetMTable(catName, dbName, tblName);
      db = ms.getDatabase(catName, dbName);

      if (!parts.isEmpty()) {
        firePreEvent(new PreAddPartitionEvent(tbl, parts, this));
      }

      Set<PartValEqWrapperLite> partsToAdd = new HashSet<>(parts.size());
      List<Partition> partitionsToAdd = new ArrayList<>(parts.size());
      List<FieldSchema> partitionKeys = tbl.getPartitionKeys();
      for (final Partition part : parts) {
        if(isSkipColSchemaForPartition) {
          part.getSd().setCols(tbl.getSd().getCols());
        }
        // Collect partition column stats to be updated if present. Partition objects passed down
        // here at the time of replication may have statistics in them, which is required to be
        // updated in the metadata. But we don't want it to be part of the Partition object when
        // it's being created or altered, lest it becomes part of the notification event.
        if (part.isSetColStats()) {
          partsColStats.add(part.getColStats());
          part.unsetColStats();
          partsWriteIds.add(part.getWriteId());
        }

        // Iterate through the partitions and validate them. If one of the partitions is
        // incorrect, an exception will be thrown before the threads which create the partition
        // folders are submitted. This way we can be sure that no partition and no partition
        // folder will be created if the list contains an invalid partition.
        if (validatePartition(part, catName, tblName, dbName, partsToAdd, ms, ifNotExists,
            partitionKeys)) {
          partitionsToAdd.add(part);
        } else {
          existingParts.add(part);
        }
      }

      newParts.addAll(createPartitionFolders(partitionsToAdd, tbl, addedPartitions));

      if (!newParts.isEmpty()) {
        ms.addPartitions(catName, dbName, tblName, newParts);
      }

      // Notification is generated for newly created partitions only. The subset of partitions
      // that already exist (existingParts), will not generate notifications.
      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.ADD_PARTITION,
                new AddPartitionEvent(tbl, newParts, true, this));
      }

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

      // Update partition column statistics if available. We need a valid writeId list to
      // update column statistics for a transactional table. But during bootstrap replication,
      // where we use this feature, we do not have a valid writeId list which was used to
      // update the stats. But we know for sure that the writeId associated with the stats was
      // valid then (otherwise stats update would have failed on the source). So, craft a valid
      // transaction list with only that writeId and use it to update the stats.
      int cnt = 0;
      for (ColumnStatistics partColStats: partsColStats) {
        long writeId = partsWriteIds.get(cnt++);
        String validWriteIds = null;
        if (writeId > 0) {
          ValidWriteIdList validWriteIdList =
              new ValidReaderWriteIdList(TableName.getDbTable(tbl.getDbName(),
                  tbl.getTableName()),
                  new long[0], new BitSet(), writeId);
          validWriteIds = validWriteIdList.toString();
        }
        updatePartitonColStatsInternal(tbl, mTable, partColStats, validWriteIds, writeId);
      }

      success = ms.commitTransaction();
    } finally {
      try {
        if (!success) {
          ms.rollbackTransaction();
          cleanupPartitionFolders(addedPartitions, db);

          if (!listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                EventType.ADD_PARTITION,
                new AddPartitionEvent(tbl, parts, false, this),
                null, null, ms);
          }
        }
      } finally {
        tableLock.unlock();
      }
    }

    return newParts;
  }

  private Lock getTableLockFor(String dbName, String tblName) {
    return tablelocks.get(dbName + "." + tblName);
  }

  /**
   * Remove the newly created partition folders. The values in the addedPartitions map indicates
   * whether or not the location of the partition was newly created. If the value is false, the
   * partition folder will not be removed.
   * @param addedPartitions
   * @throws MetaException
   * @throws IllegalArgumentException
   */
  private void cleanupPartitionFolders(final Map<PartValEqWrapperLite, Boolean> addedPartitions,
                                       Database db) throws MetaException, IllegalArgumentException {
    for (Map.Entry<PartValEqWrapperLite, Boolean> e : addedPartitions.entrySet()) {
      if (e.getValue()) {
        // we just created this directory - it's not a case of pre-creation, so we nuke.
        wh.deleteDir(new Path(e.getKey().location), true, db);
      }
    }
  }

  /**
   * Validate a partition before creating it. The validation checks
   * <ul>
   * <li>if the database and table names set in the partition are not null and they are matching
   * with the expected values set in the tblName and dbName parameters.</li>
   * <li>if the partition values are set.</li>
   * <li>if none of the partition values is null.</li>
   * <li>if the partition values are matching with the pattern set in the
   * 'metastore.partition.name.whitelist.pattern' configuration property.</li>
   * <li>if the partition doesn't already exist. If the partition already exists, an exception
   * will be thrown if the ifNotExists parameter is false, otherwise it will be just ignored.</li>
   * <li>if the partsToAdd set doesn't contain the partition. The partsToAdd set contains the
   * partitions which are already validated. If the set contains the current partition, it means
   * that the partition is tried to be added multiple times in the same batch. Please note that
   * the set will be updated with the current partition if the validation was successful.</li>
   * </ul>
   * @param part
   * @param catName
   * @param tblName
   * @param dbName
   * @param partsToAdd
   * @param ms
   * @param ifNotExists
   * @return
   * @throws MetaException
   * @throws TException
   */
  private boolean validatePartition(final Partition part, final String catName,
                                    final String tblName, final String dbName, final Set<PartValEqWrapperLite> partsToAdd,
                                    final RawStore ms, final boolean ifNotExists, List<FieldSchema> partitionKeys) throws MetaException, TException {

    if (part.getDbName() == null || part.getTableName() == null) {
      throw new MetaException("The database and table name must be set in the partition.");
    }

    if (!part.getTableName().equalsIgnoreCase(tblName)
        || !part.getDbName().equalsIgnoreCase(dbName)) {
      String errorMsg = String.format(
          "Partition does not belong to target table %s. It belongs to the table %s.%s : %s",
          TableName.getQualified(catName, dbName, tblName), part.getDbName(),
          part.getTableName(), part.toString());
      throw new MetaException(errorMsg);
    }

    if (part.getValues() == null || part.getValues().isEmpty()) {
      throw new MetaException("The partition values cannot be null or empty.");
    }

    if (part.getValues().contains(null)) {
      throw new MetaException("Partition value cannot be null.");
    }

    boolean shouldAdd = startAddPartition(ms, part, partitionKeys, ifNotExists);
    if (!shouldAdd) {
      LOG.info("Not adding partition {} as it already exists", part);
      return false;
    }

    if (!partsToAdd.add(new PartValEqWrapperLite(part))) {
      // Technically, for ifNotExists case, we could insert one and discard the other
      // because the first one now "exists", but it seems better to report the problem
      // upstream as such a command doesn't make sense.
      throw new MetaException("Duplicate partitions in the list: " + part);
    }
    return true;
  }

  /**
   * Create the location folders for the partitions. For each partition a separate thread will be
   * started to create the folder. The method will wait until all threads are finished and returns
   * the partitions whose folders were created successfully. If an error occurs during the
   * execution of a thread, a MetaException will be thrown.
   * @param partitionsToAdd
   * @param table
   * @param addedPartitions
   * @return
   * @throws MetaException
   */
  private List<Partition> createPartitionFolders(final List<Partition> partitionsToAdd,
                                                 final Table table, final Map<PartValEqWrapperLite, Boolean> addedPartitions)
      throws MetaException {

    final AtomicBoolean failureOccurred = new AtomicBoolean(false);
    final List<Future<Partition>> partFutures = new ArrayList<>(partitionsToAdd.size());
    final Map<PartValEqWrapperLite, Boolean> addedParts = new ConcurrentHashMap<>();

    final UserGroupInformation ugi;
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    for (final Partition partition : partitionsToAdd) {
      initializePartitionParameters(table, partition);

      partFutures.add(threadPool.submit(() -> {
        if (failureOccurred.get()) {
          return null;
        }
        ugi.doAs((PrivilegedExceptionAction<Partition>) () -> {
          try {
            boolean madeDir = createLocationForAddedPartition(table, partition);
            addedParts.put(new PartValEqWrapperLite(partition), madeDir);
            initializeAddedPartition(table, partition, madeDir, null);
          } catch (MetaException e) {
            throw new IOException(e.getMessage(), e);
          }
          return null;
        });
        return partition;
      }));
    }

    List<Partition> newParts = new ArrayList<>(partitionsToAdd.size());
    String errorMessage = null;
    for (Future<Partition> partFuture : partFutures) {
      try {
        Partition part = partFuture.get();
        if (part != null && !failureOccurred.get()) {
          newParts.add(part);
        }
      } catch (ExecutionException e) {
        // If an exception is thrown in the execution of a task, set the failureOccurred flag to
        // true. This flag is visible in the tasks and if its value is true, the partition
        // folders won't be created.
        // Then iterate through the remaining tasks and wait for them to finish. The tasks which
        // are started before the flag got set will then finish creating the partition folders.
        // The tasks which are started after the flag got set, won't create the partition
        // folders, to avoid unnecessary work.
        // This way it is sure that all tasks are finished, when entering the finally part where
        // the partition folders are cleaned up. It won't happen that a task is still running
        // when cleaning up the folders, so it is sure we won't have leftover folders.
        // Canceling the other tasks would be also an option but during testing it turned out
        // that it is not a trustworthy solution to avoid leftover folders.
        failureOccurred.compareAndSet(false, true);
        errorMessage = e.getMessage();
      } catch (InterruptedException e) {
        failureOccurred.compareAndSet(false, true);
        errorMessage = e.getMessage();
        // Restore interruption status of the corresponding thread
        Thread.currentThread().interrupt();
      }
    }

    addedPartitions.putAll(addedParts);
    if (failureOccurred.get()) {
      throw new MetaException(errorMessage);
    }

    return newParts;
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
      throws TException {
    startFunction("add_partitions_req",
        ": db=" + request.getDbName() + " tab=" + request.getTblName());
    AddPartitionsResult result = new AddPartitionsResult();
    if (request.getParts().isEmpty()) {
      return result;
    }
    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    startTableFunction("add_partitions_req", catName, dbName, tblName);

    Exception ex = null;
    try {
      if (!request.isSetCatName()) {
        request.setCatName(catName);
      }
      boolean isColSkippedForPartitions = request.isSkipColumnSchemaForPartition();
      // Make sure all the partitions have the catalog set as well
      request.getParts().forEach(p -> p.setCatName(catName));
      List<Partition> parts = add_partitions_core(getMS(),
          catName, dbName, tblName, request.getParts(),
          request.isIfNotExists(), request.isSkipColumnSchemaForPartition());
      if (request.isNeedResult()) {
        if (isColSkippedForPartitions) {
          if (!parts.isEmpty()) {
            StorageDescriptor sd = parts.get(0).getSd().deepCopy();
            result.setPartitionColSchema(sd.getCols());
          }
          parts.stream().forEach(partition -> partition.getSd().getCols().clear());
        }
        result.setPartitions(parts);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(TException.class).defaultMetaException();
    } finally {
      endFunction("add_partitions_req", ex == null, ex, tblName);
    }
    return result;
  }

  @Override
  public int add_partitions(final List<Partition> parts) throws MetaException,
      InvalidObjectException, AlreadyExistsException {
    if (parts == null) {
      throw new MetaException("Partition list cannot be null.");
    }
    if (parts.isEmpty()) {
      return 0;
    }
    String catName = parts.get(0).isSetCatName() ? parts.get(0).getCatName() : getDefaultCatalog(conf);
    String dbName = parts.get(0).getDbName();
    String tableName = parts.get(0).getTableName();
    startTableFunction("add_partitions", catName, dbName, tableName);

    Integer ret = null;
    Exception ex = null;
    try {
      // Old API assumed all partitions belong to the same table; keep the same assumption
      if (!parts.get(0).isSetCatName()) {
        parts.forEach(p -> p.setCatName(catName));
      }
      ret = add_partitions_core(getMS(), catName, dbName, tableName, parts, false, false).size();
      assert ret == parts.size();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("add_partitions", ret != null, ex, tableName);
    }
    return ret;
  }

  @Override
  public int add_partitions_pspec(final List<PartitionSpec> partSpecs)
      throws TException {
    if (partSpecs.isEmpty()) {
      return 0;
    }

    String catName = partSpecs.get(0).isSetCatName() ? partSpecs.get(0).getCatName() : getDefaultCatalog(conf);
    String dbName = partSpecs.get(0).getDbName();
    String tableName = partSpecs.get(0).getTableName();
    startTableFunction("add_partitions_pspec", catName, dbName, tableName);

    Integer ret = null;
    Exception ex = null;
    try {
      // If the catalog name isn't set, we need to go through and set it.
      if (!partSpecs.get(0).isSetCatName()) {
        partSpecs.forEach(ps -> ps.setCatName(catName));
      }
      dbName = normalizeIdentifier(dbName);
      tableName = normalizeIdentifier(tableName);

      PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(partSpecs);
      final PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpecProxy
              .getPartitionIterator();
      List<Partition> partitionsToAdd = new ArrayList<>(partitionSpecProxy.size());
      while (partitionIterator.hasNext()) {
        final Partition part = partitionIterator.getCurrent();
        // Normalize dbName and tblName of each part
        // to follow the case-insensitive behavior of replaced add_partitions_pspec_core
        part.setDbName(normalizeIdentifier(part.getDbName()));
        part.setTableName(normalizeIdentifier(part.getTableName()));

        partitionsToAdd.add(part);
        partitionIterator.next();
      }
      ret = add_partitions_core(getMS(), catName, dbName, tableName, partitionsToAdd, false, false).size();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
    } finally {
      endFunction("add_partitions_pspec", ret != null, ex, tableName);
    }
    return ret;
  }

  private boolean startAddPartition(
      RawStore ms, Partition part, List<FieldSchema> partitionKeys, boolean ifNotExists)
      throws TException {
    MetaStoreServerUtils.validatePartitionNameCharacters(part.getValues(),
        partitionValidationPattern);
    boolean doesExist = ms.doesPartitionExist(part.getCatName(),
        part.getDbName(), part.getTableName(), partitionKeys, part.getValues());
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

  /**
   * Verify if update stats while altering partition(s)
   * For the following three cases HMS will not update partition stats
   * 1) Table property 'DO_NOT_UPDATE_STATS' = True
   * 2) HMS configuration property 'STATS_AUTO_GATHER' = False
   * 3) Is View
   */
  private boolean canUpdateStats(Table tbl) {
    Map<String,String> tblParams = tbl.getParameters();
    boolean updateStatsTbl = true;
    if ((tblParams != null) && tblParams.containsKey(StatsSetupConst.DO_NOT_UPDATE_STATS)) {
      updateStatsTbl = !Boolean.valueOf(tblParams.get(StatsSetupConst.DO_NOT_UPDATE_STATS));
    }
    if (!MetastoreConf.getBoolVar(conf, ConfVars.STATS_AUTO_GATHER) ||
        MetaStoreUtils.isView(tbl) ||
        !updateStatsTbl) {
      return false;
    }
    return true;
  }

  private void initializeAddedPartition(final Table tbl, final Partition part, boolean madeDir,
                                        EnvironmentContext environmentContext) throws MetaException {
    initializeAddedPartition(tbl,
        new PartitionSpecProxy.SimplePartitionWrapperIterator(part), madeDir, environmentContext);
  }

  private void initializeAddedPartition(
      final Table tbl, final PartitionSpecProxy.PartitionIterator part, boolean madeDir,
      EnvironmentContext environmentContext) throws MetaException {
    if (canUpdateStats(tbl)) {
      MetaStoreServerUtils.updatePartitionStatsFast(part, tbl, wh, madeDir,
          false, environmentContext, true);
    }

    // set create time
    long time = System.currentTimeMillis() / 1000;
    part.setCreateTime((int) time);
    if (part.getParameters() == null ||
        part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      part.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
    }
  }

  private void initializePartitionParameters(final Table tbl, final Partition part)
      throws MetaException {
    initializePartitionParameters(tbl,
        new PartitionSpecProxy.SimplePartitionWrapperIterator(part));
  }

  private void initializePartitionParameters(final Table tbl,
                                             final PartitionSpecProxy.PartitionIterator part) throws MetaException {

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
    if (!part.isSetCatName()) {
      part.setCatName(getDefaultCatalog(conf));
    }
    try {
      ms.openTransaction();
      tbl = ms.getTable(part.getCatName(), part.getDbName(), part.getTableName(), null);
      if (tbl == null) {
        throw new InvalidObjectException(
            "Unable to add partition because table or database do not exist");
      }

      firePreEvent(new PreAddPartitionEvent(tbl, part, this));

      if (part.getValues() == null || part.getValues().isEmpty()) {
        throw new MetaException("The partition values cannot be null or empty.");
      }
      boolean shouldAdd = startAddPartition(ms, part, tbl.getPartitionKeys(), false);
      assert shouldAdd; // start would throw if it already existed here
      boolean madeDir = createLocationForAddedPartition(tbl, part);
      try {
        initializeAddedPartition(tbl, part, madeDir, envContext);
        initializePartitionParameters(tbl, part);
        success = ms.addPartition(part);
      } finally {
        if (!success && madeDir) {
          wh.deleteDir(new Path(part.getSd().getLocation()), true, false,
              ReplChangeManager.shouldEnableCm(ms.getDatabase(part.getCatName(), part.getDbName()), tbl));
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
    if (part == null) {
      throw new MetaException("Partition cannot be null.");
    }
    startTableFunction("add_partition",
        part.getCatName(), part.getDbName(), part.getTableName());
    Partition ret = null;
    Exception ex = null;
    try {
      ret = add_partition_core(getMS(), part, envContext);
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, InvalidObjectException.class, AlreadyExistsException.class)
          .defaultMetaException();
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
        ms.getTable(
            parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME], destTableName, null);
    if (destinationTable == null) {
      throw new MetaException( "The destination table " +
          TableName.getQualified(parsedDestDbName[CAT_NAME],
              parsedDestDbName[DB_NAME], destTableName) + " not found");
    }
    Table sourceTable =
        ms.getTable(
            parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME], sourceTableName, null);
    if (sourceTable == null) {
      throw new MetaException("The source table " +
          TableName.getQualified(parsedSourceDbName[CAT_NAME],
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

    Database srcDb = ms.getDatabase(parsedSourceDbName[CAT_NAME], parsedSourceDbName[DB_NAME]);
    Database destDb = ms.getDatabase(parsedDestDbName[CAT_NAME], parsedDestDbName[DB_NAME]);
    if (!HiveMetaStore.isRenameAllowed(srcDb, destDb)) {
      throw new MetaException("Exchange partition not allowed for " +
          TableName.getQualified(parsedSourceDbName[CAT_NAME],
              parsedSourceDbName[DB_NAME], sourceTableName) + " Dest db : " + destDbName);
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
            Warehouse.makePartName(sourceTable.getPartitionKeys(), partition.getValues()));
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

  private boolean drop_partition_common(RawStore ms, String catName, String db_name, String tbl_name, 
      List<String> part_vals, boolean deleteData, final EnvironmentContext envContext)
      throws MetaException, NoSuchObjectException, IOException, InvalidObjectException, InvalidInputException {
    Path partPath = null;
    boolean isArchived = false;
    Path archiveParentDir = null;
    boolean success = false;

    Table tbl = null;
    Partition part = null;
    boolean mustPurge = false;
    boolean tableDataShouldBeDeleted = false;
    long writeId = 0;
    
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    boolean needsCm = false;

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
      GetTableRequest request = new GetTableRequest(db_name,tbl_name);
      request.setCatName(catName);
      tbl = get_table_core(request);
      firePreEvent(new PreDropPartitionEvent(tbl, part, deleteData, this));

      tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(tbl, deleteData);
      mustPurge = isMustPurge(envContext, tbl);
      writeId = getWriteId(envContext);
            
      if (part == null) {
        throw new NoSuchObjectException("Partition doesn't exist. " + part_vals);
      }
      isArchived = MetaStoreUtils.isArchived(part);
      if (tableDataShouldBeDeleted && isArchived) {
        archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
        verifyIsWritablePath(archiveParentDir);
      }

      if (tableDataShouldBeDeleted && (part.getSd() != null) && (part.getSd().getLocation() != null)) {
        partPath = new Path(part.getSd().getLocation());
        verifyIsWritablePath(partPath);
      }

      String partName = Warehouse.makePartName(tbl.getPartitionKeys(), part_vals);
      ms.dropPartition(catName, db_name, tbl_name, partName);

      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses =
            MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                EventType.DROP_PARTITION,
                new DropPartitionEvent(tbl, part, true, deleteData, this),
                envContext);
      }
      needsCm = ReplChangeManager.shouldEnableCm(ms.getDatabase(catName, db_name), tbl);
      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (tableDataShouldBeDeleted && (partPath != null || archiveParentDir != null)) {
        LOG.info(mustPurge ?
          "dropPartition() will purge " + partPath + " directly, skipping trash." :
          "dropPartition() will move " + partPath + " to trash-directory.");
          
        // Archived partitions have har:/to_har_file as their location.
        // The original directory was saved in params
        if (isArchived) {
          wh.deleteDir(archiveParentDir, true, mustPurge, needsCm);
        } else {
          wh.deleteDir(partPath, true, mustPurge, needsCm);
          deleteParentRecursive(partPath.getParent(), part_vals.size() - 1, mustPurge, needsCm);
        }
        // ok even if the data is not deleted
      } else if (TxnUtils.isTransactionalTable(tbl) && writeId > 0) {
        addTruncateBaseFile(partPath, writeId, conf, DataFormat.DROPPED);
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

  static boolean isMustPurge(EnvironmentContext envContext, Table tbl) {
    // Data needs deletion. Check if trash may be skipped.
    // Trash may be skipped iff:
    //  1. deleteData == true, obviously.
    //  2. tbl is external.
    //  3. Either
    //    3.1. User has specified PURGE from the commandline, and if not,
    //    3.2. User has set the table to auto-purge.
    return (envContext != null && envContext.getProperties() != null
            && Boolean.parseBoolean(envContext.getProperties().get("ifPurge")))
        || MetaStoreUtils.isSkipTrash(tbl.getParameters());
  }

  static long getWriteId(EnvironmentContext context){
    return Optional.ofNullable(context)
      .map(EnvironmentContext::getProperties)
      .map(prop -> prop.get(hive_metastoreConstants.WRITE_ID))
      .map(Long::parseLong)
      .orElse(0L);
  }

  private void throwUnsupportedExceptionIfRemoteDB(String dbName, String operationName) throws MetaException {
    if (isDatabaseRemote(dbName)) {
      throw new MetaException("Operation " + operationName + " not supported for REMOTE database " + dbName);
    }
  }

  private boolean isDatabaseRemote(String name) {
    try {
      String[] parsedDbName = parseDbName(name, conf);
      Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      if (db != null && db.getType() != null && db.getType() == DatabaseType.REMOTE) {
        return true;
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge, boolean needRecycle)
      throws IOException, MetaException {
    if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmptyDir(parent)) {
      wh.deleteDir(parent, true, mustPurge, needRecycle);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge, needRecycle);
    }
  }

  @Override
  public boolean drop_partition(final String db_name, final String tbl_name,
                                final List<String> part_vals, final boolean deleteData)
      throws TException {
    return drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
        null);
  }

  /** Stores a path and its size. */
  private static class PathAndDepth implements Comparable<PathAndDepth> {
    final Path path;
    final int depth;

    public PathAndDepth(Path path, int depth) {
      this.path = path;
      this.depth = depth;
    }

    @Override
    public int hashCode() {
      return Objects.hash(path.hashCode(), depth);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PathAndDepth that = (PathAndDepth) o;
      return depth == that.depth && Objects.equals(path, that.path);
    }

    /** The largest {@code depth} is processed first in a {@link PriorityQueue}. */
    @Override
    public int compareTo(PathAndDepth o) {
      return o.depth - depth;
    }
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

    List<PathAndDepth> dirsToDelete = new ArrayList<>();
    List<Path> archToDelete = new ArrayList<>();
    EnvironmentContext envContext = 
        request.isSetEnvironmentContext() ? request.getEnvironmentContext() : null;
    boolean success = false;
    
    Table tbl = null;
    List<Partition> parts = null;
    boolean mustPurge = false;
    boolean tableDataShouldBeDeleted = false;
    long writeId = 0;
    
    Map<String, String> transactionalListenerResponses = null;
    boolean needsCm = false;

    try {
      ms.openTransaction();
      // We need Partition-s for firing events and for result; DN needs MPartition-s to drop.
      // Great... Maybe we could bypass fetching MPartitions by issuing direct SQL deletes.
      tbl = get_table_core(catName, dbName, tblName);
      mustPurge = isMustPurge(envContext, tbl);
      tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(tbl, deleteData);
      writeId = getWriteId(envContext);
      
      boolean hasMissingParts = false;
      RequestPartsSpec spec = request.getParts();
      List<String> partNames = null;
      
      if (spec.isSetExprs()) {
        // Dropping by expressions.
        parts = new ArrayList<>(spec.getExprs().size());
        for (DropPartitionsExpr expr : spec.getExprs()) {
          List<Partition> result = new ArrayList<>();
          boolean hasUnknown = ms.getPartitionsByExpr(catName, dbName, tblName, result,
              new GetPartitionsArgs.GetPartitionsArgsBuilder()
                  .expr(expr.getExpr()).skipColumnSchemaForPartition(request.isSkipColumnSchemaForPartition())
                  .build());
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
          if (result.isEmpty()) {
            hasMissingParts = true;
            if (!ifExists) {
              // fail-fast for missing partition expr
              break;
            }
          }
          parts.addAll(result);
        }
      } else if (spec.isSetNames()) {
        partNames = spec.getNames();
        parts = ms.getPartitionsByNames(catName, dbName, tblName,
            new GetPartitionsArgs.GetPartitionsArgsBuilder()
                .partNames(partNames).skipColumnSchemaForPartition(request.isSkipColumnSchemaForPartition())
                .build());
        hasMissingParts = (parts.size() != partNames.size());
      } else {
        throw new MetaException("Partition spec is not set");
      }

      if (hasMissingParts && !ifExists) {
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
        if (tableDataShouldBeDeleted && MetaStoreUtils.isArchived(part)) {
          Path archiveParentDir = MetaStoreUtils.getOriginalLocation(part);
          verifyIsWritablePath(archiveParentDir);
          archToDelete.add(archiveParentDir);
        }
        if (tableDataShouldBeDeleted && (part.getSd() != null) && (part.getSd().getLocation() != null)) {
          Path partPath = new Path(part.getSd().getLocation());
          verifyIsWritablePath(partPath);
          dirsToDelete.add(new PathAndDepth(partPath, part.getValues().size()));
        }
      }

      ms.dropPartitions(catName, dbName, tblName, partNames);
      if (!parts.isEmpty() && !transactionalListeners.isEmpty()) {
        transactionalListenerResponses = MetaStoreListenerNotifier
            .notifyEvent(transactionalListeners, EventType.DROP_PARTITION,
                new DropPartitionEvent(tbl, parts, true, deleteData, this), envContext);
      }
      success = ms.commitTransaction();
      needsCm = ReplChangeManager.shouldEnableCm(ms.getDatabase(catName, dbName), tbl);
      
      DropPartitionsResult result = new DropPartitionsResult();
      if (needResult) {
        result.setPartitions(parts);
      }
      return result;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      } else if (tableDataShouldBeDeleted) {
        LOG.info(mustPurge ?
            "dropPartition() will purge partition-directories directly, skipping trash."
            :  "dropPartition() will move partition-directories to trash-directory.");
        // Archived partitions have har:/to_har_file as their location.
        // The original directory was saved in params
        for (Path path : archToDelete) {
          wh.deleteDir(path, true, mustPurge, needsCm);
        }

        // Uses a priority queue to delete the parents of deleted directories if empty.
        // Parents with the deepest path are always processed first. It guarantees that the emptiness
        // of a parent won't be changed once it has been processed. So duplicated processing can be
        // avoided.
        PriorityQueue<PathAndDepth> parentsToDelete = new PriorityQueue<>();
        for (PathAndDepth p : dirsToDelete) {
          wh.deleteDir(p.path, true, mustPurge, needsCm);
          addParentForDel(parentsToDelete, p);
        }

        HashSet<PathAndDepth> processed = new HashSet<>();
        while (!parentsToDelete.isEmpty()) {
          try {
            PathAndDepth p = parentsToDelete.poll();
            if (processed.contains(p)) {
              continue;
            }
            processed.add(p);

            Path path = p.path;
            if (wh.isWritable(path) && wh.isEmptyDir(path)) {
              wh.deleteDir(path, true, mustPurge, needsCm);
              addParentForDel(parentsToDelete, p);
            }
          } catch (IOException ex) {
            LOG.warn("Error from recursive parent deletion", ex);
            throw new MetaException("Failed to delete parent: " + ex.getMessage());
          }
        }
      } else if (TxnUtils.isTransactionalTable(tbl) && writeId > 0) {
        for (Partition part : parts) {
          if ((part.getSd() != null) && (part.getSd().getLocation() != null)) {
            Path partPath = new Path(part.getSd().getLocation());
            verifyIsWritablePath(partPath);
            
            addTruncateBaseFile(partPath, writeId, conf, DataFormat.DROPPED);
          }
        }
      }

      if (parts != null) {
        if (!parts.isEmpty() && !listeners.isEmpty()) {
            MetaStoreListenerNotifier.notifyEvent(listeners,
                EventType.DROP_PARTITION,
                new DropPartitionEvent(tbl, parts, success, deleteData, this),
                envContext,
                transactionalListenerResponses, ms);
        }
      }
    }
  }

  private static void addParentForDel(PriorityQueue<PathAndDepth> parentsToDelete, PathAndDepth p) {
    Path parent = p.path.getParent();
    if (parent != null && p.depth - 1 > 0) {
      parentsToDelete.add(new PathAndDepth(parent, p.depth - 1));
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
    } catch (Exception e) {
      ex = e;
      handleException(e).convertIfInstance(IOException.class, MetaException.class)
          .rethrowException(e);
    } finally {
      endFunction("drop_partition", ret, ex, tbl_name);
    }
    return ret;

  }

  /**
   * Use {@link #get_partition_req(GetPartitionRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public Partition get_partition(final String db_name, final String tbl_name,
                                 final List<String> part_vals) throws MetaException, NoSuchObjectException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partition", parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
        tbl_name, part_vals);

    Partition ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      ret = getMS().getPartition(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals);
      ret = FilterUtils.filterPartitionIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, NoSuchObjectException.class).defaultMetaException();
    } finally {
      endFunction("get_partition", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  public GetPartitionResponse get_partition_req(GetPartitionRequest req)
      throws MetaException, NoSuchObjectException, TException {
    // TODO Move the logic from get_partition to here, as that method is getting deprecated
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    Partition p = get_partition(dbName, req.getTblName(), req.getPartVals());
    GetPartitionResponse res = new GetPartitionResponse();
    res.setPartition(p);
    return res;
  }

  /**
   * Fire a pre-event for read table operation, if there are any
   * pre-event listeners registered
   */
  private void fireReadTablePreEvent(String catName, String dbName, String tblName)
      throws MetaException, NoSuchObjectException {
    if(preListeners.size() > 0) {
      Supplier<Table> tableSupplier = Suppliers.memoize(new Supplier<Table>() {
        @Override public Table get() {
          try {
            Table t = getMS().getTable(catName, dbName, tblName, null);
            if (t == null) {
              throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tblName)
                  + " table not found");
            }
            return t;
          } catch(MetaException | NoSuchObjectException e) {
            throw new RuntimeException(e);
          }
        }
      });
      firePreEvent(new PreReadTableEvent(tableSupplier, this));
    }
  }

  /**
   * filters out the table meta for which read database access is not granted
   * @param catName catalog name
   * @param tableMetas list of table metas
   * @return filtered list of table metas
   * @throws RuntimeException
   * @throws NoSuchObjectException
   */
  private List<TableMeta> filterReadableTables(String catName, List<TableMeta> tableMetas)
          throws RuntimeException, NoSuchObjectException {
    List<TableMeta> finalT = new ArrayList<>();
    Map<String, Boolean> databaseNames = new HashMap();
    for (TableMeta tableMeta : tableMetas) {
      String fullDbName = prependCatalogToDbName(catName, tableMeta.getDbName(), conf);
      if (databaseNames.get(fullDbName) == null) {
        boolean isExecptionThrown = false;
        try {
          fireReadDatabasePreEvent(fullDbName);
        } catch (MetaException e) {
          isExecptionThrown = true;
        }
        databaseNames.put(fullDbName, isExecptionThrown);
      }
      if (!databaseNames.get(fullDbName)) {
        finalT.add(tableMeta);
      }
    }
    return finalT;
  }

  /**
   * Fire a pre-event for read database operation, if there are any
   * pre-event listeners registered
   */
  private void fireReadDatabasePreEvent(final String name)
          throws MetaException, RuntimeException, NoSuchObjectException {
    if(preListeners.size() > 0) {
      String[] parsedDbName = parseDbName(name, conf);
      Database db = null;
      try {
        db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        if (db == null) {
          throw new NoSuchObjectException("Database: " + name + " not found");
        }
      } catch(MetaException | NoSuchObjectException e) {
        throw new RuntimeException(e);
      }
      firePreEvent(new PreReadDatabaseEvent(db, this));
    }
  }

  @Override
  @Deprecated
  public Partition get_partition_with_auth(final String db_name,
                                           final String tbl_name, final List<String> part_vals,
                                           final String user_name, final List<String> group_names)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startFunction("get_partition_with_auth",
        " : tbl=" + TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name)
            + samplePartitionValues(part_vals) + getGroupsCountAndUsername(user_name,group_names));
    fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    Partition ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);

      ret = getMS().getPartitionWithAuth(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, part_vals, user_name, group_names);
      ret = FilterUtils.filterPartitionIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      handleException(e).convertIfInstance(InvalidObjectException.class, NoSuchObjectException.class)
          .rethrowException(e);
    } finally {
      endFunction("get_partition_with_auth", ret != null, ex, tbl_name);
    }
    return ret;
  }

  /**
   * Use {@link #get_partitions_req(PartitionsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions(final String db_name, final String tbl_name,
                                        final short max_parts) throws NoSuchObjectException, MetaException {
    return get_partitions(db_name, tbl_name,
        new GetPartitionsArgs.GetPartitionsArgsBuilder().max(max_parts).build());
  }

  private List<Partition> get_partitions(final String db_name, final String tbl_name,
    GetPartitionsArgs args) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startTableFunction("get_partitions", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    List<Partition> ret = null;
    Exception ex = null;
    try {
      checkLimitNumberOfPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, NO_FILTER_STRING, args.getMax());

      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);

      ret = getMS().getPartitions(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_partitions", ret != null, ex, tbl_name);
    }
    return ret;

  }

  @Override
  public PartitionsResponse get_partitions_req(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<Partition> partitions = get_partitions(dbName, req.getTblName(),
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .max(req.getMaxParts())
            .includeParamKeyPattern(req.getIncludeParamKeyPattern())
            .excludeParamKeyPattern(req.getExcludeParamKeyPattern())
            .skipColumnSchemaForPartition(req.isSkipColumnSchemaForPartition())
            .build());
    PartitionsResponse res = new PartitionsResponse();
    res.setPartitions(partitions);
    return res;
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_with_auth(final String dbName,
      final String tblName, final short maxParts, final String userName,
      final List<String> groupNames) throws TException {
    return get_partitions_ps_with_auth(dbName, tblName,
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .max(maxParts).userName(userName).groupNames(groupNames)
            .build());
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

  private void checkLimitNumberOfPartitionsByPs(String catName, String dbName, String tblName,
                                                List<String> partVals, int maxParts)
          throws TException {
    if (isPartitionLimitEnabled()) {
      checkLimitNumberOfPartitions(tblName, getNumPartitionsByPs(catName, dbName, tblName,
              partVals), maxParts);
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
  @Deprecated
  public List<PartitionSpec> get_partitions_pspec(final String db_name, final String tbl_name, final int max_parts)
      throws NoSuchObjectException, MetaException  {

    String[] parsedDbName = parseDbName(db_name, conf);
    String catName = parsedDbName[CAT_NAME];
    String dbName = parsedDbName[DB_NAME];
    String tableName = tbl_name.toLowerCase();

    startPartitionFunction("get_partitions_pspec", catName, dbName, tableName, max_parts);

    List<PartitionSpec> partitionSpecs = null;
    try {
      Table table = get_table_core(catName, dbName, tableName);
      // get_partitions will parse out the catalog and db names itself
      List<Partition> partitions = get_partitions(db_name, tableName, (short) max_parts);

      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      }
      else {
        PartitionSpec pSpec = new PartitionSpec();
        pSpec.setPartitionList(new PartitionListComposingSpec(partitions));
        pSpec.setCatName(normalizeIdentifier(catName));
        pSpec.setDbName(normalizeIdentifier(dbName));
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

  @Override
  public GetPartitionsResponse get_partitions_with_specs(GetPartitionsRequest request)
      throws MetaException, TException {
    String catName = null;
    if (request.isSetCatName()) {
      catName = request.getCatName();
    }
    String[] parsedDbName = parseDbName(request.getDbName(), conf);
    String tableName = request.getTblName();
    if (catName == null) {
      // if catName is not provided in the request use the catName parsed from the dbName
      catName = parsedDbName[CAT_NAME];
    }
    startTableFunction("get_partitions_with_specs", catName, parsedDbName[DB_NAME],
        tableName);
    GetPartitionsResponse response = null;
    Exception ex = null;
    try {
      Table table = get_table_core(catName, parsedDbName[DB_NAME], tableName);
      List<Partition> partitions = getMS()
          .getPartitionSpecsByFilterAndProjection(table, request.getProjectionSpec(),
              request.getFilterSpec());
      List<String> processorCapabilities = request.getProcessorCapabilities();
      String processorId = request.getProcessorIdentifier();
      if (processorCapabilities == null || processorCapabilities.size() == 0 ||
          processorCapabilities.contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with " + processorId);
      } else {
        if (transformer != null) {
          partitions = transformer.transformPartitions(partitions, table, processorCapabilities, processorId);
        }
      }
      List<PartitionSpec> partitionSpecs =
          MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      response = new GetPartitionsResponse();
      response.setPartitionSpec(partitionSpecs);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_with_specs", response != null, ex, tableName);
    }
    return response;
  }

  private static boolean is_partition_spec_grouping_enabled(Table table) {

    Map<String, String> parameters = table.getParameters();
    return parameters.containsKey("hive.hcatalog.partition.spec.grouping.enabled")
        && parameters.get("hive.hcatalog.partition.spec.grouping.enabled").equalsIgnoreCase("true");
  }

  @Override
  @Deprecated
  public List<String> get_partition_names(final String db_name, final String tbl_name,
                                          final short max_parts) throws NoSuchObjectException, MetaException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partition_names", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, max_parts);
    fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    List<String> ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      ret = getMS().listPartitionNames(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
          max_parts);
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_partition_names", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  public PartitionValuesResponse get_partition_values(PartitionValuesRequest request)
      throws MetaException {
    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    String dbName = request.getDbName();
    String tblName = request.getTblName();
    long maxParts = request.getMaxParts();
    String filter = request.isSetFilter() ? request.getFilter() : "";
    startPartitionFunction("get_partition_values", catName, dbName, tblName, (int) maxParts, filter);
    try {
      authorizeTableForPartitionMetadata(catName, dbName, tblName);

      // This is serious black magic, as the following 2 lines do nothing AFAICT but without them
      // the subsequent call to listPartitionValues fails.
      List<FieldSchema> partCols = new ArrayList<FieldSchema>();
      partCols.add(request.getPartitionKeys().get(0));
      return getMS().listPartitionValues(catName, dbName, tblName, request.getPartitionKeys(),
          request.isApplyDistinct(), request.getFilter(), request.isAscending(),
          request.getPartitionOrder(), request.getMaxParts());
    } catch (NoSuchObjectException e) {
      LOG.error(String.format("Unable to get partition for %s.%s.%s", catName, dbName, tblName), e);
      throw new MetaException(e.getMessage());
    }
  }

  @Deprecated
  @Override
  public void alter_partition(final String db_name, final String tbl_name,
                              final Partition new_part)
      throws TException {
    rename_partition(db_name, tbl_name, null, new_part);
  }

  @Deprecated
  @Override
  public void alter_partition_with_environment_context(final String dbName,
                                                       final String tableName, final Partition newPartition,
                                                       final EnvironmentContext envContext)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, null,
        newPartition, envContext, null);
  }

  @Deprecated
  @Override
  public void rename_partition(final String db_name, final String tbl_name,
                               final List<String> part_vals, final Partition new_part)
      throws TException {
    // Call alter_partition_core without an environment context.
    String[] parsedDbName = parseDbName(db_name, conf);
    alter_partition_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_vals, new_part,
        null, null);
  }

  @Override
  public RenamePartitionResponse rename_partition_req(RenamePartitionRequest req) throws TException {
    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(RENAME_PARTITION_MAKE_COPY, String.valueOf(req.isClonePart()));
    context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(req.getTxnId()));
    
    alter_partition_core(req.getCatName(), req.getDbName(), req.getTableName(), req.getPartVals(),
        req.getNewPart(), context, req.getValidWriteIdList());
    return new RenamePartitionResponse();
  };

  private void alter_partition_core(String catName, String db_name, String tbl_name,
                                List<String> part_vals, Partition new_part, EnvironmentContext envContext,
                                String validWriteIds) throws TException {
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
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = wh.getDnsPath(new Path(newLocation));
        new_part.getSd().setLocation(tblPath.toString());
      }
    }

    // Make sure the new partition has the catalog value set
    if (!new_part.isSetCatName()) {
      new_part.setCatName(catName);
    }

    Partition oldPart = null;
    Exception ex = null;
    try {
      Table table = getMS().getTable(catName, db_name, tbl_name, null);

      firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, table, part_vals, new_part, this));
      if (part_vals != null && !part_vals.isEmpty()) {
        MetaStoreServerUtils.validatePartitionNameCharacters(new_part.getValues(),
            partitionValidationPattern);
      }

      oldPart = alterHandler.alterPartition(getMS(), wh, catName, db_name, tbl_name,
          part_vals, new_part, envContext, this, validWriteIds);

      if (!listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventType.ALTER_PARTITION,
            new AlterPartitionEvent(oldPart, new_part, table, false,
                true, new_part.getWriteId(), this),
            envContext);
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(InvalidObjectException.class, InvalidOperationException.class)
          .convertIfInstance(AlreadyExistsException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      endFunction("alter_partition", oldPart != null, ex, tbl_name);
    }
  }

  @Override
  public void alter_partitions(final String db_name, final String tbl_name,
                               final List<Partition> new_parts)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    alter_partitions_with_environment_context(o[0], o[1],
        tbl_name, new_parts, null, null, -1);
  }

  @Override
  public AlterPartitionsResponse alter_partitions_req(AlterPartitionsRequest req) throws TException {
    alter_partitions_with_environment_context(req.getCatName(),
        req.getDbName(), req.getTableName(), req.getPartitions(), req.getEnvironmentContext(),
        req.isSetValidWriteIdList() ? req.getValidWriteIdList() : null,
        req.isSetWriteId() ? req.getWriteId() : -1);
    return new AlterPartitionsResponse();
  }

  // The old API we are keeping for backward compat. Not used within Hive.
  @Deprecated
  @Override
  public void alter_partitions_with_environment_context(final String db_name, final String tbl_name,
                                                        final List<Partition> new_parts, EnvironmentContext environmentContext)
      throws TException {
    String[] o = parseDbName(db_name, conf);
    alter_partitions_with_environment_context(o[0], o[1], tbl_name, new_parts, environmentContext,
        null, -1);
  }

  private void alter_partitions_with_environment_context(String catName, String db_name, final String tbl_name,
      final List<Partition> new_parts, EnvironmentContext environmentContext,
      String writeIdList, long writeId)
      throws TException {
    if (environmentContext == null) {
      environmentContext = new EnvironmentContext();
    }
    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }

    startTableFunction("alter_partitions", catName, db_name, tbl_name);

    if (LOG.isInfoEnabled()) {
      for (Partition tmpPart : new_parts) {
        LOG.info("New partition values: catalog: {} database: {} table: {} partition: {}",
                catName, db_name, tbl_name, tmpPart.getValues());
      }
    }
    // all partitions are altered atomically
    // all prehooks are fired together followed by all post hooks
    List<Partition> oldParts = null;
    Exception ex = null;
    Lock tableLock = getTableLockFor(db_name, tbl_name);
    tableLock.lock();
    try {

      Table table = null;
      table = getMS().getTable(catName, db_name, tbl_name,  null);

      for (Partition tmpPart : new_parts) {
        // Make sure the catalog name is set in the new partition
        if (!tmpPart.isSetCatName()) {
          tmpPart.setCatName(getDefaultCatalog(conf));
        }
        if (tmpPart.getSd() != null && tmpPart.getSd().getCols() != null && tmpPart.getSd().getCols().isEmpty()) {
          tmpPart.getSd().setCols(table.getSd().getCols());
        }
        firePreEvent(new PreAlterPartitionEvent(db_name, tbl_name, table, null, tmpPart, this));
      }
      oldParts = alterHandler.alterPartitions(getMS(), wh,
          catName, db_name, tbl_name, new_parts, environmentContext, writeIdList, writeId, this);
      Iterator<Partition> olditr = oldParts.iterator();

      for (Partition tmpPart : new_parts) {
        Partition oldTmpPart;
        if (olditr.hasNext()) {
          oldTmpPart = olditr.next();
        }
        else {
          throw new InvalidOperationException("failed to alterpartitions");
        }

        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.ALTER_PARTITION,
              new AlterPartitionEvent(oldTmpPart, tmpPart, table, false,
                  true, writeId, this));
        }
      }
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(InvalidObjectException.class, InvalidOperationException.class)
          .convertIfInstance(AlreadyExistsException.class, InvalidOperationException.class)
          .defaultMetaException();
    } finally {
      tableLock.unlock();
      endFunction("alter_partitions", oldParts != null, ex, tbl_name);
    }
  }

  @Override
  public String getVersion() throws TException {
    String version = MetastoreVersionInfo.getVersion();
    endFunction(startFunction("getVersion"), version != null, null);
    return version;
  }

  @Override
  public void alter_table(final String dbname, final String name,
                          final Table newTable)
      throws InvalidOperationException, MetaException {
    // Do not set an environment context.
    String[] parsedDbName = parseDbName(dbname, conf);
    alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, newTable,
        null, null, null, null, null, null);
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
    alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], name, newTable,
        envContext, null, null, null, null, null);
  }

  @Override
  public AlterTableResponse alter_table_req(AlterTableRequest req)
      throws InvalidOperationException, MetaException, TException {
    alter_table_core(req.getCatName(), req.getDbName(), req.getTableName(),
        req.getTable(), req.getEnvironmentContext(), req.getValidWriteIdList(),
        req.getProcessorCapabilities(), req.getProcessorIdentifier(),
        req.getExpectedParameterKey(), req.getExpectedParameterValue());
    return new AlterTableResponse();
  }

  @Override
  public void alter_table_with_environment_context(final String dbname,
                                                   final String name, final Table newTable,
                                                   final EnvironmentContext envContext)
      throws InvalidOperationException, MetaException {
    String[] parsedDbName = parseDbName(dbname, conf);
    alter_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
        name, newTable, envContext, null, null, null, null, null);
  }

  private void alter_table_core(String catName, String dbname, String name, Table newTable,
                                EnvironmentContext envContext, String validWriteIdList, List<String> processorCapabilities,
                                String processorId, String expectedPropertyKey, String expectedPropertyValue)
          throws InvalidOperationException, MetaException {
    startFunction("alter_table", ": " + TableName.getQualified(catName, dbname, name)
        + " newtbl=" + newTable.getTableName());
    if (envContext == null) {
      envContext = new EnvironmentContext();
    }
    // Set the values to the envContext, so we do not have to change the HiveAlterHandler API
    if (expectedPropertyKey != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_KEY, expectedPropertyKey);
    }
    if (expectedPropertyValue != null) {
      envContext.putToProperties(hive_metastoreConstants.EXPECTED_PARAMETER_VALUE, expectedPropertyValue);
    }

    if (catName == null) {
      catName = getDefaultCatalog(conf);
    }

    // HIVE-25282: Drop/Alter table in REMOTE db should fail
    try {
      Database db = get_database_core(catName, dbname);
      if (db != null && db.getType().equals(DatabaseType.REMOTE)) {
        throw new MetaException("Alter table in REMOTE database " + db.getName() + " is not allowed");
      }
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException("Alter table in REMOTE database is not allowed");
    }

    // Update the time if it hasn't been specified.
    if (newTable.getParameters() == null ||
        newTable.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
      newTable.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
    }

    // Adds the missing scheme/authority for the new table location
    if (newTable.getSd() != null) {
      String newLocation = newTable.getSd().getLocation();
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(newLocation)) {
        Path tblPath = wh.getDnsPath(new Path(newLocation));
        newTable.getSd().setLocation(tblPath.toString());
      }
    }
    // Set the catalog name if it hasn't been set in the new table
    if (!newTable.isSetCatName()) {
      newTable.setCatName(catName);
    }

    boolean success = false;
    Exception ex = null;
    try {
      GetTableRequest request = new GetTableRequest(dbname, name);
      request.setCatName(catName);
      Table oldt = get_table_core(request);
      if (transformer != null) {
        newTable = transformer.transformAlterTable(oldt, newTable, processorCapabilities, processorId);
      }
      firePreEvent(new PreAlterTableEvent(oldt, newTable, this));
      alterHandler.alterTable(getMS(), wh, catName, dbname, name, newTable,
          envContext, this, validWriteIdList);
      success = true;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(MetaException.class, InvalidOperationException.class)
          .convertIfInstance(NoSuchObjectException.class, InvalidOperationException.class)
          .defaultMetaException();
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
      if (isDatabaseRemote(dbname)) {
        Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
      }
    } catch (Exception e) { /* appears we return empty set instead of throwing an exception */ }

    try {
      ret = getMS().getTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern);
      if(ret !=  null && !ret.isEmpty()) {
        List<Table> tableInfo = new ArrayList<>();
        tableInfo = getMS().getTableObjectsByName(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], ret);
        tableInfo = FilterUtils.filterTablesIfEnabled(isServerFilterEnabled, filterHook, tableInfo);// tableInfo object has the owner information of the table which is being passed to FilterUtils.
        ret = new ArrayList<>();
        for (Table tbl : tableInfo) {
          ret.add(tbl.getTableName());
        }
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
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
      ret = getTablesByTypeCore(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], pattern, tableType);
      ret = FilterUtils.filterTableNamesIfEnabled(isServerFilterEnabled, filterHook,
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_tables_by_type", ret != null, ex);
    }
    return ret;
  }

  private List<String> getTablesByTypeCore(final String catName, final String dbname,
                                           final String pattern, final String tableType) throws MetaException {
    startFunction("getTablesByTypeCore", ": catName=" + catName +
        ": db=" + dbname + " pat=" + pattern + ",type=" + tableType);

    List<String> ret = null;
    Exception ex = null;
    Database db = null;
    try {
      db = get_database_core(catName, dbname);
      if (db != null) {
        if (db.getType().equals(DatabaseType.REMOTE)) {
          return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
        }
      }
    } catch (Exception e) { /* ignore */ }

    try {
      ret = getMS().getTables(catName, dbname, pattern, TableType.valueOf(tableType), -1);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("getTablesByTypeCore", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Table> get_all_materialized_view_objects_for_rewriting()
      throws MetaException {
    startFunction("get_all_materialized_view_objects_for_rewriting");

    List<Table> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllMaterializedViewObjectsForRewriting(DEFAULT_CATALOG_NAME);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_materialized_view_objects_for_rewriting", ret != null, ex);
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
      throw newMetaException(e);
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
      if (isDatabaseRemote(dbname)) {
        Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
        return DataConnectorProviderFactory.getDataConnectorProvider(db).getTableNames();
      }
    } catch (Exception e) { /* ignore */ }

    try {
      ret = getMS().getAllTables(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      ret = FilterUtils.filterTableNamesIfEnabled(isServerFilterEnabled, filterHook,
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], ret);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_all_tables", ret != null, ex);
    }
    return ret;
  }

  /**
   * Use {@link #get_fields_req(GetFieldsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<FieldSchema> get_fields(String db, String tableName)
      throws MetaException, UnknownTableException, UnknownDBException {
    return get_fields_with_environment_context(db, tableName, null);
  }

  @Override
  @Deprecated
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
    try {
      try {
        tbl = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], base_table_name);
        firePreEvent(new PreReadTableEvent(tbl, this));
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
      throw handleException(e).throwIfInstance(UnknownTableException.class, MetaException.class).defaultMetaException();
    } finally {
      endFunction("get_fields_with_environment_context", ret != null, ex, tableName);
    }

    return ret;
  }

  @Override
  public GetFieldsResponse get_fields_req(GetFieldsRequest req)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<FieldSchema> fields = get_fields_with_environment_context(
        dbName, req.getTblName(), req.getEnvContext());
    GetFieldsResponse res = new GetFieldsResponse();
    res.setFields(fields);
    return res;
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
   * Use {@link #get_schema_req(GetSchemaRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
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
  @Deprecated
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
      List<FieldSchema> fieldSchemas = get_fields_with_environment_context(db, base_table_name,
          envContext);

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
      throw handleException(e)
          .throwIfInstance(UnknownDBException.class, UnknownTableException.class, MetaException.class)
          .defaultMetaException();
    } finally {
      endFunction("get_schema_with_environment_context", success, ex, tableName);
    }
  }

  @Override
  public GetSchemaResponse get_schema_req(GetSchemaRequest req)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<FieldSchema> fields = get_schema_with_environment_context(
        dbName, req.getTblName(), req.getEnvContext());
    GetSchemaResponse res = new GetSchemaResponse();
    res.setFields(fields);
    return res;
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
        LOG.error("RuntimeException thrown in get_config_value - msg: "
            + e.getMessage() + " cause: " + e.getCause());
      }
      success = true;
      return toReturn;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(TException.class).defaultMetaException();
    } finally {
      endFunction("get_config_value", success, ex);
    }
  }

  public static List<String> getPartValsFromName(Table t, String partName)
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
    Table t = ms.getTable(catName, dbName, tblName,  null);
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
    p = FilterUtils.filterPartitionIfEnabled(isServerFilterEnabled, filterHook, p);

    if (p == null) {
      throw new NoSuchObjectException(TableName.getQualified(catName, db_name, tbl_name)
          + " partition (" + part_name + ") not found");
    }
    return p;
  }

  @Override
  @Deprecated
  public Partition get_partition_by_name(final String db_name, final String tbl_name,
                                         final String part_name) throws TException {

    String[] parsedDbName = parseDbName(db_name, conf);
    startFunction("get_partition_by_name", ": tbl=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name)
        + " part=" + part_name);
    Partition ret = null;
    Exception ex = null;
    try {
      ret = get_partition_by_name_core(getMS(), parsedDbName[CAT_NAME],
          parsedDbName[DB_NAME], tbl_name, part_name);
      ret = FilterUtils.filterPartitionIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
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
        + TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
        tbl_name) + " part=" + part_name);

    Partition ret = null;
    Exception ex = null;
    try {
      RawStore ms = getMS();
      List<String> partVals = getPartValsFromName(ms, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, part_name);
      ret = append_partition_common(ms, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, partVals, env_context);
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(InvalidObjectException.class, AlreadyExistsException.class, MetaException.class)
          .defaultMetaException();
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
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name)
        + " part=" + part_name);

    boolean ret = false;
    Exception ex = null;
    try {
      ret = drop_partition_by_name_core(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, part_name, deleteData, envContext);
    } catch (Exception e) {
      ex = e;
      handleException(e).convertIfInstance(IOException.class, MetaException.class).rethrowException(e);
    } finally {
      endFunction("drop_partition_by_name", ret, ex, tbl_name);
    }

    return ret;
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_ps(final String db_name,
                                           final String tbl_name, final List<String> part_vals,
                                           final short max_parts) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partitions_ps", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, max_parts,
        part_vals);

    List<Partition> ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      // Don't send the parsedDbName, as this method will parse itself.
      ret = get_partitions_ps_with_auth(db_name, tbl_name, new GetPartitionsArgs.GetPartitionsArgsBuilder()
          .part_vals(part_vals).max(max_parts)
          .build());
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_ps", ret != null, ex, tbl_name);
    }

    return ret;
  }

  /**
   * Use {@link #get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<Partition> get_partitions_ps_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts, final String userName,
      final List<String> groupNames) throws TException {
    return get_partitions_ps_with_auth(db_name, tbl_name, new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .part_vals(part_vals).max(max_parts).userName(userName).groupNames(groupNames)
            .build());
  }

  private List<Partition> get_partitions_ps_with_auth(final String db_name,
      final String tbl_name, GetPartitionsArgs args) throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partitions_ps_with_auth", parsedDbName[CAT_NAME],
        parsedDbName[DB_NAME], tbl_name, args.getPart_vals());
    fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    List<Partition> ret = null;
    Exception ex = null;
    try {
      if (args.getPart_vals() != null) {
        checkLimitNumberOfPartitionsByPs(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, args.getPart_vals(), args.getMax());
      } else {
        checkLimitNumberOfPartitionsByFilter(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tbl_name, NO_FILTER_STRING, args.getMax());
      }
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      ret = getMS().listPartitionsPsWithAuth(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
          tbl_name, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      handleException(e).convertIfInstance(InvalidObjectException.class, MetaException.class).rethrowException(e);
    } finally {
      endFunction("get_partitions_ps_with_auth", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  public GetPartitionsPsWithAuthResponse get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<Partition> partitions =
        get_partitions_ps_with_auth(dbName, req.getTblName(), new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .part_vals(req.getPartVals()).max(req.getMaxParts())
            .userName(req.getUserName()).groupNames(req.getGroupNames())
            .skipColumnSchemaForPartition(req.isSkipColumnSchemaForPartition())
            .includeParamKeyPattern(req.getIncludeParamKeyPattern())
            .excludeParamKeyPattern(req.getExcludeParamKeyPattern())
            .partNames(req.getPartNames())
            .build());
    GetPartitionsPsWithAuthResponse res = new GetPartitionsPsWithAuthResponse();
    res.setPartitions(partitions);
    return res;
  }

  /**
   * Use {@link #get_partition_names_ps_req(GetPartitionNamesPsRequest)} ()} instead.
   *
   */
  @Override
  @Deprecated
  public List<String> get_partition_names_ps(final String db_name,
                                             final String tbl_name, final List<String> part_vals, final short max_parts)
      throws TException {
    String[] parsedDbName = parseDbName(db_name, conf);
    startPartitionFunction("get_partitions_names_ps", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
        max_parts, part_vals);
    fireReadTablePreEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
    List<String> ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name);
      ret = getMS().listPartitionNamesPs(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name,
          part_vals, max_parts);
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tbl_name, ret);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_names_ps", ret != null, ex, tbl_name);
    }
    return ret;
  }

  @Override
  public GetPartitionNamesPsResponse get_partition_names_ps_req(GetPartitionNamesPsRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String dbName = MetaStoreUtils.prependCatalogToDbName(req.getCatName(), req.getDbName(), conf);
    List<String> names = get_partition_names_ps(dbName, req.getTblName(), req.getPartValues(),
        req.getMaxParts());
    GetPartitionNamesPsResponse res = new GetPartitionNamesPsResponse();
    res.setNames(names);
    return res;
  }

  @Override
  public List<String> get_partition_names_req(PartitionsByExprRequest req)
      throws MetaException, NoSuchObjectException, TException {
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String dbName = req.getDbName(), tblName = req.getTblName();
    startTableFunction("get_partition_names_req", catName,
        dbName, tblName);
    fireReadTablePreEvent(catName, dbName, tblName);
    List<String> ret = null;
    Exception ex = null;
    try {
      authorizeTableForPartitionMetadata(catName, dbName, tblName);
      ret = getMS().listPartitionNames(catName, dbName, tblName,
          req.getDefaultPartitionName(), req.getExpr(), req.getOrder(), req.getMaxParts());
      ret = FilterUtils.filterPartitionNamesIfEnabled(isServerFilterEnabled,
          filterHook, catName, dbName, tblName, ret);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partition_names_req", ret != null, ex, tblName);
    }
    return ret;
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws TException {
    if (part_name.length() == 0) {
      return Collections.emptyList();
    }
    LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
    return new ArrayList<>(map.values());
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name) throws TException {
    if (part_name.length() == 0) {
      return new HashMap<>();
    }
    return Warehouse.makeSpecFromName(part_name);
  }

  public static String lowerCaseConvertPartName(String partName) throws MetaException {
    if (partName == null) {
      return partName;
    }
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
  @Deprecated
  public ColumnStatistics get_table_column_statistics(String dbName, String tableName,
                                                      String colName) throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    parsedDbName[CAT_NAME] = parsedDbName[CAT_NAME].toLowerCase();
    parsedDbName[DB_NAME] = parsedDbName[DB_NAME].toLowerCase();
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    startFunction("get_column_statistics_by_table", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " column=" + colName);
    ColumnStatistics statsObj = null;
    try {
      statsObj = getMS().getTableColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, Lists.newArrayList(colName),
          "hive", null);
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
        TableName.getQualified(catName, dbName, tblName));
    TableStatsResult result = null;
    List<String> lowerCaseColNames = new ArrayList<>(request.getColNames().size());
    for (String colName : request.getColNames()) {
      lowerCaseColNames.add(colName.toLowerCase());
    }
    try {
      ColumnStatistics cs = getMS().getTableColumnStatistics(
          catName, dbName, tblName, lowerCaseColNames,
          request.getEngine(), request.getValidWriteIdList());
      // Note: stats compliance is not propagated to the client; instead, we just return nothing
      //       if stats are not compliant for now. This won't work for stats merging, but that
      //       is currently only done on metastore size (see set_aggr...).
      //       For some optimizations we might make use of incorrect stats that are "better than
      //       nothing", so this may change in future.
      result = new TableStatsResult((cs == null || cs.getStatsObj() == null
          || (cs.isSetIsStatsCompliant() && !cs.isIsStatsCompliant()))
          ? Lists.newArrayList() : cs.getStatsObj());
    } finally {
      endFunction("get_table_statistics_req", result == null, null, tblName);
    }
    return result;
  }

  @Override
  @Deprecated
  public ColumnStatistics get_partition_column_statistics(String dbName, String tableName,
                                                          String partName, String colName) throws TException {
    // Note: this method appears to be unused within Hive.
    //       It doesn't take txn stats into account.
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    colName = colName.toLowerCase();
    String convertedPartName = lowerCaseConvertPartName(partName);
    startFunction("get_column_statistics_by_partition", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
            tableName) + " partition=" + convertedPartName + " column=" + colName);
    ColumnStatistics statsObj = null;

    try {
      List<ColumnStatistics> list = getMS().getPartitionColumnStatistics(
          parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
          Lists.newArrayList(convertedPartName), Lists.newArrayList(colName),
          "hive");
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
    startPartitionFunction("get_partitions_statistics_req", catName, dbName, tblName, request.getPartNames());

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
          catName, dbName, tblName, lowerCasePartNames, lowerCaseColNames,
          request.getEngine(), request.isSetValidWriteIdList() ? request.getValidWriteIdList() : null);
      Map<String, List<ColumnStatisticsObj>> map = new HashMap<>();
      if (stats != null) {
        for (ColumnStatistics stat : stats) {
          // Note: stats compliance is not propagated to the client; instead, we just return nothing
          //       if stats are not compliant for now. This won't work for stats merging, but that
          //       is currently only done on metastore size (see set_aggr...).
          //       For some optimizations we might make use of incorrect stats that are "better than
          //       nothing", so this may change in future.
          if (stat.isSetIsStatsCompliant() && !stat.isIsStatsCompliant()) {
            continue;
          }
          map.put(stat.getStatsDesc().getPartName(), stat.getStatsObj());
        }
      }
      result = new PartitionsStatsResult(map);
    } finally {
      endFunction("get_partitions_statistics_req", result == null, null, tblName);
    }
    return result;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics colStats) throws TException {
    // Deprecated API, won't work for transactional tables
    return updateTableColumnStatsInternal(colStats, null, -1);
  }

  @Override
  public SetPartitionsStatsResponse update_table_column_statistics_req(
      SetPartitionsStatsRequest req) throws NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException,
      TException {
    if (req.getColStatsSize() != 1) {
      throw new InvalidInputException("Only one stats object expected");
    }
    if (req.isNeedMerge()) {
      throw new InvalidInputException("Merge is not supported for non-aggregate stats");
    }
    ColumnStatistics colStats = req.getColStatsIterator().next();
    boolean ret = updateTableColumnStatsInternal(colStats,
        req.getValidWriteIdList(), req.getWriteId());
    return new SetPartitionsStatsResponse(ret);
  }

  private boolean updateTableColumnStatsInternal(ColumnStatistics colStats,
                                                 String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    normalizeColStatsInput(colStats);

    startFunction("write_column_statistics", ":  table=" + TableName.getQualified(
        colStats.getStatsDesc().getCatName(), colStats.getStatsDesc().getDbName(),
        colStats.getStatsDesc().getTableName()));

    Map<String, String> parameters = null;
    getMS().openTransaction();
    boolean committed = false;
    try {
      parameters = getMS().updateTableColumnStatistics(colStats, validWriteIds, writeId);
      if (parameters != null) {
        Table tableObj = getMS().getTable(colStats.getStatsDesc().getCatName(),
            colStats.getStatsDesc().getDbName(),
            colStats.getStatsDesc().getTableName(), validWriteIds);
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
              EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId, this));
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId,this));
        }
      }
      committed = getMS().commitTransaction();
    } finally {
      if (!committed) {
        getMS().rollbackTransaction();
      }
      endFunction("write_column_statistics", parameters != null, null,
          colStats.getStatsDesc().getTableName());
    }

    return parameters != null;
  }

  private void normalizeColStatsInput(ColumnStatistics colStats) throws MetaException {
    // TODO: is this really needed? this code is propagated from HIVE-1362 but most of it is useless.
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    statsDesc.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() : getDefaultCatalog(conf));
    statsDesc.setDbName(statsDesc.getDbName().toLowerCase());
    statsDesc.setTableName(statsDesc.getTableName().toLowerCase());
    statsDesc.setPartName(lowerCaseConvertPartName(statsDesc.getPartName()));
    long time = System.currentTimeMillis() / 1000;
    statsDesc.setLastAnalyzed(time);

    for (ColumnStatisticsObj statsObj : colStats.getStatsObj()) {
      statsObj.setColName(statsObj.getColName().toLowerCase());
      statsObj.setColType(statsObj.getColType().toLowerCase());
    }
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(colStats.getStatsObj());
  }

  private boolean updatePartitonColStatsInternal(Table tbl, MTable mTable, ColumnStatistics colStats,
                                                 String validWriteIds, long writeId)
      throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    normalizeColStatsInput(colStats);

    ColumnStatisticsDesc csd = colStats.getStatsDesc();
    String catName = csd.getCatName(), dbName = csd.getDbName(), tableName = csd.getTableName();
    startFunction("write_partition_column_statistics", ":  db=" + dbName  + " table=" + tableName
        + " part=" + csd.getPartName());

    boolean ret = false;

    Map<String, String> parameters;
    List<String> partVals;
    boolean committed = false;
    getMS().openTransaction();
    
    try {
      tbl = Optional.ofNullable(tbl).orElse(getTable(catName, dbName, tableName));
      mTable = Optional.ofNullable(mTable).orElse(getMS().ensureGetMTable(catName, dbName, tableName));
      partVals = getPartValsFromName(tbl, csd.getPartName());
      parameters = getMS().updatePartitionColumnStatistics(tbl, mTable, colStats, partVals, validWriteIds, writeId);
      if (parameters != null) {
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
                  EventType.UPDATE_PARTITION_COLUMN_STAT,
                  new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, tbl,
                          writeId, this));
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
                  EventType.UPDATE_PARTITION_COLUMN_STAT,
                  new UpdatePartitionColumnStatEvent(colStats, partVals, parameters, tbl,
                          writeId, this));
        }
      }
      committed = getMS().commitTransaction();
    } finally {
      if (!committed) {
        getMS().rollbackTransaction();
      }
      endFunction("write_partition_column_statistics", ret != false, null, tableName);
    }
    return parameters != null;
  }

  private void updatePartitionColStatsForOneBatch(Table tbl, Map<String, ColumnStatistics> statsMap,
                                                     String validWriteIds, long writeId)
          throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    Map<String, Map<String, String>> result =
        getMS().updatePartitionColumnStatisticsInBatch(statsMap, tbl, transactionalListeners, validWriteIds, writeId);
    if (result != null && result.size() != 0 && listeners != null) {
      // The normal listeners, unlike transaction listeners are not using the same transactions used by the update
      // operations. So there is no need of keeping them within the same transactions. If notification to one of
      // the listeners failed, then even if we abort the transaction, we can not revert the notifications sent to the
      // other listeners.
      for (Map.Entry entry : result.entrySet()) {
        Map<String, String> parameters = (Map<String, String>) entry.getValue();
        ColumnStatistics colStats = statsMap.get(entry.getKey());
        List<String> partVals = getPartValsFromName(tbl, colStats.getStatsDesc().getPartName());
        MetaStoreListenerNotifier.notifyEvent(listeners,
                EventMessage.EventType.UPDATE_PARTITION_COLUMN_STAT,
                new UpdatePartitionColumnStatEvent(colStats, partVals, parameters,
                        tbl, writeId, this));
      }
    }
  }

  private boolean updatePartitionColStatsInBatch(Table tbl, Map<String, ColumnStatistics> statsMap,
                                                 String validWriteIds, long writeId)
          throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {

    if (statsMap.size() == 0) {
      return false;
    }

    String catalogName = tbl.getCatName();
    String dbName = tbl.getDbName();
    String tableName = tbl.getTableName();

    startFunction("updatePartitionColStatsInBatch", ":  db=" + dbName + " table=" + tableName);
    long start = System.currentTimeMillis();

    Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
    long numStats = 0;
    long numStatsMax = MetastoreConf.getIntVar(conf, ConfVars.JDBC_MAX_BATCH_SIZE);
    try {
      for (Map.Entry entry : statsMap.entrySet()) {
        ColumnStatistics colStats = (ColumnStatistics) entry.getValue();
        normalizeColStatsInput(colStats);
        assert catalogName.equalsIgnoreCase(colStats.getStatsDesc().getCatName());
        assert dbName.equalsIgnoreCase(colStats.getStatsDesc().getDbName());
        assert tableName.equalsIgnoreCase(colStats.getStatsDesc().getTableName());
        newStatsMap.put((String) entry.getKey(), colStats);
        numStats += colStats.getStatsObjSize();

        if (newStatsMap.size() >= numStatsMax) {
          updatePartitionColStatsForOneBatch(tbl, newStatsMap, validWriteIds, writeId);
          newStatsMap.clear();
          numStats = 0;
        }
      }
      if (numStats != 0) {
        updatePartitionColStatsForOneBatch(tbl, newStatsMap, validWriteIds, writeId);
      }
    } finally {
      endFunction("updatePartitionColStatsInBatch", true, null, tableName);
      long end = System.currentTimeMillis();
      float sec = (end - start) / 1000F;
      LOG.info("updatePartitionColStatsInBatch took " + sec + " seconds for " + statsMap.size() + " stats");
    }
    return true;
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics colStats) throws TException {
    // Deprecated API.
    return updatePartitonColStatsInternal(null, null, colStats, null, -1);
  }


  @Override
  public SetPartitionsStatsResponse update_partition_column_statistics_req(
      SetPartitionsStatsRequest req) throws NoSuchObjectException,
      InvalidObjectException, MetaException, InvalidInputException,
      TException {
    if (req.getColStatsSize() != 1) {
      throw new InvalidInputException("Only one stats object expected");
    }
    if (req.isNeedMerge()) {
      throw new InvalidInputException("Merge is not supported for non-aggregate stats");
    }
    ColumnStatistics colStats = req.getColStatsIterator().next();
    boolean ret = updatePartitonColStatsInternal(null, null, colStats,
        req.getValidWriteIdList(), req.getWriteId());
    return new SetPartitionsStatsResponse(ret);
  }

  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
                                                    String partName, String colName, String engine) throws TException {
    dbName = dbName.toLowerCase();
    String[] parsedDbName = parseDbName(dbName, conf);
    tableName = tableName.toLowerCase();
    if (colName != null) {
      colName = colName.toLowerCase();
    }
    String convertedPartName = lowerCaseConvertPartName(partName);
    startFunction("delete_column_statistics_by_partition",": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName) +
        " partition=" + convertedPartName + " column=" + colName);
    boolean ret = false, committed = false;

    getMS().openTransaction();
    try {
      List<String> partVals = getPartValsFromName(getMS(), parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, convertedPartName);
      Table table = getMS().getTable(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);
      // This API looks unused; if it were used we'd need to update stats state and write ID.
      // We cannot just randomly nuke some txn stats.
      if (TxnUtils.isTransactionalTable(table)) {
        throw new MetaException("Cannot delete stats via this API for a transactional table");
      }

      ret = getMS().deletePartitionColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
          convertedPartName, partVals, colName, engine);
      if (ret) {
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
              EventType.DELETE_PARTITION_COLUMN_STAT,
              new DeletePartitionColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                  convertedPartName, partVals, colName, engine, this));
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DELETE_PARTITION_COLUMN_STAT,
              new DeletePartitionColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName,
                  convertedPartName, partVals, colName, engine, this));
        }
      }
      committed = getMS().commitTransaction();
    } finally {
      if (!committed) {
        getMS().rollbackTransaction();
      }
      endFunction("delete_column_statistics_by_partition", ret != false, null, tableName);
    }
    return ret;
  }

  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName, String colName, String engine)
      throws TException {
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    String[] parsedDbName = parseDbName(dbName, conf);

    if (colName != null) {
      colName = colName.toLowerCase();
    }
    startFunction("delete_column_statistics_by_table", ": table=" +
        TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName) + " column=" +
        colName);


    boolean ret = false, committed = false;
    getMS().openTransaction();
    try {
      Table table = getMS().getTable(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName);
      // This API looks unused; if it were used we'd need to update stats state and write ID.
      // We cannot just randomly nuke some txn stats.
      if (TxnUtils.isTransactionalTable(table)) {
        throw new MetaException("Cannot delete stats via this API for a transactional table");
      }

      ret = getMS().deleteTableColumnStatistics(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tableName, colName, engine);
      if (ret) {
        if (transactionalListeners != null && !transactionalListeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
              EventType.DELETE_TABLE_COLUMN_STAT,
              new DeleteTableColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
                  tableName, colName, engine, this));
        }
        if (!listeners.isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(listeners,
              EventType.DELETE_TABLE_COLUMN_STAT,
              new DeleteTableColumnStatEvent(parsedDbName[CAT_NAME], parsedDbName[DB_NAME],
                  tableName, colName, engine, this));
        }
      }
      committed = getMS().commitTransaction();
    } finally {
      if (!committed) {
        getMS().rollbackTransaction();
      }
      endFunction("delete_column_statistics_by_table", ret != false, null, tableName);
    }
    return ret;
  }

  @Override
  public void update_transaction_statistics(UpdateTransactionalStatsRequest req) throws TException {
    getTxnHandler().updateTransactionStatistics(req);
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_filter(final String dbName, final String tblName,
                                                  final String filter, final short maxParts)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    return get_partitions_by_filter_internal(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
        new GetPartitionsArgs.GetPartitionsArgsBuilder().filter(filter).max(maxParts).build());
  }

  private List<Partition> get_partitions_by_filter_internal(final String catName,
      final String dbName, final String tblName, GetPartitionsArgs args) throws TException {
    startTableFunction("get_partitions_by_filter", catName, dbName,
        tblName);
    fireReadTablePreEvent(catName, dbName, tblName);
    List<Partition> ret = null;
    Exception ex = null;
    try {
      checkLimitNumberOfPartitionsByFilter(catName, dbName,
          tblName, args.getFilter(), args.getMax());

      authorizeTableForPartitionMetadata(catName, dbName, tblName);

      ret = getMS().getPartitionsByFilter(catName, dbName, tblName, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_by_filter", ret != null, ex, tblName);
    }
    return ret;
  }

  public List<Partition> get_partitions_by_filter_req(GetPartitionsByFilterRequest req) throws TException {
    return get_partitions_by_filter_internal(req.getCatName(), req.getDbName(), req.getTblName(),
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .filter(req.getFilter()).max(req.getMaxParts())
            .skipColumnSchemaForPartition(req.isSkipColumnSchemaForPartition())
            .excludeParamKeyPattern(req.getExcludeParamKeyPattern())
            .includeParamKeyPattern(req.getIncludeParamKeyPattern())
            .build());
  }

  @Override
  @Deprecated
  public List<PartitionSpec> get_part_specs_by_filter(final String dbName, final String tblName,
                                                      final String filter, final int maxParts)
      throws TException {

    String[] parsedDbName = parseDbName(dbName, conf);
    startPartitionFunction("get_partitions_by_filter_pspec", parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName,
            maxParts, filter);
    List<PartitionSpec> partitionSpecs = null;
    try {
      Table table = get_table_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName);
      // Don't pass the parsed db name, as get_partitions_by_filter will parse it itself
      List<Partition> partitions = get_partitions_by_filter(dbName, tblName, filter, (short) maxParts);

      if (is_partition_spec_grouping_enabled(table)) {
        partitionSpecs = MetaStoreServerUtils
            .getPartitionspecsGroupedByStorageDescriptor(table, partitions);
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
  public PartitionsSpecByExprResult get_partitions_spec_by_expr(
      PartitionsByExprRequest req) throws TException {
    String dbName = req.getDbName(), tblName = req.getTblName();
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    startTableFunction("get_partitions_spec_by_expr", catName, dbName, tblName);
    fireReadTablePreEvent(catName, dbName, tblName);
    PartitionsSpecByExprResult ret = null;
    Exception ex = null;
    try {
      checkLimitNumberOfPartitionsByExpr(catName, dbName, tblName, req.getExpr(), UNLIMITED_MAX_PARTITIONS);
      List<Partition> partitions = new LinkedList<>();
      boolean hasUnknownPartitions = getMS().getPartitionsByExpr(catName, dbName, tblName, partitions,
          new GetPartitionsArgs.GetPartitionsArgsBuilder()
              .expr(req.getExpr()).max(req.getMaxParts()).defaultPartName(req.getDefaultPartitionName())
              .skipColumnSchemaForPartition(req.isSkipColumnSchemaForPartition())
              .includeParamKeyPattern(req.getIncludeParamKeyPattern())
              .excludeParamKeyPattern(req.getExcludeParamKeyPattern())
              .build());
      Table table = get_table_core(catName, dbName, tblName);
      List<PartitionSpec> partitionSpecs =
          MetaStoreServerUtils.getPartitionspecsGroupedByStorageDescriptor(table, partitions);
      ret = new PartitionsSpecByExprResult(partitionSpecs, hasUnknownPartitions);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_spec_by_expr", ret != null, ex, tblName);
    }
    return ret;
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(
      PartitionsByExprRequest req) throws TException {
    String dbName = req.getDbName(), tblName = req.getTblName();
    String catName = req.isSetCatName() ? req.getCatName() : getDefaultCatalog(conf);
    String expr = req.isSetExpr() ? Arrays.toString((req.getExpr())) : "";
    String defaultPartitionName = req.isSetDefaultPartitionName() ? req.getDefaultPartitionName() : "";
    int maxParts = req.getMaxParts();
    startPartitionFunction("get_partitions_by_expr", catName, dbName, tblName, maxParts, expr, defaultPartitionName);
    fireReadTablePreEvent(catName, dbName, tblName);
    PartitionsByExprResult ret = null;
    Exception ex = null;
    try {
      checkLimitNumberOfPartitionsByExpr(catName, dbName, tblName, req.getExpr(), UNLIMITED_MAX_PARTITIONS);
      List<Partition> partitions = new LinkedList<>();
      boolean hasUnknownPartitions = getMS().getPartitionsByExpr(catName, dbName, tblName, partitions,
          new GetPartitionsArgs.GetPartitionsArgsBuilder()
              .expr(req.getExpr()).defaultPartName(req.getDefaultPartitionName()).max(req.getMaxParts())
              .skipColumnSchemaForPartition(req.isSkipColumnSchemaForPartition())
              .excludeParamKeyPattern(req.getExcludeParamKeyPattern())
              .includeParamKeyPattern(req.getIncludeParamKeyPattern())
              .build());
      ret = new PartitionsByExprResult(partitions, hasUnknownPartitions);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("get_partitions_by_expr", ret != null, ex, tblName);
    }
    return ret;
  }

  @Override
  @Deprecated
  public int get_num_partitions_by_filter(final String dbName,
                                          final String tblName, final String filter)
      throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    if (parsedDbName[DB_NAME] == null || tblName == null) {
      throw new MetaException("The DB and table name cannot be null.");
    }
    startFunction("get_num_partitions_by_filter",
        " : tbl=" + TableName.getQualified(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], tblName) + " Filter="
            + filter);

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

  private int getNumPartitionsByPs(final String catName, final String dbName,
                                   final String tblName, List<String> partVals)
          throws TException {
    String[] parsedDbName = parseDbName(dbName, conf);
    startTableFunction("getNumPartitionsByPs", parsedDbName[CAT_NAME],
            parsedDbName[DB_NAME], tblName);

    int ret = -1;
    Exception ex = null;
    try {
      ret = getMS().getNumPartitionsByPs(catName, dbName, tblName, partVals);
    } catch (Exception e) {
      ex = e;
      rethrowException(e);
    } finally {
      endFunction("getNumPartitionsByPs", ret != -1, ex, tblName);
    }
    return ret;
  }

  @Override
  @Deprecated
  public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
                                                 final List<String> partNames)
      throws TException {
    return get_partitions_by_names(dbName, tblName, false, null, null, null,
        new GetPartitionsArgs.GetPartitionsArgsBuilder().partNames(partNames).build());
  }

  @Override
  public GetPartitionsByNamesResult get_partitions_by_names_req(GetPartitionsByNamesRequest gpbnr)
      throws TException {
    List<Partition> partitions = get_partitions_by_names(gpbnr.getDb_name(),
        gpbnr.getTbl_name(),
        gpbnr.isSetGet_col_stats() && gpbnr.isGet_col_stats(), gpbnr.getEngine(),
        gpbnr.getProcessorCapabilities(), gpbnr.getProcessorIdentifier(),
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .partNames(gpbnr.getNames()).skipColumnSchemaForPartition(gpbnr.isSkipColumnSchemaForPartition())
            .excludeParamKeyPattern(gpbnr.getExcludeParamKeyPattern())
            .includeParamKeyPattern(gpbnr.getIncludeParamKeyPattern())
            .build());
    GetPartitionsByNamesResult result = new GetPartitionsByNamesResult(partitions);
    return result;
  }

  public List<Partition> get_partitions_by_names(final String dbName, final String tblName,
      boolean getColStats, String engine,
      List<String> processorCapabilities, String processorId,
      GetPartitionsArgs args) throws TException {

    String[] dbNameParts = parseDbName(dbName, conf);
    String parsedCatName = dbNameParts[CAT_NAME];
    String parsedDbName = dbNameParts[DB_NAME];
    List<Partition> ret = null;
    Table table = null;
    Exception ex = null;
    boolean success = false;
    startTableFunction("get_partitions_by_names", parsedCatName, parsedDbName,
        tblName);
    try {
      getMS().openTransaction();
      authorizeTableForPartitionMetadata(parsedCatName, parsedDbName, tblName);

      fireReadTablePreEvent(parsedCatName, parsedDbName, tblName);

      ret = getMS().getPartitionsByNames(parsedCatName, parsedDbName, tblName, args);
      ret = FilterUtils.filterPartitionsIfEnabled(isServerFilterEnabled, filterHook, ret);
      table = getTable(parsedCatName, parsedDbName, tblName);

      // If requested add column statistics in each of the partition objects
      if (getColStats) {
        // Since each partition may have stats collected for different set of columns, we
        // request them separately.
        for (Partition part: ret) {
          String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
          List<ColumnStatistics> partColStatsList =
              getMS().getPartitionColumnStatistics(parsedCatName, parsedDbName, tblName,
                  Collections.singletonList(partName),
                  StatsSetupConst.getColumnsHavingStats(part.getParameters()),
                  engine);
          if (partColStatsList != null && !partColStatsList.isEmpty()) {
            ColumnStatistics partColStats = partColStatsList.get(0);
            if (partColStats != null) {
              part.setColStats(partColStats);
            }
          }
        }
      }

      if (processorCapabilities == null || processorCapabilities.size() == 0 ||
          processorCapabilities.contains("MANAGERAWMETADATA")) {
        LOG.info("Skipping translation for processor with " + processorId);
      } else {
        if (transformer != null) {
          ret = transformer.transformPartitions(ret, table, processorCapabilities, processorId);
        }
      }
      success = getMS().commitTransaction();
    } catch (Exception e) {
      ex = e;
      throw handleException(e)
          .throwIfInstance(MetaException.class, NoSuchObjectException.class, InvalidObjectException.class)
          .defaultMetaException();
    } finally {
      if (!success) {
        getMS().rollbackTransaction();
      }
      endFunction("get_partitions_by_names", ret != null, ex, tblName);
    }
    return ret;
  }

  /**
   * Creates an instance of property manager based on the (declared) namespace.
   * @param ns the namespace
   * @return the manager instance
   * @throws TException
   */
  private PropertyManager getPropertyManager(String ns) throws MetaException, NoSuchObjectException {
    PropertyStore propertyStore = getMS().getPropertyStore();
    PropertyManager mgr = PropertyManager.create(ns, propertyStore);
    return mgr;
  }
  @Override
  public PropertyGetResponse get_properties(PropertyGetRequest req) throws TException {
    try {
      PropertyManager mgr = getPropertyManager(req.getNameSpace());
      Map<String, PropertyMap> selected = mgr.selectProperties(req.getMapPrefix(), req.getMapPredicate(), req.getMapSelection());
      PropertyGetResponse response = new PropertyGetResponse();
      Map<String, Map<String, String>> returned = new TreeMap<>();
      selected.forEach((k, v) -> {
        returned.put(k, v.export());
      });
      response.setProperties(returned);
      return response;
    } catch(PropertyException exception) {
      throw ExceptionHandler.newMetaException(exception);
    }
  }

  @Override
  public boolean set_properties(PropertySetRequest req) throws TException {
    try {
      PropertyManager mgr = getPropertyManager(req.getNameSpace());
      mgr.setProperties((Map<String, Object>) (Map<?, ?>) req.getPropertyMap());
      mgr.commit();
      return true;
    } catch(PropertyException exception) {
      throw ExceptionHandler.newMetaException(exception);
    }
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName,
                                                 List<String> groupNames) throws TException {
    firePreEvent(new PreAuthorizationCallEvent(this));
    String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() : getDefaultCatalog(conf);
    HiveObjectType debug = hiveObject.getObjectType();
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
    } else if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
      return this.get_connector_privilege_set(catName, hiveObject.getObjectName(), userName, groupNames);
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_db_privilege_set(String catName, final String dbName,
                                                     final String userName, final List<String> groupNames) throws TException {
    incrementCounter("get_db_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getDBPrivilegeSet(catName, dbName, userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_connector_privilege_set(String catName, final String connectorName,
                                                            final String userName, final List<String> groupNames) throws TException {
    incrementCounter("get_connector_privilege_set");

    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getConnectorPrivilegeSet(catName, connectorName, userName, groupNames);
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
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
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
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
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, NoSuchObjectException.class)
          .defaultTException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  @Override
  public boolean grant_privileges(final PrivilegeBag privileges) throws TException {
    incrementCounter("grant_privileges");
    firePreEvent(new PreAuthorizationCallEvent(this));
    Boolean ret;
    try {
      ret = getMS().grantPrivileges(privileges);
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
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
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, NoSuchObjectException.class)
          .defaultTException();
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
  public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
                                                         GrantRevokePrivilegeRequest grantRequest)
      throws TException {
    incrementCounter("refresh_privileges");
    firePreEvent(new PreAuthorizationCallEvent(this));
    GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
    try {
      boolean result = getMS().refreshPrivileges(objToRefresh, authorizer, grantRequest.getPrivileges());
      response.setSuccess(result);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
      LOG.error(exInfo, e);
      throw handleException(e).throwIfInstance(MetaException.class)
          .toMetaExceptionIfInstance(exInfo, InvalidObjectException.class, NoSuchObjectException.class)
          .defaultTException();
    }
    return ret;
  }

  private PrincipalPrivilegeSet get_user_privilege_set(final String userName,
                                                       final List<String> groupNames) throws TException {
    incrementCounter("get_user_privilege_set");
    PrincipalPrivilegeSet ret;
    try {
      ret = getMS().getUserPrivilegeSet(userName, groupNames);
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    if (hiveObject.getObjectType() == HiveObjectType.DATACONNECTOR) {
      return list_dc_privileges(principalName, principalType, hiveObject
              .getObjectName());
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
    privs.addAll(list_dc_privileges(principalName, principalType, null));
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
    }
  }

  private List<HiveObjectPrivilege> list_dc_privileges(final String principalName,
                                                       final PrincipalType principalType, final String dcName) throws TException {
    incrementCounter("list_security_dc_grant");

    try {
      if (dcName == null) {
        return getMS().listPrincipalDCGrantsAll(principalName, principalType);
      }
      if (principalName == null) {
        return getMS().listDCGrantsAll(dcName);
      } else {
        return getMS().listPrincipalDCGrants(principalName, principalType, dcName);
      }
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      throw handleException(e).throwIfInstance(MetaException.class).defaultRuntimeException();
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class).defaultMetaException();
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e).convertIfInstance(IOException.class, MetaException.class)
          .convertIfInstance(InterruptedException.class, MetaException.class)
          .defaultMetaException();
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw newMetaException(e);
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
      throw handleException(original)
          .throwIfInstance(UnknownTableException.class, InvalidPartitionException.class, MetaException.class)
          .defaultMetaException();
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
      LOG.error("Exception caught for isPartitionMarkedForEvent ", original);
      ex = original;
      throw handleException(original).throwIfInstance(UnknownTableException.class, InvalidPartitionException.class)
          .throwIfInstance(UnknownPartitionException.class,  MetaException.class)
          .defaultMetaException();
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
        MetaStoreServerUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
        ret = true;
      } else {
        ret = MetaStoreServerUtils.partitionNameHasValidCharacters(part_vals,
            partitionValidationPattern);
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("partition_name_has_valid_characters", true, ex);
    }
    return ret;
  }

  private void validateFunctionInfo(Function func) throws InvalidObjectException, MetaException {
    if (func == null) {
      throw new MetaException("Function cannot be null.");
    }
    if (func.getFunctionName() == null) {
      throw new MetaException("Function name cannot be null.");
    }
    if (func.getDbName() == null) {
      throw new MetaException("Database name in Function cannot be null.");
    }
    if (!MetaStoreUtils.validateName(func.getFunctionName(), null)) {
      throw new InvalidObjectException(func.getFunctionName() + " is not a valid object name");
    }
    String className = func.getClassName();
    if (className == null) {
      throw new InvalidObjectException("Function class name cannot be null");
    }
    if (func.getOwnerType() == null) {
      throw new MetaException("Function owner type cannot be null.");
    }
    if (func.getFunctionType() == null) {
      throw new MetaException("Function type cannot be null.");
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
      if (!func.isSetOwnerName()) {
        try {
          func.setOwnerName(SecurityUtils.getUGI().getShortUserName());
        } catch (Exception ex) {
          LOG.error("Cannot obtain username from the session to create a function", ex);
          throw new TException(ex);
        }
      }
      ms.openTransaction();
      Database db = ms.getDatabase(catName, func.getDbName());
      if (db == null) {
        throw new NoSuchObjectException("The database " + func.getDbName() + " does not exist");
      }

      if (db.getType() == DatabaseType.REMOTE) {
        throw new MetaException("Operation create_function not support for REMOTE database");
      }

      Function existingFunc = ms.getFunction(catName, func.getDbName(), func.getFunctionName());
      if (existingFunc != null) {
        throw new AlreadyExistsException(
            "Function " + func.getFunctionName() + " already exists");
      }
      firePreEvent(new PreCreateFunctionEvent(func, this));
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
    if (funcName == null) {
      throw new MetaException("Function name cannot be null.");
    }
    boolean success = false;
    Function func = null;
    RawStore ms = getMS();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    String[] parsedDbName = parseDbName(dbName, conf);
    if (parsedDbName[DB_NAME] == null) {
      throw new MetaException("Database name cannot be null.");
    }
    try {
      ms.openTransaction();
      func = ms.getFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName);
      if (func == null) {
        throw new NoSuchObjectException("Function " + funcName + " does not exist");
      }
      Boolean needsCm =
          ReplChangeManager.isSourceOfReplication(get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]));

      // if copy of jar to change management fails we fail the metastore transaction, since the
      // user might delete the jars on HDFS externally after dropping the function, hence having
      // a copy is required to allow incremental replication to work correctly.
      if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
        for (ResourceUri uri : func.getResourceUris()) {
          if (uri.getUri().toLowerCase().startsWith("hdfs:") && needsCm) {
            wh.addToChangeManagement(new Path(uri.getUri()));
          }
        }
      }
      firePreEvent(new PreDropFunctionEvent(func, this));

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
    String[] parsedDbName = parseDbName(dbName, conf);
    validateForAlterFunction(parsedDbName[DB_NAME], funcName, newFunc);
    boolean success = false;
    RawStore ms = getMS();
    try {
      firePreEvent(new PreCreateFunctionEvent(newFunc, this));
      ms.openTransaction();
      ms.alterFunction(parsedDbName[CAT_NAME], parsedDbName[DB_NAME], funcName, newFunc);
      success = ms.commitTransaction();
    } catch (InvalidObjectException e) {
      // Throwing MetaException instead of InvalidObjectException as the InvalidObjectException
      // is not defined for the alter_function method in the Thrift interface.
      throwMetaException(e);
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
    }
  }

  private void validateForAlterFunction(String dbName, String funcName, Function newFunc)
      throws MetaException {
    if (dbName == null || funcName == null) {
      throw new MetaException("Database and function name cannot be null.");
    }
    try {
      validateFunctionInfo(newFunc);
    } catch (InvalidObjectException e) {
      // The validateFunctionInfo method is used by the create and alter function methods as well
      // and it can throw InvalidObjectException. But the InvalidObjectException is not defined
      // for the alter_function method in the Thrift interface, therefore a TApplicationException
      // will occur at the caller side. Re-throwing the InvalidObjectException as MetaException
      // would eliminate the TApplicationException at caller side.
      throw newMetaException(e);
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
    if (dbName == null || funcName == null) {
      throw new MetaException("Database and function name cannot be null.");
    }
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
    } catch (Exception e) {
      ex = e;
      throw handleException(e).throwIfInstance(NoSuchObjectException.class).defaultMetaException();
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

  @Override
  public GetOpenTxnsResponse get_open_txns_req(GetOpenTxnsRequest getOpenTxnsRequest) throws TException {
    return getTxnHandler().getOpenTxns(getOpenTxnsRequest.getExcludeTxnTypes());
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
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    if (txnIds != null && listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.OPEN_TXN,
          new OpenTxnEvent(txnIds, this));
    }
    return response;
  }

  @Override
  public void abort_txn(AbortTxnRequest rqst) throws TException {
    getTxnHandler().abortTxn(rqst);
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    if (listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      // Not adding dbsUpdated to AbortTxnEvent because
      // only DbNotificationListener cares about it, and this is already
      // handled with transactional listeners in TxnHandler.
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ABORT_TXN,
          new AbortTxnEvent(rqst.getTxnid(), this));
    }
  }

  @Override
  public void abort_txns(AbortTxnsRequest rqst) throws TException {
    getTxnHandler().abortTxns(rqst);
    if (listeners != null && !listeners.isEmpty()) {
      for (Long txnId : rqst.getTxn_ids()) {
        // See above abort_txn() note about not adding dbsUpdated.
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ABORT_TXN,
            new AbortTxnEvent(txnId, this));
      }
    }
  }

 @Override
  public AbortCompactResponse abort_Compactions(AbortCompactionRequest rqst) throws TException {
    return getTxnHandler().abortCompactions(rqst);
  }

  @Override
  public long get_latest_txnid_in_conflict(long txnId) throws MetaException {
    return getTxnHandler().getLatestTxnIdInConflict(txnId);
  }

  @Override
  public void commit_txn(CommitTxnRequest rqst) throws TException {
    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    // in replication flow, the write notification log table will be updated here.
    if (rqst.isSetWriteEventInfos() && isReplayedReplTxn) {
      assert (rqst.isSetReplPolicy());
      long targetTxnId = getTxnHandler().getTargetTxnId(rqst.getReplPolicy(), rqst.getTxnid());
      if (targetTxnId < 0) {
        //looks like a retry
        return;
      }
      for (WriteEventInfo writeEventInfo : rqst.getWriteEventInfos()) {
        String[] filesAdded = ReplChangeManager.getListFromSeparatedString(writeEventInfo.getFiles());
        List<String> partitionValue = null;
        Partition ptnObj = null;
        String root;
        Table tbl = getTblObject(writeEventInfo.getDatabase(), writeEventInfo.getTable(), null);

        if (writeEventInfo.getPartition() != null && !writeEventInfo.getPartition().isEmpty()) {
          partitionValue = Warehouse.getPartValuesFromPartName(writeEventInfo.getPartition());
          ptnObj = getPartitionObj(writeEventInfo.getDatabase(), writeEventInfo.getTable(), partitionValue, tbl);
          root = ptnObj.getSd().getLocation();
        } else {
          root = tbl.getSd().getLocation();
        }

        InsertEventRequestData insertData = new InsertEventRequestData();
        insertData.setReplace(true);

        // The files in the commit txn message during load will have files with path corresponding to source
        // warehouse. Need to transform them to target warehouse using table or partition object location.
        for (String file : filesAdded) {
          String[] decodedPath = ReplChangeManager.decodeFileUri(file);
          String name = (new Path(decodedPath[0])).getName();
          Path newPath = FileUtils.getTransformedPath(name, decodedPath[3], root);
          insertData.addToFilesAdded(newPath.toUri().toString());
          insertData.addToSubDirectoryList(decodedPath[3]);
          try {
            insertData.addToFilesAddedChecksum(ReplChangeManager.checksumFor(newPath, newPath.getFileSystem(conf)));
          } catch (IOException e) {
            LOG.error("failed to get checksum for the file " + newPath + " with error: " + e.getMessage());
            throw new TException(e.getMessage());
          }
        }

        WriteNotificationLogRequest wnRqst = new WriteNotificationLogRequest(targetTxnId,
            writeEventInfo.getWriteId(), writeEventInfo.getDatabase(), writeEventInfo.getTable(), insertData);
        if (partitionValue != null) {
          wnRqst.setPartitionVals(partitionValue);
        }
        addTxnWriteNotificationLog(tbl, ptnObj, wnRqst);
      }
    }
    getTxnHandler().commitTxn(rqst);
    if (listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.COMMIT_TXN,
              new CommitTxnEvent(rqst.getTxnid(), this));
      Optional<CompactionInfo> compactionInfo = getTxnHandler().getCompactionByTxnId(rqst.getTxnid());
      if (compactionInfo.isPresent()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.COMMIT_COMPACTION,
            new CommitCompactionEvent(rqst.getTxnid(), compactionInfo.get(), this));
      }
    }
  }

  @Override
  public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest rqst) throws TException {
    getTxnHandler().replTableWriteIdState(rqst);
  }

  @Override
  public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest rqst) throws TException {
    return getTxnHandler().getValidWriteIds(rqst);
  }

  @Override
  public void add_write_ids_to_min_history(long txnId, Map<String, Long> validWriteIds) throws TException {
     getTxnHandler().addWriteIdsToMinHistory(txnId, validWriteIds);
  }

  @Override
  public void set_hadoop_jobid(String jobId, long cqId) throws MetaException {
    getTxnHandler().setHadoopJobId(jobId, cqId);
  }

  @Deprecated
  @Override
  public OptionalCompactionInfoStruct find_next_compact(String workerId) throws MetaException{
    return CompactionInfo.compactionInfoToOptionalStruct(
        getTxnHandler().findNextToCompact(workerId));
  }

  @Override
  public OptionalCompactionInfoStruct find_next_compact2(FindNextCompactRequest rqst) throws MetaException{
    return CompactionInfo.compactionInfoToOptionalStruct(
            getTxnHandler().findNextToCompact(rqst));
  }

  @Override
  public void mark_cleaned(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markCleaned(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_compacted(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markCompacted(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_failed(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markFailed(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_refused(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markRefused(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public boolean update_compaction_metrics_data(CompactionMetricsDataStruct struct) throws MetaException, TException {
      return getTxnHandler().updateCompactionMetricsData(CompactionMetricsDataConverter.structToData(struct));
  }

  @Override
  public void remove_compaction_metrics_data(CompactionMetricsDataRequest request)
      throws MetaException, TException {
    getTxnHandler().removeCompactionMetricsData(request.getDbName(), request.getTblName(), request.getPartitionName(),
        CompactionMetricsDataConverter.thriftCompactionMetricType2DbType(request.getType()));
  }

  @Override
  public List<String> find_columns_with_stats(CompactionInfoStruct cr) throws MetaException {
    return getTxnHandler().findColumnsWithStats(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void update_compactor_state(CompactionInfoStruct cr, long highWaterMark) throws MetaException {
    getTxnHandler().updateCompactorState(
        CompactionInfo.compactionStructToInfo(cr), highWaterMark);
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse get_latest_committed_compaction_info(
      GetLatestCommittedCompactionInfoRequest rqst) throws MetaException {
    if (rqst.getDbname() == null || rqst.getTablename() == null) {
      throw new MetaException("Database name and table name cannot be null.");
    }
    GetLatestCommittedCompactionInfoResponse response = getTxnHandler().getLatestCommittedCompactionInfo(rqst);
    return FilterUtils.filterCommittedCompactionInfoStructIfEnabled(isServerFilterEnabled, filterHook,
        getDefaultCatalog(conf), rqst.getDbname(), rqst.getTablename(), response);
  }

  @Override
  public AllocateTableWriteIdsResponse allocate_table_write_ids(
      AllocateTableWriteIdsRequest rqst) throws TException {
    AllocateTableWriteIdsResponse response = getTxnHandler().allocateTableWriteIds(rqst);
    if (listeners != null && !listeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ALLOC_WRITE_ID,
          new AllocWriteIdEvent(response.getTxnToWriteIds(), rqst.getDbName(),
              rqst.getTableName(), this));
    }
    return response;
  }

  @Override
  public MaxAllocatedTableWriteIdResponse get_max_allocated_table_write_id(MaxAllocatedTableWriteIdRequest rqst)
      throws MetaException {
    return getTxnHandler().getMaxAllocatedTableWrited(rqst);
  }

  @Override
  public void seed_write_id(SeedTableWriteIdsRequest rqst) throws MetaException {
    getTxnHandler().seedWriteId(rqst);
  }

  @Override
  public void seed_txn_id(SeedTxnIdRequest rqst) throws MetaException {
    getTxnHandler().seedTxnId(rqst);
  }

  private void addTxnWriteNotificationLog(Table tableObj, Partition ptnObj, WriteNotificationLogRequest rqst)
      throws MetaException {
    String partition = ""; //Empty string is an invalid partition name. Can be used for non partitioned table.
    if (ptnObj != null) {
      partition = Warehouse.makePartName(tableObj.getPartitionKeys(), rqst.getPartitionVals());
    }
    AcidWriteEvent event = new AcidWriteEvent(partition, tableObj, ptnObj, rqst);
    getTxnHandler().addWriteNotificationLog(event);
    if (listeners != null && !listeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.ACID_WRITE, event);
    }
  }

  private Table getTblObject(String db, String table, String catalog) throws MetaException, NoSuchObjectException {
    GetTableRequest req = new GetTableRequest(db, table);
    if (catalog != null) {
      req.setCatName(catalog);
    }
    req.setCapabilities(new ClientCapabilities(Lists.newArrayList(ClientCapability.TEST_CAPABILITY, ClientCapability.INSERT_ONLY_TABLES)));
    return get_table_req(req).getTable();
  }

  private Partition getPartitionObj(String db, String table, List<String> partitionVals, Table tableObj)
      throws MetaException, NoSuchObjectException {
    if (tableObj.isSetPartitionKeys() && !tableObj.getPartitionKeys().isEmpty()) {
      return get_partition(db, table, partitionVals);
    }
    return null;
  }

  @Override
  public WriteNotificationLogResponse add_write_notification_log(WriteNotificationLogRequest rqst)
      throws TException {
    Table tableObj = getTblObject(rqst.getDb(), rqst.getTable(), null);
    Partition ptnObj = getPartitionObj(rqst.getDb(), rqst.getTable(), rqst.getPartitionVals(), tableObj);
    addTxnWriteNotificationLog(tableObj, ptnObj, rqst);
    return new WriteNotificationLogResponse();
  }

  @Override
  public WriteNotificationLogBatchResponse add_write_notification_log_in_batch(
          WriteNotificationLogBatchRequest batchRequest) throws TException {
    if (batchRequest.getRequestList().size() == 0) {
      return new WriteNotificationLogBatchResponse();
    }

    Table tableObj = getTblObject(batchRequest.getDb(), batchRequest.getTable(), batchRequest.getCatalog());
    BatchAcidWriteEvent event = new BatchAcidWriteEvent();
    List<String> partNameList = new ArrayList<>();
    List<Partition> ptnObjList;

    Map<String, WriteNotificationLogRequest> rqstMap = new HashMap<>();
    if (tableObj.getPartitionKeys().size() != 0) {
      // partitioned table
      for (WriteNotificationLogRequest rqst : batchRequest.getRequestList()) {
        String partition = Warehouse.makePartName(tableObj.getPartitionKeys(), rqst.getPartitionVals());
        partNameList.add(partition);
        // This is used to ignore those request for which the partition does not exists.
        rqstMap.put(partition, rqst);
      }
      ptnObjList = getMS().getPartitionsByNames(tableObj.getCatName(), tableObj.getDbName(),
              tableObj.getTableName(), partNameList);
    } else {
      ptnObjList = new ArrayList<>();
      for (WriteNotificationLogRequest ignored : batchRequest.getRequestList()) {
        ptnObjList.add(null);
      }
    }

    int idx = 0;
    for (Partition partObject : ptnObjList) {
      String partition = ""; //Empty string is an invalid partition name. Can be used for non partitioned table.
      WriteNotificationLogRequest request;
      if (partObject != null) {
        partition = Warehouse.makePartName(tableObj.getPartitionKeys(), partObject.getValues());
        request = rqstMap.get(partition);
      } else {
        // for non partitioned table, we can get serially from the list.
        request = batchRequest.getRequestList().get(idx++);
      }
      event.addNotification(partition, tableObj, partObject, request);
      if (listeners != null && !listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.BATCH_ACID_WRITE,
                new BatchAcidWriteEvent(partition, tableObj, partObject, request));
      }
    }

    getTxnHandler().addWriteNotificationLog(event);
    return new WriteNotificationLogBatchResponse();
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
    ShowCompactResponse response = getTxnHandler().showCompact(rqst);
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isServerFilterEnabled,
        filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override
  public boolean submit_for_cleanup(CompactionRequest rqst, long highestWriteId, long txnId)
      throws TException {
    return getTxnHandler().submitForCleanup(rqst, highestWriteId, txnId);
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
        TableName.getQualified(catName, dbName, tblName));

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
      aggrStats = getMS().get_aggr_stats_for(catName, dbName, tblName,
          lowerCasePartNames, lowerCaseColNames, request.getEngine(), request.getValidWriteIdList());
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
      }
      if (request.isSetNeedMerge() && request.isNeedMerge()) {
        return updateTableColumnStatsWithMerge(catName, dbName, tableName, colNames, request);
      } else {
        // This is the overwrite case, we do not care about the accuracy.
        return updateTableColumnStatsInternal(firstColStats,
            request.getValidWriteIdList(), request.getWriteId());
      }
    } else {
      // partition level column stats merging
      // note that we may have two or more duplicate partition names.
      // see autoColumnStats_2.q under TestMiniLlapLocalCliDriver
      Map<String, ColumnStatistics> newStatsMap = new HashMap<>();
      for (ColumnStatistics csNew : csNews) {
        String partName = csNew.getStatsDesc().getPartName();
        if (newStatsMap.containsKey(partName)) {
          MetaStoreServerUtils.mergeColStats(csNew, newStatsMap.get(partName));
        }
        newStatsMap.put(partName, csNew);
      }

      if (request.isSetNeedMerge() && request.isNeedMerge()) {
        ret = updatePartColumnStatsWithMerge(catName, dbName, tableName,
            colNames, newStatsMap, request);
      } else { // No merge.
        Table t = getTable(catName, dbName, tableName);
        MTable mTable = getMS().ensureGetMTable(catName, dbName, tableName);
        // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
        if (MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL)) {
          ret = updatePartitionColStatsInBatch(t, newStatsMap,
                  request.getValidWriteIdList(), request.getWriteId());
        } else {
          for (Map.Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
            // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
            ret = updatePartitonColStatsInternal(t, mTable, entry.getValue(),
                    request.getValidWriteIdList(), request.getWriteId()) && ret;
          }
        }
      }
    }
    return ret;
  }

  private boolean updatePartColumnStatsWithMerge(String catName, String dbName, String tableName,
                                                 List<String> colNames, Map<String, ColumnStatistics> newStatsMap, SetPartitionsStatsRequest request)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    RawStore ms = getMS();
    ms.openTransaction();
    boolean isCommitted = false, result = false;
    try {
      // a single call to get all column stats for all partitions
      List<String> partitionNames = new ArrayList<>();
      partitionNames.addAll(newStatsMap.keySet());
      List<ColumnStatistics> csOlds = ms.getPartitionColumnStatistics(catName, dbName, tableName,
          partitionNames, colNames, request.getEngine(), request.getValidWriteIdList());
      if (newStatsMap.values().size() != csOlds.size()) {
        // some of the partitions miss stats.
        LOG.debug("Some of the partitions miss stats.");
      }
      Map<String, ColumnStatistics> oldStatsMap = new HashMap<>();
      for (ColumnStatistics csOld : csOlds) {
        oldStatsMap.put(csOld.getStatsDesc().getPartName(), csOld);
      }

      // another single call to get all the partition objects
      List<Partition> partitions = ms.getPartitionsByNames(catName, dbName, tableName, partitionNames);
      Map<String, Partition> mapToPart = new HashMap<>();
      for (int index = 0; index < partitionNames.size(); index++) {
        mapToPart.put(partitionNames.get(index), partitions.get(index));
      }

      Table t = getTable(catName, dbName, tableName);
      MTable mTable = getMS().ensureGetMTable(catName, dbName, tableName);
      Map<String, ColumnStatistics> statsMap =  new HashMap<>();
      boolean useDirectSql = MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL);
      for (Map.Entry<String, ColumnStatistics> entry : newStatsMap.entrySet()) {
        ColumnStatistics csNew = entry.getValue();
        ColumnStatistics csOld = oldStatsMap.get(entry.getKey());
        boolean isInvalidTxnStats = csOld != null
            && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
        Partition part = mapToPart.get(entry.getKey());
        if (isInvalidTxnStats) {
          // No columns can be merged; a shortcut for getMergableCols.
          csNew.setStatsObj(Lists.newArrayList());
        } else {
          // we first use getParameters() to prune the stats
          MetaStoreServerUtils.getMergableCols(csNew, part.getParameters());
          // we merge those that can be merged
          if (csOld != null && csOld.getStatsObjSize() != 0 && !csNew.getStatsObj().isEmpty()) {
            MetaStoreServerUtils.mergeColStats(csNew, csOld);
          }
        }

        if (!csNew.getStatsObj().isEmpty()) {
          // We don't short-circuit on errors here anymore. That can leave acid stats invalid.
          if (useDirectSql) {
            statsMap.put(csNew.getStatsDesc().getPartName(), csNew);
          } else {
            result = updatePartitonColStatsInternal(t, mTable, csNew,
                    request.getValidWriteIdList(), request.getWriteId()) && result;
          }
        } else if (isInvalidTxnStats) {
          // For now because the stats state is such as it is, we will invalidate everything.
          // Overall the sematics here are not clear - we could invalide only some columns, but does
          // that make any physical sense? Could query affect some columns but not others?
          part.setWriteId(request.getWriteId());
          StatsSetupConst.clearColumnStatsState(part.getParameters());
          StatsSetupConst.setBasicStatsState(part.getParameters(), StatsSetupConst.FALSE);
          ms.alterPartition(catName, dbName, tableName, part.getValues(), part,
              request.getValidWriteIdList());
          result = false;
        } else {
          // TODO: why doesn't the original call for non acid tables invalidate the stats?
          LOG.debug("All the column stats " + csNew.getStatsDesc().getPartName()
              + " are not accurate to merge.");
        }
      }
      ms.commitTransaction();
      isCommitted = true;
      // updatePartitionColStatsInBatch starts/commit transaction internally. As there is no write or select for update
      // operations is done in this transaction, it is safe to commit it before calling updatePartitionColStatsInBatch.
      if (!statsMap.isEmpty()) {
        updatePartitionColStatsInBatch(t, statsMap,  request.getValidWriteIdList(), request.getWriteId());
      }
    } finally {
      if (!isCommitted) {
        ms.rollbackTransaction();
      }
    }
    return result;
  }


  private boolean updateTableColumnStatsWithMerge(String catName, String dbName, String tableName,
                                                  List<String> colNames, SetPartitionsStatsRequest request) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    ColumnStatistics firstColStats = request.getColStats().get(0);
    RawStore ms = getMS();
    ms.openTransaction();
    boolean isCommitted = false, result = false;
    try {
      ColumnStatistics csOld = ms.getTableColumnStatistics(catName, dbName, tableName, colNames,
          request.getEngine(), request.getValidWriteIdList());
      // we first use the valid stats list to prune the stats
      boolean isInvalidTxnStats = csOld != null
          && csOld.isSetIsStatsCompliant() && !csOld.isIsStatsCompliant();
      if (isInvalidTxnStats) {
        // No columns can be merged; a shortcut for getMergableCols.
        firstColStats.setStatsObj(Lists.newArrayList());
      } else {
        Table t = getTable(catName, dbName, tableName);
        MetaStoreServerUtils.getMergableCols(firstColStats, t.getParameters());

        // we merge those that can be merged
        if (csOld != null && csOld.getStatsObjSize() != 0 && !firstColStats.getStatsObj().isEmpty()) {
          MetaStoreServerUtils.mergeColStats(firstColStats, csOld);
        }
      }

      if (!firstColStats.getStatsObj().isEmpty()) {
        result = updateTableColumnStatsInternal(firstColStats,
            request.getValidWriteIdList(), request.getWriteId());
      } else if (isInvalidTxnStats) {
        // For now because the stats state is such as it is, we will invalidate everything.
        // Overall the sematics here are not clear - we could invalide only some columns, but does
        // that make any physical sense? Could query affect some columns but not others?
        Table t = getTable(catName, dbName, tableName);
        t.setWriteId(request.getWriteId());
        StatsSetupConst.clearColumnStatsState(t.getParameters());
        StatsSetupConst.setBasicStatsState(t.getParameters(), StatsSetupConst.FALSE);
        ms.alterTable(catName, dbName, tableName, t, request.getValidWriteIdList());
      } else {
        // TODO: why doesn't the original call for non acid tables invalidate the stats?
        LOG.debug("All the column stats are not accurate to merge.");
        result = true;
      }

      ms.commitTransaction();
      isCommitted = true;
    } finally {
      if (!isCommitted) {
        ms.rollbackTransaction();
      }
    }
    return result;
  }

  private Table getTable(String catName, String dbName, String tableName)
      throws MetaException, InvalidObjectException {
    return getTable(catName, dbName, tableName, null);
  }

  private Table getTable(String catName, String dbName, String tableName,
                         String writeIdList)
      throws MetaException, InvalidObjectException {
    Table t = getMS().getTable(catName, dbName, tableName, writeIdList);
    if (t == null) {
      throw new InvalidObjectException(TableName.getQualified(catName, dbName, tableName)
          + " table not found");
    }
    return t;
  }

  @Override
  public NotificationEventResponse get_next_notification(NotificationEventRequest rqst)
      throws TException {
    authorizeProxyPrivilege();

    RawStore ms = getMS();
    return ms.getNextNotification(rqst);
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    authorizeProxyPrivilege();

    RawStore ms = getMS();
    return ms.getCurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest rqst)
      throws TException {
    authorizeProxyPrivilege();

    RawStore ms = getMS();
    return ms.getNotificationEventsCount(rqst);
  }

  private void authorizeProxyPrivilege() throws TException {
    // Skip the auth in embedded mode or if the auth is disabled
    if (!HiveMetaStore.isMetaStoreRemote() ||
        !MetastoreConf.getBoolVar(conf, ConfVars.EVENT_DB_NOTIFICATION_API_AUTH) || conf.getBoolean(HIVE_IN_TEST.getVarname(),
        false)) {
      return;
    }
    String user = null;
    try {
      user = SecurityUtils.getUGI().getShortUserName();
    } catch (Exception ex) {
      LOG.error("Cannot obtain username", ex);
      throw new TException(ex);
    }
    if (!MetaStoreServerUtils.checkUserHasHostProxyPrivileges(user, conf, getIPAddress())) {
      LOG.error("Not authorized to make the get_notification_events_count call. You can try to disable " +
          ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString());
      throw new TException("User " + user + " is not allowed to perform this API call");
    }
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    switch (rqst.getData().getSetField()) {
    case INSERT_DATA:
    case INSERT_DATAS:
      String catName =
          rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
      String dbName = rqst.getDbName();
      String tblName = rqst.getTableName();
      boolean isSuccessful = rqst.isSuccessful();
      List<InsertEvent> events = new ArrayList<>();
      if (rqst.getData().isSetInsertData()) {
        events.add(new InsertEvent(catName, dbName, tblName,
            rqst.getPartitionVals(),
            rqst.getData().getInsertData(), isSuccessful, this));
      } else {
        // this is a bulk fire insert event operation
        // we use the partition values field from the InsertEventRequestData object
        // instead of the FireEventRequest object
        for (InsertEventRequestData insertData : rqst.getData().getInsertDatas()) {
          if (!insertData.isSetPartitionVal()) {
            throw new MetaException(
                "Partition values must be set when firing multiple insert events");
          }
          events.add(new InsertEvent(catName, dbName, tblName,
              insertData.getPartitionVal(),
              insertData, isSuccessful, this));
        }
      }
      FireEventResponse response = new FireEventResponse();
      for (InsertEvent event : events) {
        /*
         * The transactional listener response will be set already on the event, so there is not need
         * to pass the response to the non-transactional listener.
         */
        MetaStoreListenerNotifier
            .notifyEvent(transactionalListeners, EventType.INSERT, event);
        MetaStoreListenerNotifier.notifyEvent(listeners, EventType.INSERT, event);
        if (event.getParameters() != null && event.getParameters()
            .containsKey(
                MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
          response.addToEventIds(Long.valueOf(event.getParameters()
              .get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)));
        } else {
          String msg = "Insert event id not generated for ";
          if (event.getPartitionObj() != null) {
            msg += "partition " + Arrays
                .toString(event.getPartitionObj().getValues().toArray()) + " of ";
          }
          msg +=
              " of table " + event.getTableObj().getDbName() + "." + event.getTableObj()
                  .getTableName();
          LOG.warn(msg);
        }
      }
      return response;
    case REFRESH_EVENT:
      response = new FireEventResponse();
      catName = rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
      dbName = rqst.getDbName();
      tblName = rqst.getTableName();
      List<String> partitionVals = rqst.getPartitionVals();
      Map<String, String> tableParams = rqst.getTblParams();
      ReloadEvent event = new ReloadEvent(catName, dbName, tblName, partitionVals, rqst.isSuccessful(),
              rqst.getData().getRefreshEvent(), tableParams, this);
      MetaStoreListenerNotifier
              .notifyEvent(transactionalListeners, EventType.RELOAD, event);
      MetaStoreListenerNotifier.notifyEvent(listeners, EventType.RELOAD, event);
      if (event.getParameters() != null && event.getParameters()
              .containsKey(
                      MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)) {
        response.addToEventIds(Long.valueOf(event.getParameters()
                .get(MetaStoreEventListenerConstants.DB_NOTIFICATION_EVENT_ID_KEY_NAME)));
      } else {
        String msg = "Reload event id not generated for ";
        if (event.getPartitionObj() != null) {
          msg += "partition " + Arrays
                  .toString(event.getPartitionObj().getValues().toArray()) + " of ";
        }
        msg +=
                " of table " + event.getTableObj().getDbName() + "." + event.getTableObj()
                        .getTableName();
        LOG.warn(msg);
      }
      return response;
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
      result.setMetadata(Collections.emptyMap()); // Set the required field.
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
      result.setMetadata(Collections.emptyMap()); // Set the required field.
    }
    return result;
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    GetFileMetadataResult result = new GetFileMetadataResult();
    RawStore ms = getMS();
    if (!ms.isFileMetadataSupported()) {
      result.setIsSupported(false);
      result.setMetadata(Collections.emptyMap()); // Set the required field.
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
      result.setMetadata(Collections.emptyMap()); // Set the required field.
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
      Table tbl = ms.getTable(getDefaultCatalog(conf), dbName, tblName);
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
          partNames = ms.listPartitionNames(getDefaultCatalog(conf), dbName, tblName, (short)-1);
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
          List<Partition> parts = ms.getPartitionsByNames(getDefaultCatalog(conf), dbName, tblName, nameBatch);
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
    if (Metrics.getRegistry() != null) {
      LOG.info("Begin calculating metadata count metrics.");
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_DATABASES).set(getMS().getTableCount());
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_TABLES).set(getMS().getPartitionCount());
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_PARTITIONS).set(getMS().getDatabaseCount());
    }
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_primary_keys", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLPrimaryKey> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getPrimaryKeys(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_primary_keys", ret != null, ex, request.getTbl_name());
    }
    return new PrimaryKeysResponse(ret);
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startFunction("get_foreign_keys",
        " : parentdb=" + request.getParent_db_name() + " parenttbl=" + request.getParent_tbl_name() + " foreigndb="
            + request.getForeign_db_name() + " foreigntbl=" + request.getForeign_tbl_name());
    List<SQLForeignKey> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getForeignKeys(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_foreign_keys", ret != null, ex, request.getForeign_tbl_name());
    }
    return new ForeignKeysResponse(ret);
  }

  @Override
  public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_unique_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLUniqueConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getUniqueConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_unique_constraints", ret != null, ex, request.getTbl_name());
    }
    return new UniqueConstraintsResponse(ret);
  }

  @Override
  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_not_null_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLNotNullConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getNotNullConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_not_null_constraints", ret != null, ex, request.getTbl_name());
    }
    return new NotNullConstraintsResponse(ret);
  }

  @Override
  public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_default_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLDefaultConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getDefaultConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_default_constraints", ret != null, ex, request.getTbl_name());
    }
    return new DefaultConstraintsResponse(ret);
  }

  @Override
  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request) throws TException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_check_constraints", request.getCatName(), request.getDb_name(), request.getTbl_name());
    List<SQLCheckConstraint> ret = null;
    Exception ex = null;
    try {
      ret = getMS().getCheckConstraints(request);
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("get_check_constraints", ret != null, ex, request.getTbl_name());
    }
    return new CheckConstraintsResponse(ret);
  }

  /**
   * Api to fetch all table constraints at once.
   * @param request it consist of catalog name, database name and table name to identify the table in metastore
   * @return all constraints attached to given table
   * @throws TException
   */
  @Override
  public AllTableConstraintsResponse get_all_table_constraints(AllTableConstraintsRequest request)
      throws TException, MetaException, NoSuchObjectException {
    request.setCatName(request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf));
    startTableFunction("get_all_table_constraints", request.getCatName(), request.getDbName(), request.getTblName());
    SQLAllTableConstraints ret = null;
    Exception ex = null;
    try {
      ret = getMS().getAllTableConstraints(request);
    } catch (Exception e) {
      ex = e;
      throwMetaException(e);
    } finally {
      endFunction("get_all_table_constraints", ret != null, ex, request.getTblName());
    }
    return new AllTableConstraintsResponse(ret);
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
      WMFullResourcePlan rp = getMS().getResourcePlan(request.getResourcePlanName(), request.getNs());
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
      resp.setResourcePlans(getMS().getAllResourcePlans(request.getNs()));
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
          request.getResourcePlanName(), request.getNs(), request.getResourcePlan(),
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
      response.setResourcePlan(getMS().getActiveResourcePlan(request.getNs()));
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
      return getMS().validateResourcePlan(request.getResourcePlanName(), request.getNs());
    } catch (MetaException e) {
      LOG.error("Exception while trying to validate resource plan", e);
      throw e;
    }
  }

  @Override
  public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropResourcePlan(request.getResourcePlanName(), request.getNs());
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
      getMS().dropWMTrigger(request.getResourcePlanName(), request.getTriggerName(), request.getNs());
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
          getMS().getTriggersForResourcePlan(request.getResourcePlanName(), request.getNs());
      WMGetTriggersForResourePlanResponse response = new WMGetTriggersForResourePlanResponse();
      response.setTriggers(triggers);
      return response;
    } catch (MetaException e) {
      LOG.error("Exception while trying to retrieve triggers plans", e);
      throw e;
    }
  }

  @Override
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

  @Override
  public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest request)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    try {
      getMS().dropWMPool(request.getResourcePlanName(), request.getPoolPath(), request.getNs());
      return new WMDropPoolResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to drop WMPool", e);
      throw e;
    }
  }

  @Override
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

  @Override
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

  @Override
  public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
      WMCreateOrDropTriggerToPoolMappingRequest request) throws AlreadyExistsException,
      NoSuchObjectException, InvalidObjectException, MetaException, TException {
    try {
      if (request.isDrop()) {
        getMS().dropWMTriggerToPoolMapping(request.getResourcePlanName(),
            request.getTriggerName(), request.getPoolPath(), request.getNs());
      } else {
        getMS().createWMTriggerToPoolMapping(request.getResourcePlanName(),
            request.getTriggerName(), request.getPoolPath(), request.getNs());
      }
      return new WMCreateOrDropTriggerToPoolMappingResponse();
    } catch (MetaException e) {
      LOG.error("Exception while trying to create or drop pool mappings", e);
      throw e;
    }
  }

  @Override
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
        if (!success) {
          ms.rollbackTransaction();
        }
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
      if (!success) {
        ms.rollbackTransaction();
      }
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

  @Override
  public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId)
      throws TException {
    return getTxnHandler().lockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Override
  public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId)
      throws TException {
    return getTxnHandler().heartbeatLockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Override
  public void add_runtime_stats(RuntimeStat stat) throws TException {
    startFunction("store_runtime_stats");
    Exception ex = null;
    boolean success = false;
    RawStore ms = getMS();
    try {
      ms.openTransaction();
      ms.addRuntimeStat(stat);
      success = ms.commitTransaction();
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      endFunction("store_runtime_stats", success, ex);
    }
  }

  @Override
  public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest rqst) throws TException {
    startFunction("get_runtime_stats");
    Exception ex = null;
    try {
      List<RuntimeStat> res = getMS().getRuntimeStats(rqst.getMaxWeight(), rqst.getMaxCreateTime());
      return res;
    } catch (MetaException e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_runtime_stats", ex == null, ex);
    }
  }

  @Override
  public ScheduledQueryPollResponse scheduled_query_poll(ScheduledQueryPollRequest request)
      throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      RawStore ms = getMS();
      return ms.scheduledQueryPoll(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  @Override
  public void scheduled_query_maintenance(ScheduledQueryMaintenanceRequest request) throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      RawStore ms = getMS();
      ms.scheduledQueryMaintenance(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  @Override
  public void scheduled_query_progress(ScheduledQueryProgressInfo info) throws MetaException, TException {
    startFunction("scheduled_query_poll");
    Exception ex = null;
    try {
      RawStore ms = getMS();
      ms.scheduledQueryProgress(info);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("scheduled_query_poll", ex == null, ex);
    }
  }

  @Override
  public ScheduledQuery get_scheduled_query(ScheduledQueryKey scheduleKey) throws TException {
    startFunction("get_scheduled_query");
    Exception ex = null;
    try {
      return getMS().getScheduledQuery(scheduleKey);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_scheduled_query", ex == null, ex);
    }
  }

  @Override
  public void add_replication_metrics(ReplicationMetricList replicationMetricList) throws MetaException{
    startFunction("add_replication_metrics");
    Exception ex = null;
    try {
      getMS().addReplicationMetrics(replicationMetricList);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("add_replication_metrics", ex == null, ex);
    }
  }

  @Override
  public ReplicationMetricList get_replication_metrics(GetReplicationMetricsRequest
                                                           getReplicationMetricsRequest) throws MetaException{
    startFunction("get_replication_metrics");
    Exception ex = null;
    try {
      return getMS().getReplicationMetrics(getReplicationMetricsRequest);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_replication_metrics", ex == null, ex);
    }
  }

  @Override
  public void create_stored_procedure(StoredProcedure proc) throws NoSuchObjectException, MetaException {
    startFunction("create_stored_procedure");
    Exception ex = null;

    throwUnsupportedExceptionIfRemoteDB(proc.getDbName(), "create_stored_procedure");
    try {
      getMS().createOrUpdateStoredProcedure(proc);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("create_stored_procedure", ex == null, ex);
    }
  }

  public StoredProcedure get_stored_procedure(StoredProcedureRequest request) throws MetaException, NoSuchObjectException {
    startFunction("get_stored_procedure");
    Exception ex = null;
    try {
      StoredProcedure proc = getMS().getStoredProcedure(request.getCatName(), request.getDbName(), request.getProcName());
        if (proc == null) {
          throw new NoSuchObjectException(
                  "HPL/SQL StoredProcedure " + request.getDbName() + "." + request.getProcName() + " does not exist");
        }
        return proc;
    } catch (Exception e) {
      if (!(e instanceof NoSuchObjectException)) {
        LOG.error("Caught exception", e);
      }
      ex = e;
      throw e;
    } finally {
      endFunction("get_stored_procedure", ex == null, ex);
    }
  }

  @Override
  public void drop_stored_procedure(StoredProcedureRequest request) throws MetaException {
    startFunction("drop_stored_procedure");
    Exception ex = null;
    try {
      getMS().dropStoredProcedure(request.getCatName(), request.getDbName(), request.getProcName());
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_stored_procedure", ex == null, ex);
    }
  }

  @Override
  public List<String> get_all_stored_procedures(ListStoredProcedureRequest request) throws MetaException {
    startFunction("get_all_stored_procedures");
    Exception ex = null;
    try {
      return getMS().getAllStoredProcedures(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_stored_procedures", ex == null, ex);
    }
  }

public Package find_package(GetPackageRequest request) throws MetaException, NoSuchObjectException {
    startFunction("find_package");
    Exception ex = null;
    try {
      Package pkg = getMS().findPackage(request);
      if (pkg == null) {
        throw new NoSuchObjectException(
                "HPL/SQL package " + request.getDbName() + "." + request.getPackageName() + " does not exist");
      }
      return pkg;
    } catch (Exception e) {
      if (!(e instanceof NoSuchObjectException)) {
        LOG.error("Caught exception", e);
      }
      ex = e;
      throw e;
    } finally {
      endFunction("find_package", ex == null, ex);
    }
  }

  public void add_package(AddPackageRequest request) throws MetaException, NoSuchObjectException {
    startFunction("add_package");
    Exception ex = null;
    try {
      getMS().addPackage(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("add_package", ex == null, ex);
    }
  }

  public List<String> get_all_packages(ListPackageRequest request) throws MetaException {
    startFunction("get_all_packages");
    Exception ex = null;
    try {
      return getMS().listPackages(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_packages", ex == null, ex);
    }
  }

  public void drop_package(DropPackageRequest request) throws MetaException {
    startFunction("drop_package");
    Exception ex = null;
    try {
      getMS().dropPackage(request);
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("drop_package", ex == null, ex);
    }
  }

  @Override
  public List<WriteEventInfo> get_all_write_event_info(GetAllWriteEventInfoRequest request)
      throws MetaException {
    startFunction("get_all_write_event_info");
    Exception ex = null;
    try {
      List<WriteEventInfo> writeEventInfoList =
          getMS().getAllWriteEventInfo(request.getTxnId(), request.getDbName(), request.getTableName());
      return writeEventInfoList == null ? Collections.emptyList() : writeEventInfoList;
    } catch (Exception e) {
      LOG.error("Caught exception", e);
      ex = e;
      throw e;
    } finally {
      endFunction("get_all_write_event_info", ex == null, ex);
    }
  }
}
