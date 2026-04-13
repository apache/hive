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

package org.apache.hadoop.hive.metastore.handler;

import com.codahale.metrics.Counter;
import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Striped;

import javax.jdo.JDOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.AcidEventListener;
import org.apache.hadoop.hive.metastore.AlterHandler;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.FileMetadataManager;
import org.apache.hadoop.hive.metastore.HMSHandlerContext;
import org.apache.hadoop.hive.metastore.HMSMetricsListener;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreMetadataTransformer;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionContext;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionListener;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.MetaStoreInit;
import org.apache.hadoop.hive.metastore.MetaStoreInitContext;
import org.apache.hadoop.hive.metastore.MetaStoreInitListener;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.SessionPropertiesListener;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.HMSHandler.createDefaultCatalog;
import static org.apache.hadoop.hive.metastore.HMSHandler.getIPAddress;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependCatalogToDbName;

/**
 * This class serves as the super class for all handlers that implement the IHMSHandler
 */
public abstract class BaseHandler extends FacebookBase implements IHMSHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BaseHandler.class);
  private static String currentUrl;
  private static volatile Striped<Lock> tablelocks;

  public static final String ADMIN = "admin";
  public static final String PUBLIC = "public";

  static final int LOG_SAMPLE_PARTITIONS_MAX_SIZE = 4;
  static final int LOG_SAMPLE_PARTITIONS_HALF_SIZE = 2;
  static final String LOG_SAMPLE_PARTITIONS_SEPARATOR = ",";

  protected final Configuration conf;
  protected FileMetadataManager fileMetadataManager;
  protected PartitionExpressionProxy expressionProxy;
  protected IMetaStoreMetadataTransformer transformer;
  protected Warehouse wh; // hdfs warehouse
  protected AlterHandler alterHandler;
  protected List<MetaStorePreEventListener> preListeners;
  protected List<MetaStoreEventListener> listeners;
  protected List<TransactionalMetaStoreEventListener> transactionalListeners;
  protected List<MetaStoreEndFunctionListener> endFunctionListeners;
  protected List<MetaStoreInitListener> initListeners;
  protected MetaStoreFilterHook filterHook;
  protected boolean isServerFilterEnabled = false;

  protected BaseHandler(String name, Configuration conf) {
    super(name);
    this.conf = conf;
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
  public IMetaStoreMetadataTransformer getMetadataTransformer() {
    return transformer;
  }

  @Override
  public MetaStoreFilterHook getMetaFilterHook() {
    return filterHook;
  }

  // Make it possible for tests to check that the right type of PartitionExpressionProxy was
  // instantiated.
  @VisibleForTesting
  public PartitionExpressionProxy getExpressionProxy() {
    return expressionProxy;
  }

  @Override
  public void init() throws MetaException {
    init(new Warehouse(conf));
  }

  @VisibleForTesting
  public void init(Warehouse wh) throws MetaException {
    Metrics.initialize(conf);

    initListeners = MetaStoreServerUtils.getMetaStoreListeners(
        MetaStoreInitListener.class, conf, MetastoreConf.getVar(conf, MetastoreConf.ConfVars.INIT_HOOKS));
    for (MetaStoreInitListener singleInitListener: initListeners) {
      MetaStoreInitContext context = new MetaStoreInitContext();
      singleInitListener.onInit(context);
    }

    String alterHandlerName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ALTER_HANDLER);
    alterHandler = ReflectionUtils.newInstance(JavaUtils.getClass(
        alterHandlerName, AlterHandler.class), conf);
    this.wh = wh;

    initDefaultSchema();

    preListeners = MetaStoreServerUtils.getMetaStoreListeners(MetaStorePreEventListener.class,
        conf, MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PRE_EVENT_LISTENERS));
    preListeners.add(0, new TransactionalValidationListener(conf));
    listeners = MetaStoreServerUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.EVENT_LISTENERS));
    listeners.add(new SessionPropertiesListener(conf));
    transactionalListeners = new ArrayList<>() {{
      add(new AcidEventListener(conf));
    }};
    transactionalListeners.addAll(MetaStoreServerUtils.getMetaStoreListeners(
        TransactionalMetaStoreEventListener.class, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS)));
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
    if (conf.getBoolean(MetastoreConf.ConfVars.METASTORE_CACHE_CAN_USE_EVENT.getVarname(), false) &&
        !canCachedStoreCanUseEvent) {
      throw new MetaException("CachedStore can not use events for invalidation as there is no " +
          " TransactionalMetaStoreEventListener to add events to notification table");
    }

    endFunctionListeners = MetaStoreServerUtils.getMetaStoreListeners(
        MetaStoreEndFunctionListener.class, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.END_FUNCTION_LISTENERS));

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
        LOG.error("Unable to create instance of class {}", className, e);
        throw new IllegalArgumentException(e);
      }
    }
  }

  private void initDefaultSchema() throws MetaException {
    synchronized (BaseHandler.class) {
      if (currentUrl == null || !currentUrl.equals(MetaStoreInit.getConnectionURL(conf))) {
        createDefaultDB();
        createDefaultRoles();
        addAdminUsers();
        currentUrl = MetaStoreInit.getConnectionURL(conf);
        updateMetrics();
      }

      if (tablelocks == null) {
        int numTableLocks = MetastoreConf.getIntVar(conf,
            MetastoreConf.ConfVars.METASTORE_NUM_STRIPED_TABLE_LOCKS);
        tablelocks = Striped.lock(numTableLocks);
      }
    }
  }

  /**
   *
   * Filter is actually enabled only when the configured filter hook is configured, not default, and
   * enabled in configuration
   * @return
   */
  private boolean getIfServerFilterenabled() throws MetaException{
    boolean isEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_SERVER_FILTER_ENABLED);
    if (!isEnabled) {
      LOG.info("HMS server filtering is disabled by configuration");
      return false;
    }
    String filterHookClassName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.FILTER_HOOK);

    if (isBlank(filterHookClassName)) {
      throw new MetaException("HMS server filtering is enabled but no filter hook is configured");
    }
    if (filterHookClassName.trim().equalsIgnoreCase(DefaultMetaStoreFilterHookImpl.class.getName())) {
      throw new MetaException("HMS server filtering is enabled but the filter hook is DefaultMetaStoreFilterHookImpl,"
          + " which does no filtering");
    }
    LOG.info("HMS server filtering is enabled. The filter class is {}", filterHookClassName);
    return true;
  }

  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException  {
    String errorMsg = "Unable to load filter hook at HMS server. ";

    String filterHookClassName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.FILTER_HOOK);
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

  @Override
  public void setConf(Configuration conf) {
    HMSHandlerContext.setConfiguration(conf);
    // reload if DS related configuration is changed
    HMSHandlerContext.getRawStore().ifPresent(ms -> ms.setConf(conf));
  }

  @Override
  public Configuration getConf() {
    return HMSHandlerContext.getConfiguration()
        .orElseGet(
            () -> {
              Configuration configuration = new Configuration(this.conf);
              HMSHandlerContext.setConfiguration(configuration);
              return configuration;
            });
  }

  @Override
  public Warehouse getWh() {
    return wh;
  }

  @Override
  public fb_status getStatus() {
    return fb_status.ALIVE;
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
  public TxnStore getTxnHandler() {
    return HMSHandlerContext.getTxnStore(conf);
  }

  @Override
  public String getVersion() throws TException {
    return MetastoreVersionInfo.getVersion();
  }

  protected static void logAndAudit(final String m) {
    LOG.debug(m);
    logAuditEvent(m);
  }

  // This will return null if the metastore is not being accessed from a metastore Thrift server,
  // or if the TTransport being used to connect is not an instance of TSocket, or if kerberos
  // is used
  public static String getThreadLocalIpAddress() {
    return HMSHandlerContext.getIpAddress().orElse(null);
  }

  public static void setThreadLocalIpAddress(String ipAddress) {
    HMSHandlerContext.setIpAddress(ipAddress);
  }

  protected String startFunction(String function, String extraLogInfo) {
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

  protected String startFunction(String function) {
    return startFunction(function, "");
  }

  protected void startTableFunction(String function, String catName, String db, String tbl) {
    startFunction(function, " : tbl=" +
        TableName.getQualified(catName, db, tbl));
  }

  protected void startMultiTableFunction(String function, String db, List<String> tbls) {
    String tableNames = join(tbls, ",");
    startFunction(function, " : db=" + db + " tbls=" + tableNames);
  }

  protected void startPartitionFunction(String function, String cat, String db, String tbl,
      List<String> partVals) {
    startFunction(function, " : tbl=" +
        TableName.getQualified(cat, db, tbl) + samplePartitionValues(partVals));
  }

  protected void startPartitionFunction(String function, String catName, String db, String tbl,
      Map<String, String> partName) {
    startFunction(function, " : tbl=" +
        TableName.getQualified(catName, db, tbl) + " partition=" + partName);
  }

  protected void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Max partitions =" + maxParts);
  }

  protected void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
      List<String> partVals) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Max partitions =" + maxParts
        + samplePartitionValues(partVals));
  }

  protected void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
      String filter) {
    startFunction(function,
        " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Filter=" + filter + ": Max partitions ="
            + maxParts);
  }

  protected void startPartitionFunction(String function, String catName, String db, String tbl, int maxParts,
      String expression, String defaultPartitionName) {
    startFunction(function, " : tbl=" + TableName.getQualified(catName, db, tbl) + ": Expression=" + expression
        + ": Default partition name=" + defaultPartitionName + ": Max partitions=" + maxParts);
  }

  protected String getGroupsCountAndUsername(final String user_name, final List<String> group_names) {
    return ". Number of groups= " + (group_names == null ? 0 : group_names.size()) + ", user name= " + user_name;
  }

  protected String samplePartitionValues(List<String> partVals) {
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

  protected void endFunction(String function, boolean successful, Exception e) {
    endFunction(function, successful, e, null);
  }

  protected void endFunction(String function, boolean successful, Exception e,
      String inputTableName) {
    endFunction(function, new MetaStoreEndFunctionContext(successful, e, inputTableName));
  }

  protected void endFunction(String function, MetaStoreEndFunctionContext context) {
    com.codahale.metrics.Timer.Context timerContext = HMSHandlerContext.getTimerContexts().remove(function);
    if (timerContext != null) {
      long timeTaken = timerContext.stop();
      LOG.debug((getThreadLocalIpAddress() == null ? "" : "source:" + getThreadLocalIpAddress() + " ") +
          function + " time taken(ns): " + timeTaken);
    }
    Counter counter = Metrics.getOrCreateCounter(MetricsConstants.ACTIVE_CALLS + function);
    if (counter != null) {
      counter.dec();
    }

    for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
      listener.onEndFunction(function, context);
    }
  }

  static final Logger auditLog = LoggerFactory.getLogger(HiveMetaStore.class.getName() + ".audit");

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

  @Override
  public void flushCache() throws TException {
    getMS().flushCache();
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws TException {
    if (part_name.isEmpty()) {
      return Collections.emptyList();
    }
    LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
    return new ArrayList<>(map.values());
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name) throws TException {
    if (part_name.isEmpty()) {
      return new HashMap<>();
    }
    return Warehouse.makeSpecFromName(part_name);
  }

  @VisibleForTesting
  void updateMetrics() throws MetaException {
    if (Metrics.getRegistry() != null &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.INIT_METADATA_COUNT_ENABLED)) {
      LOG.info("Begin calculating metadata count metrics.");
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_TABLES).set(getMS().getTableCount());
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_PARTITIONS).set(getMS().getPartitionCount());
      Metrics.getOrCreateGauge(MetricsConstants.TOTAL_DATABASES).set(getMS().getDatabaseCount());
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
      LOG.warn("Retrying adding admin users after error: {}", e.getMessage(), e);
      addAdminUsers_core();
    }
  }

  private void addAdminUsers_core() throws MetaException {

    // now add pre-configured users to admin role
    String userStr = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.USERS_IN_ADMIN_ROLE,"").trim();
    if (userStr.isEmpty()) {
      LOG.info("No user is added in admin role, since config is empty");
      return;
    }
    // Since user names need to be valid unix user names, per IEEE Std 1003.1-2001 they cannot
    // contain comma, so we can safely split above string on comma.

    Iterator<String> users = Splitter.on(",").trimResults().omitEmptyStrings().split(userStr).iterator();
    if (!users.hasNext()) {
      LOG.info("No user is added in admin role, since config value {} is in incorrect format." +
              " We accept comma separated list of users.", userStr);
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
        LOG.info("Added {} to admin role", userName);
      } catch (NoSuchObjectException e) {
        LOG.error("Failed to add {} in admin role", userName, e);
      } catch (InvalidObjectException e) {
        LOG.debug("{} already in admin role", userName, e);
      }
    }
  }

  private void createDefaultDB_core(RawStore ms) throws MetaException, InvalidObjectException {
    try {
      ms.getDatabase(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME);
    } catch (NoSuchObjectException e) {
      LOG.info("Started creating a default database with name: {}", DEFAULT_DATABASE_NAME);
      Database db = new Database(DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_COMMENT,
          wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME, true).toString(), null);
      db.setOwnerName(PUBLIC);
      db.setOwnerType(PrincipalType.ROLE);
      db.setCatalogName(DEFAULT_CATALOG_NAME);
      long time = System.currentTimeMillis() / 1000;
      db.setCreateTime((int) time);
      db.setType(DatabaseType.NATIVE);
      ms.createDatabase(db);
      LOG.info("Successfully created a default database with name: {}", DEFAULT_DATABASE_NAME);
    }
  }

  @Override
  public String getCpuProfile(int i) throws TException {
    return "";
  }

  protected void throwUnsupportedExceptionIfRemoteDB(String dbName, String operationName) throws MetaException {
    if (isDatabaseRemote(dbName)) {
      throw new MetaException("Operation " + operationName + " not supported for REMOTE database " + dbName);
    }
  }

  protected boolean isDatabaseRemote(String name) {
    try {
      String[] parsedDbName = parseDbName(name, conf);
      Database db = get_database_core(parsedDbName[CAT_NAME], parsedDbName[DB_NAME]);
      return MetaStoreUtils.isDatabaseRemote(db);
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public List<String> set_ugi(String username, List<String> groupNames) throws TException {
    Collections.addAll(groupNames, username);
    return groupNames;
  }

  @Override
  public boolean partition_name_has_valid_characters(
      List<String> part_vals, boolean throw_exception) throws TException {
    startFunction("partition_name_has_valid_characters");
    boolean ret;
    Exception ex = null;
    try {
      if (throw_exception) {
        MetaStoreServerUtils.validatePartitionNameCharacters(part_vals, getConf());
        ret = true;
      } else {
        ret = MetaStoreServerUtils.partitionNameHasValidCharacters(part_vals, getConf());
      }
    } catch (Exception e) {
      ex = e;
      throw newMetaException(e);
    } finally {
      endFunction("partition_name_has_valid_characters", true, ex);
    }
    return ret;
  }

  @Override
  public Map<String, Type> get_type_all(String name) throws MetaException {
    // TODO Auto-generated method stub
    throw new MetaException("Not yet implemented");
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
        LOG.error("RuntimeException thrown in get_config_value - msg: {} cause: {}",
            e.getMessage(), e.getCause());
      }
      success = true;
      return toReturn;
    } catch (Exception e) {
      ex = e;
      throw handleException(e).defaultTException();
    } finally {
      endFunction("get_config_value", success, ex);
    }
  }

  @Override
  public String get_metastore_db_uuid() throws TException {
    return getMS().getMetastoreDbUuid();
  }

  public Lock getTableLockFor(String dbName, String tblName) {
    return tablelocks.get(dbName + "." + tblName);
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
        !MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH) ||
        conf.getBoolean(HIVE_IN_TEST.getVarname(), false)) {
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
          MetastoreConf.ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.toString());
      throw new TException("User " + user + " is not allowed to perform this API call");
    }
  }

  @Override
  public CmRecycleResponse cm_recycle(final CmRecycleRequest request) throws MetaException {
    wh.recycleDirToCmPath(new Path(request.getDataPath()), request.isPurge());
    return new CmRecycleResponse();
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException {
    MetastoreConf.ConfVars confVar = MetastoreConf.getMetaConf(key);
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
      MetaStoreListenerNotifier.notifyEvent(eventListeners, EventMessage.EventType.CONFIG_CHANGE,
          new ConfigChangeEvent(this, key, oldValue, value));
    }
    if (MetastoreConf.ConfVars.TRY_DIRECT_SQL == confVar) {
      LOG.info("Direct SQL optimization = {}",  value);
    }
  }

  /**
   * Internal function to notify listeners to revert back to old values of keys
   * that were modified during setMetaConf. This would get called from HiveMetaStore#cleanupHandlerContext
   */
  public void notifyMetaListenersOnShutDown() {
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
            MetaStoreListenerNotifier.notifyEvent(eventListeners, EventMessage.EventType.CONFIG_CHANGE,
                new ConfigChangeEvent(this, key, oldVal, currVal));
          }
        }
      }
      logAndAudit("Meta listeners shutdown notification completed.");
    } catch (MetaException e) {
      LOG.error("Failed to notify meta listeners on shutdown: ", e);
    }
  }

  @Override
  public String getMetaConf(String key) throws MetaException {
    MetastoreConf.ConfVars confVar = MetastoreConf.getMetaConf(key);
    if (confVar == null) {
      throw new MetaException("Invalid configuration key " + key);
    }
    return getConf().get(key, confVar.getDefaultVal().toString());
  }

  public void firePreEvent(PreEventContext event) throws MetaException {
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

  public void firePreEvent(Supplier<PreEventContext> supplier) throws MetaException {
    if (preListeners.isEmpty()) {
      return;
    }
    firePreEvent(supplier.get());
  }
}
