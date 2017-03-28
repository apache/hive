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

package org.apache.hadoop.hive.metastore;

import static org.apache.commons.lang.StringUtils.join;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import javax.jdo.JDOCanRetryException;
import javax.jdo.JDODataStoreException;
import javax.jdo.JDOException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.identity.IntIdentity;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreDirectSql.SqlFilterForPushdown;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MDelegationToken;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MFunction;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MIndex;
import org.apache.hadoop.hive.metastore.model.MMasterKey;
import org.apache.hadoop.hive.metastore.model.MNotificationLog;
import org.apache.hadoop.hive.metastore.model.MNotificationNextId;
import org.apache.hadoop.hive.metastore.model.MOrder;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MPartitionEvent;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MResourceUri;
import org.apache.hadoop.hive.metastore.model.MRole;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.model.MVersionTable;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.thrift.TException;
import org.datanucleus.AbstractNucleusContext;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassLoaderResolverImpl;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.scostore.Store;
import org.datanucleus.util.WeakValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This class is the interface between the application logic and the database
 * store that contains the objects. Refrain putting any logic in mode.M* objects
 * or in this file as former could be auto generated and this class would need
 * to be made into a interface that can read both from a database and a
 * filestore.
 */
public class ObjectStore implements RawStore, Configurable {
  private static Properties prop = null;
  private static PersistenceManagerFactory pmf = null;

  private static Lock pmfPropLock = new ReentrantLock();
  /**
  * Verify the schema only once per JVM since the db connection info is static
  */
  private final static AtomicBoolean isSchemaVerified = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStore.class.getName());

  private static enum TXN_STATUS {
    NO_STATE, OPEN, COMMITED, ROLLBACK
  }

  private static final Map<String, Class> PINCLASSMAP;
  private static final String HOSTNAME;
  private static final String USER;
  private static final String JDO_PARAM = ":param";
  static {
    Map<String, Class> map = new HashMap<String, Class>();
    map.put("table", MTable.class);
    map.put("storagedescriptor", MStorageDescriptor.class);
    map.put("serdeinfo", MSerDeInfo.class);
    map.put("partition", MPartition.class);
    map.put("database", MDatabase.class);
    map.put("type", MType.class);
    map.put("fieldschema", MFieldSchema.class);
    map.put("order", MOrder.class);
    PINCLASSMAP = Collections.unmodifiableMap(map);
    String hostname = "UNKNOWN";
    try {
      InetAddress clientAddr = InetAddress.getLocalHost();
      hostname = clientAddr.getHostAddress();
    } catch (IOException e) {
    }
    HOSTNAME = hostname;
    String user = System.getenv("USER");
    if (user == null) {
      USER = "UNKNOWN";
    } else {
      USER = user;
    }
  }


  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private MetaStoreDirectSql directSql = null;
  private PartitionExpressionProxy expressionProxy = null;
  private Configuration hiveConf;
  private volatile int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  private Pattern partitionValidationPattern;

  /**
   * A class to pass the Query object to the caller to let the caller release
   * resources by calling QueryWrapper.query.closeAll() after consuming all the query results.
   */
  public static class QueryWrapper {
    public Query query;

    /**
     * Explicitly closes the query object to release the resources
     */
    public void close() {
      if (query != null) {
        query.closeAll();
        query = null;
      }
    }

    @Override
    protected void finalize() {
      this.close();
    }
  }

  public ObjectStore() {
  }

  @Override
  public Configuration getConf() {
    return hiveConf;
  }

  /**
   * Called whenever this object is instantiated using ReflectionUtils, and also
   * on connection retries. In cases of connection retries, conf will usually
   * contain modified values.
   */
  @Override
  @SuppressWarnings("nls")
  public void setConf(Configuration conf) {
    // Although an instance of ObjectStore is accessed by one thread, there may
    // be many threads with ObjectStore instances. So the static variables
    // pmf and prop need to be protected with locks.
    pmfPropLock.lock();
    try {
      isInitialized = false;
      hiveConf = conf;
      configureSSL(conf);
      Properties propsFromConf = getDataSourceProps(conf);
      boolean propsChanged = !propsFromConf.equals(prop);

      if (propsChanged) {
        if (pmf != null){
          clearOutPmfClassLoaderCache(pmf);
        }
        pmf = null;
        prop = null;
      }

      assert(!isActiveTransaction());
      shutdown();
      // Always want to re-create pm as we don't know if it were created by the
      // most recent instance of the pmf
      pm = null;
      directSql = null;
      expressionProxy = null;
      openTrasactionCalls = 0;
      currentTransaction = null;
      transactionStatus = TXN_STATUS.NO_STATE;

      initialize(propsFromConf);

      String partitionValidationRegex =
          hiveConf.get(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.name());
      if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
        partitionValidationPattern = Pattern.compile(partitionValidationRegex);
      } else {
        partitionValidationPattern = null;
      }

      if (!isInitialized) {
        throw new RuntimeException(
        "Unable to create persistence manager. Check dss.log for details");
      } else {
        LOG.info("Initialized ObjectStore");
      }
    } finally {
      pmfPropLock.unlock();
    }
  }

  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ObjectStore.class.getClassLoader();
    }
  }

  @SuppressWarnings("nls")
  private void initialize(Properties dsProps) {
    int retryLimit = HiveConf.getIntVar(hiveConf,
        HiveConf.ConfVars.HMSHANDLERATTEMPTS);
    long retryInterval = HiveConf.getTimeVar(hiveConf,
        HiveConf.ConfVars.HMSHANDLERINTERVAL, TimeUnit.MILLISECONDS);
    int numTries = retryLimit;

    while (numTries > 0){
      try {
        initializeHelper(dsProps);
        return; // If we reach here, we succeed.
      } catch (Exception e){
        numTries--;
        boolean retriable = isRetriableException(e);
        if ((numTries > 0) && retriable){
          LOG.info("Retriable exception while instantiating ObjectStore, retrying. "
              + numTries + " tries left", e);
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ie) {
            // Restore the interrupted status, since we do not want to catch it.
            LOG.debug("Interrupted while sleeping before retrying.",ie);
            Thread.currentThread().interrupt();
          }
          // If we're here, we'll proceed down the next while loop iteration.
        } else {
          // we've reached our limit, throw the last one.
          if (retriable){
            LOG.warn("Exception retry limit reached, not retrying any longer.",
              e);
          } else {
            LOG.debug("Non-retriable exception during ObjectStore initialize.", e);
          }
          throw e;
        }
      }
    }
  }

  private static final Set<Class<? extends Throwable>> retriableExceptionClasses =
      new HashSet<Class<? extends Throwable>>(Arrays.asList(JDOCanRetryException.class));
  /**
   * Helper function for initialize to determine if we should retry an exception.
   * We return true if the exception is of a known type of retriable exceptions, or if one
   * of its recursive .getCause returns a known type of retriable exception.
   */
  private boolean isRetriableException(Throwable e) {
    if (e == null){
      return false;
    }
    if (retriableExceptionClasses.contains(e.getClass())){
      return true;
    }
    for (Class<? extends Throwable> c : retriableExceptionClasses){
      if (c.isInstance(e)){
        return true;
      }
    }

    if (e.getCause() == null){
      return false;
    }
    return isRetriableException(e.getCause());
  }

  /**
   * private helper to do initialization routine, so we can retry if needed if it fails.
   * @param dsProps
   */
  private void initializeHelper(Properties dsProps) {
    LOG.info("ObjectStore, initialize called");
    prop = dsProps;
    pm = getPersistenceManager();
    isInitialized = pm != null;
    if (isInitialized) {
      expressionProxy = createExpressionProxy(hiveConf);
      if (HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL)) {
        directSql = new MetaStoreDirectSql(pm, hiveConf);
      }
    }
    LOG.debug("RawStore: " + this + ", with PersistenceManager: " + pm +
        " created in the thread with id: " + Thread.currentThread().getId());
  }

  /**
   * Creates the proxy used to evaluate expressions. This is here to prevent circular
   * dependency - ql -&gt; metastore client &lt;-&gt metastore server -&gt ql. If server and
   * client are split, this can be removed.
   * @param conf Configuration.
   * @return The partition expression proxy.
   */
  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends PartitionExpressionProxy> clazz =
          (Class<? extends PartitionExpressionProxy>)MetaStoreUtils.getClass(className);
      return MetaStoreUtils.newInstance(
          clazz, new Class<?>[0], new Object[0]);
    } catch (MetaException e) {
      LOG.error("Error loading PartitionExpressionProxy", e);
      throw new RuntimeException("Error loading PartitionExpressionProxy: " + e.getMessage());
    }
  }

  /**
   * Configure the SSL properties of the connection from provided config
   * @param conf
   */
  private static void configureSSL(Configuration conf) {
    // SSL support
    String sslPropString = conf.get(HiveConf.ConfVars.METASTORE_DBACCESS_SSL_PROPS.varname);
    if (org.apache.commons.lang.StringUtils.isNotEmpty(sslPropString)) {
      LOG.info("Metastore setting SSL properties of the connection to backed DB");
      for (String sslProp : sslPropString.split(",")) {
        String[] pair = sslProp.trim().split("=");
        if (pair != null && pair.length == 2) {
          System.setProperty(pair[0].trim(), pair[1].trim());
        } else {
          LOG.warn("Invalid metastore property value for " + HiveConf.ConfVars.METASTORE_DBACCESS_SSL_PROPS);
        }
      }
    }
  }

  /**
   * Properties specified in hive-default.xml override the properties specified
   * in jpox.properties.
   */
  @SuppressWarnings("nls")
  private static Properties getDataSourceProps(Configuration conf) {
    Properties prop = new Properties();
    correctAutoStartMechanism(conf);

    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      if (e.getKey().contains("datanucleus") || e.getKey().contains("jdo")) {
        Object prevVal = prop.setProperty(e.getKey(), conf.get(e.getKey()));
        if (LOG.isDebugEnabled()
            && !e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }
    // Password may no longer be in the conf, use getPassword()
    try {
      String passwd =
          ShimLoader.getHadoopShims().getPassword(conf, HiveConf.ConfVars.METASTOREPWD.varname);
      if (passwd != null && !passwd.isEmpty()) {
        prop.setProperty(HiveConf.ConfVars.METASTOREPWD.varname, passwd);
      }
    } catch (IOException err) {
      throw new RuntimeException("Error getting metastore password: " + err.getMessage(), err);
    }

    if (LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e : prop.entrySet()) {
        if (!e.getKey().equals(HiveConf.ConfVars.METASTOREPWD.varname)) {
          LOG.debug(e.getKey() + " = " + e.getValue());
        }
      }
    }

    return prop;
  }

  /**
   * Update conf to set datanucleus.autoStartMechanismMode=ignored.
   * This is necessary to able to use older version of hive against
   * an upgraded but compatible metastore schema in db from new version
   * of hive
   * @param conf
   */
  private static void correctAutoStartMechanism(Configuration conf) {
    final String autoStartKey = "datanucleus.autoStartMechanismMode";
    final String autoStartIgnore = "ignored";
    String currentAutoStartVal = conf.get(autoStartKey);
    if(currentAutoStartVal != null && !currentAutoStartVal.equalsIgnoreCase(autoStartIgnore)) {
      LOG.warn(autoStartKey + " is set to unsupported value " + conf.get(autoStartKey) +
          " . Setting it to value " + autoStartIgnore);
    }
    conf.set(autoStartKey, autoStartIgnore);
  }

  private static synchronized PersistenceManagerFactory getPMF() {
    if (pmf == null) {
      pmf = JDOHelper.getPersistenceManagerFactory(prop);
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc != null) {
        HiveConf conf = new HiveConf(ObjectStore.class);
        String objTypes = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CACHE_PINOBJTYPES);
        LOG.info("Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\"" + objTypes + "\"");
        if (objTypes != null && objTypes.length() > 0) {
          objTypes = objTypes.toLowerCase();
          String[] typeTokens = objTypes.split(",");
          for (String type : typeTokens) {
            type = type.trim();
            if (PINCLASSMAP.containsKey(type)) {
              dsc.pinAll(true, PINCLASSMAP.get(type));
            }
            else {
              LOG.warn(type + " is not one of the pinnable object types: " + org.apache.commons.lang.StringUtils.join(PINCLASSMAP.keySet(), " "));
            }
          }
        }
      } else {
        LOG.warn("PersistenceManagerFactory returned null DataStoreCache object. Unable to initialize object pin types defined by hive.metastore.cache.pinobjtypes");
      }
    }
    return pmf;
  }

  @InterfaceAudience.LimitedPrivate({"HCATALOG"})
  @InterfaceStability.Evolving
  public PersistenceManager getPersistenceManager() {
    return getPMF().getPersistenceManager();
  }

  @Override
  public void shutdown() {
    if (pm != null) {
      LOG.debug("RawStore: " + this + ", with PersistenceManager: " + pm +
          " will be shutdown");
      pm.close();
    }
  }

  /**
   * Opens a new one or the one already created Every call of this function must
   * have corresponding commit or rollback function call
   *
   * @return an active transaction
   */

  @Override
  public boolean openTransaction() {
    openTrasactionCalls++;
    if (openTrasactionCalls == 1) {
      currentTransaction = pm.currentTransaction();
      currentTransaction.begin();
      transactionStatus = TXN_STATUS.OPEN;
    } else {
      // openTransactionCalls > 1 means this is an interior transaction
      // We should already have a transaction created that is active.
      if ((currentTransaction == null) || (!currentTransaction.isActive())){
        throw new RuntimeException("openTransaction called in an interior"
            + " transaction scope, but currentTransaction is not active.");
      }
    }

    boolean result = currentTransaction.isActive();
    debugLog("Open transaction: count = " + openTrasactionCalls + ", isActive = " + result);
    return result;
  }

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return Always returns true
   */
  @Override
  @SuppressWarnings("nls")
  public boolean commitTransaction() {
    if (TXN_STATUS.ROLLBACK == transactionStatus) {
      debugLog("Commit transaction: rollback");
      return false;
    }
    if (openTrasactionCalls <= 0) {
      RuntimeException e = new RuntimeException("commitTransaction was called but openTransactionCalls = "
          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
          "calls to openTransaction/commitTransaction");
      LOG.error("Unbalanced calls to open/commit Transaction", e);
      throw e;
    }
    if (!currentTransaction.isActive()) {
      RuntimeException e = new RuntimeException("commitTransaction was called but openTransactionCalls = "
          + openTrasactionCalls + ". This probably indicates that there are unbalanced " +
          "calls to openTransaction/commitTransaction");
      LOG.error("Unbalanced calls to open/commit Transaction", e);
      throw e;
    }
    openTrasactionCalls--;
    debugLog("Commit transaction: count = " + openTrasactionCalls + ", isactive "+ currentTransaction.isActive());

    if ((openTrasactionCalls == 0) && currentTransaction.isActive()) {
      transactionStatus = TXN_STATUS.COMMITED;
      currentTransaction.commit();
    }

    return true;
  }

  /**
   * @return true if there is an active transaction. If the current transaction
   *         is either committed or rolled back it returns false
   */
  public boolean isActiveTransaction() {
    if (currentTransaction == null) {
      return false;
    }
    return currentTransaction.isActive();
  }

  /**
   * Rolls back the current transaction if it is active
   */
  @Override
  public void rollbackTransaction() {
    if (openTrasactionCalls < 1) {
      debugLog("rolling back transaction: no open transactions: " + openTrasactionCalls);
      return;
    }
    debugLog("Rollback transaction, isActive: " + currentTransaction.isActive());
    try {
      if (currentTransaction.isActive()
          && transactionStatus != TXN_STATUS.ROLLBACK) {
        currentTransaction.rollback();
      }
    } finally {
      openTrasactionCalls = 0;
      transactionStatus = TXN_STATUS.ROLLBACK;
      // remove all detached objects from the cache, since the transaction is
      // being rolled back they are no longer relevant, and this prevents them
      // from reattaching in future transactions
      pm.evictAll();
    }
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commited = false;
    MDatabase mdb = new MDatabase();
    mdb.setName(db.getName().toLowerCase());
    mdb.setLocationUri(db.getLocationUri());
    mdb.setDescription(db.getDescription());
    mdb.setParameters(db.getParameters());
    mdb.setOwnerName(db.getOwnerName());
    PrincipalType ownerType = db.getOwnerType();
    mdb.setOwnerType((null == ownerType ? PrincipalType.USER.name() : ownerType.name()));
    try {
      openTransaction();
      pm.makePersistent(mdb);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  @SuppressWarnings("nls")
  private MDatabase getMDatabase(String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      name = HiveStringUtils.normalizeIdentifier(name);
      query = pm.newQuery(MDatabase.class, "name == dbname");
      query.declareParameters("java.lang.String dbname");
      query.setUnique(true);
      mdb = (MDatabase) query.execute(name);
      pm.retrieve(mdb);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    if (mdb == null) {
      throw new NoSuchObjectException("There is no database named " + name);
    }
    return mdb;
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    MetaException ex = null;
    Database db = null;
    try {
      db = getDatabaseInternal(name);
    } catch (MetaException e) {
      // Signature restriction to NSOE, and NSOE being a flat exception prevents us from
      // setting the cause of the NSOE as the MetaException. We should not lose the info
      // we got here, but it's very likely that the MetaException is irrelevant and is
      // actually an NSOE message, so we should log it and throw an NSOE with the msg.
      ex = e;
    }
    if (db == null) {
      LOG.warn("Failed to get database " + name +", returning NoSuchObjectException", ex);
      throw new NoSuchObjectException(name + (ex == null ? "" : (": " + ex.getMessage())));
    }
    return db;
  }

  public Database getDatabaseInternal(String name) throws MetaException, NoSuchObjectException {
    return new GetDbHelper(name, null, true, true) {
      @Override
      protected Database getSqlResult(GetHelper<Database> ctx) throws MetaException {
        return directSql.getDatabase(dbName);
      }

      @Override
      protected Database getJdoResult(GetHelper<Database> ctx) throws MetaException, NoSuchObjectException {
        return getJDODatabase(dbName);
      }
    }.run(false);
   }

  public Database getJDODatabase(String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    try {
      openTransaction();
      mdb = getMDatabase(name);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    Database db = new Database();
    db.setName(mdb.getName());
    db.setDescription(mdb.getDescription());
    db.setLocationUri(mdb.getLocationUri());
    db.setParameters(convertMap(mdb.getParameters()));
    db.setOwnerName(mdb.getOwnerName());
    String type = mdb.getOwnerType();
    db.setOwnerType((null == type || type.trim().isEmpty()) ? null : PrincipalType.valueOf(type));
    return db;
  }

  /**
   * Alter the database object in metastore. Currently only the parameters
   * of the database or the owner can be changed.
   * @param dbName the database name
   * @param db the Hive Database object
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  @Override
  public boolean alterDatabase(String dbName, Database db)
    throws MetaException, NoSuchObjectException {

    MDatabase mdb = null;
    boolean committed = false;
    try {
      mdb = getMDatabase(dbName);
      mdb.setParameters(db.getParameters());
      mdb.setOwnerName(db.getOwnerName());
      if (db.getOwnerType() != null) {
        mdb.setOwnerType(db.getOwnerType().name());
      }
      openTransaction();
      pm.makePersistent(mdb);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    boolean success = false;
    LOG.info("Dropping database " + dbname + " along with all tables");
    dbname = HiveStringUtils.normalizeIdentifier(dbname);
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      openTransaction();

      // then drop the database
      MDatabase db = getMDatabase(dbname);
      pm.retrieve(db);
      if (db != null) {
        List<MDBPrivilege> dbGrants = this.listDatabaseGrants(dbname, queryWrapper);
        if (dbGrants != null && dbGrants.size() > 0) {
          pm.deletePersistentAll(dbGrants);
        }
        pm.deletePersistent(db);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      queryWrapper.close();
    }
    return success;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    if (pattern == null || pattern.equals("*")) {
      return getAllDatabases();
    }
    boolean commited = false;
    List<String> databases = null;
    Query query = null;
    try {
      openTransaction();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      String[] subpatterns = pattern.trim().split("\\|");
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>(subpatterns.length);
      appendPatternCondition(filterBuilder, "name", subpatterns, parameterVals);
      query = pm.newQuery(MDatabase.class, filterBuilder.toString());
      query.setResult("name");
      query.setOrdering("name ascending");
      Collection names = (Collection) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
      databases = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        databases.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return databases;
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    boolean commited = false;
    List<String> databases = null;

    String queryStr = "select name from org.apache.hadoop.hive.metastore.model.MDatabase";
    Query query = null;

    openTransaction();
    try {
      query = pm.newQuery(queryStr);
      query.setResult("name");
      databases = new ArrayList<String>((Collection<String>) query.execute());
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    Collections.sort(databases);
    return databases;
  }

  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<MFieldSchema>();
    if (type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    if (mtype.getFields() != null) {
      for (MFieldSchema field : mtype.getFields()) {
        fields.add(new FieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    Type ret = new Type();
    ret.setName(mtype.getName());
    ret.setType1(mtype.getType1());
    ret.setType2(mtype.getType2());
    ret.setFields(fields);
    return ret;
  }

  @Override
  public boolean createType(Type type) {
    boolean success = false;
    MType mtype = getMType(type);
    boolean commited = false;
    try {
      openTransaction();
      pm.makePersistent(mtype);
      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public Type getType(String typeName) {
    Type type = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType mtype = (MType) query.execute(typeName.trim());
      pm.retrieve(type);
      if (mtype != null) {
        type = getType(mtype);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return type;
  }

  @Override
  public boolean dropType(String typeName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MType.class, "name == typeName");
      query.declareParameters("java.lang.String typeName");
      query.setUnique(true);
      MType type = (MType) query.execute(typeName.trim());
      pm.retrieve(type);
      if (type != null) {
        pm.deletePersistent(type);
      }
      success = commitTransaction();
    } catch (JDOObjectNotFoundException e) {
      success = commitTransaction();
      LOG.debug("type not found " + typeName, e);
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return success;
  }

  @Override
  public void createTableWithConstraints(Table tbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
    throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      createTable(tbl);
      // Add primary keys and foreign keys.
      // We need not do a deep retrieval of the Table Column Descriptor while persisting the PK/FK
      // since this transaction involving create table is not yet committed.
      addPrimaryKeys(primaryKeys, false);
      addForeignKeys(foreignKeys, false);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MTable mtbl = convertToMTable(tbl);
      pm.makePersistent(mtbl);

      PrincipalPrivilegeSet principalPrivs = tbl.getPrivileges();
      List<Object> toPersistPrivObjs = new ArrayList<Object>();
      if (principalPrivs != null) {
        int now = (int)(System.currentTimeMillis()/1000);

        Map<String, List<PrivilegeGrantInfo>> userPrivs = principalPrivs.getUserPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, userPrivs, PrincipalType.USER);

        Map<String, List<PrivilegeGrantInfo>> groupPrivs = principalPrivs.getGroupPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, groupPrivs, PrincipalType.GROUP);

        Map<String, List<PrivilegeGrantInfo>> rolePrivs = principalPrivs.getRolePrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, rolePrivs, PrincipalType.ROLE);
      }
      pm.makePersistentAll(toPersistPrivObjs);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Convert PrivilegeGrantInfo from privMap to MTablePrivilege, and add all of
   * them to the toPersistPrivObjs. These privilege objects will be persisted as
   * part of createTable.
   *
   * @param mtbl
   * @param toPersistPrivObjs
   * @param now
   * @param privMap
   * @param type
   */
  private void putPersistentPrivObjects(MTable mtbl, List<Object> toPersistPrivObjs,
      int now, Map<String, List<PrivilegeGrantInfo>> privMap, PrincipalType type) {
    if (privMap != null) {
      for (Map.Entry<String, List<PrivilegeGrantInfo>> entry : privMap
          .entrySet()) {
        String principalName = entry.getKey();
        List<PrivilegeGrantInfo> privs = entry.getValue();
        for (int i = 0; i < privs.size(); i++) {
          PrivilegeGrantInfo priv = privs.get(i);
          if (priv == null) {
            continue;
          }
          MTablePrivilege mTblSec = new MTablePrivilege(
              principalName, type.toString(), mtbl, priv.getPrivilege(),
              now, priv.getGrantor(), priv.getGrantorType().toString(), priv
                  .isGrantOption());
          toPersistPrivObjs.add(mTblSec);
        }
      }
    }
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
    NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(dbName, tableName);
      pm.retrieve(tbl);
      if (tbl != null) {
        // first remove all the grants
        List<MTablePrivilege> tabGrants = listAllTableGrants(dbName, tableName);
        if (tabGrants != null && tabGrants.size() > 0) {
          pm.deletePersistentAll(tabGrants);
        }
        List<MTableColumnPrivilege> tblColGrants = listTableAllColumnGrants(dbName,
            tableName);
        if (tblColGrants != null && tblColGrants.size() > 0) {
          pm.deletePersistentAll(tblColGrants);
        }

        List<MPartitionPrivilege> partGrants = this.listTableAllPartitionGrants(dbName, tableName);
        if (partGrants != null && partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColGrants = listTableAllPartitionColumnGrants(dbName,
            tableName);
        if (partColGrants != null && partColGrants.size() > 0) {
          pm.deletePersistentAll(partColGrants);
        }
        // delete column statistics if present
        try {
          deleteTableColumnStatistics(dbName, tableName, null);
        } catch (NoSuchObjectException e) {
          LOG.info("Found no table level column statistics associated with db " + dbName +
          " table " + tableName + " record to delete");
        }

        List<MConstraint> tabConstraints = listAllTableConstraintsWithOptionalConstraintName(
                                           dbName, tableName, null);
        if (tabConstraints != null && tabConstraints.size() > 0) {
          pm.deletePersistentAll(tabConstraints);
        }

        preDropStorageDescriptor(tbl.getSd());
        // then remove the table
        pm.deletePersistentAll(tbl);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private List<MConstraint> listAllTableConstraintsWithOptionalConstraintName
    (String dbName, String tableName, String constraintname) {
    List<MConstraint> mConstraints = null;
    List<String> constraintNames = new ArrayList<String>();
    Query query = null;

    try {
      query = pm.newQuery("select constraintName from org.apache.hadoop.hive.metastore.model.MConstraint  where "
        + "((parentTable.tableName == ptblname && parentTable.database.name == pdbname) || "
        + "(childTable != null && childTable.tableName == ctblname && "
        + "childTable.database.name == cdbname)) " + (constraintname != null ?
        " && constraintName == constraintname" : ""));
      query.declareParameters("java.lang.String ptblname, java.lang.String pdbname,"
      + "java.lang.String ctblname, java.lang.String cdbname" +
        (constraintname != null ? ", java.lang.String constraintname" : ""));
      Collection<?> constraintNamesColl =
        constraintname != null ?
          ((Collection<?>) query.
            executeWithArray(tableName, dbName, tableName, dbName, constraintname)):
          ((Collection<?>) query.
            executeWithArray(tableName, dbName, tableName, dbName));
      for (Iterator<?> i = constraintNamesColl.iterator(); i.hasNext();) {
        String currName = (String) i.next();
        constraintNames.add(currName);
      }
      query = pm.newQuery(MConstraint.class);
      query.setFilter("param.contains(constraintName)");
      query.declareParameters("java.util.Collection param");
      Collection<?> constraints = (Collection<?>)query.execute(constraintNames);
      mConstraints = new ArrayList<MConstraint>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        mConstraints.add(currConstraint);
      }
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
    return mConstraints;
  }

@Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    boolean commited = false;
    Table tbl = null;
    try {
      openTransaction();
      tbl = convertToTable(getMTable(dbName, tableName));
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return tbl;
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    return getTables(dbName, pattern, null);
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType) throws MetaException {
    boolean commited = false;
    Query query = null;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      // Take the pattern and split it on the | to get all the composing
      // patterns
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder();
      //adds database.name == dbName to the filter
      appendSimpleCondition(filterBuilder, "database.name", new String[] {dbName}, parameterVals);
      if(pattern != null) {
        appendPatternCondition(filterBuilder, "tableName", pattern, parameterVals);
      }
      if(tableType != null) {
        appendPatternCondition(filterBuilder, "tableType", new String[] {tableType.toString()}, parameterVals);
      }

      query = pm.newQuery(MTable.class, filterBuilder.toString());
      query.setResult("tableName");
      query.setOrdering("tableName ascending");
      Collection names = (Collection) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
      tbls = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        tbls.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return tbls;
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return getObjectCount("name", MDatabase.class.getName());
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return getObjectCount("partitionName", MPartition.class.getName());
  }

  @Override
  public int getTableCount() throws MetaException {
    return getObjectCount("tableName", MTable.class.getName());
  }

  private int getObjectCount(String fieldName, String objName) {
    Long result = 0L;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      String queryStr =
        "select count(" + fieldName + ") from " + objName;
      query = pm.newQuery(queryStr);
      result = (Long) query.execute();
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return result.intValue();
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {

    boolean commited = false;
    Query query = null;
    List<TableMeta> metas = new ArrayList<TableMeta>();
    try {
      openTransaction();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>();
      if (dbNames != null && !dbNames.equals("*")) {
        appendPatternCondition(filterBuilder, "database.name", dbNames, parameterVals);
      }
      if (tableNames != null && !tableNames.equals("*")) {
        appendPatternCondition(filterBuilder, "tableName", tableNames, parameterVals);
      }
      if (tableTypes != null && !tableTypes.isEmpty()) {
        appendSimpleCondition(filterBuilder, "tableType", tableTypes.toArray(new String[0]), parameterVals);
      }

      query = pm.newQuery(MTable.class, filterBuilder.toString());
      Collection<MTable> tables = (Collection<MTable>) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
      for (MTable table : tables) {
        TableMeta metaData = new TableMeta(
            table.getDatabase().getName(), table.getTableName(), table.getTableType());
        metaData.setComments(table.getParameters().get("comment"));
        metas.add(metaData);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return metas;
  }

  private StringBuilder appendPatternCondition(StringBuilder filterBuilder, String fieldName,
      String[] elements, List<String> parameterVals) {
    return appendCondition(filterBuilder, fieldName, elements, true, parameterVals);
  }

  private StringBuilder appendPatternCondition(StringBuilder builder,
      String fieldName, String elements, List<String> parameters) {
      elements = HiveStringUtils.normalizeIdentifier(elements);
    return appendCondition(builder, fieldName, elements.split("\\|"), true, parameters);
  }

  private StringBuilder appendSimpleCondition(StringBuilder builder,
      String fieldName, String[] elements, List<String> parameters) {
    return appendCondition(builder, fieldName, elements, false, parameters);
  }

  private StringBuilder appendCondition(StringBuilder builder,
      String fieldName, String[] elements, boolean pattern, List<String> parameters) {
    if (builder.length() > 0) {
      builder.append(" && ");
    }
    builder.append(" (");
    int length = builder.length();
    for (String element : elements) {
      if (pattern) {
        element = "(?i)" + element.replaceAll("\\*", ".*");
      }
      parameters.add(element);
      if (builder.length() > length) {
        builder.append(" || ");
      }
      builder.append(fieldName);
      if (pattern) {
        builder.append(".matches(").append(JDO_PARAM).append(parameters.size()).append(")");
      } else {
        builder.append(" == ").append(JDO_PARAM).append(parameters.size());
      }
    }
    builder.append(" )");
    return builder;
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return getTables(dbName, ".*");
  }

  class AttachedMTableInfo {
    MTable mtbl;
    MColumnDescriptor mcd;

    public AttachedMTableInfo() {}

    public AttachedMTableInfo(MTable mtbl, MColumnDescriptor mcd) {
      this.mtbl = mtbl;
      this.mcd = mcd;
    }
  }

  private AttachedMTableInfo getMTable(String db, String table, boolean retrieveCD) {
    AttachedMTableInfo nmtbl = new AttachedMTableInfo();
    MTable mtbl = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      db = HiveStringUtils.normalizeIdentifier(db);
      table = HiveStringUtils.normalizeIdentifier(table);
      query = pm.newQuery(MTable.class, "tableName == table && database.name == db");
      query.declareParameters("java.lang.String table, java.lang.String db");
      query.setUnique(true);
      mtbl = (MTable) query.execute(table, db);
      pm.retrieve(mtbl);
      // Retrieving CD can be expensive and unnecessary, so do it only when required.
      if (mtbl != null && retrieveCD) {
        pm.retrieve(mtbl.getSd());
        pm.retrieveAll(mtbl.getSd().getCD());
        nmtbl.mcd = mtbl.getSd().getCD();
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    nmtbl.mtbl = mtbl;
    return nmtbl;
  }

  private MTable getMTable(String db, String table) {
    AttachedMTableInfo nmtbl = getMTable(db, table, false);
    return nmtbl.mtbl;
  }

  @Override
  public List<Table> getTableObjectsByName(String db, List<String> tbl_names) throws MetaException,
      UnknownDBException {
    List<Table> tables = new ArrayList<Table>();
    boolean committed = false;
    Query dbExistsQuery = null;
    Query query = null;
    try {
      openTransaction();
      db = HiveStringUtils.normalizeIdentifier(db);
      dbExistsQuery = pm.newQuery(MDatabase.class, "name == db");
      dbExistsQuery.declareParameters("java.lang.String db");
      dbExistsQuery.setUnique(true);
      dbExistsQuery.setResult("name");
      String dbNameIfExists = (String) dbExistsQuery.execute(db);
      if (dbNameIfExists == null || dbNameIfExists.isEmpty()) {
        throw new UnknownDBException("Could not find database " + db);
      }

      List<String> lowered_tbl_names = new ArrayList<String>();
      for (String t : tbl_names) {
        lowered_tbl_names.add(HiveStringUtils.normalizeIdentifier(t));
      }
      query = pm.newQuery(MTable.class);
      query.setFilter("database.name == db && tbl_names.contains(tableName)");
      query.declareParameters("java.lang.String db, java.util.Collection tbl_names");
      Collection mtables = (Collection) query.execute(db, lowered_tbl_names);
      for (Iterator iter = mtables.iterator(); iter.hasNext();) {
        tables.add(convertToTable((MTable) iter.next()));
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (dbExistsQuery != null) {
        dbExistsQuery.closeAll();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return tables;
  }

  /** Makes shallow copy of a list to avoid DataNucleus mucking with our objects. */
  private <T> List<T> convertList(List<T> dnList) {
    return (dnList == null) ? null : Lists.newArrayList(dnList);
  }

  /** Makes shallow copy of a map to avoid DataNucleus mucking with our objects. */
  private Map<String, String> convertMap(Map<String, String> dnMap) {
    return MetaStoreUtils.trimMapNulls(dnMap,
        HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS));
  }

  private Table convertToTable(MTable mtbl) throws MetaException {
    if (mtbl == null) {
      return null;
    }
    String tableType = mtbl.getTableType();
    if (tableType == null) {
      // for backwards compatibility with old metastore persistence
      if (mtbl.getViewOriginalText() != null) {
        tableType = TableType.VIRTUAL_VIEW.toString();
      } else if ("TRUE".equals(mtbl.getParameters().get("EXTERNAL"))) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      } else {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    final Table table = new Table(mtbl.getTableName(), mtbl.getDatabase().getName(), mtbl
            .getOwner(), mtbl.getCreateTime(), mtbl.getLastAccessTime(), mtbl
            .getRetention(), convertToStorageDescriptor(mtbl.getSd()),
            convertToFieldSchemas(mtbl.getPartitionKeys()), convertMap(mtbl.getParameters()),
            mtbl.getViewOriginalText(), mtbl.getViewExpandedText(), tableType);
    table.setRewriteEnabled(mtbl.isRewriteEnabled());
    return table;
  }

  private MTable convertToMTable(Table tbl) throws InvalidObjectException,
      MetaException {
    if (tbl == null) {
      return null;
    }
    MDatabase mdb = null;
    try {
      mdb = getMDatabase(tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("Database " + tbl.getDbName()
          + " doesn't exist.");
    }

    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = "TRUE".equals(tbl.getParameters().get("EXTERNAL"));
    if (TableType.MANAGED_TABLE.toString().equals(tableType)) {
      if (isExternal) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      }
    }
    if (TableType.EXTERNAL_TABLE.toString().equals(tableType)) {
      if (!isExternal) {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }

    // A new table is always created with a new column descriptor
    return new MTable(HiveStringUtils.normalizeIdentifier(tbl.getTableName()), mdb,
        convertToMStorageDescriptor(tbl.getSd()), tbl.getOwner(), tbl
        .getCreateTime(), tbl.getLastAccessTime(), tbl.getRetention(),
        convertToMFieldSchemas(tbl.getPartitionKeys()), tbl.getParameters(),
        tbl.getViewOriginalText(), tbl.getViewExpandedText(), tbl.isRewriteEnabled(),
        tableType);
  }

  private List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<MFieldSchema>(keys.size());
      for (FieldSchema part : keys) {
        mkeys.add(new MFieldSchema(part.getName().toLowerCase(),
            part.getType(), part.getComment()));
      }
    }
    return mkeys;
  }

  private List<FieldSchema> convertToFieldSchemas(List<MFieldSchema> mkeys) {
    List<FieldSchema> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<FieldSchema>(mkeys.size());
      for (MFieldSchema part : mkeys) {
        keys.add(new FieldSchema(part.getName(), part.getType(), part
            .getComment()));
      }
    }
    return keys;
  }

  private List<MOrder> convertToMOrders(List<Order> keys) {
    List<MOrder> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<MOrder>(keys.size());
      for (Order part : keys) {
        mkeys.add(new MOrder(HiveStringUtils.normalizeIdentifier(part.getCol()), part.getOrder()));
      }
    }
    return mkeys;
  }

  private List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<Order>(mkeys.size());
      for (MOrder part : mkeys) {
        keys.add(new Order(part.getCol(), part.getOrder()));
      }
    }
    return keys;
  }

  private SerDeInfo convertToSerDeInfo(MSerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new SerDeInfo(ms.getName(), ms.getSerializationLib(), convertMap(ms.getParameters()));
  }

  private MSerDeInfo convertToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new MSerDeInfo(ms.getName(), ms.getSerializationLib(), ms
        .getParameters());
  }

  /**
   * Given a list of model field schemas, create a new model column descriptor.
   * @param cols the columns the column descriptor contains
   * @return a new column descriptor db-backed object
   */
  private MColumnDescriptor createNewMColumnDescriptor(List<MFieldSchema> cols) {
    if (cols == null) {
      return null;
    }
    return new MColumnDescriptor(cols);
  }

  // MSD and SD should be same objects. Not sure how to make then same right now
  // MSerdeInfo *& SerdeInfo should be same as well
  private StorageDescriptor convertToStorageDescriptor(
      MStorageDescriptor msd,
      boolean noFS) throws MetaException {
    if (msd == null) {
      return null;
    }
    List<MFieldSchema> mFieldSchemas = msd.getCD() == null ? null : msd.getCD().getCols();

    StorageDescriptor sd = new StorageDescriptor(noFS ? null : convertToFieldSchemas(mFieldSchemas),
        msd.getLocation(), msd.getInputFormat(), msd.getOutputFormat(), msd
        .isCompressed(), msd.getNumBuckets(), convertToSerDeInfo(msd
        .getSerDeInfo()), convertList(msd.getBucketCols()), convertToOrders(msd
        .getSortCols()), convertMap(msd.getParameters()));
    SkewedInfo skewedInfo = new SkewedInfo(convertList(msd.getSkewedColNames()),
        convertToSkewedValues(msd.getSkewedColValues()),
        covertToSkewedMap(msd.getSkewedColValueLocationMaps()));
    sd.setSkewedInfo(skewedInfo);
    sd.setStoredAsSubDirectories(msd.isStoredAsSubDirectories());
    return sd;
  }

  private StorageDescriptor convertToStorageDescriptor(MStorageDescriptor msd)
      throws MetaException {
    return convertToStorageDescriptor(msd, false);
  }

  /**
   * Convert a list of MStringList to a list of list string
   *
   * @param mLists
   * @return
   */
  private List<List<String>> convertToSkewedValues(List<MStringList> mLists) {
    List<List<String>> lists = null;
    if (mLists != null) {
      lists = new ArrayList<List<String>>(mLists.size());
      for (MStringList element : mLists) {
        lists.add(new ArrayList<String>(element.getInternalList()));
      }
    }
    return lists;
  }

  private List<MStringList> convertToMStringLists(List<List<String>> mLists) {
    List<MStringList> lists = null ;
    if (null != mLists) {
      lists = new ArrayList<MStringList>();
      for (List<String> mList : mLists) {
        lists.add(new MStringList(mList));
      }
    }
    return lists;
  }

  /**
   * Convert a MStringList Map to a Map
   * @param mMap
   * @return
   */
  private Map<List<String>, String> covertToSkewedMap(Map<MStringList, String> mMap) {
    Map<List<String>, String> map = null;
    if (mMap != null) {
      map = new HashMap<List<String>, String>(mMap.size());
      Set<MStringList> keys = mMap.keySet();
      for (MStringList key : keys) {
        map.put(new ArrayList<String>(key.getInternalList()), mMap.get(key));
      }
    }
    return map;
  }

  /**
   * Covert a Map to a MStringList Map
   * @param mMap
   * @return
   */
  private Map<MStringList, String> covertToMapMStringList(Map<List<String>, String> mMap) {
    Map<MStringList, String> map = null;
    if (mMap != null) {
      map = new HashMap<MStringList, String>(mMap.size());
      Set<List<String>> keys = mMap.keySet();
      for (List<String> key : keys) {
        map.put(new MStringList(key), mMap.get(key));
      }
    }
    return map;
  }

  /**
   * Converts a storage descriptor to a db-backed storage descriptor.  Creates a
   *   new db-backed column descriptor object for this SD.
   * @param sd the storage descriptor to wrap in a db-backed object
   * @return the storage descriptor db-backed object
   * @throws MetaException
   */
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd)
      throws MetaException {
    if (sd == null) {
      return null;
    }
    MColumnDescriptor mcd = createNewMColumnDescriptor(convertToMFieldSchemas(sd.getCols()));
    return convertToMStorageDescriptor(sd, mcd);
  }

  /**
   * Converts a storage descriptor to a db-backed storage descriptor.  It points the
   * storage descriptor's column descriptor to the one passed as an argument,
   * so it does not create a new mcolumn descriptor object.
   * @param sd the storage descriptor to wrap in a db-backed object
   * @param mcd the db-backed column descriptor
   * @return the db-backed storage descriptor object
   * @throws MetaException
   */
  private MStorageDescriptor convertToMStorageDescriptor(StorageDescriptor sd,
      MColumnDescriptor mcd) throws MetaException {
    if (sd == null) {
      return null;
    }
    return new MStorageDescriptor(mcd, sd
        .getLocation(), sd.getInputFormat(), sd.getOutputFormat(), sd
        .isCompressed(), sd.getNumBuckets(), convertToMSerDeInfo(sd
        .getSerdeInfo()), sd.getBucketCols(),
        convertToMOrders(sd.getSortCols()), sd.getParameters(),
        (null == sd.getSkewedInfo()) ? null
            : sd.getSkewedInfo().getSkewedColNames(),
        convertToMStringLists((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValues()),
        covertToMapMStringList((null == sd.getSkewedInfo()) ? null : sd.getSkewedInfo()
            .getSkewedColValueLocationMaps()), sd.isStoredAsSubDirectories());
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    openTransaction();
    try {
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      MTable table = this.getMTable(dbName, tblName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(dbName, tblName);
        tabColumnGrants = this.listTableAllColumnGrants(dbName, tblName);
      }
      List<Object> toPersist = new ArrayList<Object>();
      for (Partition part : parts) {
        if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
          throw new MetaException("Partition does not belong to target table "
              + dbName + "." + tblName + ": " + part);
        }
        MPartition mpart = convertToMPart(part, true);
        toPersist.add(mpart);
        int now = (int)(System.currentTimeMillis()/1000);
        if (tabGrants != null) {
          for (MTablePrivilege tab: tabGrants) {
            toPersist.add(new MPartitionPrivilege(tab.getPrincipalName(),
                tab.getPrincipalType(), mpart, tab.getPrivilege(), now,
                tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption()));
          }
        }

        if (tabColumnGrants != null) {
          for (MTableColumnPrivilege col : tabColumnGrants) {
            toPersist.add(new MPartitionColumnPrivilege(col.getPrincipalName(),
                col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(),
                now, col.getGrantor(), col.getGrantorType(), col.getGrantOption()));
          }
        }
      }
      if (toPersist.size() > 0) {
        pm.makePersistentAll(toPersist);
      }

      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private boolean isValidPartition(
      Partition part, boolean ifNotExists) throws MetaException {
    MetaStoreUtils.validatePartitionNameCharacters(part.getValues(),
        partitionValidationPattern);
    boolean doesExist = doesPartitionExist(
        part.getDbName(), part.getTableName(), part.getValues());
    if (doesExist && !ifNotExists) {
      throw new MetaException("Partition already exists: " + part);
    }
    return !doesExist;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName,
                               PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    openTransaction();
    try {
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      MTable table = this.getMTable(dbName, tblName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(dbName, tblName);
        tabColumnGrants = this.listTableAllColumnGrants(dbName, tblName);
      }

      if (!partitionSpec.getTableName().equals(tblName) || !partitionSpec.getDbName().equals(dbName)) {
        throw new MetaException("Partition does not belong to target table "
            + dbName + "." + tblName + ": " + partitionSpec);
      }

      PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();

      int now = (int)(System.currentTimeMillis()/1000);

      while (iterator.hasNext()) {
        Partition part = iterator.next();

        if (isValidPartition(part, ifNotExists)) {
          MPartition mpart = convertToMPart(part, true);
          pm.makePersistent(mpart);
          if (tabGrants != null) {
            for (MTablePrivilege tab : tabGrants) {
              pm.makePersistent(new MPartitionPrivilege(tab.getPrincipalName(),
                  tab.getPrincipalType(), mpart, tab.getPrivilege(), now,
                  tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption()));
            }
          }

          if (tabColumnGrants != null) {
            for (MTableColumnPrivilege col : tabColumnGrants) {
              pm.makePersistent(new MPartitionColumnPrivilege(col.getPrincipalName(),
                  col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(),
                  now, col.getGrantor(), col.getGrantorType(), col.getGrantOption()));
            }
          }
        }
      }

      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException,
      MetaException {
    boolean success = false;
    boolean commited = false;
    try {
      MTable table = this.getMTable(part.getDbName(), part.getTableName());
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(part
            .getDbName(), part.getTableName());
        tabColumnGrants = this.listTableAllColumnGrants(
            part.getDbName(), part.getTableName());
      }
      openTransaction();
      MPartition mpart = convertToMPart(part, true);
      pm.makePersistent(mpart);

      int now = (int)(System.currentTimeMillis()/1000);
      List<Object> toPersist = new ArrayList<Object>();
      if (tabGrants != null) {
        for (MTablePrivilege tab: tabGrants) {
          MPartitionPrivilege partGrant = new MPartitionPrivilege(tab
              .getPrincipalName(), tab.getPrincipalType(),
              mpart, tab.getPrivilege(), now, tab.getGrantor(), tab
                  .getGrantorType(), tab.getGrantOption());
          toPersist.add(partGrant);
        }
      }

      if (tabColumnGrants != null) {
        for (MTableColumnPrivilege col : tabColumnGrants) {
          MPartitionColumnPrivilege partColumn = new MPartitionColumnPrivilege(col
              .getPrincipalName(), col.getPrincipalType(), mpart, col
              .getColumnName(), col.getPrivilege(), now, col.getGrantor(), col
              .getGrantorType(), col.getGrantOption());
          toPersist.add(partColumn);
        }

        if (toPersist.size() > 0) {
          pm.makePersistentAll(toPersist);
        }
      }

      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws NoSuchObjectException, MetaException {
    openTransaction();
    Partition part = convertToPart(getMPartition(dbName, tableName, part_vals));
    commitTransaction();
    if(part == null) {
      throw new NoSuchObjectException("partition values="
          + part_vals.toString());
    }
    part.setValues(part_vals);
    return part;
  }

  private MPartition getMPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException {
    List<MPartition> mparts = null;
    MPartition ret = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      tableName = HiveStringUtils.normalizeIdentifier(tableName);
      MTable mtbl = getMTable(dbName, tableName);
      if (mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      // Change the query to use part_vals instead of the name which is
      // redundant TODO: callers of this often get part_vals out of name for no reason...
      String name =
          Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), part_vals);
      query =
          pm.newQuery(MPartition.class,
              "table.tableName == t1 && table.database.name == t2 && partitionName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      mparts = (List<MPartition>) query.execute(tableName, dbName, name);
      pm.retrieveAll(mparts);
      commited = commitTransaction();
      // We need to compare partition name with requested name since some DBs
      // (like MySQL, Derby) considers 'a' = 'a ' whereas others like (Postgres,
      // Oracle) doesn't exhibit this problem.
      if (mparts != null && mparts.size() > 0) {
        if (mparts.size() > 1) {
          throw new MetaException(
              "Expecting only one partition but more than one partitions are found.");
        } else {
          MPartition mpart = mparts.get(0);
          if (name.equals(mpart.getPartitionName())) {
            ret = mpart;
          } else {
            throw new MetaException("Expecting a partition with name " + name
                + ", but metastore is returning a partition with name " + mpart.getPartitionName()
                + ".");
          }
        }
      }
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return ret;
  }

  /**
   * Convert a Partition object into an MPartition, which is an object backed by the db
   * If the Partition's set of columns is the same as the parent table's AND useTableCD
   * is true, then this partition's storage descriptor's column descriptor will point
   * to the same one as the table's storage descriptor.
   * @param part the partition to convert
   * @param useTableCD whether to try to use the parent table's column descriptor.
   * @return the model partition object
   * @throws InvalidObjectException
   * @throws MetaException
   */
  private MPartition convertToMPart(Partition part, boolean useTableCD)
      throws InvalidObjectException, MetaException {
    if (part == null) {
      return null;
    }
    MTable mt = getMTable(part.getDbName(), part.getTableName());
    if (mt == null) {
      throw new InvalidObjectException(
          "Partition doesn't have a valid table or database name");
    }

    // If this partition's set of columns is the same as the parent table's,
    // use the parent table's, so we do not create a duplicate column descriptor,
    // thereby saving space
    MStorageDescriptor msd;
    if (useTableCD &&
        mt.getSd() != null && mt.getSd().getCD() != null &&
        mt.getSd().getCD().getCols() != null &&
        part.getSd() != null &&
        convertToFieldSchemas(mt.getSd().getCD().getCols()).
        equals(part.getSd().getCols())) {
      msd = convertToMStorageDescriptor(part.getSd(), mt.getSd().getCD());
    } else {
      msd = convertToMStorageDescriptor(part.getSd());
    }

    return new MPartition(Warehouse.makePartName(convertToFieldSchemas(mt
        .getPartitionKeys()), part.getValues()), mt, part.getValues(), part
        .getCreateTime(), part.getLastAccessTime(),
        msd, part.getParameters());
  }

  private Partition convertToPart(MPartition mpart) throws MetaException {
    if (mpart == null) {
      return null;
    }
    return new Partition(convertList(mpart.getValues()), mpart.getTable().getDatabase()
        .getName(), mpart.getTable().getTableName(), mpart.getCreateTime(),
        mpart.getLastAccessTime(), convertToStorageDescriptor(mpart.getSd()),
        convertMap(mpart.getParameters()));
  }

  private Partition convertToPart(String dbName, String tblName, MPartition mpart)
      throws MetaException {
    if (mpart == null) {
      return null;
    }
    return new Partition(convertList(mpart.getValues()), dbName, tblName,
        mpart.getCreateTime(), mpart.getLastAccessTime(),
        convertToStorageDescriptor(mpart.getSd(), false), convertMap(mpart.getParameters()));
  }

  @Override
  public boolean dropPartition(String dbName, String tableName,
    List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
    InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = getMPartition(dbName, tableName, part_vals);
      dropPartitionCommon(part);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    if (partNames.isEmpty()) return;
    boolean success = false;
    openTransaction();
    try {
      // Delete all things.
      dropPartitionGrantsNoTxn(dbName, tblName, partNames);
      dropPartitionAllColumnGrantsNoTxn(dbName, tblName, partNames);
      dropPartitionColumnStatisticsNoTxn(dbName, tblName, partNames);

      // CDs are reused; go thry partition SDs, detach all CDs from SDs, then remove unused CDs.
      for (MColumnDescriptor mcd : detachCdsFromSdsNoTxn(dbName, tblName, partNames)) {
        removeUnusedColumnDescriptor(mcd);
      }
      dropPartitionsNoTxn(dbName, tblName, partNames);
      if (!(success = commitTransaction())) {
        throw new MetaException("Failed to drop partitions"); // Should not happen?
      }
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Drop an MPartition and cascade deletes (e.g., delete partition privilege grants,
   *   drop the storage descriptor cleanly, etc.)
   * @param part - the MPartition to drop
   * @return whether the transaction committed successfully
   * @throws InvalidInputException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  private boolean dropPartitionCommon(MPartition part) throws NoSuchObjectException, MetaException,
    InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      if (part != null) {
        List<MFieldSchema> schemas = part.getTable().getPartitionKeys();
        List<String> colNames = new ArrayList<String>();
        for (MFieldSchema col: schemas) {
          colNames.add(col.getName());
        }
        String partName = FileUtils.makePartName(colNames, part.getValues());

        List<MPartitionPrivilege> partGrants = listPartitionGrants(
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            Lists.newArrayList(partName));

        if (partGrants != null && partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColumnGrants = listPartitionAllColumnGrants(
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            Lists.newArrayList(partName));
        if (partColumnGrants != null && partColumnGrants.size() > 0) {
          pm.deletePersistentAll(partColumnGrants);
        }

        String dbName = part.getTable().getDatabase().getName();
        String tableName = part.getTable().getTableName();

        // delete partition level column stats if it exists
       try {
          deletePartitionColumnStatistics(dbName, tableName, partName, part.getValues(), null);
        } catch (NoSuchObjectException e) {
          LOG.info("No column statistics records found to delete");
        }

        preDropStorageDescriptor(part.getSd());
        pm.deletePersistent(part);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public List<Partition> getPartitions(
      String dbName, String tableName, int maxParts) throws MetaException, NoSuchObjectException {
    return getPartitionsInternal(dbName, tableName, maxParts, true, true);
  }

  protected List<Partition> getPartitionsInternal(
      String dbName, String tblName, final int maxParts, boolean allowSql, boolean allowJdo)
          throws MetaException, NoSuchObjectException {
    return new GetListHelper<Partition>(dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        Integer max = (maxParts < 0) ? null : maxParts;
        return directSql.getPartitions(dbName, tblName, max);
      }
      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException {
        QueryWrapper queryWrapper = new QueryWrapper();
        try {
          return convertToParts(listMPartitions(dbName, tblName, maxParts, queryWrapper));
        } finally {
          queryWrapper.close();
        }
      }
    }.run(false);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short max, String userName, List<String> groupNames)
          throws MetaException, InvalidObjectException {
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      List<MPartition> mparts = listMPartitions(dbName, tblName, max, queryWrapper);
      List<Partition> parts = new ArrayList<Partition>(mparts.size());
      if (mparts != null && mparts.size()>0) {
        for (MPartition mpart : mparts) {
          MTable mtbl = mpart.getTable();
          Partition part = convertToPart(mpart);
          parts.add(part);

          if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
                .getPartitionKeys()), part.getValues());
            PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(dbName,
                tblName, partName, userName, groupNames);
            part.setPrivileges(partAuth);
          }
        }
      }
      success =  commitTransaction();
      return parts;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      queryWrapper.close();
    }
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    boolean success = false;
    try {
      openTransaction();
      MPartition mpart = getMPartition(dbName, tblName, partVals);
      if (mpart == null) {
        commitTransaction();
        throw new NoSuchObjectException("partition values="
            + partVals.toString());
      }
      Partition part = null;
      MTable mtbl = mpart.getTable();
      part = convertToPart(mpart);
      if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
            .getPartitionKeys()), partVals);
        PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(dbName,
            tblName, partName, user_name, group_names);
        part.setPrivileges(partAuth);
      }

      success = commitTransaction();
      return part;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private List<Partition> convertToParts(List<MPartition> mparts) throws MetaException {
    return convertToParts(mparts, null);
  }

  private List<Partition> convertToParts(List<MPartition> src, List<Partition> dest)
      throws MetaException {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (MPartition mp : src) {
      dest.add(convertToPart(mp));
      Deadline.checkTimeout();
    }
    return dest;
  }

  private List<Partition> convertToParts(String dbName, String tblName, List<MPartition> mparts)
      throws MetaException {
    List<Partition> parts = new ArrayList<Partition>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(convertToPart(dbName, tblName, mp));
      Deadline.checkTimeout();
    }
    return parts;
  }

  // TODO:pc implement max
  @Override
  public List<String> listPartitionNames(String dbName, String tableName,
      short max) throws MetaException {
    List<String> pns = null;
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      pns = getPartitionNamesNoTxn(dbName, tableName, max);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return pns;
  }

  private List<String> getPartitionNamesNoTxn(String dbName, String tableName, short max) {
    List<String> pns = new ArrayList<String>();
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    Query query =
        pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
            + "where table.database.name == t1 && table.tableName == t2 "
            + "order by partitionName asc");
    query.declareParameters("java.lang.String t1, java.lang.String t2");
    query.setResult("partitionName");
    if (max > 0) {
      query.setRange(0, max);
    }
    Collection names = (Collection) query.execute(dbName, tableName);
    for (Iterator i = names.iterator(); i.hasNext();) {
      pns.add((String) i.next());
    }
    if (query != null) {
      query.closeAll();
    }
    return pns;
  }

  /**
   * Retrieves a Collection of partition-related results from the database that match
   *  the partial specification given for a specific table.
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @param part_vals the partial specification values
   * @param max_parts the maximum number of partitions to return
   * @param resultsCol the metadata column of the data to return, e.g. partitionName, etc.
   *        if resultsCol is empty or null, a collection of MPartition objects is returned
   * @throws NoSuchObjectException
   * @results A Collection of partition-related items from the db that match the partial spec
   *          for a table.  The type of each item in the collection corresponds to the column
   *          you want results for.  E.g., if resultsCol is partitionName, the Collection
   *          has types of String, and if resultsCol is null, the types are MPartition.
   */
  private Collection getPartitionPsQueryResults(String dbName, String tableName,
      List<String> part_vals, short max_parts, String resultsCol, QueryWrapper queryWrapper)
      throws MetaException, NoSuchObjectException {
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    Table table = getTable(dbName, tableName);
    if (table == null) {
      throw new NoSuchObjectException(dbName + "." + tableName + " table not found");
    }
    List<FieldSchema> partCols = table.getPartitionKeys();
    int numPartKeys = partCols.size();
    if (part_vals.size() > numPartKeys) {
      throw new MetaException("Incorrect number of partition values."
          + " numPartKeys=" + numPartKeys + ", part_val=" + part_vals.size());
    }
    partCols = partCols.subList(0, part_vals.size());
    // Construct a pattern of the form: partKey=partVal/partKey2=partVal2/...
    // where partVal is either the escaped partition value given as input,
    // or a regex of the form ".*"
    // This works because the "=" and "/" separating key names and partition key/values
    // are not escaped.
    String partNameMatcher = Warehouse.makePartName(partCols, part_vals, ".*");
    // add ".*" to the regex to match anything else afterwards the partial spec.
    if (part_vals.size() < numPartKeys) {
      partNameMatcher += ".*";
    }
    Query query = queryWrapper.query = pm.newQuery(MPartition.class);
    StringBuilder queryFilter = new StringBuilder("table.database.name == dbName");
    queryFilter.append(" && table.tableName == tableName");
    queryFilter.append(" && partitionName.matches(partialRegex)");
    query.setFilter(queryFilter.toString());
    query.declareParameters("java.lang.String dbName, "
        + "java.lang.String tableName, java.lang.String partialRegex");
    if (max_parts >= 0) {
      // User specified a row limit, set it on the Query
      query.setRange(0, max_parts);
    }
    if (resultsCol != null && !resultsCol.isEmpty()) {
      query.setResult(resultsCol);
    }

    return (Collection) query.execute(dbName, tableName, partNameMatcher);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    List<Partition> partitions = new ArrayList<Partition>();
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      LOG.debug("executing listPartitionNamesPsWithAuth");
      Collection parts = getPartitionPsQueryResults(db_name, tbl_name,
          part_vals, max_parts, null, queryWrapper);
      MTable mtbl = getMTable(db_name, tbl_name);
      for (Object o : parts) {
        Partition part = convertToPart((MPartition) o);
        //set auth privileges
        if (null != userName && null != groupNames &&
            "TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
          String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
              .getPartitionKeys()), part.getValues());
          PrincipalPrivilegeSet partAuth = getPartitionPrivilegeSet(db_name,
              tbl_name, partName, userName, groupNames);
          part.setPrivileges(partAuth);
        }
        partitions.add(part);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      queryWrapper.close();
    }
    return partitions;
  }

  @Override
  public List<String> listPartitionNamesPs(String dbName, String tableName,
      List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
    List<String> partitionNames = new ArrayList<String>();
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      LOG.debug("Executing listPartitionNamesPs");
      Collection names = getPartitionPsQueryResults(dbName, tableName,
          part_vals, max_parts, "partitionName", queryWrapper);
      for (Object o : names) {
        partitionNames.add((String) o);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      queryWrapper.close();
    }
    return partitionNames;
  }

  // TODO:pc implement max
  private List<MPartition> listMPartitions(String dbName, String tableName, int max, QueryWrapper queryWrapper) {
    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitions");
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      tableName = HiveStringUtils.normalizeIdentifier(tableName);
      Query query = queryWrapper.query = pm.newQuery(MPartition.class, "table.tableName == t1 && table.database.name == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      query.setOrdering("partitionName ascending");
      if (max > 0) {
        query.setRange(0, max);
      }
      mparts = (List<MPartition>) query.execute(tableName, dbName);
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions " + mparts);
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return getPartitionsByNamesInternal(dbName, tblName, partNames, true, true);
  }

  protected List<Partition> getPartitionsByNamesInternal(String dbName, String tblName,
      final List<String> partNames, boolean allowSql, boolean allowJdo)
          throws MetaException, NoSuchObjectException {
    return new GetListHelper<Partition>(dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        return directSql.getPartitionsViaSqlFilter(dbName, tblName, partNames);
      }
      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException, NoSuchObjectException {
        return getPartitionsViaOrmFilter(dbName, tblName, partNames);
      }
    }.run(false);
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return getPartitionsByExprInternal(
        dbName, tblName, expr, defaultPartitionName, maxParts, result, true, true);
  }

  protected boolean getPartitionsByExprInternal(String dbName, String tblName, final byte[] expr,
      final String defaultPartitionName, final  short maxParts, List<Partition> result,
      boolean allowSql, boolean allowJdo) throws TException {
    assert result != null;
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    final AtomicBoolean hasUnknownPartitions = new AtomicBoolean(false);
    result.addAll(new GetListHelper<Partition>(dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        // If we have some sort of expression tree, try SQL filter pushdown.
        List<Partition> result = null;
        if (exprTree != null) {
          SqlFilterForPushdown filter = new SqlFilterForPushdown();
          if (directSql.generateSqlFilterForPushdown(ctx.getTable(), exprTree, filter)) {
            return directSql.getPartitionsViaSqlFilter(filter, null);
          }
        }
        // We couldn't do SQL filter pushdown. Get names via normal means.
        List<String> partNames = new LinkedList<String>();
        hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
            ctx.getTable(), expr, defaultPartitionName, maxParts, partNames));
        return directSql.getPartitionsViaSqlFilter(dbName, tblName, partNames);
      }

      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException, NoSuchObjectException {
        // If we have some sort of expression tree, try JDOQL filter pushdown.
        List<Partition> result = null;
        if (exprTree != null) {
          result = getPartitionsViaOrmFilter(ctx.getTable(), exprTree, maxParts, false);
        }
        if (result == null) {
          // We couldn't do JDOQL filter pushdown. Get names via normal means.
          List<String> partNames = new ArrayList<String>();
          hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
              ctx.getTable(), expr, defaultPartitionName, maxParts, partNames));
          result = getPartitionsViaOrmFilter(dbName, tblName, partNames);
        }
        return result;
      }
    }.run(true));
    return hasUnknownPartitions.get();
  }



  /**
   * Gets the partition names from a table, pruned using an expression.
   * @param table Table.
   * @param expr Expression.
   * @param defaultPartName Default partition name from job config, if any.
   * @param maxParts Maximum number of partition names to return.
   * @param result The resulting names.
   * @return Whether the result contains any unknown partitions.
   */
  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result) throws MetaException {
    result.addAll(getPartitionNamesNoTxn(
        table.getDbName(), table.getTableName(), maxParts));
    List<String> columnNames = new ArrayList<String>();
    List<PrimitiveTypeInfo> typeInfos = new ArrayList<PrimitiveTypeInfo>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      columnNames.add(fs.getName());
      typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = HiveConf.getVar(getConf(), HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        columnNames, typeInfos, expr, defaultPartName, result);
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) filter pushdown.
   * @param table The table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param maxParts Maximum number of partitions to return.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown by a client
   *   (old hive client or non-hive one); if it was and we fail to create a filter, we will throw.
   * @return Resulting partitions. Can be null if isValidatedFilter is false, and
   *         there was error deriving the JDO filter.
   */
  private List<Partition> getPartitionsViaOrmFilter(Table table, ExpressionTree tree,
      short maxParts, boolean isValidatedFilter) throws MetaException {
    Map<String, Object> params = new HashMap<String, Object>();
    String jdoFilter =
        makeQueryFilterString(table.getDbName(), table, tree, params, isValidatedFilter);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      return null;
    }
    Query query = pm.newQuery(MPartition.class, jdoFilter);
    if (maxParts >= 0) {
      // User specified a row limit, set it on the Query
      query.setRange(0, maxParts);
    }
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    query.setOrdering("partitionName ascending");
    @SuppressWarnings("unchecked")
    List<MPartition> mparts = (List<MPartition>) query.executeWithMap(params);
    LOG.debug("Done executing query for getPartitionsViaOrmFilter");
    pm.retrieveAll(mparts); // TODO: why is this inconsistent with what we get by names?
    LOG.debug("Done retrieving all objects for getPartitionsViaOrmFilter");
    List<Partition> results = convertToParts(mparts);
    query.closeAll();
    return results;
  }


  private Integer getNumPartitionsViaOrmFilter(Table table, ExpressionTree tree, boolean isValidatedFilter)
    throws MetaException {
    Map<String, Object> params = new HashMap<String, Object>();
    String jdoFilter = makeQueryFilterString(table.getDbName(), table, tree, params, isValidatedFilter);
    if (jdoFilter == null) {
      assert !isValidatedFilter;
      return null;
    }

    Query query = pm.newQuery(
        "select count(partitionName) from org.apache.hadoop.hive.metastore.model.MPartition"
    );
    query.setFilter(jdoFilter);
    String parameterDeclaration = makeParameterDeclarationStringObj(params);
    query.declareParameters(parameterDeclaration);
    Long result = (Long) query.executeWithMap(params);
    query.closeAll();

    return result.intValue();
  }

  /**
   * Gets partition names from the table via ORM (JDOQL) name filter.
   * @param dbName Database name.
   * @param tblName Table name.
   * @param partNames Partition names to get the objects for.
   * @return Resulting partitions.
   */
  private List<Partition> getPartitionsViaOrmFilter(
      String dbName, String tblName, List<String> partNames) throws MetaException {
    if (partNames.isEmpty()) {
      return new ArrayList<Partition>();
    }
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setResultClass(MPartition.class);
    query.setClass(MPartition.class);
    query.setOrdering("partitionName ascending");
    @SuppressWarnings("unchecked")
    List<MPartition> mparts = (List<MPartition>)query.executeWithMap(queryWithParams.getSecond());
    List<Partition> partitions = convertToParts(dbName, tblName, mparts);
    if (query != null) {
      query.closeAll();
    }
    return partitions;
  }

  private void dropPartitionsNoTxn(String dbName, String tblName, List<String> partNames) {
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setClass(MPartition.class);
    long deleted = query.deletePersistentAll(queryWithParams.getSecond());
    LOG.debug("Deleted " + deleted + " partition from store");
    query.closeAll();
  }

  /**
   * Detaches column descriptors from storage descriptors; returns the set of unique CDs
   * thus detached. This is done before dropping partitions because CDs are reused between
   * SDs; so, we remove the links to delete SDs and then check the returned CDs to see if
   * they are referenced by other SDs.
   */
  private HashSet<MColumnDescriptor> detachCdsFromSdsNoTxn(
      String dbName, String tblName, List<String> partNames) {
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setClass(MPartition.class);
    query.setResult("sd");
    @SuppressWarnings("unchecked")
    List<MStorageDescriptor> sds = (List<MStorageDescriptor>)query.executeWithMap(
        queryWithParams.getSecond());
    HashSet<MColumnDescriptor> candidateCds = new HashSet<MColumnDescriptor>();
    for (MStorageDescriptor sd : sds) {
      if (sd != null && sd.getCD() != null) {
        candidateCds.add(sd.getCD());
        sd.setCD(null);
      }
    }
    if (query != null) {
      query.closeAll();
    }
    return candidateCds;
  }

  private ObjectPair<Query, Map<String, String>> getPartQueryWithParams(String dbName,
      String tblName, List<String> partNames) {
    StringBuilder sb = new StringBuilder("table.tableName == t1 && table.database.name == t2 && (");
    int n = 0;
    Map<String, String> params = new HashMap<String, String>();
    for (Iterator<String> itr = partNames.iterator(); itr.hasNext();) {
      String pn = "p" + n;
      n++;
      String part = itr.next();
      params.put(pn, part);
      sb.append("partitionName == ").append(pn);
      sb.append(" || ");
    }
    sb.setLength(sb.length() - 4); // remove the last " || "
    sb.append(')');
    Query query = pm.newQuery();
    query.setFilter(sb.toString());
    LOG.debug(" JDOQL filter is " + sb.toString());
    params.put("t1", HiveStringUtils.normalizeIdentifier(tblName));
    params.put("t2", HiveStringUtils.normalizeIdentifier(dbName));
    query.declareParameters(makeParameterDeclarationString(params));
    return new ObjectPair<Query, Map<String, String>>(query, params);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException {
    return getPartitionsByFilterInternal(dbName, tblName, filter, maxParts, true, true);
  }

  /** Helper class for getting stuff w/transaction, direct SQL, perf logging, etc. */
  @VisibleForTesting
  public abstract class GetHelper<T> {
    private final boolean isInTxn, doTrace, allowJdo;
    private boolean doUseDirectSql;
    private long start;
    private Table table;
    protected final String dbName, tblName;
    private boolean success = false;
    protected T results = null;

    public GetHelper(String dbName, String tblName, boolean allowSql, boolean allowJdo)
        throws MetaException {
      assert allowSql || allowJdo;
      this.allowJdo = allowJdo;
      this.dbName = HiveStringUtils.normalizeIdentifier(dbName);
      if (tblName != null){
        this.tblName = HiveStringUtils.normalizeIdentifier(tblName);
      } else {
        // tblName can be null in cases of Helper being used at a higher
        // abstraction level, such as with datbases
        this.tblName = null;
        this.table = null;
      }
      this.doTrace = LOG.isDebugEnabled();
      this.isInTxn = isActiveTransaction();

      // SQL usage inside a larger transaction (e.g. droptable) may not be desirable because
      // some databases (e.g. Postgres) abort the entire transaction when any query fails, so
      // the fallback from failed SQL to JDO is not possible.
      boolean isConfigEnabled = HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL)
          && (HiveConf.getBoolVar(getConf(), ConfVars.METASTORE_TRY_DIRECT_SQL_DDL) || !isInTxn);
      if (isConfigEnabled && directSql == null) {
        directSql = new MetaStoreDirectSql(pm, getConf());
      }

      if (!allowJdo && isConfigEnabled && !directSql.isCompatibleDatastore()) {
        throw new MetaException("SQL is not operational"); // test path; SQL is enabled and broken.
      }
      this.doUseDirectSql = allowSql && isConfigEnabled && directSql.isCompatibleDatastore();
    }

    protected boolean canUseDirectSql(GetHelper<T> ctx) throws MetaException {
      return true; // By default, assume we can user directSQL - that's kind of the point.
    }
    protected abstract String describeResult();
    protected abstract T getSqlResult(GetHelper<T> ctx) throws MetaException;
    protected abstract T getJdoResult(
        GetHelper<T> ctx) throws MetaException, NoSuchObjectException;

    public T run(boolean initTable) throws MetaException, NoSuchObjectException {
      try {
        start(initTable);
        if (doUseDirectSql) {
          try {
            directSql.prepareTxn();
            this.results = getSqlResult(this);
          } catch (Exception ex) {
            handleDirectSqlError(ex);
          }
        }
        // Note that this will be invoked in 2 cases:
        //    1) DirectSQL was disabled to start with;
        //    2) DirectSQL threw and was disabled in handleDirectSqlError.
        if (!doUseDirectSql) {
          this.results = getJdoResult(this);
        }
        return commit();
      } catch (NoSuchObjectException ex) {
        throw ex;
      } catch (MetaException ex) {
        throw ex;
      } catch (Exception ex) {
        LOG.error("", ex);
        throw new MetaException(ex.getMessage());
      } finally {
        close();
      }
    }

    private void start(boolean initTable) throws MetaException, NoSuchObjectException {
      start = doTrace ? System.nanoTime() : 0;
      openTransaction();
      if (initTable && (tblName != null)) {
        table = ensureGetTable(dbName, tblName);
      }
      doUseDirectSql = doUseDirectSql && canUseDirectSql(this);
    }

    private void handleDirectSqlError(Exception ex) throws MetaException, NoSuchObjectException {
      String message = null;
      try {
        message = generateShorterMessage(ex);
      } catch (Throwable t) {
        message = ex.toString() + "; error building a better message: " + t.getMessage();
      }
      LOG.warn(message); // Don't log the exception, people just get confused.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Full DirectSQL callstack for debugging (note: this is not an error)", ex);
      }
      if (!allowJdo) {
        if (ex instanceof MetaException) {
          throw (MetaException)ex;
        }
        throw new MetaException(ex.getMessage());
      }
      if (!isInTxn) {
        JDOException rollbackEx = null;
        try {
          rollbackTransaction();
        } catch (JDOException jex) {
          rollbackEx = jex;
        }
        if (rollbackEx != null) {
          // Datanucleus propagates some pointless exceptions and rolls back in the finally.
          if (currentTransaction != null && currentTransaction.isActive()) {
            throw rollbackEx; // Throw if the tx wasn't rolled back.
          }
          LOG.info("Ignoring exception, rollback succeeded: " + rollbackEx.getMessage());
        }

        start = doTrace ? System.nanoTime() : 0;
        openTransaction();
        if (table != null) {
          table = ensureGetTable(dbName, tblName);
        }
      } else {
        start = doTrace ? System.nanoTime() : 0;
      }

      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        try {
          metrics.incrementCounter(MetricsConstant.DIRECTSQL_ERRORS);
        } catch (Exception e) {
          LOG.warn("Error reporting Direct SQL errors to metrics system", e);
        }
      }

      doUseDirectSql = false;
    }

    private String generateShorterMessage(Exception ex) {
      StringBuilder message = new StringBuilder(
          "Falling back to ORM path due to direct SQL failure (this is not an error): ");
      Throwable t = ex;
      StackTraceElement[] prevStack = null;
      while (t != null) {
        message.append(t.getMessage());
        StackTraceElement[] stack = t.getStackTrace();
        int uniqueFrames = stack.length - 1;
        if (prevStack != null) {
          int n = prevStack.length - 1;
          while (uniqueFrames >= 0 && n >= 0 && stack[uniqueFrames].equals(prevStack[n])) {
            uniqueFrames--; n--;
          }
        }
        for (int i = 0; i <= uniqueFrames; ++i) {
          StackTraceElement ste = stack[i];
          message.append(" at ").append(ste);
          if (ste.getMethodName() != null && ste.getMethodName().contains("getSqlResult")
              && (ste.getFileName() == null || ste.getFileName().contains("ObjectStore"))) {
            break;
          }
        }
        prevStack = stack;
        t = t.getCause();
        if (t != null) {
          message.append(";\n Caused by: ");
        }
      }
      return message.toString();
    }

    private T commit() {
      success = commitTransaction();
      if (doTrace) {
        LOG.debug(describeResult() + " retrieved using " + (doUseDirectSql ? "SQL" : "ORM")
            + " in " + ((System.nanoTime() - start) / 1000000.0) + "ms");
      }
      return results;
    }

    private void close() {
      if (!success) {
        rollbackTransaction();
      }
    }

    public Table getTable() {
      return table;
    }
  }

  private abstract class GetListHelper<T> extends GetHelper<List<T>> {
    public GetListHelper(
        String dbName, String tblName, boolean allowSql, boolean allowJdo) throws MetaException {
      super(dbName, tblName, allowSql, allowJdo);
    }

    @Override
    protected String describeResult() {
      return results.size() + " entries";
    }
  }

  @VisibleForTesting
  public abstract class GetDbHelper extends GetHelper<Database> {
    /**
     * GetHelper for returning db info using directSql/JDO.
     * Since this is a db-level call, tblName is ignored, and null is passed irrespective of what is passed in.
     * @param dbName The Database Name
     * @param tblName Placeholder param to match signature, always ignored.
     * @param allowSql Whether or not we allow DirectSQL to perform this query.
     * @param allowJdo Whether or not we allow ORM to perform this query.
     * @throws MetaException
     */
    public GetDbHelper(
        String dbName, String tblName, boolean allowSql, boolean allowJdo) throws MetaException {
      super(dbName,null,allowSql,allowJdo);
    }

    @Override
    protected String describeResult() {
      return "db details for db " + dbName;
    }
  }

  private abstract class GetStatHelper extends GetHelper<ColumnStatistics> {
    public GetStatHelper(
        String dbName, String tblName, boolean allowSql, boolean allowJdo) throws MetaException {
      super(dbName, tblName, allowSql, allowJdo);
    }

    @Override
    protected String describeResult() {
      return "statistics for " + (results == null ? 0 : results.getStatsObjSize()) + " columns";
    }
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName,
                                      String filter) throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;

    return new GetHelper<Integer>(dbName, tblName, true, true) {
      private SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected String describeResult() {
        return "Partition count";
      }

      protected boolean canUseDirectSql(GetHelper<Integer> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), exprTree, filter);
      };

      @Override
      protected Integer getSqlResult(GetHelper<Integer> ctx) throws MetaException {
        return directSql.getNumPartitionsViaSqlFilter(filter);
      }
      @Override
      protected Integer getJdoResult(
          GetHelper<Integer> ctx) throws MetaException, NoSuchObjectException {
        return getNumPartitionsViaOrmFilter(ctx.getTable(), exprTree, true);
      }
    }.run(true);
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName,
                                             byte[] expr) throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    final byte[] tempExpr = expr; // Need to be final to pass it to an inner class


    return new GetHelper<Integer>(dbName, tblName, true, true) {
      private SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected String describeResult() {
        return "Partition count";
      }

      protected boolean canUseDirectSql(GetHelper<Integer> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), exprTree, filter);
      };

      @Override
      protected Integer getSqlResult(GetHelper<Integer> ctx) throws MetaException {
        return directSql.getNumPartitionsViaSqlFilter(filter);
      }
      @Override
      protected Integer getJdoResult(
          GetHelper<Integer> ctx) throws MetaException, NoSuchObjectException {
        Integer numPartitions = null;

        if (exprTree != null) {
          try {
            numPartitions = getNumPartitionsViaOrmFilter(ctx.getTable(), exprTree, true);
          } catch (MetaException e) {
            numPartitions = null;
          }
        }

        // if numPartitions could not be obtained from ORM filters, then get number partitions names, and count them
        if (numPartitions == null) {
          List<String> filteredPartNames = new ArrayList<String>();
          getPartitionNamesPrunedByExprNoTxn(ctx.getTable(), tempExpr, "", (short) -1, filteredPartNames);
          numPartitions = filteredPartNames.size();
        }

        return numPartitions;
      }
    }.run(true);
  }

  protected List<Partition> getPartitionsByFilterInternal(String dbName, String tblName,
      String filter, final short maxParts, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return new GetListHelper<Partition>(dbName, tblName, allowSql, allowJdo) {
      private SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected boolean canUseDirectSql(GetHelper<List<Partition>> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), tree, filter);
      };

      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        return directSql.getPartitionsViaSqlFilter(filter, (maxParts < 0) ? null : (int)maxParts);
      }

      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException, NoSuchObjectException {
        return getPartitionsViaOrmFilter(ctx.getTable(), tree, maxParts, true);
      }
    }.run(true);
  }

  /**
   * Gets the table object for a given table, throws if anything goes wrong.
   * @param dbName Database name.
   * @param tblName Table name.
   * @return Table object.
   */
  private MTable ensureGetMTable(
      String dbName, String tblName) throws NoSuchObjectException, MetaException {
    MTable mtable = getMTable(dbName, tblName);
    if (mtable == null) {
      throw new NoSuchObjectException("Specified database/table does not exist : "
          + dbName + "." + tblName);
    }
    return mtable;
  }

  private Table ensureGetTable(
      String dbName, String tblName) throws NoSuchObjectException, MetaException {
    return convertToTable(ensureGetMTable(dbName, tblName));
  }

  /**
   * Makes a JDO query filter string.
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param mtable Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param filter The filter from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @return Resulting filter.
   */
  private String makeQueryFilterString(String dbName, MTable mtable, String filter,
      Map<String, Object> params) throws MetaException {
    ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return makeQueryFilterString(dbName, convertToTable(mtable), tree, params, true);
  }

  /**
   * Makes a JDO query filter string for tables or partitions.
   * @param dbName Database name.
   * @param table Table. If null, the query returned is over tables in a database.
   *   If not null, the query returned is over partitions in a table.
   * @param tree The expression tree from which JDOQL filter will be made.
   * @param params Parameters for the filter. Some parameters may be added here.
   * @param isValidatedFilter Whether the filter was pre-validated for JDOQL pushdown
   *   by the client; if it was and we fail to create a filter, we will throw.
   * @return Resulting filter. Can be null if isValidatedFilter is false, and there was error.
   */
  private String makeQueryFilterString(String dbName, Table table, ExpressionTree tree,
      Map<String, Object> params, boolean isValidatedFilter) throws MetaException {
    assert tree != null;
    FilterBuilder queryBuilder = new FilterBuilder(isValidatedFilter);
    if (table != null) {
      queryBuilder.append("table.tableName == t1 && table.database.name == t2");
      params.put("t1", table.getTableName());
      params.put("t2", table.getDbName());
    } else {
      queryBuilder.append("database.name == dbName");
      params.put("dbName", dbName);
    }

    tree.generateJDOFilterFragment(getConf(), table, params, queryBuilder);
    if (queryBuilder.hasError()) {
      assert !isValidatedFilter;
      LOG.info("JDO filter pushdown cannot be used: " + queryBuilder.getErrorMessage());
      return null;
    }
    String jdoFilter = queryBuilder.getFilter();
    LOG.debug("jdoFilter = " + jdoFilter);
    return jdoFilter;
  }

  private String makeParameterDeclarationString(Map<String, String> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (String key : params.keySet()) {
      paramDecl.append(", java.lang.String " + key);
    }
    return paramDecl.toString();
  }

  private String makeParameterDeclarationStringObj(Map<String, Object> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (Entry<String, Object> entry : params.entrySet()) {
      paramDecl.append(", ");
      paramDecl.append(entry.getValue().getClass().getName());
      paramDecl.append(" ");
      paramDecl.append(entry.getKey());
    }
    return paramDecl.toString();
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException {
    boolean success = false;
    Query query = null;
    List<String> tableNames = new ArrayList<String>();
    try {
      openTransaction();
      LOG.debug("Executing listTableNamesByFilter");
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      Map<String, Object> params = new HashMap<String, Object>();
      String queryFilterString = makeQueryFilterString(dbName, null, filter, params);
      query = pm.newQuery(MTable.class);
      query.declareImports("import java.lang.String");
      query.setResult("tableName");
      query.setResultClass(java.lang.String.class);
      if (maxTables >= 0) {
        query.setRange(0, maxTables);
      }
      LOG.debug("filter specified is " + filter + "," + " JDOQL filter is " + queryFilterString);
      for (Entry<String, Object> entry : params.entrySet()) {
        LOG.debug("key: " + entry.getKey() + " value: " + entry.getValue() + " class: "
            + entry.getValue().getClass().getName());
      }
      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      query.setFilter(queryFilterString);
      Collection names = (Collection)query.executeWithMap(params);
      // have to emulate "distinct", otherwise tables with the same name may be returned
      Set<String> tableNamesSet = new HashSet<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        tableNamesSet.add((String) i.next());
      }
      tableNames = new ArrayList<String>(tableNamesSet);
      LOG.debug("Done executing query for listTableNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listTableNamesByFilter");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return tableNames;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName, String tableName, String filter,
      short maxParts) throws MetaException {
    boolean success = false;
    Query query = null;
    List<String> partNames = new ArrayList<String>();
    try {
      openTransaction();
      LOG.debug("Executing listMPartitionNamesByFilter");
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      tableName = HiveStringUtils.normalizeIdentifier(tableName);
      MTable mtable = getMTable(dbName, tableName);
      if (mtable == null) {
        // To be consistent with the behavior of listPartitionNames, if the
        // table or db does not exist, we return an empty list
        return partNames;
      }
      Map<String, Object> params = new HashMap<String, Object>();
      String queryFilterString = makeQueryFilterString(dbName, mtable, filter, params);
      query =
          pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
              + "where " + queryFilterString);
      if (maxParts >= 0) {
        // User specified a row limit, set it on the Query
        query.setRange(0, maxParts);
      }
      LOG.debug("Filter specified is " + filter + "," + " JDOQL filter is " + queryFilterString);
      LOG.debug("Parms is " + params);
      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      query.setOrdering("partitionName ascending");
      query.setResult("partitionName");
      Collection names = (Collection) query.executeWithMap(params);
      partNames = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        partNames.add((String) i.next());
      }
      LOG.debug("Done executing query for listMPartitionNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitionNamesByFilter");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return partNames;
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = HiveStringUtils.normalizeIdentifier(name);
      dbname = HiveStringUtils.normalizeIdentifier(dbname);
      MTable newt = convertToMTable(newTable);
      if (newt == null) {
        throw new InvalidObjectException("new table is invalid");
      }

      MTable oldt = getMTable(dbname, name);
      if (oldt == null) {
        throw new MetaException("table " + dbname + "." + name + " doesn't exist");
      }

      // For now only alter name, owner, parameters, cols, bucketcols are allowed
      oldt.setDatabase(newt.getDatabase());
      oldt.setTableName(HiveStringUtils.normalizeIdentifier(newt.getTableName()));
      oldt.setParameters(newt.getParameters());
      oldt.setOwner(newt.getOwner());
      // Fully copy over the contents of the new SD into the old SD,
      // so we don't create an extra SD in the metastore db that has no references.
      copyMSD(newt.getSd(), oldt.getSd());
      oldt.setRetention(newt.getRetention());
      oldt.setPartitionKeys(newt.getPartitionKeys());
      oldt.setTableType(newt.getTableType());
      oldt.setLastAccessTime(newt.getLastAccessTime());
      oldt.setViewOriginalText(newt.getViewOriginalText());
      oldt.setViewExpandedText(newt.getViewExpandedText());
      oldt.setRewriteEnabled(newt.isRewriteEnabled());

      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      name = HiveStringUtils.normalizeIdentifier(name);
      baseTblName = HiveStringUtils.normalizeIdentifier(baseTblName);
      dbname = HiveStringUtils.normalizeIdentifier(dbname);
      MIndex newi = convertToMIndex(newIndex);
      if (newi == null) {
        throw new InvalidObjectException("new index is invalid");
      }

      MIndex oldi = getMIndex(dbname, baseTblName, name);
      if (oldi == null) {
        throw new MetaException("index " + name + " doesn't exist");
      }

      // For now only alter parameters are allowed
      oldi.setParameters(newi.getParameters());

      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private void alterPartitionNoTxn(String dbname, String name, List<String> part_vals,
      Partition newPart) throws InvalidObjectException, MetaException {
    name = HiveStringUtils.normalizeIdentifier(name);
    dbname = HiveStringUtils.normalizeIdentifier(dbname);
    MPartition oldp = getMPartition(dbname, name, part_vals);
    MPartition newp = convertToMPart(newPart, false);
    if (oldp == null || newp == null) {
      throw new InvalidObjectException("partition does not exist.");
    }
    oldp.setValues(newp.getValues());
    oldp.setPartitionName(newp.getPartitionName());
    oldp.setParameters(newPart.getParameters());
    if (!TableType.VIRTUAL_VIEW.name().equals(oldp.getTable().getTableType())) {
      copyMSD(newp.getSd(), oldp.getSd());
    }
    if (newp.getCreateTime() != oldp.getCreateTime()) {
      oldp.setCreateTime(newp.getCreateTime());
    }
    if (newp.getLastAccessTime() != oldp.getLastAccessTime()) {
      oldp.setLastAccessTime(newp.getLastAccessTime());
    }
  }

  @Override
  public void alterPartition(String dbname, String name, List<String> part_vals, Partition newPart)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      alterPartitionNoTxn(dbname, name, part_vals, newPart);
      // commit the changes
      success = commitTransaction();
    } catch (Exception exception) {
      e = exception;
    } finally {
      if (!success) {
        rollbackTransaction();
        MetaException metaException = new MetaException(
            "The transaction for alter partition did not commit successfully.");
        if (e != null) {
          metaException.initCause(e);
        }
        throw metaException;
      }
    }
  }

  @Override
  public void alterPartitions(String dbname, String name, List<List<String>> part_vals,
      List<Partition> newParts) throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      Iterator<List<String>> part_val_itr = part_vals.iterator();
      for (Partition tmpPart: newParts) {
        List<String> tmpPartVals = part_val_itr.next();
        alterPartitionNoTxn(dbname, name, tmpPartVals, tmpPart);
      }
      // commit the changes
      success = commitTransaction();
    } catch (Exception exception) {
      e = exception;
    } finally {
      if (!success) {
        rollbackTransaction();
        MetaException metaException = new MetaException(
            "The transaction for alter partition did not commit successfully.");
        if (e != null) {
          metaException.initCause(e);
        }
        throw metaException;
      }
    }
  }

  private void copyMSD(MStorageDescriptor newSd, MStorageDescriptor oldSd) {
    oldSd.setLocation(newSd.getLocation());
    MColumnDescriptor oldCD = oldSd.getCD();
    // If the columns of the old column descriptor != the columns of the new one,
    // then change the old storage descriptor's column descriptor.
    // Convert the MFieldSchema's to their thrift object counterparts, because we maintain
    // datastore identity (i.e., identity of the model objects are managed by JDO,
    // not the application).
    if (!(oldSd != null && oldSd.getCD() != null &&
         oldSd.getCD().getCols() != null &&
         newSd != null && newSd.getCD() != null &&
         newSd.getCD().getCols() != null &&
         convertToFieldSchemas(newSd.getCD().getCols()).
         equals(convertToFieldSchemas(oldSd.getCD().getCols()))
       )) {
        oldSd.setCD(newSd.getCD());
    }

    //If oldCd does not have any more references, then we should delete it
    // from the backend db
    removeUnusedColumnDescriptor(oldCD);
    oldSd.setBucketCols(newSd.getBucketCols());
    oldSd.setCompressed(newSd.isCompressed());
    oldSd.setInputFormat(newSd.getInputFormat());
    oldSd.setOutputFormat(newSd.getOutputFormat());
    oldSd.setNumBuckets(newSd.getNumBuckets());
    oldSd.getSerDeInfo().setName(newSd.getSerDeInfo().getName());
    oldSd.getSerDeInfo().setSerializationLib(
        newSd.getSerDeInfo().getSerializationLib());
    oldSd.getSerDeInfo().setParameters(newSd.getSerDeInfo().getParameters());
    oldSd.setSkewedColNames(newSd.getSkewedColNames());
    oldSd.setSkewedColValues(newSd.getSkewedColValues());
    oldSd.setSkewedColValueLocationMaps(newSd.getSkewedColValueLocationMaps());
    oldSd.setSortCols(newSd.getSortCols());
    oldSd.setParameters(newSd.getParameters());
    oldSd.setStoredAsSubDirectories(newSd.isStoredAsSubDirectories());
  }

  /**
   * Checks if a column descriptor has any remaining references by storage descriptors
   * in the db.  If it does not, then delete the CD.  If it does, then do nothing.
   * @param oldCD the column descriptor to delete if it is no longer referenced anywhere
   */
  private void removeUnusedColumnDescriptor(MColumnDescriptor oldCD) {
    if (oldCD == null) {
      return;
    }

    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      LOG.debug("execute removeUnusedColumnDescriptor");
      List<MStorageDescriptor> referencedSDs = listStorageDescriptorsWithCD(oldCD, 1, queryWrapper);
      //if no other SD references this CD, we can throw it out.
      if (referencedSDs != null && referencedSDs.isEmpty()) {
        pm.retrieve(oldCD);
        pm.deletePersistent(oldCD);
      }
      success = commitTransaction();
      LOG.debug("successfully deleted a CD in removeUnusedColumnDescriptor");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      queryWrapper.close();
    }
  }

  /**
   * Called right before an action that would drop a storage descriptor.
   * This function makes the SD's reference to a CD null, and then deletes the CD
   * if it no longer is referenced in the table.
   * @param msd the storage descriptor to drop
   */
  private void preDropStorageDescriptor(MStorageDescriptor msd) {
    if (msd == null || msd.getCD() == null) {
      return;
    }

    MColumnDescriptor mcd = msd.getCD();
    // Because there is a 1-N relationship between CDs and SDs,
    // we must set the SD's CD to null first before dropping the storage descriptor
    // to satisfy foreign key constraints.
    msd.setCD(null);
    removeUnusedColumnDescriptor(mcd);
  }

  /**
   * Get a list of storage descriptors that reference a particular Column Descriptor
   * @param oldCD the column descriptor to get storage descriptors for
   * @param maxSDs the maximum number of SDs to return
   * @return a list of storage descriptors
   */
  private List<MStorageDescriptor> listStorageDescriptorsWithCD(
      MColumnDescriptor oldCD,
      long maxSDs,
      QueryWrapper queryWrapper) {
    boolean success = false;
    List<MStorageDescriptor> sds = null;
    try {
      openTransaction();
      LOG.debug("Executing listStorageDescriptorsWithCD");
      Query query = queryWrapper.query = pm.newQuery(MStorageDescriptor.class, "this.cd == inCD");
      query.declareParameters("MColumnDescriptor inCD");
      if (maxSDs >= 0) {
        // User specified a row limit, set it on the Query
        query.setRange(0, maxSDs);
      }
      sds = (List<MStorageDescriptor>)query.execute(oldCD);
      LOG.debug("Done executing query for listStorageDescriptorsWithCD");
      pm.retrieveAll(sds);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listStorageDescriptorsWithCD");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return sds;
  }

  private int getColumnIndexFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return -1;
    }
    for (int i = 0; i < cols.size(); i++) {
      MFieldSchema mfs = cols.get(i);
      if (mfs.getName().equalsIgnoreCase(col)) {
        return i;
      }
    }
    return -1;
  }

  private  boolean constraintNameAlreadyExists(String name) {
    boolean commited = false;
    Query constraintExistsQuery = null;
    String constraintNameIfExists = null;
    try {
      openTransaction();
      name = HiveStringUtils.normalizeIdentifier(name);
      constraintExistsQuery = pm.newQuery(MConstraint.class, "constraintName == name");
      constraintExistsQuery.declareParameters("java.lang.String name");
      constraintExistsQuery.setUnique(true);
      constraintExistsQuery.setResult("name");
      constraintNameIfExists = (String) constraintExistsQuery.execute(name);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (constraintExistsQuery != null) {
          constraintExistsQuery.closeAll();
      }
    }
    return constraintNameIfExists != null && !constraintNameIfExists.isEmpty();
  }

  private String generateConstraintName(String... parameters) throws MetaException {
    int hashcode = ArrayUtils.toString(parameters).hashCode();
    int counter = 0;
    final int MAX_RETRIES = 10;
    while (counter < MAX_RETRIES) {
      String currName = (parameters.length == 0 ? "constraint_" : parameters[parameters.length-1]) +
        "_" + hashcode + "_" + System.currentTimeMillis() + "_" + (counter++);
      if (!constraintNameAlreadyExists(currName)) {
        return currName;
      }
    }
    throw new MetaException("Error while trying to generate the constraint name for " + ArrayUtils.toString(parameters));
  }

  @Override
  public void addForeignKeys(
    List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
   addForeignKeys(fks, true);
  }

  private void addForeignKeys(
    List<SQLForeignKey> fks, boolean retrieveCD) throws InvalidObjectException,
    MetaException {
    List<MConstraint> mpkfks = new ArrayList<MConstraint>();
    String currentConstraintName = null;

    for (int i = 0; i < fks.size(); i++) {
      AttachedMTableInfo nParentTable = getMTable(fks.get(i).getPktable_db(), fks.get(i).getPktable_name(), retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + fks.get(i).getPktable_name());
      }

      AttachedMTableInfo nChildTable = getMTable(fks.get(i).getFktable_db(), fks.get(i).getFktable_name(), retrieveCD);
      MTable childTable = nChildTable.mtbl;
      if (childTable == null) {
        throw new InvalidObjectException("Child table not found: " + fks.get(i).getFktable_name());
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      List<MFieldSchema> parentCols = parentCD == null ? null : parentCD.getCols();
      int parentIntegerIndex =
        getColumnIndexFromTableColumns(parentCols, fks.get(i).getPkcolumn_name());
      if (parentIntegerIndex == -1) {
        throw new InvalidObjectException("Parent column not found: " + fks.get(i).getPkcolumn_name());
      }

      MColumnDescriptor childCD = retrieveCD ? nChildTable.mcd : childTable.getSd().getCD();
      List<MFieldSchema> childCols = childCD.getCols();
      int childIntegerIndex =
        getColumnIndexFromTableColumns(childCols, fks.get(i).getFkcolumn_name());
      if (childIntegerIndex == -1) {
        throw new InvalidObjectException("Child column not found: " + fks.get(i).getFkcolumn_name());
      }

      if (fks.get(i).getFk_name() == null) {
        // When there is no explicit foreign key name associated with the constraint and the key is composite,
        // we expect the foreign keys to be send in order in the input list.
        // Otherwise, the below code will break.
        // If this is the first column of the FK constraint, generate the foreign key name
        // NB: The below code can result in race condition where duplicate names can be generated (in theory).
        // However, this scenario can be ignored for practical purposes because of
        // the uniqueness of the generated constraint name.
        if (fks.get(i).getKey_seq() == 1) {
          currentConstraintName = generateConstraintName(fks.get(i).getFktable_db(), fks.get(i).getFktable_name(),
            fks.get(i).getPktable_db(), fks.get(i).getPktable_name(),
            fks.get(i).getPkcolumn_name(), fks.get(i).getFkcolumn_name(), "fk");
        }
      } else {
        currentConstraintName = fks.get(i).getFk_name();
      }
      Integer updateRule = fks.get(i).getUpdate_rule();
      Integer deleteRule = fks.get(i).getDelete_rule();
      int enableValidateRely = (fks.get(i).isEnable_cstr() ? 4 : 0) +
      (fks.get(i).isValidate_cstr() ? 2 : 0) + (fks.get(i).isRely_cstr() ? 1 : 0);
      MConstraint mpkfk = new MConstraint(
        currentConstraintName,
        MConstraint.FOREIGN_KEY_CONSTRAINT,
        fks.get(i).getKey_seq(),
        deleteRule,
        updateRule,
        enableValidateRely,
        parentTable,
        childTable,
        parentCD,
        childCD,
        childIntegerIndex,
        parentIntegerIndex
      );
      mpkfks.add(mpkfk);
    }
    pm.makePersistentAll(mpkfks);
  }

  @Override
  public void addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException,
    MetaException {
    addPrimaryKeys(pks, true);
  }

  private void addPrimaryKeys(List<SQLPrimaryKey> pks, boolean retrieveCD) throws InvalidObjectException,
    MetaException {
    List<MConstraint> mpks = new ArrayList<MConstraint>();
    String constraintName = null;

    for (int i = 0; i < pks.size(); i++) {
      AttachedMTableInfo nParentTable =
        getMTable(pks.get(i).getTable_db(), pks.get(i).getTable_name(), retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + pks.get(i).getTable_name());
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex =
        getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), pks.get(i).getColumn_name());

      if (parentIntegerIndex == -1) {
        throw new InvalidObjectException("Parent column not found: " + pks.get(i).getColumn_name());
      }
      if (getPrimaryKeyConstraintName(
          parentTable.getDatabase().getName(), parentTable.getTableName()) != null) {
        throw new MetaException(" Primary key already exists for: " +
          parentTable.getDatabase().getName() + "." + pks.get(i).getTable_name());
      }
      if (pks.get(i).getPk_name() == null) {
        if (pks.get(i).getKey_seq() == 1) {
          constraintName = generateConstraintName(pks.get(i).getTable_db(), pks.get(i).getTable_name(),
            pks.get(i).getColumn_name(), "pk");
        }
      } else {
        constraintName = pks.get(i).getPk_name();
      }

      int enableValidateRely = (pks.get(i).isEnable_cstr() ? 4 : 0) +
      (pks.get(i).isValidate_cstr() ? 2 : 0) + (pks.get(i).isRely_cstr() ? 1 : 0);
      MConstraint mpk = new MConstraint(
        constraintName,
        MConstraint.PRIMARY_KEY_CONSTRAINT,
        pks.get(i).getKey_seq(),
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex);
      mpks.add(mpk);
    }
    pm.makePersistentAll(mpks);
  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException,
      MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MIndex idx = convertToMIndex(index);
      pm.makePersistent(idx);
      commited = commitTransaction();
      return true;
    } finally {
      if (!commited) {
        rollbackTransaction();
        return false;
      }
    }
  }

  private MIndex convertToMIndex(Index index) throws InvalidObjectException,
      MetaException {

    StorageDescriptor sd = index.getSd();
    if (sd == null) {
      throw new InvalidObjectException("Storage descriptor is not defined for index.");
    }

    MStorageDescriptor msd = this.convertToMStorageDescriptor(sd);
    MTable origTable = getMTable(index.getDbName(), index.getOrigTableName());
    if (origTable == null) {
      throw new InvalidObjectException(
          "Original table does not exist for the given index.");
    }

    String[] qualified = MetaStoreUtils.getQualifiedName(index.getDbName(), index.getIndexTableName());
    MTable indexTable = getMTable(qualified[0], qualified[1]);
    if (indexTable == null) {
      throw new InvalidObjectException(
          "Underlying index table does not exist for the given index.");
    }

    return new MIndex(HiveStringUtils.normalizeIdentifier(index.getIndexName()), origTable, index.getCreateTime(),
        index.getLastAccessTime(), index.getParameters(), indexTable, msd,
        index.getIndexHandlerClass(), index.isDeferredRebuild());
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      MIndex index = getMIndex(dbName, origTableName, indexName);
      if (index != null) {
        pm.deletePersistent(index);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private MIndex getMIndex(String dbName, String originalTblName, String indexName)
      throws MetaException {
    MIndex midx = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      originalTblName = HiveStringUtils.normalizeIdentifier(originalTblName);
      MTable mtbl = getMTable(dbName, originalTblName);
      if (mtbl == null) {
        commited = commitTransaction();
        return null;
      }
      query =
          pm.newQuery(MIndex.class,
              "origTable.tableName == t1 && origTable.database.name == t2 && indexName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      midx =
          (MIndex) query.execute(originalTblName, dbName,
              HiveStringUtils.normalizeIdentifier(indexName));
      pm.retrieve(midx);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return midx;
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    openTransaction();
    MIndex mIndex = this.getMIndex(dbName, origTableName, indexName);
    Index ret = convertToIndex(mIndex);
    commitTransaction();
    return ret;
  }

  private Index convertToIndex(MIndex mIndex) throws MetaException {
    if (mIndex == null) {
      return null;
    }

    MTable origTable = mIndex.getOrigTable();
    MTable indexTable = mIndex.getIndexTable();

    return new Index(
    mIndex.getIndexName(),
    mIndex.getIndexHandlerClass(),
    origTable.getDatabase().getName(),
    origTable.getTableName(),
    mIndex.getCreateTime(),
    mIndex.getLastAccessTime(),
    indexTable.getTableName(),
    convertToStorageDescriptor(mIndex.getSd()),
    mIndex.getParameters(),
    mIndex.getDeferredRebuild());

  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    boolean success = false;
    Query query = null;
    try {
      LOG.debug("Executing getIndexes");
      openTransaction();

      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      origTableName = HiveStringUtils.normalizeIdentifier(origTableName);
      query =
          pm.newQuery(MIndex.class, "origTable.tableName == t1 && origTable.database.name == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MIndex> mIndexes = (List<MIndex>) query.execute(origTableName, dbName);
      pm.retrieveAll(mIndexes);

      List<Index> indexes = new ArrayList<Index>(mIndexes.size());
      for (MIndex mIdx : mIndexes) {
        indexes.add(this.convertToIndex(mIdx));
      }
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for getIndexes");

      return indexes;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max)
      throws MetaException {
    List<String> pns = new ArrayList<String>();
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listIndexNames");
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      origTableName = HiveStringUtils.normalizeIdentifier(origTableName);
      query =
          pm.newQuery("select indexName from org.apache.hadoop.hive.metastore.model.MIndex "
              + "where origTable.database.name == t1 && origTable.tableName == t2 "
              + "order by indexName asc");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      query.setResult("indexName");
      Collection names = (Collection) query.execute(dbName, origTableName);
      for (Iterator i = names.iterator(); i.hasNext();) {
        pns.add((String) i.next());
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return pns;
  }

  @Override
  public boolean addRole(String roleName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      MRole nameCheck = this.getMRole(roleName);
      if (nameCheck != null) {
        throw new InvalidObjectException("Role " + roleName + " already exists.");
      }
      int now = (int)(System.currentTimeMillis()/1000);
      MRole mRole = new MRole(roleName, now, ownerName);
      pm.makePersistent(mRole);
      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  @Override
  public boolean grantRole(Role role, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, NoSuchObjectException,InvalidObjectException {
    boolean success = false;
    boolean commited = false;
    try {
      openTransaction();
      MRoleMap roleMap = null;
      try {
        roleMap = this.getMSecurityUserRoleMap(userName, principalType, role
            .getRoleName());
      } catch (Exception e) {
      }
      if (roleMap != null) {
        throw new InvalidObjectException("Principal " + userName
            + " already has the role " + role.getRoleName());
      }
      if (principalType == PrincipalType.ROLE) {
        validateRole(userName);
      }
      MRole mRole = getMRole(role.getRoleName());
      long now = System.currentTimeMillis()/1000;
      MRoleMap roleMember = new MRoleMap(userName, principalType.toString(),
          mRole, (int) now, grantor, grantorType.toString(), grantOption);
      pm.makePersistent(roleMember);
      commited = commitTransaction();
      success = true;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return success;
  }

  /**
   * Verify that role with given name exists, if not throw exception
   * @param roleName
   * @throws NoSuchObjectException
   */
  private void validateRole(String roleName) throws NoSuchObjectException {
    // if grantee is a role, check if it exists
    MRole granteeRole = getMRole(roleName);
    if (granteeRole == null) {
      throw new NoSuchObjectException("Role " + roleName + " does not exist");
    }
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
      boolean grantOption) throws MetaException, NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();
      MRoleMap roleMember = getMSecurityUserRoleMap(userName, principalType,
          role.getRoleName());
      if (grantOption) {
        // Revoke with grant option - only remove the grant option but keep the role.
        if (roleMember.getGrantOption()) {
          roleMember.setGrantOption(false);
        } else {
          throw new MetaException("User " + userName
              + " does not have grant option with role " + role.getRoleName());
        }
      } else {
        // No grant option in revoke, remove the whole role.
        pm.deletePersistent(roleMember);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private MRoleMap getMSecurityUserRoleMap(String userName, PrincipalType principalType,
      String roleName) {
    MRoleMap mRoleMember = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query =
          pm.newQuery(MRoleMap.class,
              "principalName == t1 && principalType == t2 && role.roleName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setUnique(true);
      mRoleMember = (MRoleMap) query.executeWithArray(userName, principalType.toString(), roleName);
      pm.retrieve(mRoleMember);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mRoleMember;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException,
      NoSuchObjectException {
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      openTransaction();
      MRole mRol = getMRole(roleName);
      pm.retrieve(mRol);
      if (mRol != null) {
        // first remove all the membership, the membership that this role has
        // been granted
        List<MRoleMap> roleMap = listMRoleMembers(mRol.getRoleName());
        if (roleMap.size() > 0) {
          pm.deletePersistentAll(roleMap);
        }
        List<MRoleMap> roleMember = listMSecurityPrincipalMembershipRole(mRol
            .getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (roleMember.size() > 0) {
          pm.deletePersistentAll(roleMember);
        }
        queryWrapper.close();
        // then remove all the grants
        List<MGlobalPrivilege> userGrants = listPrincipalMGlobalGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (userGrants.size() > 0) {
          pm.deletePersistentAll(userGrants);
        }
        List<MDBPrivilege> dbGrants = listPrincipalAllDBGrant(mRol
            .getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (dbGrants.size() > 0) {
          pm.deletePersistentAll(dbGrants);
        }
        queryWrapper.close();
        List<MTablePrivilege> tabPartGrants = listPrincipalAllTableGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (tabPartGrants.size() > 0) {
          pm.deletePersistentAll(tabPartGrants);
        }
        queryWrapper.close();
        List<MPartitionPrivilege> partGrants = listPrincipalAllPartitionGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (partGrants.size() > 0) {
          pm.deletePersistentAll(partGrants);
        }
        queryWrapper.close();
        List<MTableColumnPrivilege> tblColumnGrants = listPrincipalAllTableColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (tblColumnGrants.size() > 0) {
          pm.deletePersistentAll(tblColumnGrants);
        }
        queryWrapper.close();
        List<MPartitionColumnPrivilege> partColumnGrants = listPrincipalAllPartitionColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (partColumnGrants.size() > 0) {
          pm.deletePersistentAll(partColumnGrants);
        }
        queryWrapper.close();

        // finally remove the role
        pm.deletePersistent(mRol);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }

      queryWrapper.close();
    }
    return success;
  }

  /**
   * Get all the roles in the role hierarchy that this user and groupNames belongs to
   * @param userName
   * @param groupNames
   * @return
   */
  private Set<String> listAllRolesInHierarchy(String userName,
      List<String> groupNames) {
    List<MRoleMap> ret = new ArrayList<MRoleMap>();
    if(userName != null) {
      ret.addAll(listMRoles(userName, PrincipalType.USER));
    }
    if (groupNames != null) {
      for (String groupName: groupNames) {
        ret.addAll(listMRoles(groupName, PrincipalType.GROUP));
      }
    }
    // get names of these roles and its ancestors
    Set<String> roleNames = new HashSet<String>();
    getAllRoleAncestors(roleNames, ret);
    return roleNames;
  }

  /**
   * Add role names of parentRoles and its parents to processedRoles
   *
   * @param processedRoleNames
   * @param parentRoles
   */
  private void getAllRoleAncestors(Set<String> processedRoleNames, List<MRoleMap> parentRoles) {
    for (MRoleMap parentRole : parentRoles) {
      String parentRoleName = parentRole.getRole().getRoleName();
      if (!processedRoleNames.contains(parentRoleName)) {
        // unprocessed role: get its parents, add it to processed, and call this
        // function recursively
        List<MRoleMap> nextParentRoles = listMRoles(parentRoleName, PrincipalType.ROLE);
        processedRoleNames.add(parentRoleName);
        getAllRoleAncestors(processedRoleNames, nextParentRoles);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<MRoleMap> listMRoles(String principalName,
      PrincipalType principalType) {
    boolean success = false;
    Query query = null;
    List<MRoleMap> mRoleMember = new ArrayList<MRoleMap>();

    try {
      LOG.debug("Executing listRoles");

      openTransaction();
      query = pm.newQuery(MRoleMap.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      query.setUnique(false);
      List<MRoleMap> mRoles =
          (List<MRoleMap>) query.executeWithArray(principalName, principalType.toString());
      pm.retrieveAll(mRoles);
      success = commitTransaction();

      mRoleMember.addAll(mRoles);

      LOG.debug("Done retrieving all objects for listRoles");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }

    if (principalType == PrincipalType.USER) {
      // All users belong to public role implicitly, add that role
      MRole publicRole = new MRole(HiveMetaStore.PUBLIC, 0, HiveMetaStore.PUBLIC);
      mRoleMember.add(new MRoleMap(principalName, principalType.toString(), publicRole, 0, null,
          null, false));
    }

    return mRoleMember;
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    List<Role> result = new ArrayList<Role>();
    List<MRoleMap> roleMaps = listMRoles(principalName, principalType);
    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        MRole mrole = roleMap.getRole();
        Role role = new Role(mrole.getRoleName(), mrole.getCreateTime(), mrole.getOwnerName());
        result.add(role);
      }
    }
    return result;
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    List<RolePrincipalGrant> result = new ArrayList<RolePrincipalGrant>();
    List<MRoleMap> roleMaps = listMRoles(principalName, principalType);
    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        RolePrincipalGrant rolePrinGrant = new RolePrincipalGrant(
            roleMap.getRole().getRoleName(),
            roleMap.getPrincipalName(),
            PrincipalType.valueOf(roleMap.getPrincipalType()),
            roleMap.getGrantOption(),
            roleMap.getAddTime(),
            roleMap.getGrantor(),
            // no grantor type for public role, hence the null check
            roleMap.getGrantorType() == null ? null
                : PrincipalType.valueOf(roleMap.getGrantorType())
        );
        result.add(rolePrinGrant);
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MRoleMap> listMSecurityPrincipalMembershipRole(final String roleName,
      final PrincipalType principalType,
      QueryWrapper queryWrapper) {
    boolean success = false;
    List<MRoleMap> mRoleMemebership = null;
    try {
      LOG.debug("Executing listMSecurityPrincipalMembershipRole");

      openTransaction();
      Query query = queryWrapper.query = pm.newQuery(MRoleMap.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mRoleMemebership = (List<MRoleMap>) query.execute(roleName, principalType.toString());
      pm.retrieveAll(mRoleMemebership);
      success = commitTransaction();

      LOG.debug("Done retrieving all objects for listMSecurityPrincipalMembershipRole");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mRoleMemebership;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    MRole mRole = this.getMRole(roleName);
    if (mRole == null) {
      throw new NoSuchObjectException(roleName + " role can not be found.");
    }
    Role ret = new Role(mRole.getRoleName(), mRole.getCreateTime(), mRole
        .getOwnerName());
    return ret;
  }

  private MRole getMRole(String roleName) {
    MRole mrole = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MRole.class, "roleName == t1");
      query.declareParameters("java.lang.String t1");
      query.setUnique(true);
      mrole = (MRole) query.execute(roleName);
      pm.retrieve(mrole);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mrole;
  }

  @Override
  public List<String> listRoleNames() {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listAllRoleNames");
      query = pm.newQuery("select roleName from org.apache.hadoop.hive.metastore.model.MRole");
      query.setResult("roleName");
      Collection names = (Collection) query.execute();
      List<String> roleNames = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        roleNames.add((String) i.next());
      }
      success = commitTransaction();
      return roleNames;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        List<MGlobalPrivilege> user = this.listPrincipalMGlobalGrants(userName, PrincipalType.USER);
        if(user.size()>0) {
          Map<String, List<PrivilegeGrantInfo>> userPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
          List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(user.size());
          for (int i = 0; i < user.size(); i++) {
            MGlobalPrivilege item = user.get(i);
            grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
                .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
                .getGrantorType()), item.getGrantOption()));
          }
          userPriv.put(userName, grantInfos);
          ret.setUserPrivileges(userPriv);
        }
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> groupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for(String groupName: groupNames) {
          List<MGlobalPrivilege> group =
              this.listPrincipalMGlobalGrants(groupName, PrincipalType.GROUP);
          if(group.size()>0) {
            List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(group.size());
            for (int i = 0; i < group.size(); i++) {
              MGlobalPrivilege item = group.get(i);
              grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
                  .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
                  .getGrantorType()), item.getGrantOption()));
            }
            groupPriv.put(groupName, grantInfos);
          }
        }
        ret.setGroupPrivileges(groupPriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  public List<PrivilegeGrantInfo> getDBPrivilege(String dbName,
      String principalName, PrincipalType principalType)
      throws InvalidObjectException, MetaException {
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    if (principalName != null) {
      List<MDBPrivilege> userNameDbPriv = this.listPrincipalMDBGrants(
          principalName, principalType, dbName);
      if (userNameDbPriv != null && userNameDbPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameDbPriv.size());
        for (int i = 0; i < userNameDbPriv.size(); i++) {
          MDBPrivilege item = userNameDbPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }


  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    boolean commited = false;
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> dbUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        dbUserPriv.put(userName, getDBPrivilege(dbName, userName,
            PrincipalType.USER));
        ret.setUserPrivileges(dbUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          dbGroupPriv.put(groupName, getDBPrivilege(dbName, groupName,
              PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(dbGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (roleNames != null && roleNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> dbRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String roleName : roleNames) {
          dbRolePriv
              .put(roleName, getDBPrivilege(dbName, roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(dbRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName,
      String tableName, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> partUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        partUserPriv.put(userName, getPartitionPrivilege(dbName,
            tableName, partition, userName, PrincipalType.USER));
        ret.setUserPrivileges(partUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> partGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          partGroupPriv.put(groupName, getPartitionPrivilege(dbName, tableName,
              partition, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(partGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (roleNames != null && roleNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> partRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String roleName : roleNames) {
          partRolePriv.put(roleName, getPartitionPrivilege(dbName, tableName,
              partition, roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(partRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName,
      String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> tableUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        tableUserPriv.put(userName, getTablePrivilege(dbName,
            tableName, userName, PrincipalType.USER));
        ret.setUserPrivileges(tableUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> tableGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          tableGroupPriv.put(groupName, getTablePrivilege(dbName, tableName,
              groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(tableGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (roleNames != null && roleNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> tableRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String roleName : roleNames) {
          tableRolePriv.put(roleName, getTablePrivilege(dbName, tableName,
              roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(tableRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName,
      String tableName, String partitionName, String columnName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    columnName = HiveStringUtils.normalizeIdentifier(columnName);

    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> columnUserPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        columnUserPriv.put(userName, getColumnPrivilege(dbName, tableName,
            columnName, partitionName, userName, PrincipalType.USER));
        ret.setUserPrivileges(columnUserPriv);
      }
      if (groupNames != null && groupNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> columnGroupPriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String groupName : groupNames) {
          columnGroupPriv.put(groupName, getColumnPrivilege(dbName, tableName,
              columnName, partitionName, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(columnGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (roleNames != null && roleNames.size() > 0) {
        Map<String, List<PrivilegeGrantInfo>> columnRolePriv = new HashMap<String, List<PrivilegeGrantInfo>>();
        for (String roleName : roleNames) {
          columnRolePriv.put(roleName, getColumnPrivilege(dbName, tableName,
              columnName, partitionName, roleName, PrincipalType.ROLE));
        }
        ret.setRolePrivileges(columnRolePriv);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return ret;
  }

  private List<PrivilegeGrantInfo> getPartitionPrivilege(String dbName,
      String tableName, String partName, String principalName,
      PrincipalType principalType) {

    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    if (principalName != null) {
      List<MPartitionPrivilege> userNameTabPartPriv = this
          .listPrincipalMPartitionGrants(principalName, principalType,
              dbName, tableName, partName);
      if (userNameTabPartPriv != null && userNameTabPartPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MPartitionPrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(),
              getPrincipalTypeFromStr(item.getGrantorType()), item.getGrantOption()));

        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  private PrincipalType getPrincipalTypeFromStr(String str) {
    return str == null ? null : PrincipalType.valueOf(str);
  }

  private List<PrivilegeGrantInfo> getTablePrivilege(String dbName,
      String tableName, String principalName, PrincipalType principalType) {
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    if (principalName != null) {
      List<MTablePrivilege> userNameTabPartPriv = this
          .listAllMTableGrants(principalName, principalType,
              dbName, tableName);
      if (userNameTabPartPriv != null && userNameTabPartPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameTabPartPriv.size());
        for (int i = 0; i < userNameTabPartPriv.size(); i++) {
          MTablePrivilege item = userNameTabPartPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  private List<PrivilegeGrantInfo> getColumnPrivilege(String dbName,
      String tableName, String columnName, String partitionName,
      String principalName, PrincipalType principalType) {

    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    columnName = HiveStringUtils.normalizeIdentifier(columnName);

    if (partitionName == null) {
      List<MTableColumnPrivilege> userNameColumnPriv = this
          .listPrincipalMTableColumnGrants(principalName, principalType,
              dbName, tableName, columnName);
      if (userNameColumnPriv != null && userNameColumnPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MTableColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    } else {
      List<MPartitionColumnPrivilege> userNameColumnPriv = this
          .listPrincipalMPartitionColumnGrants(principalName,
              principalType, dbName, tableName, partitionName, columnName);
      if (userNameColumnPriv != null && userNameColumnPriv.size() > 0) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<PrivilegeGrantInfo>(
            userNameColumnPriv.size());
        for (int i = 0; i < userNameColumnPriv.size(); i++) {
          MPartitionColumnPrivilege item = userNameColumnPriv.get(i);
          grantInfos.add(new PrivilegeGrantInfo(item.getPrivilege(), item
              .getCreateTime(), item.getGrantor(), getPrincipalTypeFromStr(item
              .getGrantorType()), item.getGrantOption()));
        }
        return grantInfos;
      }
    }
    return new ArrayList<PrivilegeGrantInfo>(0);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    boolean committed = false;
    int now = (int) (System.currentTimeMillis() / 1000);
    try {
      openTransaction();
      List<Object> persistentObjs = new ArrayList<Object>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();

      if (privilegeList != null && privilegeList.size() > 0) {
        Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();
        Set<String> privSet = new HashSet<String>();
        while (privIter.hasNext()) {
          HiveObjectPrivilege privDef = privIter.next();
          HiveObjectRef hiveObject = privDef.getHiveObject();
          String privilegeStr = privDef.getGrantInfo().getPrivilege();
          String[] privs = privilegeStr.split(",");
          String userName = privDef.getPrincipalName();
          PrincipalType principalType = privDef.getPrincipalType();
          String grantor = privDef.getGrantInfo().getGrantor();
          String grantorType = privDef.getGrantInfo().getGrantorType().toString();
          boolean grantOption = privDef.getGrantInfo().isGrantOption();
          privSet.clear();

          if(principalType == PrincipalType.ROLE){
            validateRole(userName);
          }

          if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
            List<MGlobalPrivilege> globalPrivs = this
                .listPrincipalMGlobalGrants(userName, principalType);
            if (globalPrivs != null) {
              for (MGlobalPrivilege priv : globalPrivs) {
                if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                  privSet.add(priv.getPrivilege());
                }
              }
            }
            for (String privilege : privs) {
              if (privSet.contains(privilege)) {
                throw new InvalidObjectException(privilege
                    + " is already granted by " + grantor);
              }
              MGlobalPrivilege mGlobalPrivs = new MGlobalPrivilege(userName,
                  principalType.toString(), privilege, now, grantor, grantorType, grantOption);
              persistentObjs.add(mGlobalPrivs);
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
            MDatabase dbObj = getMDatabase(hiveObject.getDbName());
            if (dbObj != null) {
              List<MDBPrivilege> dbPrivs = this.listPrincipalMDBGrants(
                  userName, principalType, hiveObject.getDbName());
              if (dbPrivs != null) {
                for (MDBPrivilege priv : dbPrivs) {
                  if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on database "
                      + hiveObject.getDbName() + " by " + grantor);
                }
                MDBPrivilege mDb = new MDBPrivilege(userName, principalType
                    .toString(), dbObj, privilege, now, grantor, grantorType, grantOption);
                persistentObjs.add(mDb);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            MTable tblObj = getMTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              List<MTablePrivilege> tablePrivs = this
                  .listAllMTableGrants(userName, principalType,
                      hiveObject.getDbName(), hiveObject.getObjectName());
              if (tablePrivs != null) {
                for (MTablePrivilege priv : tablePrivs) {
                  if (priv.getGrantor() != null
                      && priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on table ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + "] by " + grantor);
                }
                MTablePrivilege mTab = new MTablePrivilege(
                    userName, principalType.toString(), tblObj,
                    privilege, now, grantor, grantorType, grantOption);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
            MPartition partObj = this.getMPartition(hiveObject.getDbName(),
                hiveObject.getObjectName(), hiveObject.getPartValues());
            String partName = null;
            if (partObj != null) {
              partName = partObj.getPartitionName();
              List<MPartitionPrivilege> partPrivs = this
                  .listPrincipalMPartitionGrants(userName,
                      principalType, hiveObject.getDbName(), hiveObject
                          .getObjectName(), partObj.getPartitionName());
              if (partPrivs != null) {
                for (MPartitionPrivilege priv : partPrivs) {
                  if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                    privSet.add(priv.getPrivilege());
                  }
                }
              }
              for (String privilege : privs) {
                if (privSet.contains(privilege)) {
                  throw new InvalidObjectException(privilege
                      + " is already granted on partition ["
                      + hiveObject.getDbName() + ","
                      + hiveObject.getObjectName() + ","
                      + partName + "] by " + grantor);
                }
                MPartitionPrivilege mTab = new MPartitionPrivilege(userName,
                    principalType.toString(), partObj, privilege, now, grantor,
                    grantorType, grantOption);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
            MTable tblObj = getMTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              if (hiveObject.getPartValues() != null) {
                MPartition partObj = null;
                List<MPartitionColumnPrivilege> colPrivs = null;
                partObj = this.getMPartition(hiveObject.getDbName(), hiveObject
                    .getObjectName(), hiveObject.getPartValues());
                if (partObj == null) {
                  continue;
                }
                colPrivs = this.listPrincipalMPartitionColumnGrants(
                    userName, principalType, hiveObject.getDbName(), hiveObject
                        .getObjectName(), partObj.getPartitionName(),
                    hiveObject.getColumnName());

                if (colPrivs != null) {
                  for (MPartitionColumnPrivilege priv : colPrivs) {
                    if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                      privSet.add(priv.getPrivilege());
                    }
                  }
                }
                for (String privilege : privs) {
                  if (privSet.contains(privilege)) {
                    throw new InvalidObjectException(privilege
                        + " is already granted on column "
                        + hiveObject.getColumnName() + " ["
                        + hiveObject.getDbName() + ","
                        + hiveObject.getObjectName() + ","
                        + partObj.getPartitionName() + "] by " + grantor);
                  }
                  MPartitionColumnPrivilege mCol = new MPartitionColumnPrivilege(userName,
                      principalType.toString(), partObj, hiveObject
                          .getColumnName(), privilege, now, grantor, grantorType,
                      grantOption);
                  persistentObjs.add(mCol);
                }

              } else {
                List<MTableColumnPrivilege> colPrivs = null;
                colPrivs = this.listPrincipalMTableColumnGrants(
                    userName, principalType, hiveObject.getDbName(), hiveObject
                        .getObjectName(), hiveObject.getColumnName());

                if (colPrivs != null) {
                  for (MTableColumnPrivilege priv : colPrivs) {
                    if (priv.getGrantor().equalsIgnoreCase(grantor)) {
                      privSet.add(priv.getPrivilege());
                    }
                  }
                }
                for (String privilege : privs) {
                  if (privSet.contains(privilege)) {
                    throw new InvalidObjectException(privilege
                        + " is already granted on column "
                        + hiveObject.getColumnName() + " ["
                        + hiveObject.getDbName() + ","
                        + hiveObject.getObjectName() + "] by " + grantor);
                  }
                  MTableColumnPrivilege mCol = new MTableColumnPrivilege(userName,
                      principalType.toString(), tblObj, hiveObject
                          .getColumnName(), privilege, now, grantor, grantorType,
                      grantOption);
                  persistentObjs.add(mCol);
                }
              }
            }
          }
        }
      }
      if (persistentObjs.size() > 0) {
        pm.makePersistentAll(persistentObjs);
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    return committed;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    boolean committed = false;
    try {
      openTransaction();
      List<Object> persistentObjs = new ArrayList<Object>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();


      if (privilegeList != null && privilegeList.size() > 0) {
        Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();

        while (privIter.hasNext()) {
          HiveObjectPrivilege privDef = privIter.next();
          HiveObjectRef hiveObject = privDef.getHiveObject();
          String privilegeStr = privDef.getGrantInfo().getPrivilege();
          if (privilegeStr == null || privilegeStr.trim().equals("")) {
            continue;
          }
          String[] privs = privilegeStr.split(",");
          String userName = privDef.getPrincipalName();
          PrincipalType principalType = privDef.getPrincipalType();

          if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
            List<MGlobalPrivilege> mSecUser = this.listPrincipalMGlobalGrants(
                userName, principalType);
            boolean found = false;
            if (mSecUser != null) {
              for (String privilege : privs) {
                for (MGlobalPrivilege userGrant : mSecUser) {
                  String userGrantPrivs = userGrant.getPrivilege();
                  if (privilege.equals(userGrantPrivs)) {
                    found = true;
                    if (grantOption) {
                      if (userGrant.getGrantOption()) {
                        userGrant.setGrantOption(false);
                      } else {
                        throw new MetaException("User " + userName
                            + " does not have grant option with privilege " + privilege);
                      }
                    }
                    persistentObjs.add(userGrant);
                    break;
                  }
                }
                if (!found) {
                  throw new InvalidObjectException(
                      "No user grant found for privileges " + privilege);
                }
              }
            }

          } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
            MDatabase dbObj = getMDatabase(hiveObject.getDbName());
            if (dbObj != null) {
              String db = hiveObject.getDbName();
              boolean found = false;
              List<MDBPrivilege> dbGrants = this.listPrincipalMDBGrants(
                  userName, principalType, db);
              for (String privilege : privs) {
                for (MDBPrivilege dbGrant : dbGrants) {
                  String dbGrantPriv = dbGrant.getPrivilege();
                  if (privilege.equals(dbGrantPriv)) {
                    found = true;
                    if (grantOption) {
                      if (dbGrant.getGrantOption()) {
                        dbGrant.setGrantOption(false);
                      } else {
                        throw new MetaException("User " + userName
                            + " does not have grant option with privilege " + privilege);
                      }
                    }
                    persistentObjs.add(dbGrant);
                    break;
                  }
                }
                if (!found) {
                  throw new InvalidObjectException(
                      "No database grant found for privileges " + privilege
                          + " on database " + db);
                }
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            boolean found = false;
            List<MTablePrivilege> tableGrants = this
                .listAllMTableGrants(userName, principalType,
                    hiveObject.getDbName(), hiveObject.getObjectName());
            for (String privilege : privs) {
              for (MTablePrivilege tabGrant : tableGrants) {
                String tableGrantPriv = tabGrant.getPrivilege();
                if (privilege.equalsIgnoreCase(tableGrantPriv)) {
                  found = true;
                  if (grantOption) {
                    if (tabGrant.getGrantOption()) {
                      tabGrant.setGrantOption(false);
                    } else {
                      throw new MetaException("User " + userName
                          + " does not have grant option with privilege " + privilege);
                    }
                  }
                  persistentObjs.add(tabGrant);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + hiveObject.getObjectName()
                    + ", database is " + hiveObject.getDbName());
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {

            boolean found = false;
            Table tabObj = this.getTable(hiveObject.getDbName(), hiveObject.getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(), hiveObject.getPartValues());
            }
            List<MPartitionPrivilege> partitionGrants = this
                .listPrincipalMPartitionGrants(userName, principalType,
                    hiveObject.getDbName(), hiveObject.getObjectName(), partName);
            for (String privilege : privs) {
              for (MPartitionPrivilege partGrant : partitionGrants) {
                String partPriv = partGrant.getPrivilege();
                if (partPriv.equalsIgnoreCase(privilege)) {
                  found = true;
                  if (grantOption) {
                    if (partGrant.getGrantOption()) {
                      partGrant.setGrantOption(false);
                    } else {
                      throw new MetaException("User " + userName
                          + " does not have grant option with privilege " + privilege);
                    }
                  }
                  persistentObjs.add(partGrant);
                  break;
                }
              }
              if (!found) {
                throw new InvalidObjectException("No grant (" + privilege
                    + ") found " + " on table " + tabObj.getTableName()
                    + ", partition is " + partName + ", database is " + tabObj.getDbName());
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {

            Table tabObj = this.getTable(hiveObject.getDbName(), hiveObject
                .getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(),
                  hiveObject.getPartValues());
            }

            if (partName != null) {
              List<MPartitionColumnPrivilege> mSecCol = listPrincipalMPartitionColumnGrants(
                  userName, principalType, hiveObject.getDbName(), hiveObject
                      .getObjectName(), partName, hiveObject.getColumnName());
              boolean found = false;
              if (mSecCol != null) {
                for (String privilege : privs) {
                  for (MPartitionColumnPrivilege col : mSecCol) {
                    String colPriv = col.getPrivilege();
                    if (colPriv.equalsIgnoreCase(privilege)) {
                      found = true;
                      if (grantOption) {
                        if (col.getGrantOption()) {
                          col.setGrantOption(false);
                        } else {
                          throw new MetaException("User " + userName
                              + " does not have grant option with privilege " + privilege);
                        }
                      }
                      persistentObjs.add(col);
                      break;
                    }
                  }
                  if (!found) {
                    throw new InvalidObjectException("No grant (" + privilege
                        + ") found " + " on table " + tabObj.getTableName()
                        + ", partition is " + partName + ", column name = "
                        + hiveObject.getColumnName() + ", database is "
                        + tabObj.getDbName());
                  }
                }
              }
            } else {
              List<MTableColumnPrivilege> mSecCol = listPrincipalMTableColumnGrants(
                  userName, principalType, hiveObject.getDbName(), hiveObject
                      .getObjectName(), hiveObject.getColumnName());
              boolean found = false;
              if (mSecCol != null) {
                for (String privilege : privs) {
                  for (MTableColumnPrivilege col : mSecCol) {
                    String colPriv = col.getPrivilege();
                    if (colPriv.equalsIgnoreCase(privilege)) {
                      found = true;
                      if (grantOption) {
                        if (col.getGrantOption()) {
                          col.setGrantOption(false);
                        } else {
                          throw new MetaException("User " + userName
                              + " does not have grant option with privilege " + privilege);
                        }
                      }
                      persistentObjs.add(col);
                      break;
                    }
                  }
                  if (!found) {
                    throw new InvalidObjectException("No grant (" + privilege
                        + ") found " + " on table " + tabObj.getTableName()
                        + ", column name = "
                        + hiveObject.getColumnName() + ", database is "
                        + tabObj.getDbName());
                  }
                }
              }
            }

          }
        }
      }

      if (persistentObjs.size() > 0) {
        if (grantOption) {
          // If grant option specified, only update the privilege, don't remove it.
          // Grant option has already been removed from the privileges in the section above
        } else {
          pm.deletePersistentAll(persistentObjs);
        }
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
    return committed;
  }

  @SuppressWarnings("unchecked")
  public List<MRoleMap> listMRoleMembers(String roleName) {
    boolean success = false;
    Query query = null;
    List<MRoleMap> mRoleMemeberList = new ArrayList<MRoleMap>();
    try {
      LOG.debug("Executing listRoleMembers");

      openTransaction();
      query = pm.newQuery(MRoleMap.class, "role.roleName == t1");
      query.declareParameters("java.lang.String t1");
      query.setUnique(false);
      List<MRoleMap> mRoles = (List<MRoleMap>) query.execute(roleName);
      pm.retrieveAll(mRoles);
      success = commitTransaction();

      mRoleMemeberList.addAll(mRoles);

      LOG.debug("Done retrieving all objects for listRoleMembers");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mRoleMemeberList;
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    List<MRoleMap> roleMaps = listMRoleMembers(roleName);
    List<RolePrincipalGrant> rolePrinGrantList = new ArrayList<RolePrincipalGrant>();

    if (roleMaps != null) {
      for (MRoleMap roleMap : roleMaps) {
        RolePrincipalGrant rolePrinGrant = new RolePrincipalGrant(
            roleMap.getRole().getRoleName(),
            roleMap.getPrincipalName(),
            PrincipalType.valueOf(roleMap.getPrincipalType()),
            roleMap.getGrantOption(),
            roleMap.getAddTime(),
            roleMap.getGrantor(),
            // no grantor type for public role, hence the null check
            roleMap.getGrantorType() == null ? null
                : PrincipalType.valueOf(roleMap.getGrantorType())
        );
        rolePrinGrantList.add(rolePrinGrant);

      }
    }
    return rolePrinGrantList;
  }

  @SuppressWarnings("unchecked")
  public List<MGlobalPrivilege> listPrincipalMGlobalGrants(String principalName,
                                                           PrincipalType principalType) {
    boolean commited = false;
    Query query = null;
    List<MGlobalPrivilege> userNameDbPriv = new ArrayList<MGlobalPrivilege>();
    try {
      List<MGlobalPrivilege> mPrivs = null;
      openTransaction();
      if (principalName != null) {
        query = pm.newQuery(MGlobalPrivilege.class, "principalName == t1 && principalType == t2 ");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mPrivs = (List<MGlobalPrivilege>) query
                .executeWithArray(principalName, principalType.toString());
        pm.retrieveAll(mPrivs);
      }
      commited = commitTransaction();
      if (mPrivs != null) {
        userNameDbPriv.addAll(mPrivs);
      }
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return userNameDbPriv;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
                                                             PrincipalType principalType) {
    List<MGlobalPrivilege> mUsers =
        listPrincipalMGlobalGrants(principalName, principalType);
    if (mUsers.isEmpty()) {
      return Collections.<HiveObjectPrivilege> emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mUsers.size(); i++) {
      MGlobalPrivilege sUsr = mUsers.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.GLOBAL, null, null, null, null);
      HiveObjectPrivilege secUser = new HiveObjectPrivilege(
          objectRef, sUsr.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sUsr.getPrivilege(), sUsr
              .getCreateTime(), sUsr.getGrantor(), PrincipalType
              .valueOf(sUsr.getGrantorType()), sUsr.getGrantOption()));
      result.add(secUser);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MGlobalPrivilege.class);
      List<MGlobalPrivilege> userNameDbPriv = (List<MGlobalPrivilege>) query.execute();
      pm.retrieveAll(userNameDbPriv);
      commited = commitTransaction();
      return convertGlobal(userNameDbPriv);
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private List<HiveObjectPrivilege> convertGlobal(List<MGlobalPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MGlobalPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.GLOBAL, null, null, null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public List<MDBPrivilege> listPrincipalMDBGrants(String principalName,
      PrincipalType principalType, String dbName) {
    boolean success = false;
    Query query = null;
    List<MDBPrivilege> mSecurityDBList = new ArrayList<MDBPrivilege>();
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    try {
      LOG.debug("Executing listPrincipalDBGrants");

      openTransaction();
      query =
          pm.newQuery(MDBPrivilege.class,
              "principalName == t1 && principalType == t2 && database.name == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      List<MDBPrivilege> mPrivs =
          (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(),
              dbName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityDBList.addAll(mPrivs);
      LOG.debug("Done retrieving all objects for listPrincipalDBGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityDBList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String dbName) {
    List<MDBPrivilege> mDbs = listPrincipalMDBGrants(principalName, principalType, dbName);
    if (mDbs.isEmpty()) {
      return Collections.<HiveObjectPrivilege>emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mDbs.size(); i++) {
      MDBPrivilege sDB = mDbs.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.DATABASE, dbName, null, null, null);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sDB.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sDB.getPrivilege(), sDB
              .getCreateTime(), sDB.getGrantor(), PrincipalType
              .valueOf(sDB.getGrantorType()), sDB.getGrantOption()));
      result.add(secObj);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType) {
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      return convertDB(listPrincipalAllDBGrant(principalName, principalType, queryWrapper));
    } finally {
      queryWrapper.close();
    }
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      return convertDB(listDatabaseGrants(dbName, queryWrapper));
      } finally {
        queryWrapper.close();
      }
  }

  private List<HiveObjectPrivilege> convertDB(List<MDBPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MDBPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());
      String database = priv.getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.DATABASE, database,
          null, null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listPrincipalAllDBGrant(String principalName,
      PrincipalType principalType,
      QueryWrapper queryWrapper) {
    boolean success = false;
    Query query = null;
    List<MDBPrivilege> mSecurityDBList = null;
    try {
      LOG.debug("Executing listPrincipalAllDBGrant");

      openTransaction();
      if (principalName != null && principalType != null) {
        query = queryWrapper.query = pm.newQuery(MDBPrivilege.class, "principalName == t1 && principalType == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityDBList =
            (List<MDBPrivilege>) query.execute(principalName, principalType.toString());
      } else {
        query = queryWrapper.query = pm.newQuery(MDBPrivilege.class);
        mSecurityDBList = (List<MDBPrivilege>) query.execute();
      }
      pm.retrieveAll(mSecurityDBList);
      success = commitTransaction();

      LOG.debug("Done retrieving all objects for listPrincipalAllDBGrant");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityDBList;
  }

  @SuppressWarnings("unchecked")
  public List<MTablePrivilege> listAllTableGrants(String dbName, String tableName) {
    boolean success = false;
    Query query = null;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    List<MTablePrivilege> mSecurityTabList = new ArrayList<MTablePrivilege>();
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    try {
      LOG.debug("Executing listAllTableGrants");

      openTransaction();
      String queryStr = "table.tableName == t1 && table.database.name == t2";
      query = pm.newQuery(MTablePrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MTablePrivilege> mPrivs  = (List<MTablePrivilege>) query.executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listAllTableGrants");
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityTabList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionPrivilege> listTableAllPartitionGrants(String dbName, String tableName) {
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    boolean success = false;
    Query query = null;
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<MPartitionPrivilege>();
    try {
      LOG.debug("Executing listTableAllPartitionGrants");

      openTransaction();
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2";
      query = pm.newQuery(MPartitionPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MPartitionPrivilege> mPrivs = (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityTabPartList;
  }

  @SuppressWarnings("unchecked")
  public List<MTableColumnPrivilege> listTableAllColumnGrants(String dbName, String tableName) {
    boolean success = false;
    Query query = null;
    List<MTableColumnPrivilege> mTblColPrivilegeList = new ArrayList<MTableColumnPrivilege>();
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    try {
      LOG.debug("Executing listTableAllColumnGrants");

      openTransaction();
      String queryStr = "table.tableName == t1 && table.database.name == t2";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MTableColumnPrivilege> mPrivs =
          (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mTblColPrivilegeList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mTblColPrivilegeList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listTableAllPartitionColumnGrants(String dbName,
      String tableName) {
    boolean success = false;
    Query query = null;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<MPartitionColumnPrivilege>();
    try {
      LOG.debug("Executing listTableAllPartitionColumnGrants");

      openTransaction();
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2";
      query = pm.newQuery(MPartitionColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MPartitionColumnPrivilege> mPrivs =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllPartitionColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listPartitionAllColumnGrants(String dbName,
      String tableName, List<String> partNames) {
    boolean success = false;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    List<MPartitionColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionAllColumnGrants");
      mSecurityColList = queryByPartitionNames(
          dbName, tableName, partNames, MPartitionColumnPrivilege.class,
          "partition.table.tableName", "partition.table.database.name", "partition.partitionName");
      LOG.debug("Done executing query for listPartitionAllColumnGrants");
      pm.retrieveAll(mSecurityColList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionAllColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColList;
  }

  public void dropPartitionAllColumnGrantsNoTxn(
      String dbName, String tableName, List<String> partNames) {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(
          dbName, tableName, partNames, MPartitionColumnPrivilege.class,
          "partition.table.tableName", "partition.table.database.name", "partition.partitionName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listDatabaseGrants(String dbName, QueryWrapper queryWrapper) {
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    boolean success = false;

    try {
      LOG.debug("Executing listDatabaseGrants");

      openTransaction();
      Query query = queryWrapper.query = pm.newQuery(MDBPrivilege.class, "database.name == t1");
      query.declareParameters("java.lang.String t1");
      List<MDBPrivilege> mSecurityDBList = (List<MDBPrivilege>) query.executeWithArray(dbName);
      pm.retrieveAll(mSecurityDBList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listDatabaseGrants");
      return mSecurityDBList;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listPartitionGrants(String dbName, String tableName,
      List<String> partNames) {
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);

    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionGrants");
      mSecurityTabPartList = queryByPartitionNames(
          dbName, tableName, partNames, MPartitionPrivilege.class, "partition.table.tableName",
          "partition.table.database.name", "partition.partitionName");
      LOG.debug("Done executing query for listPartitionGrants");
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  private void dropPartitionGrantsNoTxn(String dbName, String tableName, List<String> partNames) {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(
          dbName, tableName, partNames,MPartitionPrivilege.class, "partition.table.tableName",
          "partition.table.database.name", "partition.partitionName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> queryByPartitionNames(String dbName, String tableName,
      List<String> partNames, Class<T> clazz, String tbCol, String dbCol, String partCol) {
    ObjectPair<Query, Object[]> queryAndParams = makeQueryByPartitionNames(
        dbName, tableName, partNames, clazz, tbCol, dbCol, partCol);
    return (List<T>)queryAndParams.getFirst().executeWithArray(queryAndParams.getSecond());
  }

  private ObjectPair<Query, Object[]> makeQueryByPartitionNames(
      String dbName, String tableName, List<String> partNames, Class<?> clazz,
      String tbCol, String dbCol, String partCol) {
    String queryStr = tbCol + " == t1 && " + dbCol + " == t2";
    String paramStr = "java.lang.String t1, java.lang.String t2";
    Object[] params = new Object[2 + partNames.size()];
    params[0] = HiveStringUtils.normalizeIdentifier(tableName);
    params[1] = HiveStringUtils.normalizeIdentifier(dbName);
    int index = 0;
    for (String partName : partNames) {
      params[index + 2] = partName;
      queryStr += ((index == 0) ? " && (" : " || ") + partCol + " == p" + index;
      paramStr += ", java.lang.String p" + index;
      ++index;
    }
    queryStr += ")";
    Query query = pm.newQuery(clazz, queryStr);
    query.declareParameters(paramStr);
    return new ObjectPair<Query, Object[]>(query, params);
  }

  @SuppressWarnings("unchecked")
  public List<MTablePrivilege> listAllMTableGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName) {
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    boolean success = false;
    Query query = null;
    List<MTablePrivilege> mSecurityTabPartList = new ArrayList<MTablePrivilege>();
    try {
      openTransaction();
      LOG.debug("Executing listAllTableGrants");
      query =
          pm.newQuery(MTablePrivilege.class,
              "principalName == t1 && principalType == t2 && table.tableName == t3 && table.database.name == t4");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
      List<MTablePrivilege> mPrivs =
          (List<MTablePrivilege>) query.executeWithArray(principalName, principalType.toString(),
              tableName, dbName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType,
                                                      String dbName,
                                                      String tableName) {
    List<MTablePrivilege> mTbls =
        listAllMTableGrants(principalName, principalType, dbName, tableName);
    if (mTbls.isEmpty()) {
      return Collections.<HiveObjectPrivilege> emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mTbls.size(); i++) {
      MTablePrivilege sTbl = mTbls.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.TABLE, dbName, tableName, null, null);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sTbl.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sTbl.getPrivilege(), sTbl.getCreateTime(), sTbl
              .getGrantor(), PrincipalType.valueOf(sTbl
              .getGrantorType()), sTbl.getGrantOption()));
      result.add(secObj);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionPrivilege> listPrincipalMPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName) {
    boolean success = false;
    Query query = null;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<MPartitionPrivilege>();
    try {
      LOG.debug("Executing listPrincipalPartitionGrants");

      openTransaction();
      query =
          pm.newQuery(MPartitionPrivilege.class,
              "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                  + "&& partition.table.database.name == t4 && partition.partitionName == t5");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
              + "java.lang.String t5");
      List<MPartitionPrivilege> mPrivs =
          (List<MPartitionPrivilege>) query.executeWithArray(principalName,
              principalType.toString(), tableName, dbName, partName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String dbName,
                                                                String tableName,
                                                                List<String> partValues,
                                                                String partName) {
    List<MPartitionPrivilege> mParts = listPrincipalMPartitionGrants(principalName,
        principalType, dbName, tableName, partName);
    if (mParts.isEmpty()) {
      return Collections.<HiveObjectPrivilege> emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mParts.size(); i++) {
      MPartitionPrivilege sPart = mParts.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.PARTITION, dbName, tableName, partValues, null);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sPart.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sPart.getPrivilege(), sPart
              .getCreateTime(), sPart.getGrantor(), PrincipalType
              .valueOf(sPart.getGrantorType()), sPart
              .getGrantOption()));

      result.add(secObj);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public List<MTableColumnPrivilege> listPrincipalMTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName) {
    boolean success = false;
    Query query = null;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    columnName = HiveStringUtils.normalizeIdentifier(columnName);
    List<MTableColumnPrivilege> mSecurityColList = new ArrayList<MTableColumnPrivilege>();
    try {
      LOG.debug("Executing listPrincipalTableColumnGrants");

      openTransaction();
      String queryStr =
          "principalName == t1 && principalType == t2 && "
              + "table.tableName == t3 && table.database.name == t4 &&  columnName == t5 ";
      query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5");
      List<MTableColumnPrivilege> mPrivs =
          (List<MTableColumnPrivilege>) query.executeWithArray(principalName,
              principalType.toString(), tableName, dbName, columnName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                  PrincipalType principalType,
                                                                  String dbName,
                                                                  String tableName,
                                                                  String columnName) {
    List<MTableColumnPrivilege> mTableCols =
        listPrincipalMTableColumnGrants(principalName, principalType, dbName, tableName, columnName);
    if (mTableCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mTableCols.size(); i++) {
      MTableColumnPrivilege sCol = mTableCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, null, sCol.getColumnName());
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(
          objectRef, sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol
              .getGrantOption()));
      result.add(secObj);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public List<MPartitionColumnPrivilege> listPrincipalMPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partitionName, String columnName) {
    boolean success = false;
    Query query = null;
    tableName = HiveStringUtils.normalizeIdentifier(tableName);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    columnName = HiveStringUtils.normalizeIdentifier(columnName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<MPartitionColumnPrivilege>();
    try {
      LOG.debug("Executing listPrincipalPartitionColumnGrants");

      openTransaction();
      query = pm.newQuery(
              MPartitionColumnPrivilege.class,
              "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                  + "&& partition.table.database.name == t4 && partition.partitionName == t5 && columnName == t6");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4, java.lang.String t5, java.lang.String t6");
      List<MPartitionColumnPrivilege> mPrivs =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
              principalType.toString(), tableName, dbName, partitionName, columnName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalPartitionColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                      PrincipalType principalType,
                                                                      String dbName,
                                                                      String tableName,
                                                                      List<String> partValues,
                                                                      String partitionName,
                                                                      String columnName) {
    List<MPartitionColumnPrivilege> mPartitionCols =
        listPrincipalMPartitionColumnGrants(principalName, principalType, dbName, tableName,
            partitionName, columnName);
    if (mPartitionCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (int i = 0; i < mPartitionCols.size(); i++) {
      MPartitionColumnPrivilege sCol = mPartitionCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, partValues, sCol.getColumnName());
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol.getGrantOption()));
      result.add(secObj);
    }
    return result;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalPartitionColumnGrantsAll");
      List<MPartitionColumnPrivilege> mSecurityTabPartList;
      if (principalName != null && principalType != null) {
        query =
            pm.newQuery(MPartitionColumnPrivilege.class,
                "principalName == t1 && principalType == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityTabPartList =
            (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
                principalType.toString());
      } else {
        query = pm.newQuery(MPartitionColumnPrivilege.class);
        mSecurityTabPartList = (List<MPartitionColumnPrivilege>) query.execute();
      }
      LOG.debug("Done executing query for listPrincipalPartitionColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalPartitionColumnGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
      String partitionName, String columnName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionColumnGrantsAll");
      query =
          pm.newQuery(MPartitionColumnPrivilege.class,
              "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                  + "partition.partitionName == t5 && columnName == t6");
      query
          .declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5, java.lang.String t6");
      List<MPartitionColumnPrivilege> mSecurityTabPartList =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName,
              partitionName, columnName);
      LOG.debug("Done executing query for listPartitionColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionColumnGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private List<HiveObjectPrivilege> convertPartCols(List<MPartitionColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MPartitionColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), priv.getColumnName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MTablePrivilege> listPrincipalAllTableGrants(
      String principalName, PrincipalType principalType, QueryWrapper queryWrapper) {
    boolean success = false;
    List<MTablePrivilege> mSecurityTabPartList = null;
    try {
      LOG.debug("Executing listPrincipalAllTableGrants");

      openTransaction();
      Query query = queryWrapper.query = pm.newQuery(MTablePrivilege.class,
          "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList = (List<MTablePrivilege>) query.execute(
          principalName, principalType.toString());
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();

      LOG.debug("Done retrieving all objects for listPrincipalAllTableGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
      PrincipalType principalType) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllTableGrants");
      List<MTablePrivilege> mSecurityTabPartList;
      if (principalName != null && principalType != null) {
        query = pm.newQuery(MTablePrivilege.class, "principalName == t1 && principalType == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityTabPartList =
            (List<MTablePrivilege>) query.execute(principalName, principalType.toString());
      } else {
        query = pm.newQuery(MTablePrivilege.class);
        mSecurityTabPartList = (List<MTablePrivilege>) query.execute();
      }
      LOG.debug("Done executing query for listPrincipalAllTableGrants");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTable(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllTableGrants");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listTableGrantsAll");
      query =
          pm.newQuery(MTablePrivilege.class, "table.tableName == t1 && table.database.name == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      List<MTablePrivilege> mSecurityTabPartList =
          (List<MTablePrivilege>) query.executeWithArray(tableName, dbName);
      LOG.debug("Done executing query for listTableGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTable(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllTableGrants");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private List<HiveObjectPrivilege> convertTable(List<MTablePrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MTablePrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      String table = priv.getTable().getTableName();
      String database = priv.getTable().getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.TABLE, database, table,
          null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listPrincipalAllPartitionGrants(String principalName,
      PrincipalType principalType, QueryWrapper queryWrapper) {
    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalAllPartitionGrants");
      Query query = queryWrapper.query = pm.newQuery(MPartitionPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityTabPartList =
          (List<MPartitionPrivilege>) query.execute(principalName, principalType.toString());
      pm.retrieveAll(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllPartitionGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
      PrincipalType principalType) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalPartitionGrantsAll");
      List<MPartitionPrivilege> mSecurityTabPartList;
      if (principalName != null && principalType != null) {
        query =
            pm.newQuery(MPartitionPrivilege.class, "principalName == t1 && principalType == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityTabPartList =
            (List<MPartitionPrivilege>) query.execute(principalName, principalType.toString());
      } else {
        query = pm.newQuery(MPartitionPrivilege.class);
        mSecurityTabPartList = (List<MPartitionPrivilege>) query.execute();
      }
      LOG.debug("Done executing query for listPrincipalPartitionGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartition(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalPartitionGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
      String partitionName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalPartitionGrantsAll");
      query =
          pm.newQuery(MPartitionPrivilege.class,
              "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                  + "partition.partitionName == t5");
      query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5");
      List<MPartitionPrivilege> mSecurityTabPartList =
          (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName, partitionName);
      LOG.debug("Done executing query for listPrincipalPartitionGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartition(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalPartitionGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private List<HiveObjectPrivilege> convertPartition(List<MPartitionPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MPartitionPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.PARTITION,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MTableColumnPrivilege> listPrincipalAllTableColumnGrants(String principalName,
      PrincipalType principalType, QueryWrapper queryWrapper) {
    boolean success = false;

    List<MTableColumnPrivilege> mSecurityColumnList = null;
    try {
      LOG.debug("Executing listPrincipalAllTableColumnGrants");

      openTransaction();
      Query query = queryWrapper.query =
          pm.newQuery(MTableColumnPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityColumnList =
          (List<MTableColumnPrivilege>) query.execute(principalName, principalType.toString());
      pm.retrieveAll(mSecurityColumnList);
      success = commitTransaction();

      LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColumnList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
      PrincipalType principalType) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalTableColumnGrantsAll");

      List<MTableColumnPrivilege> mSecurityTabPartList;
      if (principalName != null && principalType != null) {
        query =
            pm.newQuery(MTableColumnPrivilege.class, "principalName == t1 && principalType == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityTabPartList =
            (List<MTableColumnPrivilege>) query.execute(principalName, principalType.toString());
      } else {
        query = pm.newQuery(MTableColumnPrivilege.class);
        mSecurityTabPartList = (List<MTableColumnPrivilege>) query.execute();
      }
      LOG.debug("Done executing query for listPrincipalTableColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTableCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalTableColumnGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
      String columnName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalTableColumnGrantsAll");
      query =
          pm.newQuery(MTableColumnPrivilege.class,
              "table.tableName == t3 && table.database.name == t4 &&  columnName == t5");
      query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5");
      List<MTableColumnPrivilege> mSecurityTabPartList =
          (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, columnName);
      LOG.debug("Done executing query for listPrincipalTableColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTableCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalTableColumnGrantsAll");
      return result;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private List<HiveObjectPrivilege> convertTableCols(List<MTableColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<HiveObjectPrivilege>();
    for (MTableColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MTable mtable = priv.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), null, priv.getColumnName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionColumnPrivilege> listPrincipalAllPartitionColumnGrants(
      String principalName, PrincipalType principalType, QueryWrapper queryWrapper) {
    boolean success = false;
    List<MPartitionColumnPrivilege> mSecurityColumnList = null;
    try {
      LOG.debug("Executing listPrincipalAllTableColumnGrants");

      openTransaction();
      Query query = queryWrapper.query =
          pm.newQuery(MPartitionColumnPrivilege.class, "principalName == t1 && principalType == t2");
      query.declareParameters("java.lang.String t1, java.lang.String t2");
      mSecurityColumnList =
          (List<MPartitionColumnPrivilege>) query.execute(principalName, principalType.toString());
      pm.retrieveAll(mSecurityColumnList);
      success = commitTransaction();

      LOG.debug("Done retrieving all objects for listPrincipalAllTableColumnGrants");
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mSecurityColumnList;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType) throws UnknownTableException,
      MetaException, InvalidPartitionException, UnknownPartitionException {
    boolean success = false;
    Query query = null;

    try {
      LOG.debug("Begin Executing isPartitionMarkedForEvent");

      openTransaction();
      query = pm.newQuery(MPartitionEvent.class,
              "dbName == t1 && tblName == t2 && partName == t3 && eventType == t4");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, int t4");
      Table tbl = getTable(dbName, tblName); // Make sure dbName and tblName are valid.
      if (null == tbl) {
        throw new UnknownTableException("Table: " + tblName + " is not found.");
      }
      Collection<MPartitionEvent> partEvents =
          (Collection<MPartitionEvent>) query.executeWithArray(dbName, tblName,
              getPartitionStr(tbl, partName), evtType.getValue());
      pm.retrieveAll(partEvents);
      success = commitTransaction();

      LOG.debug("Done executing isPartitionMarkedForEvent");
      return (partEvents != null && !partEvents.isEmpty()) ? true : false;
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String,String> partName,
      PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {

    LOG.debug("Begin executing markPartitionForEvent");
    boolean success = false;
    Table tbl = null;
    try{
    openTransaction();
    tbl = getTable(dbName, tblName); // Make sure dbName and tblName are valid.
    if(null == tbl) {
      throw new UnknownTableException("Table: "+ tblName + " is not found.");
    }
    pm.makePersistent(new MPartitionEvent(dbName,tblName,getPartitionStr(tbl, partName), evtType.getValue()));
    success = commitTransaction();
    LOG.debug("Done executing markPartitionForEvent");
    } finally {
      if(!success) {
        rollbackTransaction();
      }
    }
    return tbl;
  }

  private String getPartitionStr(Table tbl, Map<String,String> partName) throws InvalidPartitionException{
    if(tbl.getPartitionKeysSize() != partName.size()){
      throw new InvalidPartitionException("Number of partition columns in table: "+ tbl.getPartitionKeysSize() +
          " doesn't match with number of supplied partition values: "+partName.size());
    }
    final List<String> storedVals = new ArrayList<String>(tbl.getPartitionKeysSize());
    for(FieldSchema partKey : tbl.getPartitionKeys()){
      String partVal = partName.get(partKey.getName());
      if(null == partVal) {
        throw new InvalidPartitionException("No value found for partition column: "+partKey.getName());
      }
      storedVals.add(partVal);
    }
    return join(storedVals,',');
  }

  /** The following API
   *
   *  - executeJDOQLSelect
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public Collection<?> executeJDOQLSelect(String queryStr, QueryWrapper queryWrapper) {
    boolean committed = false;
    Collection<?> result = null;

    try {
      openTransaction();
      Query query = queryWrapper.query = pm.newQuery(queryStr);
      result = ((Collection<?>) query.execute());
      committed = commitTransaction();

      if (committed) {
        return result;
      } else {
        return null;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  /** The following API
  *
  *  - executeJDOQLUpdate
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public long executeJDOQLUpdate(String queryStr) {
    boolean committed = false;
    long numUpdated = 0;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(queryStr);
      numUpdated = (Long) query.execute();
      committed = commitTransaction();
      if (committed) {
        return numUpdated;
      } else {
        return -1;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  /** The following API
  *
  *  - listFSRoots
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public Set<String> listFSRoots() {
    boolean committed = false;
    Query query = null;
    Set<String> fsRoots = new HashSet<String>();
    try {
      openTransaction();
      query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);
      for (MDatabase mDB : mDBs) {
        fsRoots.add(mDB.getLocationUri());
      }
      committed = commitTransaction();
      if (committed) {
        return fsRoots;
      } else {
        return null;
      }
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private boolean shouldUpdateURI(URI onDiskUri, URI inputUri) {
    String onDiskHost = onDiskUri.getHost();
    String inputHost = inputUri.getHost();

    int onDiskPort = onDiskUri.getPort();
    int inputPort = inputUri.getPort();

    String onDiskScheme = onDiskUri.getScheme();
    String inputScheme = inputUri.getScheme();

    //compare ports
    if (inputPort != -1) {
      if (inputPort != onDiskPort) {
        return false;
      }
    }
    //compare schemes
    if (inputScheme != null) {
      if (onDiskScheme == null) {
        return false;
      }
      if (!inputScheme.equalsIgnoreCase(onDiskScheme)) {
        return false;
      }
    }
    //compare hosts
    if (onDiskHost != null) {
      if (!inputHost.equalsIgnoreCase(onDiskHost)) {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  public class UpdateMDatabaseURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateMDatabaseURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateMDatabaseURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateMDatabaseURIRetVal updateMDatabaseURI(URI oldLoc, URI newLoc, boolean dryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateMDatabaseURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MDatabase.class);
      List<MDatabase> mDBs = (List<MDatabase>) query.execute();
      pm.retrieveAll(mDBs);

      for (MDatabase mDB : mDBs) {
        URI locationURI = null;
        String location = mDB.getLocationUri();
        try {
          locationURI = new Path(location).toUri();
        } catch (IllegalArgumentException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String dbLoc = mDB.getLocationUri().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), dbLoc);
            if (!dryRun) {
              mDB.setLocationUri(dbLoc);
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMDatabaseURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  public class UpdatePropURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdatePropURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  private void updatePropURIHelper(URI oldLoc, URI newLoc, String tblPropKey, boolean isDryRun,
                                   List<String> badRecords, Map<String, String> updateLocations,
                                   Map<String, String> parameters) {
    URI tablePropLocationURI = null;
    if (parameters.containsKey(tblPropKey)) {
      String tablePropLocation = parameters.get(tblPropKey);
      try {
        tablePropLocationURI = new Path(tablePropLocation).toUri();
      } catch (IllegalArgumentException e) {
        badRecords.add(tablePropLocation);
      }
      // if tablePropKey that was passed in lead to a valid URI resolution, update it if
      //parts of it match the old-NN-loc, else add to badRecords
      if (tablePropLocationURI == null) {
        badRecords.add(tablePropLocation);
      } else {
        if (shouldUpdateURI(tablePropLocationURI, oldLoc)) {
          String tblPropLoc = parameters.get(tblPropKey).replaceAll(oldLoc.toString(), newLoc
              .toString());
          updateLocations.put(tablePropLocationURI.toString(), tblPropLoc);
          if (!isDryRun) {
            parameters.put(tblPropKey, tblPropLoc);
          }
        }
      }
    }
  }

  /** The following APIs
   *
   *  - updateMStorageDescriptorTblPropURI
   *
   * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
   *
   */
  public UpdatePropURIRetVal updateTblPropURI(URI oldLoc, URI newLoc, String tblPropKey,
      boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
    UpdatePropURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MTable.class);
      List<MTable> mTbls = (List<MTable>) query.execute();
      pm.retrieveAll(mTbls);

      for (MTable mTbl : mTbls) {
        updatePropURIHelper(oldLoc, newLoc, tblPropKey, isDryRun, badRecords, updateLocations,
            mTbl.getParameters());
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdatePropURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  /** The following APIs
  *
  *  - updateMStorageDescriptorTblPropURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  @Deprecated
  public UpdatePropURIRetVal updateMStorageDescriptorTblPropURI(URI oldLoc, URI newLoc,
      String tblPropKey, boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdatePropURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);
      for (MStorageDescriptor mSDS : mSDSs) {
        updatePropURIHelper(oldLoc, newLoc, tblPropKey, isDryRun, badRecords, updateLocations,
            mSDS.getParameters());
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdatePropURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  public class UpdateMStorageDescriptorTblURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;
    private int numNullRecords;

    UpdateMStorageDescriptorTblURIRetVal(List<String> badRecords,
      Map<String, String> updateLocations, int numNullRecords) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
      this.numNullRecords = numNullRecords;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }

    public int getNumNullRecords() {
      return numNullRecords;
    }

    public void setNumNullRecords(int numNullRecords) {
      this.numNullRecords = numNullRecords;
    }
  }

  /** The following APIs
  *
  *  - updateMStorageDescriptorTblURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateMStorageDescriptorTblURIRetVal updateMStorageDescriptorTblURI(URI oldLoc,
      URI newLoc, boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    int numNullRecords = 0;
    UpdateMStorageDescriptorTblURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MStorageDescriptor.class);
      List<MStorageDescriptor> mSDSs = (List<MStorageDescriptor>) query.execute();
      pm.retrieveAll(mSDSs);
      for (MStorageDescriptor mSDS : mSDSs) {
        URI locationURI = null;
        String location = mSDS.getLocation();
        if (location == null) { // This can happen for View or Index
          numNullRecords++;
          continue;
        }
        try {
          locationURI = new Path(location).toUri();
        } catch (IllegalArgumentException e) {
          badRecords.add(location);
        }
        if (locationURI == null) {
          badRecords.add(location);
        } else {
          if (shouldUpdateURI(locationURI, oldLoc)) {
            String tblLoc = mSDS.getLocation().replaceAll(oldLoc.toString(), newLoc.toString());
            updateLocations.put(locationURI.toString(), tblLoc);
            if (!isDryRun) {
              mSDS.setLocation(tblLoc);
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateMStorageDescriptorTblURIRetVal(badRecords, updateLocations, numNullRecords);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  public class UpdateSerdeURIRetVal {
    private List<String> badRecords;
    private Map<String, String> updateLocations;

    UpdateSerdeURIRetVal(List<String> badRecords, Map<String, String> updateLocations) {
      this.badRecords = badRecords;
      this.updateLocations = updateLocations;
    }

    public List<String> getBadRecords() {
      return badRecords;
    }

    public void setBadRecords(List<String> badRecords) {
      this.badRecords = badRecords;
    }

    public Map<String, String> getUpdateLocations() {
      return updateLocations;
    }

    public void setUpdateLocations(Map<String, String> updateLocations) {
      this.updateLocations = updateLocations;
    }
  }

  /** The following APIs
  *
  *  - updateSerdeURI
  *
  * is used by HiveMetaTool. This API **shouldn't** be exposed via Thrift.
  *
  */
  public UpdateSerdeURIRetVal updateSerdeURI(URI oldLoc, URI newLoc, String serdeProp,
      boolean isDryRun) {
    boolean committed = false;
    Query query = null;
    Map<String, String> updateLocations = new HashMap<String, String>();
    List<String> badRecords = new ArrayList<String>();
    UpdateSerdeURIRetVal retVal = null;
    try {
      openTransaction();
      query = pm.newQuery(MSerDeInfo.class);
      List<MSerDeInfo> mSerdes = (List<MSerDeInfo>) query.execute();
      pm.retrieveAll(mSerdes);
      for (MSerDeInfo mSerde : mSerdes) {
        if (mSerde.getParameters().containsKey(serdeProp)) {
          String schemaLoc = mSerde.getParameters().get(serdeProp);
          URI schemaLocURI = null;
          try {
            schemaLocURI = new Path(schemaLoc).toUri();
          } catch (IllegalArgumentException e) {
            badRecords.add(schemaLoc);
          }
          if (schemaLocURI == null) {
            badRecords.add(schemaLoc);
          } else {
            if (shouldUpdateURI(schemaLocURI, oldLoc)) {
              String newSchemaLoc = schemaLoc.replaceAll(oldLoc.toString(), newLoc.toString());
              updateLocations.put(schemaLocURI.toString(), newSchemaLoc);
              if (!isDryRun) {
                mSerde.getParameters().put(serdeProp, newSchemaLoc);
              }
            }
          }
        }
      }
      committed = commitTransaction();
      if (committed) {
        retVal = new UpdateSerdeURIRetVal(badRecords, updateLocations);
      }
      return retVal;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private void writeMTableColumnStatistics(Table table, MTableColumnStatistics mStatsObj,
      MTableColumnStatistics oldStats) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    String dbName = mStatsObj.getDbName();
    String tableName = mStatsObj.getTableName();
    String colName = mStatsObj.getColName();
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      LOG.info("Updating table level column statistics for db=" + dbName + " tableName=" + tableName
        + " colName=" + colName);
      validateTableCols(table, Lists.newArrayList(colName));

      if (oldStats != null) {
        StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
      } else {
        pm.makePersistent(mStatsObj);
      }
    } finally {
      queryWrapper.close();
    }
  }

  private void writeMPartitionColumnStatistics(Table table, Partition partition,
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    String dbName = mStatsObj.getDbName();
    String tableName = mStatsObj.getTableName();
    String partName = mStatsObj.getPartitionName();
    String colName = mStatsObj.getColName();

    LOG.info("Updating partition level column statistics for db=" + dbName + " tableName=" +
      tableName + " partName=" + partName + " colName=" + colName);

    boolean foundCol = false;
    List<FieldSchema> colList = partition.getSd().getCols();
    for (FieldSchema col : colList) {
      if (col.getName().equals(mStatsObj.getColName())) {
        foundCol = true;
        break;
      }
    }

    if (!foundCol) {
      LOG.warn("Column " + colName + " for which stats gathering is requested doesn't exist.");
    }

    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      if (oldStats != null) {
        StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
      } else {
        pm.makePersistent(mStatsObj);
      }
    } finally {
      queryWrapper.close();
    }
  }

  /**
   * Get table's column stats
   *
   * @param table
   * @param colNames
   * @return Map of column name and its stats
   * @throws NoSuchObjectException
   * @throws MetaException
   */
  private Map<String, MTableColumnStatistics> getPartitionColStats(Table table,
     List<String> colNames) throws NoSuchObjectException, MetaException {
    Map<String, MTableColumnStatistics> statsMap = Maps.newHashMap();
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      List<MTableColumnStatistics> stats = getMTableColumnStatistics(table,
          colNames, queryWrapper);
      for(MTableColumnStatistics cStat : stats) {
        statsMap.put(cStat.getColName(), cStat);
      }
    } finally {
      queryWrapper.close();
    }
    return statsMap;
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean committed = false;

    openTransaction();
    try {
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();

      // DataNucleus objects get detached all over the place for no (real) reason.
      // So let's not use them anywhere unless absolutely necessary.
      Table table = ensureGetTable(statsDesc.getDbName(), statsDesc.getTableName());
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      Map<String, MTableColumnStatistics> oldStats = getPartitionColStats(table, colNames);

      for (ColumnStatisticsObj statsObj:statsObjs) {
        // We have to get mtable again because DataNucleus.
        MTableColumnStatistics mStatsObj = StatObjectConverter.convertToMTableColumnStatistics(
            ensureGetMTable(statsDesc.getDbName(), statsDesc.getTableName()), statsDesc, statsObj);
        writeMTableColumnStatistics(table, mStatsObj, oldStats.get(statsObj.getColName()));
        colNames.add(statsObj.getColName());
      }

      // Set the table properties
      // No need to check again if it exists.
      String dbname = table.getDbName();
      String name = table.getTableName();
      MTable oldt = getMTable(dbname, name);
      Map<String, String> parameters = table.getParameters();
      StatsSetupConst.setColumnStatsState(parameters, colNames);
      oldt.setParameters(parameters);

      committed = commitTransaction();
      return committed;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Get partition's column stats
   *
   * @param table
   * @param partitionName
   * @param colNames
   * @return Map of column name and its stats
   * @throws NoSuchObjectException
   * @throws MetaException
   */
  private Map<String, MPartitionColumnStatistics> getPartitionColStats(Table table,
      String partitionName, List<String> colNames) throws NoSuchObjectException, MetaException {
    Map<String, MPartitionColumnStatistics> statsMap = Maps.newHashMap();
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      List<MPartitionColumnStatistics> stats = getMPartitionColumnStatistics(table,
          Lists.newArrayList(partitionName), colNames, queryWrapper);
      for(MPartitionColumnStatistics cStat : stats) {
        statsMap.put(cStat.getColName(), cStat);
      }
    } finally {
      queryWrapper.close();
    }
    return statsMap;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean committed = false;

    try {
      openTransaction();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
      Table table = ensureGetTable(statsDesc.getDbName(), statsDesc.getTableName());
      Partition partition = convertToPart(getMPartition(
          statsDesc.getDbName(), statsDesc.getTableName(), partVals));
      List<String> colNames = new ArrayList<>();

      for(ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }

      Map<String, MPartitionColumnStatistics> oldStats = getPartitionColStats(table, statsDesc
          .getPartName(), colNames);

      MPartition mPartition = getMPartition(
          statsDesc.getDbName(), statsDesc.getTableName(), partVals);
      if (partition == null) {
        throw new NoSuchObjectException("Partition for which stats is gathered doesn't exist.");
      }

      for (ColumnStatisticsObj statsObj : statsObjs) {
        MPartitionColumnStatistics mStatsObj =
            StatObjectConverter.convertToMPartitionColumnStatistics(mPartition, statsDesc, statsObj);
        writeMPartitionColumnStatistics(table, partition, mStatsObj,
            oldStats.get(statsObj.getColName()));
      }
      Map<String, String> parameters = mPartition.getParameters();
      StatsSetupConst.setColumnStatsState(parameters, colNames);
      mPartition.setParameters(parameters);
      committed = commitTransaction();
      return committed;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private List<MTableColumnStatistics> getMTableColumnStatistics(Table table, List<String> colNames, QueryWrapper queryWrapper)
      throws MetaException {
    if (colNames == null || colNames.isEmpty()) {
      return null;
    }

    boolean committed = false;

    try {
      openTransaction();

      List<MTableColumnStatistics> result = null;
      validateTableCols(table, colNames);
      Query query = queryWrapper.query = pm.newQuery(MTableColumnStatistics.class);
      String filter = "tableName == t1 && dbName == t2 && (";
      String paramStr = "java.lang.String t1, java.lang.String t2";
      Object[] params = new Object[colNames.size() + 2];
      params[0] = table.getTableName();
      params[1] = table.getDbName();
      for (int i = 0; i < colNames.size(); ++i) {
        filter += ((i == 0) ? "" : " || ") + "colName == c" + i;
        paramStr += ", java.lang.String c" + i;
        params[i + 2] = colNames.get(i);
      }
      filter += ")";
      query.setFilter(filter);
      query.declareParameters(paramStr);
      result = (List<MTableColumnStatistics>) query.executeWithArray(params);
      pm.retrieveAll(result);
      if (result.size() > colNames.size()) {
        throw new MetaException("Unexpected " + result.size() + " statistics for "
            + colNames.size() + " columns");
      }
      committed = commitTransaction();
      return result;
    } catch (Exception ex) {
      LOG.error("Error retrieving statistics via jdo", ex);
      if (ex instanceof MetaException) {
        throw (MetaException) ex;
      }
      throw new MetaException(ex.getMessage());
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @VisibleForTesting
  public void validateTableCols(Table table, List<String> colNames) throws MetaException {
    List<FieldSchema> colList = table.getSd().getCols();
    for (String colName : colNames) {
      boolean foundCol = false;
      for (FieldSchema mCol : colList) {
        if (mCol.getName().equals(colName)) {
          foundCol = true;
          break;
        }
      }
      if (!foundCol) {
        throw new MetaException("Column " + colName + " doesn't exist in table "
            + table.getTableName() + " in database " + table.getDbName());
      }
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    return getTableColumnStatisticsInternal(dbName, tableName, colNames, true, true);
  }

  protected ColumnStatistics getTableColumnStatisticsInternal(
      String dbName, String tableName, final List<String> colNames, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    return new GetStatHelper(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName), allowSql, allowJdo) {
      @Override
      protected ColumnStatistics getSqlResult(GetHelper<ColumnStatistics> ctx) throws MetaException {
        return directSql.getTableStats(dbName, tblName, colNames);
      }
      @Override
      protected ColumnStatistics getJdoResult(
          GetHelper<ColumnStatistics> ctx) throws MetaException {
        QueryWrapper queryWrapper = new QueryWrapper();

        try {
        List<MTableColumnStatistics> mStats = getMTableColumnStatistics(getTable(), colNames, queryWrapper);
        if (mStats.isEmpty()) return null;
        // LastAnalyzed is stored per column, but thrift object has it per multiple columns.
        // Luckily, nobody actually uses it, so we will set to lowest value of all columns for now.
        ColumnStatisticsDesc desc = StatObjectConverter.getTableColumnStatisticsDesc(mStats.get(0));
        List<ColumnStatisticsObj> statObjs = new ArrayList<ColumnStatisticsObj>(mStats.size());
        for (MTableColumnStatistics mStat : mStats) {
          if (desc.getLastAnalyzed() > mStat.getLastAnalyzed()) {
            desc.setLastAnalyzed(mStat.getLastAnalyzed());
          }
          statObjs.add(StatObjectConverter.getTableColumnStatisticsObj(mStat));
          Deadline.checkTimeout();
        }
        return new ColumnStatistics(desc, statObjs);
        } finally {
          queryWrapper.close();
        }
      }
    }.run(true);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return getPartitionColumnStatisticsInternal(
        dbName, tableName, partNames, colNames, true, true);
  }

  protected List<ColumnStatistics> getPartitionColumnStatisticsInternal(
      String dbName, String tableName, final List<String> partNames, final List<String> colNames,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    return new GetListHelper<ColumnStatistics>(dbName, tableName, allowSql, allowJdo) {
      @Override
      protected List<ColumnStatistics> getSqlResult(
          GetHelper<List<ColumnStatistics>> ctx) throws MetaException {
        return directSql.getPartitionStats(dbName, tblName, partNames, colNames);
      }
      @Override
      protected List<ColumnStatistics> getJdoResult(
          GetHelper<List<ColumnStatistics>> ctx) throws MetaException, NoSuchObjectException {
        QueryWrapper queryWrapper = new QueryWrapper();
        try {
          List<MPartitionColumnStatistics> mStats =
              getMPartitionColumnStatistics(getTable(), partNames, colNames, queryWrapper);
          List<ColumnStatistics> result = new ArrayList<ColumnStatistics>(
              Math.min(mStats.size(), partNames.size()));
          String lastPartName = null;
          List<ColumnStatisticsObj> curList = null;
          ColumnStatisticsDesc csd = null;
          for (int i = 0; i <= mStats.size(); ++i) {
            boolean isLast = i == mStats.size();
            MPartitionColumnStatistics mStatsObj = isLast ? null : mStats.get(i);
            String partName = isLast ? null : (String)mStatsObj.getPartitionName();
            if (isLast || !partName.equals(lastPartName)) {
              if (i != 0) {
                result.add(new ColumnStatistics(csd, curList));
              }
              if (isLast) {
                continue;
              }
              csd = StatObjectConverter.getPartitionColumnStatisticsDesc(mStatsObj);
              curList = new ArrayList<ColumnStatisticsObj>(colNames.size());
            }
            curList.add(StatObjectConverter.getPartitionColumnStatisticsObj(mStatsObj));
            lastPartName = partName;
            Deadline.checkTimeout();
          }
          return result;
        } finally {
          queryWrapper.close();
        }
      }
    }.run(true);
  }


  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName,
      final List<String> partNames, final List<String> colNames) throws MetaException, NoSuchObjectException {
    final boolean useDensityFunctionForNDVEstimation = HiveConf.getBoolVar(getConf(),
        HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = HiveConf.getFloatVar(getConf(),
        HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_TUNER);
    return new GetHelper<AggrStats>(dbName, tblName, true, false) {
      @Override
      protected AggrStats getSqlResult(GetHelper<AggrStats> ctx)
          throws MetaException {
        return directSql.aggrColStatsForPartitions(dbName, tblName, partNames,
            colNames, useDensityFunctionForNDVEstimation, ndvTuner);
      }
      @Override
      protected AggrStats getJdoResult(GetHelper<AggrStats> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using
        // directSql, do it. No point in failing back to slow path here.
        throw new MetaException("Jdo path is not implemented for stats aggr.");
      }
      @Override
      protected String describeResult() {
        return null;
      }
    }.run(true);
  }

  @Override
  public void flushCache() {
    // NOP as there's no caching
  }

  private List<MPartitionColumnStatistics> getMPartitionColumnStatistics(
      Table table, List<String> partNames, List<String> colNames, QueryWrapper queryWrapper)
          throws NoSuchObjectException, MetaException {
    boolean committed = false;

    try {
      openTransaction();
      // We are not going to verify SD for each partition. Just verify for the table.
      validateTableCols(table, colNames);
      Query query = queryWrapper.query = pm.newQuery(MPartitionColumnStatistics.class);
      String paramStr = "java.lang.String t1, java.lang.String t2";
      String filter = "tableName == t1 && dbName == t2 && (";
      Object[] params = new Object[colNames.size() + partNames.size() + 2];
      int i = 0;
      params[i++] = table.getTableName();
      params[i++] = table.getDbName();
      int firstI = i;
      for (String s : partNames) {
        filter += ((i == firstI) ? "" : " || ") + "partitionName == p" + i;
        paramStr += ", java.lang.String p" + i;
        params[i++] = s;
      }
      filter += ") && (";
      firstI = i;
      for (String s : colNames) {
        filter += ((i == firstI) ? "" : " || ") + "colName == c" + i;
        paramStr += ", java.lang.String c" + i;
        params[i++] = s;
      }
      filter += ")";
      query.setFilter(filter);
      query.declareParameters(paramStr);
      query.setOrdering("partitionName ascending");
      @SuppressWarnings("unchecked")
      List<MPartitionColumnStatistics> result =
          (List<MPartitionColumnStatistics>) query.executeWithArray(params);
      pm.retrieveAll(result);
      committed = commitTransaction();
      return result;
    } catch (Exception ex) {
      LOG.error("Error retrieving statistics via jdo", ex);
      if (ex instanceof MetaException) {
        throw (MetaException) ex;
      }
      throw new MetaException(ex.getMessage());
    } finally {
      if (!committed) {
        rollbackTransaction();
        return Lists.newArrayList();
      }
    }
  }

  private void dropPartitionColumnStatisticsNoTxn(
      String dbName, String tableName, List<String> partNames) throws MetaException {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(
        dbName, tableName, partNames, MPartitionColumnStatistics.class,
        "tableName", "dbName", "partition.partitionName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
      List<String> partVals, String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    try {
      openTransaction();
      MTable mTable = getMTable(dbName, tableName);
      MPartitionColumnStatistics mStatsObj;
      List<MPartitionColumnStatistics> mStatsObjColl;
      if (mTable == null) {
        throw new NoSuchObjectException("Table " + tableName
            + "  for which stats deletion is requested doesn't exist");
      }
      MPartition mPartition = getMPartition(dbName, tableName, partVals);
      if (mPartition == null) {
        throw new NoSuchObjectException("Partition " + partName
            + " for which stats deletion is requested doesn't exist");
      }
      query = pm.newQuery(MPartitionColumnStatistics.class);
      String filter;
      String parameters;
      if (colName != null) {
        filter =
            "partition.partitionName == t1 && dbName == t2 && tableName == t3 && "
                + "colName == t4";
        parameters =
            "java.lang.String t1, java.lang.String t2, "
                + "java.lang.String t3, java.lang.String t4";
      } else {
        filter = "partition.partitionName == t1 && dbName == t2 && tableName == t3";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      }
      query.setFilter(filter);
      query.declareParameters(parameters);
      if (colName != null) {
        query.setUnique(true);
        mStatsObj =
            (MPartitionColumnStatistics) query.executeWithArray(partName.trim(),
                HiveStringUtils.normalizeIdentifier(dbName),
                HiveStringUtils.normalizeIdentifier(tableName),
                HiveStringUtils.normalizeIdentifier(colName));
        pm.retrieve(mStatsObj);
        if (mStatsObj != null) {
          pm.deletePersistent(mStatsObj);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName + " table="
              + tableName + " partition=" + partName + " col=" + colName);
        }
      } else {
        mStatsObjColl =
            (List<MPartitionColumnStatistics>) query.execute(partName.trim(),
                HiveStringUtils.normalizeIdentifier(dbName),
                HiveStringUtils.normalizeIdentifier(tableName));
        pm.retrieveAll(mStatsObjColl);
        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName + " table="
              + tableName + " partition" + partName);
        }
      }
      ret = commitTransaction();
    } catch (NoSuchObjectException e) {
      rollbackTransaction();
      throw e;
    } finally {
      if (!ret) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return ret;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    if (dbName == null) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    try {
      openTransaction();
      MTable mTable = getMTable(dbName, tableName);
      MTableColumnStatistics mStatsObj;
      List<MTableColumnStatistics> mStatsObjColl;
      if (mTable == null) {
        throw new NoSuchObjectException("Table " + tableName
            + "  for which stats deletion is requested doesn't exist");
      }
      query = pm.newQuery(MTableColumnStatistics.class);
      String filter;
      String parameters;
      if (colName != null) {
        filter = "table.tableName == t1 && dbName == t2 && colName == t3";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      } else {
        filter = "table.tableName == t1 && dbName == t2";
        parameters = "java.lang.String t1, java.lang.String t2";
      }

      query.setFilter(filter);
      query.declareParameters(parameters);
      if (colName != null) {
        query.setUnique(true);
        mStatsObj =
            (MTableColumnStatistics) query.execute(HiveStringUtils.normalizeIdentifier(tableName),
                HiveStringUtils.normalizeIdentifier(dbName),
                HiveStringUtils.normalizeIdentifier(colName));
        pm.retrieve(mStatsObj);

        if (mStatsObj != null) {
          pm.deletePersistent(mStatsObj);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName + " table="
              + tableName + " col=" + colName);
        }
      } else {
        mStatsObjColl =
            (List<MTableColumnStatistics>) query.execute(
                HiveStringUtils.normalizeIdentifier(tableName),
                HiveStringUtils.normalizeIdentifier(dbName));
        pm.retrieveAll(mStatsObjColl);
        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName + " table="
              + tableName);
        }
      }
      ret = commitTransaction();
    } catch (NoSuchObjectException e) {
      rollbackTransaction();
      throw e;
    } finally {
      if (!ret) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return ret;
  }

  @Override
  public long cleanupEvents() {
    boolean commited = false;
    Query query = null;
    long delCnt;
    LOG.debug("Begin executing cleanupEvents");
    Long expiryTime =
        HiveConf.getTimeVar(getConf(), ConfVars.METASTORE_EVENT_EXPIRY_DURATION,
            TimeUnit.MILLISECONDS);
    Long curTime = System.currentTimeMillis();
    try {
      openTransaction();
      query = pm.newQuery(MPartitionEvent.class, "curTime - eventTime > expiryTime");
      query.declareParameters("java.lang.Long curTime, java.lang.Long expiryTime");
      delCnt = query.deletePersistentAll(curTime, expiryTime);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
      LOG.debug("Done executing cleanupEvents");
    }
    return delCnt;
  }

  private MDelegationToken getTokenFrom(String tokenId) {
    Query query = pm.newQuery(MDelegationToken.class, "tokenIdentifier == tokenId");
    query.declareParameters("java.lang.String tokenId");
    query.setUnique(true);
    MDelegationToken delegationToken = (MDelegationToken) query.execute(tokenId);
    if (query != null) {
      query.closeAll();
    }
    return delegationToken;
  }

  @Override
  public boolean addToken(String tokenId, String delegationToken) {

    LOG.debug("Begin executing addToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (token == null) {
        // add Token, only if it already doesn't exist
        pm.makePersistent(new MDelegationToken(tokenId, delegationToken));
      }
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing addToken with status : " + committed);
    return committed && (token == null);
  }

  @Override
  public boolean removeToken(String tokenId) {

    LOG.debug("Begin executing removeToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (null != token) {
        pm.deletePersistent(token);
      }
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing removeToken with status : " + committed);
    return committed && (token != null);
  }

  @Override
  public String getToken(String tokenId) {

    LOG.debug("Begin executing getToken");
    boolean committed = false;
    MDelegationToken token;
    try{
      openTransaction();
      token = getTokenFrom(tokenId);
      if (null != token) {
        pm.retrieve(token);
      }
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing getToken with status : " + committed);
    return (null == token) ? null : token.getTokenStr();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    LOG.debug("Begin executing getAllTokenIdentifiers");
    boolean committed = false;
    Query query = null;
    List<String> tokenIdents = new ArrayList<String>();

    try {
      openTransaction();
      query = pm.newQuery(MDelegationToken.class);
      List<MDelegationToken> tokens = (List<MDelegationToken>) query.execute();
      pm.retrieveAll(tokens);
      committed = commitTransaction();

      for (MDelegationToken token : tokens) {
        tokenIdents.add(token.getTokenIdentifier());
      }
      return tokenIdents;
    } finally {
      LOG.debug("Done executing getAllTokenIdentifers with status : " + committed);
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public int addMasterKey(String key) throws MetaException{
    LOG.debug("Begin executing addMasterKey");
    boolean committed = false;
    MMasterKey masterKey = new MMasterKey(key);
    try{
      openTransaction();
      pm.makePersistent(masterKey);
      committed = commitTransaction();
    } finally {
      if(!committed) {
        rollbackTransaction();
      }
    }
    LOG.debug("Done executing addMasterKey with status : " + committed);
    if (committed) {
      return ((IntIdentity)pm.getObjectId(masterKey)).getKey();
    } else {
      throw new MetaException("Failed to add master key.");
    }
  }

  @Override
  public void updateMasterKey(Integer id, String key) throws NoSuchObjectException, MetaException {
    LOG.debug("Begin executing updateMasterKey");
    boolean committed = false;
    Query query = null;
    MMasterKey masterKey;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class, "keyId == id");
      query.declareParameters("java.lang.Integer id");
      query.setUnique(true);
      masterKey = (MMasterKey) query.execute(id);
      if (null != masterKey) {
        masterKey.setMasterKey(key);
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    LOG.debug("Done executing updateMasterKey with status : " + committed);
    if (null == masterKey) {
      throw new NoSuchObjectException("No key found with keyId: " + id);
    }
    if (!committed) {
      throw new MetaException("Though key is found, failed to update it. " + id);
    }
  }

  @Override
  public boolean removeMasterKey(Integer id) {
    LOG.debug("Begin executing removeMasterKey");
    boolean success = false;
    Query query = null;
    MMasterKey masterKey;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class, "keyId == id");
      query.declareParameters("java.lang.Integer id");
      query.setUnique(true);
      masterKey = (MMasterKey) query.execute(id);
      if (null != masterKey) {
        pm.deletePersistent(masterKey);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    LOG.debug("Done executing removeMasterKey with status : " + success);
    return (null != masterKey) && success;
  }

  @Override
  public String[] getMasterKeys() {
    LOG.debug("Begin executing getMasterKeys");
    boolean committed = false;
    Query query = null;
    List<MMasterKey> keys;
    try {
      openTransaction();
      query = pm.newQuery(MMasterKey.class);
      keys = (List<MMasterKey>) query.execute();
      pm.retrieveAll(keys);
      committed = commitTransaction();

      String[] masterKeys = new String[keys.size()];
      for (int i = 0; i < keys.size(); i++) {
        masterKeys[i] = keys.get(i).getMasterKey();
      }
      return masterKeys;
    } finally {
      LOG.debug("Done executing getMasterKeys with status : " + committed);
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  // compare hive version and metastore version
  @Override
  public void verifySchema() throws MetaException {
    // If the schema version is already checked, then go ahead and use this metastore
    if (isSchemaVerified.get()) {
      return;
    }
    checkSchema();
  }

  public static void setSchemaVerified(boolean val) {
    isSchemaVerified.set(val);
  }

  private synchronized void checkSchema() throws MetaException {
    // recheck if it got verified by another thread while we were waiting
    if (isSchemaVerified.get()) {
      return;
    }

    boolean strictValidation =
      HiveConf.getBoolVar(getConf(), HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION);
    // read the schema version stored in metastore db
    String dbSchemaVer = getMetaStoreSchemaVersion();
    // version of schema for this version of hive
    String hiveSchemaVer = MetaStoreSchemaInfo.getHiveSchemaVersion();

    if (dbSchemaVer == null) {
      if (strictValidation) {
        throw new MetaException("Version information not found in metastore. ");
      } else {
        LOG.warn("Version information not found in metastore. "
            + HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION.toString() +
            " is not enabled so recording the schema version " +
            hiveSchemaVer);
        setMetaStoreSchemaVersion(hiveSchemaVer,
          "Set by MetaStore " + USER + "@" + HOSTNAME);
      }
    } else {
      if (MetaStoreSchemaInfo.isVersionCompatible(hiveSchemaVer, dbSchemaVer)) {
        LOG.debug("Found expected HMS version of " + dbSchemaVer);
      } else {
        // metastore schema version is different than Hive distribution needs
        if (strictValidation) {
          throw new MetaException("Hive Schema version " + hiveSchemaVer +
              " does not match metastore's schema version " + dbSchemaVer +
              " Metastore is not upgraded or corrupt");
        } else {
          LOG.error("Version information found in metastore differs " + dbSchemaVer +
              " from expected schema version " + hiveSchemaVer +
              ". Schema verififcation is disabled " +
              HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION);
          setMetaStoreSchemaVersion(hiveSchemaVer,
            "Set by MetaStore " + USER + "@" + HOSTNAME);
        }
      }
    }
    isSchemaVerified.set(true);
    return;
  }

  // load the schema version stored in metastore db
  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {

    MVersionTable mSchemaVer;
    try {
      mSchemaVer = getMSchemaVersion();
    } catch (NoSuchObjectException e) {
      return null;
    }
    return mSchemaVer.getSchemaVersion();
  }

  @SuppressWarnings("unchecked")
  private MVersionTable getMSchemaVersion() throws NoSuchObjectException, MetaException {
    boolean committed = false;
    Query query = null;
    List<MVersionTable> mVerTables = new ArrayList<MVersionTable>();
    try {
      openTransaction();
      query = pm.newQuery(MVersionTable.class);
      try {
        mVerTables = (List<MVersionTable>) query.execute();
        pm.retrieveAll(mVerTables);
      } catch (JDODataStoreException e) {
        if (e.getCause() instanceof MissingTableException) {
          throw new MetaException("Version table not found. " + "The metastore is not upgraded to "
              + MetaStoreSchemaInfo.getHiveSchemaVersion());
        } else {
          throw e;
        }
      }
      committed = commitTransaction();
      if (mVerTables.isEmpty()) {
        throw new NoSuchObjectException("No matching version found");
      }
      if (mVerTables.size() > 1) {
        String msg = "Metastore contains multiple versions (" + mVerTables.size() + ") ";
        for (MVersionTable version : mVerTables) {
          msg +=
              "[ version = " + version.getSchemaVersion() + ", comment = "
                  + version.getVersionComment() + " ] ";
        }
        throw new MetaException(msg.trim());
      }
      return mVerTables.get(0);
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public void setMetaStoreSchemaVersion(String schemaVersion, String comment) throws MetaException {
    MVersionTable mSchemaVer;
    boolean commited = false;
    boolean recordVersion =
      HiveConf.getBoolVar(getConf(), HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION_RECORD_VERSION);
    if (!recordVersion) {
      LOG.warn("setMetaStoreSchemaVersion called but recording version is disabled: " +
        "version = " + schemaVersion + ", comment = " + comment);
      return;
    }
    LOG.warn("Setting metastore schema version in db to " + schemaVersion);

    try {
      mSchemaVer = getMSchemaVersion();
    } catch (NoSuchObjectException e) {
      // if the version doesn't exist, then create it
      mSchemaVer = new MVersionTable();
    }

    mSchemaVer.setSchemaVersion(schemaVersion);
    mSchemaVer.setVersionComment(comment);
    try {
      openTransaction();
      pm.makePersistent(mSchemaVer);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> partVals)
      throws MetaException {
    try {
      return this.getPartition(dbName, tableName, partVals) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  private void debugLog(String message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message + getCallStack());
    }
  }

  private static final int stackLimit = 5;

  private String getCallStack() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    int thislimit = Math.min(stackLimit, stackTrace.length);
    StringBuilder sb = new StringBuilder();
    sb.append(" at:");
    for (int i = 4; i < thislimit; i++) {
      sb.append("\n\t");
      sb.append(stackTrace[i].toString());
    }
    return sb.toString();
  }

  private Function convertToFunction(MFunction mfunc) {
    if (mfunc == null) {
      return null;
    }

    Function func = new Function(mfunc.getFunctionName(),
        mfunc.getDatabase().getName(),
        mfunc.getClassName(),
        mfunc.getOwnerName(),
        PrincipalType.valueOf(mfunc.getOwnerType()),
        mfunc.getCreateTime(),
        FunctionType.findByValue(mfunc.getFunctionType()),
        convertToResourceUriList(mfunc.getResourceUris()));
    return func;
  }

  private List<Function> convertToFunctions(List<MFunction> mfuncs) {
    if (mfuncs == null) {
      return null;
    }
    List<Function> functions = new ArrayList<>();
    for (MFunction mfunc : mfuncs) {
      functions.add(convertToFunction(mfunc));
    }
    return functions;
  }

  private MFunction convertToMFunction(Function func) throws InvalidObjectException {
    if (func == null) {
      return null;
    }

    MDatabase mdb = null;
    try {
      mdb = getMDatabase(func.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new InvalidObjectException("Database " + func.getDbName() + " doesn't exist.");
    }

    MFunction mfunc = new MFunction(func.getFunctionName(),
        mdb,
        func.getClassName(),
        func.getOwnerName(),
        func.getOwnerType().name(),
        func.getCreateTime(),
        func.getFunctionType().getValue(),
        convertToMResourceUriList(func.getResourceUris()));
    return mfunc;
  }

  private List<ResourceUri> convertToResourceUriList(List<MResourceUri> mresourceUriList) {
    List<ResourceUri> resourceUriList = null;
    if (mresourceUriList != null) {
      resourceUriList = new ArrayList<ResourceUri>(mresourceUriList.size());
      for (MResourceUri mres : mresourceUriList) {
        resourceUriList.add(
            new ResourceUri(ResourceType.findByValue(mres.getResourceType()), mres.getUri()));
      }
    }
    return resourceUriList;
  }

  private List<MResourceUri> convertToMResourceUriList(List<ResourceUri> resourceUriList) {
    List<MResourceUri> mresourceUriList = null;
    if (resourceUriList != null) {
      mresourceUriList = new ArrayList<MResourceUri>(resourceUriList.size());
      for (ResourceUri res : resourceUriList) {
        mresourceUriList.add(new MResourceUri(res.getResourceType().getValue(), res.getUri()));
      }
    }
    return mresourceUriList;
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MFunction mfunc = convertToMFunction(func);
      pm.makePersistent(mfunc);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      funcName = HiveStringUtils.normalizeIdentifier(funcName);
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      MFunction newf = convertToMFunction(newFunction);
      if (newf == null) {
        throw new InvalidObjectException("new function is invalid");
      }

      MFunction oldf = getMFunction(dbName, funcName);
      if (oldf == null) {
        throw new MetaException("function " + funcName + " doesn't exist");
      }

      // For now only alter name, owner, class name, type
      oldf.setFunctionName(HiveStringUtils.normalizeIdentifier(newf.getFunctionName()));
      oldf.setDatabase(newf.getDatabase());
      oldf.setOwnerName(newf.getOwnerName());
      oldf.setOwnerType(newf.getOwnerType());
      oldf.setClassName(newf.getClassName());
      oldf.setFunctionType(newf.getFunctionType());

      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
  NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MFunction mfunc = getMFunction(dbName, funcName);
      pm.retrieve(mfunc);
      if (mfunc != null) {
        // TODO: When function privileges are implemented, they should be deleted here.
        pm.deletePersistentAll(mfunc);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  private MFunction getMFunction(String db, String function) {
    MFunction mfunc = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      db = HiveStringUtils.normalizeIdentifier(db);
      function = HiveStringUtils.normalizeIdentifier(function);
      query = pm.newQuery(MFunction.class, "functionName == function && database.name == db");
      query.declareParameters("java.lang.String function, java.lang.String db");
      query.setUnique(true);
      mfunc = (MFunction) query.execute(function, db);
      pm.retrieve(mfunc);
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return mfunc;
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException {
    boolean commited = false;
    Function func = null;
    try {
      openTransaction();
      func = convertToFunction(getMFunction(dbName, funcName));
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return func;
  }

  @Override
  public List<Function> getAllFunctions() throws MetaException {
    boolean commited = false;
    try {
      openTransaction();
      Query query = pm.newQuery(MFunction.class);
      List<MFunction> allFunctions = (List<MFunction>) query.execute();
      pm.retrieveAll(allFunctions);
      commited = commitTransaction();
      return convertToFunctions(allFunctions);
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    boolean commited = false;
    Query query = null;
    List<String> funcs = null;
    try {
      openTransaction();
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      // Take the pattern and split it on the | to get all the composing
      // patterns
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder();
      appendSimpleCondition(filterBuilder, "database.name", new String[] { dbName }, parameterVals);
      if(pattern != null) {
        appendPatternCondition(filterBuilder, "functionName", pattern, parameterVals);
      }
      query = pm.newQuery(MFunction.class, filterBuilder.toString());
      query.setResult("functionName");
      query.setOrdering("functionName ascending");
      Collection names = (Collection) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
      funcs = new ArrayList<String>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        funcs.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return funcs;
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      long lastEvent = rqst.getLastEvent();
      query = pm.newQuery(MNotificationLog.class, "eventId > lastEvent");
      query.declareParameters("java.lang.Long lastEvent");
      query.setOrdering("eventId ascending");
      Collection<MNotificationLog> events = (Collection) query.execute(lastEvent);
      commited = commitTransaction();
      if (events == null) {
        return null;
      }
      Iterator<MNotificationLog> i = events.iterator();
      NotificationEventResponse result = new NotificationEventResponse();
      result.setEvents(new ArrayList<NotificationEvent>());
      int maxEvents = rqst.getMaxEvents() > 0 ? rqst.getMaxEvents() : Integer.MAX_VALUE;
      int numEvents = 0;
      while (i.hasNext() && numEvents++ < maxEvents) {
        result.addToEvents(translateDbToThrift(i.next()));
      }
      return result;
    } finally {
      if (query != null) {
        query.closeAll();
      }
      if (!commited) {
        rollbackTransaction();
        return null;
      }
    }
  }

  @Override
  public void addNotificationEvent(NotificationEvent entry) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MNotificationNextId.class);
      Collection<MNotificationNextId> ids = (Collection) query.execute();
      MNotificationNextId id = null;
      boolean needToPersistId;
      if (ids == null || ids.size() == 0) {
        id = new MNotificationNextId(1L);
        needToPersistId = true;
      } else {
        id = ids.iterator().next();
        needToPersistId = false;
      }
      entry.setEventId(id.getNextEventId());
      id.incrementEventId();
      if (needToPersistId)
        pm.makePersistent(id);
      pm.makePersistent(translateThriftToDb(entry));
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      long tmp = System.currentTimeMillis() / 1000 - olderThan;
      int tooOld = (tmp > Integer.MAX_VALUE) ? 0 : (int) tmp;
      query = pm.newQuery(MNotificationLog.class, "eventTime < tooOld");
      query.declareParameters("java.lang.Integer tooOld");
      Collection<MNotificationLog> toBeRemoved = (Collection) query.execute(tooOld);
      if (toBeRemoved != null && toBeRemoved.size() > 0) {
        pm.deletePersistentAll(toBeRemoved);
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MNotificationNextId.class);
      Collection<MNotificationNextId> ids = (Collection) query.execute();
      long id = 0;
      if (ids != null && ids.size() > 0) {
        id = ids.iterator().next().getNextEventId() - 1;
      }
      commited = commitTransaction();
      return new CurrentNotificationEventId(id);
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
  }

  private MNotificationLog translateThriftToDb(NotificationEvent entry) {
    MNotificationLog dbEntry = new MNotificationLog();
    dbEntry.setEventId(entry.getEventId());
    dbEntry.setEventTime(entry.getEventTime());
    dbEntry.setEventType(entry.getEventType());
    dbEntry.setDbName(entry.getDbName());
    dbEntry.setTableName(entry.getTableName());
    dbEntry.setMessage(entry.getMessage());
    dbEntry.setMessageFormat(entry.getMessageFormat());
    return dbEntry;
  }

  private NotificationEvent translateDbToThrift(MNotificationLog dbEvent) {
    NotificationEvent event = new NotificationEvent();
    event.setEventId(dbEvent.getEventId());
    event.setEventTime(dbEvent.getEventTime());
    event.setEventType(dbEvent.getEventType());
    event.setDbName(dbEvent.getDbName());
    event.setTableName(dbEvent.getTableName());
    event.setMessage((dbEvent.getMessage()));
    event.setMessageFormat(dbEvent.getMessageFormat());
    return event;
  }

  @Override
  public boolean isFileMetadataSupported() {
    return false;
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putFileMetadata(
      List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] stripeBitsets, boolean[] eliminated) {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    throw new UnsupportedOperationException();
  }

  /**
   * Removed cached classloaders from DataNucleus
   * DataNucleus caches classloaders in NucleusContext.
   * In UDFs, this can result in classloaders not getting GCed resulting in PermGen leaks.
   * This is particularly an issue when using embedded metastore with HiveServer2,
   * since the current classloader gets modified with each new add jar,
   * becoming the classloader for downstream classes, which DataNucleus ends up using.
   * The NucleusContext cache gets freed up only on calling a close on it.
   * We're not closing NucleusContext since it does a bunch of other things which we don't want.
   * We're not clearing the cache HashMap by calling HashMap#clear to avoid concurrency issues.
   */
  public static void unCacheDataNucleusClassLoaders() {
    PersistenceManagerFactory pmf = ObjectStore.getPMF();
    clearOutPmfClassLoaderCache(pmf);
  }

  private static void clearOutPmfClassLoaderCache(PersistenceManagerFactory pmf) {
    if ((pmf == null) || (!(pmf instanceof JDOPersistenceManagerFactory))) {
      return;
    }
    // NOTE : This is hacky, and this section of code is fragile depending on DN code varnames
    // so it's likely to stop working at some time in the future, especially if we upgrade DN
    // versions, so we actively need to find a better way to make sure the leak doesn't happen
    // instead of just clearing out the cache after every call.
    JDOPersistenceManagerFactory jdoPmf = (JDOPersistenceManagerFactory) pmf;
    NucleusContext nc = jdoPmf.getNucleusContext();
    try {
      Field pmCache = pmf.getClass().getDeclaredField("pmCache");
      pmCache.setAccessible(true);
      Set<JDOPersistenceManager> pmSet = (Set<JDOPersistenceManager>)pmCache.get(pmf);
      for (JDOPersistenceManager pm : pmSet) {
        org.datanucleus.ExecutionContext ec = (org.datanucleus.ExecutionContext)pm.getExecutionContext();
        if (ec instanceof org.datanucleus.ExecutionContextThreadedImpl) {
          ClassLoaderResolver clr = ((org.datanucleus.ExecutionContextThreadedImpl)ec).getClassLoaderResolver();
          clearClr(clr);
        }
      }
      org.datanucleus.plugin.PluginManager pluginManager = jdoPmf.getNucleusContext().getPluginManager();
      Field registryField = pluginManager.getClass().getDeclaredField("registry");
      registryField.setAccessible(true);
      org.datanucleus.plugin.PluginRegistry registry = (org.datanucleus.plugin.PluginRegistry)registryField.get(pluginManager);
      if (registry instanceof org.datanucleus.plugin.NonManagedPluginRegistry) {
        org.datanucleus.plugin.NonManagedPluginRegistry nRegistry = (org.datanucleus.plugin.NonManagedPluginRegistry)registry;
        Field clrField = nRegistry.getClass().getDeclaredField("clr");
        clrField.setAccessible(true);
        ClassLoaderResolver clr = (ClassLoaderResolver)clrField.get(nRegistry);
        clearClr(clr);
      }
      if (nc instanceof org.datanucleus.PersistenceNucleusContextImpl) {
        org.datanucleus.PersistenceNucleusContextImpl pnc = (org.datanucleus.PersistenceNucleusContextImpl)nc;
        org.datanucleus.store.types.TypeManagerImpl tm = (org.datanucleus.store.types.TypeManagerImpl)pnc.getTypeManager();
        Field clrField = tm.getClass().getDeclaredField("clr");
        clrField.setAccessible(true);
        ClassLoaderResolver clr = (ClassLoaderResolver)clrField.get(tm);
        clearClr(clr);
        Field storeMgrField = pnc.getClass().getDeclaredField("storeMgr");
        storeMgrField.setAccessible(true);
        org.datanucleus.store.rdbms.RDBMSStoreManager storeMgr = (org.datanucleus.store.rdbms.RDBMSStoreManager)storeMgrField.get(pnc);
        Field backingStoreField = storeMgr.getClass().getDeclaredField("backingStoreByMemberName");
        backingStoreField.setAccessible(true);
        Map<String, Store> backingStoreByMemberName = (Map<String, Store>)backingStoreField.get(storeMgr);
        for (Store store : backingStoreByMemberName.values()) {
          org.datanucleus.store.rdbms.scostore.BaseContainerStore baseStore = (org.datanucleus.store.rdbms.scostore.BaseContainerStore)store;
          clrField = org.datanucleus.store.rdbms.scostore.BaseContainerStore.class.getDeclaredField("clr");
          clrField.setAccessible(true);
          clr = (ClassLoaderResolver)clrField.get(baseStore);
          clearClr(clr);
        }
      }
      Field classLoaderResolverMap = AbstractNucleusContext.class.getDeclaredField(
          "classLoaderResolverMap");
      classLoaderResolverMap.setAccessible(true);
      Map<String,ClassLoaderResolver> loaderMap =
          (Map<String, ClassLoaderResolver>) classLoaderResolverMap.get(nc);
      for (ClassLoaderResolver clr : loaderMap.values()){
        clearClr(clr);
      }
      classLoaderResolverMap.set(nc, new HashMap<String, ClassLoaderResolver>());
      LOG.debug("Removed cached classloaders from DataNucleus NucleusContext");
    } catch (Exception e) {
      LOG.warn("Failed to remove cached classloaders from DataNucleus NucleusContext ", e);
    }
  }

  private static void clearClr(ClassLoaderResolver clr) throws Exception {
    if (clr != null){
      if (clr instanceof ClassLoaderResolverImpl){
        ClassLoaderResolverImpl clri = (ClassLoaderResolverImpl) clr;
        long resourcesCleared = clearFieldMap(clri,"resources");
        long loadedClassesCleared = clearFieldMap(clri,"loadedClasses");
        long unloadedClassesCleared = clearFieldMap(clri, "unloadedClasses");
        LOG.debug("Cleared ClassLoaderResolverImpl: " +
            resourcesCleared + "," + loadedClassesCleared + "," + unloadedClassesCleared);
      }
    }
  }
  private static long clearFieldMap(ClassLoaderResolverImpl clri, String mapFieldName) throws Exception {
    Field mapField = ClassLoaderResolverImpl.class.getDeclaredField(mapFieldName);
    mapField.setAccessible(true);

    Map<String,Class> map = (Map<String, Class>) mapField.get(clri);
    long sz = map.size();
    mapField.set(clri, Collections.synchronizedMap(new WeakValueMap()));
    return sz;
  }


  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name) throws MetaException {
    try {
      return getPrimaryKeysInternal(db_name, tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(e.getMessage());
    }
  }

  protected List<SQLPrimaryKey> getPrimaryKeysInternal(final String db_name,
    final String tbl_name,
    boolean allowSql, boolean allowJdo)
  throws MetaException, NoSuchObjectException {
    return new GetListHelper<SQLPrimaryKey>(db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLPrimaryKey> getSqlResult(GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException {
        return directSql.getPrimaryKeys(db_name, tbl_name);
      }

      @Override
      protected List<SQLPrimaryKey> getJdoResult(
        GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException, NoSuchObjectException {
        return getPrimaryKeysViaJdo(db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLPrimaryKey> getPrimaryKeysViaJdo(String db_name, String tbl_name) throws MetaException {
    boolean commited = false;
    List<SQLPrimaryKey> primaryKeys = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name);
      pm.retrieveAll(constraints);
      primaryKeys = new ArrayList<SQLPrimaryKey>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        int enableValidateRely = currPK.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        primaryKeys.add(new SQLPrimaryKey(db_name,
         tbl_name,
         currPK.getParentColumn().getCols().get(currPK.getParentIntegerIndex()).getName(),
         currPK.getPosition(),
         currPK.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return primaryKeys;
  }

  private String getPrimaryKeyConstraintName(String db_name, String tbl_name) throws MetaException {
    boolean commited = false;
    String ret = null;
    Query query = null;

    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name);
      pm.retrieveAll(constraints);
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        ret = currPK.getConstraintName();
        break;
      }
      commited = commitTransaction();
     } finally {
       if (!commited) {
        rollbackTransaction();
       }
       if (query != null) {
        query.closeAll();
       }
     }
     return ret;
   }

  @Override
  public List<SQLForeignKey> getForeignKeys(String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name) throws MetaException {
    try {
      return getForeignKeysInternal(parent_db_name,
        parent_tbl_name, foreign_db_name, foreign_tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(e.getMessage());
    }
  }

  protected List<SQLForeignKey> getForeignKeysInternal(final String parent_db_name,
    final String parent_tbl_name, final String foreign_db_name, final String foreign_tbl_name,
    boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    return new GetListHelper<SQLForeignKey>(foreign_db_name, foreign_tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLForeignKey> getSqlResult(GetHelper<List<SQLForeignKey>> ctx) throws MetaException {
        return directSql.getForeignKeys(parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }

      @Override
      protected List<SQLForeignKey> getJdoResult(
        GetHelper<List<SQLForeignKey>> ctx) throws MetaException, NoSuchObjectException {
        return getForeignKeysViaJdo(parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }
    }.run(false);
  }

  private List<SQLForeignKey> getForeignKeysViaJdo(String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name) throws MetaException {
    boolean commited = false;
    List<SQLForeignKey> foreignKeys = null;
    Collection<?> constraints = null;
    Query query = null;
    Map<String, String> tblToConstraint = new HashMap<String, String>();
    try {
      openTransaction();
      String queryText = (parent_tbl_name != null ? "parentTable.tableName == parent_tbl_name && " : "")
        + (parent_db_name != null ? " parentTable.database.name == parent_db_name && " : "")
        + (foreign_tbl_name != null ? " childTable.tableName == foreign_tbl_name && " : "")
        + (foreign_db_name != null ? " childTable.database.name == foreign_db_name && " : "")
        + " constraintType == MConstraint.FOREIGN_KEY_CONSTRAINT";
      queryText = queryText.trim();
      query = pm.newQuery(MConstraint.class, queryText);
      String paramText = (parent_tbl_name == null ? "" : "java.lang.String parent_tbl_name,")
        + (parent_db_name == null ? "" : " java.lang.String parent_db_name, ")
        + (foreign_tbl_name == null ? "" : "java.lang.String foreign_tbl_name,")
        + (foreign_db_name == null ? "" : " java.lang.String foreign_db_name");
      paramText=paramText.trim();
      if (paramText.endsWith(",")) {
        paramText = paramText.substring(0, paramText.length()-1);
      }
      query.declareParameters(paramText);
      List<String> params = new ArrayList<String>();
      if (parent_tbl_name != null) {
        params.add(parent_tbl_name);
      }
      if (parent_db_name != null) {
        params.add(parent_db_name);
      }
      if (foreign_tbl_name != null) {
        params.add(foreign_tbl_name);
      }
      if (foreign_db_name != null) {
        params.add(foreign_db_name);
      }
      if (params.size() == 0) {
        constraints = (Collection<?>) query.execute();
      } else if (params.size() ==1) {
        constraints = (Collection<?>) query.execute(params.get(0));
      } else if (params.size() == 2) {
        constraints = (Collection<?>) query.execute(params.get(0), params.get(1));
      } else if (params.size() == 3) {
        constraints = (Collection<?>) query.execute(params.get(0), params.get(1), params.get(2));
      } else {
        constraints = (Collection<?>) query.executeWithArray(params.get(0), params.get(1),
          params.get(2), params.get(3));
      }
      pm.retrieveAll(constraints);
      foreignKeys = new ArrayList<SQLForeignKey>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPKFK = (MConstraint) i.next();
        int enableValidateRely = currPKFK.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        String consolidatedtblName =
          currPKFK.getParentTable().getDatabase().getName() + "." +
          currPKFK.getParentTable().getTableName();
        String pkName;
        if (tblToConstraint.containsKey(consolidatedtblName)) {
          pkName = tblToConstraint.get(consolidatedtblName);
        } else {
          pkName = getPrimaryKeyConstraintName(currPKFK.getParentTable().getDatabase().getName(),
            currPKFK.getParentTable().getDatabase().getName());
          tblToConstraint.put(consolidatedtblName, pkName);
        }
        foreignKeys.add(new SQLForeignKey(
          currPKFK.getParentTable().getDatabase().getName(),
          currPKFK.getParentTable().getDatabase().getName(),
          currPKFK.getParentColumn().getCols().get(currPKFK.getParentIntegerIndex()).getName(),
          currPKFK.getChildTable().getDatabase().getName(),
          currPKFK.getChildTable().getTableName(),
          currPKFK.getChildColumn().getCols().get(currPKFK.getChildIntegerIndex()).getName(),
          currPKFK.getPosition(),
          currPKFK.getUpdateRule(),
          currPKFK.getDeleteRule(),
          currPKFK.getConstraintName(), pkName, enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return foreignKeys;
  }

  @Override
  public void dropConstraint(String dbName, String tableName,
    String constraintName) throws NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();

      List<MConstraint> tabConstraints = listAllTableConstraintsWithOptionalConstraintName(
                                         dbName, tableName, constraintName);
      if (tabConstraints != null && tabConstraints.size() > 0) {
        pm.deletePersistentAll(tabConstraints);
      } else {
        throw new NoSuchObjectException("The constraint: " + constraintName +
          " does not exist for the associated table: " + dbName + "." + tableName);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

}
