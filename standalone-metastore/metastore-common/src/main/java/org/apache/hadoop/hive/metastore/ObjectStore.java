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

import static org.apache.commons.lang.StringUtils.join;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
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
import javax.jdo.datastore.JDOConnection;
import javax.jdo.identity.IntIdentity;
import javax.sql.DataSource;

import com.google.common.base.Strings;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.MetaStoreDirectSql.SqlFilterForPushdown;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaCompatibility;
import org.apache.hadoop.hive.metastore.api.SchemaType;
import org.apache.hadoop.hive.metastore.api.SchemaValidation;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SerdeType;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProvider;
import org.apache.hadoop.hive.metastore.datasource.DataSourceProviderFactory;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.model.MCatalog;
import org.apache.hadoop.hive.metastore.model.MColumnDescriptor;
import org.apache.hadoop.hive.metastore.model.MConstraint;
import org.apache.hadoop.hive.metastore.model.MCreationMetadata;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDatabase;
import org.apache.hadoop.hive.metastore.model.MDelegationToken;
import org.apache.hadoop.hive.metastore.model.MFieldSchema;
import org.apache.hadoop.hive.metastore.model.MFunction;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MISchema;
import org.apache.hadoop.hive.metastore.model.MMasterKey;
import org.apache.hadoop.hive.metastore.model.MMetastoreDBProperties;
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
import org.apache.hadoop.hive.metastore.model.MRuntimeStat;
import org.apache.hadoop.hive.metastore.model.MSchemaVersion;
import org.apache.hadoop.hive.metastore.model.MSerDeInfo;
import org.apache.hadoop.hive.metastore.model.MStorageDescriptor;
import org.apache.hadoop.hive.metastore.model.MStringList;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MType;
import org.apache.hadoop.hive.metastore.model.MVersionTable;
import org.apache.hadoop.hive.metastore.model.MWMMapping;
import org.apache.hadoop.hive.metastore.model.MWMMapping.EntityType;
import org.apache.hadoop.hive.metastore.model.MWMPool;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan;
import org.apache.hadoop.hive.metastore.model.MWMResourcePlan.Status;
import org.apache.hadoop.hive.metastore.model.MWMTrigger;
import org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree.FilterBuilder;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;
import org.datanucleus.AbstractNucleusContext;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassLoaderResolverImpl;
import org.datanucleus.NucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.store.rdbms.exceptions.MissingTableException;
import org.datanucleus.store.scostore.Store;
import org.datanucleus.util.WeakValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


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
  private static boolean forTwoMetastoreTesting = false;
  private int batchSize = Batchable.NO_BATCHING;

  private static final DateTimeFormatter YMDHMS_FORMAT = DateTimeFormatter.ofPattern(
      "yyyy_MM_dd_HH_mm_ss");

  private static Lock pmfPropLock = new ReentrantLock();
  /**
  * Verify the schema only once per JVM since the db connection info is static
  */
  private final static AtomicBoolean isSchemaVerified = new AtomicBoolean(false);
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStore.class);

  private enum TXN_STATUS {
    NO_STATE, OPEN, COMMITED, ROLLBACK
  }

  private static final Map<String, Class<?>> PINCLASSMAP;
  private static final String HOSTNAME;
  private static final String USER;
  private static final String JDO_PARAM = ":param";
  static {
    Map<String, Class<?>> map = new HashMap<>();
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
    USER = org.apache.commons.lang.StringUtils.defaultString(user, "UNKNOWN");
  }


  private boolean isInitialized = false;
  private PersistenceManager pm = null;
  private SQLGenerator sqlGenerator = null;
  private MetaStoreDirectSql directSql = null;
  private DatabaseProduct dbType = null;
  private PartitionExpressionProxy expressionProxy = null;
  private Configuration conf;
  private volatile int openTrasactionCalls = 0;
  private Transaction currentTransaction = null;
  private TXN_STATUS transactionStatus = TXN_STATUS.NO_STATE;
  private Pattern partitionValidationPattern;
  private Counter directSqlErrors;

  /**
   * A Autocloseable wrapper around Query class to pass the Query object to the caller and let the caller release
   * the resources when the QueryWrapper goes out of scope
   */
  public static class QueryWrapper implements AutoCloseable {
    public Query query;

    /**
     * Explicitly closes the query object to release the resources
     */
    @Override
    public void close() {
      if (query != null) {
        query.closeAll();
        query = null;
      }
    }
  }

  public ObjectStore() {
  }

  @Override
  public Configuration getConf() {
    return conf;
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
      this.conf = conf;
      configureSSL(conf);
      Properties propsFromConf = getDataSourceProps(conf);
      boolean propsChanged = !propsFromConf.equals(prop);

      if (propsChanged) {
        if (pmf != null){
          clearOutPmfClassLoaderCache(pmf);
          if (!forTwoMetastoreTesting) {
            // close the underlying connection pool to avoid leaks
            pmf.close();
          }
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
          MetastoreConf.getVar(this.conf, ConfVars.PARTITION_NAME_WHITELIST_PATTERN);
      if (partitionValidationRegex != null && !partitionValidationRegex.isEmpty()) {
        partitionValidationPattern = Pattern.compile(partitionValidationRegex);
      } else {
        partitionValidationPattern = null;
      }

      // Note, if metrics have not been initialized this will return null, which means we aren't
      // using metrics.  Thus we should always check whether this is non-null before using.
      MetricRegistry registry = Metrics.getRegistry();
      if (registry != null) {
        directSqlErrors = Metrics.getOrCreateCounter(MetricsConstants.DIRECTSQL_ERRORS);
      }

      this.batchSize = MetastoreConf.getIntVar(conf, ConfVars.RAWSTORE_PARTITION_BATCH_SIZE);

      if (!isInitialized) {
        throw new RuntimeException(
        "Unable to create persistence manager. Check dss.log for details");
      } else {
        LOG.debug("Initialized ObjectStore");
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
    int retryLimit = MetastoreConf.getIntVar(conf, ConfVars.HMS_HANDLER_ATTEMPTS);
    long retryInterval = MetastoreConf.getTimeVar(conf,
        ConfVars.HMS_HANDLER_INTERVAL, TimeUnit.MILLISECONDS);
    int numTries = retryLimit;

    while (numTries > 0){
      try {
        initializeHelper(dsProps);
        return; // If we reach here, we succeed.
      } catch (Exception e){
        numTries--;
        boolean retriable = isRetriableException(e);
        if ((numTries > 0) && retriable){
          LOG.info("Retriable exception while instantiating ObjectStore, retrying. " +
              "{} tries left", numTries, e);
          try {
            Thread.sleep(retryInterval);
          } catch (InterruptedException ie) {
            // Restore the interrupted status, since we do not want to catch it.
            LOG.debug("Interrupted while sleeping before retrying.", ie);
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
      new HashSet<>(Arrays.asList(JDOCanRetryException.class));
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
    LOG.debug("ObjectStore, initialize called");
    prop = dsProps;
    pm = getPersistenceManager();
    try {
      String productName = MetaStoreDirectSql.getProductName(pm);
      sqlGenerator = new SQLGenerator(DatabaseProduct.determineDatabaseProduct(productName), conf);
    } catch (SQLException e) {
      LOG.error("error trying to figure out the database product", e);
      throw new RuntimeException(e);
    }
    isInitialized = pm != null;
    if (isInitialized) {
      dbType = determineDatabaseProduct();
      expressionProxy = createExpressionProxy(conf);
      if (MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL)) {
        String schema = prop.getProperty("javax.jdo.mapping.Schema");
        schema = org.apache.commons.lang.StringUtils.defaultIfBlank(schema, null);
        directSql = new MetaStoreDirectSql(pm, conf, schema);
      }
    }
    LOG.debug("RawStore: {}, with PersistenceManager: {}" +
        " created in the thread with id: {}", this, pm, Thread.currentThread().getId());
  }

  private DatabaseProduct determineDatabaseProduct() {
    try {
      return DatabaseProduct.determineDatabaseProduct(getProductName(pm));
    } catch (SQLException e) {
      LOG.warn("Cannot determine database product; assuming OTHER", e);
      return DatabaseProduct.OTHER;
    }
  }

  private static String getProductName(PersistenceManager pm) {
    JDOConnection jdoConn = pm.getDataStoreConnection();
    try {
      return ((Connection)jdoConn.getNativeConnection()).getMetaData().getDatabaseProductName();
    } catch (Throwable t) {
      LOG.warn("Error retrieving product name", t);
      return null;
    } finally {
      jdoConn.close(); // We must release the connection before we call other pm methods.
    }
  }

  /**
   * Creates the proxy used to evaluate expressions. This is here to prevent circular
   * dependency - ql -&gt; metastore client &lt;-&gt metastore server -&gt ql. If server and
   * client are split, this can be removed.
   * @param conf Configuration.
   * @return The partition expression proxy.
   */
  private static PartitionExpressionProxy createExpressionProxy(Configuration conf) {
    String className = MetastoreConf.getVar(conf, ConfVars.EXPRESSION_PROXY_CLASS);
    try {
      Class<? extends PartitionExpressionProxy> clazz =
           JavaUtils.getClass(className, PartitionExpressionProxy.class);
      return JavaUtils.newInstance(clazz, new Class<?>[0], new Object[0]);
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
    String sslPropString = MetastoreConf.getVar(conf, ConfVars.DBACCESS_SSL_PROPS);
    if (org.apache.commons.lang.StringUtils.isNotEmpty(sslPropString)) {
      LOG.info("Metastore setting SSL properties of the connection to backed DB");
      for (String sslProp : sslPropString.split(",")) {
        String[] pair = sslProp.trim().split("=");
        if (pair != null && pair.length == 2) {
          System.setProperty(pair[0].trim(), pair[1].trim());
        } else {
          LOG.warn("Invalid metastore property value for {}", ConfVars.DBACCESS_SSL_PROPS);
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

    // First, go through and set all our values for datanucleus and javax.jdo parameters.  This
    // has to be a separate first step because we don't set the default values in the config object.
    for (ConfVars var : MetastoreConf.dataNucleusAndJdoConfs) {
      String confVal = MetastoreConf.getAsString(conf, var);
      String varName = var.getVarname();
      Object prevVal = prop.setProperty(varName, confVal);
      if (MetastoreConf.isPrintable(varName)) {
        LOG.debug("Overriding {} value {} from jpox.properties with {}",
          varName, prevVal, confVal);
      }
    }

    // Now, we need to look for any values that the user set that MetastoreConf doesn't know about.
    // TODO Commenting this out for now, as it breaks because the conf values aren't getting properly
    // interpolated in case of variables.  See HIVE-17788.
    /*
    for (Map.Entry<String, String> e : conf) {
      if (e.getKey().startsWith("datanucleus.") || e.getKey().startsWith("javax.jdo.")) {
        // We have to handle this differently depending on whether it is a value known to
        // MetastoreConf or not.  If it is, we need to get the default value if a value isn't
        // provided.  If not, we just set whatever the user has set.
        Object prevVal = prop.setProperty(e.getKey(), e.getValue());
        if (LOG.isDebugEnabled() && MetastoreConf.isPrintable(e.getKey())) {
          LOG.debug("Overriding " + e.getKey() + " value " + prevVal
              + " from  jpox.properties with " + e.getValue());
        }
      }
    }
    */

    // Password may no longer be in the conf, use getPassword()
    try {
      String passwd = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
      if (org.apache.commons.lang.StringUtils.isNotEmpty(passwd)) {
        // We can get away with the use of varname here because varname == hiveName for PWD
        prop.setProperty(ConfVars.PWD.getVarname(), passwd);
      }
    } catch (IOException err) {
      throw new RuntimeException("Error getting metastore password: " + err.getMessage(), err);
    }

    if (LOG.isDebugEnabled()) {
      for (Entry<Object, Object> e : prop.entrySet()) {
        if (MetastoreConf.isPrintable(e.getKey().toString())) {
          LOG.debug("{} = {}", e.getKey(), e.getValue());
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
    if (!autoStartIgnore.equalsIgnoreCase(currentAutoStartVal)) {
      LOG.warn("{} is set to unsupported value {} . Setting it to value: {}", autoStartKey,
        conf.get(autoStartKey), autoStartIgnore);
    }
    conf.set(autoStartKey, autoStartIgnore);
  }

  private static synchronized PersistenceManagerFactory getPMF() {
    if (pmf == null) {

      Configuration conf = MetastoreConf.newMetastoreConf();
      DataSourceProvider dsp = DataSourceProviderFactory.hasProviderSpecificConfigurations(conf) ?
              DataSourceProviderFactory.getDataSourceProvider(conf) : null;

      if (dsp == null) {
        pmf = JDOHelper.getPersistenceManagerFactory(prop);
      } else {
        try {
          DataSource ds = dsp.create(conf);
          Map<Object, Object> dsProperties = new HashMap<>();
          //Any preexisting datanucleus property should be passed along
          dsProperties.putAll(prop);
          dsProperties.put(PropertyNames.PROPERTY_CONNECTION_FACTORY, ds);
          dsProperties.put(PropertyNames.PROPERTY_CONNECTION_FACTORY2, ds);
          dsProperties.put("javax.jdo.PersistenceManagerFactoryClass",
              "org.datanucleus.api.jdo.JDOPersistenceManagerFactory");
          pmf = JDOHelper.getPersistenceManagerFactory(dsProperties);
        } catch (SQLException e) {
          LOG.warn("Could not create PersistenceManagerFactory using " +
              "connection pool properties, will fall back", e);
          pmf = JDOHelper.getPersistenceManagerFactory(prop);
        }
      }
      DataStoreCache dsc = pmf.getDataStoreCache();
      if (dsc != null) {
        String objTypes = MetastoreConf.getVar(conf, ConfVars.CACHE_PINOBJTYPES);
        LOG.info("Setting MetaStore object pin classes with hive.metastore.cache.pinobjtypes=\"{}\"", objTypes);
        if (org.apache.commons.lang.StringUtils.isNotEmpty(objTypes)) {
          String[] typeTokens = objTypes.toLowerCase().split(",");
          for (String type : typeTokens) {
            type = type.trim();
            if (PINCLASSMAP.containsKey(type)) {
              dsc.pinAll(true, PINCLASSMAP.get(type));
            } else {
              LOG.warn("{} is not one of the pinnable object types: {}", type,
                org.apache.commons.lang.StringUtils.join(PINCLASSMAP.keySet(), " "));
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
    LOG.debug("RawStore: {}, with PersistenceManager: {} will be shutdown", this, pm);
    if (pm != null) {
      pm.close();
      pm = null;
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
  @Override
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
  public void createCatalog(Catalog cat) throws MetaException {
    LOG.debug("Creating catalog " + cat.getName());
    boolean committed = false;
    MCatalog mCat = catToMCat(cat);
    try {
      openTransaction();
      pm.makePersistent(mCat);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void alterCatalog(String catName, Catalog cat)
      throws MetaException, InvalidOperationException {
    if (!cat.getName().equals(catName)) {
      throw new InvalidOperationException("You cannot change a catalog's name");
    }
    boolean committed = false;
    try {
      MCatalog mCat = getMCatalog(catName);
      if (org.apache.commons.lang.StringUtils.isNotBlank(cat.getLocationUri())) {
        mCat.setLocationUri(cat.getLocationUri());
      }
      if (org.apache.commons.lang.StringUtils.isNotBlank(cat.getDescription())) {
        mCat.setDescription(cat.getDescription());
      }
      openTransaction();
      pm.makePersistent(mCat);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    LOG.debug("Fetching catalog " + catalogName);
    MCatalog mCat = getMCatalog(catalogName);
    if (mCat == null) {
      throw new NoSuchObjectException("No catalog " + catalogName);
    }
    return mCatToCat(mCat);
  }

  @Override
  public List<String> getCatalogs() throws MetaException {
    LOG.debug("Fetching all catalog names");
    boolean commited = false;
    List<String> catalogs = null;

    String queryStr = "select name from org.apache.hadoop.hive.metastore.model.MCatalog";
    Query query = null;

    openTransaction();
    try {
      query = pm.newQuery(queryStr);
      query.setResult("name");
      catalogs = new ArrayList<>((Collection<String>) query.execute());
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    Collections.sort(catalogs);
    return catalogs;
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    LOG.debug("Dropping catalog " + catalogName);
    boolean committed = false;
    try {
      openTransaction();
      MCatalog mCat = getMCatalog(catalogName);
      pm.retrieve(mCat);
      if (mCat == null) {
        throw new NoSuchObjectException("No catalog " + catalogName);
      }
      pm.deletePersistent(mCat);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private MCatalog getMCatalog(String catalogName) throws MetaException {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      catalogName = normalizeIdentifier(catalogName);
      query = pm.newQuery(MCatalog.class, "name == catname");
      query.declareParameters("java.lang.String catname");
      query.setUnique(true);
      MCatalog mCat = (MCatalog)query.execute(catalogName);
      pm.retrieve(mCat);
      committed = commitTransaction();
      return mCat;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  private MCatalog catToMCat(Catalog cat) {
    MCatalog mCat = new MCatalog();
    mCat.setName(normalizeIdentifier(cat.getName()));
    if (cat.isSetDescription()) {
      mCat.setDescription(cat.getDescription());
    }
    mCat.setLocationUri(cat.getLocationUri());
    return mCat;
  }

  private Catalog mCatToCat(MCatalog mCat) {
    Catalog cat = new Catalog(mCat.getName(), mCat.getLocationUri());
    if (mCat.getDescription() != null) {
      cat.setDescription(mCat.getDescription());
    }
    return cat;
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commited = false;
    MDatabase mdb = new MDatabase();
    assert db.getCatalogName() != null;
    mdb.setCatalogName(normalizeIdentifier(db.getCatalogName()));
    assert mdb.getCatalogName() != null;
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
  private MDatabase getMDatabase(String catName, String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      name = normalizeIdentifier(name);
      catName = normalizeIdentifier(catName);
      query = pm.newQuery(MDatabase.class, "name == dbname && catalogName == catname");
      query.declareParameters("java.lang.String dbname, java.lang.String catname");
      query.setUnique(true);
      mdb = (MDatabase) query.execute(name, catName);
      pm.retrieve(mdb);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    if (mdb == null) {
      throw new NoSuchObjectException("There is no database " + catName + "." + name);
    }
    return mdb;
  }

  @Override
  public Database getDatabase(String catalogName, String name) throws NoSuchObjectException {
    MetaException ex = null;
    Database db = null;
    try {
      db = getDatabaseInternal(catalogName, name);
    } catch (MetaException e) {
      // Signature restriction to NSOE, and NSOE being a flat exception prevents us from
      // setting the cause of the NSOE as the MetaException. We should not lose the info
      // we got here, but it's very likely that the MetaException is irrelevant and is
      // actually an NSOE message, so we should log it and throw an NSOE with the msg.
      ex = e;
    }
    if (db == null) {
      LOG.warn("Failed to get database {}.{}, returning NoSuchObjectException",
          catalogName, name, ex);
      throw new NoSuchObjectException(name + (ex == null ? "" : (": " + ex.getMessage())));
    }
    return db;
  }

  public Database getDatabaseInternal(String catalogName, String name)
      throws MetaException, NoSuchObjectException {
    return new GetDbHelper(catalogName, name, true, true) {
      @Override
      protected Database getSqlResult(GetHelper<Database> ctx) throws MetaException {
        return directSql.getDatabase(catalogName, dbName);
      }

      @Override
      protected Database getJdoResult(GetHelper<Database> ctx) throws MetaException, NoSuchObjectException {
        return getJDODatabase(catalogName, dbName);
      }
    }.run(false);
   }

  public Database getJDODatabase(String catName, String name) throws NoSuchObjectException {
    MDatabase mdb = null;
    boolean commited = false;
    try {
      openTransaction();
      mdb = getMDatabase(catName, name);
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
    String type = org.apache.commons.lang.StringUtils.defaultIfBlank(mdb.getOwnerType(), null);
    PrincipalType principalType = (type == null) ? null : PrincipalType.valueOf(type);
    db.setOwnerType(principalType);
    db.setCatalogName(catName);
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
  public boolean alterDatabase(String catName, String dbName, Database db)
    throws MetaException, NoSuchObjectException {

    MDatabase mdb = null;
    boolean committed = false;
    try {
      mdb = getMDatabase(catName, dbName);
      mdb.setParameters(db.getParameters());
      mdb.setOwnerName(db.getOwnerName());
      if (db.getOwnerType() != null) {
        mdb.setOwnerType(db.getOwnerType().name());
      }
      if (org.apache.commons.lang.StringUtils.isNotBlank(db.getDescription())) {
        mdb.setDescription(db.getDescription());
      }
      if (org.apache.commons.lang.StringUtils.isNotBlank(db.getLocationUri())) {
        mdb.setLocationUri(db.getLocationUri());
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
  public boolean dropDatabase(String catName, String dbname)
      throws NoSuchObjectException, MetaException {
    boolean success = false;
    LOG.info("Dropping database {}.{} along with all tables", catName, dbname);
    dbname = normalizeIdentifier(dbname);
    catName = normalizeIdentifier(catName);
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      openTransaction();

      // then drop the database
      MDatabase db = getMDatabase(catName, dbname);
      pm.retrieve(db);
      if (db != null) {
        List<MDBPrivilege> dbGrants = this.listDatabaseGrants(catName, dbname, null, queryWrapper);
        if (CollectionUtils.isNotEmpty(dbGrants)) {
          pm.deletePersistentAll(dbGrants);
        }
        pm.deletePersistent(db);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, queryWrapper);
    }
    return success;
  }

  @Override
  public List<String> getDatabases(String catName, String pattern) throws MetaException {
    if (pattern == null || pattern.equals("*")) {
      return getAllDatabases(catName);
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
      appendSimpleCondition(filterBuilder, "catalogName", new String[] {catName}, parameterVals);
      appendPatternCondition(filterBuilder, "name", subpatterns, parameterVals);
      query = pm.newQuery(MDatabase.class, filterBuilder.toString());
      query.setResult("name");
      query.setOrdering("name ascending");
      Collection<String> names = (Collection<String>) query.executeWithArray(parameterVals.toArray(new String[0]));
      databases = new ArrayList<>(names);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return databases;
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException {
    boolean commited = false;
    List<String> databases = null;

    Query query = null;
    catName = normalizeIdentifier(catName);

    openTransaction();
    try {
      query = pm.newQuery("select name from org.apache.hadoop.hive.metastore.model.MDatabase " +
          "where catalogName == catname");
      query.declareParameters("java.lang.String catname");
      query.setResult("name");
      databases = new ArrayList<>((Collection<String>) query.execute(catName));
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    Collections.sort(databases);
    return databases;
  }

  private MType getMType(Type type) {
    List<MFieldSchema> fields = new ArrayList<>();
    if (type.getFields() != null) {
      for (FieldSchema field : type.getFields()) {
        fields.add(new MFieldSchema(field.getName(), field.getType(), field
            .getComment()));
      }
    }
    return new MType(type.getName(), type.getType1(), type.getType2(), fields);
  }

  private Type getType(MType mtype) {
    List<FieldSchema> fields = new ArrayList<>();
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
      rollbackAndCleanup(commited, query);
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
      LOG.debug("type not found {}", typeName, e);
    } finally {
      rollbackAndCleanup(success, query);
    }
    return success;
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints)
    throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      openTransaction();
      createTable(tbl);
      // Add constraints.
      // We need not do a deep retrieval of the Table Column Descriptor while persisting the
      // constraints since this transaction involving create table is not yet committed.
      List<String> constraintNames = new ArrayList<>();
      if (foreignKeys != null) {
        constraintNames.addAll(addForeignKeys(foreignKeys, false, primaryKeys, uniqueConstraints));
      }
      if (primaryKeys != null) {
        constraintNames.addAll(addPrimaryKeys(primaryKeys, false));
      }
      if (uniqueConstraints != null) {
        constraintNames.addAll(addUniqueConstraints(uniqueConstraints, false));
      }
      if (notNullConstraints != null) {
        constraintNames.addAll(addNotNullConstraints(notNullConstraints, false));
      }
      if (defaultConstraints != null) {
        constraintNames.addAll(addDefaultConstraints(defaultConstraints, false));
      }
      if (checkConstraints != null) {
        constraintNames.addAll(addCheckConstraints(checkConstraints, false));
      }
      success = commitTransaction();
      return constraintNames;
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

      if (tbl.getCreationMetadata() != null) {
        MCreationMetadata mcm = convertToMCreationMetadata(tbl.getCreationMetadata());
        pm.makePersistent(mcm);
      }

      PrincipalPrivilegeSet principalPrivs = tbl.getPrivileges();
      List<Object> toPersistPrivObjs = new ArrayList<>();
      if (principalPrivs != null) {
        int now = (int)(System.currentTimeMillis()/1000);

        Map<String, List<PrivilegeGrantInfo>> userPrivs = principalPrivs.getUserPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, userPrivs, PrincipalType.USER, "SQL");

        Map<String, List<PrivilegeGrantInfo>> groupPrivs = principalPrivs.getGroupPrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, groupPrivs, PrincipalType.GROUP, "SQL");

        Map<String, List<PrivilegeGrantInfo>> rolePrivs = principalPrivs.getRolePrivileges();
        putPersistentPrivObjects(mtbl, toPersistPrivObjs, now, rolePrivs, PrincipalType.ROLE, "SQL");
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
      int now, Map<String, List<PrivilegeGrantInfo>> privMap, PrincipalType type, String authorizer) {
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
                  .isGrantOption(), authorizer);
          toPersistPrivObjs.add(mTblSec);
        }
      }
    }
  }

  @Override
  public boolean dropTable(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean materializedView = false;
    boolean success = false;
    try {
      openTransaction();
      MTable tbl = getMTable(catName, dbName, tableName);
      pm.retrieve(tbl);
      if (tbl != null) {
        materializedView = TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType());
        // first remove all the grants
        List<MTablePrivilege> tabGrants = listAllTableGrants(catName, dbName, tableName);
        if (CollectionUtils.isNotEmpty(tabGrants)) {
          pm.deletePersistentAll(tabGrants);
        }
        List<MTableColumnPrivilege> tblColGrants = listTableAllColumnGrants(catName, dbName,
            tableName);
        if (CollectionUtils.isNotEmpty(tblColGrants)) {
          pm.deletePersistentAll(tblColGrants);
        }

        List<MPartitionPrivilege> partGrants = this.listTableAllPartitionGrants(catName, dbName, tableName);
        if (CollectionUtils.isNotEmpty(partGrants)) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColGrants = listTableAllPartitionColumnGrants(catName, dbName,
            tableName);
        if (CollectionUtils.isNotEmpty(partColGrants)) {
          pm.deletePersistentAll(partColGrants);
        }
        // delete column statistics if present
        try {
          deleteTableColumnStatistics(catName, dbName, tableName, null);
        } catch (NoSuchObjectException e) {
          LOG.info("Found no table level column statistics associated with {} to delete",
              TableName.getQualified(catName, dbName, tableName));
        }

        List<MConstraint> tabConstraints = listAllTableConstraintsWithOptionalConstraintName(
                                           catName, dbName, tableName, null);
        if (CollectionUtils.isNotEmpty(tabConstraints)) {
          pm.deletePersistentAll(tabConstraints);
        }

        preDropStorageDescriptor(tbl.getSd());

        if (materializedView) {
          dropCreationMetadata(tbl.getDatabase().getCatalogName(),
              tbl.getDatabase().getName(), tbl.getTableName());
        }

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

  private boolean dropCreationMetadata(String catName, String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    try {
      openTransaction();
      MCreationMetadata mcm = getCreationMetadata(catName, dbName, tableName);
      pm.retrieve(mcm);
      if (mcm != null) {
        pm.deletePersistentAll(mcm);
      }
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return success;
  }

  private List<MConstraint> listAllTableConstraintsWithOptionalConstraintName(
      String catName, String dbName, String tableName, String constraintname) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    constraintname = constraintname!=null?normalizeIdentifier(constraintname):null;
    List<MConstraint> mConstraints = null;
    List<String> constraintNames = new ArrayList<>();
    Query query = null;

    try {
      query = pm.newQuery("select constraintName from org.apache.hadoop.hive.metastore.model.MConstraint  where "
        + "((parentTable.tableName == ptblname && parentTable.database.name == pdbname && " +
              "parentTable.database.catalogName == pcatname) || "
        + "(childTable != null && childTable.tableName == ctblname &&" +
              "childTable.database.name == cdbname && childTable.database.catalogName == ccatname)) " +
          (constraintname != null ? " && constraintName == constraintname" : ""));
      query.declareParameters("java.lang.String ptblname, java.lang.String pdbname,"
          + "java.lang.String pcatname, java.lang.String ctblname, java.lang.String cdbname," +
          "java.lang.String ccatname" +
        (constraintname != null ? ", java.lang.String constraintname" : ""));
      Collection<?> constraintNamesColl =
        constraintname != null ?
          ((Collection<?>) query.
            executeWithArray(tableName, dbName, catName, tableName, dbName, catName, constraintname)):
          ((Collection<?>) query.
            executeWithArray(tableName, dbName, catName, tableName, dbName, catName));
      for (Iterator<?> i = constraintNamesColl.iterator(); i.hasNext();) {
        String currName = (String) i.next();
        constraintNames.add(currName);
      }
      query = pm.newQuery(MConstraint.class);
      query.setFilter("param.contains(constraintName)");
      query.declareParameters("java.util.Collection param");
      Collection<?> constraints = (Collection<?>)query.execute(constraintNames);
      mConstraints = new ArrayList<>();
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
  public Table getTable(String catName, String dbName, String tableName) throws MetaException {
    boolean commited = false;
    Table tbl = null;
    try {
      openTransaction();
      tbl = convertToTable(getMTable(catName, dbName, tableName));
      // Retrieve creation metadata if needed
      if (tbl != null && TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType())) {
        tbl.setCreationMetadata(
            convertToCreationMetadata(getCreationMetadata(catName, dbName, tableName)));
      }
      commited = commitTransaction();
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
    return tbl;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern)
      throws MetaException {
    return getTables(catName, dbName, pattern, null);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType)
      throws MetaException {
    try {
      // We only support pattern matching via jdo since pattern matching in Java
      // might be different than the one used by the metastore backends
      return getTablesInternal(catName, dbName, pattern, tableType,
          (pattern == null || pattern.equals(".*")), true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName>(null, null, null, true, false) {
      @Override
      protected List<TableName> getSqlResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        return directSql.getTableNamesWithStats();
      }

      @Override
      protected List<TableName> getJdoResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }
    }.run(false);
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return new GetHelper<Map<String, List<String>>>(catName, dbName, null, true, false) {
      @Override
      protected Map<String, List<String>> getSqlResult(
          GetHelper<Map<String, List<String>>> ctx) throws MetaException {
        try {
          return directSql.getColAndPartNamesWithStats(catName, dbName, tableName);
        } catch (Throwable ex) {
          LOG.error("DirectSQL failed", ex);
          throw new MetaException(ex.getMessage());
        }
      }

      @Override
      protected Map<String, List<String>> getJdoResult(
          GetHelper<Map<String, List<String>>> ctx) throws MetaException {
        throw new UnsupportedOperationException("UnsupportedOperationException"); // TODO: implement?
      }

      @Override
      protected String describeResult() {
        return results.size() + " partitions";
      }
    }.run(false);
  }

  @Override
  public List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException {
    return new GetListHelper<TableName>(null, null, null, true, false) {
      @Override
      protected List<TableName> getSqlResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        return directSql.getAllTableNamesForStats();
      }

      @Override
      protected List<TableName> getJdoResult(
          GetHelper<List<TableName>> ctx) throws MetaException {
        boolean commited = false;
        Query query = null;
        List<TableName> result = new ArrayList<>();
        openTransaction();
        try {
          String paramStr = "", whereStr = "";
          for (int i = 0; i < MetaStoreDirectSql.STATS_TABLE_TYPES.length; ++i) {
            if (i != 0) {
              paramStr += ", ";
              whereStr += "||";
            }
            paramStr += "java.lang.String tt" + i;
            whereStr += " tableType == tt" + i;
          }
          query = pm.newQuery(MTable.class, whereStr);
          query.declareParameters(paramStr);
          @SuppressWarnings("unchecked")
          Collection<MTable> tbls = (Collection<MTable>) query.executeWithArray(
              query, MetaStoreDirectSql.STATS_TABLE_TYPES);
          pm.retrieveAll(tbls);
          for (MTable tbl : tbls) {
            result.add(new TableName(
                tbl.getDatabase().getCatalogName(), tbl.getDatabase().getName(), tbl.getTableName()));
          }
          commited = commitTransaction();
        } finally {
          rollbackAndCleanup(commited, query);
        }
        return result;
      }
    }.run(false);
  }

  protected List<String> getTablesInternal(String catName, String dbName, String pattern,
                                           TableType tableType, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(dbName);
    final String cat_name = normalizeIdentifier(catName);
    return new GetListHelper<String>(cat_name, dbName, null, allowSql, allowJdo) {
      @Override
      protected List<String> getSqlResult(GetHelper<List<String>> ctx)
              throws MetaException {
        return directSql.getTables(cat_name, db_name, tableType);
      }

      @Override
      protected List<String> getJdoResult(GetHelper<List<String>> ctx)
              throws MetaException, NoSuchObjectException {
        return getTablesInternalViaJdo(cat_name, db_name, pattern, tableType);
      }
    }.run(false);
  }

  private List<String> getTablesInternalViaJdo(String catName, String dbName, String pattern,
                                               TableType tableType) throws MetaException {
    boolean commited = false;
    Query query = null;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = normalizeIdentifier(dbName);
      // Take the pattern and split it on the | to get all the composing
      // patterns
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder();
      //adds database.name == dbName to the filter
      appendSimpleCondition(filterBuilder, "database.name", new String[] {dbName}, parameterVals);
      appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
      if(pattern != null) {
        appendPatternCondition(filterBuilder, "tableName", pattern, parameterVals);
      }
      if(tableType != null) {
        appendPatternCondition(filterBuilder, "tableType", new String[] {tableType.toString()}, parameterVals);
      }

      query = pm.newQuery(MTable.class, filterBuilder.toString());
      query.setResult("tableName");
      query.setOrdering("tableName ascending");
      Collection<String> names = (Collection<String>) query.executeWithArray(parameterVals.toArray(new String[0]));
      tbls = new ArrayList<>(names);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return tbls;
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    boolean commited = false;
    Query<?> query = null;
    List<String> tbls = null;
    try {
      openTransaction();
      dbName = normalizeIdentifier(dbName);
      query = pm.newQuery(MTable.class,
          "database.name == db && database.catalogName == cat && tableType == tt && rewriteEnabled == re");
      query.declareParameters(
          "java.lang.String db, java.lang.String cat, java.lang.String tt, boolean re");
      query.setResult("tableName");
      Collection<String> names = (Collection<String>) query.executeWithArray(
          db_name, catName, TableType.MATERIALIZED_VIEW.toString(), true);
      tbls = new ArrayList<>(names);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
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
      rollbackAndCleanup(commited, query);
    }
    return result.intValue();
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
                                      List<String> tableTypes) throws MetaException {

    boolean commited = false;
    Query query = null;
    List<TableMeta> metas = new ArrayList<>();
    try {
      openTransaction();
      // Take the pattern and split it on the | to get all the composing
      // patterns
      StringBuilder filterBuilder = new StringBuilder();
      List<String> parameterVals = new ArrayList<>();
      appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
      if (dbNames != null && !dbNames.equals("*")) {
        appendPatternCondition(filterBuilder, "database.name", dbNames, parameterVals);
      }
      if (tableNames != null && !tableNames.equals("*")) {
        appendPatternCondition(filterBuilder, "tableName", tableNames, parameterVals);
      }
      if (tableTypes != null && !tableTypes.isEmpty()) {
        appendSimpleCondition(filterBuilder, "tableType", tableTypes.toArray(new String[0]), parameterVals);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("getTableMeta with filter " + filterBuilder.toString() + " params: " +
            StringUtils.join(parameterVals, ", "));
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
      rollbackAndCleanup(commited, query);
    }
    return metas;
  }

  private StringBuilder appendPatternCondition(StringBuilder filterBuilder, String fieldName,
      String[] elements, List<String> parameterVals) {
    return appendCondition(filterBuilder, fieldName, elements, true, parameterVals);
  }

  private StringBuilder appendPatternCondition(StringBuilder builder,
      String fieldName, String elements, List<String> parameters) {
      elements = normalizeIdentifier(elements);
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
  public List<String> getAllTables(String catName, String dbName) throws MetaException {
    return getTables(catName, dbName, ".*");
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

  private AttachedMTableInfo getMTable(String catName, String db, String table,
                                       boolean retrieveCD) {
    AttachedMTableInfo nmtbl = new AttachedMTableInfo();
    MTable mtbl = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      db = normalizeIdentifier(db);
      table = normalizeIdentifier(table);
      query = pm.newQuery(MTable.class,
          "tableName == table && database.name == db && database.catalogName == catname");
      query.declareParameters(
          "java.lang.String table, java.lang.String db, java.lang.String catname");
      query.setUnique(true);
      LOG.debug("Executing getMTable for " +
          TableName.getQualified(catName, db, table));
      mtbl = (MTable) query.execute(table, db, catName);
      pm.retrieve(mtbl);
      // Retrieving CD can be expensive and unnecessary, so do it only when required.
      if (mtbl != null && retrieveCD) {
        pm.retrieve(mtbl.getSd());
        pm.retrieveAll(mtbl.getSd().getCD());
        nmtbl.mcd = mtbl.getSd().getCD();
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    nmtbl.mtbl = mtbl;
    return nmtbl;
  }

  private MCreationMetadata getCreationMetadata(String catName, String dbName, String tblName) {
    boolean commited = false;
    MCreationMetadata mcm = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(
          MCreationMetadata.class, "tblName == table && dbName == db && catalogName == cat");
      query.declareParameters("java.lang.String table, java.lang.String db, java.lang.String cat");
      query.setUnique(true);
      mcm = (MCreationMetadata) query.execute(tblName, dbName, catName);
      pm.retrieve(mcm);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return mcm;
  }

  private MTable getMTable(String catName, String db, String table) {
    AttachedMTableInfo nmtbl = getMTable(catName, db, table, false);
    return nmtbl.mtbl;
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String db, List<String> tbl_names)
      throws MetaException, UnknownDBException {
    List<Table> tables = new ArrayList<>();
    boolean committed = false;
    Query dbExistsQuery = null;
    Query query = null;
    try {
      openTransaction();
      db = normalizeIdentifier(db);
      catName = normalizeIdentifier(catName);

      List<String> lowered_tbl_names = new ArrayList<>(tbl_names.size());
      for (String t : tbl_names) {
        lowered_tbl_names.add(normalizeIdentifier(t));
      }
      query = pm.newQuery(MTable.class);
      query.setFilter("database.name == db && database.catalogName == cat && tbl_names.contains(tableName)");
      query.declareParameters("java.lang.String db, java.lang.String cat, java.util.Collection tbl_names");
      Collection mtables = (Collection) query.execute(db, catName, lowered_tbl_names);
      if (mtables == null || mtables.isEmpty()) {
        // Need to differentiate between an unmatched pattern and a non-existent database
        dbExistsQuery = pm.newQuery(MDatabase.class, "name == db && catalogName == cat");
        dbExistsQuery.declareParameters("java.lang.String db, java.lang.String cat");
        dbExistsQuery.setUnique(true);
        dbExistsQuery.setResult("name");
        String dbNameIfExists = (String) dbExistsQuery.execute(db, catName);
        if (org.apache.commons.lang.StringUtils.isEmpty(dbNameIfExists)) {
          throw new UnknownDBException("Could not find database " +
              DatabaseName.getQualified(catName, db));
        }
      } else {
        for (Iterator iter = mtables.iterator(); iter.hasNext(); ) {
          Table tbl = convertToTable((MTable) iter.next());
          // Retrieve creation metadata if needed
          if (TableType.MATERIALIZED_VIEW.toString().equals(tbl.getTableType())) {
            tbl.setCreationMetadata(
                convertToCreationMetadata(
                    getCreationMetadata(tbl.getCatName(), tbl.getDbName(), tbl.getTableName())));
          }
          tables.add(tbl);
        }
      }
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, query);
      if (dbExistsQuery != null) {
        dbExistsQuery.closeAll();
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
        MetastoreConf.getBoolVar(getConf(), ConfVars.ORM_RETRIEVE_MAPNULLS_AS_EMPTY_STRINGS));
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
      } else if (Boolean.parseBoolean(mtbl.getParameters().get("EXTERNAL"))) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      } else {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    final Table t = new Table(mtbl.getTableName(), mtbl.getDatabase().getName(), mtbl
        .getOwner(), mtbl.getCreateTime(), mtbl.getLastAccessTime(), mtbl
        .getRetention(), convertToStorageDescriptor(mtbl.getSd()),
        convertToFieldSchemas(mtbl.getPartitionKeys()), convertMap(mtbl.getParameters()),
        mtbl.getViewOriginalText(), mtbl.getViewExpandedText(), tableType);

    if (Strings.isNullOrEmpty(mtbl.getOwnerType())) {
      // Before the ownerType exists in an old Hive schema, USER was the default type for owner.
      // Let's set the default to USER to keep backward compatibility.
      t.setOwnerType(PrincipalType.USER);
    } else {
      t.setOwnerType(PrincipalType.valueOf(mtbl.getOwnerType()));
    }

    t.setRewriteEnabled(mtbl.isRewriteEnabled());
    t.setCatName(mtbl.getDatabase().getCatalogName());
    return t;
  }

  private MTable convertToMTable(Table tbl) throws InvalidObjectException,
      MetaException {
    if (tbl == null) {
      return null;
    }
    MDatabase mdb = null;
    String catName = tbl.isSetCatName() ? tbl.getCatName() : getDefaultCatalog(conf);
    try {
      mdb = getMDatabase(catName, tbl.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error("Could not convert to MTable", e);
      throw new InvalidObjectException("Database " +
          DatabaseName.getQualified(catName, tbl.getDbName()) + " doesn't exist.");
    }

    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = Boolean.parseBoolean(tbl.getParameters().get("EXTERNAL"));
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

    PrincipalType ownerPrincipalType = tbl.getOwnerType();
    String ownerType = (ownerPrincipalType == null) ? PrincipalType.USER.name() : ownerPrincipalType.name();

    // A new table is always created with a new column descriptor
    return new MTable(normalizeIdentifier(tbl.getTableName()), mdb,
        convertToMStorageDescriptor(tbl.getSd()), tbl.getOwner(), ownerType, tbl
        .getCreateTime(), tbl.getLastAccessTime(), tbl.getRetention(),
        convertToMFieldSchemas(tbl.getPartitionKeys()), tbl.getParameters(),
        tbl.getViewOriginalText(), tbl.getViewExpandedText(), tbl.isRewriteEnabled(),
        tableType);
  }

  private List<MFieldSchema> convertToMFieldSchemas(List<FieldSchema> keys) {
    List<MFieldSchema> mkeys = null;
    if (keys != null) {
      mkeys = new ArrayList<>(keys.size());
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
      keys = new ArrayList<>(mkeys.size());
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
      mkeys = new ArrayList<>(keys.size());
      for (Order part : keys) {
        mkeys.add(new MOrder(normalizeIdentifier(part.getCol()), part.getOrder()));
      }
    }
    return mkeys;
  }

  private List<Order> convertToOrders(List<MOrder> mkeys) {
    List<Order> keys = null;
    if (mkeys != null) {
      keys = new ArrayList<>(mkeys.size());
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
    SerDeInfo serde =
        new SerDeInfo(ms.getName(), ms.getSerializationLib(), convertMap(ms.getParameters()));
    if (ms.getDescription() != null) {
      serde.setDescription(ms.getDescription());
    }
    if (ms.getSerializerClass() != null) {
      serde.setSerializerClass(ms.getSerializerClass());
    }
    if (ms.getDeserializerClass() != null) {
      serde.setDeserializerClass(ms.getDeserializerClass());
    }
    if (ms.getSerdeType() > 0) {
      serde.setSerdeType(SerdeType.findByValue(ms.getSerdeType()));
    }
    return serde;
  }

  private MSerDeInfo convertToMSerDeInfo(SerDeInfo ms) throws MetaException {
    if (ms == null) {
      throw new MetaException("Invalid SerDeInfo object");
    }
    return new MSerDeInfo(ms.getName(), ms.getSerializationLib(), ms.getParameters(),
        ms.getDescription(), ms.getSerializerClass(), ms.getDeserializerClass(),
        ms.getSerdeType() == null ? 0 : ms.getSerdeType().getValue());
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
      lists = new ArrayList<>(mLists.size());
      for (MStringList element : mLists) {
        lists.add(new ArrayList<>(element.getInternalList()));
      }
    }
    return lists;
  }

  private List<MStringList> convertToMStringLists(List<List<String>> mLists) {
    List<MStringList> lists = null ;
    if (null != mLists) {
      lists = new ArrayList<>();
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
      map = new HashMap<>(mMap.size());
      Set<MStringList> keys = mMap.keySet();
      for (MStringList key : keys) {
        map.put(new ArrayList<>(key.getInternalList()), mMap.get(key));
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
      map = new HashMap<>(mMap.size());
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

  private MCreationMetadata convertToMCreationMetadata(
      CreationMetadata m) throws MetaException {
    if (m == null) {
      return null;
    }
    assert !m.isSetMaterializationTime();
    Set<MTable> tablesUsed = new HashSet<>();
    for (String fullyQualifiedName : m.getTablesUsed()) {
      String[] names =  fullyQualifiedName.split("\\.");
      tablesUsed.add(getMTable(m.getCatName(), names[0], names[1], false).mtbl);
    }
    return new MCreationMetadata(m.getCatName(), m.getDbName(), m.getTblName(),
        tablesUsed, m.getValidTxnList(), System.currentTimeMillis());
  }

  private CreationMetadata convertToCreationMetadata(
      MCreationMetadata s) throws MetaException {
    if (s == null) {
      return null;
    }
    Set<String> tablesUsed = new HashSet<>();
    for (MTable mtbl : s.getTables()) {
      tablesUsed.add(
          Warehouse.getQualifiedName(
              mtbl.getDatabase().getName(), mtbl.getTableName()));
    }
    CreationMetadata r = new CreationMetadata(s.getCatalogName(),
        s.getDbName(), s.getTblName(), tablesUsed);
    r.setMaterializationTime(s.getMaterializationTime());
    if (s.getTxnList() != null) {
      r.setValidTxnList(s.getTxnList());
    }
    return r;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    openTransaction();
    try {
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      MTable table = this.getMTable(catName, dbName, tblName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(catName, dbName, tblName);
        tabColumnGrants = this.listTableAllColumnGrants(catName, dbName, tblName);
      }
      List<Object> toPersist = new ArrayList<>();
      for (Partition part : parts) {
        if (!part.getTableName().equals(tblName) || !part.getDbName().equals(dbName)) {
          throw new MetaException("Partition does not belong to target table "
              + dbName + "." + tblName + ": " + part);
        }
        MPartition mpart = convertToMPart(part, table, true);
        toPersist.add(mpart);
        int now = (int)(System.currentTimeMillis()/1000);
        if (tabGrants != null) {
          for (MTablePrivilege tab: tabGrants) {
            toPersist.add(new MPartitionPrivilege(tab.getPrincipalName(),
                tab.getPrincipalType(), mpart, tab.getPrivilege(), now,
                tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption(),
                tab.getAuthorizer()));
          }
        }

        if (tabColumnGrants != null) {
          for (MTableColumnPrivilege col : tabColumnGrants) {
            toPersist.add(new MPartitionColumnPrivilege(col.getPrincipalName(),
                col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(),
                now, col.getGrantor(), col.getGrantorType(), col.getGrantOption(),
                col.getAuthorizer()));
          }
        }
      }
      if (CollectionUtils.isNotEmpty(toPersist)) {
        pm.makePersistentAll(toPersist);
        pm.flush();
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
      Partition part, List<FieldSchema> partitionKeys, boolean ifNotExists) throws MetaException {
    MetaStoreUtils.validatePartitionNameCharacters(part.getValues(),
        partitionValidationPattern);
    boolean doesExist = doesPartitionExist(part.getCatName(),
        part.getDbName(), part.getTableName(), partitionKeys, part.getValues());
    if (doesExist && !ifNotExists) {
      throw new MetaException("Partition already exists: " + part);
    }
    return !doesExist;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName,
                               PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    openTransaction();
    try {
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      MTable table = this.getMTable(catName, dbName, tblName);
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(catName, dbName, tblName);
        tabColumnGrants = this.listTableAllColumnGrants(catName, dbName, tblName);
      }

      if (!partitionSpec.getTableName().equals(tblName) || !partitionSpec.getDbName().equals(dbName)) {
        throw new MetaException("Partition does not belong to target table "
            + dbName + "." + tblName + ": " + partitionSpec);
      }

      PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();

      int now = (int)(System.currentTimeMillis()/1000);

      List<FieldSchema> partitionKeys = convertToFieldSchemas(table.getPartitionKeys());
      while (iterator.hasNext()) {
        Partition part = iterator.next();

        if (isValidPartition(part, partitionKeys, ifNotExists)) {
          MPartition mpart = convertToMPart(part, table, true);
          pm.makePersistent(mpart);
          if (tabGrants != null) {
            for (MTablePrivilege tab : tabGrants) {
              pm.makePersistent(new MPartitionPrivilege(tab.getPrincipalName(),
                  tab.getPrincipalType(), mpart, tab.getPrivilege(), now,
                  tab.getGrantor(), tab.getGrantorType(), tab.getGrantOption(),
                  tab.getAuthorizer()));
            }
          }

          if (tabColumnGrants != null) {
            for (MTableColumnPrivilege col : tabColumnGrants) {
              pm.makePersistent(new MPartitionColumnPrivilege(col.getPrincipalName(),
                  col.getPrincipalType(), mpart, col.getColumnName(), col.getPrivilege(),
                  now, col.getGrantor(), col.getGrantorType(), col.getGrantOption(),
                  col.getAuthorizer()));
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
      String catName = part.isSetCatName() ? part.getCatName() : getDefaultCatalog(conf);
      MTable table = this.getMTable(catName, part.getDbName(), part.getTableName());
      List<MTablePrivilege> tabGrants = null;
      List<MTableColumnPrivilege> tabColumnGrants = null;
      if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
        tabGrants = this.listAllTableGrants(catName, part.getDbName(), part.getTableName());
        tabColumnGrants = this.listTableAllColumnGrants(
            catName, part.getDbName(), part.getTableName());
      }
      openTransaction();
      MPartition mpart = convertToMPart(part, true);
      pm.makePersistent(mpart);

      int now = (int)(System.currentTimeMillis()/1000);
      List<Object> toPersist = new ArrayList<>();
      if (tabGrants != null) {
        for (MTablePrivilege tab: tabGrants) {
          MPartitionPrivilege partGrant = new MPartitionPrivilege(tab
              .getPrincipalName(), tab.getPrincipalType(),
              mpart, tab.getPrivilege(), now, tab.getGrantor(), tab
                  .getGrantorType(), tab.getGrantOption(), tab.getAuthorizer());
          toPersist.add(partGrant);
        }
      }

      if (tabColumnGrants != null) {
        for (MTableColumnPrivilege col : tabColumnGrants) {
          MPartitionColumnPrivilege partColumn = new MPartitionColumnPrivilege(col
              .getPrincipalName(), col.getPrincipalType(), mpart, col
              .getColumnName(), col.getPrivilege(), now, col.getGrantor(), col
              .getGrantorType(), col.getGrantOption(), col.getAuthorizer());
          toPersist.add(partColumn);
        }

        if (CollectionUtils.isNotEmpty(toPersist)) {
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
  public Partition getPartition(String catName, String dbName, String tableName,
      List<String> part_vals) throws NoSuchObjectException, MetaException {
    openTransaction();
    Partition part = convertToPart(getMPartition(catName, dbName, tableName, part_vals));
    commitTransaction();
    if(part == null) {
      throw new NoSuchObjectException("partition values="
          + part_vals.toString());
    }
    part.setValues(part_vals);
    return part;
  }

  /**
   * Getting MPartition object. Use this method only if the partition name is not available,
   * since then the table will be queried to get the partition keys.
   * @param catName The catalogue
   * @param dbName The database
   * @param tableName The table
   * @param part_vals The values defining the partition
   * @return The MPartition object in the backend database
   * @throws MetaException
   */
  private MPartition getMPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    boolean committed = false;
    MPartition result = null;
    try {
      openTransaction();
      MTable mtbl = getMTable(catName, dbName, tableName);
      if (mtbl == null) {
        return null;
      }
      // Change the query to use part_vals instead of the name which is
      // redundant TODO: callers of this often get part_vals out of name for no reason...
      String name =
          Warehouse.makePartName(convertToFieldSchemas(mtbl.getPartitionKeys()), part_vals);
      result = getMPartition(catName, dbName, tableName, name);
      committed = commitTransaction();
    } finally {
      rollbackAndCleanup(committed, (Query)null);
    }
    return result;
  }

  /**
   * Getting MPartition object. Use this method if the partition name is available, so we do not
   * query the table object again.
   * @param catName The catalogue
   * @param dbName The database
   * @param tableName The table
   * @param name The partition name
   * @return The MPartition object in the backend database
   * @throws MetaException
   */
  private MPartition getMPartition(String catName, String dbName, String tableName,
      String name) throws MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    List<MPartition> mparts = null;
    MPartition ret = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query =
          pm.newQuery(MPartition.class,
              "table.tableName == t1 && table.database.name == t2 && partitionName == t3 " +
                  " && table.database.catalogName == t4");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
          + "java.lang.String t4");
      mparts = (List<MPartition>) query.executeWithArray(tableName, dbName, name, catName);
      pm.retrieveAll(mparts);
      commited = commitTransaction();
      // We need to compare partition name with requested name since some DBs
      // (like MySQL, Derby) considers 'a' = 'a ' whereas others like (Postgres,
      // Oracle) doesn't exhibit this problem.
      if (CollectionUtils.isNotEmpty(mparts)) {
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
      rollbackAndCleanup(commited, query);
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
   * @return the model partition object, and null if the input partition is null.
   * @throws InvalidObjectException
   * @throws MetaException
   */
  private MPartition convertToMPart(Partition part, boolean useTableCD)
      throws InvalidObjectException, MetaException {
    if (part == null) {
      return null;
    }
    MTable mt = getMTable(part.getCatName(), part.getDbName(), part.getTableName());
    return convertToMPart(part, mt, useTableCD);
  }

  /**
   * Convert a Partition object into an MPartition, which is an object backed by the db
   * If the Partition's set of columns is the same as the parent table's AND useTableCD
   * is true, then this partition's storage descriptor's column descriptor will point
   * to the same one as the table's storage descriptor.
   * @param part the partition to convert
   * @param mt the parent table object
   * @param useTableCD whether to try to use the parent table's column descriptor.
   * @return the model partition object, and null if the input partition is null.
   * @throws InvalidObjectException
   * @throws MetaException
   */
  private MPartition convertToMPart(Partition part, MTable mt, boolean useTableCD)
      throws InvalidObjectException, MetaException {
    if (part == null) {
      return null;
    }
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
    Partition p = new Partition(convertList(mpart.getValues()), mpart.getTable().getDatabase()
        .getName(), mpart.getTable().getTableName(), mpart.getCreateTime(),
        mpart.getLastAccessTime(), convertToStorageDescriptor(mpart.getSd()),
        convertMap(mpart.getParameters()));
    p.setCatName(mpart.getTable().getDatabase().getCatalogName());
    return p;
  }

  private Partition convertToPart(String catName, String dbName, String tblName, MPartition mpart)
      throws MetaException {
    if (mpart == null) {
      return null;
    }
    Partition p = new Partition(convertList(mpart.getValues()), dbName, tblName,
        mpart.getCreateTime(), mpart.getLastAccessTime(),
        convertToStorageDescriptor(mpart.getSd(), false), convertMap(mpart.getParameters()));
    p.setCatName(catName);
    return p;
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName,
    List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
    InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MPartition part = getMPartition(catName, dbName, tableName, part_vals);
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
  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    dropPartitionsInternal(catName, dbName, tblName, partNames, true, true);
  }

  @VisibleForTesting
  void dropPartitionsInternal(String catName, String dbName, String tblName,
       List<String> partNames, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    if (CollectionUtils.isEmpty(partNames)) {
      return;
    }
    new GetListHelper<Void>(catName, dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Void> getSqlResult(GetHelper<List<Void>> ctx) throws MetaException {
        directSql.dropPartitionsViaSqlFilter(catName, dbName, tblName, partNames);
        return Collections.emptyList();
      }
      @Override
      protected List<Void> getJdoResult(GetHelper<List<Void>> ctx) throws MetaException {
        dropPartitionsViaJdo(catName, dbName, tblName, partNames);
        return Collections.emptyList();
      }
    }.run(false);
  }

  private void dropPartitionsViaJdo(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException {
    boolean success = false;
    openTransaction();
    try {
      // Delete all things.
      dropPartitionGrantsNoTxn(catName, dbName, tblName, partNames);
      dropPartitionAllColumnGrantsNoTxn(catName, dbName, tblName, partNames);
      dropPartitionColumnStatisticsNoTxn(catName, dbName, tblName, partNames);

      // CDs are reused; go try partition SDs, detach all CDs from SDs, then remove unused CDs.
      for (MColumnDescriptor mcd : detachCdsFromSdsNoTxn(catName, dbName, tblName, partNames)) {
        removeUnusedColumnDescriptor(mcd);
      }
      dropPartitionsNoTxn(catName, dbName, tblName, partNames);
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
   */
  private boolean dropPartitionCommon(MPartition part) throws NoSuchObjectException, MetaException,
    InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      if (part != null) {
        List<MFieldSchema> schemas = part.getTable().getPartitionKeys();
        List<String> colNames = new ArrayList<>();
        for (MFieldSchema col: schemas) {
          colNames.add(col.getName());
        }
        String partName = FileUtils.makePartName(colNames, part.getValues());

        List<MPartitionPrivilege> partGrants = listPartitionGrants(
            part.getTable().getDatabase().getCatalogName(),
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            Lists.newArrayList(partName));

        if (CollectionUtils.isNotEmpty(partGrants)) {
          pm.deletePersistentAll(partGrants);
        }

        List<MPartitionColumnPrivilege> partColumnGrants = listPartitionAllColumnGrants(
            part.getTable().getDatabase().getCatalogName(),
            part.getTable().getDatabase().getName(),
            part.getTable().getTableName(),
            Lists.newArrayList(partName));
        if (CollectionUtils.isNotEmpty(partColumnGrants)) {
          pm.deletePersistentAll(partColumnGrants);
        }

        String catName = part.getTable().getDatabase().getCatalogName();
        String dbName = part.getTable().getDatabase().getName();
        String tableName = part.getTable().getTableName();

        // delete partition level column stats if it exists
       try {
          deletePartitionColumnStatistics(catName, dbName, tableName, partName, part.getValues(), null);
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
  public List<Partition> getPartitions(String catName, String dbName, String tableName,
                                       int maxParts) throws MetaException, NoSuchObjectException {
    return getPartitionsInternal(catName, dbName, tableName, maxParts, true, true);
  }

  @Override
  public Map<String, String> getPartitionLocations(String catName, String dbName, String tblName,
      String baseLocationToNotShow, int max) {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tblName = normalizeIdentifier(tblName);

    boolean success = false;
    Query query = null;
    Map<String, String> partLocations = new HashMap<>();
    try {
      openTransaction();
      LOG.debug("Executing getPartitionLocations");

      query = pm.newQuery(MPartition.class);
      query.setFilter("this.table.database.catalogName == t1 && this.table.database.name == t2 "
          + "&& this.table.tableName == t3");
      query.declareParameters("String t1, String t2, String t3");
      query.setResult("this.partitionName, this.sd.location");
      if (max >= 0) {
        //Row limit specified, set it on the Query
        query.setRange(0, max);
      }

      List<Object[]> result = (List<Object[]>)query.execute(catName, dbName, tblName);
      for(Object[] row:result) {
        String location = (String)row[1];
        if (baseLocationToNotShow != null && location != null
            && FileUtils.isSubdirectory(baseLocationToNotShow, location)) {
          location = null;
        }
        partLocations.put((String)row[0], location);
      }
      LOG.debug("Done executing query for getPartitionLocations");
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
      if (query != null) {
        query.closeAll();
      }
    }
    return partLocations;
  }

  protected List<Partition> getPartitionsInternal(String catName, String dbName, String tblName,
                                                  final int maxParts, boolean allowSql, boolean allowJdo)
          throws MetaException, NoSuchObjectException {
    return new GetListHelper<Partition>(catName, dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        Integer max = (maxParts < 0) ? null : maxParts;
        return directSql.getPartitions(catName, dbName, tblName, max);
      }
      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException {
        QueryWrapper queryWrapper = new QueryWrapper();
        try {
          return convertToParts(listMPartitions(catName, dbName, tblName, maxParts, queryWrapper));
        } finally {
          queryWrapper.close();
        }
      }
    }.run(false);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName,
      short max, String userName, List<String> groupNames)
          throws MetaException, InvalidObjectException {
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      List<MPartition> mparts = listMPartitions(catName, dbName, tblName, max, queryWrapper);
      List<Partition> parts = new ArrayList<>(mparts.size());
      if (CollectionUtils.isNotEmpty(mparts)) {
        for (MPartition mpart : mparts) {
          MTable mtbl = mpart.getTable();
          Partition part = convertToPart(mpart);
          parts.add(part);

          if ("TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
                .getPartitionKeys()), part.getValues());
            PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(catName, dbName,
                tblName, partName, userName, groupNames);
            part.setPrivileges(partAuth);
          }
        }
      }
      success =  commitTransaction();
      return parts;
    } finally {
      rollbackAndCleanup(success, queryWrapper);
    }
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    boolean success = false;
    try {
      openTransaction();
      MPartition mpart = getMPartition(catName, dbName, tblName, partVals);
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
        PrincipalPrivilegeSet partAuth = this.getPartitionPrivilegeSet(catName, dbName,
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
      dest = new ArrayList<>(src.size());
    }
    for (MPartition mp : src) {
      dest.add(convertToPart(mp));
      Deadline.checkTimeout();
    }
    return dest;
  }

  private List<Partition> convertToParts(String catName, String dbName, String tblName, List<MPartition> mparts)
      throws MetaException {
    List<Partition> parts = new ArrayList<>(mparts.size());
    for (MPartition mp : mparts) {
      parts.add(convertToPart(catName, dbName, tblName, mp));
      Deadline.checkTimeout();
    }
    return parts;
  }

  // TODO:pc implement max
  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName,
      short max) throws MetaException {
    List<String> pns = null;
    boolean success = false;
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNames");
      pns = getPartitionNamesNoTxn(catName, dbName, tableName, max);
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return pns;
  }

  private String extractPartitionKey(FieldSchema key, List<FieldSchema> pkeys) {
    StringBuilder buffer = new StringBuilder(256);

    assert pkeys.size() >= 1;

    String partKey = "/" + key.getName() + "=";

    // Table is partitioned by single key
    if (pkeys.size() == 1 && (pkeys.get(0).getName().matches(key.getName()))) {
      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(key.getName()).append("=\") + ").append(key.getName().length() + 1)
          .append(")");

      // First partition key - anything between key= and first /
    } else if ((pkeys.get(0).getName().matches(key.getName()))) {

      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(key.getName()).append("=\") + ").append(key.getName().length() + 1).append(", ")
          .append("partitionName.indexOf(\"/\")")
          .append(")");

      // Last partition key - anything between /key= and end
    } else if ((pkeys.get(pkeys.size() - 1).getName().matches(key.getName()))) {
      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(partKey).append("\") + ").append(partKey.length())
          .append(")");

      // Intermediate key - anything between /key= and the following /
    } else {

      buffer.append("partitionName.substring(partitionName.indexOf(\"")
          .append(partKey).append("\") + ").append(partKey.length()).append(", ")
          .append("partitionName.indexOf(\"/\", partitionName.indexOf(\"").append(partKey)
          .append("\") + 1))");
    }
    LOG.info("Query for Key:" + key.getName() + " is :" + buffer);
    return buffer.toString();
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String catName, String dbName,
                                                     String tableName, List<FieldSchema> cols,
                                                     boolean applyDistinct, String filter,
                                                     boolean ascending, List<FieldSchema> order,
                                                     long maxParts) throws MetaException {

    catName = normalizeIdentifier(catName);
    dbName = dbName.toLowerCase().trim();
    tableName = tableName.toLowerCase().trim();
    try {
      if (filter == null || filter.isEmpty()) {
        PartitionValuesResponse response = getDistinctValuesForPartitionsNoTxn(catName, dbName,
            tableName, cols, applyDistinct, maxParts);
        LOG.info("Number of records fetched: {}", response.getPartitionValues().size());
        return response;
      } else {
        PartitionValuesResponse response =
            extractPartitionNamesByFilter(catName, dbName, tableName, filter, cols, ascending, maxParts);
        if (response != null && response.getPartitionValues() != null) {
          LOG.info("Number of records fetched with filter: {}", response.getPartitionValues().size());
        }
        return response;
      }
    } catch (Exception t) {
      LOG.error("Exception in ORM", t);
      throw new MetaException("Error retrieving partition values: " + t);
    }
  }

  private PartitionValuesResponse extractPartitionNamesByFilter(
      String catName, String dbName, String tableName, String filter, List<FieldSchema> cols,
      boolean ascending, long maxParts)
      throws MetaException, NoSuchObjectException {

    LOG.info("Table: {} filter: \"{}\" cols: {}",
        TableName.getQualified(catName, dbName, tableName), filter, cols);
    List<String> partitionNames = null;
    List<Partition> partitions = null;
    Table tbl = getTable(catName, dbName, tableName);
    try {
      // Get partitions by name - ascending or descending
      partitionNames = getPartitionNamesByFilter(catName, dbName, tableName, filter, ascending,
          maxParts);
    } catch (MetaException e) {
      LOG.warn("Querying by partition names failed, trying out with partition objects, filter: {}", filter);
    }

    if (partitionNames == null) {
      partitions = getPartitionsByFilter(catName, dbName, tableName, filter, (short) maxParts);
    }

    if (partitions != null) {
      partitionNames = new ArrayList<>(partitions.size());
      for (Partition partition : partitions) {
        // Check for NULL's just to be safe
        if (tbl.getPartitionKeys() != null && partition.getValues() != null) {
          partitionNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), partition.getValues()));
        }
      }
    }

    if (partitionNames == null && partitions == null) {
      throw new MetaException("Cannot obtain list of partitions by filter:\"" + filter +
          "\" for " + TableName.getQualified(catName, dbName, tableName));
    }

    if (!ascending) {
      Collections.sort(partitionNames, Collections.reverseOrder());
    }

    // Return proper response
    PartitionValuesResponse response = new PartitionValuesResponse();
    response.setPartitionValues(new ArrayList<>(partitionNames.size()));
    LOG.info("Converting responses to Partition values for items: {}", partitionNames.size());
    for (String partName : partitionNames) {
      ArrayList<String> vals = new ArrayList<String>(Collections.nCopies(tbl.getPartitionKeys().size(), null));
      PartitionValuesRow row = new PartitionValuesRow();
      Warehouse.makeValsFromName(partName, vals);
      for (String value : vals) {
        row.addToRow(value);
      }
      response.addToPartitionValues(row);
    }
    return response;
  }

  private List<String> getPartitionNamesByFilter(String catName, String dbName, String tableName,
                                                 String filter, boolean ascending, long maxParts)
      throws MetaException {

    boolean success = false;
    List<String> partNames = new ArrayList<>();
    try {
      openTransaction();
      LOG.debug("Executing getPartitionNamesByFilter");
      catName = normalizeIdentifier(catName);
      dbName = dbName.toLowerCase();
      tableName = tableName.toLowerCase();

      MTable mtable = getMTable(catName, dbName, tableName);
      if( mtable == null ) {
        // To be consistent with the behavior of listPartitionNames, if the
        // table or db does not exist, we return an empty list
        return partNames;
      }
      Map<String, Object> params = new HashMap<>();
      String queryFilterString = makeQueryFilterString(catName, dbName, mtable, filter, params);
      Query query = pm.newQuery(
          "select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
              + "where " + queryFilterString);

      if (maxParts >= 0) {
        //User specified a row limit, set it on the Query
        query.setRange(0, maxParts);
      }

      LOG.debug("Filter specified is {}, JDOQL filter is {}", filter,
        queryFilterString);

      LOG.debug("Parms is {}", params);

      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      if (ascending) {
        query.setOrdering("partitionName ascending");
      } else {
        query.setOrdering("partitionName descending");
      }
      query.setResult("partitionName");

      Collection<String> names = (Collection<String>) query.executeWithMap(params);
      partNames = new ArrayList<String>(names);

      LOG.debug("Done executing query for getPartitionNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for getPartitionNamesByFilter, size: {}", partNames.size());
      query.closeAll();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return partNames;
  }

  private PartitionValuesResponse getDistinctValuesForPartitionsNoTxn(
      String catName, String dbName, String tableName, List<FieldSchema> cols,
      boolean applyDistinct, long maxParts)
      throws MetaException {
    try {
      openTransaction();
      Query q = pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
          + "where table.database.name == t1 && table.database.catalogName == t2 && " +
          "table.tableName == t3 ");
      q.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");

      // TODO: Ordering seems to affect the distinctness, needs checking, disabling.
/*
      if (ascending) {
        q.setOrdering("partitionName ascending");
      } else {
        q.setOrdering("partitionName descending");
      }
*/
      if (maxParts > 0) {
        q.setRange(0, maxParts);
      }
      StringBuilder partValuesSelect = new StringBuilder(256);
      if (applyDistinct) {
        partValuesSelect.append("DISTINCT ");
      }
      List<FieldSchema> partitionKeys = getTable(catName, dbName, tableName).getPartitionKeys();
      for (FieldSchema key : cols) {
        partValuesSelect.append(extractPartitionKey(key, partitionKeys)).append(", ");
      }
      partValuesSelect.setLength(partValuesSelect.length() - 2);
      LOG.info("Columns to be selected from Partitions: {}", partValuesSelect);
      q.setResult(partValuesSelect.toString());

      PartitionValuesResponse response = new PartitionValuesResponse();
      response.setPartitionValues(new ArrayList<>());
      if (cols.size() > 1) {
        List<Object[]> results = (List<Object[]>) q.execute(dbName, catName, tableName);
        for (Object[] row : results) {
          PartitionValuesRow rowResponse = new PartitionValuesRow();
          for (Object columnValue : row) {
            rowResponse.addToRow((String) columnValue);
          }
          response.addToPartitionValues(rowResponse);
        }
      } else {
        List<Object> results = (List<Object>) q.execute(dbName, catName, tableName);
        for (Object row : results) {
          PartitionValuesRow rowResponse = new PartitionValuesRow();
          rowResponse.addToRow((String) row);
          response.addToPartitionValues(rowResponse);
        }
      }
      q.closeAll();
      return response;
    } finally {
      commitTransaction();
    }
  }

  private List<String> getPartitionNamesNoTxn(String catName, String dbName, String tableName, short max) {
    List<String> pns = new ArrayList<>();
    if (max == 0) {
      return pns;
    }
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    Query query =
        pm.newQuery("select partitionName from org.apache.hadoop.hive.metastore.model.MPartition "
            + "where table.database.name == t1 && table.tableName == t2 && table.database.catalogName == t3 "
            + "order by partitionName asc");
    query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
    query.setResult("partitionName");

    if (max > 0) {
      query.setRange(0, max);
    }
    Collection<String> names = (Collection<String>) query.execute(dbName, tableName, catName);
    pns.addAll(names);

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
  private Collection getPartitionPsQueryResults(String catName, String dbName, String tableName,
      List<String> part_vals, short max_parts, String resultsCol, QueryWrapper queryWrapper)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    Table table = getTable(catName, dbName, tableName);
    if (table == null) {
      throw new NoSuchObjectException(TableName.getQualified(catName, dbName, tableName)
          + " table not found");
    }
    String partNameMatcher = MetaStoreUtils.makePartNameMatcher(table, part_vals);
    Query query = queryWrapper.query = pm.newQuery(MPartition.class);
    StringBuilder queryFilter = new StringBuilder("table.database.name == dbName");
    queryFilter.append(" && table.database.catalogName == catName");
    queryFilter.append(" && table.tableName == tableName");
    queryFilter.append(" && partitionName.matches(partialRegex)");
    query.setFilter(queryFilter.toString());
    query.declareParameters("java.lang.String dbName, java.lang.String catName, "
        + "java.lang.String tableName, java.lang.String partialRegex");
    if (max_parts >= 0) {
      // User specified a row limit, set it on the Query
      query.setRange(0, max_parts);
    }
    if (resultsCol != null && !resultsCol.isEmpty()) {
      query.setResult(resultsCol);
    }

    return (Collection) query.executeWithArray(dbName, catName, tableName, partNameMatcher);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    List<Partition> partitions = new ArrayList<>();
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      LOG.debug("executing listPartitionNamesPsWithAuth");
      Collection parts = getPartitionPsQueryResults(catName, db_name, tbl_name,
          part_vals, max_parts, null, queryWrapper);
      MTable mtbl = getMTable(catName, db_name, tbl_name);
      for (Object o : parts) {
        Partition part = convertToPart((MPartition) o);
        //set auth privileges
        if (null != userName && null != groupNames &&
            "TRUE".equalsIgnoreCase(mtbl.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
          String partName = Warehouse.makePartName(this.convertToFieldSchemas(mtbl
              .getPartitionKeys()), part.getValues());
          PrincipalPrivilegeSet partAuth = getPartitionPrivilegeSet(catName, db_name,
              tbl_name, partName, userName, groupNames);
          part.setPrivileges(partAuth);
        }
        partitions.add(part);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, queryWrapper);
    }
    return partitions;
  }

  @Override
  public List<String> listPartitionNamesPs(String catName, String dbName, String tableName,
      List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
    List<String> partitionNames = new ArrayList<>();
    boolean success = false;
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      openTransaction();
      LOG.debug("Executing listPartitionNamesPs");
      Collection<String> names = getPartitionPsQueryResults(catName, dbName, tableName,
          part_vals, max_parts, "partitionName", queryWrapper);
      partitionNames.addAll(names);
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, queryWrapper);
    }
    return partitionNames;
  }

  // TODO:pc implement max
  private List<MPartition> listMPartitions(String catName, String dbName, String tableName,
                                           int max, QueryWrapper queryWrapper) {
    boolean success = false;
    List<MPartition> mparts = null;
    try {
      openTransaction();
      LOG.debug("Executing listMPartitions");
      dbName = normalizeIdentifier(dbName);
      tableName = normalizeIdentifier(tableName);
      Query query = queryWrapper.query = pm.newQuery(MPartition.class,
          "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      query.setOrdering("partitionName ascending");
      if (max >= 0) {
        query.setRange(0, max);
      }
      mparts = (List<MPartition>) query.execute(tableName, dbName, catName);
      LOG.debug("Done executing query for listMPartitions");
      pm.retrieveAll(mparts);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listMPartitions {}", mparts);
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
    return mparts;
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return getPartitionsByNamesInternal(catName, dbName, tblName, partNames, true, true);
  }

  protected List<Partition> getPartitionsByNamesInternal(String catName, String dbName,
                                                         String tblName,
                                                         final List<String> partNames,
                                                         boolean allowSql, boolean allowJdo)
          throws MetaException, NoSuchObjectException {
    return new GetListHelper<Partition>(catName, dbName, tblName, allowSql, allowJdo) {
      @Override
      protected List<Partition> getSqlResult(GetHelper<List<Partition>> ctx) throws MetaException {
        return directSql.getPartitionsViaSqlFilter(catName, dbName, tblName, partNames);
      }
      @Override
      protected List<Partition> getJdoResult(
          GetHelper<List<Partition>> ctx) throws MetaException, NoSuchObjectException {
        return getPartitionsViaOrmFilter(catName, dbName, tblName, partNames);
      }
    }.run(false);
  }

  @Override
  public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return getPartitionsByExprInternal(
        catName, dbName, tblName, expr, defaultPartitionName, maxParts, result, true, true);
  }

  protected boolean getPartitionsByExprInternal(String catName, String dbName, String tblName, final byte[] expr,
      final String defaultPartitionName, final  short maxParts, List<Partition> result,
      boolean allowSql, boolean allowJdo) throws TException {
    assert result != null;
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    final AtomicBoolean hasUnknownPartitions = new AtomicBoolean(false);
    result.addAll(new GetListHelper<Partition>(catName, dbName, tblName, allowSql, allowJdo) {
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
        List<String> partNames = new LinkedList<>();
        hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
            ctx.getTable(), expr, defaultPartitionName, maxParts, partNames));
        return directSql.getPartitionsViaSqlFilter(catName, dbName, tblName, partNames);
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
          List<String> partNames = new ArrayList<>();
          hasUnknownPartitions.set(getPartitionNamesPrunedByExprNoTxn(
              ctx.getTable(), expr, defaultPartitionName, maxParts, partNames));
          result = getPartitionsViaOrmFilter(catName, dbName, tblName, partNames);
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
    result.addAll(getPartitionNamesNoTxn(table.getCatName(),
        table.getDbName(), table.getTableName(), maxParts));
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(table.getPartitionKeys(), expr, defaultPartName, result);
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
    Map<String, Object> params = new HashMap<>();
    String jdoFilter =
        makeQueryFilterString(table.getCatName(), table.getDbName(), table, tree, params, isValidatedFilter);
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
    Map<String, Object> params = new HashMap<>();
    String jdoFilter = makeQueryFilterString(table.getCatName(), table.getDbName(), table, tree,
        params, isValidatedFilter);
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
  private List<Partition> getPartitionsViaOrmFilter(String catName,
      String dbName, String tblName, List<String> partNames) throws MetaException {
    if (partNames.isEmpty()) {
      return new ArrayList<>();
    }
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(catName, dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setResultClass(MPartition.class);
    query.setClass(MPartition.class);
    query.setOrdering("partitionName ascending");
    @SuppressWarnings("unchecked")
    List<MPartition> mparts = (List<MPartition>)query.executeWithMap(queryWithParams.getSecond());
    List<Partition> partitions = convertToParts(catName, dbName, tblName, mparts);
    if (query != null) {
      query.closeAll();
    }
    return partitions;
  }

  private void dropPartitionsNoTxn(String catName, String dbName, String tblName, List<String> partNames) {
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(catName, dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setClass(MPartition.class);
    long deleted = query.deletePersistentAll(queryWithParams.getSecond());
    LOG.debug("Deleted {} partition from store", deleted);
    query.closeAll();
  }

  /**
   * Detaches column descriptors from storage descriptors; returns the set of unique CDs
   * thus detached. This is done before dropping partitions because CDs are reused between
   * SDs; so, we remove the links to delete SDs and then check the returned CDs to see if
   * they are referenced by other SDs.
   */
  private HashSet<MColumnDescriptor> detachCdsFromSdsNoTxn(
      String catName, String dbName, String tblName, List<String> partNames) {
    ObjectPair<Query, Map<String, String>> queryWithParams =
        getPartQueryWithParams(catName, dbName, tblName, partNames);
    Query query = queryWithParams.getFirst();
    query.setClass(MPartition.class);
    query.setResult("sd");
    @SuppressWarnings("unchecked")
    List<MStorageDescriptor> sds = (List<MStorageDescriptor>)query.executeWithMap(
        queryWithParams.getSecond());
    HashSet<MColumnDescriptor> candidateCds = new HashSet<>();
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

  private ObjectPair<Query, Map<String, String>> getPartQueryWithParams(
      String catName, String dbName, String tblName, List<String> partNames) {
    StringBuilder sb = new StringBuilder("table.tableName == t1 && table.database.name == t2 &&" +
        " table.database.catalogName == t3 && (");
    int n = 0;
    Map<String, String> params = new HashMap<>();
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
    LOG.debug(" JDOQL filter is {}", sb);
    params.put("t1", normalizeIdentifier(tblName));
    params.put("t2", normalizeIdentifier(dbName));
    params.put("t3", normalizeIdentifier(catName));
    query.declareParameters(makeParameterDeclarationString(params));
    return new ObjectPair<>(query, params);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException {
    return getPartitionsByFilterInternal(catName, dbName, tblName, filter, maxParts, true, true);
  }

  /** Helper class for getting stuff w/transaction, direct SQL, perf logging, etc. */
  @VisibleForTesting
  public abstract class GetHelper<T> {
    private final boolean isInTxn, doTrace, allowJdo;
    private boolean doUseDirectSql;
    private long start;
    private Table table;
    protected final String catName, dbName, tblName;
    private boolean success = false;
    protected T results = null;

    public GetHelper(String catalogName, String dbName, String tblName,
                     boolean allowSql, boolean allowJdo) throws MetaException {
      assert allowSql || allowJdo;
      this.allowJdo = allowJdo;
      this.catName = (catalogName != null) ? normalizeIdentifier(catalogName) : null;
      this.dbName = (dbName != null) ? normalizeIdentifier(dbName) : null;
      if (tblName != null) {
        this.tblName = normalizeIdentifier(tblName);
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
      boolean isConfigEnabled = MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL)
          && (MetastoreConf.getBoolVar(getConf(), ConfVars.TRY_DIRECT_SQL_DDL) || !isInTxn);
      if (isConfigEnabled && directSql == null) {
        dbType = determineDatabaseProduct();
        directSql = new MetaStoreDirectSql(pm, getConf(), "");
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
            LOG.debug("Using direct SQL optimization.");
          } catch (Exception ex) {
            handleDirectSqlError(ex);
          }
        }
        // Note that this will be invoked in 2 cases:
        //    1) DirectSQL was disabled to start with;
        //    2) DirectSQL threw and was disabled in handleDirectSqlError.
        if (!doUseDirectSql) {
          this.results = getJdoResult(this);
          LOG.debug("Not using direct SQL optimization.");
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
        table = ensureGetTable(catName, dbName, tblName);
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
          table = ensureGetTable(catName, dbName, tblName);
        }
      } else {
        start = doTrace ? System.nanoTime() : 0;
      }

      if (directSqlErrors != null) {
        directSqlErrors.inc();
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
        double time = ((System.nanoTime() - start) / 1000000.0);
        String result = describeResult();
        String retrieveType = doUseDirectSql ? "SQL" : "ORM";

        LOG.debug("{} retrieved using {} in {}ms", result, retrieveType, time);
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
    public GetListHelper(String catName, String dbName, String tblName, boolean allowSql,
                         boolean allowJdo) throws MetaException {
      super(catName, dbName, tblName, allowSql, allowJdo);
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
     * @param dbName The Database Name
     * @param allowSql Whether or not we allow DirectSQL to perform this query.
     * @param allowJdo Whether or not we allow ORM to perform this query.
     * @throws MetaException
     */
    public GetDbHelper(String catalogName, String dbName,boolean allowSql, boolean allowJdo)
        throws MetaException {
      super(catalogName, dbName,null,allowSql,allowJdo);
    }

    @Override
    protected String describeResult() {
      return "db details for db ".concat(dbName);
    }
  }

  private abstract class GetStatHelper extends GetHelper<ColumnStatistics> {
    public GetStatHelper(String catalogName, String dbName, String tblName, boolean allowSql,
                         boolean allowJdo) throws MetaException {
      super(catalogName, dbName, tblName, allowSql, allowJdo);
    }

    @Override
    protected String describeResult() {
      return "statistics for " + (results == null ? 0 : results.getStatsObjSize()) + " columns";
    }
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tblName,
                                      String filter) throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = org.apache.commons.lang.StringUtils.isNotEmpty(filter)
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;

    return new GetHelper<Integer>(catName, dbName, tblName, true, true) {
      private final SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected String describeResult() {
        return "Partition count";
      }

      @Override
      protected boolean canUseDirectSql(GetHelper<Integer> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), exprTree, filter);
      }

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
  public int getNumPartitionsByExpr(String catName, String dbName, String tblName,
                                    byte[] expr) throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    final byte[] tempExpr = expr; // Need to be final to pass it to an inner class


    return new GetHelper<Integer>(catName, dbName, tblName, true, true) {
      private final SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected String describeResult() {
        return "Partition count";
      }

      @Override
      protected boolean canUseDirectSql(GetHelper<Integer> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), exprTree, filter);
      }

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
          List<String> filteredPartNames = new ArrayList<>();
          getPartitionNamesPrunedByExprNoTxn(ctx.getTable(), tempExpr, "", (short) -1, filteredPartNames);
          numPartitions = filteredPartNames.size();
        }

        return numPartitions;
      }
    }.run(true);
  }

  protected List<Partition> getPartitionsByFilterInternal(
      String catName, String dbName, String tblName, String filter, final short maxParts,
      boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return new GetListHelper<Partition>(catName, dbName, tblName, allowSql, allowJdo) {
      private final SqlFilterForPushdown filter = new SqlFilterForPushdown();

      @Override
      protected boolean canUseDirectSql(GetHelper<List<Partition>> ctx) throws MetaException {
        return directSql.generateSqlFilterForPushdown(ctx.getTable(), tree, filter);
      }

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
  private MTable ensureGetMTable(String catName, String dbName, String tblName)
      throws NoSuchObjectException, MetaException {
    MTable mtable = getMTable(catName, dbName, tblName);
    if (mtable == null) {
      throw new NoSuchObjectException("Specified catalog.database.table does not exist : "
          + TableName.getQualified(catName, dbName, tblName));
    }
    return mtable;
  }

  private Table ensureGetTable(String catName, String dbName, String tblName)
      throws NoSuchObjectException, MetaException {
    return convertToTable(ensureGetMTable(catName, dbName, tblName));
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
  private String makeQueryFilterString(String catName, String dbName, MTable mtable, String filter,
      Map<String, Object> params) throws MetaException {
    ExpressionTree tree = (filter != null && !filter.isEmpty())
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return makeQueryFilterString(catName, dbName, convertToTable(mtable), tree, params, true);
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
  private String makeQueryFilterString(String catName, String dbName, Table table,
                                       ExpressionTree tree, Map<String, Object> params,
                                       boolean isValidatedFilter) throws MetaException {
    assert tree != null;
    FilterBuilder queryBuilder = new FilterBuilder(isValidatedFilter);
    if (table != null) {
      queryBuilder.append("table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
      params.put("t1", table.getTableName());
      params.put("t2", table.getDbName());
      params.put("t3", table.getCatName());
    } else {
      queryBuilder.append("database.name == dbName && database.catalogName == catName");
      params.put("dbName", dbName);
      params.put("catName", catName);
    }

    tree.generateJDOFilterFragment(getConf(), table, params, queryBuilder);
    if (queryBuilder.hasError()) {
      assert !isValidatedFilter;
      LOG.info("JDO filter pushdown cannot be used: {}", queryBuilder.getErrorMessage());
      return null;
    }
    String jdoFilter = queryBuilder.getFilter();
    LOG.debug("jdoFilter = {}", jdoFilter);
    return jdoFilter;
  }

  private String makeParameterDeclarationString(Map<String, String> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (String key : params.keySet()) {
      paramDecl.append(", java.lang.String ")
          .append(key);
    }
    return paramDecl.toString();
  }

  private String makeParameterDeclarationStringObj(Map<String, Object> params) {
    //Create the parameter declaration string
    StringBuilder paramDecl = new StringBuilder();
    for (Entry<String, Object> entry : params.entrySet()) {
      paramDecl.append(", ");
      paramDecl.append(entry.getValue().getClass().getName());
      paramDecl.append(' ');
      paramDecl.append(entry.getKey());
    }
    return paramDecl.toString();
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                             short maxTables) throws MetaException {
    boolean success = false;
    Query query = null;
    List<String> tableNames = new ArrayList<>();
    try {
      openTransaction();
      LOG.debug("Executing listTableNamesByFilter");
      catName = normalizeIdentifier(catName);
      dbName = normalizeIdentifier(dbName);
      Map<String, Object> params = new HashMap<>();
      String queryFilterString = makeQueryFilterString(catName, dbName, null, filter, params);
      query = pm.newQuery(MTable.class);
      query.declareImports("import java.lang.String");
      query.setResult("tableName");
      query.setResultClass(java.lang.String.class);
      if (maxTables >= 0) {
        query.setRange(0, maxTables);
      }
      LOG.debug("filter specified is {}, JDOQL filter is {}", filter, queryFilterString);
      if (LOG.isDebugEnabled()) {
        for (Entry<String, Object> entry : params.entrySet()) {
          LOG.debug("key: {} value: {} class: {}", entry.getKey(), entry.getValue(),
             entry.getValue().getClass().getName());
        }
      }
      String parameterDeclaration = makeParameterDeclarationStringObj(params);
      query.declareParameters(parameterDeclaration);
      query.setFilter(queryFilterString);
      Collection<String> names = (Collection<String>)query.executeWithMap(params);
      // have to emulate "distinct", otherwise tables with the same name may be returned
      tableNames = new ArrayList<>(new HashSet<>(names));
      LOG.debug("Done executing query for listTableNamesByFilter");
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listTableNamesByFilter");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return tableNames;
  }

  @Override
  public void alterTable(String catName, String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    boolean registerCreationSignature = false;
    try {
      openTransaction();
      name = normalizeIdentifier(name);
      dbname = normalizeIdentifier(dbname);
      catName = normalizeIdentifier(catName);
      MTable newt = convertToMTable(newTable);
      if (newt == null) {
        throw new InvalidObjectException("new table is invalid");
      }

      MTable oldt = getMTable(catName, dbname, name);
      if (oldt == null) {
        throw new MetaException("table " + dbname + "." + name + " doesn't exist");
      }

      // For now only alter name, owner, parameters, cols, bucketcols are allowed
      oldt.setDatabase(newt.getDatabase());
      oldt.setTableName(normalizeIdentifier(newt.getTableName()));
      oldt.setParameters(newt.getParameters());
      oldt.setOwner(newt.getOwner());
      oldt.setOwnerType(newt.getOwnerType());
      // Fully copy over the contents of the new SD into the old SD,
      // so we don't create an extra SD in the metastore db that has no references.
      MColumnDescriptor oldCD = null;
      MStorageDescriptor oldSD = oldt.getSd();
      if (oldSD != null) {
        oldCD = oldSD.getCD();
      }
      copyMSD(newt.getSd(), oldt.getSd());
      removeUnusedColumnDescriptor(oldCD);
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
  public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    boolean success = false;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      dbname = normalizeIdentifier(dbname);
      tablename = normalizeIdentifier(tablename);
      // Update creation metadata
      MCreationMetadata newMcm = convertToMCreationMetadata(cm);
      MCreationMetadata mcm = getCreationMetadata(catName, dbname, tablename);
      mcm.setTables(newMcm.getTables());
      mcm.setMaterializationTime(newMcm.getMaterializationTime());
      mcm.setTxnList(newMcm.getTxnList());
      // commit the changes
      success = commitTransaction();
    } finally {
      if (!success) {
        rollbackTransaction();
      }
    }
  }

  /**
   * Alters an existing partition. Initiates copy of SD. Returns the old CD.
   * @param dbname
   * @param name
   * @param part_vals Partition values (of the original partition instance)
   * @param newPart Partition object containing new information
   * @return The column descriptor of the old partition instance (null if table is a view)
   * @throws InvalidObjectException
   * @throws MetaException
   */
  private MColumnDescriptor alterPartitionNoTxn(String catName, String dbname, String name,
                                                List<String> part_vals, Partition newPart)
      throws InvalidObjectException, MetaException {
    catName = normalizeIdentifier(catName);
    name = normalizeIdentifier(name);
    dbname = normalizeIdentifier(dbname);
    MPartition oldp = getMPartition(catName, dbname, name, part_vals);
    MPartition newp = convertToMPart(newPart, false);
    MColumnDescriptor oldCD = null;
    MStorageDescriptor oldSD = oldp.getSd();
    if (oldSD != null) {
      oldCD = oldSD.getCD();
    }
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
    return oldCD;
  }

  @Override
  public void alterPartition(String catName, String dbname, String name, List<String> part_vals,
                             Partition newPart) throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      MColumnDescriptor oldCd = alterPartitionNoTxn(catName, dbname, name, part_vals, newPart);
      removeUnusedColumnDescriptor(oldCd);
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
  public void alterPartitions(String catName, String dbname, String name,
                              List<List<String>> part_vals, List<Partition> newParts)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    Exception e = null;
    try {
      openTransaction();
      Iterator<List<String>> part_val_itr = part_vals.iterator();
      Set<MColumnDescriptor> oldCds = new HashSet<>();
      for (Partition tmpPart: newParts) {
        List<String> tmpPartVals = part_val_itr.next();
        MColumnDescriptor oldCd = alterPartitionNoTxn(catName, dbname, name, tmpPartVals, tmpPart);
        if (oldCd != null) {
          oldCds.add(oldCd);
        }
      }
      for (MColumnDescriptor oldCd : oldCds) {
        removeUnusedColumnDescriptor(oldCd);
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
    Query query = null;

    try {
      openTransaction();
      LOG.debug("execute removeUnusedColumnDescriptor");

      query = pm.newQuery("select count(1) from " +
        "org.apache.hadoop.hive.metastore.model.MStorageDescriptor where (this.cd == inCD)");
      query.declareParameters("MColumnDescriptor inCD");
      long count = ((Long)query.execute(oldCD)).longValue();

      //if no other SD references this CD, we can throw it out.
      if (count == 0) {
        pm.retrieve(oldCD);
        pm.deletePersistent(oldCD);
      }
      success = commitTransaction();
      LOG.debug("successfully deleted a CD in removeUnusedColumnDescriptor");
    } finally {
      rollbackAndCleanup(success, query);
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

  private static MFieldSchema getColumnFromTableColumns(List<MFieldSchema> cols, String col) {
    if (cols == null) {
      return null;
    }
    for (MFieldSchema mfs : cols) {
      if (mfs.getName().equalsIgnoreCase(col)) {
        return mfs;
      }
    }
    return null;
  }

  private static int getColumnIndexFromTableColumns(List<MFieldSchema> cols, String col) {
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
      name = normalizeIdentifier(name);
      constraintExistsQuery = pm.newQuery(MConstraint.class, "constraintName == name");
      constraintExistsQuery.declareParameters("java.lang.String name");
      constraintExistsQuery.setUnique(true);
      constraintExistsQuery.setResult("name");
      constraintNameIfExists = (String) constraintExistsQuery.execute(name);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, constraintExistsQuery);
    }
    return constraintNameIfExists != null && !constraintNameIfExists.isEmpty();
  }

  private String generateConstraintName(String... parameters) throws MetaException {
    int hashcode = ArrayUtils.toString(parameters).hashCode() & 0xfffffff;
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
  public List<String> addForeignKeys(
    List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
   return addForeignKeys(fks, true, null, null);
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    String ret = getGuidFromDB();
    if(ret != null) {
      return ret;
    }
    return createDbGuidAndPersist();
  }

  private String createDbGuidAndPersist() throws MetaException {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      MMetastoreDBProperties prop = new MMetastoreDBProperties();
      prop.setPropertykey("guid");
      final String guid = UUID.randomUUID().toString();
      LOG.debug("Attempting to add a guid {} for the metastore db", guid);
      prop.setPropertyValue(guid);
      prop.setDescription("Metastore DB GUID generated on "
          + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
      pm.makePersistent(prop);
      success = commitTransaction();
      if (success) {
        LOG.info("Metastore db guid {} created successfully", guid);
        return guid;
      }
    } catch (Exception e) {
      LOG.warn("Metastore db guid creation failed", e);
    } finally {
      rollbackAndCleanup(success, query);
    }
    // it possible that some other HMS instance could have created the guid
    // at the same time due which this instance could not create a guid above
    // in such case return the guid already generated
    final String guid = getGuidFromDB();
    if (guid == null) {
      throw new MetaException("Unable to create or fetch the metastore database uuid");
    }
    return guid;
  }

  private String getGuidFromDB() throws MetaException {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MMetastoreDBProperties.class, "this.propertyKey == key");
      query.declareParameters("java.lang.String key");
      Collection<MMetastoreDBProperties> names = (Collection<MMetastoreDBProperties>) query.execute("guid");
      List<String> uuids = new ArrayList<>();
      for (Iterator<MMetastoreDBProperties> i = names.iterator(); i.hasNext();) {
        String uuid = i.next().getPropertyValue();
        LOG.debug("Found guid {}", uuid);
        uuids.add(uuid);
      }
      success = commitTransaction();
      if(uuids.size() > 1) {
        throw new MetaException("Multiple uuids found");
      }
      if(!uuids.isEmpty()) {
        LOG.debug("Returning guid of metastore db : {}", uuids.get(0));
        return uuids.get(0);
      }
    } finally {
      rollbackAndCleanup(success, query);
    }
    LOG.warn("Guid for metastore db not found");
    return null;
  }

  private List<String> addForeignKeys(List<SQLForeignKey> foreignKeys, boolean retrieveCD,
      List<SQLPrimaryKey> primaryKeys, List<SQLUniqueConstraint> uniqueConstraints)
          throws InvalidObjectException, MetaException {
    List<String> fkNames = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(foreignKeys)) {
      List<MConstraint> mpkfks = new ArrayList<>();
      String currentConstraintName = null;
      String catName = null;
      // We start iterating through the foreign keys. This list might contain more than a single
      // foreign key, and each foreign key might contain multiple columns. The outer loop retrieves
      // the information that is common for a single key (table information) while the inner loop
      // checks / adds information about each column.
      for (int i = 0; i < foreignKeys.size(); i++) {
        if (catName == null) {
          catName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ? foreignKeys.get(i).getCatName() :
              getDefaultCatalog(conf));
        } else {
          String tmpCatName = normalizeIdentifier(foreignKeys.get(i).isSetCatName() ?
              foreignKeys.get(i).getCatName() : getDefaultCatalog(conf));
          if (!catName.equals(tmpCatName)) {
            throw new InvalidObjectException("Foreign keys cannot span catalogs");
          }
        }
        final String fkTableDB = normalizeIdentifier(foreignKeys.get(i).getFktable_db());
        final String fkTableName = normalizeIdentifier(foreignKeys.get(i).getFktable_name());
        // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
        // For instance, this is the case when we are creating the table.
        final AttachedMTableInfo nChildTable = getMTable(catName, fkTableDB, fkTableName, retrieveCD);
        final MTable childTable = nChildTable.mtbl;
        if (childTable == null) {
          throw new InvalidObjectException("Child table not found: " + fkTableName);
        }
        MColumnDescriptor childCD = retrieveCD ? nChildTable.mcd : childTable.getSd().getCD();
        final List<MFieldSchema> childCols = childCD == null || childCD.getCols() == null ?
            new ArrayList<>() : new ArrayList<>(childCD.getCols());
        if (childTable.getPartitionKeys() != null) {
          childCols.addAll(childTable.getPartitionKeys());
        }

        final String pkTableDB = normalizeIdentifier(foreignKeys.get(i).getPktable_db());
        final String pkTableName = normalizeIdentifier(foreignKeys.get(i).getPktable_name());
        // For primary keys, we retrieve the column descriptors if retrieveCD is true (which means
        // it is an alter table statement) or if it is a create table statement but we are
        // referencing another table instead of self for the primary key.
        final AttachedMTableInfo nParentTable;
        final MTable parentTable;
        MColumnDescriptor parentCD;
        final List<MFieldSchema> parentCols;
        final List<SQLPrimaryKey> existingTablePrimaryKeys;
        final List<SQLUniqueConstraint> existingTableUniqueConstraints;
        final boolean sameTable = fkTableDB.equals(pkTableDB) && fkTableName.equals(pkTableName);
        if (sameTable) {
          nParentTable = nChildTable;
          parentTable = childTable;
          parentCD = childCD;
          parentCols = childCols;
          existingTablePrimaryKeys = primaryKeys;
          existingTableUniqueConstraints = uniqueConstraints;
        } else {
          nParentTable = getMTable(catName, pkTableDB, pkTableName, true);
          parentTable = nParentTable.mtbl;
          if (parentTable == null) {
            throw new InvalidObjectException("Parent table not found: " + pkTableName);
          }
          parentCD = nParentTable.mcd;
          parentCols = parentCD == null || parentCD.getCols() == null ?
              new ArrayList<>() : new ArrayList<>(parentCD.getCols());
          if (parentTable.getPartitionKeys() != null) {
            parentCols.addAll(parentTable.getPartitionKeys());
          }
          existingTablePrimaryKeys = getPrimaryKeys(catName, pkTableDB, pkTableName);
          existingTableUniqueConstraints = getUniqueConstraints(catName, pkTableDB, pkTableName);
        }

        // Here we build an aux structure that is used to verify that the foreign key that is declared
        // is actually referencing a valid primary key or unique key. We also check that the types of
        // the columns correspond.
        if (existingTablePrimaryKeys.isEmpty() && existingTableUniqueConstraints.isEmpty()) {
          throw new MetaException(
              "Trying to define foreign key but there are no primary keys or unique keys for referenced table");
        }
        final Set<String> validPKsOrUnique = generateValidPKsOrUniqueSignatures(parentCols,
            existingTablePrimaryKeys, existingTableUniqueConstraints);

        StringBuilder fkSignature = new StringBuilder();
        StringBuilder referencedKSignature = new StringBuilder();
        for (; i < foreignKeys.size(); i++) {
          final SQLForeignKey foreignKey = foreignKeys.get(i);
          final String fkColumnName = normalizeIdentifier(foreignKey.getFkcolumn_name());
          int childIntegerIndex = getColumnIndexFromTableColumns(childCD.getCols(), fkColumnName);
          if (childIntegerIndex == -1) {
            if (childTable.getPartitionKeys() != null) {
              childCD = null;
              childIntegerIndex = getColumnIndexFromTableColumns(childTable.getPartitionKeys(), fkColumnName);
            }
            if (childIntegerIndex == -1) {
              throw new InvalidObjectException("Child column not found: " + fkColumnName);
            }
          }

          final String pkColumnName = normalizeIdentifier(foreignKey.getPkcolumn_name());
          int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD.getCols(), pkColumnName);
          if (parentIntegerIndex == -1) {
            if (parentTable.getPartitionKeys() != null) {
              parentCD = null;
              parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), pkColumnName);
            }
            if (parentIntegerIndex == -1) {
              throw new InvalidObjectException("Parent column not found: " + pkColumnName);
            }
          }

          if (foreignKey.getFk_name() == null) {
            // When there is no explicit foreign key name associated with the constraint and the key is composite,
            // we expect the foreign keys to be send in order in the input list.
            // Otherwise, the below code will break.
            // If this is the first column of the FK constraint, generate the foreign key name
            // NB: The below code can result in race condition where duplicate names can be generated (in theory).
            // However, this scenario can be ignored for practical purposes because of
            // the uniqueness of the generated constraint name.
            if (foreignKey.getKey_seq() == 1) {
              currentConstraintName = generateConstraintName(
                fkTableDB, fkTableName, pkTableDB, pkTableName, pkColumnName, fkColumnName, "fk");
            }
          } else {
            currentConstraintName = normalizeIdentifier(foreignKey.getFk_name());
            if(constraintNameAlreadyExists(currentConstraintName)) {
              throw new InvalidObjectException("Constraint name already exists: " + currentConstraintName);
            }
          }
          fkNames.add(currentConstraintName);
          Integer updateRule = foreignKey.getUpdate_rule();
          Integer deleteRule = foreignKey.getDelete_rule();
          int enableValidateRely = (foreignKey.isEnable_cstr() ? 4 : 0) +
                  (foreignKey.isValidate_cstr() ? 2 : 0) + (foreignKey.isRely_cstr() ? 1 : 0);
          MConstraint mpkfk = new MConstraint(
            currentConstraintName,
            MConstraint.FOREIGN_KEY_CONSTRAINT,
            foreignKey.getKey_seq(),
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

          final String fkColType = getColumnFromTableColumns(childCols, fkColumnName).getType();
          fkSignature.append(
              generateColNameTypeSignature(fkColumnName, fkColType));
          referencedKSignature.append(
              generateColNameTypeSignature(pkColumnName, fkColType));

          if (i + 1 < foreignKeys.size() && foreignKeys.get(i + 1).getKey_seq() == 1) {
            // Next one is a new key, we bail out from the inner loop
            break;
          }
        }
        String referenced = referencedKSignature.toString();
        if (!validPKsOrUnique.contains(referenced)) {
          throw new MetaException(
              "Foreign key references " + referenced + " but no corresponding "
              + "primary key or unique key exists. Possible keys: " + validPKsOrUnique);
        }
        if (sameTable && fkSignature.toString().equals(referenced)) {
          throw new MetaException(
              "Cannot be both foreign key and primary/unique key on same table: " + referenced);
        }
        fkSignature = new StringBuilder();
        referencedKSignature = new StringBuilder();
      }
      pm.makePersistentAll(mpkfks);
    }
    return fkNames;
  }

  private static Set<String> generateValidPKsOrUniqueSignatures(List<MFieldSchema> tableCols,
      List<SQLPrimaryKey> refTablePrimaryKeys, List<SQLUniqueConstraint> refTableUniqueConstraints) {
    final Set<String> validPKsOrUnique = new HashSet<>();
    if (!refTablePrimaryKeys.isEmpty()) {
      Collections.sort(refTablePrimaryKeys, new Comparator<SQLPrimaryKey>() {
        @Override
        public int compare(SQLPrimaryKey o1, SQLPrimaryKey o2) {
          int keyNameComp = o1.getPk_name().compareTo(o2.getPk_name());
          if (keyNameComp == 0) { return Integer.compare(o1.getKey_seq(), o2.getKey_seq()); }
          return keyNameComp;
        }
      });
      StringBuilder pkSignature = new StringBuilder();
      for (SQLPrimaryKey pk : refTablePrimaryKeys) {
        pkSignature.append(
            generateColNameTypeSignature(
                pk.getColumn_name(), getColumnFromTableColumns(tableCols, pk.getColumn_name()).getType()));
      }
      validPKsOrUnique.add(pkSignature.toString());
    }
    if (!refTableUniqueConstraints.isEmpty()) {
      Collections.sort(refTableUniqueConstraints, new Comparator<SQLUniqueConstraint>() {
        @Override
        public int compare(SQLUniqueConstraint o1, SQLUniqueConstraint o2) {
          int keyNameComp = o1.getUk_name().compareTo(o2.getUk_name());
          if (keyNameComp == 0) { return Integer.compare(o1.getKey_seq(), o2.getKey_seq()); }
          return keyNameComp;
        }
      });
      StringBuilder ukSignature = new StringBuilder();
      for (int j = 0; j < refTableUniqueConstraints.size(); j++) {
        SQLUniqueConstraint uk = refTableUniqueConstraints.get(j);
        ukSignature.append(
            generateColNameTypeSignature(
                uk.getColumn_name(), getColumnFromTableColumns(tableCols, uk.getColumn_name()).getType()));
        if (j + 1 < refTableUniqueConstraints.size()) {
          if (!refTableUniqueConstraints.get(j + 1).getUk_name().equals(
                  refTableUniqueConstraints.get(j).getUk_name())) {
            validPKsOrUnique.add(ukSignature.toString());
            ukSignature = new StringBuilder();
          }
        } else {
          validPKsOrUnique.add(ukSignature.toString());
        }
      }
    }
    return validPKsOrUnique;
  }

  private static String generateColNameTypeSignature(String colName, String colType) {
    return colName + ":" + colType + ";";
  }

  @Override
  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException,
    MetaException {
    return addPrimaryKeys(pks, true);
  }

  private List<String> addPrimaryKeys(List<SQLPrimaryKey> pks, boolean retrieveCD) throws InvalidObjectException,
    MetaException {
    List<String> pkNames = new ArrayList<>();
    List<MConstraint> mpks = new ArrayList<>();
    String constraintName = null;

    for (int i = 0; i < pks.size(); i++) {
      final String catName = normalizeIdentifier(pks.get(i).getCatName());
      final String tableDB = normalizeIdentifier(pks.get(i).getTable_db());
      final String tableName = normalizeIdentifier(pks.get(i).getTable_name());
      final String columnName = normalizeIdentifier(pks.get(i).getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (getPrimaryKeyConstraintName(parentTable.getDatabase().getCatalogName(),
          parentTable.getDatabase().getName(), parentTable.getTableName()) != null) {
        throw new MetaException(" Primary key already exists for: " +
            TableName.getQualified(catName, tableDB, tableName));
      }
      if (pks.get(i).getPk_name() == null) {
        if (pks.get(i).getKey_seq() == 1) {
          constraintName = generateConstraintName(tableDB, tableName, columnName, "pk");
        }
      } else {
        constraintName = normalizeIdentifier(pks.get(i).getPk_name());
        if(constraintNameAlreadyExists(constraintName)) {
          throw new InvalidObjectException("Constraint name already exists: " + constraintName);
        }
      }
      pkNames.add(constraintName);
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
    return pkNames;
  }

  @Override
  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks)
          throws InvalidObjectException, MetaException {
    return addUniqueConstraints(uks, true);
  }

  private List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks, boolean retrieveCD)
          throws InvalidObjectException, MetaException {
    List<String> ukNames = new ArrayList<>();
    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (int i = 0; i < uks.size(); i++) {
      final String catName = normalizeIdentifier(uks.get(i).getCatName());
      final String tableDB = normalizeIdentifier(uks.get(i).getTable_db());
      final String tableName = normalizeIdentifier(uks.get(i).getTable_name());
      final String columnName = normalizeIdentifier(uks.get(i).getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (uks.get(i).getUk_name() == null) {
        if (uks.get(i).getKey_seq() == 1) {
            constraintName = generateConstraintName(tableDB, tableName, columnName, "uk");
        }
      } else {
        constraintName = normalizeIdentifier(uks.get(i).getUk_name());
        if(constraintNameAlreadyExists(constraintName)) {
          throw new InvalidObjectException("Constraint name already exists: " + constraintName);
        }
      }
      ukNames.add(constraintName);

      int enableValidateRely = (uks.get(i).isEnable_cstr() ? 4 : 0) +
          (uks.get(i).isValidate_cstr() ? 2 : 0) + (uks.get(i).isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
        constraintName,
        MConstraint.UNIQUE_CONSTRAINT,
        uks.get(i).getKey_seq(),
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex);
      cstrs.add(muk);
    }
    pm.makePersistentAll(cstrs);
    return ukNames;
  }

  @Override
  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
          throws InvalidObjectException, MetaException {
    return addNotNullConstraints(nns, true);
  }

  @Override
  public List<String> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addDefaultConstraints(nns, true);
  }

  @Override
  public List<String> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    return addCheckConstraints(nns, true);
  }

  private List<String> addCheckConstraints(List<SQLCheckConstraint> cc, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    List<String> nnNames = new ArrayList<>();
    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (int i = 0; i < cc.size(); i++) {
      final String catName = normalizeIdentifier(cc.get(i).getCatName());
      final String tableDB = normalizeIdentifier(cc.get(i).getTable_db());
      final String tableName = normalizeIdentifier(cc.get(i).getTable_name());
      final String columnName = cc.get(i).getColumn_name() == null? null
          : normalizeIdentifier(cc.get(i).getColumn_name());
      final String ccName = cc.get(i).getDc_name();
      boolean isEnable = cc.get(i).isEnable_cstr();
      boolean isValidate = cc.get(i).isValidate_cstr();
      boolean isRely = cc.get(i).isRely_cstr();
      String constraintValue = cc.get(i).getCheck_expression();
      addConstraint(catName, tableDB, tableName, columnName, ccName, isEnable, isRely, isValidate,
                    MConstraint.CHECK_CONSTRAINT, constraintValue, retrieveCD, nnNames, cstrs);
    }
    pm.makePersistentAll(cstrs);
    return nnNames;
  }

  private void addConstraint(String catName, String tableDB, String tableName, String columnName, String ccName,
                               boolean isEnable, boolean isRely, boolean isValidate, int constraintType,
                               String constraintValue, boolean retrieveCD, List<String> nnNames,
                               List<MConstraint> cstrs)
      throws InvalidObjectException, MetaException {
    String constraintName = null;
    // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
    // For instance, this is the case when we are creating the table.
    AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
    MTable parentTable = nParentTable.mtbl;
    if (parentTable == null) {
      throw new InvalidObjectException("Parent table not found: " + tableName);
    }

    MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
    int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
    if (parentIntegerIndex == -1) {
      if (parentTable.getPartitionKeys() != null) {
        parentCD = null;
        parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
      }
    }
    if (ccName == null) {
      constraintName = generateConstraintName(tableDB, tableName, columnName, "dc");
    } else {
      constraintName = normalizeIdentifier(ccName);
      if(constraintNameAlreadyExists(constraintName)) {
        throw new InvalidObjectException("Constraint name already exists: " + constraintName);
      }
    }
    nnNames.add(constraintName);

    int enableValidateRely = (isEnable ? 4 : 0) +
        (isValidate ? 2 : 0) + (isRely ? 1 : 0);
    MConstraint muk = new MConstraint(
        constraintName,
        constraintType,
        1, // Not null constraint should reference a single column
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex,
        constraintValue);
    cstrs.add(muk);
  }

  private List<String> addDefaultConstraints(List<SQLDefaultConstraint> nns, boolean retrieveCD)
      throws InvalidObjectException, MetaException {
    List<String> nnNames = new ArrayList<>();
    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (int i = 0; i < nns.size(); i++) {
      final String catName = normalizeIdentifier(nns.get(i).getCatName());
      final String tableDB = normalizeIdentifier(nns.get(i).getTable_db());
      final String tableName = normalizeIdentifier(nns.get(i).getTable_name());
      final String columnName = normalizeIdentifier(nns.get(i).getColumn_name());
      final String ccName = nns.get(i).getDc_name();
      boolean isEnable = nns.get(i).isEnable_cstr();
      boolean isValidate = nns.get(i).isValidate_cstr();
      boolean isRely = nns.get(i).isRely_cstr();
      String constraintValue = nns.get(i).getDefault_value();
      addConstraint(catName, tableDB, tableName, columnName, ccName, isEnable, isRely, isValidate,
      MConstraint.DEFAULT_CONSTRAINT, constraintValue, retrieveCD, nnNames, cstrs);
    }
    pm.makePersistentAll(cstrs);
    return nnNames;
  }

  private List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns, boolean retrieveCD)
          throws InvalidObjectException, MetaException {
    List<String> nnNames = new ArrayList<>();
    List<MConstraint> cstrs = new ArrayList<>();
    String constraintName = null;

    for (int i = 0; i < nns.size(); i++) {
      final String catName = normalizeIdentifier(nns.get(i).getCatName());
      final String tableDB = normalizeIdentifier(nns.get(i).getTable_db());
      final String tableName = normalizeIdentifier(nns.get(i).getTable_name());
      final String columnName = normalizeIdentifier(nns.get(i).getColumn_name());

      // If retrieveCD is false, we do not need to do a deep retrieval of the Table Column Descriptor.
      // For instance, this is the case when we are creating the table.
      AttachedMTableInfo nParentTable = getMTable(catName, tableDB, tableName, retrieveCD);
      MTable parentTable = nParentTable.mtbl;
      if (parentTable == null) {
        throw new InvalidObjectException("Parent table not found: " + tableName);
      }

      MColumnDescriptor parentCD = retrieveCD ? nParentTable.mcd : parentTable.getSd().getCD();
      int parentIntegerIndex = getColumnIndexFromTableColumns(parentCD == null ? null : parentCD.getCols(), columnName);
      if (parentIntegerIndex == -1) {
        if (parentTable.getPartitionKeys() != null) {
          parentCD = null;
          parentIntegerIndex = getColumnIndexFromTableColumns(parentTable.getPartitionKeys(), columnName);
        }
        if (parentIntegerIndex == -1) {
          throw new InvalidObjectException("Parent column not found: " + columnName);
        }
      }
      if (nns.get(i).getNn_name() == null) {
        constraintName = generateConstraintName(tableDB, tableName, columnName, "nn");
      } else {
        constraintName = normalizeIdentifier(nns.get(i).getNn_name());
        if(constraintNameAlreadyExists(constraintName)) {
          throw new InvalidObjectException("Constraint name already exists: " + constraintName);
        }
      }
      nnNames.add(constraintName);

      int enableValidateRely = (nns.get(i).isEnable_cstr() ? 4 : 0) +
          (nns.get(i).isValidate_cstr() ? 2 : 0) + (nns.get(i).isRely_cstr() ? 1 : 0);
      MConstraint muk = new MConstraint(
        constraintName,
        MConstraint.NOT_NULL_CONSTRAINT,
        1, // Not null constraint should reference a single column
        null,
        null,
        enableValidateRely,
        parentTable,
        null,
        parentCD,
        null,
        null,
        parentIntegerIndex);
      cstrs.add(muk);
    }
    pm.makePersistentAll(cstrs);
    return nnNames;
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
      rollbackAndCleanup(commited, query);
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
        if (CollectionUtils.isNotEmpty(roleMap)) {
          pm.deletePersistentAll(roleMap);
        }
        List<MRoleMap> roleMember = listMSecurityPrincipalMembershipRole(mRol
            .getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(roleMember)) {
          pm.deletePersistentAll(roleMember);
        }
        queryWrapper.close();
        // then remove all the grants
        List<MGlobalPrivilege> userGrants = listPrincipalMGlobalGrants(
            mRol.getRoleName(), PrincipalType.ROLE);
        if (CollectionUtils.isNotEmpty(userGrants)) {
          pm.deletePersistentAll(userGrants);
        }
        List<MDBPrivilege> dbGrants = listPrincipalAllDBGrant(mRol
            .getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(dbGrants)) {
          pm.deletePersistentAll(dbGrants);
        }
        queryWrapper.close();
        List<MTablePrivilege> tabPartGrants = listPrincipalAllTableGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(tabPartGrants)) {
          pm.deletePersistentAll(tabPartGrants);
        }
        queryWrapper.close();
        List<MPartitionPrivilege> partGrants = listPrincipalAllPartitionGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(partGrants)) {
          pm.deletePersistentAll(partGrants);
        }
        queryWrapper.close();
        List<MTableColumnPrivilege> tblColumnGrants = listPrincipalAllTableColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(tblColumnGrants)) {
          pm.deletePersistentAll(tblColumnGrants);
        }
        queryWrapper.close();
        List<MPartitionColumnPrivilege> partColumnGrants = listPrincipalAllPartitionColumnGrants(
            mRol.getRoleName(), PrincipalType.ROLE, queryWrapper);
        if (CollectionUtils.isNotEmpty(partColumnGrants)) {
          pm.deletePersistentAll(partColumnGrants);
        }
        queryWrapper.close();

        // finally remove the role
        pm.deletePersistent(mRol);
      }
      success = commitTransaction();
    } finally {
      rollbackAndCleanup(success, queryWrapper);
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
    List<MRoleMap> ret = new ArrayList<>();
    if(userName != null) {
      ret.addAll(listMRoles(userName, PrincipalType.USER));
    }
    if (groupNames != null) {
      for (String groupName: groupNames) {
        ret.addAll(listMRoles(groupName, PrincipalType.GROUP));
      }
    }
    // get names of these roles and its ancestors
    Set<String> roleNames = new HashSet<>();
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
    List<MRoleMap> mRoleMember = new ArrayList<>();

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
      rollbackAndCleanup(success, query);
    }

    if (principalType == PrincipalType.USER) {
      // All users belong to public role implicitly, add that role
      // TODO MS-SPLIT Change this back to HiveMetaStore.PUBLIC once HiveMetaStore has moved to
      // stand-alone metastore.
      //MRole publicRole = new MRole(HiveMetaStore.PUBLIC, 0, HiveMetaStore.PUBLIC);
      MRole publicRole = new MRole("public", 0, "public");
      mRoleMember.add(new MRoleMap(principalName, principalType.toString(), publicRole, 0, null,
          null, false));
    }

    return mRoleMember;
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    List<Role> result = new ArrayList<>();
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
    List<RolePrincipalGrant> result = new ArrayList<>();
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
      rollbackAndCleanup(commited, query);
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
      List<String> roleNames = new ArrayList<>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        roleNames.add((String) i.next());
      }
      success = commitTransaction();
      return roleNames;
    } finally {
      rollbackAndCleanup(success, query);
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
        if(CollectionUtils.isNotEmpty(user)) {
          Map<String, List<PrivilegeGrantInfo>> userPriv = new HashMap<>();
          List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(user.size());
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
      if (CollectionUtils.isNotEmpty(groupNames)) {
        Map<String, List<PrivilegeGrantInfo>> groupPriv = new HashMap<>();
        for(String groupName: groupNames) {
          List<MGlobalPrivilege> group =
              this.listPrincipalMGlobalGrants(groupName, PrincipalType.GROUP);
          if(CollectionUtils.isNotEmpty(group)) {
            List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(group.size());
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

  private List<PrivilegeGrantInfo> getDBPrivilege(String catName, String dbName,
      String principalName, PrincipalType principalType)
      throws InvalidObjectException, MetaException {
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    if (principalName != null) {
      List<MDBPrivilege> userNameDbPriv = this.listPrincipalMDBGrants(
          principalName, principalType, catName, dbName);
      if (CollectionUtils.isNotEmpty(userNameDbPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
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
    return new ArrayList<>(0);
  }


  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    boolean commited = false;
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> dbUserPriv = new HashMap<>();
        dbUserPriv.put(userName, getDBPrivilege(catName, dbName, userName,
            PrincipalType.USER));
        ret.setUserPrivileges(dbUserPriv);
      }
      if (CollectionUtils.isNotEmpty(groupNames)) {
        Map<String, List<PrivilegeGrantInfo>> dbGroupPriv = new HashMap<>();
        for (String groupName : groupNames) {
          dbGroupPriv.put(groupName, getDBPrivilege(catName, dbName, groupName,
              PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(dbGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (CollectionUtils.isNotEmpty(roleNames)) {
        Map<String, List<PrivilegeGrantInfo>> dbRolePriv = new HashMap<>();
        for (String roleName : roleNames) {
          dbRolePriv
              .put(roleName, getDBPrivilege(catName, dbName, roleName, PrincipalType.ROLE));
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
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName,
      String tableName, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> partUserPriv = new HashMap<>();
        partUserPriv.put(userName, getPartitionPrivilege(catName, dbName,
            tableName, partition, userName, PrincipalType.USER));
        ret.setUserPrivileges(partUserPriv);
      }
      if (CollectionUtils.isNotEmpty(groupNames)) {
        Map<String, List<PrivilegeGrantInfo>> partGroupPriv = new HashMap<>();
        for (String groupName : groupNames) {
          partGroupPriv.put(groupName, getPartitionPrivilege(catName, dbName, tableName,
              partition, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(partGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (CollectionUtils.isNotEmpty(roleNames)) {
        Map<String, List<PrivilegeGrantInfo>> partRolePriv = new HashMap<>();
        for (String roleName : roleNames) {
          partRolePriv.put(roleName, getPartitionPrivilege(catName, dbName, tableName,
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
  public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName,
      String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    tableName = normalizeIdentifier(tableName);
    catName = normalizeIdentifier(catName);
    dbName = normalizeIdentifier(dbName);

    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> tableUserPriv = new HashMap<>();
        tableUserPriv.put(userName, getTablePrivilege(catName, dbName,
            tableName, userName, PrincipalType.USER));
        ret.setUserPrivileges(tableUserPriv);
      }
      if (CollectionUtils.isNotEmpty(groupNames)) {
        Map<String, List<PrivilegeGrantInfo>> tableGroupPriv = new HashMap<>();
        for (String groupName : groupNames) {
          tableGroupPriv.put(groupName, getTablePrivilege(catName, dbName, tableName,
              groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(tableGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (CollectionUtils.isNotEmpty(roleNames)) {
        Map<String, List<PrivilegeGrantInfo>> tableRolePriv = new HashMap<>();
        for (String roleName : roleNames) {
          tableRolePriv.put(roleName, getTablePrivilege(catName, dbName, tableName,
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
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName,
      String tableName, String partitionName, String columnName,
      String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    catName = normalizeIdentifier(catName);

    boolean commited = false;
    PrincipalPrivilegeSet ret = new PrincipalPrivilegeSet();
    try {
      openTransaction();
      if (userName != null) {
        Map<String, List<PrivilegeGrantInfo>> columnUserPriv = new HashMap<>();
        columnUserPriv.put(userName, getColumnPrivilege(catName, dbName, tableName,
            columnName, partitionName, userName, PrincipalType.USER));
        ret.setUserPrivileges(columnUserPriv);
      }
      if (CollectionUtils.isNotEmpty(groupNames)) {
        Map<String, List<PrivilegeGrantInfo>> columnGroupPriv = new HashMap<>();
        for (String groupName : groupNames) {
          columnGroupPriv.put(groupName, getColumnPrivilege(catName, dbName, tableName,
              columnName, partitionName, groupName, PrincipalType.GROUP));
        }
        ret.setGroupPrivileges(columnGroupPriv);
      }
      Set<String> roleNames = listAllRolesInHierarchy(userName, groupNames);
      if (CollectionUtils.isNotEmpty(roleNames)) {
        Map<String, List<PrivilegeGrantInfo>> columnRolePriv = new HashMap<>();
        for (String roleName : roleNames) {
          columnRolePriv.put(roleName, getColumnPrivilege(catName, dbName, tableName,
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

  private List<PrivilegeGrantInfo> getPartitionPrivilege(String catName, String dbName,
      String tableName, String partName, String principalName,
      PrincipalType principalType) {

    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    if (principalName != null) {
      List<MPartitionPrivilege> userNameTabPartPriv = this
          .listPrincipalMPartitionGrants(principalName, principalType,
              catName, dbName, tableName, partName);
      if (CollectionUtils.isNotEmpty(userNameTabPartPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
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
    return new ArrayList<>(0);
  }

  private PrincipalType getPrincipalTypeFromStr(String str) {
    return str == null ? null : PrincipalType.valueOf(str);
  }

  private List<PrivilegeGrantInfo> getTablePrivilege(String catName, String dbName,
      String tableName, String principalName, PrincipalType principalType) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    if (principalName != null) {
      List<MTablePrivilege> userNameTabPartPriv = this
          .listAllMTableGrants(principalName, principalType,
              catName, dbName, tableName);
      if (CollectionUtils.isNotEmpty(userNameTabPartPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
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
    return new ArrayList<>(0);
  }

  private List<PrivilegeGrantInfo> getColumnPrivilege(String catName, String dbName,
      String tableName, String columnName, String partitionName,
      String principalName, PrincipalType principalType) {

    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    catName = normalizeIdentifier(catName);

    if (partitionName == null) {
      List<MTableColumnPrivilege> userNameColumnPriv = this
          .listPrincipalMTableColumnGrants(principalName, principalType,
              catName, dbName, tableName, columnName);
      if (CollectionUtils.isNotEmpty(userNameColumnPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
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
              principalType, catName, dbName, tableName, partitionName, columnName);
      if (CollectionUtils.isNotEmpty(userNameColumnPriv)) {
        List<PrivilegeGrantInfo> grantInfos = new ArrayList<>(
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
    return new ArrayList<>(0);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    boolean committed = false;
    int now = (int) (System.currentTimeMillis() / 1000);
    try {
      openTransaction();
      List<Object> persistentObjs = new ArrayList<>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();

      if (CollectionUtils.isNotEmpty(privilegeList)) {
        Iterator<HiveObjectPrivilege> privIter = privilegeList.iterator();
        Set<String> privSet = new HashSet<>();
        while (privIter.hasNext()) {
          HiveObjectPrivilege privDef = privIter.next();
          HiveObjectRef hiveObject = privDef.getHiveObject();
          String privilegeStr = privDef.getGrantInfo().getPrivilege();
          String[] privs = privilegeStr.split(",");
          String userName = privDef.getPrincipalName();
          String authorizer = privDef.getAuthorizer();
          PrincipalType principalType = privDef.getPrincipalType();
          String grantor = privDef.getGrantInfo().getGrantor();
          String grantorType = privDef.getGrantInfo().getGrantorType().toString();
          boolean grantOption = privDef.getGrantInfo().isGrantOption();
          privSet.clear();

          if(principalType == PrincipalType.ROLE){
            validateRole(userName);
          }

          String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
              getDefaultCatalog(conf);
          if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
            List<MGlobalPrivilege> globalPrivs = this
                .listPrincipalMGlobalGrants(userName, principalType, authorizer);
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
                  principalType.toString(), privilege, now, grantor, grantorType, grantOption,
                  authorizer);
              persistentObjs.add(mGlobalPrivs);
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
            MDatabase dbObj = getMDatabase(catName, hiveObject.getDbName());
            if (dbObj != null) {
              List<MDBPrivilege> dbPrivs = this.listPrincipalMDBGrants(
                  userName, principalType, catName, hiveObject.getDbName(), authorizer);
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
                    .toString(), dbObj, privilege, now, grantor, grantorType, grantOption, authorizer);
                persistentObjs.add(mDb);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
            MTable tblObj = getMTable(catName, hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              List<MTablePrivilege> tablePrivs = this
                  .listAllMTableGrants(userName, principalType,
                      catName, hiveObject.getDbName(), hiveObject.getObjectName(), authorizer);
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
                    privilege, now, grantor, grantorType, grantOption, authorizer);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
            MPartition partObj = this.getMPartition(catName, hiveObject.getDbName(),
                hiveObject.getObjectName(), hiveObject.getPartValues());
            String partName = null;
            if (partObj != null) {
              partName = partObj.getPartitionName();
              List<MPartitionPrivilege> partPrivs = this
                  .listPrincipalMPartitionGrants(userName,
                      principalType, catName, hiveObject.getDbName(), hiveObject
                          .getObjectName(), partObj.getPartitionName(), authorizer);
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
                    grantorType, grantOption, authorizer);
                persistentObjs.add(mTab);
              }
            }
          } else if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
            MTable tblObj = getMTable(catName, hiveObject.getDbName(), hiveObject
                .getObjectName());
            if (tblObj != null) {
              if (hiveObject.getPartValues() != null) {
                MPartition partObj = null;
                List<MPartitionColumnPrivilege> colPrivs = null;
                partObj = this.getMPartition(catName, hiveObject.getDbName(), hiveObject
                    .getObjectName(), hiveObject.getPartValues());
                if (partObj == null) {
                  continue;
                }
                colPrivs = this.listPrincipalMPartitionColumnGrants(
                    userName, principalType, catName, hiveObject.getDbName(), hiveObject
                        .getObjectName(), partObj.getPartitionName(),
                    hiveObject.getColumnName(), authorizer);

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
                      grantOption, authorizer);
                  persistentObjs.add(mCol);
                }

              } else {
                List<MTableColumnPrivilege> colPrivs = null;
                colPrivs = this.listPrincipalMTableColumnGrants(
                    userName, principalType, catName, hiveObject.getDbName(), hiveObject
                        .getObjectName(), hiveObject.getColumnName(), authorizer);

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
                      grantOption, authorizer);
                  persistentObjs.add(mCol);
                }
              }
            }
          }
        }
      }
      if (CollectionUtils.isNotEmpty(persistentObjs)) {
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
      List<Object> persistentObjs = new ArrayList<>();

      List<HiveObjectPrivilege> privilegeList = privileges.getPrivileges();


      if (CollectionUtils.isNotEmpty(privilegeList)) {
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

          String catName = hiveObject.isSetCatName() ? hiveObject.getCatName() :
              getDefaultCatalog(conf);
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
            MDatabase dbObj = getMDatabase(catName, hiveObject.getDbName());
            if (dbObj != null) {
              String db = hiveObject.getDbName();
              boolean found = false;
              List<MDBPrivilege> dbGrants = this.listPrincipalMDBGrants(
                  userName, principalType, catName, db);
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
                    catName, hiveObject.getDbName(), hiveObject.getObjectName());
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
            Table tabObj = this.getTable(catName, hiveObject.getDbName(), hiveObject.getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(), hiveObject.getPartValues());
            }
            List<MPartitionPrivilege> partitionGrants = this
                .listPrincipalMPartitionGrants(userName, principalType,
                    catName, hiveObject.getDbName(), hiveObject.getObjectName(), partName);
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

            Table tabObj = this.getTable(catName, hiveObject.getDbName(), hiveObject
                .getObjectName());
            String partName = null;
            if (hiveObject.getPartValues() != null) {
              partName = Warehouse.makePartName(tabObj.getPartitionKeys(),
                  hiveObject.getPartValues());
            }

            if (partName != null) {
              List<MPartitionColumnPrivilege> mSecCol = listPrincipalMPartitionColumnGrants(
                  userName, principalType, catName, hiveObject.getDbName(), hiveObject
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
                  userName, principalType, catName, hiveObject.getDbName(), hiveObject
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

      if (CollectionUtils.isNotEmpty(persistentObjs)) {
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

  class PrivilegeWithoutCreateTimeComparator implements Comparator<HiveObjectPrivilege> {
    @Override
    public int compare(HiveObjectPrivilege o1, HiveObjectPrivilege o2) {
      int createTime1 = o1.getGrantInfo().getCreateTime();
      int createTime2 = o2.getGrantInfo().getCreateTime();
      o1.getGrantInfo().setCreateTime(0);
      o2.getGrantInfo().setCreateTime(0);
      int result = o1.compareTo(o2);
      o1.getGrantInfo().setCreateTime(createTime1);
      o2.getGrantInfo().setCreateTime(createTime2);
      return result;
    }
  }

  @Override
  public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    boolean committed = false;
    try {
      openTransaction();
      Set<HiveObjectPrivilege> revokePrivilegeSet
          = new TreeSet<HiveObjectPrivilege>(new PrivilegeWithoutCreateTimeComparator());
      Set<HiveObjectPrivilege> grantPrivilegeSet
          = new TreeSet<HiveObjectPrivilege>(new PrivilegeWithoutCreateTimeComparator());

      List<HiveObjectPrivilege> grants = null;
      String catName = objToRefresh.isSetCatName() ? objToRefresh.getCatName() :
          getDefaultCatalog(conf);
      switch (objToRefresh.getObjectType()) {
      case DATABASE:
        grants = this.listDBGrantsAll(catName, objToRefresh.getDbName(), authorizer);
        break;
      case TABLE:
        grants = listTableGrantsAll(catName, objToRefresh.getDbName(), objToRefresh.getObjectName(), authorizer);
        break;
      case COLUMN:
        Preconditions.checkArgument(objToRefresh.getColumnName()==null, "columnName must be null");
        grants = convertTableCols(listTableAllColumnGrants(catName,
            objToRefresh.getDbName(), objToRefresh.getObjectName(), authorizer));
        break;
      default:
        throw new MetaException("Unexpected object type " + objToRefresh.getObjectType());
      }
      if (grants != null) {
        for (HiveObjectPrivilege grant : grants) {
          revokePrivilegeSet.add(grant);
        }
      }

      // Optimize revoke/grant list, remove the overlapping
      if (grantPrivileges.getPrivileges() != null) {
        for (HiveObjectPrivilege grantPrivilege : grantPrivileges.getPrivileges()) {
          if (revokePrivilegeSet.contains(grantPrivilege)) {
            revokePrivilegeSet.remove(grantPrivilege);
          } else {
            grantPrivilegeSet.add(grantPrivilege);
          }
        }
      }
      if (!revokePrivilegeSet.isEmpty()) {
        PrivilegeBag remainingRevokePrivileges = new PrivilegeBag();
        for (HiveObjectPrivilege revokePrivilege : revokePrivilegeSet) {
          remainingRevokePrivileges.addToPrivileges(revokePrivilege);
        }
        revokePrivileges(remainingRevokePrivileges, false);
      }
      if (!grantPrivilegeSet.isEmpty()) {
        PrivilegeBag remainingGrantPrivileges = new PrivilegeBag();
        for (HiveObjectPrivilege grantPrivilege : grantPrivilegeSet) {
          remainingGrantPrivileges.addToPrivileges(grantPrivilege);
        }
        grantPrivileges(remainingGrantPrivileges);
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
    List<MRoleMap> mRoleMemeberList = new ArrayList<>();
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
      rollbackAndCleanup(success, query);
    }
    return mRoleMemeberList;
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    List<MRoleMap> roleMaps = listMRoleMembers(roleName);
    List<RolePrincipalGrant> rolePrinGrantList = new ArrayList<>();

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

  private List<MGlobalPrivilege> listPrincipalMGlobalGrants(String principalName,
      PrincipalType principalType) {
    return listPrincipalMGlobalGrants(principalName, principalType, null);
  }

  @SuppressWarnings("unchecked")
  private List<MGlobalPrivilege> listPrincipalMGlobalGrants(String principalName,
      PrincipalType principalType, String authorizer) {
    boolean commited = false;
    Query query = null;
    List<MGlobalPrivilege> userNameDbPriv = new ArrayList<>();
    try {
      List<MGlobalPrivilege> mPrivs = null;
      openTransaction();
      if (principalName != null) {
        if (authorizer != null) {
          query = pm.newQuery(MGlobalPrivilege.class, "principalName == t1 && principalType == t2 "
              + "&& authorizer == t3");
          query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
          mPrivs = (List<MGlobalPrivilege>) query
                  .executeWithArray(principalName, principalType.toString(), authorizer);
        } else {
          query = pm.newQuery(MGlobalPrivilege.class, "principalName == t1 && principalType == t2 ");
          query.declareParameters("java.lang.String t1, java.lang.String t2");
          mPrivs = (List<MGlobalPrivilege>) query
                  .executeWithArray(principalName, principalType.toString());
        }
        pm.retrieveAll(mPrivs);
      }
      commited = commitTransaction();
      if (mPrivs != null) {
        userNameDbPriv.addAll(mPrivs);
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return userNameDbPriv;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
                                                             PrincipalType principalType) {
    List<MGlobalPrivilege> mUsers =
        listPrincipalMGlobalGrants(principalName, principalType);
    if (mUsers.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mUsers.size(); i++) {
      MGlobalPrivilege sUsr = mUsers.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.GLOBAL, null, null, null, null);
      HiveObjectPrivilege secUser = new HiveObjectPrivilege(
          objectRef, sUsr.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sUsr.getPrivilege(), sUsr
              .getCreateTime(), sUsr.getGrantor(), PrincipalType
              .valueOf(sUsr.getGrantorType()), sUsr.getGrantOption()),
          sUsr.getAuthorizer());
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
      rollbackAndCleanup(commited, query);
    }
  }

  private List<HiveObjectPrivilege> convertGlobal(List<MGlobalPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MGlobalPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.GLOBAL, null, null, null, null);
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
    }
    return result;
  }

  private List<MDBPrivilege> listPrincipalMDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName) {
    return listPrincipalMDBGrants(principalName, principalType, catName, dbName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listPrincipalMDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String authorizer) {
    boolean success = false;
    Query query = null;
    List<MDBPrivilege> mSecurityDBList = new ArrayList<>();
    dbName = normalizeIdentifier(dbName);
    try {
      LOG.debug("Executing listPrincipalDBGrants");

      openTransaction();
      List<MDBPrivilege> mPrivs;
      if (authorizer != null) {
        query = pm.newQuery(MDBPrivilege.class,
            "principalName == t1 && principalType == t2 && database.name == t3 && " +
            "database.catalogName == t4 && authorizer == t5");
        query.declareParameters(
            "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
            + "java.lang.String t5");
        mPrivs = (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(),
                dbName, catName, authorizer);
      } else {
        query = pm.newQuery(MDBPrivilege.class,
                "principalName == t1 && principalType == t2 && database.name == t3 && database.catalogName == t4");
        query.declareParameters(
            "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4");
        mPrivs = (List<MDBPrivilege>) query.executeWithArray(principalName, principalType.toString(),
                dbName, catName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityDBList.addAll(mPrivs);
      LOG.debug("Done retrieving all objects for listPrincipalDBGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityDBList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String catName, String dbName) {
    List<MDBPrivilege> mDbs = listPrincipalMDBGrants(principalName, principalType, catName, dbName);
    if (mDbs.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mDbs.size(); i++) {
      MDBPrivilege sDB = mDbs.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.DATABASE, dbName, null, null, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sDB.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sDB.getPrivilege(), sDB
              .getCreateTime(), sDB.getGrantor(), PrincipalType
              .valueOf(sDB.getGrantorType()), sDB.getGrantOption()), sDB.getAuthorizer());
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
  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return listDBGrantsAll(catName, dbName, null);
  }

  private List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName, String authorizer) {
    QueryWrapper queryWrapper = new QueryWrapper();
    try {
      return convertDB(listDatabaseGrants(catName, dbName, authorizer, queryWrapper));
      } finally {
        queryWrapper.close();
      }
  }

  private List<HiveObjectPrivilege> convertDB(List<MDBPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MDBPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());
      String database = priv.getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.DATABASE, database,
          null, null, null);
      objectRef.setCatName(priv.getDatabase().getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
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
  private List<MTablePrivilege> listAllTableGrants(String catName, String dbName, String tableName) {
    boolean success = false;
    Query query = null;
    List<MTablePrivilege> mSecurityTabList = new ArrayList<>();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    try {
      LOG.debug("Executing listAllTableGrants");

      openTransaction();
      String queryStr = "table.tableName == t1 && table.database.name == t2" +
          "&& table.database.catalogName == t3";
      query = pm.newQuery(MTablePrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      List<MTablePrivilege> mPrivs  =
          (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName);
      LOG.debug("Done executing query for listAllTableGrants");
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityTabList;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listTableAllPartitionGrants(String catName, String dbName, String tableName) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    boolean success = false;
    Query query = null;
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<>();
    try {
      LOG.debug("Executing listTableAllPartitionGrants");

      openTransaction();
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2 " +
          "&& partition.table.database.catalogName == t3";
      query = pm.newQuery(MPartitionPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      List<MPartitionPrivilege> mPrivs =
          (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName, catName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllPartitionGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityTabPartList;
  }

  private List<MTableColumnPrivilege> listTableAllColumnGrants(
      String catName, String dbName, String tableName) {
    return listTableAllColumnGrants(catName, dbName, tableName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MTableColumnPrivilege> listTableAllColumnGrants(
      String catName, String dbName, String tableName, String authorizer) {
    boolean success = false;
    Query query = null;
    List<MTableColumnPrivilege> mTblColPrivilegeList = new ArrayList<>();
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    try {
      LOG.debug("Executing listTableAllColumnGrants");

      openTransaction();
      List<MTableColumnPrivilege> mPrivs = null;
      if (authorizer != null) {
        String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
            "table.database.catalogName == t3 && authorizer == t4";
        query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
            "java.lang.String t4");
        mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName, authorizer);
      } else {
        String queryStr = "table.tableName == t1 && table.database.name == t2 &&" +
            "table.database.catalogName == t3";
        query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
        mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName, catName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mTblColPrivilegeList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllColumnGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mTblColPrivilegeList;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionColumnPrivilege> listTableAllPartitionColumnGrants(
      String catName, String dbName, String tableName) {
    boolean success = false;
    Query query = null;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<>();
    try {
      LOG.debug("Executing listTableAllPartitionColumnGrants");

      openTransaction();
      String queryStr = "partition.table.tableName == t1 && partition.table.database.name == t2 " +
          "&& partition.table.database.catalogName == t3";
      query = pm.newQuery(MPartitionColumnPrivilege.class, queryStr);
      query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
      List<MPartitionColumnPrivilege> mPrivs =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName, catName);
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listTableAllPartitionColumnGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityColList;
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionColumnPrivilege> listPartitionAllColumnGrants(
      String catName, String dbName, String tableName, List<String> partNames) {
    boolean success = false;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);

    List<MPartitionColumnPrivilege> mSecurityColList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionAllColumnGrants");
      mSecurityColList = queryByPartitionNames(catName,
          dbName, tableName, partNames, MPartitionColumnPrivilege.class,
          "partition.table.tableName", "partition.table.database.name", "partition.partitionName",
          "partition.table.database.catalogName");
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

  private void dropPartitionAllColumnGrantsNoTxn(
      String catName, String dbName, String tableName, List<String> partNames) {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(catName,
          dbName, tableName, partNames, MPartitionColumnPrivilege.class,
          "partition.table.tableName", "partition.table.database.name", "partition.partitionName",
          "partition.table.database.catalogName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @SuppressWarnings("unchecked")
  private List<MDBPrivilege> listDatabaseGrants(String catName, String dbName,
      String authorizer, QueryWrapper queryWrapper) {
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    boolean success = false;
    try {
      LOG.debug("Executing listDatabaseGrants");

      openTransaction();
      List<MDBPrivilege> mSecurityDBList = null;
      if (authorizer != null) {
        Query query = queryWrapper.query = pm.newQuery(MDBPrivilege.class,
            "database.name == t1 && database.catalogName == t2 && authorizer == t3");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
        mSecurityDBList = (List<MDBPrivilege>) query.executeWithArray(dbName, catName, authorizer);
      } else {
        Query query = queryWrapper.query = pm.newQuery(MDBPrivilege.class,
            "database.name == t1 && database.catalogName == t2");
        query.declareParameters("java.lang.String t1, java.lang.String t2");
        mSecurityDBList = (List<MDBPrivilege>) query.executeWithArray(dbName, catName);
      }
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
  private List<MPartitionPrivilege> listPartitionGrants(String catName, String dbName, String tableName,
      List<String> partNames) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);

    boolean success = false;
    List<MPartitionPrivilege> mSecurityTabPartList = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionGrants");
      mSecurityTabPartList = queryByPartitionNames(catName,
          dbName, tableName, partNames, MPartitionPrivilege.class, "partition.table.tableName",
          "partition.table.database.name", "partition.partitionName",
          "partition.table.database.catalogName");
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

  private void dropPartitionGrantsNoTxn(String catName, String dbName, String tableName,
                                        List<String> partNames) {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(catName,
          dbName, tableName, partNames,MPartitionPrivilege.class, "partition.table.tableName",
          "partition.table.database.name", "partition.partitionName",
          "partition.table.database.catalogName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> queryByPartitionNames(String catName, String dbName, String tableName,
      List<String> partNames, Class<T> clazz, String tbCol, String dbCol, String partCol,
      String catCol) {
    ObjectPair<Query, Object[]> queryAndParams = makeQueryByPartitionNames(catName,
        dbName, tableName, partNames, clazz, tbCol, dbCol, partCol, catCol);
    return (List<T>)queryAndParams.getFirst().executeWithArray(queryAndParams.getSecond());
  }

  private ObjectPair<Query, Object[]> makeQueryByPartitionNames(
      String catName, String dbName, String tableName, List<String> partNames, Class<?> clazz,
      String tbCol, String dbCol, String partCol, String catCol) {
    String queryStr = tbCol + " == t1 && " + dbCol + " == t2 && " + catCol + " == t3";
    String paramStr = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
    Object[] params = new Object[3 + partNames.size()];
    params[0] = normalizeIdentifier(tableName);
    params[1] = normalizeIdentifier(dbName);
    params[2] = normalizeIdentifier(catName);
    int index = 0;
    for (String partName : partNames) {
      params[index + 3] = partName;
      queryStr += ((index == 0) ? " && (" : " || ") + partCol + " == p" + index;
      paramStr += ", java.lang.String p" + index;
      ++index;
    }
    queryStr += ")";
    Query query = pm.newQuery(clazz, queryStr);
    query.declareParameters(paramStr);
    return new ObjectPair<>(query, params);
  }

  private List<MTablePrivilege> listAllMTableGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName) {
    return listAllMTableGrants(principalName, principalType, catName, dbName, tableName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MTablePrivilege> listAllMTableGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String authorizer) {
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    boolean success = false;
    Query query = null;
    List<MTablePrivilege> mSecurityTabPartList = new ArrayList<>();
    try {
      openTransaction();
      LOG.debug("Executing listAllTableGrants");
      List<MTablePrivilege> mPrivs;
      if (authorizer != null) {
        query = pm.newQuery(MTablePrivilege.class,
            "principalName == t1 && principalType == t2 && table.tableName == t3 &&" +
                "table.database.name == t4 && table.database.catalogName == t5 && authorizer == t6");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3," +
            "java.lang.String t4, java.lang.String t5, java.lang.String t6");
        mPrivs = (List<MTablePrivilege>) query.executeWithArray(principalName, principalType.toString(),
            tableName, dbName, catName, authorizer);
      } else {
        query = pm.newQuery(MTablePrivilege.class,
                "principalName == t1 && principalType == t2 && table.tableName == t3 &&" +
                    "table.database.name == t4 && table.database.catalogName == t5");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3," +
                "java.lang.String t4, java.lang.String t5");
        mPrivs = (List<MTablePrivilege>) query.executeWithArray(principalName, principalType.toString(),
                tableName, dbName, catName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listAllTableGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType,
                                                      String catName,
                                                      String dbName,
                                                      String tableName) {
    List<MTablePrivilege> mTbls =
        listAllMTableGrants(principalName, principalType, catName, dbName, tableName);
    if (mTbls.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mTbls.size(); i++) {
      MTablePrivilege sTbl = mTbls.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.TABLE, dbName, tableName, null, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sTbl.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sTbl.getPrivilege(), sTbl.getCreateTime(), sTbl
              .getGrantor(), PrincipalType.valueOf(sTbl
              .getGrantorType()), sTbl.getGrantOption()), sTbl.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  private List<MPartitionPrivilege> listPrincipalMPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partName) {
    return listPrincipalMPartitionGrants(principalName, principalType, catName, dbName, tableName, partName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionPrivilege> listPrincipalMPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partName, String authorizer) {
    boolean success = false;
    Query query = null;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    catName = normalizeIdentifier(catName);
    List<MPartitionPrivilege> mSecurityTabPartList = new ArrayList<>();
    try {
      LOG.debug("Executing listPrincipalPartitionGrants");

      openTransaction();
      List<MPartitionPrivilege> mPrivs;
      if (authorizer != null) {
        query = pm.newQuery(MPartitionPrivilege.class,
            "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5"
                + "&& partition.partitionName == t6 && authorizer == t7");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
                + "java.lang.String t5, java.lang.String t6, java.lang.String t7");
        mPrivs = (List<MPartitionPrivilege>) query.executeWithArray(principalName,
        principalType.toString(), tableName, dbName, catName, partName, authorizer);
      } else {
        query = pm.newQuery(MPartitionPrivilege.class,
                "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                    + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5"
                    + "&& partition.partitionName == t6");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4, "
                + "java.lang.String t5, java.lang.String t6");
        mPrivs = (List<MPartitionPrivilege>) query.executeWithArray(principalName,
            principalType.toString(), tableName, dbName, catName, partName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityTabPartList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalPartitionGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityTabPartList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String catName,
                                                                String dbName,
                                                                String tableName,
                                                                List<String> partValues,
                                                                String partName) {
    List<MPartitionPrivilege> mParts = listPrincipalMPartitionGrants(principalName,
        principalType, catName, dbName, tableName, partName);
    if (mParts.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mParts.size(); i++) {
      MPartitionPrivilege sPart = mParts.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.PARTITION, dbName, tableName, partValues, null);
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sPart.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sPart.getPrivilege(), sPart
              .getCreateTime(), sPart.getGrantor(), PrincipalType
              .valueOf(sPart.getGrantorType()), sPart
              .getGrantOption()), sPart.getAuthorizer());

      result.add(secObj);
    }
    return result;
  }

  private List<MTableColumnPrivilege> listPrincipalMTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName) {
    return listPrincipalMTableColumnGrants(principalName, principalType, catName, dbName, tableName,
        columnName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MTableColumnPrivilege> listPrincipalMTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName, String authorizer) {
    boolean success = false;
    Query query = null;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    List<MTableColumnPrivilege> mSecurityColList = new ArrayList<>();
    try {
      LOG.debug("Executing listPrincipalTableColumnGrants");

      openTransaction();
      List<MTableColumnPrivilege> mPrivs;
      if (authorizer != null) {
        String queryStr =
            "principalName == t1 && principalType == t2 && "
                + "table.tableName == t3 && table.database.name == t4 &&  " +
                "table.database.catalogName == t5 && columnName == t6 && authorizer == t7";
        query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
            + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7");
        mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(principalName,
                principalType.toString(), tableName, dbName, catName, columnName, authorizer);
      } else {
        String queryStr =
            "principalName == t1 && principalType == t2 && "
                + "table.tableName == t3 && table.database.name == t4 &&  " +
                "table.database.catalogName == t5 && columnName == t6 ";
        query = pm.newQuery(MTableColumnPrivilege.class, queryStr);
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
            + "java.lang.String t4, java.lang.String t5, java.lang.String t6");
        mPrivs = (List<MTableColumnPrivilege>) query.executeWithArray(principalName,
                principalType.toString(), tableName, dbName, catName, columnName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalTableColumnGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                  PrincipalType principalType,
                                                                  String catName,
                                                                  String dbName,
                                                                  String tableName,
                                                                  String columnName) {
    List<MTableColumnPrivilege> mTableCols =
        listPrincipalMTableColumnGrants(principalName, principalType, catName, dbName, tableName, columnName);
    if (mTableCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mTableCols.size(); i++) {
      MTableColumnPrivilege sCol = mTableCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, null, sCol.getColumnName());
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(
          objectRef, sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol
              .getGrantOption()), sCol.getAuthorizer());
      result.add(secObj);
    }
    return result;
  }

  private List<MPartitionColumnPrivilege> listPrincipalMPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partitionName, String columnName) {
    return listPrincipalMPartitionColumnGrants(principalName, principalType, catName, dbName,
        tableName, partitionName, columnName, null);
  }

  @SuppressWarnings("unchecked")
  private List<MPartitionColumnPrivilege> listPrincipalMPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String partitionName, String columnName, String authorizer) {
    boolean success = false;
    Query query = null;
    tableName = normalizeIdentifier(tableName);
    dbName = normalizeIdentifier(dbName);
    columnName = normalizeIdentifier(columnName);
    catName = normalizeIdentifier(catName);
    List<MPartitionColumnPrivilege> mSecurityColList = new ArrayList<>();
    try {
      LOG.debug("Executing listPrincipalPartitionColumnGrants");

      openTransaction();
      List<MPartitionColumnPrivilege> mPrivs;
      if (authorizer != null) {
        query = pm.newQuery(
            MPartitionColumnPrivilege.class,
            "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5" +
                " && partition.partitionName == t6 && columnName == t7 && authorizer == t8");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
            + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7, "
            + "java.lang.String t8");
        mPrivs = (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
                principalType.toString(), tableName, dbName, catName, partitionName, columnName, authorizer);
      } else {
        query = pm.newQuery(
                MPartitionColumnPrivilege.class,
                "principalName == t1 && principalType == t2 && partition.table.tableName == t3 "
                    + "&& partition.table.database.name == t4 && partition.table.database.catalogName == t5" +
                    " && partition.partitionName == t6 && columnName == t7");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, "
            + "java.lang.String t4, java.lang.String t5, java.lang.String t6, java.lang.String t7");
        mPrivs = (List<MPartitionColumnPrivilege>) query.executeWithArray(principalName,
                principalType.toString(), tableName, dbName, catName, partitionName, columnName);
      }
      pm.retrieveAll(mPrivs);
      success = commitTransaction();

      mSecurityColList.addAll(mPrivs);

      LOG.debug("Done retrieving all objects for listPrincipalPartitionColumnGrants");
    } finally {
      rollbackAndCleanup(success, query);
    }
    return mSecurityColList;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                      PrincipalType principalType,
                                                                      String catName,
                                                                      String dbName,
                                                                      String tableName,
                                                                      List<String> partValues,
                                                                      String partitionName,
                                                                      String columnName) {
    List<MPartitionColumnPrivilege> mPartitionCols =
        listPrincipalMPartitionColumnGrants(principalName, principalType, catName, dbName, tableName,
            partitionName, columnName);
    if (mPartitionCols.isEmpty()) {
      return Collections.emptyList();
    }
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (int i = 0; i < mPartitionCols.size(); i++) {
      MPartitionColumnPrivilege sCol = mPartitionCols.get(i);
      HiveObjectRef objectRef = new HiveObjectRef(
          HiveObjectType.COLUMN, dbName, tableName, partValues, sCol.getColumnName());
      objectRef.setCatName(catName);
      HiveObjectPrivilege secObj = new HiveObjectPrivilege(objectRef,
          sCol.getPrincipalName(), principalType,
          new PrivilegeGrantInfo(sCol.getPrivilege(), sCol
              .getCreateTime(), sCol.getGrantor(), PrincipalType
              .valueOf(sCol.getGrantorType()), sCol.getGrantOption()), sCol.getAuthorizer());
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
      rollbackAndCleanup(success, query);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
      String catName, String dbName, String tableName, String partitionName, String columnName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPartitionColumnGrantsAll");
      query =
          pm.newQuery(MPartitionColumnPrivilege.class,
              "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                  + "partition.table.database.name == t5 && "
                  + "partition.partitionName == t6 && columnName == t7");
      query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5," +
          "java.lang.String t6, java.lang.String t7");
      List<MPartitionColumnPrivilege> mSecurityTabPartList =
          (List<MPartitionColumnPrivilege>) query.executeWithArray(tableName, dbName, catName,
              partitionName, columnName);
      LOG.debug("Done executing query for listPartitionColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPartitionColumnGrantsAll");
      return result;
    } finally {
      rollbackAndCleanup(success, query);
    }
  }

  private List<HiveObjectPrivilege> convertPartCols(List<MPartitionColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MPartitionColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), priv.getColumnName());
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
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
      rollbackAndCleanup(success, query);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName) {
    return listTableGrantsAll(catName, dbName, tableName, null);
  }

  private List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName,
      String authorizer) {
    boolean success = false;
    Query query = null;
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    try {
      openTransaction();
      LOG.debug("Executing listTableGrantsAll");
      List<MTablePrivilege> mSecurityTabPartList = null;
      if (authorizer != null) {
        query = pm.newQuery(MTablePrivilege.class,
                "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3" +
                " && authorizer == t4");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, " +
                "java.lang.String t4");
        mSecurityTabPartList = (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName, authorizer);
      } else {
        query = pm.newQuery(MTablePrivilege.class,
                "table.tableName == t1 && table.database.name == t2 && table.database.catalogName == t3");
        query.declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3");
        mSecurityTabPartList = (List<MTablePrivilege>) query.executeWithArray(tableName, dbName, catName);
      }
      LOG.debug("Done executing query for listTableGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTable(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalAllTableGrants");
      return result;
    } finally {
      rollbackAndCleanup(success, query);
    }
  }

  private List<HiveObjectPrivilege> convertTable(List<MTablePrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MTablePrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      String table = priv.getTable().getTableName();
      String database = priv.getTable().getDatabase().getName();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.TABLE, database, table,
          null, null);
      objectRef.setCatName(priv.getTable().getDatabase().getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
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
      rollbackAndCleanup(success, query);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName, String tableName,
      String partitionName) {
    boolean success = false;
    Query query = null;
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalPartitionGrantsAll");
      query =
          pm.newQuery(MPartitionPrivilege.class,
              "partition.table.tableName == t3 && partition.table.database.name == t4 && "
                  + "partition.table.database.catalogName == t5 && partition.partitionName == t6");
      query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5, " +
          "java.lang.String t6");
      List<MPartitionPrivilege> mSecurityTabPartList =
          (List<MPartitionPrivilege>) query.executeWithArray(tableName, dbName, catName, partitionName);
      LOG.debug("Done executing query for listPrincipalPartitionGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertPartition(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalPartitionGrantsAll");
      return result;
    } finally {
      rollbackAndCleanup(success, query);
    }
  }

  private List<HiveObjectPrivilege> convertPartition(List<MPartitionPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MPartitionPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MPartition mpartition = priv.getPartition();
      MTable mtable = mpartition.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.PARTITION,
          mdatabase.getName(), mtable.getTableName(), mpartition.getValues(), null);
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
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
      rollbackAndCleanup(success, query);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName, String tableName,
      String columnName) {
    boolean success = false;
    Query query = null;
    dbName = normalizeIdentifier(dbName);
    tableName = normalizeIdentifier(tableName);
    try {
      openTransaction();
      LOG.debug("Executing listPrincipalTableColumnGrantsAll");
      query =
          pm.newQuery(MTableColumnPrivilege.class,
              "table.tableName == t3 && table.database.name == t4 && " +
                  "table.database.catalogName == t5 && columnName == t6");
      query.declareParameters("java.lang.String t3, java.lang.String t4, java.lang.String t5, " +
          "java.lang.String t6");
      List<MTableColumnPrivilege> mSecurityTabPartList =
          (List<MTableColumnPrivilege>) query.executeWithArray(tableName, dbName,
              catName, columnName);
      LOG.debug("Done executing query for listPrincipalTableColumnGrantsAll");
      pm.retrieveAll(mSecurityTabPartList);
      List<HiveObjectPrivilege> result = convertTableCols(mSecurityTabPartList);
      success = commitTransaction();
      LOG.debug("Done retrieving all objects for listPrincipalTableColumnGrantsAll");
      return result;
    } finally {
      rollbackAndCleanup(success, query);
    }
  }

  private List<HiveObjectPrivilege> convertTableCols(List<MTableColumnPrivilege> privs) {
    List<HiveObjectPrivilege> result = new ArrayList<>();
    for (MTableColumnPrivilege priv : privs) {
      String pname = priv.getPrincipalName();
      String authorizer = priv.getAuthorizer();
      PrincipalType ptype = PrincipalType.valueOf(priv.getPrincipalType());

      MTable mtable = priv.getTable();
      MDatabase mdatabase = mtable.getDatabase();

      HiveObjectRef objectRef = new HiveObjectRef(HiveObjectType.COLUMN,
          mdatabase.getName(), mtable.getTableName(), null, priv.getColumnName());
      objectRef.setCatName(mdatabase.getCatalogName());
      PrivilegeGrantInfo grantor = new PrivilegeGrantInfo(priv.getPrivilege(), priv.getCreateTime(),
          priv.getGrantor(), PrincipalType.valueOf(priv.getGrantorType()), priv.getGrantOption());

      result.add(new HiveObjectPrivilege(objectRef, pname, ptype, grantor, authorizer));
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
  public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType) throws UnknownTableException,
      MetaException, InvalidPartitionException, UnknownPartitionException {
    boolean success = false;
    Query query = null;

    try {
      LOG.debug("Begin Executing isPartitionMarkedForEvent");

      openTransaction();
      query = pm.newQuery(MPartitionEvent.class,
              "dbName == t1 && tblName == t2 && partName == t3 && eventType == t4 && catalogName == t5");
      query
          .declareParameters("java.lang.String t1, java.lang.String t2, java.lang.String t3, int t4," +
              "java.lang.String t5");
      Table tbl = getTable(catName, dbName, tblName); // Make sure dbName and tblName are valid.
      if (null == tbl) {
        throw new UnknownTableException("Table: " + tblName + " is not found.");
      }
      Collection<MPartitionEvent> partEvents =
          (Collection<MPartitionEvent>) query.executeWithArray(dbName, tblName,
              getPartitionStr(tbl, partName), evtType.getValue(), catName);
      pm.retrieveAll(partEvents);
      success = commitTransaction();

      LOG.debug("Done executing isPartitionMarkedForEvent");
      return (partEvents != null && !partEvents.isEmpty()) ? true : false;
    } finally {
      rollbackAndCleanup(success, query);
    }
  }

  @Override
  public Table markPartitionForEvent(String catName, String dbName, String tblName, Map<String,String> partName,
      PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {

    LOG.debug("Begin executing markPartitionForEvent");
    boolean success = false;
    Table tbl = null;
    try{
    openTransaction();
    tbl = getTable(catName, dbName, tblName); // Make sure dbName and tblName are valid.
    if(null == tbl) {
      throw new UnknownTableException("Table: "+ tblName + " is not found.");
    }
    pm.makePersistent(new MPartitionEvent(catName, dbName,tblName,getPartitionStr(tbl, partName), evtType.getValue()));
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
    final List<String> storedVals = new ArrayList<>(tbl.getPartitionKeysSize());
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
      rollbackAndCleanup(committed, query);
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
    Set<String> fsRoots = new HashSet<>();
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
      rollbackAndCleanup(committed, query);
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
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
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
      rollbackAndCleanup(committed, query);
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
      rollbackAndCleanup(committed, query);
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
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
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
      rollbackAndCleanup(committed, query);
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
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
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
      rollbackAndCleanup(committed, query);
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
    Map<String, String> updateLocations = new HashMap<>();
    List<String> badRecords = new ArrayList<>();
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
      rollbackAndCleanup(committed, query);
    }
  }

  private void writeMTableColumnStatistics(Table table, MTableColumnStatistics mStatsObj,
      MTableColumnStatistics oldStats) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    String colName = mStatsObj.getColName();
    QueryWrapper queryWrapper = new QueryWrapper();

    try {
      LOG.info("Updating table level column statistics for table={}" +
        " colName={}", Warehouse.getCatalogQualifiedTableName(table), colName);
      validateTableCols(table, Lists.newArrayList(colName));

      if (oldStats != null) {
        StatObjectConverter.setFieldsIntoOldStats(mStatsObj, oldStats);
      } else {
        if (sqlGenerator.getDbProduct().equals(DatabaseProduct.POSTGRES) && mStatsObj.getBitVector() == null) {
          // workaround for DN bug in persisting nulls in pg bytea column
          // instead set empty bit vector with header.
          mStatsObj.setBitVector(new byte[] {'H','L'});
        }
        pm.makePersistent(mStatsObj);
      }
    } finally {
      queryWrapper.close();
    }
  }

  private void writeMPartitionColumnStatistics(Table table, Partition partition,
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    String catName = mStatsObj.getCatName();
    String dbName = mStatsObj.getDbName();
    String tableName = mStatsObj.getTableName();
    String partName = mStatsObj.getPartitionName();
    String colName = mStatsObj.getColName();

    LOG.info("Updating partition level column statistics for table=" +
        TableName.getQualified(catName, dbName, tableName) +
        " partName=" + partName + " colName=" + colName);

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
        if (sqlGenerator.getDbProduct().equals(DatabaseProduct.POSTGRES) && mStatsObj.getBitVector() == null) {
          // workaround for DN bug in persisting nulls in pg bytea column
          // instead set empty bit vector with header.
          mStatsObj.setBitVector(new byte[] {'H','L'});
        }
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
      String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
      Table table = ensureGetTable(catName, statsDesc.getDbName(), statsDesc.getTableName());
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      Map<String, MTableColumnStatistics> oldStats = getPartitionColStats(table, colNames);

      for (ColumnStatisticsObj statsObj:statsObjs) {
        // We have to get mtable again because DataNucleus.
        MTableColumnStatistics mStatsObj = StatObjectConverter.convertToMTableColumnStatistics(
            ensureGetMTable(catName, statsDesc.getDbName(), statsDesc.getTableName()), statsDesc,
            statsObj);
        writeMTableColumnStatistics(table, mStatsObj, oldStats.get(statsObj.getColName()));
        // There is no need to add colname again, otherwise we will get duplicate colNames.
      }

      // Set the table properties
      // No need to check again if it exists.
      String dbname = table.getDbName();
      String name = table.getTableName();
      MTable oldt = getMTable(catName, dbname, name);
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
      String catName = statsDesc.isSetCatName() ? statsDesc.getCatName() : getDefaultCatalog(conf);
      Table table = ensureGetTable(catName, statsDesc.getDbName(), statsDesc.getTableName());
      Partition partition = convertToPart(getMPartition(
          catName, statsDesc.getDbName(), statsDesc.getTableName(), partVals));
      List<String> colNames = new ArrayList<>();

      for(ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }

      Map<String, MPartitionColumnStatistics> oldStats = getPartitionColStats(table, statsDesc
          .getPartName(), colNames);

      MPartition mPartition = getMPartition(
          catName, statsDesc.getDbName(), statsDesc.getTableName(), partVals);
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
      return Collections.emptyList();
    }

    boolean committed = false;
    try {
      openTransaction();

      validateTableCols(table, colNames);
      Query query = queryWrapper.query = pm.newQuery(MTableColumnStatistics.class);
      List<MTableColumnStatistics> result =
          Batchable.runBatched(batchSize, colNames, new Batchable<String, MTableColumnStatistics>() {
            @Override
            public List<MTableColumnStatistics> run(List<String> input)
                throws MetaException {
              String filter = "tableName == t1 && dbName == t2 && catName == t3 && (";
              String paramStr = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
              Object[] params = new Object[input.size() + 3];
              params[0] = table.getTableName();
              params[1] = table.getDbName();
              params[2] = table.getCatName();
              for (int i = 0; i < input.size(); ++i) {
                filter += ((i == 0) ? "" : " || ") + "colName == c" + i;
                paramStr += ", java.lang.String c" + i;
                params[i + 3] = input.get(i);
              }
              filter += ")";
              query.setFilter(filter);
              query.declareParameters(paramStr);
              List<MTableColumnStatistics> paritial = (List<MTableColumnStatistics>) query.executeWithArray(params);
              pm.retrieveAll(paritial);
              return paritial;
            }
          });

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
  public ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    return getTableColumnStatisticsInternal(catName, dbName, tableName, colNames, true, true);
  }

  protected ColumnStatistics getTableColumnStatisticsInternal(
      String catName, String dbName, String tableName, final List<String> colNames, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    return new GetStatHelper(normalizeIdentifier(catName), normalizeIdentifier(dbName),
        normalizeIdentifier(tableName), allowSql, allowJdo) {
      @Override
      protected ColumnStatistics getSqlResult(GetHelper<ColumnStatistics> ctx) throws MetaException {
        return directSql.getTableStats(catName, dbName, tblName, colNames, enableBitVector);
      }
      @Override
      protected ColumnStatistics getJdoResult(
          GetHelper<ColumnStatistics> ctx) throws MetaException {
        QueryWrapper queryWrapper = new QueryWrapper();

        try {
        List<MTableColumnStatistics> mStats = getMTableColumnStatistics(getTable(), colNames, queryWrapper);
        if (mStats.isEmpty()) {
          return null;
        }
        // LastAnalyzed is stored per column, but thrift object has it per multiple columns.
        // Luckily, nobody actually uses it, so we will set to lowest value of all columns for now.
        ColumnStatisticsDesc desc = StatObjectConverter.getTableColumnStatisticsDesc(mStats.get(0));
        List<ColumnStatisticsObj> statObjs = new ArrayList<>(mStats.size());
        for (MTableColumnStatistics mStat : mStats) {
          if (desc.getLastAnalyzed() > mStat.getLastAnalyzed()) {
            desc.setLastAnalyzed(mStat.getLastAnalyzed());
          }
          statObjs.add(StatObjectConverter.getTableColumnStatisticsObj(mStat, enableBitVector));
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
  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return getPartitionColumnStatisticsInternal(
        catName, dbName, tableName, partNames, colNames, true, true);
  }

  protected List<ColumnStatistics> getPartitionColumnStatisticsInternal(
      String catName, String dbName, String tableName, final List<String> partNames, final List<String> colNames,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    return new GetListHelper<ColumnStatistics>(catName, dbName, tableName, allowSql, allowJdo) {
      @Override
      protected List<ColumnStatistics> getSqlResult(
          GetHelper<List<ColumnStatistics>> ctx) throws MetaException {
        return directSql.getPartitionStats(catName, dbName, tblName, partNames, colNames, enableBitVector);
      }
      @Override
      protected List<ColumnStatistics> getJdoResult(
          GetHelper<List<ColumnStatistics>> ctx) throws MetaException, NoSuchObjectException {
        QueryWrapper queryWrapper = new QueryWrapper();
        try {
          List<MPartitionColumnStatistics> mStats =
              getMPartitionColumnStatistics(getTable(), partNames, colNames, queryWrapper);
          List<ColumnStatistics> result = new ArrayList<>(
              Math.min(mStats.size(), partNames.size()));
          String lastPartName = null;
          List<ColumnStatisticsObj> curList = null;
          ColumnStatisticsDesc csd = null;
          for (int i = 0; i <= mStats.size(); ++i) {
            boolean isLast = i == mStats.size();
            MPartitionColumnStatistics mStatsObj = isLast ? null : mStats.get(i);
            String partName = isLast ? null : mStatsObj.getPartitionName();
            if (isLast || !partName.equals(lastPartName)) {
              if (i != 0) {
                result.add(new ColumnStatistics(csd, curList));
              }
              if (isLast) {
                continue;
              }
              csd = StatObjectConverter.getPartitionColumnStatisticsDesc(mStatsObj);
              curList = new ArrayList<>(colNames.size());
            }
            curList.add(StatObjectConverter.getPartitionColumnStatisticsObj(mStatsObj, enableBitVector));
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
  public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
      final List<String> partNames, final List<String> colNames) throws MetaException, NoSuchObjectException {
    final boolean useDensityFunctionForNDVEstimation = MetastoreConf.getBoolVar(getConf(),
        ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER);
    final boolean enableBitVector = MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    return new GetHelper<AggrStats>(catName, dbName, tblName, true, false) {
      @Override
      protected AggrStats getSqlResult(GetHelper<AggrStats> ctx)
          throws MetaException {
        return directSql.aggrColStatsForPartitions(catName, dbName, tblName, partNames,
            colNames, useDensityFunctionForNDVEstimation, ndvTuner, enableBitVector);
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
  public List<MetaStoreUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    final boolean enableBitVector =
        MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_FETCH_BITVECTOR);
    return new GetHelper<List<MetaStoreUtils.ColStatsObjWithSourceInfo>>(catName, dbName, null, true, false) {
      @Override
      protected List<MetaStoreUtils.ColStatsObjWithSourceInfo> getSqlResult(
          GetHelper<List<MetaStoreUtils.ColStatsObjWithSourceInfo>> ctx) throws MetaException {
        return directSql.getColStatsForAllTablePartitions(catName, dbName, enableBitVector);
      }

      @Override
      protected List<MetaStoreUtils.ColStatsObjWithSourceInfo> getJdoResult(
          GetHelper<List<MetaStoreUtils.ColStatsObjWithSourceInfo>> ctx)
          throws MetaException, NoSuchObjectException {
        // This is fast path for query optimizations, if we can find this info
        // quickly using directSql, do it. No point in failing back to slow path
        // here.
        throw new MetaException("Jdo path is not implemented for getPartitionColStatsForDatabase.");
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
      // ToDo: we need verify the partition column instead
      try {
        validateTableCols(table, colNames);
      } catch (MetaException me) {
        LOG.warn("The table does not have the same column definition as its partition.");
      }
      Query query = queryWrapper.query = pm.newQuery(MPartitionColumnStatistics.class);
      String paramStr = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      String filter = "tableName == t1 && dbName == t2 && catName == t3 && (";
      Object[] params = new Object[colNames.size() + partNames.size() + 3];
      int i = 0;
      params[i++] = table.getTableName();
      params[i++] = table.getDbName();
      params[i++] = table.isSetCatName() ? table.getCatName() : getDefaultCatalog(conf);
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
      String catName, String dbName, String tableName, List<String> partNames) throws MetaException {
    ObjectPair<Query, Object[]> queryWithParams = makeQueryByPartitionNames(
        catName, dbName, tableName, partNames, MPartitionColumnStatistics.class,
        "tableName", "dbName", "partition.partitionName", "catName");
    queryWithParams.getFirst().deletePersistentAll(queryWithParams.getSecond());
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
                                                 String partName, List<String> partVals,
                                                 String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    dbName = org.apache.commons.lang.StringUtils.defaultString(dbName,
      Warehouse.DEFAULT_DATABASE_NAME);
    catName = normalizeIdentifier(catName);
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    try {
      openTransaction();
      MTable mTable = getMTable(catName, dbName, tableName);
      MPartitionColumnStatistics mStatsObj;
      List<MPartitionColumnStatistics> mStatsObjColl;
      if (mTable == null) {
        throw new NoSuchObjectException("Table " + tableName
            + "  for which stats deletion is requested doesn't exist");
      }
      MPartition mPartition = getMPartition(catName, dbName, tableName, partVals);
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
                + "colName == t4 && catName == t5";
        parameters =
            "java.lang.String t1, java.lang.String t2, "
                + "java.lang.String t3, java.lang.String t4, java.lang.String t5";
      } else {
        filter = "partition.partitionName == t1 && dbName == t2 && tableName == t3 && catName == t4";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4";
      }
      query.setFilter(filter);
      query.declareParameters(parameters);
      if (colName != null) {
        query.setUnique(true);
        mStatsObj =
            (MPartitionColumnStatistics) query.executeWithArray(partName.trim(),
                normalizeIdentifier(dbName),
                normalizeIdentifier(tableName),
                normalizeIdentifier(colName),
                normalizeIdentifier(catName));
        pm.retrieve(mStatsObj);
        if (mStatsObj != null) {
          pm.deletePersistent(mStatsObj);
        } else {
          throw new NoSuchObjectException("Column stats doesn't exist for table="
              + TableName.getQualified(catName, dbName, tableName) +
              " partition=" + partName + " col=" + colName);
        }
      } else {
        mStatsObjColl =
            (List<MPartitionColumnStatistics>) query.executeWithArray(partName.trim(),
                normalizeIdentifier(dbName),
                normalizeIdentifier(tableName),
                normalizeIdentifier(catName));
        pm.retrieveAll(mStatsObjColl);
        if (mStatsObjColl != null) {
          pm.deletePersistentAll(mStatsObjColl);
        } else {
          throw new NoSuchObjectException("Column stats don't exist for table="
              + TableName.getQualified(catName, dbName, tableName) + " partition" + partName);
        }
      }
      ret = commitTransaction();
    } catch (NoSuchObjectException e) {
      rollbackTransaction();
      throw e;
    } finally {
      rollbackAndCleanup(ret, query);
    }
    return ret;
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
                                             String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean ret = false;
    Query query = null;
    dbName = org.apache.commons.lang.StringUtils.defaultString(dbName,
      Warehouse.DEFAULT_DATABASE_NAME);
    if (tableName == null) {
      throw new InvalidInputException("Table name is null.");
    }
    try {
      openTransaction();
      MTable mTable = getMTable(catName, dbName, tableName);
      MTableColumnStatistics mStatsObj;
      List<MTableColumnStatistics> mStatsObjColl;
      if (mTable == null) {
        throw new NoSuchObjectException("Table " +
            TableName.getQualified(catName, dbName, tableName)
            + "  for which stats deletion is requested doesn't exist");
      }
      query = pm.newQuery(MTableColumnStatistics.class);
      String filter;
      String parameters;
      if (colName != null) {
        filter = "table.tableName == t1 && dbName == t2 && catName == t3 && colName == t4";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3, java.lang.String t4";
      } else {
        filter = "table.tableName == t1 && dbName == t2 && catName == t3";
        parameters = "java.lang.String t1, java.lang.String t2, java.lang.String t3";
      }

      query.setFilter(filter);
      query.declareParameters(parameters);
      if (colName != null) {
        query.setUnique(true);
        mStatsObj =
            (MTableColumnStatistics) query.executeWithArray(normalizeIdentifier(tableName),
                normalizeIdentifier(dbName),
                normalizeIdentifier(catName),
                normalizeIdentifier(colName));
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
                normalizeIdentifier(tableName),
                normalizeIdentifier(dbName),
                normalizeIdentifier(catName));
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
      rollbackAndCleanup(ret, query);
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
        MetastoreConf.getTimeVar(getConf(), ConfVars.EVENT_EXPIRY_DURATION, TimeUnit.MILLISECONDS);
    Long curTime = System.currentTimeMillis();
    try {
      openTransaction();
      query = pm.newQuery(MPartitionEvent.class, "curTime - eventTime > expiryTime");
      query.declareParameters("java.lang.Long curTime, java.lang.Long expiryTime");
      delCnt = query.deletePersistentAll(curTime, expiryTime);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
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
    LOG.debug("Done executing addToken with status : {}", committed);
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
    LOG.debug("Done executing removeToken with status : {}", committed);
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
    LOG.debug("Done executing getToken with status : {}", committed);
    return (null == token) ? null : token.getTokenStr();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    LOG.debug("Begin executing getAllTokenIdentifiers");
    boolean committed = false;
    Query query = null;
    List<String> tokenIdents = new ArrayList<>();

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
      LOG.debug("Done executing getAllTokenIdentifers with status : {}", committed);
      rollbackAndCleanup(committed, query);
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
    LOG.debug("Done executing addMasterKey with status : {}", committed);
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
      rollbackAndCleanup(committed, query);
    }
    LOG.debug("Done executing updateMasterKey with status : {}", committed);
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
      rollbackAndCleanup(success, query);
    }
    LOG.debug("Done executing removeMasterKey with status : {}", success);
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
      LOG.debug("Done executing getMasterKeys with status : {}", committed);
      rollbackAndCleanup(committed, query);
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

    boolean strictValidation = MetastoreConf.getBoolVar(getConf(), ConfVars.SCHEMA_VERIFICATION);
    // read the schema version stored in metastore db
    String dbSchemaVer = getMetaStoreSchemaVersion();
    // version of schema for this version of hive
    IMetaStoreSchemaInfo metastoreSchemaInfo = MetaStoreSchemaInfoFactory.get(getConf());
    String hiveSchemaVer = metastoreSchemaInfo.getHiveSchemaVersion();

    if (dbSchemaVer == null) {
      if (strictValidation) {
        throw new MetaException("Version information not found in metastore.");
      } else {
        LOG.warn("Version information not found in metastore. {} is not " +
          "enabled so recording the schema version {}", ConfVars.SCHEMA_VERIFICATION,
            hiveSchemaVer);
        setMetaStoreSchemaVersion(hiveSchemaVer,
          "Set by MetaStore " + USER + "@" + HOSTNAME);
      }
    } else {
      if (metastoreSchemaInfo.isVersionCompatible(hiveSchemaVer, dbSchemaVer)) {
        LOG.debug("Found expected HMS version of {}", dbSchemaVer);
      } else {
        // metastore schema version is different than Hive distribution needs
        if (strictValidation) {
          throw new MetaException("Hive Schema version " + hiveSchemaVer +
              " does not match metastore's schema version " + dbSchemaVer +
              " Metastore is not upgraded or corrupt");
        } else {
          LOG.error("Version information found in metastore differs {} " +
              "from expected schema version {}. Schema verification is disabled {}",
              dbSchemaVer, hiveSchemaVer, ConfVars.SCHEMA_VERIFICATION);
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
    List<MVersionTable> mVerTables = new ArrayList<>();
    try {
      openTransaction();
      query = pm.newQuery(MVersionTable.class);
      try {
        mVerTables = (List<MVersionTable>) query.execute();
        pm.retrieveAll(mVerTables);
      } catch (JDODataStoreException e) {
        if (e.getCause() instanceof MissingTableException) {
          throw new MetaException("Version table not found. " + "The metastore is not upgraded to "
              + MetaStoreSchemaInfoFactory.get(getConf()).getHiveSchemaVersion());
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
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public void setMetaStoreSchemaVersion(String schemaVersion, String comment) throws MetaException {
    MVersionTable mSchemaVer;
    boolean commited = false;
    boolean recordVersion =
      MetastoreConf.getBoolVar(getConf(), ConfVars.SCHEMA_VERIFICATION_RECORD_VERSION);
    if (!recordVersion) {
      LOG.warn("setMetaStoreSchemaVersion called but recording version is disabled: " +
        "version = {}, comment = {}", schemaVersion, comment);
      return;
    }
    LOG.warn("Setting metastore schema version in db to {}", schemaVersion);

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
  public boolean doesPartitionExist(String catName, String dbName, String tableName,
                                    List<FieldSchema> partKeys, List<String> partVals)
      throws MetaException {
    String name = Warehouse.makePartName(partKeys, partVals);
    return this.getMPartition(catName, dbName, tableName, name) != null;
  }

  private void debugLog(String message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} {}", message, getCallStack());
    }
  }

  private static final int stackLimit = 3;

  private String getCallStack() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    int thislimit = Math.min(stackLimit, stackTrace.length);
    StringBuilder sb = new StringBuilder();
    sb.append(" at:");
    // Offset by 4 because the first 4 frames are just calls to get down here.
    for (int i = 4; i < thislimit + 4; i++) {
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
    func.setCatName(mfunc.getDatabase().getCatalogName());
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
    String catName = func.isSetCatName() ? func.getCatName() : getDefaultCatalog(conf);
    try {
      mdb = getMDatabase(catName, func.getDbName());
    } catch (NoSuchObjectException e) {
      LOG.error("Database does not exist", e);
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
      resourceUriList = new ArrayList<>(mresourceUriList.size());
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
      mresourceUriList = new ArrayList<>(resourceUriList.size());
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
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    boolean success = false;
    try {
      String newFuncCat = newFunction.isSetCatName() ? newFunction.getCatName() :
          getDefaultCatalog(conf);
      if (!newFuncCat.equalsIgnoreCase(catName)) {
        throw new InvalidObjectException("You cannot move a function between catalogs");
      }
      openTransaction();
      catName = normalizeIdentifier(catName);
      funcName = normalizeIdentifier(funcName);
      dbName = normalizeIdentifier(dbName);
      MFunction newf = convertToMFunction(newFunction);
      if (newf == null) {
        throw new InvalidObjectException("new function is invalid");
      }

      MFunction oldf = getMFunction(catName, dbName, funcName);
      if (oldf == null) {
        throw new MetaException("function " + funcName + " doesn't exist");
      }

      // For now only alter name, owner, class name, type
      oldf.setFunctionName(normalizeIdentifier(newf.getFunctionName()));
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
  public void dropFunction(String catName, String dbName, String funcName) throws MetaException,
  NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean success = false;
    try {
      openTransaction();
      MFunction mfunc = getMFunction(catName, dbName, funcName);
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

  private MFunction getMFunction(String catName, String db, String function) {
    MFunction mfunc = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      db = normalizeIdentifier(db);
      function = normalizeIdentifier(function);
      query = pm.newQuery(MFunction.class,
          "functionName == function && database.name == db && database.catalogName == catName");
      query.declareParameters("java.lang.String function, java.lang.String db, java.lang.String catName");
      query.setUnique(true);
      mfunc = (MFunction) query.execute(function, db, catName);
      pm.retrieve(mfunc);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return mfunc;
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws MetaException {
    boolean commited = false;
    Function func = null;
    Query query = null;
    try {
      openTransaction();
      func = convertToFunction(getMFunction(catName, dbName, funcName));
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return func;
  }

  @Override
  public List<Function> getAllFunctions(String catName) throws MetaException {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      catName = normalizeIdentifier(catName);
      query = pm.newQuery(MFunction.class, "database.catalogName == catName");
      query.declareParameters("java.lang.String catName");
      List<MFunction> allFunctions = (List<MFunction>) query.execute(catName);
      pm.retrieveAll(allFunctions);
      commited = commitTransaction();
      return convertToFunctions(allFunctions);
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException {
    boolean commited = false;
    Query query = null;
    List<String> funcs = null;
    try {
      openTransaction();
      dbName = normalizeIdentifier(dbName);
      // Take the pattern and split it on the | to get all the composing
      // patterns
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder();
      appendSimpleCondition(filterBuilder, "database.name", new String[] { dbName }, parameterVals);
      appendSimpleCondition(filterBuilder, "database.catalogName", new String[] {catName}, parameterVals);
      if(pattern != null) {
        appendPatternCondition(filterBuilder, "functionName", pattern, parameterVals);
      }
      query = pm.newQuery(MFunction.class, filterBuilder.toString());
      query.setResult("functionName");
      query.setOrdering("functionName ascending");
      Collection names = (Collection) query.executeWithArray(parameterVals.toArray(new String[parameterVals.size()]));
      funcs = new ArrayList<>();
      for (Iterator i = names.iterator(); i.hasNext();) {
        funcs.add((String) i.next());
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return funcs;
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    boolean commited = false;
    Query query = null;

    NotificationEventResponse result = new NotificationEventResponse();
    result.setEvents(new ArrayList<>());
    try {
      openTransaction();
      long lastEvent = rqst.getLastEvent();
      int maxEvents = rqst.getMaxEvents() > 0 ? rqst.getMaxEvents() : Integer.MAX_VALUE;
      query = pm.newQuery(MNotificationLog.class, "eventId > lastEvent");
      query.declareParameters("java.lang.Long lastEvent");
      query.setOrdering("eventId ascending");
      query.setRange(0, maxEvents);
      Collection<MNotificationLog> events = (Collection) query.execute(lastEvent);
      commited = commitTransaction();
      if (events == null) {
        return result;
      }
      Iterator<MNotificationLog> i = events.iterator();
      while (i.hasNext()) {
        result.addToEvents(translateDbToThrift(i.next()));
      }
      return result;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void cleanWriteNotificationEvents(int olderThan) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      long tmp = System.currentTimeMillis() / 1000 - olderThan;
      int tooOld = (tmp > Integer.MAX_VALUE) ? 0 : (int) tmp;
      query = pm.newQuery(MTxnWriteNotificationLog.class, "eventTime < tooOld");
      query.declareParameters("java.lang.Integer tooOld");
      Collection<MTxnWriteNotificationLog> toBeRemoved = (Collection) query.execute(tooOld);
      if (CollectionUtils.isNotEmpty(toBeRemoved)) {
        pm.deletePersistentAll(toBeRemoved);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName) throws MetaException {
    List<WriteEventInfo> writeEventInfoList = null;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      List<String> parameterVals = new ArrayList<>();
      StringBuilder filterBuilder = new StringBuilder(" txnId == " + Long.toString(txnId));
      if (dbName != null && !"*".equals(dbName)) { // * means get all database, so no need to add filter
        appendSimpleCondition(filterBuilder, "database", new String[]{dbName}, parameterVals);
      }
      if (tableName != null && !"*".equals(tableName)) {
        appendSimpleCondition(filterBuilder, "table", new String[]{tableName}, parameterVals);
      }
      query = pm.newQuery(MTxnWriteNotificationLog.class, filterBuilder.toString());
      query.setOrdering("database,table ascending");
      List<MTxnWriteNotificationLog> mplans = (List<MTxnWriteNotificationLog>)query.executeWithArray(
              parameterVals.toArray(new String[parameterVals.size()]));
      pm.retrieveAll(mplans);
      commited = commitTransaction();
      if (mplans != null && mplans.size() > 0) {
        writeEventInfoList = Lists.newArrayList();
        for (MTxnWriteNotificationLog mplan : mplans) {
          WriteEventInfo writeEventInfo = new WriteEventInfo(mplan.getWriteId(), mplan.getDatabase(),
                  mplan.getTable(), mplan.getFiles());
          writeEventInfo.setPartition(mplan.getPartition());
          writeEventInfo.setPartitionObj(mplan.getPartObject());
          writeEventInfo.setTableObj(mplan.getTableObject());
          writeEventInfoList.add(writeEventInfo);
        }
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return writeEventInfoList;
  }

  private void prepareQuotes() throws SQLException {
    if (dbType == DatabaseProduct.MYSQL) {
      assert pm.currentTransaction().isActive();
      JDOConnection jdoConn = pm.getDataStoreConnection();
      Statement statement = null;
      try {
        statement = ((Connection)jdoConn.getNativeConnection()).createStatement();
        statement.execute("SET @@session.sql_mode=ANSI_QUOTES");
      } finally {
        if(statement != null){
          statement.close();
        }
        jdoConn.close();
      }
    }
  }

  private void lockForUpdate() throws MetaException {
    String selectQuery = "select \"NEXT_EVENT_ID\" from \"NOTIFICATION_SEQUENCE\"";
    String selectForUpdateQuery = sqlGenerator.addForUpdateClause(selectQuery);
    new RetryingExecutor(conf, () -> {
      prepareQuotes();
      Query query = pm.newQuery("javax.jdo.query.SQL", selectForUpdateQuery);
      query.setUnique(true);
      // only need to execute it to get db Lock
      query.execute();
      query.closeAll();
    }).run();
  }

  static class RetryingExecutor {
    interface Command {
      void process() throws Exception;
    }

    private static Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);
    private final int maxRetries;
    private final long sleepInterval;
    private int currentRetries = 0;
    private final Command command;

    RetryingExecutor(Configuration config, Command command) {
      this.maxRetries =
          MetastoreConf.getIntVar(config, ConfVars.NOTIFICATION_SEQUENCE_LOCK_MAX_RETRIES);
      this.sleepInterval = MetastoreConf.getTimeVar(config,
          ConfVars.NOTIFICATION_SEQUENCE_LOCK_RETRY_SLEEP_INTERVAL, TimeUnit.MILLISECONDS);
      this.command = command;
    }

    public void run() throws MetaException {
      while (true) {
        try {
          command.process();
          break;
        } catch (Exception e) {
          LOG.info(
              "Attempting to acquire the DB log notification lock: {} out of {}" +
                " retries", currentRetries, maxRetries, e);
          if (currentRetries >= maxRetries) {
            String message =
                "Couldn't acquire the DB log notification lock because we reached the maximum"
                    + " # of retries: " + maxRetries
                    + " retries. If this happens too often, then is recommended to "
                    + "increase the maximum number of retries on the"
                    + " hive.notification.sequence.lock.max.retries configuration";
            LOG.error(message, e);
            throw new MetaException(message + " :: " + e.getMessage());
          }
          currentRetries++;
          try {
            Thread.sleep(sleepInterval);
          } catch (InterruptedException e1) {
            String msg = "Couldn't acquire the DB notification log lock on " + currentRetries
                + " retry, because the following error: ";
            LOG.error(msg, e1);
            throw new MetaException(msg + e1.getMessage());
          }
        }
      }
    }
    public long getSleepInterval() {
      return sleepInterval;
    }
  }

  @Override
  public void addNotificationEvent(NotificationEvent entry) throws MetaException {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      lockForUpdate();
      query = pm.newQuery(MNotificationNextId.class);
      Collection<MNotificationNextId> ids = (Collection) query.execute();
      MNotificationNextId mNotificationNextId = null;
      boolean needToPersistId;
      if (CollectionUtils.isEmpty(ids)) {
        mNotificationNextId = new MNotificationNextId(1L);
        needToPersistId = true;
      } else {
        mNotificationNextId = ids.iterator().next();
        needToPersistId = false;
      }
      entry.setEventId(mNotificationNextId.getNextEventId());
      mNotificationNextId.incrementEventId();
      if (needToPersistId) {
        pm.makePersistent(mNotificationNextId);
      }
      pm.makePersistent(translateThriftToDb(entry));
      commited = commitTransaction();
    } catch (MetaException e) {
      LOG.error("Couldn't get lock for update", e);
      throw e;
    } finally {
      rollbackAndCleanup(commited, query);
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
      if (CollectionUtils.isNotEmpty(toBeRemoved)) {
        pm.deletePersistentAll(toBeRemoved);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
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
      if (CollectionUtils.isNotEmpty(ids)) {
        id = ids.iterator().next().getNextEventId() - 1;
      }
      commited = commitTransaction();
      return new CurrentNotificationEventId(id);
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    Long result = 0L;
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      long fromEventId = rqst.getFromEventId();
      String inputDbName = rqst.getDbName();
      String catName = rqst.isSetCatName() ? rqst.getCatName() : getDefaultCatalog(conf);
      String queryStr = "select count(eventId) from " + MNotificationLog.class.getName()
                + " where eventId > fromEventId && dbName == inputDbName && catalogName == catName";
      query = pm.newQuery(queryStr);
      query.declareParameters("java.lang.Long fromEventId, java.lang.String inputDbName," +
          " java.lang.String catName");
      result = (Long) query.execute(fromEventId, inputDbName, catName);
      commited = commitTransaction();
      return new NotificationEventsCountResponse(result.longValue());
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private MNotificationLog translateThriftToDb(NotificationEvent entry) {
    MNotificationLog dbEntry = new MNotificationLog();
    dbEntry.setEventId(entry.getEventId());
    dbEntry.setEventTime(entry.getEventTime());
    dbEntry.setEventType(entry.getEventType());
    dbEntry.setCatalogName(entry.isSetCatName() ? entry.getCatName() : getDefaultCatalog(conf));
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
    event.setCatName(dbEvent.getCatalogName());
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
        org.datanucleus.ExecutionContext ec = pm.getExecutionContext();
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
      LOG.warn("Failed to remove cached classloaders from DataNucleus NucleusContext", e);
    }
  }

  private static void clearClr(ClassLoaderResolver clr) throws Exception {
    if (clr != null){
      if (clr instanceof ClassLoaderResolverImpl){
        ClassLoaderResolverImpl clri = (ClassLoaderResolverImpl) clr;
        long resourcesCleared = clearFieldMap(clri,"resources");
        long loadedClassesCleared = clearFieldMap(clri,"loadedClasses");
        long unloadedClassesCleared = clearFieldMap(clri, "unloadedClasses");
        LOG.debug("Cleared ClassLoaderResolverImpl: {}, {}, {}",
            resourcesCleared, loadedClassesCleared, unloadedClassesCleared);
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
  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name)
      throws MetaException {
    try {
      return getPrimaryKeysInternal(catName, db_name, tbl_name);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLPrimaryKey> getPrimaryKeysInternal(final String catName,
                                                     final String db_name_input,
                                                     final String tbl_name_input)
  throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLPrimaryKey>(catName, db_name, tbl_name, true, true) {

      @Override
      protected List<SQLPrimaryKey> getSqlResult(GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException {
        return directSql.getPrimaryKeys(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLPrimaryKey> getJdoResult(
        GetHelper<List<SQLPrimaryKey>> ctx) throws MetaException, NoSuchObjectException {
        return getPrimaryKeysViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLPrimaryKey> getPrimaryKeysViaJdo(String catName, String db_name, String tbl_name)
      throws MetaException {
    boolean commited = false;
    List<SQLPrimaryKey> primaryKeys = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " parentTable.database.catalogName == cat_name &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
          "java.lang.String cat_name");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      primaryKeys = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        List<MFieldSchema> cols = currPK.getParentColumn() != null ?
            currPK.getParentColumn().getCols() : currPK.getParentTable().getPartitionKeys();
        int enableValidateRely = currPK.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        SQLPrimaryKey keyCol = new SQLPrimaryKey(db_name,
         tbl_name,
         cols.get(currPK.getParentIntegerIndex()).getName(),
         currPK.getPosition(),
         currPK.getConstraintName(), enable, validate, rely);
        keyCol.setCatName(catName);
        primaryKeys.add(keyCol);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return primaryKeys;
  }

  private String getPrimaryKeyConstraintName(String catName, String db_name, String tbl_name)
      throws MetaException {
    boolean commited = false;
    String ret = null;
    Query query = null;

    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
        + " parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.PRIMARY_KEY_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, " +
          "java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPK = (MConstraint) i.next();
        ret = currPK.getConstraintName();
        break;
      }
      commited = commitTransaction();
     } finally {
        rollbackAndCleanup(commited, query);
     }
     return ret;
   }

  @Override
  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name) throws MetaException {
    try {
      return getForeignKeysInternal(catName, parent_db_name,
        parent_tbl_name, foreign_db_name, foreign_tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLForeignKey> getForeignKeysInternal(
      final String catName, final String parent_db_name_input, final String parent_tbl_name_input,
      final String foreign_db_name_input, final String foreign_tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String parent_db_name = (parent_db_name_input != null) ? normalizeIdentifier(parent_db_name_input) : null;
    final String parent_tbl_name = (parent_tbl_name_input != null) ? normalizeIdentifier(parent_tbl_name_input) : null;
    final String foreign_db_name = (foreign_db_name_input != null) ? normalizeIdentifier(foreign_db_name_input) : null;
    final String foreign_tbl_name = (foreign_tbl_name_input != null)
                                    ? normalizeIdentifier(foreign_tbl_name_input) : null;
    final String db_name;
    final String tbl_name;
    if (foreign_tbl_name == null) {
      // The FK table name might be null if we are retrieving the constraint from the PK side
      db_name = parent_db_name;
      tbl_name = parent_tbl_name;
    } else {
      db_name = foreign_db_name;
      tbl_name = foreign_tbl_name;
    }
    return new GetListHelper<SQLForeignKey>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLForeignKey> getSqlResult(GetHelper<List<SQLForeignKey>> ctx) throws MetaException {
        return directSql.getForeignKeys(catName, parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }

      @Override
      protected List<SQLForeignKey> getJdoResult(
        GetHelper<List<SQLForeignKey>> ctx) throws MetaException, NoSuchObjectException {
        return getForeignKeysViaJdo(catName, parent_db_name,
          parent_tbl_name, foreign_db_name, foreign_tbl_name);
      }
    }.run(false);
  }

  private List<SQLForeignKey> getForeignKeysViaJdo(String catName, String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name) throws MetaException {
    boolean commited = false;
    List<SQLForeignKey> foreignKeys = null;
    Collection<?> constraints = null;
    Query query = null;
    Map<String, String> tblToConstraint = new HashMap<>();
    try {
      openTransaction();
      String queryText = " parentTable.database.catalogName == catName1 &&" +
          "childTable.database.catalogName == catName2 && "
        + (parent_tbl_name != null ? "parentTable.tableName == parent_tbl_name && " : "")
        + (parent_db_name != null ? " parentTable.database.name == parent_db_name && " : "")
        + (foreign_tbl_name != null ? " childTable.tableName == foreign_tbl_name && " : "")
        + (foreign_db_name != null ? " childTable.database.name == foreign_db_name && " : "")
        + " constraintType == MConstraint.FOREIGN_KEY_CONSTRAINT";
      queryText = queryText.trim();
      query = pm.newQuery(MConstraint.class, queryText);
      String paramText = "java.lang.String catName1, java.lang.String catName2" +
          (parent_tbl_name == null ? "" : ", java.lang.String parent_tbl_name")
        + (parent_db_name == null ? "" : " , java.lang.String parent_db_name")
        + (foreign_tbl_name == null ? "" : ", java.lang.String foreign_tbl_name")
        + (foreign_db_name == null ? "" : " , java.lang.String foreign_db_name");
      query.declareParameters(paramText);
      List<String> params = new ArrayList<>();
      params.add(catName);
      params.add(catName); // This is not a mistake, catName is in the where clause twice
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
      constraints = (Collection<?>) query.executeWithArray(params.toArray(new String[params.size()]));

      pm.retrieveAll(constraints);
      foreignKeys = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currPKFK = (MConstraint) i.next();
        List<MFieldSchema> parentCols = currPKFK.getParentColumn() != null ?
            currPKFK.getParentColumn().getCols() : currPKFK.getParentTable().getPartitionKeys();
        List<MFieldSchema> childCols = currPKFK.getChildColumn() != null ?
            currPKFK.getChildColumn().getCols() : currPKFK.getChildTable().getPartitionKeys();
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
          pkName = getPrimaryKeyConstraintName(currPKFK.getParentTable().getDatabase().getCatalogName(),
              currPKFK.getParentTable().getDatabase().getName(),
              currPKFK.getParentTable().getTableName());
          tblToConstraint.put(consolidatedtblName, pkName);
        }
        SQLForeignKey fk = new SQLForeignKey(
          currPKFK.getParentTable().getDatabase().getName(),
          currPKFK.getParentTable().getTableName(),
          parentCols.get(currPKFK.getParentIntegerIndex()).getName(),
          currPKFK.getChildTable().getDatabase().getName(),
          currPKFK.getChildTable().getTableName(),
          childCols.get(currPKFK.getChildIntegerIndex()).getName(),
          currPKFK.getPosition(),
          currPKFK.getUpdateRule(),
          currPKFK.getDeleteRule(),
          currPKFK.getConstraintName(), pkName, enable, validate, rely);
        fk.setCatName(catName);
        foreignKeys.add(fk);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return foreignKeys;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
          throws MetaException {
    try {
      return getUniqueConstraintsInternal(catName, db_name, tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsInternal(
      String catNameInput, final String db_name_input, final String tbl_name_input,
      boolean allowSql, boolean allowJdo) throws MetaException, NoSuchObjectException {
    final String catName = normalizeIdentifier(catNameInput);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLUniqueConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLUniqueConstraint> getSqlResult(GetHelper<List<SQLUniqueConstraint>> ctx)
              throws MetaException {
        return directSql.getUniqueConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLUniqueConstraint> getJdoResult(GetHelper<List<SQLUniqueConstraint>> ctx)
              throws MetaException, NoSuchObjectException {
        return getUniqueConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLUniqueConstraint> getUniqueConstraintsViaJdo(String catName, String db_name, String tbl_name)
          throws MetaException {
    boolean commited = false;
    List<SQLUniqueConstraint> uniqueConstraints = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name && parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.UNIQUE_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      uniqueConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        uniqueConstraints.add(new SQLUniqueConstraint(catName, db_name,
         tbl_name,
         cols.get(currConstraint.getParentIntegerIndex()).getName(),
         currConstraint.getPosition(),
         currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return uniqueConstraints;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
          throws MetaException {
    try {
      return getNotNullConstraintsInternal(catName, db_name, tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    try {
      return getDefaultConstraintsInternal(catName, db_name, tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    try {
      return getCheckConstraintsInternal(catName, db_name, tbl_name, true, true);
    } catch (NoSuchObjectException e) {
      throw new MetaException(ExceptionUtils.getStackTrace(e));
    }
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsInternal(
      String catName, final String db_name_input, final String tbl_name_input, boolean allowSql,
      boolean allowJdo) throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLDefaultConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLDefaultConstraint> getSqlResult(GetHelper<List<SQLDefaultConstraint>> ctx)
              throws MetaException {
        return directSql.getDefaultConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLDefaultConstraint> getJdoResult(GetHelper<List<SQLDefaultConstraint>> ctx)
              throws MetaException, NoSuchObjectException {
        return getDefaultConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  protected List<SQLCheckConstraint> getCheckConstraintsInternal(String catName, final String db_name_input,
                                                                     final String tbl_name_input, boolean allowSql,
                                                                 boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLCheckConstraint>(normalizeIdentifier(catName), db_name, tbl_name,
        allowSql, allowJdo) {

      @Override
      protected List<SQLCheckConstraint> getSqlResult(GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException {
        return directSql.getCheckConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLCheckConstraint> getJdoResult(GetHelper<List<SQLCheckConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getCheckConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLCheckConstraint> getCheckConstraintsViaJdo(String catName, String db_name, String tbl_name)
      throws MetaException {
    boolean commited = false;
    List<SQLCheckConstraint> checkConstraints= null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
          "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
          + " parentTable.database.catalogName == catName && constraintType == MConstraint.CHECK_CONSTRAINT");
      query.declareParameters("java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      checkConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        checkConstraints.add(new SQLCheckConstraint(catName, db_name, tbl_name,
                                                        cols.get(currConstraint.getParentIntegerIndex()).getName(),
                                                        currConstraint.getDefaultOrCheckValue(),
                                                    currConstraint.getConstraintName(), enable, validate, rely));
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
    return checkConstraints;
  }

  private List<SQLDefaultConstraint> getDefaultConstraintsViaJdo(String catName, String db_name, String tbl_name)
          throws MetaException {
    boolean commited = false;
    List<SQLDefaultConstraint> defaultConstraints= null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
        "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
            + " parentTable.database.catalogName == catName &&"
        + " constraintType == MConstraint.DEFAULT_CONSTRAINT");
      query.declareParameters(
          "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      defaultConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        defaultConstraints.add(new SQLDefaultConstraint(catName, db_name,
         tbl_name,
         cols.get(currConstraint.getParentIntegerIndex()).getName(),
         currConstraint.getDefaultOrCheckValue(), currConstraint.getConstraintName(), enable, validate, rely));
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
    return defaultConstraints;
  }

  protected List<SQLNotNullConstraint> getNotNullConstraintsInternal(String catName, final String db_name_input,
                                                                     final String tbl_name_input, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    catName = normalizeIdentifier(catName);
    final String db_name = normalizeIdentifier(db_name_input);
    final String tbl_name = normalizeIdentifier(tbl_name_input);
    return new GetListHelper<SQLNotNullConstraint>(catName, db_name, tbl_name, allowSql, allowJdo) {

      @Override
      protected List<SQLNotNullConstraint> getSqlResult(GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException {
        return directSql.getNotNullConstraints(catName, db_name, tbl_name);
      }

      @Override
      protected List<SQLNotNullConstraint> getJdoResult(GetHelper<List<SQLNotNullConstraint>> ctx)
          throws MetaException, NoSuchObjectException {
        return getNotNullConstraintsViaJdo(catName, db_name, tbl_name);
      }
    }.run(false);
  }

  private List<SQLNotNullConstraint> getNotNullConstraintsViaJdo(String catName, String db_name, String tbl_name)
      throws MetaException {
    boolean commited = false;
    List<SQLNotNullConstraint> notNullConstraints = null;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MConstraint.class,
          "parentTable.tableName == tbl_name && parentTable.database.name == db_name &&"
              + " parentTable.database.catalogName == catName && constraintType == MConstraint.NOT_NULL_CONSTRAINT");
      query.declareParameters(
          "java.lang.String tbl_name, java.lang.String db_name, java.lang.String catName");
      Collection<?> constraints = (Collection<?>) query.execute(tbl_name, db_name, catName);
      pm.retrieveAll(constraints);
      notNullConstraints = new ArrayList<>();
      for (Iterator<?> i = constraints.iterator(); i.hasNext();) {
        MConstraint currConstraint = (MConstraint) i.next();
        List<MFieldSchema> cols = currConstraint.getParentColumn() != null ?
            currConstraint.getParentColumn().getCols() : currConstraint.getParentTable().getPartitionKeys();
        int enableValidateRely = currConstraint.getEnableValidateRely();
        boolean enable = (enableValidateRely & 4) != 0;
        boolean validate = (enableValidateRely & 2) != 0;
        boolean rely = (enableValidateRely & 1) != 0;
        notNullConstraints.add(new SQLNotNullConstraint(catName, db_name,
            tbl_name,
            cols.get(currConstraint.getParentIntegerIndex()).getName(),
            currConstraint.getConstraintName(), enable, validate, rely));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return notNullConstraints;
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName,
                             String constraintName, boolean missingOk)
      throws NoSuchObjectException {
    boolean success = false;
    try {
      openTransaction();

      List<MConstraint> tabConstraints =
          listAllTableConstraintsWithOptionalConstraintName(catName,  dbName, tableName, constraintName);
      if (CollectionUtils.isNotEmpty(tabConstraints)) {
        pm.deletePersistentAll(tabConstraints);
      } else if (!missingOk) {
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

  @Override
  public void createISchema(ISchema schema) throws AlreadyExistsException, MetaException,
      NoSuchObjectException {
    boolean committed = false;
    MISchema mSchema = convertToMISchema(schema);
    try {
      openTransaction();
      if (getMISchema(schema.getCatName(), schema.getDbName(), schema.getName()) != null) {
        throw new AlreadyExistsException("Schema with name " + schema.getDbName() + "." +
            schema.getName() + " already exists");
      }
      pm.makePersistent(mSchema);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema)
      throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MISchema oldMSchema = getMISchema(schemaName.getCatName(), schemaName.getDbName(), schemaName.getSchemaName());
      if (oldMSchema == null) {
        throw new NoSuchObjectException("Schema " + schemaName + " does not exist");
      }

      // Don't support changing name or type
      oldMSchema.setCompatibility(newSchema.getCompatibility().getValue());
      oldMSchema.setValidationLevel(newSchema.getValidationLevel().getValue());
      oldMSchema.setCanEvolve(newSchema.isCanEvolve());
      if (newSchema.isSetSchemaGroup()) {
        oldMSchema.setSchemaGroup(newSchema.getSchemaGroup());
      }
      if (newSchema.isSetDescription()) {
        oldMSchema.setDescription(newSchema.getDescription());
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      ISchema schema = convertToISchema(getMISchema(schemaName.getCatName(), schemaName.getDbName(),
          schemaName.getSchemaName()));
      committed = commitTransaction();
      return schema;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private MISchema getMISchema(String catName, String dbName, String name) {
    Query query = null;
    try {
      name = normalizeIdentifier(name);
      dbName = normalizeIdentifier(dbName);
      catName = normalizeIdentifier(catName);
      query = pm.newQuery(MISchema.class,
          "name == schemaName && db.name == dbname && db.catalogName == cat");
      query.declareParameters(
          "java.lang.String schemaName, java.lang.String dbname, java.lang.String cat");
      query.setUnique(true);
      MISchema mSchema = (MISchema)query.execute(name, dbName, catName);
      pm.retrieve(mSchema);
      return mSchema;
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MISchema mSchema = getMISchema(schemaName.getCatName(), schemaName.getDbName(), schemaName.getSchemaName());
      if (mSchema != null) {
        pm.deletePersistentAll(mSchema);
      } else {
        throw new NoSuchObjectException("Schema " + schemaName + " does not exist");
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, NoSuchObjectException, MetaException {
    boolean committed = false;
    MSchemaVersion mSchemaVersion = convertToMSchemaVersion(schemaVersion);
    try {
      openTransaction();
      // Make sure it doesn't already exist
      if (getMSchemaVersion(schemaVersion.getSchema().getCatName(), schemaVersion.getSchema().getDbName(),
          schemaVersion.getSchema().getSchemaName(), schemaVersion.getVersion()) != null) {
        throw new AlreadyExistsException("Schema name " + schemaVersion.getSchema() +
            " version " + schemaVersion.getVersion() + " already exists");
      }
      // Make sure the referenced Schema exists
      if (getMISchema(schemaVersion.getSchema().getCatName(), schemaVersion.getSchema().getDbName(),
          schemaVersion.getSchema().getSchemaName()) == null) {
        throw new NoSuchObjectException("Schema " + schemaVersion.getSchema() + " does not exist");
      }
      pm.makePersistent(mSchemaVersion);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSchemaVersion oldMSchemaVersion = getMSchemaVersion(version.getSchema().getCatName(),
          version.getSchema().getDbName(), version.getSchema().getSchemaName(), version.getVersion());
      if (oldMSchemaVersion == null) {
        throw new NoSuchObjectException("No schema version " + version + " exists");
      }

      // We only support changing the SerDe mapping and the state.
      if (newVersion.isSetSerDe()) {
        oldMSchemaVersion.setSerDe(convertToMSerDeInfo(newVersion.getSerDe()));
      }
      if (newVersion.isSetState()) {
        oldMSchemaVersion.setState(newVersion.getState().getValue());
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        commitTransaction();
      }
    }
  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      SchemaVersion schemaVersion = convertToSchemaVersion(getMSchemaVersion(
          version.getSchema().getCatName(), version.getSchema().getDbName(),
          version.getSchema().getSchemaName(), version.getVersion()));
      committed = commitTransaction();
      return schemaVersion;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private MSchemaVersion getMSchemaVersion(String catName, String dbName, String schemaName, int version) {
    Query query = null;
    try {
      dbName = normalizeIdentifier(dbName);
      schemaName = normalizeIdentifier(schemaName);
      query = pm.newQuery(MSchemaVersion.class,
          "iSchema.name == schemaName && iSchema.db.name == dbName &&" +
              "iSchema.db.catalogName == cat && version == schemaVersion");
      query.declareParameters( "java.lang.String schemaName, java.lang.String dbName," +
          "java.lang.String cat, java.lang.Integer schemaVersion");
      query.setUnique(true);
      MSchemaVersion mSchemaVersion =
          (MSchemaVersion)query.executeWithArray(schemaName, dbName, catName, version);
      pm.retrieve(mSchemaVersion);
      if (mSchemaVersion != null) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
      }
      return mSchemaVersion;
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      String name = normalizeIdentifier(schemaName.getSchemaName());
      String dbName = normalizeIdentifier(schemaName.getDbName());
      String catName = normalizeIdentifier(schemaName.getCatName());
      query = pm.newQuery(MSchemaVersion.class,
          "iSchema.name == schemaName && iSchema.db.name == dbName && iSchema.db.catalogName == cat");
      query.declareParameters("java.lang.String schemaName, java.lang.String dbName, " +
          "java.lang.String cat");
      query.setUnique(true);
      query.setOrdering("version descending");
      query.setRange(0, 1);
      MSchemaVersion mSchemaVersion = (MSchemaVersion)query.execute(name, dbName, catName);
      pm.retrieve(mSchemaVersion);
      if (mSchemaVersion != null) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
      }
      SchemaVersion version = mSchemaVersion == null ? null : convertToSchemaVersion(mSchemaVersion);
      committed = commitTransaction();
      return version;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      String name = normalizeIdentifier(schemaName.getSchemaName());
      String dbName = normalizeIdentifier(schemaName.getDbName());
      String catName = normalizeIdentifier(schemaName.getCatName());
      query = pm.newQuery(MSchemaVersion.class, "iSchema.name == schemaName &&" +
          "iSchema.db.name == dbName && iSchema.db.catalogName == cat");
      query.declareParameters("java.lang.String schemaName, java.lang.String dbName," +
          " java.lang.String cat");
      query.setOrdering("version descending");
      List<MSchemaVersion> mSchemaVersions = query.setParameters(name, dbName, catName).executeList();
      pm.retrieveAll(mSchemaVersions);
      if (mSchemaVersions == null || mSchemaVersions.isEmpty()) {
        return null;
      }
      List<SchemaVersion> schemaVersions = new ArrayList<>(mSchemaVersions.size());
      for (MSchemaVersion mSchemaVersion : mSchemaVersions) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
        schemaVersions.add(convertToSchemaVersion(mSchemaVersion));
      }
      committed = commitTransaction();
      return schemaVersions;
    } finally {
      rollbackAndCleanup(committed, query);
    }
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace,
                                                        String type) throws MetaException {
    if (colName == null && colNamespace == null) {
      // Don't allow a query that returns everything, it will blow stuff up.
      throw new MetaException("You must specify column name or column namespace, else your query " +
          "may be too large");
    }
    boolean committed = false;
    Query query = null;
    try {
      openTransaction();
      if (colName != null) {
        colName = normalizeIdentifier(colName);
      }
      if (type != null) {
        type = normalizeIdentifier(type);
      }
      Map<String, String> parameters = new HashMap<>(3);
      StringBuilder sql = new StringBuilder("select SCHEMA_VERSION_ID from " +
          "SCHEMA_VERSION, COLUMNS_V2 where SCHEMA_VERSION.CD_ID = COLUMNS_V2.CD_ID ");
      if (colName != null) {
        sql.append("and COLUMNS_V2.COLUMN_NAME = :colName ");
        parameters.put("colName", colName);
      }
      if (colNamespace != null) {
        sql.append("and COLUMNS_V2.COMMENT = :colComment ");
        parameters.put("colComment", colNamespace);
      }
      if (type != null) {
        sql.append("and COLUMNS_V2.TYPE_NAME = :colType ");
        parameters.put("colType", type);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("getSchemaVersionsByColumns going to execute query " + sql.toString());
        LOG.debug("With parameters");
        for (Map.Entry<String, String> p : parameters.entrySet()) {
          LOG.debug(p.getKey() + " : " + p.getValue());
        }
      }
      query = pm.newQuery("javax.jdo.query.SQL", sql.toString());
      query.setClass(MSchemaVersion.class);
      List<MSchemaVersion> mSchemaVersions = query.setNamedParameters(parameters).executeList();
      if (mSchemaVersions == null || mSchemaVersions.isEmpty()) {
        return Collections.emptyList();
      }
      pm.retrieveAll(mSchemaVersions);
      List<SchemaVersion> schemaVersions = new ArrayList<>(mSchemaVersions.size());
      for (MSchemaVersion mSchemaVersion : mSchemaVersions) {
        pm.retrieveAll(mSchemaVersion.getCols());
        if (mSchemaVersion.getSerDe() != null) {
          pm.retrieve(mSchemaVersion.getSerDe());
        }
        schemaVersions.add(convertToSchemaVersion(mSchemaVersion));
      }
      committed = commitTransaction();
      return schemaVersions;
    } finally {
      rollbackAndCleanup(committed, query);
    }

  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException,
      MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSchemaVersion mSchemaVersion = getMSchemaVersion(version.getSchema().getCatName(),
          version.getSchema().getDbName(),
          version.getSchema().getSchemaName(), version.getVersion());
      if (mSchemaVersion != null) {
        pm.deletePersistentAll(mSchemaVersion);
      } else {
        throw new NoSuchObjectException("Schema version " + version + "does not exist");
      }
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      MSerDeInfo mSerDeInfo = getMSerDeInfo(serDeName);
      if (mSerDeInfo == null) {
        throw new NoSuchObjectException("No SerDe named " + serDeName);
      }
      SerDeInfo serde = convertToSerDeInfo(mSerDeInfo);
      committed = commitTransaction();
      return serde;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private MSerDeInfo getMSerDeInfo(String serDeName) throws MetaException {
    Query query = null;
    try {
      query = pm.newQuery(MSerDeInfo.class, "name == serDeName");
      query.declareParameters("java.lang.String serDeName");
      query.setUnique(true);
      MSerDeInfo mSerDeInfo = (MSerDeInfo)query.execute(serDeName);
      pm.retrieve(mSerDeInfo);
      return mSerDeInfo;
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    boolean committed = false;
    try {
      openTransaction();
      if (getMSerDeInfo(serde.getName()) != null) {
        throw new AlreadyExistsException("Serde with name " + serde.getName() + " already exists");
      }
      MSerDeInfo mSerde = convertToMSerDeInfo(serde);
      pm.makePersistent(mSerde);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }

  }

  private MISchema convertToMISchema(ISchema schema) throws NoSuchObjectException {
    return new MISchema(schema.getSchemaType().getValue(),
                        normalizeIdentifier(schema.getName()),
                        getMDatabase(schema.getCatName(), schema.getDbName()),
                        schema.getCompatibility().getValue(),
                        schema.getValidationLevel().getValue(),
                        schema.isCanEvolve(),
                        schema.isSetSchemaGroup() ? schema.getSchemaGroup() : null,
                        schema.isSetDescription() ? schema.getDescription() : null);
  }

  private ISchema convertToISchema(MISchema mSchema) {
    if (mSchema == null) {
      return null;
    }
    ISchema schema = new ISchema(SchemaType.findByValue(mSchema.getSchemaType()),
                                 mSchema.getName(),
                                 mSchema.getDb().getCatalogName(),
                                 mSchema.getDb().getName(),
                                 SchemaCompatibility.findByValue(mSchema.getCompatibility()),
                                 SchemaValidation.findByValue(mSchema.getValidationLevel()),
                                 mSchema.getCanEvolve());
    if (mSchema.getDescription() != null) {
      schema.setDescription(mSchema.getDescription());
    }
    if (mSchema.getSchemaGroup() != null) {
      schema.setSchemaGroup(mSchema.getSchemaGroup());
    }
    return schema;
  }

  private MSchemaVersion convertToMSchemaVersion(SchemaVersion schemaVersion) throws MetaException {
    return new MSchemaVersion(getMISchema(
        normalizeIdentifier(schemaVersion.getSchema().getCatName()),
        normalizeIdentifier(schemaVersion.getSchema().getDbName()),
        normalizeIdentifier(schemaVersion.getSchema().getSchemaName())),
        schemaVersion.getVersion(),
        schemaVersion.getCreatedAt(),
        createNewMColumnDescriptor(convertToMFieldSchemas(schemaVersion.getCols())),
        schemaVersion.isSetState() ? schemaVersion.getState().getValue() : 0,
        schemaVersion.isSetDescription() ? schemaVersion.getDescription() : null,
        schemaVersion.isSetSchemaText() ? schemaVersion.getSchemaText() : null,
        schemaVersion.isSetFingerprint() ? schemaVersion.getFingerprint() : null,
        schemaVersion.isSetName() ? schemaVersion.getName() : null,
        schemaVersion.isSetSerDe() ? convertToMSerDeInfo(schemaVersion.getSerDe()) : null);
  }

  private SchemaVersion convertToSchemaVersion(MSchemaVersion mSchemaVersion) throws MetaException {
    if (mSchemaVersion == null) {
      return null;
    }
    SchemaVersion schemaVersion = new SchemaVersion(
        new ISchemaName(mSchemaVersion.getiSchema().getDb().getCatalogName(),
            mSchemaVersion.getiSchema().getDb().getName(), mSchemaVersion.getiSchema().getName()),
        mSchemaVersion.getVersion(),
        mSchemaVersion.getCreatedAt(),
        convertToFieldSchemas(mSchemaVersion.getCols().getCols()));
    if (mSchemaVersion.getState() > 0) {
      schemaVersion.setState(SchemaVersionState.findByValue(mSchemaVersion.getState()));
    }
    if (mSchemaVersion.getDescription() != null) {
      schemaVersion.setDescription(mSchemaVersion.getDescription());
    }
    if (mSchemaVersion.getSchemaText() != null) {
      schemaVersion.setSchemaText(mSchemaVersion.getSchemaText());
    }
    if (mSchemaVersion.getFingerprint() != null) {
      schemaVersion.setFingerprint(mSchemaVersion.getFingerprint());
    }
    if (mSchemaVersion.getName() != null) {
      schemaVersion.setName(mSchemaVersion.getName());
    }
    if (mSchemaVersion.getSerDe() != null) {
      schemaVersion.setSerDe(convertToSerDeInfo(mSchemaVersion.getSerDe()));
    }
    return schemaVersion;
  }

  /**
   * This is a cleanup method which is used to rollback a active transaction
   * if the success flag is false and close the associated Query object. This method is used
   * internally and visible for testing purposes only
   * @param success Rollback the current active transaction if false
   * @param query Query object which needs to be closed
   */
  @VisibleForTesting
  void rollbackAndCleanup(boolean success, Query query) {
    try {
      if (!success) {
        rollbackTransaction();
      }
    } finally {
      if (query != null) {
        query.closeAll();
      }
    }
  }

  /**
   * This is a cleanup method which is used to rollback a active transaction
   * if the success flag is false and close the associated QueryWrapper object. This method is used
   * internally and visible for testing purposes only
   * @param success Rollback the current active transaction if false
   * @param queryWrapper QueryWrapper object which needs to be closed
   */
  @VisibleForTesting
  void rollbackAndCleanup(boolean success, QueryWrapper queryWrapper) {
    try {
      if (!success) {
        rollbackTransaction();
      }
    } finally {
      if (queryWrapper != null) {
        queryWrapper.close();
      }
    }
  }

  /**
   * To make possible to run multiple metastore in unit test
   * @param twoMetastoreTesting if we are using multiple metastore in unit test
   */
  @VisibleForTesting
  public static void setTwoMetastoreTesting(boolean twoMetastoreTesting) {
    forTwoMetastoreTesting = twoMetastoreTesting;
  }

  @VisibleForTesting
  Properties getProp() {
    return prop;
  }

  private void checkForConstraintException(Exception e, String msg) throws AlreadyExistsException {
    if (getConstraintException(e) != null) {
      LOG.error(msg, e);
      throw new AlreadyExistsException(msg);
    }
  }

  private Throwable getConstraintException(Throwable t) {
    while (t != null) {
      if (t instanceof SQLIntegrityConstraintViolationException) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  @Override
  public void createResourcePlan(
      WMResourcePlan resourcePlan, String copyFromName, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    boolean commited = false;
    String rpName = normalizeIdentifier(resourcePlan.getName());
    if (rpName.isEmpty()) {
      throw new InvalidObjectException("Resource name cannot be empty.");
    }
    MWMResourcePlan rp = null;
    if (copyFromName == null) {
      Integer queryParallelism = null;
      if (resourcePlan.isSetQueryParallelism()) {
        queryParallelism = resourcePlan.getQueryParallelism();
        if (queryParallelism <= 0) {
          throw new InvalidObjectException("Query parallelism should be positive.");
        }
      }
      rp = new MWMResourcePlan(rpName, queryParallelism, Status.DISABLED);
    } else {
      rp = new MWMResourcePlan(rpName, null, Status.DISABLED);
    }
    try {
      openTransaction();
      pm.makePersistent(rp);
      if (copyFromName != null) {
        MWMResourcePlan copyFrom = getMWMResourcePlan(copyFromName, false);
        if (copyFrom == null) {
          throw new NoSuchObjectException(copyFromName);
        }
        copyRpContents(rp, copyFrom);
      } else {
        // TODO: ideally, this should be moved outside to HiveMetaStore to be shared between
        //       all the RawStore-s. Right now there's no method to create a pool.
        if (defaultPoolSize > 0) {
          MWMPool defaultPool = new MWMPool(rp, "default", 1.0, defaultPoolSize, null);
          pm.makePersistent(defaultPool);
          rp.setPools(Sets.newHashSet(defaultPool));
          rp.setDefaultPool(defaultPool);
        }
      }
      commited = commitTransaction();
    } catch (InvalidOperationException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan already exists: ");
      throw e;
    } finally {
      if (!commited) {
        rollbackTransaction();
      }
    }
  }

  private void copyRpContents(MWMResourcePlan dest, MWMResourcePlan src) {
    dest.setQueryParallelism(src.getQueryParallelism());
    Map<String, MWMPool> pools = new HashMap<>();
    Map<String, Set<MWMPool>> triggersToPools = new HashMap<>();
    for (MWMPool copyPool : src.getPools()) {
      MWMPool pool = new MWMPool(dest, copyPool.getPath(), copyPool.getAllocFraction(),
          copyPool.getQueryParallelism(), copyPool.getSchedulingPolicy());
      pm.makePersistent(pool);
      pools.put(copyPool.getPath(), pool);
      if (copyPool.getTriggers() != null) {
        for (MWMTrigger trigger : copyPool.getTriggers()) {
          Set<MWMPool> p2t = triggersToPools.get(trigger.getName());
          if (p2t == null) {
            p2t = new HashSet<>();
            triggersToPools.put(trigger.getName(), p2t);
          }
          p2t.add(pool);
          pool.setTriggers(new HashSet<>());
        }
      }
    }
    dest.setPools(new HashSet<>(pools.values()));
    if (src.getDefaultPool() != null) {
      dest.setDefaultPool(pools.get(src.getDefaultPool().getPath()));
    }
    Set<MWMMapping> mappings = new HashSet<>();
    for (MWMMapping copyMapping : src.getMappings()) {
      MWMPool pool = null;
      if (copyMapping.getPool() != null) {
        pool = pools.get(copyMapping.getPool().getPath());
      }
      MWMMapping mapping = new MWMMapping(dest, copyMapping.getEntityType(),
          copyMapping.getEntityName(), pool, copyMapping.getOrdering());
      pm.makePersistent(mapping);
      mappings.add(mapping);
    }
    dest.setMappings(mappings);
    Set<MWMTrigger> triggers = new HashSet<>();
    for (MWMTrigger copyTrigger : src.getTriggers()) {
      Set<MWMPool> p2t = triggersToPools.get(copyTrigger.getName());
      if (p2t == null) {
        p2t = new HashSet<>();
      }
      MWMTrigger trigger = new MWMTrigger(dest, copyTrigger.getName(),
          copyTrigger.getTriggerExpression(), copyTrigger.getActionExpression(), p2t,
          copyTrigger.getIsInUnmanaged());
      pm.makePersistent(trigger);
      for (MWMPool pool : p2t) {
        pool.getTriggers().add(trigger);
      }
      triggers.add(trigger);
    }
    dest.setTriggers(triggers);
  }

  private WMResourcePlan fromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMResourcePlan rp = new WMResourcePlan();
    rp.setName(mplan.getName());
    rp.setStatus(WMResourcePlanStatus.valueOf(mplan.getStatus().name()));
    if (mplan.getQueryParallelism() != null) {
      rp.setQueryParallelism(mplan.getQueryParallelism());
    }
    if (mplan.getDefaultPool() != null) {
      rp.setDefaultPoolPath(mplan.getDefaultPool().getPath());
    }
    return rp;
  }

  private WMFullResourcePlan fullFromMResourcePlan(MWMResourcePlan mplan) {
    if (mplan == null) {
      return null;
    }
    WMFullResourcePlan rp = new WMFullResourcePlan();
    rp.setPlan(fromMResourcePlan(mplan));
    for (MWMPool mPool : mplan.getPools()) {
      rp.addToPools(fromMPool(mPool, mplan.getName()));
      for (MWMTrigger mTrigger : mPool.getTriggers()) {
        rp.addToPoolTriggers(new WMPoolTrigger(mPool.getPath(), mTrigger.getName()));
      }
    }
    for (MWMTrigger mTrigger : mplan.getTriggers()) {
      rp.addToTriggers(fromMWMTrigger(mTrigger, mplan.getName()));
    }
    for (MWMMapping mMapping : mplan.getMappings()) {
      rp.addToMappings(fromMMapping(mMapping, mplan.getName()));
    }
    return rp;
  }

  private WMPool fromMPool(MWMPool mPool, String rpName) {
    WMPool result = new WMPool(rpName, mPool.getPath());
    assert mPool.getAllocFraction() != null;
    result.setAllocFraction(mPool.getAllocFraction());
    assert mPool.getQueryParallelism() != null;
    result.setQueryParallelism(mPool.getQueryParallelism());
    result.setSchedulingPolicy(mPool.getSchedulingPolicy());
    return result;
  }

  private WMMapping fromMMapping(MWMMapping mMapping, String rpName) {
    WMMapping result = new WMMapping(rpName,
        mMapping.getEntityType().toString(), mMapping.getEntityName());
    if (mMapping.getPool() != null) {
      result.setPoolPath(mMapping.getPool().getPath());
    }
    if (mMapping.getOrdering() != null) {
      result.setOrdering(mMapping.getOrdering());
    }
    return result;
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException {
    boolean commited = false;
    try {
      openTransaction();
      WMFullResourcePlan fullRp = fullFromMResourcePlan(getMWMResourcePlan(name, false));
      commited = commitTransaction();
      return fullRp;
    } catch (InvalidOperationException e) {
      // Should not happen, edit check is false.
      throw new RuntimeException(e);
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  private MWMResourcePlan getMWMResourcePlan(String name, boolean editCheck)
      throws NoSuchObjectException, InvalidOperationException {
    return getMWMResourcePlan(name, editCheck, true);
  }

  private MWMResourcePlan getMWMResourcePlan(String name, boolean editCheck, boolean mustExist)
      throws NoSuchObjectException, InvalidOperationException {
    MWMResourcePlan resourcePlan;
    boolean commited = false;
    Query query = null;

    name = normalizeIdentifier(name);
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "name == rpname");
      query.declareParameters("java.lang.String rpname");
      query.setUnique(true);
      resourcePlan = (MWMResourcePlan) query.execute(name);
      pm.retrieve(resourcePlan);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    if (mustExist && resourcePlan == null) {
      throw new NoSuchObjectException("There is no resource plan named: " + name);
    }
    if (editCheck && resourcePlan != null
        && resourcePlan.getStatus() != MWMResourcePlan.Status.DISABLED) {
      throw new InvalidOperationException("Resource plan must be disabled to edit it.");
    }
    return resourcePlan;
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    List<WMResourcePlan> resourcePlans = new ArrayList();
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class);
      List<MWMResourcePlan> mplans = (List<MWMResourcePlan>) query.execute();
      pm.retrieveAll(mplans);
      commited = commitTransaction();
      if (mplans != null) {
        for (MWMResourcePlan mplan : mplans) {
          resourcePlans.add(fromMResourcePlan(mplan));
        }
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return resourcePlans;
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
    throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException, MetaException {
    name = name == null ? null : normalizeIdentifier(name);
    if (isReplace && name == null) {
      throw new InvalidOperationException("Cannot replace without specifying the source plan");
    }
    boolean commited = false;
    Query query = null;
    // This method only returns the result when activating a resource plan.
    // We could also add a boolean flag to be specified by the caller to see
    // when the result might be needed.
    WMFullResourcePlan result = null;
    try {
      openTransaction();
      if (isReplace) {
        result = handleAlterReplace(name, changes);
      } else {
        result = handleSimpleAlter(name, changes, canActivateDisabled, canDeactivate);
      }

      commited = commitTransaction();
      return result;
    } catch (Exception e) {
      checkForConstraintException(e, "Resource plan name should be unique: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private WMFullResourcePlan handleSimpleAlter(String name, WMNullableResourcePlan changes,
      boolean canActivateDisabled, boolean canDeactivate)
          throws InvalidOperationException, NoSuchObjectException, MetaException {
    MWMResourcePlan plan = name == null ? getActiveMWMResourcePlan()
        : getMWMResourcePlan(name, !changes.isSetStatus());
    boolean hasNameChange = changes.isSetName() && !changes.getName().equals(name);
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetIsSetQueryParallelism()
        || changes.isSetIsSetDefaultPoolPath() || hasNameChange) {
      if (changes.isSetStatus()) {
        throw new InvalidOperationException("Cannot change values during status switch.");
      } else if (plan.getStatus() != MWMResourcePlan.Status.DISABLED) {
        throw new InvalidOperationException("Resource plan must be disabled to edit it.");
      }
    }

    // Handle rename and other changes.
    if (changes.isSetName()) {
      String newName = normalizeIdentifier(changes.getName());
      if (newName.isEmpty()) {
        throw new InvalidOperationException("Cannot rename to empty value.");
      }
      if (!newName.equals(plan.getName())) {
        plan.setName(newName);
      }
    }
    if (changes.isSetIsSetQueryParallelism() && changes.isIsSetQueryParallelism()) {
      if (changes.isSetQueryParallelism()) {
        if (changes.getQueryParallelism() <= 0) {
          throw new InvalidOperationException("queryParallelism should be positive.");
        }
        plan.setQueryParallelism(changes.getQueryParallelism());
      } else {
        plan.setQueryParallelism(null);
      }
    }
    if (changes.isSetIsSetDefaultPoolPath() && changes.isIsSetDefaultPoolPath()) {
      if (changes.isSetDefaultPoolPath()) {
        MWMPool pool = getPool(plan, changes.getDefaultPoolPath());
        plan.setDefaultPool(pool);
      } else {
        plan.setDefaultPool(null);
      }
    }

    // Handle the status change.
    if (changes.isSetStatus()) {
      return switchStatus(name, plan,
          changes.getStatus().name(), canActivateDisabled, canDeactivate);
    }
    return null;
  }

  private WMFullResourcePlan handleAlterReplace(String name, WMNullableResourcePlan changes)
          throws InvalidOperationException, NoSuchObjectException, MetaException {
    // Verify that field changes are consistent with what Hive does. Note: we could handle this.
    if (changes.isSetQueryParallelism() || changes.isSetDefaultPoolPath()) {
      throw new InvalidOperationException("Cannot change values during replace.");
    }
    boolean isReplacingSpecific = changes.isSetName();
    boolean isReplacingActive = (changes.isSetStatus()
        && changes.getStatus() == WMResourcePlanStatus.ACTIVE);
    if (isReplacingActive == isReplacingSpecific) {
      throw new InvalidOperationException("Must specify a name, or the active plan; received "
          + changes.getName() + ", " + (changes.isSetStatus() ? changes.getStatus() : null));
    }
    if (name == null) {
      throw new InvalidOperationException("Invalid replace - no name specified");
    }
    MWMResourcePlan replacedPlan = isReplacingSpecific
        ? getMWMResourcePlan(changes.getName(), false) : getActiveMWMResourcePlan();
    MWMResourcePlan plan = getMWMResourcePlan(name, false);

    if (replacedPlan.getName().equals(plan.getName())) {
      throw new InvalidOperationException("A plan cannot replace itself");
    }
    // We will inherit the name and status from the plan we are replacing.
    String newName = replacedPlan.getName();
    int i = 0;
    String copyName = generateOldPlanName(newName, i);
    while (true) {
      MWMResourcePlan dup = getMWMResourcePlan(copyName, false, false);
      if (dup == null) {
        break;
      }
      // Note: this can still conflict with parallel transactions. We do not currently handle
      //       parallel changes from two admins (by design :().
      copyName = generateOldPlanName(newName, ++i);
    }
    replacedPlan.setName(copyName);
    plan.setName(newName);
    plan.setStatus(replacedPlan.getStatus());
    replacedPlan.setStatus(MWMResourcePlan.Status.DISABLED);
    // TODO: add a configurable option to skip the history and just drop it?
    return plan.getStatus() == Status.ACTIVE ? fullFromMResourcePlan(plan) : null;
  }

  private String generateOldPlanName(String newName, int i) {
    if (MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
      // Do not use datetime in tests to avoid result changes.
      return newName + "_old_" + i;
    } else {
      return newName + "_old_"
          + LocalDateTime.now().format(YMDHMS_FORMAT) + (i == 0 ? "" : ("_" + i));
    }
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    // Note: fullFromMResroucePlan needs to be called inside the txn, otherwise we could have
    //       deduplicated this with getActiveMWMResourcePlan.
    boolean commited = false;
    Query query = null;
    WMFullResourcePlan result = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "status == activeStatus");
      query.declareParameters("java.lang.String activeStatus");
      query.setUnique(true);
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(Status.ACTIVE.toString());
      if (mResourcePlan != null) {
        result = fullFromMResourcePlan(mResourcePlan);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return result;
  }

  private MWMResourcePlan getActiveMWMResourcePlan() throws MetaException {
    boolean commited = false;
    Query query = null;
    MWMResourcePlan result = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "status == activeStatus");
      query.declareParameters("java.lang.String activeStatus");
      query.setUnique(true);
      result = (MWMResourcePlan) query.execute(Status.ACTIVE.toString());
      pm.retrieve(result);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return result;
  }

  private WMFullResourcePlan switchStatus(String name, MWMResourcePlan mResourcePlan, String status,
      boolean canActivateDisabled, boolean canDeactivate) throws InvalidOperationException {
    Status currentStatus = mResourcePlan.getStatus();
    Status newStatus = null;
    try {
      newStatus = Status.valueOf(status);
    } catch (IllegalArgumentException e) {
      throw new InvalidOperationException("Invalid status: " + status);
    }

    if (newStatus == currentStatus) {
      return null;
    }

    boolean doActivate = false, doValidate = false;
    switch (currentStatus) {
      case ACTIVE: // No status change for active resource plan, first activate another plan.
        if (!canDeactivate) {
          throw new InvalidOperationException(
            "Resource plan " + name
              + " is active; activate another plan first, or disable workload management.");
        }
        break;
      case DISABLED:
        assert newStatus == Status.ACTIVE || newStatus == Status.ENABLED;
        doValidate = true;
        doActivate = (newStatus == Status.ACTIVE);
        if (doActivate && !canActivateDisabled) {
          throw new InvalidOperationException("Resource plan " +name
              + " is disabled and should be enabled before activation (or in the same command)");
        }
        break;
      case ENABLED:
        if (newStatus == Status.DISABLED) {
          mResourcePlan.setStatus(newStatus);
          return null; // A simple case.
        }
        assert newStatus == Status.ACTIVE;
        doActivate = true;
        break;
      default: throw new AssertionError("Unexpected status " + currentStatus);
    }
    if (doValidate) {
      // Note: this may use additional inputs from the caller, e.g. maximum query
      // parallelism in the cluster based on physical constraints.
      WMValidateResourcePlanResponse response = getResourcePlanErrors(mResourcePlan);
      if (!response.getErrors().isEmpty()) {
        throw new InvalidOperationException(
            "ResourcePlan: " + name + " is invalid: " + response.getErrors());
      }
    }
    if (doActivate) {
      // Deactivate currently active resource plan.
      deactivateActiveResourcePlan();
      mResourcePlan.setStatus(newStatus);
      return fullFromMResourcePlan(mResourcePlan);
    } else {
      mResourcePlan.setStatus(newStatus);
    }
    return null;
  }

  private void deactivateActiveResourcePlan() {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "status == \"ACTIVE\"");
      query.setUnique(true);
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute();
      // We may not have an active resource plan in the start.
      if (mResourcePlan != null) {
        mResourcePlan.setStatus(Status.ENABLED);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private static class PoolData {
    double totalChildrenAllocFraction = 0;
    boolean found = false;
    boolean hasChildren = false;
  }

  private PoolData getPoolData(Map<String, PoolData> poolInfo, String poolPath) {
    PoolData poolData = poolInfo.get(poolPath);
    if (poolData == null) {
      poolData = new PoolData();
      poolInfo.put(poolPath, poolData);
    }
    return poolData;
  }

  private WMValidateResourcePlanResponse getResourcePlanErrors(MWMResourcePlan mResourcePlan) {
    WMValidateResourcePlanResponse response = new WMValidateResourcePlanResponse();
    response.setErrors(new ArrayList());
    response.setWarnings(new ArrayList());
    Integer rpParallelism = mResourcePlan.getQueryParallelism();
    if (rpParallelism != null && rpParallelism < 1) {
      response.addToErrors("Query parallelism should for resource plan be positive. Got: " +
          rpParallelism);
    }
    int totalQueryParallelism = 0;
    Map<String, PoolData> poolInfo = new HashMap<>();
    for (MWMPool pool : mResourcePlan.getPools()) {
      PoolData currentPoolData = getPoolData(poolInfo, pool.getPath());
      currentPoolData.found = true;
      String parent = getParentPath(pool.getPath(), "");
      PoolData parentPoolData = getPoolData(poolInfo, parent);
      parentPoolData.hasChildren = true;
      parentPoolData.totalChildrenAllocFraction += pool.getAllocFraction();
      if (pool.getQueryParallelism() != null && pool.getQueryParallelism() < 1) {
        response.addToErrors("Invalid query parallelism for pool: " + pool.getPath());
      } else {
        totalQueryParallelism += pool.getQueryParallelism();
      }
      if (!MetaStoreUtils.isValidSchedulingPolicy(pool.getSchedulingPolicy())) {
        response.addToErrors("Invalid scheduling policy " + pool.getSchedulingPolicy() +
            " for pool: " + pool.getPath());
      }
    }
    if (rpParallelism != null) {
      if (rpParallelism < totalQueryParallelism) {
        response.addToErrors("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " exceeds resource plan query parallelism: " + rpParallelism);
      } else if (rpParallelism != totalQueryParallelism) {
        response.addToWarnings("Sum of all pools' query parallelism: " + totalQueryParallelism  +
            " is less than resource plan query parallelism: " + rpParallelism);
      }
    }
    for (Entry<String, PoolData> entry : poolInfo.entrySet()) {
      final PoolData poolData = entry.getValue();
      final boolean isRoot = entry.getKey().isEmpty();
      // Special case for root parent
      if (isRoot) {
        poolData.found = true;
        if (!poolData.hasChildren) {
          response.addToErrors("Root has no children");
        } else if (Math.abs(1.0 - poolData.totalChildrenAllocFraction) > 0.001) {
          response.addToErrors("Sum of root children pools' alloc fraction should be 1.0 got: " +
              poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
      if (!poolData.found) {
        response.addToErrors("Pool does not exists but has children: " + entry.getKey());
      }
      if (poolData.hasChildren) {
        if (!isRoot && 1.0 <= poolData.totalChildrenAllocFraction) {
          response.addToErrors("Sum of children pools' alloc fraction should be less than 1 got: "
              + poolData.totalChildrenAllocFraction + " for pool: " + entry.getKey());
        }
      }
    }
    // trigger and action expressions are not validated here, since counters are not
    // available and grammar check is there in the language itself.
    return response;
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    name = normalizeIdentifier(name);
    Query query = null;
    try {
      query = pm.newQuery(MWMResourcePlan.class, "name == rpName");
      query.declareParameters("java.lang.String rpName");
      query.setUnique(true);
      MWMResourcePlan mResourcePlan = (MWMResourcePlan) query.execute(name);
      if (mResourcePlan == null) {
        throw new NoSuchObjectException("Cannot find resourcePlan: " + name);
      }
      // Validate resource plan.
      return getResourcePlanErrors(mResourcePlan);
    } finally {
      rollbackAndCleanup(true, query);
    }
  }

  @Override
  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    name = normalizeIdentifier(name);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMResourcePlan.class, "name == rpname");
      query.declareParameters("java.lang.String rpname");
      query.setUnique(true);
      MWMResourcePlan resourcePlan = (MWMResourcePlan) query.execute(name);
      pm.retrieve(resourcePlan);
      if (resourcePlan == null) {
        throw new NoSuchObjectException("There is no resource plan named: " + name);
      }
      if (resourcePlan.getStatus() == Status.ACTIVE) {
        throw new MetaException("Cannot drop an active resource plan");
      }
      // First, drop all the dependencies.
      resourcePlan.setDefaultPool(null);
      pm.deletePersistentAll(resourcePlan.getTriggers());
      pm.deletePersistentAll(resourcePlan.getMappings());
      pm.deletePersistentAll(resourcePlan.getPools());
      pm.deletePersistent(resourcePlan);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(trigger.getResourcePlanName(), true);
      MWMTrigger mTrigger = new MWMTrigger(resourcePlan,
          normalizeIdentifier(trigger.getTriggerName()), trigger.getTriggerExpression(),
          trigger.getActionExpression(), null,
          trigger.isSetIsInUnmanaged() && trigger.isIsInUnmanaged());
      pm.makePersistent(mTrigger);
      commited = commitTransaction();
    } catch (Exception e) {
      checkForConstraintException(e, "Trigger already exists, use alter: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(trigger.getResourcePlanName(), true);
      MWMTrigger mTrigger = getTrigger(resourcePlan, trigger.getTriggerName());
      // Update the object.
      if (trigger.isSetTriggerExpression()) {
        mTrigger.setTriggerExpression(trigger.getTriggerExpression());
      }
      if (trigger.isSetActionExpression()) {
        mTrigger.setActionExpression(trigger.getActionExpression());
      }
      if (trigger.isSetIsInUnmanaged()) {
        mTrigger.setIsInUnmanaged(trigger.isIsInUnmanaged());
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private MWMTrigger getTrigger(MWMResourcePlan resourcePlan, String triggerName)
      throws NoSuchObjectException {
    triggerName = normalizeIdentifier(triggerName);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      // Get the MWMTrigger object from DN
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
      query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
      query.setUnique(true);
      MWMTrigger mTrigger = (MWMTrigger) query.execute(resourcePlan, triggerName);
      if (mTrigger == null) {
        throw new NoSuchObjectException("Cannot find trigger with name: " + triggerName);
      }
      pm.retrieve(mTrigger);
      commited = commitTransaction();
      return mTrigger;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException  {
    resourcePlanName = normalizeIdentifier(resourcePlanName);
    triggerName = normalizeIdentifier(triggerName);

    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, true);
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp && name == triggerName");
      query.declareParameters("MWMResourcePlan rp, java.lang.String triggerName");
      if (query.deletePersistentAll(resourcePlan, triggerName) != 1) {
        throw new NoSuchObjectException("Cannot delete trigger: " + triggerName);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    List<WMTrigger> triggers = new ArrayList();
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan;
      try {
        resourcePlan = getMWMResourcePlan(resourcePlanName, false);
      } catch (InvalidOperationException e) {
        // Should not happen, edit check is false.
        throw new RuntimeException(e);
      }
      query = pm.newQuery(MWMTrigger.class, "resourcePlan == rp");
      query.declareParameters("MWMResourcePlan rp");
      List<MWMTrigger> mTriggers = (List<MWMTrigger>) query.execute(resourcePlan);
      pm.retrieveAll(mTriggers);
      commited = commitTransaction();
      if (mTriggers != null) {
        for (MWMTrigger trigger : mTriggers) {
          triggers.add(fromMWMTrigger(trigger, resourcePlanName));
        }
      }
    } finally {
      rollbackAndCleanup(commited, query);
    }
    return triggers;
  }

  private WMTrigger fromMWMTrigger(MWMTrigger mTrigger, String resourcePlanName) {
    WMTrigger trigger = new WMTrigger();
    trigger.setResourcePlanName(resourcePlanName);
    trigger.setTriggerName(mTrigger.getName());
    trigger.setTriggerExpression(mTrigger.getTriggerExpression());
    trigger.setActionExpression(mTrigger.getActionExpression());
    trigger.setIsInUnmanaged(mTrigger.getIsInUnmanaged());
    return trigger;
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(pool.getResourcePlanName(), true);

      if (!poolParentExists(resourcePlan, pool.getPoolPath())) {
        throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
      }
      String policy = pool.getSchedulingPolicy();
      if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
        throw new InvalidOperationException("Invalid scheduling policy " + policy);
      }
      MWMPool mPool = new MWMPool(resourcePlan, pool.getPoolPath(), pool.getAllocFraction(),
          pool.getQueryParallelism(), policy);
      pm.makePersistent(mPool);
      commited = commitTransaction();
    } catch (Exception e) {
      checkForConstraintException(e, "Pool already exists: ");
      throw e;
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(pool.getResourcePlanName(), true);
      MWMPool mPool = getPool(resourcePlan, poolPath);
      pm.retrieve(mPool);
      if (pool.isSetAllocFraction()) {
        mPool.setAllocFraction(pool.getAllocFraction());
      }
      if (pool.isSetQueryParallelism()) {
        mPool.setQueryParallelism(pool.getQueryParallelism());
      }
      if (pool.isSetIsSetSchedulingPolicy() && pool.isIsSetSchedulingPolicy()) {
        if (pool.isSetSchedulingPolicy()) {
          String policy = pool.getSchedulingPolicy();
          if (!MetaStoreUtils.isValidSchedulingPolicy(policy)) {
            throw new InvalidOperationException("Invalid scheduling policy " + policy);
          }
          mPool.setSchedulingPolicy(pool.getSchedulingPolicy());
        } else {
          mPool.setSchedulingPolicy(null);
        }
      }
      if (pool.isSetPoolPath() && !pool.getPoolPath().equals(mPool.getPath())) {
        moveDescendents(resourcePlan, mPool.getPath(), pool.getPoolPath());
        mPool.setPath(pool.getPoolPath());
      }

      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  private MWMPool getPool(MWMResourcePlan resourcePlan, String poolPath)
      throws NoSuchObjectException {
    poolPath = normalizeIdentifier(poolPath);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path == poolPath");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      query.setUnique(true);
      MWMPool mPool = (MWMPool) query.execute(resourcePlan, poolPath);
      commited = commitTransaction();
      if (mPool == null) {
        throw new NoSuchObjectException("Cannot find pool: " + poolPath);
      }
      pm.retrieve(mPool);
      return mPool;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private void moveDescendents(MWMResourcePlan resourcePlan, String path, String newPoolPath)
      throws NoSuchObjectException {
    if (!poolParentExists(resourcePlan, newPoolPath)) {
      throw new NoSuchObjectException("Pool path is invalid, the parent does not exist");
    }
    boolean commited = false;
    Query query = null;
    openTransaction();
    try {
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      List<MWMPool> descPools = (List<MWMPool>) query.execute(resourcePlan, path + ".");
      pm.retrieveAll(descPools);
      for (MWMPool pool : descPools) {
        pool.setPath(newPoolPath + pool.getPath().substring(path.length()));
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private String getParentPath(String poolPath, String defValue) {
    int idx = poolPath.lastIndexOf('.');
    if (idx == -1) {
      return defValue;
    }
    return poolPath.substring(0, idx);
  }

  private boolean poolParentExists(MWMResourcePlan resourcePlan, String poolPath) {
    String parent = getParentPath(poolPath, null);
    if (parent == null) {
      return true;
    }
    try {
      getPool(resourcePlan, parent);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    poolPath = normalizeIdentifier(poolPath);
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, true);
      if (resourcePlan.getDefaultPool() != null &&
          resourcePlan.getDefaultPool().getPath().equals(poolPath)) {
        throw new InvalidOperationException("Cannot drop default pool of a resource plan");
      }
      if (poolHasChildren(resourcePlan, poolPath)) {
        throw new InvalidOperationException("Pool has children cannot drop.");
      }
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      if (query.deletePersistentAll(resourcePlan, poolPath) != 1) {
        throw new NoSuchObjectException("Cannot delete pool: " + poolPath);
      }
      commited = commitTransaction();
    } catch(Exception e) {
      if (getConstraintException(e) != null) {
        throw new InvalidOperationException("Please remove all mappings for this pool.");
      }
      throw e;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  private boolean poolHasChildren(MWMResourcePlan resourcePlan, String poolPath) {
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      query = pm.newQuery(MWMPool.class, "resourcePlan == rp && path.startsWith(poolPath)");
      query.declareParameters("MWMResourcePlan rp, java.lang.String poolPath");
      query.setResult("count(this)");
      query.setUnique(true);
      Long count = (Long) query.execute(resourcePlan, poolPath + ".");
      commited = commitTransaction();
      return count != null && count > 0;
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    EntityType entityType = EntityType.valueOf(mapping.getEntityType().trim().toUpperCase());
    String entityName = normalizeIdentifier(mapping.getEntityName());
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(mapping.getResourcePlanName(), true);
      MWMPool pool = null;
      if (mapping.isSetPoolPath()) {
        pool = getPool(resourcePlan, mapping.getPoolPath());
      }
      if (!update) {
        MWMMapping mMapping = new MWMMapping(resourcePlan, entityType, entityName, pool,
            mapping.getOrdering());
        pm.makePersistent(mMapping);
      } else {
        query = pm.newQuery(MWMMapping.class, "resourcePlan == rp && entityType == type " +
            "&& entityName == name");
        query.declareParameters(
            "MWMResourcePlan rp, java.lang.String type, java.lang.String name");
        query.setUnique(true);
        MWMMapping mMapping = (MWMMapping) query.execute(
            resourcePlan, entityType.toString(), entityName);
        mMapping.setPool(pool);
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    String entityType = mapping.getEntityType().trim().toUpperCase();
    String entityName = normalizeIdentifier(mapping.getEntityName());
    boolean commited = false;
    Query query = null;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(mapping.getResourcePlanName(), true);
      query = pm.newQuery(MWMMapping.class,
          "resourcePlan == rp && entityType == type && entityName == name");
      query.declareParameters("MWMResourcePlan rp, java.lang.String type, java.lang.String name");
      if (query.deletePersistentAll(resourcePlan, entityType, entityName) != 1) {
        throw new NoSuchObjectException("Cannot delete mapping.");
      }
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, query);
    }
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, true);
      MWMPool pool = getPool(resourcePlan, poolPath);
      MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
      pool.getTriggers().add(trigger);
      trigger.getPools().add(pool);
      pm.makePersistent(pool);
      pm.makePersistent(trigger);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException {
    boolean commited = false;
    try {
      openTransaction();
      MWMResourcePlan resourcePlan = getMWMResourcePlan(resourcePlanName, true);
      MWMPool pool = getPool(resourcePlan, poolPath);
      MWMTrigger trigger = getTrigger(resourcePlan, triggerName);
      pool.getTriggers().remove(trigger);
      trigger.getPools().remove(pool);
      pm.makePersistent(pool);
      pm.makePersistent(trigger);
      commited = commitTransaction();
    } finally {
      rollbackAndCleanup(commited, (Query)null);
    }
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
    LOG.debug("runtimeStat: " + stat);
    MRuntimeStat mStat = MRuntimeStat.fromThrift(stat);
    boolean committed = false;
    openTransaction();
    try {
      pm.makePersistent(mStat);
      committed = commitTransaction();
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    if (maxRetainSecs < 0) {
      LOG.warn("runtime stats retention is disabled");
      return 0;
    }
    boolean committed = false;
    try {
      openTransaction();
      int maxCreateTime = (int) (System.currentTimeMillis() / 1000) - maxRetainSecs;
      Query q = pm.newQuery(MRuntimeStat.class);
      q.setFilter("createTime <= maxCreateTime");
      q.declareParameters("int maxCreateTime");
      long deleted = q.deletePersistentAll(maxCreateTime);
      committed = commitTransaction();
      return (int) deleted;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    boolean committed = false;
    try {
      openTransaction();
      List<RuntimeStat> stats = getMRuntimeStats(maxEntries, maxCreateTime);
      committed = commitTransaction();
      return stats;
    } finally {
      if (!committed) {
        rollbackTransaction();
      }
    }
  }

  private List<RuntimeStat> getMRuntimeStats(int maxEntries, int maxCreateTime) {
    Query<MRuntimeStat> query = pm.newQuery(MRuntimeStat.class);
    query.setOrdering("createTime descending");
    if (maxCreateTime > 0) {
      query.setFilter("createTime < "+maxCreateTime);
    }
    if (maxEntries < 0) {
      maxEntries = Integer.MAX_VALUE;
    }
    List<RuntimeStat> ret = new ArrayList<>();
    List<MRuntimeStat> res = (List<MRuntimeStat>) query.execute();
    int totalEntries = 0;
    for (MRuntimeStat mRuntimeStat : res) {
      pm.retrieve(mRuntimeStat);
      totalEntries += mRuntimeStat.getWeight();
      ret.add(MRuntimeStat.toThrift(mRuntimeStat));
      if (totalEntries >= maxEntries) {
        break;
      }
    }
    return ret;
  }

}
